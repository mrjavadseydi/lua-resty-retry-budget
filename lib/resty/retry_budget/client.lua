local budget_mod = require "resty.retry_budget.budget"
local histogram = require "resty.retry_budget.histogram"
local backoff = require "resty.retry_budget.backoff"
local deadline = require "resty.retry_budget.deadline"

local _M = {}

local ALLOWED_PHASE = {
    rewrite = true,
    access = true,
    content = true,
    timer = true,
}

local function default_clock()
    return ngx.now()
end

local function new_meta()
    return {
        attempts = 0,
        hedged = false,
        budget_exhausted = false,
        winner = nil,
        retry_skipped = nil,
        hedge_skipped = nil,
        deadline_exceeded = false,
    }
end

local function copy_map(src)
    if type(src) ~= "table" then
        return nil
    end
    local out = {}
    for k, v in pairs(src) do
        out[k] = v
    end
    return out
end

function _M.header(headers, name)
    if type(headers) ~= "table" or type(name) ~= "string" then
        return nil
    end
    local want = name:lower()
    for k, v in pairs(headers) do
        if type(k) == "string" and k:lower() == want then
            if type(v) == "table" then
                for i = 1, #v do
                    local item = v[i]
                    if item ~= nil and tostring(item) ~= "" then
                        return tostring(item)
                    end
                end
            elseif v ~= nil and tostring(v) ~= "" then
                return tostring(v)
            end
        end
    end
    return nil
end

local function status_set(list)
    local set = {}
    if not list then
        set[502] = true
        set[503] = true
        set[504] = true
        return set
    end
    for i = 1, #list do
        set[list[i]] = true
    end
    return set
end

local function idempotent_set(src)
    local set = {}
    local from = src or { GET = true, HEAD = true, OPTIONS = true }
    -- Accepts a list { "GET", "PUT" } or a set { GET = true }.
    for k, v in pairs(from) do
        if type(k) == "number" then
            set[tostring(v):upper()] = true
        elseif v then
            set[tostring(k):upper()] = true
        end
    end
    return set
end

local mt = { __index = _M }

function _M.new(name, opts)
    opts = opts or {}
    local http_ok, http = pcall(require, "resty.http")
    if not http_ok then
        return nil, "retry-budget: lua-resty-http is required: " .. http
    end
    local headers_ok, http_headers = pcall(require, "resty.http_headers")
    if not headers_ok then
        return nil, "retry-budget: lua-resty-http is required: " .. http_headers
    end

    local clock = opts.clock or default_clock
    local bopts = {
        dict = opts.dict,
        dict_obj = opts.dict_obj,
        clock = clock,
        retry_ratio = opts.retry_ratio,
        min_retries_per_sec = opts.min_retries_per_sec,
        window_sec = opts.window_sec,
        refresh_interval = opts.refresh_interval,
        fail_mode = opts.fail_mode,
    }
    local budget, err = budget_mod.new(name, bopts)
    if not budget then
        return nil, err
    end

    local eopts = opts.estimator or {}
    local est, eerr = histogram.new(name, {
        dict = opts.dict,
        dict_obj = opts.dict_obj,
        clock = clock,
        window_sec = eopts.window_sec,
        slot_sec = eopts.slot_sec,
        min_samples = eopts.min_samples,
    })
    if not est then
        return nil, eerr
    end

    local bo = opts.backoff or {}
    local retry_on = opts.retry_on or {}
    local connect_error = retry_on.connect_error
    if connect_error == nil then
        connect_error = true
    end
    local timeout_retry = retry_on.timeout
    if timeout_retry == nil then
        timeout_retry = false
    end

    local max_attempts = opts.max_attempts or 3
    if type(max_attempts) ~= "number" or max_attempts < 1
        or max_attempts ~= math.floor(max_attempts) then
        return nil, "retry-budget: max_attempts must be an integer >= 1"
    end

    return setmetatable({
        name = name,
        budget = budget,
        estimator = est,
        clock = clock,
        random = opts.random or math.random,
        http = http,
        http_headers = http_headers,
        max_attempts = max_attempts,
        base_ms = bo.base_ms or 25,
        max_ms = bo.max_ms or 1000,
        connect_timeout = opts.connect_timeout or 1000,
        send_timeout = opts.send_timeout or 5000,
        read_timeout = opts.read_timeout or 5000,
        deadline_ms = opts.deadline_ms,
        keepalive_timeout = opts.keepalive_timeout,
        keepalive_pool = opts.keepalive_pool,
        respect_retry_after = opts.respect_retry_after ~= false,
        idempotency_header = opts.idempotency_header or "Idempotency-Key",
        idempotent = idempotent_set(opts.idempotent_methods),
        connect_error = connect_error,
        timeout_retry = timeout_retry,
        status_retry = status_set(retry_on.status),
    }, mt)
end

function _M.skip_reason(self, method, params)
    if type(params.body) == "function" then
        return "streaming_body"
    end
    if self.idempotent[method] then
        return nil
    end
    if _M.header(params.headers, self.idempotency_header) then
        return nil
    end
    return "non_idempotent"
end

function _M.should_retry(self, res, class)
    if class == "connect_error" or class == "connect_timeout"
        or class == "reset" then
        return self.connect_error
    end
    if class == "send_timeout" or class == "read_timeout" then
        return self.timeout_retry
    end
    if not res then
        return false
    end
    if self.status_retry[res.status] then
        return true
    end
    if self.respect_retry_after and res.status == 429 then
        local h = _M.header(res.headers, "Retry-After")
        return backoff.retry_after_ms(h, self.clock()) ~= nil
    end
    return false
end

function _M.response_wins(self, res, err, bad_status_waits)
    if err or not res then
        return false
    end
    if self:should_retry(res, "ok") then
        return not bad_status_waits
    end
    return true
end

function _M.retry_delay(self, failed_attempts, res)
    local jitter = backoff.delay_ms(failed_attempts, self.base_ms, self.max_ms,
                                     self.random)
    if res and self.respect_retry_after
        and (res.status == 429 or res.status == 503) then
        local h = _M.header(res.headers, "Retry-After")
        local parsed = backoff.retry_after_ms(h, self.clock())
        if parsed then
            if parsed > self.max_ms then
                parsed = self.max_ms
            end
            return parsed
        end
    end
    return jitter
end

local function arm(clock, deadline_ms, start, sock, configured)
    local remain = deadline.remaining_ms(deadline_ms, start, clock())
    local ms = deadline.next_timeout_ms(remain, configured)
    if not ms then
        return nil, "deadline", false
    end
    sock:settimeout(ms)
    local capped = remain ~= nil and remain < configured
    return true, nil, capped
end

local function class_of(stage, err, capped, reused)
    if err == "deadline" then
        return "deadline"
    end
    if err == "timeout" then
        if capped then
            return "deadline"
        end
        if stage == "connect" then
            return "connect_timeout"
        end
        if stage == "send" then
            return "send_timeout"
        end
        return "read_timeout"
    end
    if stage == "connect" then
        return "connect_error"
    end
    -- A reused keepalive connection the upstream already closed fails on
    -- send or before the status line. Only idempotent requests retry. On a
    -- fresh connection the same error would repeat, so it is not a reset.
    if reused and (stage == "send" or stage == "status") then
        return "reset"
    end
    return "transport"
end

local function recv_line(clock, deadline_ms, start, sock, read_timeout)
    local ok, err, capped = arm(clock, deadline_ms, start, sock, read_timeout)
    if not ok then
        return nil, err, capped
    end
    local line, rerr = sock:receive("*l")
    if not line then
        return nil, rerr, capped
    end
    return line, nil, capped
end

-- receiveany returns on the first bytes instead of waiting for a full
-- buffer. A single receive(n) re-arms its idle timer on every trickle
-- byte inside the C read loop, which would walk past the deadline.
local function recv_some(clock, deadline_ms, start, sock, read_timeout, maxn)
    local ok, err, capped = arm(clock, deadline_ms, start, sock, read_timeout)
    if not ok then
        return nil, err, capped
    end
    if sock.receiveany then
        local data, rerr = sock:receiveany(maxn)
        if not data then
            return nil, rerr, capped
        end
        return data, nil, capped
    end
    local data, rerr, partial = sock:receive(maxn)
    if not data then
        return nil, rerr, capped, partial
    end
    return data, nil, capped
end

local function recv_exact(clock, deadline_ms, start, sock, read_timeout, n)
    local chunks = {}
    local got = 0
    local i = 0
    while got < n do
        local data, err, capped, partial = recv_some(clock, deadline_ms, start,
            sock, read_timeout, math.min(n - got, 8192))
        if not data then
            if partial and partial ~= "" then
                i = i + 1
                chunks[i] = partial
            end
            return nil, err, capped, table.concat(chunks)
        end
        i = i + 1
        chunks[i] = data
        got = got + #data
    end
    return table.concat(chunks), nil, false
end

local function apply_keepalive(httpc, version, headers)
    local connection = headers["Connection"]
    if type(connection) == "table" then
        connection = table.concat(connection, ",")
    end
    if type(connection) ~= "string" then
        if version == 1.0 then
            httpc.keepalive = false
        end
        return
    end
    connection = connection:lower()
    if (version == 1.1 and connection:find("close", 1, true))
        or (version == 1.0 and not connection:find("keep-alive", 1, true)) then
        httpc.keepalive = false
    end
end

local function is_chunked(headers)
    local te = headers["Transfer-Encoding"]
    if not te then
        return false
    end
    if type(te) ~= "string" then
        te = table.concat(te, ",")
    end
    local last, seen_chunked
    for coding in (te .. ","):gmatch("(.-),") do
        last = coding:match("^[ \t]*([!#$%%&'*+%.%^_`|~%w%-]+)[ \t]*$")
        if not last then
            return nil, "invalid transfer-encoding"
        end
        if last:lower() == "chunked" then
            if seen_chunked then
                return nil, "invalid transfer-encoding"
            end
            seen_chunked = true
        end
    end
    return last:lower() == "chunked"
end

local function content_length(cl)
    if cl == nil then
        return nil
    end
    if type(cl) == "table" then
        cl = table.concat(cl, ",")
    end
    local len
    for value in (cl .. ","):gmatch("(.-),") do
        local digits = value:match("^[ \t]*([0-9]+)[ \t]*$")
        local n = digits and tonumber(digits)
        if not n or n > 9007199254740991 then
            return nil, "invalid content-length"
        end
        if len ~= nil and n ~= len then
            return nil, "conflicting content-length"
        end
        len = n
    end
    return len
end

local function read_headers(clock, deadline_ms, start, sock, read_timeout, http_headers)
    local headers = http_headers.new()
    while true do
        local line, err, capped = recv_line(clock, deadline_ms, start, sock,
                                             read_timeout)
        if not line then
            return nil, err, capped
        end
        if line == "" then
            return headers, nil, capped
        end
        local key, val = line:match("^([^:%s]+):[ \t]*(.*)$")
        if key then
            local prev = headers[key]
            if prev == nil then
                headers[key] = val
            elseif type(prev) == "table" then
                prev[#prev + 1] = val
            else
                headers[key] = { prev, val }
            end
        end
    end
end

local function read_chunked(clock, deadline_ms, start, sock, read_timeout)
    local chunks = {}
    local n = 0
    while true do
        local line, err, capped = recv_line(clock, deadline_ms, start, sock,
                                             read_timeout)
        if not line then
            return nil, err, capped
        end
        local hex = line:match("^(%x+)$") or line:match("^(%x+)[ \t]*;")
        local size = hex and tonumber(hex, 16)
        if not size or size > 9007199254740991 then
            return nil, "invalid chunk size", capped
        end
        if size == 0 then
            while true do
                local trailer, terr, tcapped = recv_line(clock, deadline_ms,
                    start, sock, read_timeout)
                if not trailer then
                    return nil, terr, tcapped
                end
                if trailer == "" then
                    break
                end
            end
            return table.concat(chunks), nil, capped
        end
        local data, rerr, rcapped = recv_exact(clock, deadline_ms, start, sock,
                                               read_timeout, size)
        if not data then
            return nil, rerr, rcapped
        end
        n = n + 1
        chunks[n] = data
        local crlf, cerr, ccapped = recv_exact(clock, deadline_ms, start, sock,
                                                read_timeout, 2)
        if not crlf then
            return nil, cerr, ccapped
        end
        if crlf ~= "\r\n" then
            return nil, "invalid chunk delimiter", ccapped
        end
    end
end

local function read_body(clock, deadline_ms, start, sock, read_timeout, headers, method, status, httpc)
    -- 101 takes over the connection and has no HTTP body. Other 1xx
    -- responses are interim and are consumed before this is called.
    if method == "HEAD" or status == 204 or status == 304 or status == 101 then
        return "", nil, false
    end
    local te, cl = headers["Transfer-Encoding"], headers["Content-Length"]
    if te ~= nil and cl ~= nil then
        return nil, "conflicting transfer-encoding and content-length", false
    end
    local chunked, terr = is_chunked(headers)
    if terr then
        return nil, terr, false
    end
    if chunked then
        return read_chunked(clock, deadline_ms, start, sock, read_timeout)
    end
    local len, lerr = content_length(cl)
    if lerr then
        return nil, lerr, false
    end
    if len == nil then
        httpc.keepalive = false
        local chunks = {}
        local n = 0
        while true do
            local data, err, capped, partial = recv_some(clock, deadline_ms,
                start, sock, read_timeout, 8192)
            if data then
                n = n + 1
                chunks[n] = data
            else
                if partial and partial ~= "" then
                    n = n + 1
                    chunks[n] = partial
                end
                if err == "closed" then
                    return table.concat(chunks), nil, capped
                end
                return nil, err, capped
            end
        end
    end
    if len == 0 then
        return "", nil, false
    end
    return recv_exact(clock, deadline_ms, start, sock, read_timeout, len)
end

local function close_httpc(httpc)
    if not httpc then
        return
    end
    httpc.keepalive = false
    httpc:close()
end

local function send_body(self, sock, body, deadline_ms, start)
    local streaming = type(body) == "function"
    while true do
        local chunk, err
        if streaming then
            chunk, err = body()
        else
            chunk = body
        end
        if chunk == nil then
            if err then
                return nil, err, false
            end
            return true
        end
        local ok, aerr, capped = arm(self.clock, deadline_ms, start, sock,
                                     self.send_timeout)
        if not ok then
            return nil, aerr, capped
        end
        ok, err = sock:send(chunk)
        if not ok then
            return nil, err, capped
        end
        if not streaming then
            return true
        end
    end
end

function _M.finish_socket(self, httpc, keep)
    if not httpc then
        return
    end
    if not keep or httpc.keepalive == false then
        close_httpc(httpc)
        return
    end
    local ok, err = httpc:set_keepalive(self.keepalive_timeout, self.keepalive_pool)
    if not ok and err ~= "closed" then
        close_httpc(httpc)
    end
end

function _M.record_sample(self, kind, ms)
    if ms == nil then
        return
    end
    if kind == "response" or kind == "timeout" then
        self.estimator:record(ms)
    end
end

-- One HTTP exchange. The body is read on this light thread so a later
-- ngx.thread.kill finalizes the cosocket. Caller pools or closes `httpc`.
function _M.attempt(self, uri, params, deadline_ms, slot, request_start)
    local started = self.clock()
    -- Socket timeouts share the request_uri clock. A retry must not
    -- receive a fresh deadline_ms.
    local origin = request_start or started
    local httpc, err = self.http.new()
    if not httpc then
        return nil, err or "no socket", nil, nil, nil, "error"
    end
    if slot then
        slot.httpc = httpc
        slot.started = started
    end

    local parsed, perr = httpc:parse_uri(uri, false)
    if not parsed then
        close_httpc(httpc)
        return nil, perr, nil, nil, nil, "error"
    end
    local scheme, host, port = parsed[1], parsed[2], parsed[3]
    local path = params.path or parsed[4]
    local query = params.query or parsed[5]

    local sock = httpc.sock
    local ok, aerr, capped = arm(self.clock, deadline_ms, origin, sock,
                                 self.connect_timeout)
    if not ok then
        close_httpc(httpc)
        return nil, aerr, nil, nil, nil, "deadline"
    end

    if slot then
        slot.io_started = true
    end
    ok, err = httpc:connect({
        scheme = scheme,
        host = host,
        port = port,
        ssl_server_name = params.ssl_server_name or host,
        ssl_verify = params.ssl_verify,
        pool = params.pool,
        pool_size = params.pool_size,
    })
    if not ok then
        local class = class_of("connect", err, capped)
        local elapsed = (self.clock() - started) * 1000
        local kind = (err == "timeout" or err == "deadline") and "timeout" or nil
        close_httpc(httpc)
        return nil, err, nil, elapsed, kind, class
    end
    local reused = sock.getreusedtimes ~= nil and (sock:getreusedtimes() or 0) > 0

    ok, aerr, capped = arm(self.clock, deadline_ms, origin, sock, self.send_timeout)
    if not ok then
        close_httpc(httpc)
        local elapsed = (self.clock() - started) * 1000
        return nil, aerr, nil, elapsed, "timeout", "deadline"
    end

    local req = {
        method = params.method,
        path = path,
        query = query,
        headers = copy_map(params.headers),
        body = params.body,
        version = params.version,
    }
    ok, err = httpc:send_request(req)
    if not ok then
        local class = class_of("send", err, capped, reused)
        local elapsed = (self.clock() - started) * 1000
        local kind = (err == "timeout" or err == "deadline") and "timeout" or nil
        close_httpc(httpc)
        return nil, err, nil, elapsed, kind, class
    end
    local body_pending = _M.header(req.headers, "Expect") == "100-continue"

    local version, status, reason, headers
    local interim = 0
    while true do
        local line, lerr, lcapped = recv_line(self.clock, deadline_ms, origin,
                                               sock, self.read_timeout)
        if not line then
            local class = class_of(interim == 0 and "status" or "read", lerr,
                                   lcapped, reused)
            local elapsed = (self.clock() - started) * 1000
            local kind = (lerr == "timeout" or lerr == "deadline") and "timeout" or nil
            close_httpc(httpc)
            return nil, lerr, nil, elapsed, kind, class
        end
        version = tonumber(line:sub(6, 8))
        status = tonumber(line:sub(10, 12))
        if not version or not status then
            close_httpc(httpc)
            return nil, "bad status line", nil, nil, nil, "transport"
        end
        reason = line:sub(14)
        local herr, hcapped
        headers, herr, hcapped = read_headers(self.clock, deadline_ms, origin,
            sock, self.read_timeout, self.http_headers)
        if not headers then
            local class = class_of("read", herr, hcapped)
            local elapsed = (self.clock() - started) * 1000
            local kind = (herr == "timeout" or herr == "deadline") and "timeout" or nil
            close_httpc(httpc)
            return nil, herr, nil, elapsed, kind, class
        end
        if status == 100 and body_pending then
            body_pending = false
            ok, err, capped = send_body(self, sock, req.body, deadline_ms, origin)
            if not ok then
                local class = class_of("send", err, capped)
                local elapsed = (self.clock() - started) * 1000
                local kind = (err == "timeout" or err == "deadline") and "timeout" or nil
                close_httpc(httpc)
                return nil, err, nil, elapsed, kind, class
            end
        end
        -- 100, 102, and 103 are followed by a final response on this
        -- socket. 101 is final: the connection is no longer HTTP.
        if status < 100 or status >= 200 or status == 101 then
            break
        end
        interim = interim + 1
        if interim > 16 then
            close_httpc(httpc)
            return nil, "too many interim responses", nil, nil, nil, "transport"
        end
    end
    apply_keepalive(httpc, version, headers)
    if status == 101 then
        httpc.keepalive = false
    end

    local body, berr, bcapped = read_body(self.clock, deadline_ms, origin, sock,
        self.read_timeout, headers, params.method, status, httpc)
    if not body then
        local class = class_of("read", berr, bcapped)
        local elapsed = (self.clock() - started) * 1000
        local kind = (berr == "timeout" or berr == "deadline") and "timeout" or nil
        close_httpc(httpc)
        return nil, berr, nil, elapsed, kind, class
    end

    local elapsed = (self.clock() - started) * 1000
    return {
        status = status,
        reason = reason,
        headers = headers,
        body = body,
    }, nil, httpc, elapsed, "response", "ok"
end

local function attempt_with_deadline(self, uri, params, deadline_ms, start)
    if deadline_ms == nil then
        return self:attempt(uri, params, nil, nil, start)
    end

    local slot = {}
    local attempt = ngx.thread.spawn(function()
        return { self:attempt(uri, params, deadline_ms, slot, start) }
    end)
    local remain = deadline.remaining_ms(deadline_ms, start, self.clock())
    local timer
    if remain >= 1 then
        timer = ngx.thread.spawn(function()
            ngx.sleep(remain / 1000)
            return true
        end)
    end
    local ok, result
    if timer then
        ok, result = ngx.thread.wait(attempt, timer)
        ngx.thread.kill(timer)
        remain = deadline.remaining_ms(deadline_ms, start, self.clock())
    end
    if not timer or result == true or remain < 1 then
        -- Kill the reader before close(), which otherwise reports busy.
        ngx.thread.kill(attempt)
        close_httpc(slot.httpc)
        local elapsed, kind
        if slot.io_started then
            elapsed = (self.clock() - slot.started) * 1000
            kind = "timeout"
        end
        return nil, "deadline", nil, elapsed, kind, "deadline"
    end
    if not ok then
        close_httpc(slot.httpc)
        return nil, result, nil, nil, nil, "error"
    end
    return unpack(result, 1, 6)
end

function _M.sequential(self, uri, params, meta, already, last_res, last_err,
                       last_class, deadline_ms, start)
    local max_attempts = self.max_attempts
    if meta.retry_skipped then
        max_attempts = 1
    end
    local res, err, class = last_res, last_err, last_class
    local attempt = already
    while attempt < max_attempts do
        if attempt >= 1 then
            if class == "deadline" or meta.deadline_exceeded then
                meta.deadline_exceeded = true
                break
            end
            if not self:should_retry(res, class) then
                break
            end
            local delay = self:retry_delay(attempt, res)
            local remain = deadline.remaining_ms(deadline_ms, start, self.clock())
            if remain and delay >= remain then
                meta.deadline_exceeded = true
                break
            end
            if not self.budget:try_acquire() then
                meta.budget_exhausted = true
                break
            end
            if delay > 0 then
                ngx.sleep(delay / 1000)
            end
            remain = deadline.remaining_ms(deadline_ms, start, self.clock())
            if remain and remain < 1 then
                meta.deadline_exceeded = true
                break
            end
        end

        attempt = attempt + 1
        meta.attempts = attempt
        local httpc, sample_ms, sample_kind
        res, err, httpc, sample_ms, sample_kind, class =
            attempt_with_deadline(self, uri, params, deadline_ms, start)
        meta.winner = attempt
        self:record_sample(sample_kind, sample_ms)
        if class == "ok" then
            self:finish_socket(httpc, true)
        end
        if class == "deadline" then
            meta.deadline_exceeded = true
            break
        end
    end
    return res, err, meta
end

function _M.request_uri(self, uri, params, call_opts)
    local meta = new_meta()
    local phase = ngx.get_phase()
    if not ALLOWED_PHASE[phase] then
        meta.retry_skipped = "phase"
        meta.hedge_skipped = "phase"
        return nil, 'retry-budget: client API is not available in phase "'
            .. phase .. '"', meta
    end

    params = params or {}
    call_opts = call_opts or {}
    local method = (params.method or "GET"):upper()
    local req = copy_map(params) or {}
    req.method = method

    local reason = self:skip_reason(method, req)
    meta.retry_skipped = reason

    local deadline_ms = call_opts.deadline_ms
    if deadline_ms == nil then
        deadline_ms = self.deadline_ms
    end
    local start = self.clock()
    self.budget:record_request()

    local hedge_opt = call_opts.hedge
    if hedge_opt and hedge_opt.enabled then
        if reason then
            meta.hedge_skipped = reason
        elseif self.max_attempts < 2 then
            meta.hedge_skipped = "max_attempts"
        else
            local hedge = require "resty.retry_budget.hedge"
            return hedge.run(self, uri, req, hedge_opt, meta, deadline_ms, start)
        end
    elseif hedge_opt then
        meta.hedge_skipped = "disabled"
    end

    return self:sequential(uri, req, meta, 0, nil, nil, nil, deadline_ms, start)
end

return _M
