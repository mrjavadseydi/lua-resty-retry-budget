local budget_mod = require "resty.retry_budget.budget"
local histogram = require "resty.retry_budget.histogram"
local backoff = require "resty.retry_budget.backoff"
local deadline = require "resty.retry_budget.deadline"

local byte = string.byte
local find = string.find
local floor = math.floor
local gmatch = string.gmatch
local huge = math.huge
local lower = string.lower
local match = string.match
local min = math.min
local random = math.random
local sub = string.sub
local upper = string.upper
local concat = table.concat
local pairs = pairs
local pcall = pcall
local require = require
local setmetatable = setmetatable
local tonumber = tonumber
local tostring = tostring
local type = type
local loaded = package.loaded

local hedge_mod

local TCHAR = {}
do
    local chars = "!#$%&'*+-.^_`|~0123456789"
        .. "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    for i = 1, #chars do
        TCHAR[byte(chars, i)] = true
    end
end

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

local function first_non_empty(v)
    if type(v) == "table" then
        for i = 1, #v do
            local item = v[i]
            if item ~= nil then
                local s = type(item) == "string" and item or tostring(item)
                if s ~= "" then
                    return s
                end
            end
        end
        return nil
    end
    if v ~= nil then
        local s = type(v) == "string" and v or tostring(v)
        if s ~= "" then
            return s
        end
    end
    return nil
end

function _M.header(headers, name)
    if type(headers) ~= "table" or type(name) ~= "string" then
        return nil
    end
    local direct = first_non_empty(headers[name])
    if direct ~= nil then
        return direct
    end
    local want = lower(name)
    if want ~= name then
        direct = first_non_empty(headers[want])
        if direct ~= nil then
            return direct
        end
    end
    for k, v in pairs(headers) do
        if type(k) == "string" and lower(k) == want then
            local found = first_non_empty(v)
            if found ~= nil then
                return found
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
            set[upper(tostring(v))] = true
        elseif v then
            set[upper(tostring(k))] = true
        end
    end
    return set
end

local mt = { __index = _M }

function _M.new(name, opts)
    opts = opts or {}
    local http = loaded["resty.http"]
    if not http then
        local http_ok, mod = pcall(require, "resty.http")
        if not http_ok then
            return nil, "retry-budget: lua-resty-http is required: " .. mod
        end
        http = mod
    end
    local http_headers = loaded["resty.http_headers"]
    if not http_headers then
        local headers_ok, mod = pcall(require, "resty.http_headers")
        if not headers_ok then
            return nil, "retry-budget: lua-resty-http is required: " .. mod
        end
        http_headers = mod
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
        or max_attempts ~= floor(max_attempts) then
        return nil, "retry-budget: max_attempts must be an integer >= 1"
    end

    local base_ms = bo.base_ms
    if base_ms == nil then
        base_ms = 25
    end
    if type(base_ms) ~= "number" or not (base_ms >= 0 and base_ms < huge) then
        return nil, "retry-budget: backoff.base_ms must be >= 0"
    end

    local max_ms = bo.max_ms
    if max_ms == nil then
        max_ms = 1000
    end
    if type(max_ms) ~= "number" or not (max_ms >= 0 and max_ms < huge) then
        return nil, "retry-budget: backoff.max_ms must be >= 0"
    end

    local connect_timeout = opts.connect_timeout
    if connect_timeout == nil then
        connect_timeout = 1000
    end
    if type(connect_timeout) ~= "number"
        or not (connect_timeout >= 1 and connect_timeout < huge) then
        return nil, "retry-budget: connect_timeout must be >= 1"
    end

    local send_timeout = opts.send_timeout
    if send_timeout == nil then
        send_timeout = 5000
    end
    if type(send_timeout) ~= "number"
        or not (send_timeout >= 1 and send_timeout < huge) then
        return nil, "retry-budget: send_timeout must be >= 1"
    end

    local read_timeout = opts.read_timeout
    if read_timeout == nil then
        read_timeout = 5000
    end
    if type(read_timeout) ~= "number"
        or not (read_timeout >= 1 and read_timeout < huge) then
        return nil, "retry-budget: read_timeout must be >= 1"
    end

    local deadline_ms = opts.deadline_ms
    if deadline_ms ~= nil and (type(deadline_ms) ~= "number"
        or not (deadline_ms >= 0 and deadline_ms < huge)) then
        return nil, "retry-budget: deadline_ms must be >= 0"
    end

    return setmetatable({
        name = name,
        budget = budget,
        estimator = est,
        clock = clock,
        random = opts.random or random,
        http = http,
        http_headers = http_headers,
        max_attempts = max_attempts,
        base_ms = base_ms,
        max_ms = max_ms,
        connect_timeout = connect_timeout,
        send_timeout = send_timeout,
        read_timeout = read_timeout,
        deadline_ms = deadline_ms,
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
    local step = sock.receiveany and 65536 or 8192
    local first, err, capped, partial = recv_some(clock, deadline_ms, start,
        sock, read_timeout, min(n, step))
    if not first then
        return nil, err, capped, partial or ""
    end
    local got = #first
    if got >= n then
        return first, nil, false
    end
    local chunks = { first }
    local i = 1
    while got < n do
        local data, rerr, rcapped, rpartial = recv_some(clock, deadline_ms,
            start, sock, read_timeout, min(n - got, step))
        if not data then
            if rpartial and rpartial ~= "" then
                i = i + 1
                chunks[i] = rpartial
            end
            return nil, rerr, rcapped, concat(chunks)
        end
        i = i + 1
        chunks[i] = data
        got = got + #data
    end
    return concat(chunks), nil, false
end

local function apply_keepalive(httpc, version, headers)
    local connection = headers["Connection"]
    if type(connection) == "table" then
        connection = concat(connection, ",")
    end
    if type(connection) ~= "string" then
        if version == 1.0 then
            httpc.keepalive = false
        end
        return
    end
    connection = lower(connection)
    if (version == 1.1 and find(connection, "close", 1, true))
        or (version == 1.0 and not find(connection, "keep-alive", 1, true)) then
        httpc.keepalive = false
    end
end

local function is_chunked(headers)
    local te = headers["Transfer-Encoding"]
    if not te then
        return false
    end
    if type(te) ~= "string" then
        te = concat(te, ",")
    end
    local last, seen_chunked
    for coding in gmatch(te .. ",", "(.-),") do
        last = match(coding, "^[ \t]*([!#$%%&'*+%.%^_`|~%w%-]+)[ \t]*$")
        if not last then
            return nil, "invalid transfer-encoding"
        end
        if lower(last) == "chunked" then
            if seen_chunked then
                return nil, "invalid transfer-encoding"
            end
            seen_chunked = true
        end
    end
    return lower(last) == "chunked"
end

local function parse_uint_ows(s, i, j)
    while i <= j do
        local b = byte(s, i)
        if b ~= 32 and b ~= 9 then
            break
        end
        i = i + 1
    end
    while j >= i do
        local b = byte(s, j)
        if b ~= 32 and b ~= 9 then
            break
        end
        j = j - 1
    end
    if i > j then
        return nil
    end
    local n = 0
    for p = i, j do
        local b = byte(s, p)
        if b < 48 or b > 57 then
            return nil
        end
        n = n * 10 + (b - 48)
        if n > 9007199254740991 then
            return nil
        end
    end
    return n
end

local function content_length(cl)
    if cl == nil then
        return nil
    end
    if type(cl) == "table" then
        cl = concat(cl, ",")
    end
    local total_len = #cl
    local first_comma = find(cl, ",", 1, true)
    if not first_comma then
        local n = parse_uint_ows(cl, 1, total_len)
        if not n then
            return nil, "invalid content-length"
        end
        return n
    end
    local len
    local pos = 1
    while true do
        local comma = pos == 1 and first_comma or find(cl, ",", pos, true)
        local seg_end = comma and (comma - 1) or total_len
        local n = parse_uint_ows(cl, pos, seg_end)
        if not n then
            return nil, "invalid content-length"
        end
        if len ~= nil and n ~= len then
            return nil, "conflicting content-length"
        end
        len = n
        if not comma then
            break
        end
        pos = comma + 1
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
        local colon = find(line, ":", 1, true)
        if not colon or colon == 1 then
            return nil, "malformed header line", capped
        end
        for i = 1, colon - 1 do
            if not TCHAR[byte(line, i)] then
                return nil, "malformed header line", capped
            end
        end
        local key = sub(line, 1, colon - 1)
        local vs = colon + 1
        local ve = #line
        while vs <= ve do
            local b = byte(line, vs)
            if b ~= 32 and b ~= 9 then
                break
            end
            vs = vs + 1
        end
        while ve >= vs do
            local b = byte(line, ve)
            if b ~= 32 and b ~= 9 then
                break
            end
            ve = ve - 1
        end
        local val = vs <= ve and sub(line, vs, ve) or ""
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

local function read_chunked(clock, deadline_ms, start, sock, read_timeout)
    local chunks = {}
    local n = 0
    while true do
        local line, err, capped = recv_line(clock, deadline_ms, start, sock,
                                             read_timeout)
        if not line then
            return nil, err, capped
        end
        local hex = match(line, "^(%x+)$") or match(line, "^(%x+)[ \t]*;")
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
            return concat(chunks), nil, capped
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
            local step = sock.receiveany and 65536 or 8192
            while true do
                local data, err, capped, partial = recv_some(clock, deadline_ms,
                    start, sock, read_timeout, step)
                if data then
                    n = n + 1
                    chunks[n] = data
                else
                    if partial and partial ~= "" then
                        n = n + 1
                        chunks[n] = partial
                    end
                    if err == "closed" then
                        return concat(chunks), nil, capped
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

function _M.finish_socket(self, httpc, keep, params)
    if not httpc then
        return
    end
    if not keep or httpc.keepalive == false
        or (params and params.keepalive == false) then
        close_httpc(httpc)
        return
    end
    local timeout = (params and params.keepalive_timeout) or self.keepalive_timeout
    local pool = (params and params.keepalive_pool) or self.keepalive_pool
    local ok, err = httpc:set_keepalive(timeout, pool)
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

local function parse_status_line(line)
    if sub(line, 1, 5) ~= "HTTP/" then
        return nil
    end
    local version, code_pos
    local b6, b7, b8, b9 = byte(line, 6, 9)
    if b7 == 46 and b9 == 32
        and b6 >= 48 and b6 <= 57 and b8 >= 48 and b8 <= 57 then
        if b6 == 49 and b8 == 49 then
            version = 1.1
        elseif b6 == 49 and b8 == 48 then
            version = 1.0
        else
            version = (b6 - 48) + (b8 - 48) * 0.1
        end
        code_pos = 10
    else
        local maj, min_v, rest_pos = match(line, "^HTTP/(%d+)%.(%d+) ()")
        if not maj then
            return nil
        end
        version = tonumber(maj .. "." .. min_v)
        if not version then
            return nil
        end
        code_pos = rest_pos
    end

    local c1, c2, c3, c4 = byte(line, code_pos, code_pos + 3)
    if not c1 or not c2 or not c3
        or c1 < 49 or c1 > 57
        or c2 < 48 or c2 > 57
        or c3 < 48 or c3 > 57
        or (c4 ~= nil and c4 ~= 32 and c4 ~= 9) then
        return nil
    end
    local status = (c1 - 48) * 100 + (c2 - 48) * 10 + (c3 - 48)
    local reason = c4 and sub(line, code_pos + 4) or ""
    return version, status, reason
end

-- One HTTP exchange. The body is read on this light thread so a later
-- ngx.thread.kill finalizes the cosocket. Caller pools or closes `httpc`.
function _M.attempt(self, uri, params, deadline_ms, slot, request_start, call_ctx)
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

    local parsed = call_ctx and call_ctx.parsed
    if not parsed then
        local perr
        parsed, perr = httpc:parse_uri(uri, false)
        if not parsed then
            close_httpc(httpc)
            return nil, perr, nil, nil, nil, "error"
        end
        if call_ctx then
            call_ctx.parsed = parsed
        end
    end
    local scheme, host, port = parsed[1], parsed[2], parsed[3]
    local path = params.path or parsed[4]
    local query = params.query or parsed[5]
    local method = (call_ctx and call_ctx.method)
        or (params.method and upper(params.method) or "GET")

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
        ssl_client_cert = params.ssl_client_cert,
        ssl_client_priv_key = params.ssl_client_priv_key,
        ssl_session_reuse = params.ssl_session_reuse,
        pool = params.pool,
        pool_size = params.pool_size,
        backlog = params.backlog,
        proxy_opts = params.proxy_opts,
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

    local req_headers = copy_map(params.headers)
    local expect_continue
    if call_ctx ~= nil then
        expect_continue = call_ctx.expect_continue
    elseif req_headers then
        local expect = _M.header(req_headers, "Expect")
        expect_continue = expect ~= nil
            and match(lower(expect), "^[ \t]*(.-)[ \t]*$") == "100-continue"
    end
    local body_pending = false
    if expect_continue and req_headers then
        for k in pairs(req_headers) do
            if type(k) == "string" and lower(k) == "expect" then
                req_headers[k] = "100-continue"
            end
        end
        body_pending = params.body ~= nil
    end
    local req = {
        method = method,
        path = path,
        query = query,
        headers = req_headers,
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
        version, status, reason = parse_status_line(line)
        if not version then
            close_httpc(httpc)
            return nil, "bad status line", nil, nil, nil, "transport"
        end
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
        if status >= 200 or status == 101 then
            break
        end
        interim = interim + 1
        if interim > 16 then
            close_httpc(httpc)
            return nil, "too many interim responses", nil, nil, nil, "transport"
        end
    end
    apply_keepalive(httpc, version, headers)
    if status == 101 or body_pending then
        httpc.keepalive = false
    end

    local body, berr, bcapped = read_body(self.clock, deadline_ms, origin, sock,
        self.read_timeout, headers, method, status, httpc)
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

local function attempt_with_deadline(self, uri, params, deadline_ms, start, call_ctx)
    if deadline_ms == nil then
        return self:attempt(uri, params, nil, nil, start, call_ctx)
    end
    local remain = deadline.remaining_ms(deadline_ms, start, self.clock())
    if remain < 1 - 1e-3 then
        return nil, "deadline", nil, nil, nil, "deadline"
    end

    local slot = {}
    local attempt = ngx.thread.spawn(function()
        local res, err, httpc, sample_ms, sample_kind, class =
            self:attempt(uri, params, deadline_ms, slot, start, call_ctx)
        slot.done = true
        slot.res = res
        slot.err = err
        slot.out_httpc = httpc
        slot.sample_ms = sample_ms
        slot.sample_kind = sample_kind
        slot.class = class
        return false
    end)

    local ok = true
    local err_msg
    local timed_out = false
    if not slot.done then
        remain = deadline.remaining_ms(deadline_ms, start, self.clock())
        if remain >= 1 - 1e-3 then
            local sleep_s = (remain < 1 and 1 or remain) / 1000
            local timer = ngx.thread.spawn(function()
                ngx.sleep(sleep_s)
                return true
            end)
            local wok, wres = ngx.thread.wait(attempt, timer)
            ngx.thread.kill(timer)
            if not wok then
                ok = false
                err_msg = wres
            elseif wres == true then
                timed_out = true
            end
        else
            timed_out = true
        end
    end

    if not ok then
        close_httpc(slot.httpc)
        return nil, err_msg, nil, nil, nil, "error"
    end

    remain = deadline.remaining_ms(deadline_ms, start, self.clock())
    if timed_out or not slot.done or remain <= 0 then
        if not slot.done then
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
        close_httpc(slot.out_httpc or slot.httpc)
        local kind = slot.sample_kind
        if kind == "response" then
            kind = "timeout"
        end
        return nil, "deadline", nil, slot.sample_ms, kind, "deadline"
    end
    return slot.res, slot.err, slot.out_httpc, slot.sample_ms,
           slot.sample_kind, slot.class
end

function _M.sequential(self, uri, params, meta, already, last_res, last_err,
                       last_class, deadline_ms, start, call_ctx)
    local max_attempts = self.max_attempts
    if meta.retry_skipped then
        max_attempts = 1
    end
    local res, err, class = last_res, last_err, last_class
    local prev_winner = meta.winner
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
            if remain and remain < 1 - 1e-3 then
                meta.deadline_exceeded = true
                break
            end
        end

        local prev_res, prev_err = res, err
        attempt = attempt + 1
        meta.attempts = attempt
        local httpc, sample_ms, sample_kind
        res, err, httpc, sample_ms, sample_kind, class =
            attempt_with_deadline(self, uri, params, deadline_ms, start, call_ctx)
        self:record_sample(sample_kind, sample_ms)
        if class == "ok" then
            self:finish_socket(httpc, true, params)
        end
        if class == "deadline" then
            meta.deadline_exceeded = true
            if attempt > 1 and (prev_res ~= nil or prev_err ~= nil) then
                res, err = prev_res, prev_err
                meta.winner = prev_winner
            else
                meta.winner = attempt
            end
            break
        end
        meta.winner = attempt
        prev_winner = attempt
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
    local raw_method = params.method
    local method = raw_method and upper(raw_method) or "GET"

    local reason = self:skip_reason(method, params)
    meta.retry_skipped = reason

    local deadline_ms = call_opts.deadline_ms
    if deadline_ms == nil then
        deadline_ms = self.deadline_ms
    elseif type(deadline_ms) ~= "number"
        or not (deadline_ms >= 0 and deadline_ms < huge) then
        return nil, "retry-budget: deadline_ms must be >= 0", meta
    end

    local expect = params.headers and _M.header(params.headers, "Expect")
    local call_ctx = {
        method = method,
        parsed = nil,
        expect_continue = expect ~= nil
            and match(lower(expect), "^[ \t]*(.-)[ \t]*$") == "100-continue",
    }

    local start = self.clock()
    self.budget:record_request()

    local hedge_opt = call_opts.hedge
    if hedge_opt and hedge_opt.enabled then
        if reason then
            meta.hedge_skipped = reason
        elseif self.max_attempts < 2 then
            meta.hedge_skipped = "max_attempts"
        else
            local hedge = hedge_mod
            if not hedge then
                hedge = require "resty.retry_budget.hedge"
                hedge_mod = hedge
            end
            return hedge.run(self, uri, params, hedge_opt, meta, deadline_ms,
                             start, call_ctx)
        end
    elseif hedge_opt then
        meta.hedge_skipped = "disabled"
    end

    return self:sequential(uri, params, meta, 0, nil, nil, nil, deadline_ms,
                           start, call_ctx)
end

return _M
