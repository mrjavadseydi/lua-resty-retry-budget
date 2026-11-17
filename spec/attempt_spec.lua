package.path = "lib/?.lua;spec/?.lua;" .. package.path

local budget = require "resty.retry_budget.budget"
local histogram = require "resty.retry_budget.histogram"
local client = require "resty.retry_budget.client"
local fake = require "fake_dict"

local function clock_of(box)
    return function()
        return box.now
    end
end

local function minimal_http()
    package.loaded["resty.http"] = {
        new = function()
            return { sock = {} }
        end,
    }
    package.loaded["resty.http_headers"] = {
        new = function()
            return {}
        end,
    }
end

-- Scripted cosocket. Each entry is one attempt: status/header lines, then
-- body chunks returned by receiveany.
local function scripted_http(exchanges, on_keepalive)
    local connect_timeouts = {}
    local connect_opts = {}
    local keepalive_args = {}
    local request_headers = {}
    local keepalive = 0
    local closed = 0
    local attempt_n = 0
    local sent = {}
    package.loaded["resty.http_headers"] = {
        new = function()
            return {}
        end,
    }
    package.loaded["resty.http"] = {
        new = function()
            attempt_n = attempt_n + 1
            local ex = exchanges[attempt_n]
            local li, ci = 1, 1
            local timeouts = {}
            local sock = {}
            function sock:getreusedtimes()
                return ex.reused or 0
            end
            function sock:settimeout(ms)
                timeouts[#timeouts + 1] = ms
            end
            function sock:send(data)
                if ex.send_error then
                    return nil, ex.send_error
                end
                sent[#sent + 1] = data
                return type(data) == "table" and #table.concat(data) or #data
            end
            function sock:receive(pat)
                if pat == "*l" then
                    local line = ex.lines[li]
                    li = li + 1
                    if line == nil then
                        if ex.on_line_error then ex.on_line_error() end
                        return nil, ex.line_error or "closed"
                    end
                    return line
                end
                local data = ex.chunks[ci]
                ci = ci + 1
                if not data then
                    return nil, ex.body_error or "closed"
                end
                return data
            end
            function sock:receiveany()
                local data = ex.chunks[ci]
                ci = ci + 1
                if ex.on_body then
                    ex.on_body()
                end
                if not data then
                    return nil, ex.body_error or "closed"
                end
                return data
            end
            local httpc = { sock = sock, keepalive = true }
            function httpc:parse_uri()
                if ex.bad_uri then
                    return nil, "bad uri"
                end
                return { "http", "127.0.0.1", 80, "/", nil }
            end
            function httpc:connect(opts)
                connect_timeouts[#connect_timeouts + 1] = timeouts[#timeouts]
                connect_opts[#connect_opts + 1] = opts
                if ex.on_connect then ex.on_connect() end
                if ex.connect_error then return nil, ex.connect_error end
                return true
            end
            function httpc:send_request(req)
                request_headers[#request_headers + 1] = req and req.headers
                if ex.on_request then ex.on_request() end
                if ex.request_error then return nil, ex.request_error end
                return true
            end
            function httpc:set_keepalive(timeout, pool)
                if self.keepalive == false then
                    return nil, "connection must be closed"
                end
                if on_keepalive then
                    on_keepalive(li, ex.lines)
                end
                keepalive = keepalive + 1
                keepalive_args[#keepalive_args + 1] = { timeout, pool }
                return true
            end
            function httpc:close()
                closed = closed + 1
            end
            return httpc
        end,
    }
    return {
        connect_timeouts = connect_timeouts,
        connect_opts = connect_opts,
        keepalive_args = keepalive_args,
        request_headers = request_headers,
        sent = sent,
        keepalive = function()
            return keepalive
        end,
        closed = function()
            return closed
        end,
    }
end

describe("client attempts", function()
    local box

    before_each(function()
        budget._reset()
        histogram._reset()
        box = { now = 1000 }
        _G.ngx = {
            ERR = 3,
            WARN = 4,
            log = function() end,
            get_phase = function()
                return "content"
            end,
            sleep = function() end,
            thread = {
                spawn = function(fn)
                    return { result = fn() }
                end,
                wait = function(thread)
                    return true, thread.result
                end,
                kill = function() end,
            },
            now = function()
                return box.now
            end,
        }
    end)

    after_each(function()
        minimal_http()
        _G.ngx = nil
    end)

    it("keeps the request deadline across retries", function()
        local script = scripted_http({
            {
                lines = {
                    "HTTP/1.1 503 Service Unavailable",
                    "Content-Length: 2",
                    "Connection: close",
                    "",
                },
                chunks = { "no" },
                on_body = function()
                    box.now = box.now + 0.5
                end,
            },
            {
                lines = {
                    "HTTP/1.1 200 OK",
                    "Content-Length: 2",
                    "Connection: close",
                    "",
                },
                chunks = { "ok" },
            },
        })
        local c = client.new("dead", {
            dict = "retry_budget",
            dict_obj = fake.new(clock_of(box)),
            clock = clock_of(box),
            random = function()
                return 0
            end,
            max_attempts = 2,
            deadline_ms = 800,
            connect_timeout = 2000,
            send_timeout = 5000,
            read_timeout = 5000,
            backoff = { base_ms = 1, max_ms = 1 },
        })
        local res, err, meta = c:request_uri("http://127.0.0.1/")
        assert.is_nil(err)
        assert.equal(200, res.status)
        assert.equal("ok", res.body)
        assert.equal(2, meta.attempts)
        assert.is_false(meta.deadline_exceeded)
        -- 500 ms already spent, so the second connect is capped near 300 ms
        -- rather than the fresh 800 ms deadline.
        local second = script.connect_timeouts[2]
        assert.is_true(second > 200 and second < 400)
    end)

    it("keeps repeated headers and retries a closed pooled connection", function()
        scripted_http({
            { lines = {}, chunks = {}, reused = 1 },
            {
                lines = {
                    "HTTP/1.1 200 OK",
                    "Set-Cookie: a=1",
                    "Set-Cookie: b=2",
                    "Content-Length: 2",
                    "",
                },
                chunks = { "ok" },
            },
        })
        local c = client.new("reset", {
            dict = "retry_budget",
            dict_obj = fake.new(clock_of(box)),
            clock = clock_of(box),
            random = function()
                return 0
            end,
            idempotent_methods = { "GET" },
        })
        local res, err, meta = c:request_uri("http://127.0.0.1/")
        assert.is_nil(err)
        assert.equal(2, meta.attempts)
        assert.same({ "a=1", "b=2" }, res.headers["Set-Cookie"])
    end)

    it("closes a response that completes after the deadline instead of pooling it", function()
        local script = scripted_http({
            {
                lines = { "HTTP/1.1 200 OK", "Content-Length: 2", "" },
                chunks = { "ok" },
                on_body = function() box.now = box.now + 0.6 end,
            },
        })
        local c = client.new("late", {
            dict_obj = fake.new(clock_of(box)), clock = clock_of(box),
            deadline_ms = 500,
        })
        local res, err, meta = c:request_uri("http://127.0.0.1/")
        assert.is_nil(res)
        assert.equal("deadline", err)
        assert.is_true(meta.deadline_exceeded)
        assert.equal(1, meta.attempts)
        assert.equal(0, script.keepalive())
        assert.is_true(script.closed() >= 1)
        assert.equal(0, c.budget:stats().retries)
    end)

    it("sends a deferred body once after 100, including after early hints", function()
        local script = scripted_http({
            {
                lines = {
                    "HTTP/1.1 103 Early Hints", "",
                    "HTTP/1.1 100 Continue", "",
                    "HTTP/1.1 100 Continue", "",
                    "HTTP/1.1 200 OK", "Content-Length: 2", "",
                },
                chunks = { "ok" },
            },
        })
        local c = client.new("continue", {
            dict_obj = fake.new(clock_of(box)), clock = clock_of(box),
        })
        local body = { "he", "llo" }
        local res, err = c:request_uri("http://127.0.0.1/", {
            method = "POST", body = body,
            headers = { ["expect"] = "100-continue" },
        })
        assert.is_nil(err)
        assert.equal(200, res.status)
        assert.same({ body }, script.sent)
    end)

    it("does not consume a deferred stream when the upstream rejects it", function()
        local script = scripted_http({
            {
                lines = { "HTTP/1.1 417 Expectation Failed", "Content-Length: 0", "" },
                chunks = {},
            },
        })
        local c = client.new("rejected", {
            dict_obj = fake.new(clock_of(box)), clock = clock_of(box),
        })
        local consumed = false
        local res, err = c:request_uri("http://127.0.0.1/", {
            method = "POST", headers = { Expect = "100-continue" },
            body = function() consumed = true; return nil end,
        })
        assert.is_nil(err)
        assert.equal(417, res.status)
        assert.is_false(consumed)
        assert.same({}, script.sent)
    end)

    it("does not retry a timeout while sending a deferred body", function()
        scripted_http({
            { lines = { "HTTP/1.1 100 Continue", "" }, send_error = "timeout" },
        })
        local c = client.new("continue_timeout", {
            dict_obj = fake.new(clock_of(box)), clock = clock_of(box),
        })
        local res, err, meta = c:request_uri("http://127.0.0.1/", {
            body = "hello", headers = { Expect = "100-continue" },
        })
        assert.is_nil(res)
        assert.equal("timeout", err)
        assert.equal(1, meta.attempts)
    end)

    it("does not retry a fresh connection closed before the status line", function()
        scripted_http({
            { lines = {}, chunks = {} },
            { lines = { "HTTP/1.1 200 OK", "Content-Length: 0", "" }, chunks = {} },
        })
        local c = client.new("fresh", {
            dict = "retry_budget",
            dict_obj = fake.new(clock_of(box)),
            clock = clock_of(box),
        })
        local res, err, meta = c:request_uri("http://127.0.0.1/")
        assert.is_nil(res)
        assert.equal("closed", err)
        assert.equal(1, meta.attempts)
    end)

    it("does not retry a URI that fails to parse", function()
        scripted_http({
            { bad_uri = true },
            { lines = { "HTTP/1.1 200 OK", "Content-Length: 0", "" }, chunks = {} },
        })
        local c = client.new("baduri", {
            dict = "retry_budget",
            dict_obj = fake.new(clock_of(box)),
            clock = clock_of(box),
        })
        local res, err, meta = c:request_uri("htp://bad uri")
        assert.is_nil(res)
        assert.equal("bad uri", err)
        assert.equal(1, meta.attempts)
    end)

    it("accepts repeated Content-Length values that are numerically equal", function()
        scripted_http({
            {
                lines = {
                    "HTTP/1.1 200 OK",
                    "Content-Length: 2",
                    "Content-Length: 2 ",
                    "",
                },
                chunks = { "ok" },
            },
        })
        local c = client.new("dupcl", {
            dict = "retry_budget",
            dict_obj = fake.new(clock_of(box)),
            clock = clock_of(box),
            max_attempts = 1,
        })
        local res, err = c:request_uri("http://127.0.0.1/")
        assert.is_nil(err)
        assert.equal("ok", res.body)
    end)

    it("rejects invalid and conflicting lengths without retrying or pooling", function()
        local cases = {
            { "-1" }, { "+2" }, { "0x2" }, { "2.0" }, { "2e0" },
            { "" }, { "bad" }, { "2, bad" }, { "2," }, { "2", "3" },
            { "bad", "wrong" }, { "9007199254740992", "9007199254740993" },
            { "\v2" },
        }
        for i, lengths in ipairs(cases) do
            local lines = { "HTTP/1.1 200 OK" }
            for _, value in ipairs(lengths) do
                lines[#lines + 1] = "Content-Length: " .. value
            end
            lines[#lines + 1] = ""
            local script = scripted_http({ { lines = lines, chunks = { "ok" }, reused = 1 } })
            local c = client.new("invalid_length" .. i, {
                dict_obj = fake.new(clock_of(box)), clock = clock_of(box),
            })
            local res, err, meta = c:request_uri("http://127.0.0.1/")
            assert.is_nil(res)
            assert.is_truthy(err:find("content-length", 1, true))
            assert.equal(1, meta.attempts)
            assert.equal(0, script.keepalive())
            assert.is_true(script.closed() >= 1)
        end
    end)

    it("accepts equal comma-separated lengths and leading zeros", function()
        scripted_http({ {
            lines = { "HTTP/1.1 200 OK", "Content-Length: 0002, 2", "Content-Length: 02 ", "" },
            chunks = { "ok" },
        } })
        local c = client.new("length_list", {
            dict_obj = fake.new(clock_of(box)), clock = clock_of(box),
        })
        local res, err = c:request_uri("http://127.0.0.1/")
        assert.is_nil(err)
        assert.equal("ok", res.body)
    end)

    it("rejects ambiguous or invalid transfer framing", function()
        local cases = {
            { "Transfer-Encoding: chunked", "Content-Length: 2" },
            { "Transfer-Encoding: gzip", "Content-Length: 0" },
            { "Transfer-Encoding: chunked," },
            { "Transfer-Encoding: chunked, chunked" },
            { "Transfer-Encoding: " },
        }
        for i, fields in ipairs(cases) do
            local lines = { "HTTP/1.1 200 OK" }
            for _, field in ipairs(fields) do lines[#lines + 1] = field end
            lines[#lines + 1] = ""
            local script = scripted_http({ { lines = lines, chunks = { "ok" }, reused = 1 } })
            local c = client.new("transfer" .. i, {
                dict_obj = fake.new(clock_of(box)), clock = clock_of(box),
            })
            local res, err, meta = c:request_uri("http://127.0.0.1/")
            assert.is_nil(res)
            assert.is_truthy(err:find("transfer-encoding", 1, true))
            assert.equal(1, meta.attempts)
            assert.equal(0, script.keepalive())
        end
    end)

    it("uses the exact final transfer coding to frame the body", function()
        for i, coding in ipairs({ "notchunked", "chunked, gzip" }) do
            local script = scripted_http({ {
                lines = { "HTTP/1.1 200 OK", "Transfer-Encoding: " .. coding, "" },
                chunks = { "wire" },
            } })
            local c = client.new("close_framing" .. i, {
                dict_obj = fake.new(clock_of(box)), clock = clock_of(box),
            })
            local res, err = c:request_uri("http://127.0.0.1/")
            assert.is_nil(err)
            assert.equal("wire", res.body)
            assert.equal(0, script.keepalive())
        end
    end)

    it("rejects malformed and incomplete chunks without pooling", function()
        local cases = {
            { lines = { "2", "0", "" }, chunks = { "ok", "XY" }, err = "invalid chunk delimiter" },
            { lines = { "2junk" }, chunks = {}, err = "invalid chunk size" },
            { lines = { "20000000000000" }, chunks = {}, err = "invalid chunk size" },
            { lines = { "0" }, chunks = {}, err = "closed" },
        }
        for i, ex in ipairs(cases) do
            local lines = { "HTTP/1.1 200 OK", "Transfer-Encoding: chunked", "" }
            for _, line in ipairs(ex.lines) do lines[#lines + 1] = line end
            local script = scripted_http({ { lines = lines, chunks = ex.chunks } })
            local c = client.new("bad_chunk" .. i, {
                dict_obj = fake.new(clock_of(box)), clock = clock_of(box),
            })
            local res, err, meta = c:request_uri("http://127.0.0.1/")
            assert.is_nil(res)
            assert.equal(ex.err, err)
            assert.equal(1, meta.attempts)
            assert.equal(0, script.keepalive())
            assert.is_true(script.closed() >= 1)
        end
    end)

    it("reads chunk extensions, trailers, and repeated transfer headers", function()
        local script = scripted_http({ {
            lines = {
                "HTTP/1.1 200 OK", "Transfer-Encoding: gzip", "Transfer-Encoding: CHUNKED", "",
                '2;tag="a,b"', "000 ; done", "X-Trace: yes", "",
            },
            chunks = { "ok", "\r\n" },
        } })
        local c = client.new("chunk_extensions", {
            dict_obj = fake.new(clock_of(box)), clock = clock_of(box),
        })
        local res, err = c:request_uri("http://127.0.0.1/")
        assert.is_nil(err)
        assert.equal("ok", res.body)
        assert.equal(1, script.keepalive())
    end)

    it("records deadline-capped connect, send, and read timeouts", function()
        local advance = function() box.now = box.now + 0.1 end
        local cases = {
            { connect_error = "timeout", on_connect = advance },
            { request_error = "timeout", on_request = advance },
            { lines = {}, line_error = "timeout", on_line_error = advance },
            { lines = { "HTTP/1.1 200 OK" }, line_error = "timeout", on_line_error = advance },
            {
                lines = { "HTTP/1.1 200 OK", "Content-Length: 2", "" }, chunks = {},
                body_error = "timeout", on_body = advance,
            },
        }
        for i, ex in ipairs(cases) do
            scripted_http({ ex })
            local c = client.new("capped_timeout" .. i, {
                dict_obj = fake.new(clock_of(box)), clock = clock_of(box),
                estimator = { min_samples = 1 },
            })
            local res, err, _, ms, kind, class = c:attempt("http://127.0.0.1/",
                { method = "GET" }, 100, nil, box.now)
            assert.is_nil(res)
            assert.equal("timeout", err)
            assert.equal("deadline", class)
            assert.equal("timeout", kind)
            c:record_sample(kind, ms)
            assert.equal(1, c.estimator:samples())
            local p95 = c.estimator:percentile(95)
            assert.is_true(p95 > 90 and p95 < 110)
        end
    end)

    it("records cancellation once and excludes attempts that never start I/O", function()
        scripted_http({ {
            lines = { "HTTP/1.1 200 OK", "Content-Length: 2", "" }, chunks = { "ok" },
            on_body = function() box.now = box.now + 0.6 end,
        } })
        local c = client.new("cancel_sample", {
            dict_obj = fake.new(clock_of(box)), clock = clock_of(box),
            deadline_ms = 500, estimator = { min_samples = 1 },
        })
        local res, _, meta = c:request_uri("http://127.0.0.1/")
        assert.is_nil(res)
        assert.is_true(meta.deadline_exceeded)
        assert.equal(1, c.estimator:samples())

        scripted_http({ {} })
        local idle = client.new("not_started", {
            dict_obj = fake.new(clock_of(box)), clock = clock_of(box),
            deadline_ms = 0, estimator = { min_samples = 1 },
        })
        res, _, meta = idle:request_uri("http://127.0.0.1/")
        assert.is_nil(res)
        assert.is_true(meta.deadline_exceeded)
        assert.equal(0, idle.estimator:samples())
    end)

    it("reads the final response that follows 100 and 103", function()
        local consumed
        scripted_http({
            {
                lines = {
                    "HTTP/1.1 100 Continue",
                    "",
                    "HTTP/1.1 103 Early Hints",
                    "Link: </style.css>; rel=preload",
                    "",
                    "HTTP/1.1 200 OK",
                    "Content-Length: 2",
                    "Connection: keep-alive",
                    "",
                },
                chunks = { "ok" },
            },
        }, function(li, lines)
            consumed = li > #lines
        end)
        local c = client.new("hints", {
            dict = "retry_budget",
            dict_obj = fake.new(clock_of(box)),
            clock = clock_of(box),
            max_attempts = 1,
        })
        local res, err = c:request_uri("http://127.0.0.1/")
        assert.is_nil(err)
        assert.equal(200, res.status)
        assert.equal("ok", res.body)
        assert.is_true(consumed)
    end)

    it("does not pool a 101", function()
        local script = scripted_http({
            {
                lines = {
                    "HTTP/1.1 101 Switching Protocols",
                    "Upgrade: websocket",
                    "Connection: upgrade",
                    "",
                },
                chunks = {},
            },
        })
        local c = client.new("switch", {
            dict = "retry_budget",
            dict_obj = fake.new(clock_of(box)),
            clock = clock_of(box),
            max_attempts = 1,
        })
        local res, err = c:request_uri("http://127.0.0.1/")
        assert.is_nil(err)
        assert.equal(101, res.status)
        assert.equal(0, script.keepalive())
        assert.is_true(script.closed() >= 1)
    end)

    it("normalizes Expect: 100-Continue and closes socket on early rejection", function()
        local script = scripted_http({
            {
                lines = {
                    "HTTP/1.1 100 Continue", "",
                    "HTTP/1.1 200 OK", "Content-Length: 2", "",
                },
                chunks = { "ok" },
            },
            {
                lines = {
                    "HTTP/1.1 417 Expectation Failed",
                    "Content-Length: 0",
                    "",
                },
                chunks = {},
            },
        })
        local c = client.new("continue_case", {
            dict_obj = fake.new(clock_of(box)),
            clock = clock_of(box),
        })
        local res, err = c:request_uri("http://127.0.0.1/", {
            method = "POST",
            body = "hello",
            headers = { Expect = "100-Continue" },
        })
        assert.is_nil(err)
        assert.equal(200, res.status)
        assert.equal("100-continue", script.request_headers[1].Expect)
        assert.same({ "hello" }, script.sent)
        assert.equal(1, script.keepalive())

        res, err = c:request_uri("http://127.0.0.1/", {
            method = "POST",
            body = "hello",
            headers = { Expect = "100-Continue" },
        })
        assert.is_nil(err)
        assert.equal(417, res.status)
        -- Still 1: the rejected Expect connection with unsent body was closed, not pooled.
        assert.equal(1, script.keepalive())
        assert.is_true(script.closed() >= 1)
    end)

    it("rejects malformed status lines and header lines and trims header OWS", function()
        local bad_lines = {
            { "JUNK/1.1 200 OK", "Content-Length: 0", "" },
            { "HTTP/1.1 2000 OK", "Content-Length: 0", "" },
            { "HTTP/1.1 0x1 OK", "Content-Length: 0", "" },
            { "HTTP/1.1 099 OK", "Content-Length: 0", "" },
            { "HTTP/1.1 200 OK", "Content-Length : 0", "" },
            { "HTTP/1.1 200 OK", "MalformedHeaderLine", "" },
        }
        for i, lines in ipairs(bad_lines) do
            local script = scripted_http({ { lines = lines, chunks = {} } })
            local c = client.new("bad_proto" .. i, {
                dict_obj = fake.new(clock_of(box)),
                clock = clock_of(box),
            })
            local res, err, meta = c:request_uri("http://127.0.0.1/")
            assert.is_nil(res)
            assert.is_truthy(err)
            assert.equal(1, meta.attempts)
            assert.equal(0, script.keepalive())
        end

        scripted_http({
            {
                lines = {
                    "HTTP/1.1 429",
                    "Retry-After: 1   ",
                    "Content-Length: 0",
                    "",
                },
                chunks = {},
            },
            {
                lines = { "HTTP/1.1 200", "Content-Length: 2", "" },
                chunks = { "ok" },
            },
        })
        local c = client.new("ows_ra", {
            dict_obj = fake.new(clock_of(box)),
            clock = clock_of(box),
        })
        local res, err, meta = c:request_uri("http://127.0.0.1/")
        assert.is_nil(err)
        assert.equal(200, res.status)
        assert.equal(2, meta.attempts)
    end)

    it("forwards connect TLS/pool options and per-call keepalive settings", function()
        local script = scripted_http({
            {
                lines = { "HTTP/1.1 200 OK", "Content-Length: 2", "" },
                chunks = { "ok" },
            },
            {
                lines = { "HTTP/1.1 200 OK", "Content-Length: 2", "" },
                chunks = { "ok" },
            },
        })
        local c = client.new("conn_opts", {
            dict_obj = fake.new(clock_of(box)),
            clock = clock_of(box),
            keepalive_timeout = 10000,
            keepalive_pool = 16,
        })
        local res, err = c:request_uri("https://127.0.0.1/", {
            ssl_client_cert = "cert",
            ssl_client_priv_key = "key",
            ssl_session_reuse = false,
            backlog = 32,
            proxy_opts = { http_proxy = "http://proxy:8080" },
            keepalive_timeout = 5000,
            keepalive_pool = 8,
        })
        assert.is_nil(err)
        assert.equal(200, res.status)
        assert.equal("cert", script.connect_opts[1].ssl_client_cert)
        assert.equal("key", script.connect_opts[1].ssl_client_priv_key)
        assert.equal(false, script.connect_opts[1].ssl_session_reuse)
        assert.equal(32, script.connect_opts[1].backlog)
        assert.same({ http_proxy = "http://proxy:8080" }, script.connect_opts[1].proxy_opts)
        assert.same({ 5000, 8 }, script.keepalive_args[1])

        res, err = c:request_uri("http://127.0.0.1/", { keepalive = false })
        assert.is_nil(err)
        assert.equal(200, res.status)
        assert.equal(1, script.keepalive())
    end)

    it("keeps sub-ms pre-deadline responses, preserves 503 on retry deadline, and skips fast connect errors", function()
        scripted_http({
            {
                lines = { "HTTP/1.1 200 OK", "Content-Length: 2", "" },
                chunks = { "ok" },
                on_body = function() box.now = box.now + 0.4995 end,
            },
        })
        local c = client.new("subms_dead", {
            dict_obj = fake.new(clock_of(box)),
            clock = clock_of(box),
            deadline_ms = 500,
        })
        local res, err, meta = c:request_uri("http://127.0.0.1/")
        assert.is_nil(err)
        assert.equal(200, res.status)
        assert.is_false(meta.deadline_exceeded)

        scripted_http({
            {
                lines = { "HTTP/1.1 503 Service Unavailable", "Content-Length: 2", "" },
                chunks = { "no" },
                on_body = function() box.now = box.now + 0.35 end,
            },
            {
                lines = { "HTTP/1.1 200 OK", "Content-Length: 2", "" },
                chunks = { "ok" },
                on_body = function() box.now = box.now + 0.35 end,
            },
        })
        local c2 = client.new("preserve_503", {
            dict_obj = fake.new(clock_of(box)),
            clock = clock_of(box),
            random = function() return 0 end,
            max_attempts = 2,
            deadline_ms = 500,
            backoff = { base_ms = 0, max_ms = 0 },
        })
        res, err, meta = c2:request_uri("http://127.0.0.1/")
        assert.is_nil(err)
        assert.equal(503, res.status)
        assert.is_true(meta.deadline_exceeded)
        assert.equal(2, meta.attempts)
        assert.equal(1, meta.winner)

        scripted_http({
            {
                connect_error = "connection refused",
                on_connect = function() box.now = box.now + 0.1 end,
            },
        })
        local c3 = client.new("refused_at_dead", {
            dict_obj = fake.new(clock_of(box)),
            clock = clock_of(box),
            max_attempts = 1,
            deadline_ms = 100,
            estimator = { min_samples = 1 },
        })
        res, err, meta = c3:request_uri("http://127.0.0.1/")
        assert.is_nil(res)
        assert.is_truthy(err)
        assert.is_true(meta.deadline_exceeded)
        assert.equal(0, c3.estimator:samples())
    end)
end)
