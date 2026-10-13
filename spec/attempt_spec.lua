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
                        return nil, "closed"
                    end
                    return line
                end
                local data = ex.chunks[ci]
                ci = ci + 1
                if not data then
                    return nil, "closed"
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
                    return nil, "closed"
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
            function httpc:connect()
                connect_timeouts[#connect_timeouts + 1] = timeouts[#timeouts]
                return true
            end
            function httpc:send_request()
                return true
            end
            function httpc:set_keepalive()
                if self.keepalive == false then
                    return nil, "connection must be closed"
                end
                if on_keepalive then
                    on_keepalive(li, ex.lines)
                end
                keepalive = keepalive + 1
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
end)
