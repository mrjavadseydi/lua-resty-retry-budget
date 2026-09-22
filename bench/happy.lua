-- Added cost of the client on a fast upstream, max_attempts = 1, no hedge.
-- ngx.now() is millisecond resolution, so each sample is the mean of K
-- calls. The reported p50 is the median of those means.

local rb = require "resty.retry_budget"
local http = require "resty.http"

local URL = "http://127.0.0.1:1986/fast"
local N = 50
local K = 200
local WARMUP = 300

local _M = {}

local function raw_once()
    local httpc = http.new()
    local res, err = httpc:request_uri(URL, {
        method = "GET",
        keepalive_timeout = 60000,
        keepalive_pool = 32,
    })
    if not res then
        return err or "raw request failed"
    end
    if res.status ~= 200 then
        return "raw status " .. tostring(res.status)
    end
end

local function burst(fn)
    local t0 = ngx.now()
    for _ = 1, K do
        local err = fn()
        if err then
            return nil, err
        end
    end
    return (ngx.now() - t0) * 1e6 / K
end

local function summarize(samples)
    table.sort(samples)
    local n = #samples
    local sum = 0
    for i = 1, n do
        sum = sum + samples[i]
    end
    local idx = math.ceil(0.5 * n)
    local p99 = math.ceil(0.99 * n)
    return samples[idx], sum / n, samples[p99]
end

function _M.run()
    local client, err = rb.new_client("happy", {
        dict = "retry_budget",
        max_attempts = 1,
        connect_timeout = 1000,
        send_timeout = 1000,
        read_timeout = 1000,
        keepalive_timeout = 60000,
        keepalive_pool = 32,
    })
    if not client then
        ngx.status = 500
        ngx.say(err)
        return
    end

    local function client_once()
        local res, cerr = client:request_uri(URL, { method = "GET" })
        if not res then
            return cerr or "client request failed"
        end
        if res.status ~= 200 then
            return "client status " .. tostring(res.status)
        end
    end

    for _ = 1, WARMUP do
        local werr = client_once()
        if werr then
            ngx.status = 500
            ngx.say("warmup client: ", werr)
            return
        end
        werr = raw_once()
        if werr then
            ngx.status = 500
            ngx.say("warmup raw: ", werr)
            return
        end
    end

    local client_samples = {}
    local raw_samples = {}
    for i = 1, N do
        local cus, cerr = burst(client_once)
        if not cus then
            ngx.status = 500
            ngx.say("client burst: ", cerr)
            return
        end
        local rus, rerr = burst(raw_once)
        if not rus then
            ngx.status = 500
            ngx.say("raw burst: ", rerr)
            return
        end
        client_samples[i] = cus
        raw_samples[i] = rus
    end

    local c50, cmean, c99 = summarize(client_samples)
    local r50, rmean, r99 = summarize(raw_samples)
    ngx.say("happy_n ", N)
    ngx.say("happy_k ", K)
    ngx.say("happy_client_p50_us ", string.format("%.3f", c50))
    ngx.say("happy_raw_p50_us ", string.format("%.3f", r50))
    ngx.say("happy_delta_p50_us ", string.format("%.3f", c50 - r50))
    ngx.say("happy_client_mean_us ", string.format("%.3f", cmean))
    ngx.say("happy_raw_mean_us ", string.format("%.3f", rmean))
    ngx.say("happy_delta_mean_us ", string.format("%.3f", cmean - rmean))
    ngx.say("happy_client_p99_us ", string.format("%.3f", c99))
    ngx.say("happy_raw_p99_us ", string.format("%.3f", r99))
end

return _M
