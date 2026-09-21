-- Full jitter: delay is uniform in [0, min(max_ms, base_ms * 2^(attempt-1))].
-- `attempt` is how many attempts have already failed (1 after the first failure).

local _M = {}

function _M.delay_ms(attempt, base_ms, max_ms, random)
    if attempt < 1 then
        attempt = 1
    end
    local cap = base_ms * (2 ^ (attempt - 1))
    if cap ~= cap or cap > max_ms then
        cap = max_ms
    end
    if cap < 0 then
        cap = 0
    end
    local r = random()
    if r < 0 then
        r = 0
    elseif r > 1 then
        r = 1
    end
    return r * cap
end

-- Delta-seconds, or an HTTP-date via ngx.parse_http_time.
-- Returns milliseconds from `now`, or nil when the header is unusable.
function _M.retry_after_ms(header, now)
    if type(header) ~= "string" or header == "" then
        return nil
    end
    if header:match("^%d+$") then
        return tonumber(header) * 1000
    end
    local n = ngx
    if not n or not n.parse_http_time then
        return nil
    end
    local epoch = n.parse_http_time(header)
    if not epoch then
        return nil
    end
    local delta = (epoch - now) * 1000
    if delta < 0 then
        delta = 0
    end
    return delta
end

return _M
