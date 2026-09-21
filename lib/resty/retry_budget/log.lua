-- Rate-limited warnings. One line per code per key per second.
-- Dict add is the cluster-wide limiter. If the dict cannot record the
-- limiter key, a worker-local timestamp is the fallback.

local _M = {}

local worker_next = {}

function _M.warn(dict, name, code, msg, now)
    local key = "rb:" .. name .. ":log:" .. code
    local ok, err
    if dict then
        ok, err = dict:add(key, "1", 1)
    else
        err = "no dict"
    end

    if ok then
        -- logged below
    elseif err == "exists" then
        return
    else
        local slot_key = name .. "\0" .. code
        local until_t = worker_next[slot_key]
        if until_t and now < until_t then
            return
        end
        worker_next[slot_key] = now + 1
    end

    local n = ngx
    if n and n.log and n.WARN then
        n.log(n.WARN, "[retry-budget] key=", name, " ", msg)
    end
end

function _M._reset()
    for k in pairs(worker_next) do
        worker_next[k] = nil
    end
end

return _M
