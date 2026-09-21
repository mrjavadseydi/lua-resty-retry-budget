-- Minimal ngx.shared.DICT stand-in.
-- incr(key, value, init, ttl) stores init+value when the key is missing,
-- and ttl is applied only on that creation. Matches resty.core.shdict.

local _M = {}

function _M.new(clock)
    local data = {}
    local err_mode = nil
    local forcible_next = false

    local function alive(entry, now)
        if not entry then
            return nil
        end
        if entry.exp and now >= entry.exp then
            return nil
        end
        return entry
    end

    local dict = {}

    function dict:get(key)
        local entry = alive(data[key], clock())
        if not entry then
            return nil
        end
        return entry.value
    end

    function dict:incr(key, value, init, ttl)
        if err_mode then
            return nil, err_mode
        end
        local now = clock()
        local entry = alive(data[key], now)
        local forcible = false
        if not entry then
            if init == nil then
                return nil, "not found"
            end
            local exp
            if ttl and ttl > 0 then
                exp = now + ttl
            end
            entry = { value = init + value, exp = exp }
            data[key] = entry
            if forcible_next then
                forcible = true
                forcible_next = false
            end
        else
            entry.value = entry.value + value
            if forcible_next then
                forcible = true
                forcible_next = false
            end
        end
        return entry.value, nil, forcible
    end

    function dict:add(key, value, ttl)
        if err_mode then
            return nil, err_mode
        end
        local now = clock()
        if alive(data[key], now) then
            return nil, "exists"
        end
        local exp
        if ttl and ttl > 0 then
            exp = now + ttl
        end
        data[key] = { value = value, exp = exp }
        return true
    end

    function dict:set(key, value, ttl)
        if err_mode then
            return nil, err_mode, false
        end
        local exp
        if ttl and ttl > 0 then
            exp = clock() + ttl
        end
        data[key] = { value = value, exp = exp }
        return true, nil, false
    end

    function dict:delete(key)
        data[key] = nil
    end

    function dict._set_error(mode)
        err_mode = mode
    end

    function dict._set_forcible(on)
        forcible_next = on and true or false
    end

    return dict
end

return _M
