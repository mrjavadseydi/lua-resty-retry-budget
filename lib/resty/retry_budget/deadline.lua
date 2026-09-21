-- Wall-clock remainder for one socket operation.
-- nil `remaining` means the caller set no deadline.

local _M = {}

function _M.remaining_ms(deadline_ms, start, now)
    if deadline_ms == nil then
        return nil
    end
    return deadline_ms - (now - start) * 1000
end

-- nil when the attempt must not start another operation.
function _M.next_timeout_ms(remaining, configured)
    if remaining == nil then
        return configured
    end
    if remaining < 1 then
        return nil
    end
    if configured < remaining then
        return configured
    end
    return remaining
end

return _M
