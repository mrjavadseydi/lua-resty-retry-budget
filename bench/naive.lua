-- Always-on retries, same cap and same single placeholder as the budget
-- path. One server line starts peer.tries at 1, so three tries only happen
-- because this file arms them.

local balancer = require "ngx.balancer"

local MAX_ATTEMPTS = 3
local _M = {}

local function mark(key)
    ngx.shared.bench_state:incr(key, 1, 0)
end

function _M.run()
    local ctx = ngx.ctx
    local n = (ctx.rb_attempt or 0) + 1
    ctx.rb_attempt = n
    if n <= 4 then
        mark("naive_attempt_" .. n)
    else
        mark("naive_attempt_other")
    end

    local ok, err = balancer.set_current_peer("127.0.0.1", 1986)
    if not ok then
        ngx.log(ngx.ERR, "bench naive: ", err)
        return
    end
    if n < MAX_ATTEMPTS then
        ok, err = balancer.set_more_tries(1)
        if not ok then
            ngx.log(ngx.ERR, "bench naive: ", err)
            return
        end
        mark("naive_armed")
    end
end

return _M
