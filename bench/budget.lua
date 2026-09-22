-- Same arming rules as the README balancer recipe.
-- Loaded once per worker; the budget object stays at file scope.

local balancer = require "ngx.balancer"
local rb = require "resty.retry_budget"

local budget = rb.new_budget("payments", {
    dict = "retry_budget",
    retry_ratio = 0.2,
    min_retries_per_sec = 10,
    window_sec = 10,
})
local MAX_ATTEMPTS = 3
local _M = {}

local function mark(key)
    ngx.shared.bench_state:incr(key, 1, 0)
end

function _M.stats()
    return budget:stats()
end

function _M.run()
    local ctx = ngx.ctx
    local n = (ctx.rb_attempt or 0) + 1
    ctx.rb_attempt = n
    if n <= 4 then
        mark("budget_attempt_" .. n)
    else
        mark("budget_attempt_other")
    end

    local may_arm = n < MAX_ATTEMPTS
    if n == 1 then
        budget:record_request()
    elseif not budget:try_acquire() then
        budget:record_retry()
        may_arm = false
        mark("budget_acquire_denied")
    end

    local ok, err = balancer.set_current_peer("127.0.0.1", 1986)
    if not ok then
        ngx.log(ngx.ERR, "[retry-budget] key=payments ", err)
        return
    end

    if may_arm and budget:can_acquire() then
        ok, err = balancer.set_more_tries(1)
        if not ok then
            ngx.log(ngx.ERR, "[retry-budget] key=payments ", err)
            return
        end
        mark("budget_armed")
    end
end

return _M
