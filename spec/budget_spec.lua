package.path = "lib/?.lua;spec/?.lua;" .. package.path

local budget = require "resty.retry_budget.budget"
local fake = require "fake_dict"

local logs

local function clock_of(box)
    return function()
        return box.now
    end
end

local function new_budget(name, box, opts)
    opts = opts or {}
    opts.dict = "retry_budget"
    opts.dict_obj = box.dict
    opts.clock = clock_of(box)
    return budget.new(name, opts)
end

describe("budget", function()
    before_each(function()
        budget._reset()
        logs = {}
        _G.ngx = {
            WARN = 4,
            log = function(_, ...)
                local n = select("#", ...)
                local parts = {}
                for i = 1, n do
                    parts[i] = tostring(select(i, ...))
                end
                logs[#logs + 1] = table.concat(parts)
            end,
        }
    end)

    after_each(function()
        _G.ngx = nil
    end)

    local function box(now)
        local b = { now = now or 0 }
        b.dict = fake.new(clock_of(b))
        return b
    end

    it("drops a bucket once it leaves the window", function()
        local env = box(0.2)
        local b = new_budget("payments", env)
        assert.is_true(b:record_request())
        env.now = 9.9
        assert.equal(1, b:stats().requests)
        env.now = 10
        assert.equal(0, b:stats().requests)
    end)

    it("uses the floor until the ratio is larger", function()
        local env = box(1)
        local b = new_budget("payments", env, {
            min_retries_per_sec = 10,
            retry_ratio = 0.2,
            window_sec = 10,
        })
        for _ = 1, 100 do
            assert.is_true(b:try_acquire())
        end
        assert.is_false(b:try_acquire())
        local st = b:stats()
        assert.equal(100, st.retries)
        assert.equal(1, st.rejected)
        assert.equal(0, st.requests)
        assert.equal(0.2, st.allowed_ratio)
        assert.equal(100, st.allowance)

        env.dict:set("rb:payments:q:1", 10000)
        -- cache still has the old closed sum; current second is live
        st = b:stats()
        assert.equal(10000, st.requests)
        assert.equal(2000, st.allowance)
    end)

    it("treats a fractional allowance as a strict less-than", function()
        local env = box(3)
        local b = new_budget("frac", env, {
            min_retries_per_sec = 0,
            retry_ratio = 0.2,
        })
        assert.is_true(b:record_request())
        assert.is_true(b:record_request())
        assert.is_true(b:record_request())
        assert.is_true(b:try_acquire())
        assert.is_false(b:try_acquire())
        local st = b:stats()
        assert.equal(1, st.retries)
        assert.equal(1, st.rejected)
    end)

    it("recovers after the window slides", function()
        local env = box(0.4)
        local b = new_budget("payments", env, {
            min_retries_per_sec = 1,
            retry_ratio = 0,
            window_sec = 10,
        })
        for _ = 1, 10 do
            assert.is_true(b:try_acquire())
        end
        assert.is_false(b:try_acquire())
        env.now = 10.4
        assert.is_true(b:try_acquire())
        assert.equal(1, b:stats().retries)
    end)

    it("fails open or closed without counting a lost incr", function()
        local env = box(2)
        local open_b = new_budget("openb", env, { fail_mode = "open" })
        env.dict._set_error("no memory")
        assert.is_false(open_b:record_request())
        assert.is_true(open_b:try_acquire())
        env.dict._set_error(nil)
        assert.equal(0, open_b:stats().retries)
        assert.equal(1, #logs)
        assert.is_truthy(logs[1]:find("[retry-budget] key=openb ", 1, true))

        logs = {}
        assert.is_true(open_b:try_acquire())
        env.dict._set_error("no memory")
        assert.is_true(open_b:try_acquire())
        assert.equal(0, #logs)

        env.now = 3.2
        assert.is_true(open_b:try_acquire())
        assert.equal(1, #logs)

        env.dict._set_error(nil)
        local closed_b = new_budget("closedb", env, { fail_mode = "closed" })
        env.dict._set_error("no memory")
        assert.is_false(closed_b:try_acquire())
    end)

    it("follows fail_mode when incr evicts live entries", function()
        local env = box(4)
        local open_b = new_budget("evictopen", env, { fail_mode = "open" })
        env.dict._set_forcible(true)
        assert.is_true(open_b:try_acquire())
        assert.equal(1, open_b:stats().retries)

        local closed_b = new_budget("evictclosed", env, { fail_mode = "closed" })
        env.dict._set_forcible(true)
        assert.is_false(closed_b:try_acquire())
        assert.equal(0, closed_b:stats().retries)
        assert.equal(1, closed_b:stats().rejected)
    end)

    it("refreshes only the previous second inside the current second", function()
        local env = box(10.2)
        local slow = new_budget("payments", env, {
            refresh_interval = 0.05,
            min_retries_per_sec = 0,
            retry_ratio = 1,
        })
        local fast = new_budget("other", env, {
            refresh_interval = 0,
            min_retries_per_sec = 0,
            retry_ratio = 1,
        })
        assert.equal(0, slow:stats().requests)
        env.dict:incr("rb:payments:q:9", 5, 0, 20)
        env.now = 10.22
        assert.equal(0, slow:stats().requests)
        env.now = 10.30
        assert.equal(5, slow:stats().requests)

        assert.is_true(fast:record_request())
        assert.equal(1, slow:stats().requests == 5 and fast:stats().requests)
        -- current second of `payments` is visible without waiting
        env.dict:incr("rb:payments:q:10", 1, 0, 20)
        assert.equal(6, slow:stats().requests)

        env.dict:incr("rb:payments:q:8", 7, 0, 20)
        env.now = 10.40
        assert.equal(6, slow:stats().requests)
        env.now = 11
        assert.equal(13, slow:stats().requests)
    end)

    it("counts hedges apart from the retry withdraw", function()
        local env = box(6)
        local b = new_budget("payments", env, { min_retries_per_sec = 0 })
        assert.is_true(b:record_hedge_sent())
        assert.is_true(b:record_hedge_won())
        local st = b:stats()
        assert.equal(1, st.hedges_sent)
        assert.equal(1, st.hedges_won)
        assert.equal(0, st.retries)
    end)

    it("lists every budget built in this worker", function()
        local env = box(1)
        new_budget("payments", env)
        new_budget("redis", env)
        local all = budget.stats_all()
        assert.equal(0, all.retry_budget.payments.requests)
        assert.equal(0, all.retry_budget.redis.requests)
        assert.equal(0.2, all.retry_budget.payments.allowed_ratio)
        assert.equal("retry_budget", all.retry_budget.payments.dict)
    end)

    it("keeps one entry per zone when the name is shared", function()
        local env = box(10)
        local other = box(10)
        local a = new_budget("payments", env)
        local b = budget.new("payments", {
            dict = "other",
            dict_obj = other.dict,
            clock = clock_of(other),
        })
        assert.is_true(a:record_request())
        assert.is_true(b:record_request())
        assert.is_true(b:record_request())
        local all = budget.stats_all()
        assert.equal(1, all.retry_budget.payments.requests)
        assert.equal(2, all.other.payments.requests)
    end)

    it("writes the name index after a busy lock is released", function()
        local env = box(10)
        _G.ngx.now = function()
            return env.now
        end
        _G.ngx.ERR = 3
        env.dict:set("rb:\0lock", "1", 30)
        local b = new_budget("payments", env)
        assert.is_truthy(b)
        assert.is_false(budget.register_name(env.dict, "retry_budget", "payments"))
        b:record_request()
        assert.is_nil(env.dict:get("rb:\0names"))
        env.dict:delete("rb:\0lock")
        b:record_request()
        local blob = env.dict:get("rb:\0names")
        assert.is_truthy(blob and blob:find("payments", 1, true))

        budget._reset()
        _G.ngx.now = function()
            return env.now
        end
        new_budget("other", env)
        local all = budget.stats_all()
        assert.equal(2, all.retry_budget.payments.requests)
        assert.equal(0, all.retry_budget.other.requests)
    end)

    it("rejects a bad name or window", function()
        local env = box(1)
        local _, err = new_budget("pay:ments", env)
        assert.is_truthy(err:find("illegal"))
        _, err = new_budget("payments", env, { window_sec = 1.5 })
        assert.is_truthy(err:find("window_sec"))
    end)
end)
