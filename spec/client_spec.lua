package.path = "lib/?.lua;spec/?.lua;" .. package.path

package.loaded["resty.http"] = package.loaded["resty.http"] or {
    new = function()
        return { sock = {} }
    end,
}
package.loaded["resty.http_headers"] = package.loaded["resty.http_headers"] or {
    new = function()
        return {}
    end,
}

local client = require "resty.retry_budget.client"
local budget = require "resty.retry_budget.budget"
local histogram = require "resty.retry_budget.histogram"
local fake = require "fake_dict"

describe("client policy", function()
    local c

    before_each(function()
        budget._reset()
        histogram._reset()
        local box = { now = 10 }
        local clock = function()
            return box.now
        end
        local dict = fake.new(clock)
        local err
        c, err = client.new("payments", {
            dict = "retry_budget",
            dict_obj = dict,
            clock = clock,
            random = function()
                return 0
            end,
            backoff = { base_ms = 25, max_ms = 1000 },
        })
        assert.is_nil(err)
    end)

    it("does not retry a POST or a streaming body", function()
        assert.equal("non_idempotent", c:skip_reason("POST", { body = "x" }))
        assert.is_nil(c:skip_reason("POST", {
            body = "x",
            headers = { ["idempotency-key"] = "abc" },
        }))
        assert.equal("streaming_body", c:skip_reason("GET", {
            body = function() end,
        }))
        assert.is_nil(c:skip_reason("GET", {}))
        assert.is_nil(c:header({ ["Retry-After"] = "" }, "Retry-After"))
    end)

    it("accepts idempotent_methods as a list or a set", function()
        local list = client.new("list", {
            dict = "retry_budget",
            dict_obj = fake.new(function() return 10 end),
            idempotent_methods = { "get", "PUT" },
        })
        assert.is_nil(list:skip_reason("GET", {}))
        assert.is_nil(list:skip_reason("PUT", {}))
        assert.equal("non_idempotent", list:skip_reason("POST", {}))
        local set = client.new("set", {
            dict = "retry_budget",
            dict_obj = fake.new(function() return 10 end),
            idempotent_methods = { PUT = true, GET = false },
        })
        assert.is_nil(set:skip_reason("PUT", {}))
        assert.equal("non_idempotent", set:skip_reason("GET", {}))
    end)

    it("retries connect failures and not read timeouts", function()
        assert.is_true(c:should_retry(nil, "connect_error"))
        assert.is_true(c:should_retry(nil, "connect_timeout"))
        assert.is_true(c:should_retry(nil, "reset"))
        assert.is_false(c:should_retry(nil, "transport"))
        assert.is_false(c:should_retry(nil, "read_timeout"))
        assert.is_false(c:should_retry(nil, "send_timeout"))
        assert.is_true(c:should_retry({ status = 503, headers = {} }, "ok"))
        assert.is_false(c:should_retry({ status = 500, headers = {} }, "ok"))
        assert.is_false(c:should_retry({ status = 429, headers = {} }, "ok"))
        assert.is_true(c:should_retry({
            status = 429,
            headers = { ["Retry-After"] = "1" },
        }, "ok"))
    end)

    it("caps Retry-After by max_ms", function()
        local delay = c:retry_delay(1, {
            status = 503,
            headers = { ["Retry-After"] = "30" },
        })
        assert.equal(1000, delay)
        assert.equal(0, c:retry_delay(1, { status = 502, headers = {} }))
    end)

    it("lets a pending hedge beat a 5xx", function()
        local res = { status = 503, headers = {} }
        assert.is_false(c:response_wins(res, nil, true))
        assert.is_true(c:response_wins(res, nil, false))
        assert.is_true(c:response_wins({ status = 200, headers = {} }, nil, true))
    end)
end)
