package.path = "lib/?.lua;spec/?.lua;" .. package.path

local backoff = require "resty.retry_budget.backoff"

describe("backoff", function()
    it("stays inside full-jitter bounds", function()
        assert.equal(0, backoff.delay_ms(1, 25, 1000, function()
            return 0
        end))
        local almost = backoff.delay_ms(1, 25, 1000, function()
            return 0.999999
        end)
        assert.is_true(almost > 24 and almost < 25)

        local third = backoff.delay_ms(3, 25, 1000, function()
            return 1
        end)
        assert.equal(100, third)

        assert.equal(1000, backoff.delay_ms(8, 25, 1000, function()
            return 1
        end))
        assert.equal(1000, backoff.delay_ms(20, 25, 1000, function()
            return 1
        end))
    end)

    it("parses Retry-After delta-seconds and http dates", function()
        assert.equal(2000, backoff.retry_after_ms("2", 100))
        assert.is_nil(backoff.retry_after_ms("", 0))
        assert.is_nil(backoff.retry_after_ms("soon", 0))

        local saved = _G.ngx
        _G.ngx = {
            parse_http_time = function()
                return 100
            end,
        }
        assert.equal(5000, backoff.retry_after_ms(
            "Sun, 06 Nov 1994 08:49:37 GMT", 95))
        assert.equal(0, backoff.retry_after_ms(
            "Sun, 06 Nov 1994 08:49:37 GMT", 150))
        _G.ngx = saved
    end)
end)
