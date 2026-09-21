package.path = "lib/?.lua;spec/?.lua;" .. package.path

local deadline = require "resty.retry_budget.deadline"

describe("deadline", function()
    it("caps each operation by the wall-clock remainder", function()
        assert.equal(1000, deadline.next_timeout_ms(nil, 1000))
        assert.equal(100, deadline.next_timeout_ms(500, 100))
        assert.equal(500, deadline.next_timeout_ms(500, 1000))
        assert.is_nil(deadline.next_timeout_ms(0.5, 1000))
        assert.is_nil(deadline.next_timeout_ms(0, 1000))
        assert.near(250, deadline.remaining_ms(1000, 10, 10.75), 1e-6)
    end)
end)
