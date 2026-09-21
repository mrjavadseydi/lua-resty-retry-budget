local budget = require "resty.retry_budget.budget"
local histogram = require "resty.retry_budget.histogram"

local _M = {
    _VERSION = "0.1.0",
    new_budget = budget.new,
    new_estimator = histogram.new,
    stats = budget.stats_all,
}

function _M.new_client(name, opts)
    local client = require "resty.retry_budget.client"
    return client.new(name, opts)
end

return _M
