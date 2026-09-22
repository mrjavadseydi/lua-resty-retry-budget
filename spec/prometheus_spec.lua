package.path = "lib/?.lua;spec/?.lua;" .. package.path

local budget = require "resty.retry_budget.budget"
local histogram = require "resty.retry_budget.histogram"
local prom = require "resty.retry_budget.prometheus"
local fake = require "fake_dict"

local function clock_of(box)
    return function()
        return box.now
    end
end

local function new_prom()
    local metrics = {}
    local inst = {
        gauge = function(_, name)
            local metric = { samples = {} }
            function metric:set(value, label_values)
                self.samples[#self.samples + 1] = {
                    value = value,
                    labels = label_values,
                }
            end
            metrics[name] = metric
            return metric
        end,
    }
    return inst, metrics
end

describe("prometheus", function()
    local box

    before_each(function()
        budget._reset()
        histogram._reset()
        prom._reset()
        box = { now = 1000, dict = nil }
        box.dict = fake.new(clock_of(box))
        _G.ngx = {
            ERR = 3,
            WARN = 4,
            log = function() end,
        }
    end)

    after_each(function()
        package.loaded["prometheus"] = nil
        _G.ngx = nil
    end)

    it("requires an initialized prometheus instance", function()
        local ok, err = prom.register(nil)
        assert.is_nil(ok)
        assert.equal("nginx-lua-prometheus not installed", err)

        package.loaded["prometheus"] = {}
        ok, err = prom.register({})
        assert.is_nil(ok)
        assert.equal("pass an initialized prometheus instance", err)
    end)

    it("refuses to observe before register", function()
        local ok, err = prom.observe()
        assert.is_nil(ok)
        assert.equal("retry-budget: prometheus.register was not called", err)
    end)

    it("publishes window gauges", function()
        local inst, metrics = new_prom()
        assert.is_true(prom.register(inst))

        local clock = clock_of(box)
        local b = budget.new("pay", {
            dict = "retry_budget",
            dict_obj = box.dict,
            clock = clock,
            min_retries_per_sec = 0,
            retry_ratio = 0.2,
        })
        assert.is_true(b:record_request())
        assert.is_true(b:try_acquire())

        local est = histogram.new("pay", {
            dict = "retry_budget",
            dict_obj = box.dict,
            clock = clock,
            min_samples = 100,
        })
        for _ = 1, 100 do
            assert.is_true(est:record(8))
        end

        assert.is_true(prom.observe())

        local function sample(name, quantile)
            local metric = metrics[name]
            assert.is_table(metric)
            for i = 1, #metric.samples do
                local row = metric.samples[i]
                if quantile == nil or row.labels[3] == quantile then
                    return row
                end
            end
            return nil
        end

        local requests = sample("retry_budget_requests_window")
        assert.equal("pay", requests.labels[1])
        assert.equal("retry_budget", requests.labels[2])
        assert.equal(1, requests.value)
        assert.equal(1, sample("retry_budget_retries_window").value)
        assert.equal(0, sample("retry_budget_retries_rejected_window").value)

        local p50 = sample("retry_budget_latency_ms", "0.5")
        assert.equal("pay", p50.labels[1])
        assert.equal("retry_budget", p50.labels[2])
        assert.is_true(p50.value >= 8 and p50.value < 9)
    end)

    it("sets expired percentiles to NaN", function()
        local inst, metrics = new_prom()
        assert.is_true(prom.register(inst))
        local clock = clock_of(box)
        budget.new("pay", {
            dict = "retry_budget",
            dict_obj = box.dict,
            clock = clock,
        })
        local est = histogram.new("pay", {
            dict = "retry_budget",
            dict_obj = box.dict,
            clock = clock,
            window_sec = 10,
            slot_sec = 10,
            min_samples = 1,
        })
        assert.is_true(est:record(8))
        assert.is_true(prom.observe())

        local function last_p50()
            local metric = metrics.retry_budget_latency_ms
            local found
            for i = 1, #metric.samples do
                local row = metric.samples[i]
                if row.labels[3] == "0.5" and row.labels[2] == "retry_budget" then
                    found = row
                end
            end
            return found
        end

        local first = last_p50()
        assert.is_true(first.value == first.value)
        box.now = box.now + 30
        assert.is_true(prom.observe())
        local second = last_p50()
        assert.is_true(second.value ~= second.value)
    end)
end)
