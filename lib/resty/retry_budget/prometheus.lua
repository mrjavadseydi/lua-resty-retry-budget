-- Optional gauges for knyar/nginx-lua-prometheus.
-- Call register() with an already constructed prometheus object, then
-- observe() in the scrape location before collect(). The numbers are
-- sliding-window gauges, not lifetime counters.

local _M = {}

local gauges

local function ensure_prom(prom)
    if type(prom) == "table" and prom.gauge then
        return prom
    end
    local ok = pcall(require, "prometheus")
    if not ok then
        return nil, "nginx-lua-prometheus not installed"
    end
    return nil, "pass an initialized prometheus instance"
end

function _M.register(prom)
    local inst, err = ensure_prom(prom)
    if not inst then
        return nil, err
    end
    gauges = {
        requests = inst:gauge("retry_budget_requests_window",
            "Original requests in the current retry-budget window",
            { "budget", "dict" }),
        retries = inst:gauge("retry_budget_retries_window",
            "Retries and hedges withdrawn in the current window",
            { "budget", "dict" }),
        rejected = inst:gauge("retry_budget_retries_rejected_window",
            "Retries refused by the budget in the current window",
            { "budget", "dict" }),
        hedges_sent = inst:gauge("retry_budget_hedges_sent_window",
            "Hedge attempts sent in the current window",
            { "budget", "dict" }),
        hedges_won = inst:gauge("retry_budget_hedges_won_window",
            "Hedge attempts that won in the current window",
            { "budget", "dict" }),
        dict_errors = inst:gauge("retry_budget_dict_errors_window",
            "Shared dict failures in the current window",
            { "budget", "dict" }),
        latency = inst:gauge("retry_budget_latency_ms",
            "Latency percentile in the current histogram window",
            { "budget", "dict", "quantile" }),
    }
    return true
end

function _M._reset()
    gauges = nil
end

function _M.observe()
    if not gauges then
        return nil, "retry-budget: prometheus.register was not called"
    end
    local rb = require "resty.retry_budget"
    local all = rb.stats()
    -- NaN clears a percentile whose samples have left the window.
    -- A number that is only omitted would keep the previous scrape.
    local missing = 0 / 0
    for zone, names in pairs(all) do
        for name, st in pairs(names) do
            local label = { name, zone }
            gauges.requests:set(st.requests or 0, label)
            gauges.retries:set(st.retries or 0, label)
            gauges.rejected:set(st.rejected or 0, label)
            gauges.hedges_sent:set(st.hedges_sent or 0, label)
            gauges.hedges_won:set(st.hedges_won or 0, label)
            gauges.dict_errors:set(st.dict_errors or 0, label)
            local lat = st.latency_ms or {}
            gauges.latency:set(lat.p50 or missing, { name, zone, "0.5" })
            gauges.latency:set(lat.p95 or missing, { name, zone, "0.95" })
            gauges.latency:set(lat.p99 or missing, { name, zone, "0.99" })
        end
    end
    return true
end

return _M
