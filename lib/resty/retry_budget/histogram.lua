local log = require "resty.retry_budget.log"

local _M = {}

local LN = math.log(2) / 8
local MIN_MS = 1
local MAX_MS = 60000
local RATIO = 2 ^ (1 / 8)
local N_FINITE = math.ceil(math.log(MAX_MS / MIN_MS) / LN)
local N = N_FINITE + 1

_M.BUCKETS = N
_M.MIN_MS = MIN_MS
_M.MAX_MS = MAX_MS
_M.RATIO = RATIO

-- Samples under 1 ms share bucket 1, whose lower edge is 1 ms, so a
-- percentile never comes back below 1 ms.
function _M.bucket_of(ms)
    if ms < MIN_MS then
        return 1
    end
    local x = math.log(ms / MIN_MS) / LN
    local k = math.floor(x + 1e-10) + 1
    if k < 1 then
        return 1
    end
    if k > N then
        return N
    end
    return k
end

local function lower_of(k)
    return MIN_MS * (RATIO ^ (k - 1))
end

local registry = {}

local function reg_key(zone, name)
    return zone .. "\0" .. name
end

local function default_clock()
    return ngx.now()
end

function _M._reset()
    for k in pairs(registry) do
        registry[k] = nil
    end
end

local function state_for(zone, name)
    local key = reg_key(zone, name)
    local st = registry[key]
    if not st then
        st = {
            zone = zone,
            name = name,
            keys = {},
            slot = nil,
            counts = {},
            total = 0,
            refreshed_at = -math.huge,
            timer = false,
            timer_failed = false,
            have_counts = false,
        }
        registry[key] = st
    end
    return st
end

local function ensure_keys(st, slot)
    if st.slot == slot and st.keys[1] then
        return
    end
    local base = st.prefix .. slot .. ":"
    local keys = st.keys
    for i = 1, N do
        keys[i] = base .. i
    end
    st.slot = slot
end

local function refresh(st, now)
    if st.have_counts and (now - st.refreshed_at) < 1 then
        return true
    end
    local slot = math.floor(now / st.slot_sec)
    local nslots = math.floor(st.window_sec / st.slot_sec)
    local counts = st.counts
    for i = 1, N do
        counts[i] = 0
    end
    local total = 0
    local from = slot - nslots + 1
    local dict = st.dict
    for s = from, slot do
        local base = st.prefix .. s .. ":"
        for i = 1, N do
            local v = dict:get(base .. i) or 0
            counts[i] = counts[i] + v
            total = total + v
        end
    end
    st.total = total
    st.refreshed_at = now
    st.have_counts = true
    return true
end

local function ensure_timer(st)
    if st.timer or st.timer_failed then
        return
    end
    local n = ngx
    if not n or not n.timer or not n.timer.at then
        return
    end
    local ok, err = n.timer.at(1, function(premature)
        if premature then
            st.timer = false
            return
        end
        local now = st.clock()
        refresh(st, now)
        st.timer = false
        ensure_timer(st)
    end)
    if ok then
        st.timer = true
    else
        st.timer_failed = true
        log.warn(st.dict, st.name, "timer",
                 "histogram refresh timer failed: " .. (err or ""), st.clock())
    end
end

local function quantile(st, p)
    if st.total == 0 or st.total < st.min_samples then
        return nil, "not enough samples"
    end
    local target = math.ceil(p / 100 * st.total)
    local cum = 0
    for k = 1, N do
        local c = st.counts[k]
        if c > 0 and cum + c >= target then
            local frac = (target - cum - 0.5) / c
            if frac < 0 then
                frac = 0
            elseif frac > 1 then
                frac = 1
            end
            local lower = lower_of(k)
            return lower * (RATIO ^ frac)
        end
        cum = cum + c
    end
    return MAX_MS
end

local mt = { __index = _M }

function _M.new(name, opts)
    if type(name) ~= "string" or #name < 1 or #name > 64
        or not name:match("^[%w%._%%-]+$") then
        return nil, "retry-budget: name must be 1..64 bytes of [A-Za-z0-9._%-]"
    end
    opts = opts or {}

    local window = opts.window_sec or 60
    local slot = opts.slot_sec or 10
    if type(window) ~= "number" or window ~= math.floor(window)
        or window < 1 or window > 3600 then
        return nil, "retry-budget: histogram window_sec must be an integer in 1..3600"
    end
    if type(slot) ~= "number" or slot ~= math.floor(slot) or slot < 1
        or window % slot ~= 0 then
        return nil, "retry-budget: slot_sec must divide window_sec"
    end

    local min_samples = opts.min_samples
    if min_samples == nil then
        min_samples = 100
    end
    if type(min_samples) ~= "number" or min_samples < 0
        or min_samples ~= math.floor(min_samples) then
        return nil, "retry-budget: min_samples must be an integer >= 0"
    end

    local dict = opts.dict_obj
    local zone = opts.dict
    if dict == nil then
        if type(zone) ~= "string" or zone == "" then
            return nil, "retry-budget: dict name required"
        end
        local core_ok, core_err = pcall(require, "resty.core.shdict")
        if not core_ok then
            return nil, "retry-budget: resty.core.shdict required: " .. core_err
        end
        local shared = ngx.shared
        dict = shared and shared[zone]
        if not dict then
            return nil, "retry-budget: shared dict \"" .. zone .. "\" not found"
        end
    else
        if type(zone) ~= "string" or zone == "" then
            zone = "test"
        end
    end

    local st = state_for(zone, name)
    local window_changed = st.window_sec ~= window or st.slot_sec ~= slot
        or st.dict ~= dict
    st.dict = dict
    st.clock = opts.clock or default_clock
    st.window_sec = window
    st.slot_sec = slot
    st.min_samples = min_samples
    st.ttl = window + slot
    st.prefix = "rb:" .. name .. ":L:"
    if window_changed then
        st.have_counts = false
        st.refreshed_at = -math.huge
        st.slot = nil
    end

    local budget = require "resty.retry_budget.budget"
    budget.register_name(dict, zone, name)
    dict:set("rb:" .. name .. ":hcfg", window .. "," .. slot .. "," .. min_samples)

    return setmetatable({ _st = st, name = name, zone = zone }, mt)
end

function _M.record(self, ms)
    if type(ms) ~= "number" or ms ~= ms then
        return nil, "retry-budget: latency must be a number"
    end
    local st = self._st
    local now = st.clock()
    local slot = math.floor(now / st.slot_sec)
    ensure_keys(st, slot)
    local b = _M.bucket_of(ms)
    local newval, err, forcible = st.dict:incr(st.keys[b], 1, 0, st.ttl)
    if not newval then
        log.warn(st.dict, st.name, "dict_op",
                 "histogram incr failed: " .. (err or ""), now)
        return nil, err
    end
    if forcible then
        log.warn(st.dict, st.name, "dict_full",
                 "shared dict evicted live entries", now)
    end
    ensure_timer(st)
    return true
end

function _M.samples(self)
    local st = self._st
    refresh(st, st.clock())
    ensure_timer(st)
    return st.total
end

function _M.percentile(self, p)
    if type(p) ~= "number" or p < 1 or p > 100 then
        return nil, "retry-budget: percentile out of range"
    end
    local st = self._st
    refresh(st, st.clock())
    ensure_timer(st)
    return quantile(st, p)
end

local function parse_hcfg(raw)
    if type(raw) ~= "string" then
        return nil
    end
    local w, s, m = raw:match("^(%d+),(%d+),(%d+)$")
    w, s, m = tonumber(w), tonumber(s), tonumber(m)
    if not w or not s or not m or s < 1 or w % s ~= 0 then
        return nil
    end
    return w, s, m
end

function _M.percentiles_for(dict, zone, name, clock)
    local empty = { p50 = nil, p95 = nil, p99 = nil }
    local st = registry[reg_key(zone, name)]
    if not st then
        local w, s, m = parse_hcfg(dict:get("rb:" .. name .. ":hcfg"))
        if not w then
            return empty
        end
        local est, err = _M.new(name, {
            dict = zone,
            dict_obj = dict,
            clock = clock,
            window_sec = w,
            slot_sec = s,
            min_samples = m,
        })
        if not est then
            return empty
        end
        st = est._st
    elseif clock then
        st.clock = clock
    end
    refresh(st, st.clock())
    return {
        p50 = quantile(st, 50),
        p95 = quantile(st, 95),
        p99 = quantile(st, 99),
    }
end

return _M
