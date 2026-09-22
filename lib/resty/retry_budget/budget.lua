local log = require "resty.retry_budget.log"

local _M = {}

local NAMES_KEY = "rb:\0names"
local LOCK_KEY = "rb:\0lock"

-- Parallel arrays. Closed-window fields live on the per-object cache.
local KIND_N = 6
local CLOSED = { "closed_req", "closed_ret", "closed_rej",
                 "closed_hs", "closed_hw", "closed_err" }
local PREV = { "prev_req", "prev_ret", "prev_rej",
               "prev_hs", "prev_hw", "prev_err" }
local CUR = { "q_key", "t_key", "j_key", "hs_key", "hw_key", "e_key" }
local PREV_KEY = { "prev_q_key", "prev_t_key", "prev_j_key",
                   "prev_hs_key", "prev_hw_key", "prev_e_key" }

local zones = {}
local seen = {}
local objects = {}

local function default_clock()
    return ngx.now()
end

local function clear_map(t)
    for k in pairs(t) do
        t[k] = nil
    end
end

function _M._reset()
    clear_map(zones)
    clear_map(seen)
    clear_map(objects)
    log._reset()
end

local function check_name(name)
    if type(name) ~= "string" or #name < 1 or #name > 64 then
        return nil, "retry-budget: name must be 1..64 bytes"
    end
    if not name:match("^[%w%._%%-]+$") then
        return nil, "retry-budget: name has illegal characters"
    end
    return true
end

local function has_name(blob, name)
    if not blob or blob == "" then
        return false
    end
    local padded = "\n" .. blob
    if padded:sub(-1) ~= "\n" then
        padded = padded .. "\n"
    end
    return padded:find("\n" .. name .. "\n", 1, true) ~= nil
end

function _M.register_name(dict, zone, name)
    local bucket = seen[zone]
    if not bucket then
        bucket = {}
        seen[zone] = bucket
    end
    bucket[name] = true
    zones[zone] = dict

    local marker = "rb:" .. name .. ":n"
    local ok, err = dict:add(marker, "1")
    if not ok and err ~= "exists" then
        return nil, err
    end

    -- The marker is written before the lock. Its presence does not mean
    -- the index was updated; a busy lock leaves both behind for the next
    -- registration to finish.
    local cur, gerr = dict:get(NAMES_KEY)
    if cur == nil and type(gerr) == "string" then
        return nil, gerr
    end
    if has_name(cur or "", name) then
        return true
    end

    local locked = dict:add(LOCK_KEY, "1", 1)
    if not locked then
        return true
    end
    cur, gerr = dict:get(NAMES_KEY)
    if cur == nil and type(gerr) == "string" then
        dict:delete(LOCK_KEY)
        return nil, gerr
    end
    cur = cur or ""
    if not has_name(cur, name) then
        local _, serr = dict:set(NAMES_KEY, cur .. name .. "\n")
        if serr then
            dict:delete(LOCK_KEY)
            return nil, serr
        end
    end
    dict:delete(LOCK_KEY)
    return true
end

local function read_num(dict, key)
    local v, extra = dict:get(key)
    if v == nil and type(extra) == "string" then
        return nil, extra
    end
    if v == nil then
        return 0
    end
    return v
end

local function new_cache()
    return {
        sec = nil,
        refreshed_at = -math.huge,
        closed_req = 0, closed_ret = 0, closed_rej = 0,
        closed_hs = 0, closed_hw = 0, closed_err = 0,
        prev_req = 0, prev_ret = 0, prev_rej = 0,
        prev_hs = 0, prev_hw = 0, prev_err = 0,
    }
end

local function note(self, code, msg)
    local c = self._cache
    local now = self.clock()
    if c and c.e_key then
        local _, _, forcible = self.dict:incr(c.e_key, 1, 0, self.ttl)
        if forcible then
            code = "dict_full"
        end
    end
    log.warn(self.dict, self.name, code, msg, now)
end

local function full_sum(self, sec, now)
    local c = self._cache
    local from = sec - self.window_sec + 1
    local to = sec - 1
    local totals = { 0, 0, 0, 0, 0, 0 }
    local prevs = { 0, 0, 0, 0, 0, 0 }
    local dict = self.dict
    local p = self._p
    for s = from, to do
        for i = 1, KIND_N do
            local v, err = read_num(dict, p[i] .. s)
            if err then
                return err
            end
            totals[i] = totals[i] + v
            if s == to then
                prevs[i] = v
            end
        end
    end
    for i = 1, KIND_N do
        c[CLOSED[i]] = totals[i]
        c[PREV[i]] = prevs[i]
        c[CUR[i]] = p[i] .. sec
        if self.window_sec > 1 then
            c[PREV_KEY[i]] = p[i] .. to
        end
    end
    c.sec = sec
    c.refreshed_at = now
    return nil
end

local function refresh_prev(self, now)
    if self.window_sec < 2 then
        self._cache.refreshed_at = now
        return nil
    end
    local c = self._cache
    local dict = self.dict
    for i = 1, KIND_N do
        local v, err = read_num(dict, c[PREV_KEY[i]])
        if err then
            return err
        end
        c[CLOSED[i]] = c[CLOSED[i]] - c[PREV[i]] + v
        c[PREV[i]] = v
    end
    c.refreshed_at = now
    return nil
end

local function cur_total(dict, cache, i)
    local v, err = read_num(dict, cache[CUR[i]])
    if err then
        return nil, err
    end
    return cache[CLOSED[i]] + v
end

-- Six window totals, then an error string.
local function sums(self)
    local now = self.clock()
    local sec = math.floor(now)
    local c = self._cache
    if c.sec ~= sec then
        local err = full_sum(self, sec, now)
        if err then
            return nil, nil, nil, nil, nil, nil, err
        end
    elseif now - c.refreshed_at >= self.refresh_interval then
        local err = refresh_prev(self, now)
        if err then
            return nil, nil, nil, nil, nil, nil, err
        end
    end

    local dict = self.dict
    local a, ea = cur_total(dict, c, 1)
    if ea then
        return nil, nil, nil, nil, nil, nil, ea
    end
    local b, eb = cur_total(dict, c, 2)
    if eb then
        return nil, nil, nil, nil, nil, nil, eb
    end
    local c1, ec = cur_total(dict, c, 3)
    if ec then
        return nil, nil, nil, nil, nil, nil, ec
    end
    local d, ed = cur_total(dict, c, 4)
    if ed then
        return nil, nil, nil, nil, nil, nil, ed
    end
    local e, ee = cur_total(dict, c, 5)
    if ee then
        return nil, nil, nil, nil, nil, nil, ee
    end
    local f, ef = cur_total(dict, c, 6)
    if ef then
        return nil, nil, nil, nil, nil, nil, ef
    end
    return a, b, c1, d, e, f, nil
end

local function allowance_of(self, requests)
    local floor_n = self.min_retries_per_sec * self.window_sec
    local ratio_n = self.retry_ratio * requests
    if ratio_n > floor_n then
        return ratio_n
    end
    return floor_n
end

local function on_fail(self, code, msg)
    note(self, code, msg)
    return self.fail_mode == "open"
end

local mt = { __index = _M }

function _M.new(name, opts)
    local ok, err = check_name(name)
    if not ok then
        return nil, err
    end
    opts = opts or {}

    local window = opts.window_sec or 10
    if type(window) ~= "number" or window ~= math.floor(window)
        or window < 1 or window > 300 then
        return nil, "retry-budget: window_sec must be an integer in 1..300"
    end

    local ratio = opts.retry_ratio
    if ratio == nil then
        ratio = 0.2
    end
    if type(ratio) ~= "number" or ratio < 0 or ratio > 10 then
        return nil, "retry-budget: retry_ratio must be in [0, 10]"
    end

    local min_r = opts.min_retries_per_sec
    if min_r == nil then
        min_r = 10
    end
    if type(min_r) ~= "number" or min_r < 0 then
        return nil, "retry-budget: min_retries_per_sec must be >= 0"
    end

    local refresh = opts.refresh_interval
    if refresh == nil then
        refresh = 0.1
    end
    if type(refresh) ~= "number" or refresh < 0 then
        return nil, "retry-budget: refresh_interval must be >= 0"
    end

    local fail_mode = opts.fail_mode or "open"
    if fail_mode ~= "open" and fail_mode ~= "closed" then
        return nil, "retry-budget: fail_mode must be \"open\" or \"closed\""
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

    local prefix = "rb:" .. name .. ":"
    local self = setmetatable({
        name = name,
        zone = zone,
        dict = dict,
        clock = opts.clock or default_clock,
        window_sec = window,
        retry_ratio = ratio,
        min_retries_per_sec = min_r,
        refresh_interval = refresh,
        fail_mode = fail_mode,
        ttl = window + 1,
        _p = {
            prefix .. "q:",
            prefix .. "t:",
            prefix .. "j:",
            prefix .. "hs:",
            prefix .. "hw:",
            prefix .. "e:",
        },
        _cache = new_cache(),
        cfg_key = prefix .. "cfg",
    }, mt)

    local by_zone = objects[zone]
    if not by_zone then
        by_zone = {}
        objects[zone] = by_zone
    end
    by_zone[name] = self
    zones[zone] = dict

    local rok, rerr = _M.register_name(dict, zone, name)
    if not rok then
        note(self, "dict_op", "name registration failed: " .. (rerr or ""))
    end

    dict:set(self.cfg_key, window .. "," .. ratio .. "," .. min_r
        .. "," .. refresh .. "," .. fail_mode)

    return self
end

function _M.record_request(self)
    local _, _, _, _, _, _, err = sums(self)
    if err then
        note(self, "dict_op", "shared dict read failed: " .. err)
        return false
    end
    local newval, ierr, forcible = self.dict:incr(self._cache.q_key, 1, 0, self.ttl)
    if not newval then
        note(self, "dict_op", "shared dict incr failed: " .. (ierr or ""))
        return false
    end
    if forcible then
        note(self, "dict_full", "shared dict evicted live entries")
    end
    return true
end

function _M.can_acquire(self)
    local requests, retries, _, _, _, _, err = sums(self)
    if err then
        return on_fail(self, "dict_op", "shared dict read failed: " .. err)
    end
    return retries < allowance_of(self, requests)
end

function _M.try_acquire(self)
    local requests, retries, _, _, _, _, err = sums(self)
    if err then
        return on_fail(self, "dict_op", "shared dict read failed: " .. err)
    end
    local allowance = allowance_of(self, requests)
    if retries >= allowance then
        local _, ierr, forcible = self.dict:incr(self._cache.j_key, 1, 0, self.ttl)
        if not ierr and forcible then
            note(self, "dict_full", "shared dict evicted live entries")
        elseif ierr then
            note(self, "dict_op", "shared dict incr failed: " .. ierr)
        end
        return false
    end

    local newval, ierr, forcible = self.dict:incr(self._cache.t_key, 1, 0, self.ttl)
    if not newval then
        return on_fail(self, "dict_op", "shared dict incr failed: " .. (ierr or ""))
    end
    if forcible then
        note(self, "dict_full", "shared dict evicted live entries")
        if self.fail_mode == "closed" then
            return false
        end
    end
    return true
end

function _M.record_retry(self)
    local _, _, _, _, _, _, err = sums(self)
    if err then
        note(self, "dict_op", "shared dict read failed: " .. err)
        return false
    end
    local newval, ierr, forcible = self.dict:incr(self._cache.t_key, 1, 0, self.ttl)
    if not newval then
        note(self, "dict_op", "shared dict incr failed: " .. (ierr or ""))
        return false
    end
    if forcible then
        note(self, "dict_full", "shared dict evicted live entries")
    end
    return true
end

local function incr_kind(self, idx)
    local _, _, _, _, _, _, err = sums(self)
    if err then
        note(self, "dict_op", "shared dict read failed: " .. err)
        return false
    end
    local newval, ierr, forcible = self.dict:incr(self._cache[CUR[idx]], 1, 0, self.ttl)
    if not newval then
        note(self, "dict_op", "shared dict incr failed: " .. (ierr or ""))
        return false
    end
    if forcible then
        note(self, "dict_full", "shared dict evicted live entries")
    end
    return true
end

function _M.record_hedge_sent(self)
    return incr_kind(self, 4)
end

function _M.record_hedge_won(self)
    return incr_kind(self, 5)
end

local function latency_for(zone, name, dict, clock)
    local ok, histogram = pcall(require, "resty.retry_budget.histogram")
    if not ok then
        return { p50 = nil, p95 = nil, p99 = nil }
    end
    return histogram.percentiles_for(dict, zone, name, clock)
end

function _M.stats(self)
    local requests, retries, rejected, hs, hw, dict_errors, err = sums(self)
    if err then
        requests, retries, rejected = 0, 0, 0
        hs, hw, dict_errors = 0, 0, 0
        note(self, "dict_op", "shared dict read failed: " .. err)
    end
    return {
        requests = requests,
        retries = retries,
        allowed_ratio = self.retry_ratio,
        allowance = allowance_of(self, requests),
        rejected = rejected,
        hedges_sent = hs,
        hedges_won = hw,
        dict_errors = dict_errors,
        dict = self.zone,
        latency_ms = latency_for(self.zone, self.name, self.dict, self.clock),
    }
end

local function parse_cfg(raw)
    if type(raw) ~= "string" then
        return nil
    end
    local w, ratio, min_r, refresh, mode = raw:match(
        "^(%d+%.?%d*),([^,]+),([^,]+),([^,]+),(%a+)$")
    w = tonumber(w)
    ratio = tonumber(ratio)
    min_r = tonumber(min_r)
    refresh = tonumber(refresh)
    if not w or not ratio or not min_r or not refresh then
        return nil
    end
    if mode ~= "open" and mode ~= "closed" then
        return nil
    end
    return w, ratio, min_r, refresh, mode
end

local function stats_from_cfg(dict, zone, name, clock)
    local prefix = "rb:" .. name .. ":"
    local cfg = dict:get(prefix .. "cfg")
    local window, ratio, min_r, refresh, mode = parse_cfg(cfg)
    if not window then
        window, ratio, min_r, refresh, mode = 10, 0.2, 10, 0.1, "open"
    end
    local obj = _M.new(name, {
        dict = zone,
        dict_obj = dict,
        clock = clock,
        window_sec = window,
        retry_ratio = ratio,
        min_retries_per_sec = min_r,
        refresh_interval = refresh,
        fail_mode = mode,
    })
    if not obj then
        return nil
    end
    return obj:stats()
end

function _M.stats_all()
    local out = {}
    for zone, dict in pairs(zones) do
        local names = {}
        local blob = dict:get(NAMES_KEY) or ""
        for name in blob:gmatch("[^\n]+") do
            names[name] = true
        end
        local local_names = seen[zone]
        if local_names then
            for name in pairs(local_names) do
                names[name] = true
            end
        end
        local bucket = {}
        local by_zone = objects[zone] or {}
        for name in pairs(names) do
            local obj = by_zone[name]
            local st
            if obj then
                st = obj:stats()
            else
                st = stats_from_cfg(dict, zone, name, default_clock)
            end
            if st then
                bucket[name] = st
            end
        end
        out[zone] = bucket
    end
    return out
end

return _M
