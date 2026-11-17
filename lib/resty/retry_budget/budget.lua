local log = require "resty.retry_budget.log"

local floor = math.floor
local huge = math.huge
local pairs = pairs
local pcall = pcall
local require = require
local setmetatable = setmetatable
local tonumber = tonumber
local type = type
local loaded = package.loaded

local _M = {}

local NAMES_KEY = "rb:\0names"
local LOCK_KEY = "rb:\0lock"

-- Parallel arrays. Closed-window fields live on the per-object cache.
-- Indices 1..2 (q, t) drive budget enforcement; 3..6 are stats-only.
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
local indexed = {}
local cfg_written = {}

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
    clear_map(indexed)
    clear_map(cfg_written)
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
    if zones[zone] and zones[zone] ~= dict then
        seen[zone] = nil
        objects[zone] = nil
        indexed[zone] = nil
        cfg_written[zone] = nil
    end
    local bucket = seen[zone]
    if not bucket then
        bucket = {}
        seen[zone] = bucket
    end
    bucket[name] = true
    zones[zone] = dict

    local idx_bucket = indexed[zone]
    if idx_bucket and idx_bucket[name] then
        return true
    end

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
        if not idx_bucket then
            idx_bucket = {}
            indexed[zone] = idx_bucket
        end
        idx_bucket[name] = true
        return true
    end

    local locked = dict:add(LOCK_KEY, "1", 1)
    if not locked then
        return false, "busy"
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
    if not idx_bucket then
        idx_bucket = {}
        indexed[zone] = idx_bucket
    end
    idx_bucket[name] = true
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
        key_sec = nil,
        sec = nil,
        refreshed_at = -huge,
        stats_sec = nil,
        stats_refreshed_at = -huge,
        closed_req = 0, closed_ret = 0, closed_rej = 0,
        closed_hs = 0, closed_hw = 0, closed_err = 0,
        prev_req = 0, prev_ret = 0, prev_rej = 0,
        prev_hs = 0, prev_hw = 0, prev_err = 0,
    }
end

local function ensure_cur_keys(self, sec)
    local c = self._cache
    if c.key_sec == sec then
        return c
    end
    local p = self._p
    c.q_key = p[1] .. sec
    c.t_key = p[2] .. sec
    c.j_key = p[3] .. sec
    c.hs_key = p[4] .. sec
    c.hw_key = p[5] .. sec
    c.e_key = p[6] .. sec
    c.key_sec = sec
    return c
end

local function note(self, code, msg)
    local now = self.clock()
    local c = ensure_cur_keys(self, floor(now))
    if c and c.e_key then
        local _, _, forcible = self.dict:incr(c.e_key, 1, 0, self.ttl)
        if forcible then
            code = "dict_full"
        end
    end
    log.warn(self.dict, self.name, code, msg, now)
end

local function full_core_sum(self, sec, now)
    local c = self._cache
    local from = sec - self.window_sec + 1
    local to = sec - 1
    local q_tot, t_tot = 0, 0
    local q_prev, t_prev = 0, 0
    local dict = self.dict
    local p1, p2 = self._p[1], self._p[2]
    for s = from, to do
        local qv, qerr = read_num(dict, p1 .. s)
        if qerr then
            return qerr
        end
        local tv, terr = read_num(dict, p2 .. s)
        if terr then
            return terr
        end
        q_tot = q_tot + qv
        t_tot = t_tot + tv
        if s == to then
            q_prev = qv
            t_prev = tv
        end
    end
    c.closed_req = q_tot
    c.closed_ret = t_tot
    c.prev_req = q_prev
    c.prev_ret = t_prev
    if self.window_sec > 1 then
        c.prev_q_key = p1 .. to
        c.prev_t_key = p2 .. to
    end
    c.sec = sec
    c.refreshed_at = now
    return nil
end

local function refresh_core_prev(self, now)
    local c = self._cache
    if self.window_sec < 2 then
        c.refreshed_at = now
        return nil
    end
    local dict = self.dict
    local qv, qerr = read_num(dict, c.prev_q_key)
    if qerr then
        return qerr
    end
    local tv, terr = read_num(dict, c.prev_t_key)
    if terr then
        return terr
    end
    c.closed_req = c.closed_req - c.prev_req + qv
    c.prev_req = qv
    c.closed_ret = c.closed_ret - c.prev_ret + tv
    c.prev_ret = tv
    c.refreshed_at = now
    return nil
end

-- Only requests (q) and retries (t) are needed to check or acquire budget.
local function core_sums(self, now, sec)
    now = now or self.clock()
    sec = sec or floor(now)
    local c = ensure_cur_keys(self, sec)
    if c.sec ~= sec then
        local err = full_core_sum(self, sec, now)
        if err then
            return nil, nil, err
        end
    elseif now - c.refreshed_at >= self.refresh_interval then
        local err = refresh_core_prev(self, now)
        if err then
            return nil, nil, err
        end
    end
    local dict = self.dict
    local qv, qerr = read_num(dict, c.q_key)
    if qerr then
        return nil, nil, qerr
    end
    local tv, terr = read_num(dict, c.t_key)
    if terr then
        return nil, nil, terr
    end
    return c.closed_req + qv, c.closed_ret + tv, nil
end

local function full_stats_sum(self, sec, now)
    local c = self._cache
    local from = sec - self.window_sec + 1
    local to = sec - 1
    local t3, t4, t5, t6 = 0, 0, 0, 0
    local p3, p4, p5, p6 = 0, 0, 0, 0
    local dict = self.dict
    local p = self._p
    for s = from, to do
        local v3, e3 = read_num(dict, p[3] .. s)
        if e3 then return e3 end
        local v4, e4 = read_num(dict, p[4] .. s)
        if e4 then return e4 end
        local v5, e5 = read_num(dict, p[5] .. s)
        if e5 then return e5 end
        local v6, e6 = read_num(dict, p[6] .. s)
        if e6 then return e6 end
        t3, t4, t5, t6 = t3 + v3, t4 + v4, t5 + v5, t6 + v6
        if s == to then
            p3, p4, p5, p6 = v3, v4, v5, v6
        end
    end
    c.closed_rej, c.closed_hs, c.closed_hw, c.closed_err = t3, t4, t5, t6
    c.prev_rej, c.prev_hs, c.prev_hw, c.prev_err = p3, p4, p5, p6
    if self.window_sec > 1 then
        for i = 3, KIND_N do
            c[PREV_KEY[i]] = p[i] .. to
        end
    end
    c.stats_sec = sec
    c.stats_refreshed_at = now
    return nil
end

local function refresh_stats_prev(self, now)
    local c = self._cache
    if self.window_sec < 2 then
        c.stats_refreshed_at = now
        return nil
    end
    local dict = self.dict
    for i = 3, KIND_N do
        local v, err = read_num(dict, c[PREV_KEY[i]])
        if err then
            return err
        end
        c[CLOSED[i]] = c[CLOSED[i]] - c[PREV[i]] + v
        c[PREV[i]] = v
    end
    c.stats_refreshed_at = now
    return nil
end

local function cur_total(dict, cache, i)
    local v, err = read_num(dict, cache[CUR[i]])
    if err then
        return nil, err
    end
    return cache[CLOSED[i]] + v
end

-- Six window totals for stats(), then an error string.
local function sums(self)
    local now = self.clock()
    local sec = floor(now)
    local a, b, cerr = core_sums(self, now, sec)
    if cerr then
        return nil, nil, nil, nil, nil, nil, cerr
    end
    local c = self._cache
    if c.stats_sec ~= sec then
        local err = full_stats_sum(self, sec, now)
        if err then
            return nil, nil, nil, nil, nil, nil, err
        end
    elseif now - c.stats_refreshed_at >= self.refresh_interval then
        local err = refresh_stats_prev(self, now)
        if err then
            return nil, nil, nil, nil, nil, nil, err
        end
    end

    local dict = self.dict
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
    if type(window) ~= "number" or window ~= floor(window)
        or window < 1 or window > 300 then
        return nil, "retry-budget: window_sec must be an integer in 1..300"
    end

    local ratio = opts.retry_ratio
    if ratio == nil then
        ratio = 0.2
    end
    if type(ratio) ~= "number" or not (ratio >= 0 and ratio <= 10) then
        return nil, "retry-budget: retry_ratio must be in [0, 10]"
    end

    local min_r = opts.min_retries_per_sec
    if min_r == nil then
        min_r = 10
    end
    if type(min_r) ~= "number" or not (min_r >= 0 and min_r < huge) then
        return nil, "retry-budget: min_retries_per_sec must be >= 0"
    end

    local refresh = opts.refresh_interval
    if refresh == nil then
        refresh = 0.1
    end
    if type(refresh) ~= "number" or not (refresh >= 0 and refresh < huge) then
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
        if not loaded["resty.core.shdict"] then
            local core_ok, core_err = pcall(require, "resty.core.shdict")
            if not core_ok then
                return nil, "retry-budget: resty.core.shdict required: " .. core_err
            end
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

    if zones[zone] and zones[zone] ~= dict then
        seen[zone] = nil
        objects[zone] = nil
        indexed[zone] = nil
        cfg_written[zone] = nil
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
        -- record_request retries until the name reaches the shared index.
        self._unindexed = true
        if rerr ~= "busy" then
            note(self, "dict_op", "name registration failed: " .. (rerr or ""))
        end
    end

    local cfg_str = window .. "," .. ratio .. "," .. min_r
        .. "," .. refresh .. "," .. fail_mode
    local cw_zone = cfg_written[zone]
    if not cw_zone then
        cw_zone = {}
        cfg_written[zone] = cw_zone
    end
    if cw_zone[name] ~= cfg_str then
        if dict:set(self.cfg_key, cfg_str) then
            cw_zone[name] = cfg_str
        end
    end

    return self
end

local function incr_kind(self, idx)
    local c = ensure_cur_keys(self, floor(self.clock()))
    local newval, ierr, forcible = self.dict:incr(c[CUR[idx]], 1, 0, self.ttl)
    if not newval then
        note(self, "dict_op", "shared dict incr failed: " .. (ierr or ""))
        return false
    end
    if forcible then
        note(self, "dict_full", "shared dict evicted live entries")
    end
    return true
end

function _M.record_request(self)
    if self._unindexed
        and _M.register_name(self.dict, self.zone, self.name) then
        self._unindexed = nil
    end
    return incr_kind(self, 1)
end

function _M.can_acquire(self)
    local requests, retries, err = core_sums(self)
    if err then
        return on_fail(self, "dict_op", "shared dict read failed: " .. err)
    end
    return retries < allowance_of(self, requests)
end

function _M.try_acquire(self)
    local requests, retries, err = core_sums(self)
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
            -- Denied, so it counts as rejected, not as a sent retry. No
            -- init: if the key was evicted meanwhile, there is nothing to
            -- undo, and init would create it at -1.
            self.dict:incr(self._cache.t_key, -1)
            self.dict:incr(self._cache.j_key, 1, 0, self.ttl)
            return false
        end
    end
    return true
end

function _M.record_retry(self)
    return incr_kind(self, 2)
end

function _M.record_hedge_sent(self)
    return incr_kind(self, 4)
end

function _M.record_hedge_won(self)
    return incr_kind(self, 5)
end

local histogram_mod
local function latency_for(zone, name, dict, clock)
    local hist = histogram_mod or loaded["resty.retry_budget.histogram"]
    if not hist then
        local ok, mod = pcall(require, "resty.retry_budget.histogram")
        if not ok then
            return { p50 = nil, p95 = nil, p99 = nil }
        end
        hist = mod
    end
    histogram_mod = hist
    return hist.percentiles_for(dict, zone, name, clock)
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

function _M.stats_all(extra_zones)
    local scan = {}
    for zone, dict in pairs(zones) do
        scan[zone] = dict
    end
    if extra_zones ~= nil then
        local n = ngx
        local shared = n and n.shared
        if shared then
            if type(extra_zones) == "string" then
                if not scan[extra_zones] and shared[extra_zones] then
                    scan[extra_zones] = shared[extra_zones]
                end
            elseif type(extra_zones) == "table" then
                for i = 1, #extra_zones do
                    local z = extra_zones[i]
                    if type(z) == "string" and not scan[z] and shared[z] then
                        scan[z] = shared[z]
                    end
                end
            end
        end
    end

    local out = {}
    for zone, dict in pairs(scan) do
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
