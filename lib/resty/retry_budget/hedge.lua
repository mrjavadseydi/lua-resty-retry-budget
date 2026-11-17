local deadline = require "resty.retry_budget.deadline"

local _M = {}

local function resolve_delay(client, hedge)
    local min_delay = hedge.min_delay_ms
    if min_delay == nil then
        min_delay = 5
    elseif type(min_delay) ~= "number"
        or not (min_delay >= 0 and min_delay < math.huge) then
        return nil, "bad_delay"
    end
    local spec = hedge.delay
    if spec == nil then
        spec = "p95"
    end
    local ms, err
    if type(spec) == "number" then
        if not (spec >= 0 and spec < math.huge) then
            return nil, "bad_delay"
        end
        ms = spec
    elseif type(spec) == "string" then
        local p = tonumber(spec:match("^p(%d+)$") or "")
        if not p or p < 1 or p > 100 then
            return nil, "bad_delay"
        end
        ms, err = client.estimator:percentile(p, hedge.min_samples)
        if not ms then
            if err == "not enough samples" then
                return nil, "not_enough_samples"
            end
            return nil, "bad_delay"
        end
    else
        return nil, "bad_delay"
    end
    if ms < min_delay then
        ms = min_delay
    end
    return ms
end

local function choose_fallback(client, prev, cand, may_retry)
    if not prev then
        return cand
    end
    local prev_retry = client:should_retry(prev.res, prev.class)
    local cand_retry = client:should_retry(cand.res, cand.class)
    if may_retry and cand_retry ~= prev_retry then
        return cand_retry and cand or prev
    end
    if (cand.res ~= nil) ~= (prev.res ~= nil) then
        return cand.res ~= nil and cand or prev
    end
    if cand.res and prev.res and client.respect_retry_after then
        local cand_ra = client.header(cand.res.headers, "Retry-After") ~= nil
        local prev_ra = client.header(prev.res.headers, "Retry-After") ~= nil
        if cand_ra and not prev_ra then
            return cand
        end
    end
    return prev
end

function _M.run(client, uri, params, hedge_opt, meta, deadline_ms, start)
    local delay_ms, derr = resolve_delay(client, hedge_opt)
    if not delay_ms then
        meta.hedge_skipped = derr
        return client:sequential(uri, params, meta, 0, nil, nil, nil,
                                 deadline_ms, start)
    end

    local max_hedges = hedge_opt.max_hedges or 1
    local bad_waits = hedge_opt.bad_status_waits
    if bad_waits == nil then
        bad_waits = true
    end

    local slots = {}
    local pending = {}
    local spawned = 0
    local hedges = 0
    local timer
    local deadline_timer
    local fallback

    local function spawn_attempt(is_hedge)
        spawned = spawned + 1
        local id = spawned
        local slot = {
            id = id,
            is_hedge = is_hedge,
            cancelled = false,
            reaped = false,
        }
        slots[id] = slot
        -- One table: a nil err in the middle of a multi-value return is
        -- dropped by ngx.thread.wait, and the later fields slide left.
        slot.thread = ngx.thread.spawn(function()
            local ok, res, err, httpc, sample_ms, sample_kind, class =
                pcall(client.attempt, client, uri, params, deadline_ms, slot,
                      start)
            if not ok then
                if slot.httpc then
                    slot.httpc.keepalive = false
                    slot.httpc:close()
                end
                return { id = id, err = res, class = "error" }
            end
            return {
                id = id,
                res = res,
                err = err,
                httpc = httpc,
                sample_ms = sample_ms,
                sample_kind = sample_kind,
                class = class,
            }
        end)
        pending[id] = slot.thread
        return slot
    end

    -- Kill first: close() while read_waiting returns "socket busy reading"
    -- and leaves the fd open. Kill runs the cosocket cleanup, which closes
    -- the connection. The following close() then reports "closed".
    local function cancel(slot, record_censored)
        if not slot or slot.reaped then
            return
        end
        slot.cancelled = true
        if record_censored and slot.io_started then
            client.estimator:record((client.clock() - slot.started) * 1000)
        end
        if slot.httpc then
            slot.httpc.keepalive = false
        end
        if slot.thread then
            ngx.thread.kill(slot.thread)
        end
        if slot.httpc then
            slot.httpc:close()
        end
        pending[slot.id] = nil
        slot.reaped = true
    end

    local function kill_timer()
        if not timer then
            return
        end
        ngx.thread.kill(timer)
        timer = nil
    end

    local function stop_timers()
        kill_timer()
        if deadline_timer then
            ngx.thread.kill(deadline_timer)
            deadline_timer = nil
        end
    end

    local function arm_timer(ms)
        local remain = deadline.remaining_ms(deadline_ms, start, client.clock())
        local sleep_ms = ms > 0 and ms or 0
        local hit_deadline = false
        if remain and remain < sleep_ms then
            sleep_ms = remain
            hit_deadline = true
        end
        if hit_deadline and sleep_ms < 1 - 1e-3 then
            meta.deadline_exceeded = true
            return
        end
        if sleep_ms < 1 and hit_deadline then
            sleep_ms = 1
        end
        timer = ngx.thread.spawn(function()
            local ok, err = pcall(ngx.sleep, sleep_ms / 1000)
            if not ok then
                return { id = 0, err = err }
            end
            if hit_deadline then
                return { id = 0, deadline = true }
            end
            return { id = 0 }
        end)
    end

    local function pending_n()
        local n = 0
        for _ in pairs(pending) do
            n = n + 1
        end
        return n
    end

    local remain = deadline.remaining_ms(deadline_ms, start, client.clock())
    if remain then
        if remain < 1 - 1e-3 then
            meta.deadline_exceeded = true
            return nil, "deadline", meta
        end
        local dsleep = remain < 1 and 1 or remain
        deadline_timer = ngx.thread.spawn(function()
            ngx.sleep(dsleep / 1000)
            return { id = -1 }
        end)
    end

    spawn_attempt(false)
    arm_timer(delay_ms)

    while true do
        local list = {}
        local n = 0
        for _, th in pairs(pending) do
            n = n + 1
            list[n] = th
        end
        if timer then
            n = n + 1
            list[n] = timer
        end
        if deadline_timer then
            n = n + 1
            list[n] = deadline_timer
        end
        if n == 0 then
            break
        end

        local first, second = ngx.thread.wait(unpack(list))
        local pkt = first
        if type(pkt) ~= "table" and type(second) == "table" then
            pkt = second
        end
        if type(pkt) ~= "table" or pkt.id == nil then
            for _, slot in pairs(slots) do
                cancel(slot, not slot.is_hedge)
            end
            stop_timers()
            meta.hedge_skipped = "wait"
            return nil, "wait failed: " .. type(first) .. " " .. tostring(first), meta
        end
        local who = pkt.id
        local res, err = pkt.res, pkt.err
        local httpc, sample_ms = pkt.httpc, pkt.sample_ms
        local sample_kind, class = pkt.sample_kind, pkt.class

        remain = deadline.remaining_ms(deadline_ms, start, client.clock())
        if who == -1 or pkt.deadline or (remain and remain <= 0) then
            if type(who) == "number" and who >= 1 and slots[who] then
                pending[who] = nil
                slots[who].reaped = true
                client:finish_socket(httpc, false, params)
                if sample_kind == "response" or sample_kind == "timeout" then
                    client.estimator:record(sample_ms)
                end
            end
            for _, slot in pairs(slots) do
                cancel(slot, true)
            end
            stop_timers()
            meta.deadline_exceeded = true
            meta.attempts = spawned
            if fallback then
                meta.winner = fallback.id
                return fallback.res, fallback.err, meta
            end
            return nil, "deadline", meta
        end

        if who == 0 then
            timer = nil
            if pending_n() > 0
                and spawned < client.max_attempts and hedges < max_hedges then
                if client.budget:try_acquire() then
                    hedges = hedges + 1
                    client.budget:record_hedge_sent()
                    meta.hedged = true
                    spawn_attempt(true)
                    if hedges < max_hedges and spawned < client.max_attempts then
                        arm_timer(delay_ms)
                    end
                else
                    meta.budget_exhausted = true
                end
            end
        elseif type(who) == "number" then
            local slot = slots[who]
            pending[who] = nil
            slot.reaped = true
            if sample_kind == "response" or sample_kind == "timeout" then
                client.estimator:record(sample_ms)
            end
            if class == "deadline" then
                meta.deadline_exceeded = true
            end
            if client:response_wins(res, err, bad_waits) then
                for id, other in pairs(slots) do
                    if id ~= who then
                        cancel(other, not other.is_hedge)
                    end
                end
                stop_timers()
                client:finish_socket(httpc, true, params)
                if slot.is_hedge then
                    client.budget:record_hedge_won()
                end
                meta.attempts = spawned
                meta.winner = who
                return res, err, meta
            end

            if class == "ok" then
                client:finish_socket(httpc, true, params)
            end
            local may_retry = spawned < client.max_attempts
                and not meta.budget_exhausted
                and not meta.deadline_exceeded
            fallback = choose_fallback(client, fallback, {
                res = res,
                err = err,
                id = who,
                class = class,
            }, may_retry)
            -- Attempt 1 finished before any hedge was sent: this is a retry,
            -- not a hedge.
            if who == 1 and hedges == 0 then
                kill_timer()
                break
            end
            if pending_n() == 0 then
                kill_timer()
                break
            end
            if hedges >= max_hedges or spawned >= client.max_attempts then
                kill_timer()
            end
        else
            for _, slot in pairs(slots) do
                cancel(slot, false)
            end
            kill_timer()
            break
        end
    end

    stop_timers()
    meta.attempts = spawned
    local last_res, last_err, last_class, winner
    if fallback then
        last_res = fallback.res
        last_err = fallback.err
        last_class = fallback.class
        winner = fallback.id
        meta.winner = winner
    end
    return client:sequential(uri, params, meta, spawned, last_res, last_err,
                             last_class, deadline_ms, start)
end

return _M
