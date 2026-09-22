# lua-resty-retry-budget

Retry budgets and hedged requests for OpenResty.

A retry budget limits extra attempts to a fraction of recent traffic, so a
partial outage does not turn into a retry storm. Hedged requests send one
speculative duplicate of an idempotent call when the first attempt is still
running at the upstream's recent tail latency, and that duplicate draws from
the same budget.

This is not a circuit breaker. It does not discover hosts or balance them.

Requires OpenResty >= 1.21.4. The HTTP client also requires
[lua-resty-http](https://github.com/ledgetech/lua-resty-http). Counter TTLs
need `resty.core.shdict` (loaded by default in OpenResty 1.21).

## Install

```bash
opm get ledgetech/lua-resty-http
# copy lib/ into your lua_package_path, or:
luarocks make lua-resty-retry-budget-0.1.0-1.rockspec
```

```nginx
lua_shared_dict retry_budget 10m;
```

Use a dedicated zone. A histogram is about 800 keys per budget name, and a
full zone evicts the counters the budget is reading. Give it room.

## Quick start

```lua
local rb = require "resty.retry_budget"

local client = rb.new_client("payments", {
    dict = "retry_budget",
    max_attempts = 3,
    backoff = { base_ms = 25, max_ms = 1000 },
    retry_on = {
        connect_error = true,
        timeout = false,
        status = { 502, 503, 504 },
    },
})

local res, err, meta = client:request_uri("http://payments.internal/charge", {
    method = "POST",
    body = "{}",
    headers = { ["Idempotency-Key"] = "8f14e45f" },
})
-- meta.attempts, meta.hedged, meta.budget_exhausted, meta.winner
```

The client runs only in `rewrite`, `access`, `content`, and `timer`. Anywhere
else it returns an error and does not touch a socket:

```text
retry-budget: client API is not available in phase "log"
```

Hoist the client (or the budget) to file scope so the per-worker window cache
stays warm. Constructing one per request re-sums the window every time.

## Budget

```lua
local budget = rb.new_budget("payments", {
    dict = "retry_budget",
    retry_ratio = 0.2,
    min_retries_per_sec = 10,
    window_sec = 10,
})

budget:record_request()          -- once per original request
local ok = budget:try_acquire()  -- check and, if allowed, count a retry
local peek = budget:can_acquire()
budget:record_retry()            -- count a retry nginx already committed to
```

A retry is allowed when

```text
retries_in_window < max(min_retries_per_sec * window_sec,
                        retry_ratio * requests_in_window)
```

With the defaults and no traffic the floor is 100 retries per 10 seconds.
The comparison is strict. Rejected retries are counted separately and do not
consume the budget.

The limit is soft. `try_acquire` is one atomic `incr` of the current second,
not a transaction across the request counters and the retry counters. Workers
that read the same sum can all pass. Overshoot is on the order of in-flight
checks, plus at most `refresh_interval` (default 100 ms) of writes that another
worker is still putting in the previous second. Size the upstream for that,
not for a hard cap.

If the shared dict cannot complete an operation, the default `fail_mode` is
`"open"`: the retry is allowed and a rate-limited error is logged. `"closed"`
denies the retry. The original request is never refused. Open mode during a
full dict means retries are unlimited until the zone works again.

```lua
budget:stats()
-- requests, retries, allowed_ratio, allowance, rejected,
-- hedges_sent, hedges_won, dict_errors, dict, latency_ms
local all = rb.stats()
-- all[dict_name][budget_name]
```

The same budget name in two shared dicts stays two entries. `dict` on each
stats table is the zone name.

`allowed_ratio` is the configured ratio. The floor shows up as `allowance`
(100 with the defaults and an empty window).

## Why timeouts are not retried

`retry_on.timeout` defaults to false. A send or read timeout means the bytes
may already have reached the upstream and the upstream may already have done
the work. Retrying it is how a charge, a write, or an email goes out twice.
A connect timeout is different: the connection was never established, so
nothing was sent, and `retry_on.connect_error` (default true) covers it.

`request_uri` from lua-resty-http reports every timeout as the string
`"timeout"`. This client uses `connect`, `send_request`, and its own read
loop so those cases stay distinct.

## HTTP client options

| Option | Default | |
| --- | --- | --- |
| `dict` | required | `lua_shared_dict` zone name |
| `max_attempts` | `3` | total sends, including the first and any hedge |
| `retry_ratio` | `0.2` | passed to the budget |
| `min_retries_per_sec` | `10` | passed to the budget |
| `window_sec` | `10` | passed to the budget |
| `refresh_interval` | `0.1` | seconds |
| `fail_mode` | `"open"` | `"open"` or `"closed"` |
| `backoff.base_ms` | `25` | full jitter, `base * 2^(attempt-1)` |
| `backoff.max_ms` | `1000` | cap, also the cap for `Retry-After` |
| `retry_on.connect_error` | `true` | refused, DNS, connect timeout, and a reset before the status line |
| `retry_on.timeout` | `false` | send or read timeout |
| `retry_on.status` | `502, 503, 504` | |
| `idempotent_methods` | `GET`, `HEAD`, `OPTIONS` | list `{ "GET" }` or set `{ GET = true }` |
| `idempotency_header` | `Idempotency-Key` | any non-empty value makes the call retryable |
| `respect_retry_after` | `true` | on 429 and 503, capped by `max_ms` and the deadline |
| `connect_timeout` | `1000` | milliseconds |
| `send_timeout` | `5000` | |
| `read_timeout` | `5000` | |
| `deadline_ms` | none | overridable per `request_uri` |
| `keepalive_timeout`, `keepalive_pool` | none | passed to `set_keepalive` |
| `estimator.window_sec` | `60` | latency window |
| `estimator.slot_sec` | `10` | must divide `window_sec` |
| `estimator.min_samples` | `100` | below this, `percentile` returns nil |

A 429 is retried only when `respect_retry_after` is set and `Retry-After`
parses (delta-seconds or an HTTP-date). A 429 without the header is returned
as-is.

Request bodies must be strings or tables of strings. A function body is a
stream: that call is not retried or hedged, and `meta.retry_skipped` is
`"streaming_body"`. A non-idempotent method without the idempotency header
sets `retry_skipped` to `"non_idempotent"`.

When the budget refuses another attempt, the return value is the last real
response or error from the upstream. `meta.budget_exhausted` is true. No
synthetic status is invented.

`deadline_ms` is a wall clock for the whole call, starting when
`request_uri` is entered. A retry and a hedge share that clock. They do
not each receive a new `deadline_ms`. Before each `connect`, `send`, and
`receive`, the socket timeout is set to the lesser of the configured
timeout and the time still left. Body reads use `receiveany` so a trickle
of bytes cannot refresh an idle timer for the rest of the deadline. A
retry sleep that would not finish before the deadline is not started, and
the budget token for it is not taken. A hedge timer that wakes because
the deadline was reached does not send another attempt, and
`meta.deadline_exceeded` is set.

Interim `1xx` responses (`100`, `102`, `103`) are read and discarded. The
status and body returned are the final response. `101` is returned as-is
and the connection is closed, because it is no longer an HTTP connection.

## Hedged requests

```lua
local res, err, meta = client:request_uri(url, params, {
    hedge = {
        enabled = true,
        delay = "p95",       -- or a number of milliseconds
        max_hedges = 1,
        min_delay_ms = 5,
        bad_status_waits = true,
    },
})
```

`delay = "p95"` uses the latency estimator. With fewer than `min_samples`
(default 100) the call does not hedge; `meta.hedge_skipped` is
`"not_enough_samples"`. A numeric delay is `max(delay, min_delay_ms)`.

The first attempt is sent immediately. If it is still running when the delay
elapses, and the budget allows it, a second attempt is sent. The first
successful response wins. With `bad_status_waits` (the default), a retryable
status such as 503 does not beat an attempt that is still in flight. A 503
that arrives before the delay elapses is an ordinary retry, not a hedge: the
duplicate is not sent while the first attempt is already dead.

Only idempotent calls are hedged. Hedges count against `max_attempts` and
against the retry budget. `meta.winner` is the attempt id whose response was
returned. `hedges_won` increments only when that attempt was a hedge, not
when a later sequential retry wins.

The loser is stopped with `ngx.thread.kill` and then `close()`. `close()`
while a read is pending returns `"socket busy reading"` and does not drop the
fd, so the kill has to come first: it runs the cosocket cleanup and closes
the connection. A response that was fully read may be returned to the
keepalive pool. A killed in-flight connection is not.

Samples under 1 ms all land in the first histogram bucket, so `percentile`
does not return a value below 1 ms. That does not move a hedge delay whose
`min_delay_ms` is 5, and it is the wrong tool for timing a local upstream.

### What feeds the latency estimator

| Outcome | Recorded |
| --- | --- |
| Complete response, any status | yes |
| Both hedge attempts finish | yes, both |
| Attempt 1 killed because a hedge won | yes, at the elapsed time (at least the hedge delay) |
| Hedge attempt killed because attempt 1 won | no |
| Connect, send, or read timeout | yes |
| Fast connect error (refused, reset, DNS) | no |

Killed attempt 1 is the request that was still running at the delay, so it
belongs in the tail. Dropping it makes the next "p95" the old p90, the delay
shrinks, and more requests hedge. The sample is censored, so the reported
p95 is a lower bound, and it stays stable. Timeouts are recorded for the same
reason: if more than 5% of attempts time out, p95 really is the timeout, and
the hedge delay grows instead of adding load to a dying upstream.

## Balancer recipe

`balancer_by_lua` cannot sleep or spawn threads, so it cannot hedge. It can
spend the budget. `set_more_tries(1)` adds one future try on every balancer
run, so a successful `try_acquire()` must not by itself arm another try.
`ngx.ctx` survives re-entry and holds the attempt count.

`proxy_next_upstream` does not retry 502/503/504 unless you list them. The
budget cannot override that. `proxy_next_upstream_tries 0` means unlimited;
set it to the same cap as `MAX_ATTEMPTS` so a mistake in the Lua still stops.
The upstream block needs exactly one placeholder `server`. Extra `server`
lines become free retries that skip the arming check.

```lua
-- payments_balancer.lua, loaded once per worker
local balancer = require "ngx.balancer"
local rb = require "resty.retry_budget"
local budget = rb.new_budget("payments", { dict = "retry_budget" })
local MAX_ATTEMPTS = 3

local _M = {}

function _M.run()
    local ctx = ngx.ctx
    local n = (ctx.rb_attempt or 0) + 1
    ctx.rb_attempt = n

    local may_arm = n < MAX_ATTEMPTS
    if n == 1 then
        budget:record_request()
    elseif not budget:try_acquire() then
        budget:record_retry()   -- nginx already committed to this send
        may_arm = false
    end

    local ok, err = balancer.set_current_peer("10.0.0.1", 8080)
    if not ok then
        ngx.log(ngx.ERR, "[retry-budget] key=payments ", err)
        return
    end

    if may_arm and budget:can_acquire() then
        ok, err = balancer.set_more_tries(1)
        if not ok then
            ngx.log(ngx.ERR, "[retry-budget] key=payments ", err)
        elseif err == "reduced tries due to limit" then
            ngx.log(ngx.WARN, "[retry-budget] key=payments ", err)
        end
    end
end

return _M
```

```nginx
upstream payments {
    server 0.0.0.1;
    balancer_by_lua_block {
        require("payments_balancer").run()
    }
}

location /charge {
    proxy_pass http://payments;
    proxy_next_upstream error http_502 http_503 http_504;
    proxy_next_upstream_tries 3;
    proxy_next_upstream_timeout 10s;
}
```

`timeout` is omitted on purpose, matching the client. One already-armed retry
can still exceed the budget: by the time `get_last_failure()` is set, nginx
has committed to this send. Refusing `set_current_peer` would answer 502
`no peer set`, which replaces the upstream status. The recipe sends that one
attempt, counts it with `record_retry()`, and arms nothing further.

Resolve the peer address before the balancer phase. Cosockets and DNS are not
available there.

## Redis

The budget object does not know HTTP.

```lua
local rb = require "resty.retry_budget"
local redis = require "resty.redis"
local budget = rb.new_budget("redis", { dict = "retry_budget" })

local function get(key)
    budget:record_request()
    local red = redis:new()
    red:set_timeouts(1000, 1000, 1000)
    local ok, err = red:connect("127.0.0.1", 6379)
    if not ok then
        if budget:try_acquire() then
            ngx.sleep(0.02)
            ok, err = red:connect("127.0.0.1", 6379)
        end
        if not ok then
            return nil, err
        end
    end
    local res, rerr = red:get(key)
    red:set_keepalive(10000, 100)
    return res, rerr
end
```

## Latency estimator

```lua
local est = rb.new_estimator("payments", { dict = "retry_budget" })
est:record(12.5)                  -- one incr
local ms, err = est:percentile(95)
```

`percentile` takes 1..100. The histogram is log-linear, ratio `2^(1/8)`, from
1 ms to 60 s. One timer per name per worker refreshes the cached scan at most
once a second. Constructing estimators per request does not start more timers.

## Prometheus

Soft dependency. Nothing is required unless you call it.

```lua
local prometheus = require("prometheus").init("prometheus_metrics")
local prom = require "resty.retry_budget.prometheus"
prom.register(prometheus)

-- scrape location, before collect()
prom.observe()
prometheus:collect()
```

The series are window gauges (`retry_budget_requests_window`,
`retry_budget_retries_window`, `retry_budget_retries_rejected_window`,
`retry_budget_hedges_sent_window`, `retry_budget_hedges_won_window`,
`retry_budget_dict_errors_window`, `retry_budget_latency_ms`). Each one is
labeled `budget` and `dict`. Latency also has `quantile` (`0.5`, `0.95`,
`0.99`). Read them directly. `rate()` is the wrong function for a window
count. A percentile with no samples in the current window is set to NaN,
so an expired histogram does not keep the previous scrape.

## Logs

```text
[retry-budget] key=payments shared dict evicted live entries
```

One line per code per budget name per second. Dict failures are logged at
error level so nginx does not sit on them in a block buffer.

## Tests

```bash
make test    # busted, LuaJIT
make t       # local Test::Nginx, or a skip message if it is not installed
make prove   # Test::Nginx inside openresty/openresty
make bench   # wrk amplification and happy-path overhead, in Docker
```

GitHub Actions runs the unit tests and `t/docker-prove.sh` on every push
and pull request.

## Benchmark

The upstream returns 503 on every other response. The naive balancer and the
budget balancer each arm their own retries, cap them at 3, and use one
placeholder peer. The budget is the balancer recipe above, ratio 0.2, floor
10 retries per second.

Measured in `openresty/openresty:1.31.1.1-3-jammy` on 2026-09-22, wrk
`-t2 -c64 -d15s` after a 12 second warmup:

| path | amplification | client rps |
| --- | --- | --- |
| budget | 1.19997 | 35989 |
| naive | 1.8040 | 22044 |

The budget target at that rate is `1.2 + 10/35989` = 1.2003. In the last 10
seconds the allowance was `0.2 * requests` (67528.4) and retries were 67532.
Happy-path added p50 against raw `request_uri` was 0 µs at a 5 µs
`ngx.now()` step, under the 20 µs target. Method, attempt counts, and the
status mix are in `bench/README.md`.

## License

BSD 2-clause. See `LICENSE`.
