# lua-resty-retry-budget — design

Status: design only. No library code until this is accepted.

This library gives OpenResty a Finagle-style **retry budget** (retries may add only a bounded fraction of recent load) and **hedged requests** (one speculative duplicate of an idempotent call, drawn from that same budget). It is not a circuit breaker.

Targets: OpenResty >= 1.21.4 (LuaJIT 2.1, lua-nginx-module >= 0.10.21). Runtime dependency: `ledgetech/lua-resty-http` only, and only for the HTTP client. `resty.core.shdict` is part of OpenResty and is required for counter TTLs (`lua_load_resty_core on` is the 1.21 default; `new()` requires it anyway).

## Platform constraints

These contradict a literal reading of the task. The implementation will follow this section.

1. **`try_acquire` is one atomic `incr`, not a cross-key transaction.** Shared dict has no CAS that covers "sum of requests" and "sum of retries" together. `try_acquire` reads the window, and if the retry fits, `incr`s the current retry bucket. That `incr` is atomic. Concurrent workers that all read the old sum can each pass the check. The budget is a **soft** limit: overshoot is on the order of in-flight `try_acquire` calls, plus at most `refresh_interval` of cross-worker writes into the previous second (clock skew). Document this in the README. Do not pretend otherwise.

2. **`balancer_by_lua` cannot approve a retry after the failure is known without having armed it first.** `ngx.balancer.set_more_tries(count)` adds *future* tries on every balancer run (`peer.tries += count`). The current attempt is excluded. Nginx only re-enters the balancer after a failure that `proxy_next_upstream` already accepted. Calling `try_acquire()` only inside that re-entry is too late to stop the attempt already in progress: skipping `set_current_peer` makes nginx log `lua balancer: no peer set` and answer **502**, a synthetic response.

   Arming again after every successful `try_acquire()` is unbounded. With `proxy_next_upstream_tries 0` a hard-down upstream is retried until the budget is empty or `proxy_next_upstream_timeout` fires. The recipe caps sends with `ngx.ctx.rb_attempt` and a caller `MAX_ATTEMPTS` (the example uses 3):
   - attempt 1 calls `record_request()` and does not withdraw;
   - a later attempt withdraws with `try_acquire()`;
   - if that withdraw loses the race, the send still happens, `record_retry()` counts it, and nothing further is armed;
   - `set_more_tries(1)` runs only when `attempt < MAX_ATTEMPTS` **and** `can_acquire()` is true. A successful withdraw does not by itself arm the next try.
   One already-armed retry can exceed the budget. That is the balancer form of the soft limit. `proxy_next_upstream_tries` is set to the same `MAX_ATTEMPTS` so a bug in the Lua arm still stops.

   `peer.tries` starts as the round-robin server count. Extra `server` lines are free retries that bypass the arming decision. The upstream block must contain **one** placeholder `server 0.0.0.1;`. Verified in `ngx_http_lua_ffi_balancer_set_more_tries`: `proxy_next_upstream_tries 0` (the nginx default) means "no clamp" because the limit is applied only when `max_tries` is non-zero. A non-zero value clamps the arm and returns the warning `"reduced tries due to limit"` while still returning true. The recipe uses that clamp as a backstop, not as the only cap.

   `proxy_next_upstream` defaults to `error timeout` and does **not** include `http_502`, `http_503`, `http_504`, or `non_idempotent`. The budget cannot force nginx to retry a status nginx has not been told to retry. The recipe sets the directive. Putting `timeout` or `non_idempotent` there retries classes the HTTP client refuses by default; the balancer path has no separate timeout switch because `get_last_failure()` only reports `"failed"` vs `"next"`.

3. **`request_uri` hides the timeout class and pools the socket before the caller can cancel.** `httpc:request_uri` returns the string `"timeout"` for connect, send, and read timeouts, then calls `set_keepalive` on success (`lib/resty/http.lua`). The client uses `connect`, `send_request`, and `read_response` instead. A connect timeout is a `connect_error` (nothing was sent; retried by default). A send or read timeout is `timeout` (the upstream may have processed the request; **not** retried by default).

4. **Cancel order is kill, then close.** `ngx_http_lua_socket_tcp_close` calls `ngx_http_lua_socket_check_busy_reading` before it finalizes. While `u->read_waiting` is set that macro returns `nil, "socket busy reading"` and does not touch the fd, so `httpc:close()` from the parent does not unblock the child's `receive`. `ngx.thread.kill` on a running light thread calls `ngx_http_lua_cleanup_pending_operation`, which runs `ngx_http_lua_coctx_cleanup`, which calls `ngx_http_lua_socket_tcp_finalize` and `ngx_close_connection`. That path does not `setkeepalive`. After kill, `u->peer.connection` is NULL, and a following `close()` returns `nil, "closed"`, which the parent ignores. `lua-resty-http`'s `read_body` still cannot be used here: it reads inside `coroutine.wrap`, and kill cleanup runs on the light thread's `co_ctx`, not that inner coroutine. Attempt threads call `sock:receive` on the light thread itself so the cleanup above owns the read. A fully read response may be pooled, including a finished loser we did not use. A killed in-flight connection is never pooled.

5. **`ngx.thread.wait` returns the first thread's values and does not say which thread finished.** A Lua error inside the thread makes this worse: `wait` returns `false, err` and the sentinel id is gone, so the coordinator cannot tell which slot died. Each attempt body is wrapped in `pcall` and always returns its id first; the error string is the attempt's `err`. Left-running threads can also wake a later `ngx.sleep` in the parent (lua-nginx-module #1439). The parent does not call `ngx.sleep` while a spawned thread is still alive; it kills or reaps first. `ngx.thread.kill` only works from the light thread that spawned the child, so the request handler spawns every attempt and the timer. It does not spawn attempts from inside attempts. A thread already killed is not passed to a later `wait` (`"already waited or killed"`).

6. **`dict:incr(key, value, init, init_ttl)` stores `init + value` when the key is missing, and `init_ttl` is applied only then.** Later incrs do not extend the TTL. Call `incr(key, 1, 0, ttl)` so the first hit stores `1`. `init_ttl` requires `resty.core.shdict`; without it the pure-C incr ignores the TTL and buckets live forever. The third return `forcible == true` means a live LRU entry was evicted to make room. That is a dict failure even though the incr succeeded (see failure policy).

7. **There is no cheap key listing.** `get_keys()` locks the slab and defaults to 1024 keys. Histogram state will exceed that. Names are appended to a single index key under a short `add` lock. `stats()` reads the index. It does not scan the dict.

8. **`ngx.now()` does not move until the running request yields.** Budget checks do not need time to advance mid-call. Sleeps and cosocket I/O yield, so deadlines move. Tests inject `opts.clock`. Production code never calls `os.time()` or `ngx.update_time()`.

9. **Hedging cannot run in `balancer_by_lua`.** That phase cannot yield, `ngx.sleep`, or `ngx.thread.spawn`. Hedges exist only on the HTTP client, in `rewrite`, `access`, `content`, and `timer`. The budget object itself is usable anywhere shared dict is legal, including `balancer`, `log`, `header_filter`, `body_filter`, `set`, and `init_by_lua*`.

10. **No FFI.** `dict:incr` is already the C fast path. An FFI copy of the same slab op would not be faster.

## Module map

```
lib/resty/retry_budget.lua                 public entry, lazy-loads the client
lib/resty/retry_budget/budget.lua          window, cache, stats, name index
lib/resty/retry_budget/histogram.lua       log-linear estimator
lib/resty/retry_budget/backoff.lua         pure jitter math
lib/resty/retry_budget/deadline.lua        pure remaining-time math
lib/resty/retry_budget/client.lua          retries + hedge coordinator
lib/resty/retry_budget/hedge.lua           state machine, no policy tables of its own
lib/resty/retry_budget/prometheus.lua      optional, separate require
```

`require "resty.retry_budget"` loads budget + histogram only. `new_client` then requires `resty.http` and the client. A Redis caller that never constructs a client does not touch cosockets.

No module-level `ngx.*` call. Defaults such as `ngx.now` are read inside functions so busted can load the modules without OpenResty. No globals: every library chunk keeps `_M` local. Makefile fails the build if `luajit -bl` prints `GSET` for a library file.

Budget objects are not singletons. Dict keys are shared; the window-sum cache is per object per worker. Callers hoist one object per worker (`local budget = rb.new_budget(...)` at file scope) so the cache stays warm. Constructing one per request pays a name-registration `add` and re-sums the window on every call. Two long-lived objects with the same name are correct and merely refresh twice.

The histogram percentile cache, per-slot key strings, and refresh timer are module-level, keyed by dict name and budget name. One timer runs per name per worker however many estimator objects exist.

## Shared dict key schema

Zone name is `opts.dict` (a string). The object resolves it once with `ngx.shared[name]`. Pass `opts.dict_obj` only from tests.

Budget name: 1..64 bytes, `^[A-Za-z0-9._%-]+$`. The delimiter is `:`. Names cannot contain `:`, whitespace, or NUL.

Prefixes are built once in `new()`:

```
P = "rb:" .. name .. ":"

P .. "q:" .. epoch     requests in that unix second
P .. "t:" .. epoch     retries and hedges withdrawn
P .. "j:" .. epoch     retries rejected by the budget (not a withdraw)
P .. "hs:" .. epoch    hedges sent
P .. "hw:" .. epoch    hedges that won
P .. "e:" .. epoch     dict errors
P .. "L:" .. slot .. ":" .. bucket     histogram
P .. "n"               presence marker, no TTL
"rb:\0names"           newline-separated name index
"rb:\0lock"            index lock, 1s TTL via add
```

`epoch = floor(clock())`. `slot = floor(clock() / slot_sec)`.

| Key | TTL | Written by |
| --- | --- | --- |
| `q`, `t`, `j`, `hs`, `hw`, `e` | `window_sec + 1` | `incr(key, 1, 0, ttl)` |
| `L` | `hist_window_sec + slot_sec` | one `incr` per sample |
| `n`, index | none | `add` / `set` at construction |

TTL starts when the key is created, not on later incrs. A bucket for second `T` is created during `T` and expires `window_sec + 1` seconds later. Readers only address seconds still inside the window, so they do not observe a bucket after its TTL if the clock is monotonic.

Counter kinds for one budget over a 10s window: ~60 keys. Histogram default: 6 slots × 128 buckets ≈ 768 keys. Recommend a **dedicated** zone, `lua_shared_dict retry_budget 10m`, so unrelated modules cannot LRU-evict the counters. Sharing a zone is allowed and is the main way to hit `forcible`.

Name registration in `new()`:

1. `add(P.."n", "1")`. The marker means registration has started. `"exists"` is not proof the index contains the name.
2. If `rb:\0names` already lists the name, stop.
3. `add("rb:\0lock", "1", 1)`. On success, re-read the index, append `name .. "\n"` if absent, then `delete` the lock.
4. If the lock is busy, leave the marker and return. The next `register_name` of that name tries the index again. `stats()` unions the dict index with names constructed in this worker, so a lock miss does not hide a local budget, and a later registration publishes it to other workers.

## Budget algorithm

```
epoch = floor(now)
window = buckets [epoch - window_sec + 1, epoch]     -- window_sec buckets, current one included

allowance = max(min_retries_per_sec * window_sec,
                retry_ratio * requests_in_window)

retry is allowed iff retries_in_window < allowance     -- strict
```

Defaults: `retry_ratio = 0.2`, `min_retries_per_sec = 10`, `window_sec = 10`, `refresh_interval = 0.1` (seconds), `fail_mode = "open"`.

`window_sec` is an integer in `1..300`. `retry_ratio` is in `[0, 10]`. `min_retries_per_sec >= 0`. `refresh_interval >= 0` (`0` refreshes on every check).

With the defaults and no traffic, `allowance = 100`: retries `0..99` pass and the 100th is rejected. At 10_000 requests in the window, `allowance = max(100, 2000) = 2000`. A fractional allowance is compared as a number (`3 * 0.2 = 0.6` allows a retry only while `retries < 0.6`, so one retry).

The original request is never refused. `record_request` failing does not fail the user request. It under-counts the ratio term, which only makes later retries stricter. The floor still applies. `fail_mode = "closed"` denies **retries and hedges**, not the first attempt.

### Per-worker cache

Each budget object holds one table, allocated in `new()`:

```
cache = {
  sec = nil, refreshed_at = -math.huge,
  closed_req = 0, closed_ret = 0, closed_rej = 0,
  closed_hs = 0, closed_hw = 0, closed_err = 0,
  q_key, t_key, j_key, hs_key, hw_key, e_key,   -- strings for `sec`
}
```

`sums()`:

- Let `sec = floor(now)`.
- If `cache.sec ~= sec`, re-sum epochs `[sec - window_sec + 1, sec - 1]` and remember the value read for `sec - 1` (`prev_req`, and the same for the other counter kinds). Rebuild the current-second key strings.
- Else if `now - refreshed_at >= refresh_interval`, `get` **only** epoch `sec - 1` and add the delta to the closed sum (`closed = closed - prev + latest`). Older closed buckets no longer change.
- Always `get` the current second and add it.

Closed buckets older than the previous second are immutable. The second-boundary full sum exists so the second that just ended joins the closed sum immediately; without it, that second would be neither "closed" nor "current" for up to `refresh_interval`, and the budget would over-allow. The intra-second refresh picks up another worker that is still writing the previous second because its `ngx.now()` has not rolled yet. Hot path after that is one `get` of that previous second, `get`s of the current second, and one `incr` to record.

`record_request`: ensure key cache, `incr` the `q` key.

`can_acquire`: `sums()`, compare, no write.

`try_acquire`: if not allowed, `incr` the `j` key and return false. If allowed, `incr` the `t` key. On incr error or `forcible`, apply the failure policy. On success return true.

`record_retry`: `incr` the `t` key with no check. Used only for a balancer attempt nginx has already committed to. Returns false on dict error (the send still happens; the caller logs).

Rejected retries do **not** increase `t`. A reject storm must not pin the budget shut.

Hedge counters (`hs`, `hw`) are the same shape: `record_hedge_sent`, `record_hedge_won`. They do not affect `allowance`. A hedge withdraws through `try_acquire` like any other retry, **before** the extra attempt is sent. No refunds. A hedge that is killed still happened: the bytes were sent.

### Failure policy

| Event | `fail_mode = "open"` (default) | `fail_mode = "closed"` |
| --- | --- | --- |
| `incr` / `get` path returns an error (`"no memory"`, `"not a number"`, zone missing at call time) | allow the retry, do not claim it was counted | deny the retry |
| `forcible == true` | allow; the incr already counted | deny; the incr already counted, so later retries are tighter |
| `record_request` fails | log, continue with the user request | same; the first attempt is not gated |

Every such event: best-effort `incr` of the `e` bucket, and one warning per distinct code per key per second. The limiter is `add(P.."log:"..code, "1", 1)`. If that `add` fails because the key exists, skip the log. If it fails because the dict is unusable, a worker-local timestamp suppresses repeats for 1s. Log line shape:

```
[retry-budget] key=<name> <message>
```

No happy-path logs. Open mode during a full dict means retries are unlimited until the dict works again. That is the availability choice. Size the zone so it stays cold.

## Histogram

Public estimator, also used by the client for `delay = "p95"`.

```lua
local est = rb.new_estimator("payments", {
    dict = "retry_budget",
    window_sec = 60,     -- hist_window_sec
    slot_sec = 10,       -- must divide window_sec
    min_samples = 100,
    clock = nil,         -- test injection
})
est:record(latency_ms)          -- exactly one incr
local ms, err = est:percentile(95)
```

`percentile` takes `1..100` (the call `percentile(95)` means p95). Out of range returns `nil, err`.

### Buckets

Geometric ratio `2^(1/8) ≈ 1.0905` (8 buckets per doubling), from 1 ms to 60 s. That is finer than "about two buckets per power of √2". Two buckets per √2 step is ratio `2^(1/4) ≈ 1.189`, and a point mass can land ~9% away from the reported geometric midpoint, which fails the 5% accuracy bar. Half a `2^(1/8)` bucket is ~4.4% wide, so the bar is achievable.

```
LN = math.log(2) / 8
if ms < 1 then bucket = 1
else
  k = floor(log(ms / 1) / LN) + 1
  bucket = min(k, N)          -- N covers the overflow at the top
end
N = ceil(log(60000) / LN) + 1     -- 128 buckets: 127 finite + 1 overflow
```

Bucket `k` (1-based, below the overflow) represents `[2^((k-1)/8), 2^(k/8))` milliseconds. Anything under 1 ms lands in bucket 1, whose lower edge is 1 ms, so `percentile` does not return a value below 1 ms. Local upstreams are often sub-millisecond; a reported p50 of about 1 ms is that clamp, not a measured 1 ms. With `min_delay_ms = 5` the hedge delay is unaffected. The estimator is the wrong tool for a microbenchmark.

`record` maps with that function and `incr`s one histogram key. The 128 key strings for the current slot live in the module registry and are rebuilt only when `floor(now / slot_sec)` changes, same as the budget's per-second key cache. A sample does not concatenate `P.."L:"..slot..":"..bucket`.

### Percentile

Once per worker per name, at most once a second, sum the live slots `slot - (window/slot_sec) + 1 .. slot` into a counts array in the module registry. Later `percentile` calls in that second only walk the array. Objects share that array.

Rank: `target = ceil(p/100 * total)`. Walk to the bucket that contains that rank. Report the log-interpolated value at the mid-rank inside the bucket:

```
frac = clamp((target - cum_before - 0.5) / count, 0, 1)
value_ms = lower * RATIO ^ frac
```

If `total < min_samples`, return `nil, "not enough samples"`. Hedge treats that as "do not hedge".

The first request of each second must not pay ~768 `get`s (that is a self-inflicted tail). The registry starts one `ngx.timer.at` chain per name per worker, on first `record` or `percentile` in a context where timers are legal. The callback refreshes the shared cache and reschedules itself. `premature` clears the registry slot so a worker exit does not leave a dead flag set. `new()` does not start a timer: `ngx.timer.at` is illegal in `init_by_lua`, and a per-object timer would leak if callers constructed an estimator per request. If `ngx.timer.at` fails, `percentile` refreshes lazily and the failure is logged once per name. Until the timer has run, `percentile` refreshes lazily.

### What gets recorded

Percentiles are ranks. Dropping a sample moves every later rank down.

| Outcome | Recorded? | Value |
| --- | --- | --- |
| Complete HTTP response, any status, not cancelled | yes | time to the end of the body |
| Both hedge attempts finish | yes, both | each attempt's own time |
| Attempt 1 killed because a hedge won | yes | elapsed time at kill, which is at least the hedge delay |
| A hedge attempt killed because attempt 1 won | no | its elapsed time is "how fast we noticed attempt 1", which is short |
| Send or read timeout | yes | elapsed time, which sits on the configured timeout |
| Connect timeout | yes | elapsed time, same rank argument |
| Connect error that is not a timeout (refused, reset, DNS) | no | that is not a latency |
| Lua error inside the attempt (`pcall` failure) | no | no meaningful elapsed time |

Killed attempt 1 is the request that was still running at the hedge delay, so it belongs in the tail. Leaving it out removes the top of the distribution: the next "p95" is the old p90, the delay shrinks, more requests hedge, and more tail samples are dropped until the delay sits on `min_delay_ms`. The sample is censored (the attempt would have taken longer), so the reported p95 is a lower bound. It stays stable. Recording only attempts that both finish does not fix this, because the loser is killed as soon as the winner arrives and both rarely finish.

Send and read timeouts are recorded for the same rank reason. If more than 5% of attempts time out, p95 really is the timeout. The hedge delay then grows toward that timeout, so a dying upstream is hedged less, not more. Omitting those samples would make the upstream look faster and increase hedge load during the outage. Connect timeouts are included on that same argument. A fast connect error is not a latency and stays out, so a refuse storm does not pull p95 down.

## Backoff and deadline

Pure functions. The client applies them; busted tests them without `ngx.sleep`.

Full jitter, `attempt` = how many attempts have already failed (1 after the first failure):

```
cap = min(max_ms, base_ms * 2^(attempt - 1))
delay_ms = random() * cap          -- random() in [0, 1)
```

Defaults: `base_ms = 25`, `max_ms = 1000`. Inject `opts.random` (default `math.random`).

`Retry-After` (only when `respect_retry_after`, and only for status 429 or 503 that we are already going to retry):

- `^%d+$` → delta-seconds.
- otherwise `ngx.parse_http_time` (IMF-fixdate and the other formats nginx parses).
- unparseable → fall through to jitter.
- `delay = min(parsed_ms, max_ms, remaining_ms)`.

429 is **not** in the default status set. It is retried only when `respect_retry_after` is true **and** a parseable `Retry-After` is present. 429 without the header is returned as-is. 503 is in the default set and uses `Retry-After` as the delay when the header parses; otherwise jitter.

Deadline (`deadline_ms` on the client, overridable per `request_uri`):

```
remaining_ms = deadline_ms - (clock() - start) * 1000
```

`start` is the `request_uri` call. Every connect, send, receive, retry sleep, and hedge delay uses that same origin. An attempt does not take a fresh `deadline_ms`. Before a retry sleep: if `delay_ms >= remaining_ms`, do not `try_acquire` and do not sleep. Stop. A retry token is acquired only after the delay is known to fit, and before `ngx.sleep`.

The hedge timer sleeps `min(delay, remaining)`. If that sleep ends because the deadline was reached, it does not `try_acquire` and does not spawn another attempt. `meta.deadline_exceeded` is set. A deadline failure on a hedged attempt sets the same flag, including when the call then falls through to the sequential loop.

`settimeouts(connect, send, read)` does not enforce the deadline. Each value is an idle timeout for that operation, and the read timer is re-armed on every `receive`. An upstream that writes one byte every 900 ms keeps the attempt alive past any sum of the three configured timeouts. The client calls `sock:settimeout(ms)` immediately before `connect`, before `send`, and before every `receive` (status line, each header line, each body read). `ms = min(configured_for_that_op, remaining_ms)`. `sock:settimeout` writes that one value into the connect, send, and read slots; calling it immediately before the operation makes the next op use the wall-clock remainder. If `remaining_ms < 1`, the attempt stops, the socket is closed, and `meta.deadline_exceeded` is set. `settimeout(0)` is not used. The pure helper is `next_timeout_ms(remaining, configured) -> ms or nil`.

## HTTP client

```lua
local client = rb.new_client("payments", { ... })
local res, err, meta = client:request_uri(uri, params, call_opts)
```

Phases: `rewrite`, `access`, `content`, `timer`. Anything else returns

```lua
nil,
'retry-budget: client API is not available in phase "log"',
{ attempts = 0, hedged = false, budget_exhausted = false, winner = nil,
  retry_skipped = "phase", hedge_skipped = "phase", deadline_exceeded = false }
```

without indexing a cosocket. `ngx.get_phase()` is the check.

`params` is the `lua-resty-http` table (`method`, `headers`, `body`, `query`, `ssl_server_name`, ...), passed through. Default method is `GET`.

### When a retry is legal

Retryable iff all of:

- `type(body) ~= "function"` (a table or string body is replayable; a function is a stream);
- method is in `idempotent_methods` (default `GET`, `HEAD`, `OPTIONS`) **or** `headers[idempotency_header]` is a non-empty string. Header lookup is case-insensitive. An empty string does not count.

Otherwise one attempt, `meta.retry_skipped = "streaming_body"` or `"non_idempotent"`. Hedges use the same predicate.

A **response** is retried when `retry_on.status[status]` is true, or the 429 rule above matches. A **transport error** is retried when it was classified `connect_error` and `retry_on.connect_error` (default true), or classified `timeout` and `retry_on.timeout` (default **false**).

Default `retry_on.status`: 502, 503, 504. Built into a lookup table in `new()`.

`max_attempts` (default 3) counts every send: the first attempt, sequential retries, and hedges.

On budget denial: return the last real `res, err` from `lua-resty-http`'s conventions (a 503 is `res` with `err == nil`; a connect failure is `nil, err`). Set `meta.budget_exhausted = true`. Never synthesize a status.

`meta`:

| Field | Meaning |
| --- | --- |
| `attempts` | sends actually started |
| `hedged` | at least one hedge send was started |
| `budget_exhausted` | a further send was wanted and `try_acquire` refused |
| `winner` | 1-based id of the attempt whose `res`/`err` is returned |
| `retry_skipped` | `nil`, `"phase"`, `"non_idempotent"`, `"streaming_body"` |
| `hedge_skipped` | `nil`, `"phase"`, `"disabled"`, `"non_idempotent"`, `"streaming_body"`, `"max_attempts"`, `"not_enough_samples"`, `"bad_delay"` |
| `deadline_exceeded` | stopped because the next sleep or attempt would not fit |

Sequential loop: `record_request` once; attempt 1 does not withdraw; attempts `2..max` withdraw after the delay fits and before the sleep. Each socket operation uses `next_timeout_ms`, not one `set_timeouts` for the whole attempt.

Each attempt creates its own `httpc`. The parent `set_keepalive`s only after a full read. Errors `close`.

## Hedge state machine

`request_uri(uri, params, { hedge = { enabled = true, delay = "p95", max_hedges = 1, min_delay_ms = 5, bad_status_waits = true } })`.

`delay` is a number of milliseconds or `"pNN"`. Resolved delay is `max(computed, min_delay_ms)`. `"pNN"` with fewer than `min_samples` (default 100, overridable on the estimator and on this table) does not hedge; the call falls through to the sequential loop.

`max_hedges` extra attempts, each also capped by `max_attempts - 1`. `max_attempts < 2` skips hedging (`hedge_skipped = "max_attempts"`).

A result **wins** if there is no transport error and the status is not a retryable status. When `bad_status_waits` is true (default), a retryable status does not win while another attempt is still in flight. When false, the first completed response wins, 5xx included.

```
spawn attempt 1 (is_hedge = false), body wrapped in pcall
spawn timer = ngx.sleep(min(delay, remaining))

loop:
  wait on every still-running attempt and the timer
  -- each returns its id first; a pcall failure is id, nil, err
  if timer finished first:
      if attempt 1 already reaped: ignore (should not arm)
      else if spawned < max_attempts and hedges < max_hedges
              and try_acquire():
          spawn next attempt with is_hedge = true
          record_hedge_sent
          meta.hedged = true
          if more hedges allowed: arm another timer
      else:
          meta.budget_exhausted = true if the acquire failed
          do not arm another hedge
  if an attempt finished first:
      kill the timer before any later sleep
      record this attempt's latency under the table above
      if it wins:
          cancel every other in-flight attempt
          pool the winner (and any other fully read socket)
          if the winner's slot is_hedge: record_hedge_won
          return that result
      else:
          keep the earliest failure as the fallback
          if attempt 1 failed before the timer fired:
              kill the timer
              break to sequential retries for the unused attempt slots
          if nothing is in flight and no hedge is left to arm:
              break to sequential retries

sequential continuation uses the same budget, backoff, and deadline.
those slots have is_hedge = false. attempt ids keep incrementing.
meta.winner is the id that produced the return value.
```

`hedges_won` follows `slot.is_hedge`, not `winner id > 1`. A sequential retry after a failed hedge reuses the id sequence and must not count as a hedge win.

If attempt 1 fails fast, this is an ordinary retry, not a hedge: the duplicate is not sent while the first is already dead. Hedging is only "the first is still in flight at the delay".

Timer and attempts return a sentinel number so `wait` is unambiguous (`0` = timer, `n` = attempt `n`). The `pcall` wrapper is what makes a Lua error still carry that id. After `wait` returns, any thread still running is either the one we still care about (included in the next `wait`) or is cancelled before the next `ngx.sleep`.

Cancel path, for an in-flight slot only:

1. `slot.cancelled = true`, `httpc.keepalive = false`.
2. `ngx.thread.kill(slot.thread)`. Kill's coctx cleanup finalizes the cosocket and `ngx_close_connection`s it.
3. `httpc:close()`, ignoring `nil, "closed"`. If kill did not run cleanup (thread already dead), `close` still runs and refuses to `setkeepalive`.
4. Do not `wait` on that thread afterward.

`set_keepalive` is not called on a cancelled slot. The integration test holds the slow upstream open, hedges, and asserts the next request to that upstream is a new connection (`$connection_requests == 1` on the slow listener, which saw the reset and must not have been pooled).

## Balancer recipe

The budget object is constructed once per worker, at file scope. `balancer_by_lua_block` only calls `run`. `ngx.ctx.rb_attempt` survives balancer re-entry for the same downstream request and is the send cap. `set_more_tries(1)` adds one future try per call, so it is gated by both the cap and `can_acquire()`.

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
lua_shared_dict retry_budget 10m;

upstream payments {
    server 0.0.0.1;          # single placeholder; see constraint 2
    balancer_by_lua_block {
        require("payments_balancer").run()
    }
}

server {
    location /charge {
        proxy_pass http://payments;
        proxy_next_upstream error http_502 http_503 http_504;
        proxy_next_upstream_tries 3;     # same as MAX_ATTEMPTS, a backstop
        proxy_next_upstream_timeout 10s;
    }
}
```

`timeout` is omitted on purpose, matching the client default. Add it only with the same eyes-open this design asks for on `retry_on.timeout`. `proxy_next_upstream_tries 0` would leave a mistaken `set_more_tries` unbounded until the 10s timeout.

Peers are resolved before the balancer phase (cosockets and DNS are illegal there). The snippet uses literals; a real config resolves in `access_by_lua` and stores the address on `ngx.ctx`.

## Observability

`budget:stats()` reads live current buckets plus the cached closed sum (refreshing if due):

```lua
{
  requests = N,
  retries = N,
  allowed_ratio = 0.2,   -- configured retry_ratio, not retries/requests
  allowance = N,         -- absolute cap right now, floor included
  rejected = N,
  hedges_sent = N,
  hedges_won = N,
  dict_errors = N,
  latency_ms = { p50 = ms_or_nil, p95 = ms_or_nil, p99 = ms_or_nil },
}
```

The four fields in the task (`requests`, `retries`, `allowed_ratio`, `rejected`) are always present. The rest are the observability set. `allowed_ratio` is the configured ratio; the floor is visible through `allowance` (with zero traffic, `allowance == 100` while `allowed_ratio == 0.2`).

`rb.stats()` returns `{ [zone] = { [name] = budget:stats() } }` for every indexed name. The zone is the shared-dict name, so the same budget name in two zones does not overwrite. `budget:stats()` includes `dict` with that zone name. Percentiles are nil until an estimator for that name has samples. The client constructs the estimator. A budget-only user who wants percentiles constructs `new_estimator` with the same name and dict.

`resty.retry_budget.prometheus.register(prom, { dict = "retry_budget" })` soft-depends on `knyar/nginx-lua-prometheus` via `pcall(require, "prometheus")`. The caller passes an already constructed prometheus object (that library owns a dict and an `init_worker` sync; this one must not invent a second). `register` creates gauges. `observe()` sets them from `rb.stats()` and is called from the scrape location **before** `prom:collect()`. Window values are gauges, not lifetime counters, so `rate()` is the wrong function; the number is already a window count. Hot path does not call prometheus.

Gauges: `retry_budget_requests_window`, `retry_budget_retries_window`, `retry_budget_retries_rejected_window`, `retry_budget_hedges_sent_window`, `retry_budget_hedges_won_window`, `retry_budget_dict_errors_window`, `retry_budget_latency_ms`. Every gauge is labeled `budget` and `dict` (the shared-dict zone). Latency also has `quantile` in `{"0.5","0.95","0.99"}`. When a percentile is nil, `observe` sets that series to NaN so an expired window does not keep the previous value.

## Non-HTTP use

The budget object does not know HTTP. A Redis retry is `record_request`, the call, and `try_acquire` before each extra attempt. The README shows that with `lua-resty-redis`. No Redis code lives in this repo.

## Tests

### busted (`spec/`)

Fake dict: `incr(k, v, init, ttl)` stores `init + v` or adds `v`; expiry uses the injected clock; supports `"no memory"` and `forcible`. No `ngx`.

- Window math: deposit at `t=0.2` (bucket 0) is visible through `floor(now)=9` and gone at `floor(now)=10`.
- Floor vs ratio, strict `<`, fractional allowance, rejection does not consume `t`.
- Exhaustion, then recovery after the window slides.
- Fail open allows and logs-once; fail closed denies; `forcible` follows the same switch; first attempt is not denied.
- Two objects, one dict: current-second incr is visible immediately. A write into `sec - 1` shows up after `refresh_interval`. A write into an older closed bucket does not, until the second rolls.
- Jitter bounds for attempts 1..8, including the `max_ms` cap, with `random` stubbed to `0` and `0.999999`.
- Bucket edges: `1`, the first ratio boundary, `60000`, and a value above the top. A sample of `0.2` ms is bucket 1 and does not produce a percentile below 1 ms.
- 20k exponential samples, mean 20 ms: estimated p50/p95/p99 within 5% relative of `-ln(1-p)*mean`. Same bar for one lognormal.
- `record` of many samples in one slot does not build a new key string per sample (the slot's key table is reused). Two `new_estimator` calls for the same name share one registry entry.

`next_timeout_ms(remaining, configured)` returns `nil` when `remaining < 1`, otherwise `min(remaining, configured)`.

### Test::Nginx (`t/*.t`)

Mock upstreams are extra `server` blocks (fixed status, `echo_sleep`, or a shared-dict schedule).

- 503 then 200: two attempts, `meta.attempts == 2`, `winner == 2`.
- POST without the idempotency header: one attempt. POST with `Idempotency-Key` retries.
- Sustained 503: accepted retries stay at about `max(floor, 0.2 * requests)` ; `meta.budget_exhausted` on the refused ones; the body returned is the upstream 503.
- `Retry-After: 2` on 503 delays the next attempt (timestamp delta), and a value above `max_ms` is capped.
- `deadline_ms` small enough that the second attempt does not start.
- Deadline versus a trickle: the upstream writes one byte every 900 ms forever. The client stops at `deadline_ms` and `meta.deadline_exceeded` is set. A single `set_timeouts` at the start of the attempt would not.
- Hedge: slow peer sleeps past `delay`, fast peer returns 200, `meta.hedged` and `winner == 2`, the winner slot is `is_hedge`, `hedges_won` incremented. A later sequential retry that wins does not increment `hedges_won`. With `< min_samples` and `delay = "p95"`, no hedge.
- After a hedge win, attempt 1's censored latency is in the histogram and the killed hedge is not.
- Loser connection not reused (constraint 4 test above). The slow listener sees the reset; the next request is `$connection_requests == 1`.
- Client called from `log_by_lua_block` returns the phase error and does not crash.
- Dict filled until `incr` is forcible / `"no memory"`: default client still retries (fail open) and the warning is rate-limited.

### Benchmark (`bench/`)

`nginx.conf` + wrk. Upstream fails 50% with 503.

- Naive: the same single placeholder peer, and a balancer file that calls `set_more_tries(1)` while `ngx.ctx` attempt `< 3`. No budget. One `server` line starts `peer.tries` at 1, so "three tries" does not happen unless something arms them. Three identical `server` lines would also start `tries` at 3; the bench uses the explicit arm so the only difference from the library config is the budget and the cap.
- Library: ratio `0.2`, `MAX_ATTEMPTS = 3`, `proxy_next_upstream_tries 3`.

Amplification = upstream requests / client requests. Target for the library: `<= 1.2 + floor/client_rps`. The floor dominates at low RPS; the run uses enough RPS that `0.2 * requests` is the binding term, and the README states the floor term next to the number.

Happy path: a `content_by_lua` loop, `max_attempts = 1`, no hedge, fast upstream, `ngx.now()` around N calls, compared with raw `request_uri`. Target added p50 `< 20 µs`. This path is two dict incrs (`record_request`, histogram `record`) plus a Lua wrapper. Results go in `bench/README.md` as measured, including a miss against the target if that is what the machine shows.

## Packaging

BSD 2-clause `LICENSE`. `dist.ini` for OPM. A rockspec with the same version. `Makefile` targets: `lint` (luacheck + `GSET` check), `test` (busted), `t` (Test::Nginx, skipped with a clear message if `openresty` or `prove` is absent), `bench`. `.luacheckrc` uses `std = "ngx_lua"` and busted std for `spec/`. `CHANGELOG.md` records 0.1.0. Version constant `_VERSION = "0.1.0"`.

## Implementation order

Shipped in this order.

1. `budget` + `histogram` + `backoff` + `deadline`, with busted tests.
2. Client sequential retries, then hedge, each with the Test::Nginx files that cover them.
3. Prometheus helper, balancer recipe in the README, Redis example, benchmark, changelog.

No `TODO` and no stubbed success paths in the shipped Lua. Test doubles live under `spec/` only.
