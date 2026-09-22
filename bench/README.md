# Benchmark

`make bench` runs this directory inside Docker. The image is
`openresty/openresty:1.31.1.1-3-jammy` unless `OPENRESTY_IMAGE` is set.
`WARMUP` (seconds), `DURATION`, `THREADS`, and `CONNS` override the wrk
settings. The container installs `wrk` and `ledgetech/lua-resty-http` 0.17.1
on each run.

## Setup

`bench/nginx.conf` listens on `127.0.0.1:8080` and proxies to a single
placeholder, `server 0.0.0.1`. The balancer sets the peer to
`127.0.0.1:1986`. One `server` line starts `peer.tries` at 1, so a retry
happens only when the balancer calls `set_more_tries(1)`. Both paths cap
that at three attempts, and nginx has `proxy_next_upstream_tries 3` as the
same backstop. `proxy_next_upstream` lists `error`, `http_502`, `http_503`,
and `http_504`. The list omits timeout, matching the client.

The upstream answers 503 when its request counter is odd and 200 when it is
even, so half of upstream responses fail. Concurrent clients share that
stream, so a failure on one request consumes the next slot that another
request might have seen.

`bench/naive.lua` arms the next try whenever the attempt number is under 3.
`bench/budget.lua` is the balancer recipe from the project README:
`retry_ratio` 0.2, `min_retries_per_sec` 10, `window_sec` 10, one budget
object per worker. `worker_processes` is 2.

Amplification is upstream responses divided by client requests. Both
counters live in a shared dict. The budget dict is a different zone and
stays intact across the warmup, so the 10 second window is already full
when timing starts. Warmup is 12 seconds against `/budget`. Each case then
resets only the bench counters.

The target for the library is `1.2 + min_retries_per_sec / client_rps`.
At a few requests per second the floor of 10 retries/s is the allowance.
This run is fast enough that the allowance is `0.2 * requests`.

## Result

2026-09-22. Container reported one processor. wrk `-t2 -c64 -d15s`.

| path | client requests | upstream responses | amplification | client rps | client non-2xx |
| --- | --- | --- | --- | --- | --- |
| budget | 540279 | 648321 | 1.19997 | 35989 | 40.00% |
| naive | 330925 | 596987 | 1.8040 | 22044 | 9.79% |

Client rps is the nginx client counter divided by wrk's duration.
Non-2xx is wrk's view of the status line (`bench/wrk.lua`). Budget wrk
recorded 2 timeouts in 540217 completed requests.

Budget target at 35989 rps: `1.2 + 10/35989` = 1.2003. Measured
amplification is 1.19997.

The budget's own window, read at the end of its 15 second run:

| requests | retries | allowance | rejected |
| --- | --- | --- | --- |
| 337642 | 67532 | 67528.4 | 37578 |

Allowance is `0.2 * 337642`. The floor of 100 sits under that. Retries finished 4 above
that allowance. That is the soft shared-dict check (workers can pass the
same read) plus the one retry nginx had already armed with `can_acquire`
before the failure was known. Over the whole 15 seconds the extra upstream
calls were 0.19997 per client request.

Attempt numbers seen by the balancer. Nothing reached 4, so `ngx.ctx` kept
the count across re-entry and both caps held.

| path | attempt 1 | attempt 2 | attempt 3 |
| --- | --- | --- | --- |
| budget | 540279 | 95432 | 12613 |
| naive | 330925 | 191082 | 74983 |

The budget armed 213488 future tries and then refused 61784 of the retries
nginx had already committed to. Those refused sends still reached the
upstream and were counted with `record_retry`. They are included in the
amplification above.

An earlier run the same day, same image and wrk flags, measured budget
amplification 1.20001 and naive 1.8059.

## Happy path

`GET /happy` (`bench/happy.lua`) calls the library client with
`max_attempts = 1` and no hedge, against `/fast` on the same upstream, and
compares it with `lua-resty-http` `request_uri`. Both use a 60 second
keepalive. The client is built once. 300 calls warm the pool, then 50
bursts of 200 calls, library and raw interleaved.

`ngx.now()` advances in milliseconds, so each sample is the mean of the
burst: one millisecond is 5 µs per call. A real 5 µs of overhead on every
call would add a full millisecond to the burst and move the p50.

| | p50 µs | mean µs | p99 µs |
| --- | --- | --- | --- |
| library client | 15.000 | 16.600 | 20.001 |
| raw `request_uri` | 15.000 | 16.600 | 20.001 |
| added | 0 | 0 | 0 |

Added p50 is under the 20 µs target. The same measurement a few minutes
earlier had an added mean of 0.7 µs and an added p50 of 0, which is the
same result at this clock.
