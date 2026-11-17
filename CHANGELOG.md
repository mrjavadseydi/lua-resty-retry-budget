# Changelog

## 0.2.0 - 2026-11-18

- Strict HTTP response framing: validate status lines, header field names, `Content-Length` lists/bounds, and `Transfer-Encoding` codings and chunk terminators; trim header OWS.
- `Expect: 100-continue` support with case-insensitive header normalization, deferred body writes after `100`, and keepalive closure when a final response rejects an unsent body.
- Hardened request deadlines: cancel trickled headers and yielding upload streams via light-thread deadline timers, preserve previous HTTP responses when retries hit the deadline, handle IEEE-754 timestamp rounding, and record censored tail latencies once per started attempt.
- Hedged request fixes: support per-call `hedge.min_samples` below `estimator.min_samples`, keep subsequent hedge timers armed when `max_hedges > 1`, and prefer retryable/HTTP fallback responses over earlier transport errors.
- Forward `lua-resty-http` TLS/connection options (`ssl_client_cert`, `ssl_client_priv_key`, `ssl_session_reuse`, `backlog`, `proxy_opts`) and per-call `keepalive` settings.
- Rate-limited logger checks per-dict worker suppression before touching `lua_shared_dict` to prevent log floods and slab eviction churn on full dicts.
- Cold-worker budget discovery via `rb.stats(zones)` and `prometheus.register(prom, { dict = ... })`, plus `retry_budget_allowance_window` gauge.
- Hot-path optimizations: eliminate shared-dict reads on write-only budget calls, split enforcement counters (`q`, `t`) from stats counters, cache closed histogram slots, arm histogram refresh timers lazily with idle expiry, and use LuaJIT JIT-friendly HTTP parsing.

## 0.1.0 - 2026-09-22

- Sliding-window retry budget in a `lua_shared_dict`, shared by every worker.
- HTTP client that retries idempotent requests through `lua-resty-http`, with full-jitter backoff, `Retry-After`, and a deadline.
- Hedged requests drawn from the same budget.
- Log-linear latency histogram with `percentile(95)`.
- Optional gauges for `nginx-lua-prometheus`, labeled by budget name and shared-dict zone. An expired percentile is set to NaN.
- The request deadline starts at `request_uri` and applies to every retry and hedge.
- Interim 1xx responses are consumed before the final response is read. 101 closes the connection.
- Name registration finishes the shared index after the lock was busy.
