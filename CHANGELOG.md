# Changelog

## 0.1.0 - 2026-09-22

- Sliding-window retry budget in a `lua_shared_dict`, shared by every worker.
- HTTP client that retries idempotent requests through `lua-resty-http`, with full-jitter backoff, `Retry-After`, and a deadline.
- Hedged requests drawn from the same budget.
- Log-linear latency histogram with `percentile(95)`.
- Optional gauges for `nginx-lua-prometheus`, labeled by budget name and shared-dict zone. An expired percentile is set to NaN.
- The request deadline starts at `request_uri` and applies to every retry and hedge.
- Interim 1xx responses are consumed before the final response is read. 101 closes the connection.
- Name registration finishes the shared index after the lock was busy.
