# tiny-rate

> Zero-dependency rate limiter for Python. Four algorithms — sync + async, decorator-friendly, now with Redis and Prometheus metrics.

```bash
pip install tiny-rate
```

![version 0.2.0](https://img.shields.io/badge/version-0.2.0-blue)

## What's new in v0.2

- **LeakyBucket** — constant drain rate, no burst. Perfect for connection pools and concurrent request gates.
- **RedisBackend** — distributed rate limiting across multiple processes/hosts via Lua scripts. Token-bucket and sliding-window in Redis.
- **RateMetrics** — Prometheus-compatible hit/miss/error counters and wait-time tracking.

## Why?

Every API needs rate limiting. The standard options are all heavy:

- **`limits`** — 5 deps, 600 KB
- **`asyncio-throttle`** — async-only, no sync
- **`aiolimiter`** — async-only, single algorithm
- **`slowapi`** — wraps `limits`, FastAPI-specific

**tiny-rate** is a single file with four algorithms and one decorator, sync or async, ~600 LOC.

## Algorithms

| Algorithm | Burst | Smoothness | Distributed | Use when |
|-----------|-------|-----------|------------|---------|
| **TokenBucket** | ✅ (configurable) | Smooth | ✅ (Redis) | APIs with burst tolerance |
| **LeakyBucket** | ❌ (none) | Constant | ❌ | Connection pools, concurrency gates |
| **FixedWindow** | ❌ (2x at edge) | Stepped | ✅ (Redis) | Simple quotas, dashboards |
| **SlidingWindow** | ❌ (exact) | Exact | ✅ (Redis) | Strict SLOs, audit logs |

All four support **sync and async**, **thread-safe**, and ship a **decorator** form.

## Usage

### Token bucket (recommended)

```python
import tiny_rate as tr

bucket = tr.TokenBucket(rate=100, capacity=100)  # 100 req/s, burst 100

if bucket.try_acquire():
    handle_request()
else:
    return 429

bucket.acquire()  # blocks until a token is available
```

### Leaky bucket — connection pool

```python
import tiny_rate as tr

lb = tr.LeakyBucket(rate=50, capacity=200)  # 50 conn/s, max 200 concurrent

if lb.try_acquire():
    conn = get_connection()
    try:
        handle(conn)
    finally:
        lb.release()
else:
    raise RateLimitExceeded(1 / lb.rate)
```

### Redis — distributed rate limiting

```python
import tiny_rate as tr

backend = tr.RedisBackend("redis://localhost:6379/0")

# Token bucket in Redis (atomic, across all workers)
allowed = backend.acquire("user:42", limit=100, window=60.0, algorithm="token")

# Sliding window in Redis (exact across all instances)
allowed = await backend.aacquire("api:global", limit=1000, window=60.0, algorithm="sliding")
```

### Prometheus metrics

```python
import tiny_rate as tr

metrics = tr.RateMetrics()
bucket = tr.TokenBucket(rate=10, capacity=10)

for req in requests:
    if bucket.try_acquire():
        metrics.hit(wait_us=0)
        handle(req)
    else:
        metrics.miss()
        reject(req)

# Expose on /metrics endpoint
print(metrics.prometheus())
# ratelimit_hits_total 982
# ratelimit_misses_total 18
# ratelimit_hit_rate 0.982000
```

### Decorator

```python
import tiny_rate as tr

@tr.limit("5/s", timeout=0.0)  # raise immediately on rejection
def call_api():
    return requests.get("https://api.example.com/data")

@tr.alimit("100/min", algorithm="sliding")
async def heavy_endpoint():
    return await db.query(...)
```

### Parse any rate string

```python
tr.parse_rate("100/s")       # (100.0, 1.0)
tr.parse_rate("5_000/min")   # (5000.0, 60.0)
tr.parse_rate("10/h")        # (10.0, 3600.0)
tr.parse_rate(("1000", "min"))  # tuple form
```

### Async

```python
bucket = tr.TokenBucket(rate=10, capacity=10)
await bucket.aacquire(timeout=1.0)  # waits up to 1s
```

## Rate format

`"<count>/<unit>"` where unit is `s/sec/second(s)`, `m/min/minute(s)`, `h/hr/hour(s)`, or `d/day(s)`. Underscores in numbers are ignored (`5_000` == `5000`).

## API

| Class / function | Description |
|------------------|-------------|
| `TokenBucket(rate, capacity)` | Smooth, supports burst |
| `LeakyBucket(rate, capacity)` | Constant drain, no burst, concurrency gate |
| `FixedWindow(rate, period)` | Counter per window |
| `SlidingWindow(rate, period)` | Exact log of recent timestamps |
| `RedisBackend(redis_url, key)` | Distributed limiter via Redis Lua scripts |
| `RateMetrics()` | Prometheus-compatible counters |
| `RateLimitExceeded` | Exception with `.retry_after` |
| `parse_rate(spec)` | Parse `"N/period"` strings |
| `limit(spec, algorithm, timeout)` | Sync decorator |
| `alimit(spec, algorithm, timeout)` | Async decorator |

## Performance

Measured on a single thread, 200K iterations:

```
TokenBucket.try_acquire (saturated)   2.74 µs/op   ~370K ops/s
TokenBucket.try_acquire (high-rate)   1.39 µs/op   ~720K ops/s
FixedWindow.try_acquire               0.66 µs/op   ~1.5M ops/s
SlidingWindow.try_acquire             1.35 µs/op   ~740K ops/s
LeakyBucket.try_acquire               1.12 µs/op   ~890K ops/s
parse_rate('100/s')                   0.99 µs/op   ~1.0M ops/s
```

## Ecosystem

Part of the **tiny-*** zero-dep stack by [OpenClaw](https://github.com/hussain-alsaibai):

| Repo | What |
|------|------|
| [tiny-router](https://github.com/hussain-alsaibai/tiny-router) | HTTP routing, 76K req/s |
| [tiny-log](https://github.com/hussain-alsaibai/tiny-log) | Structured logs, 32K logs/s |
| [tiny-validator](https://github.com/hussain-alsaibai/tiny-validator) | Input validation, 247K val/s |
| [tiny-config](https://github.com/hussain-alsaibai/tiny-config) | Layered config loader |
| [tiny-cli](https://github.com/hussain-alsaibai/tiny-cli) | CLI builder with colors |
| [fast-cache](https://github.com/hussain-alsaibai/fast-cache) | LRU+TTL+SWR cache |
| [tiny-retry](https://github.com/hussain-alsaibai/tiny-retry) | Retry + backoff + circuit breaker |
| [tiny-pool](https://github.com/hussain-alsaibai/tiny-pool) | Thread / async worker pools |
| [tiny-agent](https://github.com/hussain-alsaibai/tiny-agent) | Zero-dep agent framework |
| [tiny-mcp](https://github.com/hussain-alsaibai/tiny-mcp) | Model Context Protocol server |
| [tiny-embed](https://github.com/hussain-alsaibai/tiny-embed) | Embeddings + vector search |
| [tiny-compose](https://github.com/hussain-alsaibai/tiny-compose) | Stack any decorators in any order, declaratively |
| [tiny-trace](https://github.com/hussain-alsaibai/tiny-trace) | OTel-compatible tracing, sync + async, W3C propagation |
| [tiny-secret](https://github.com/hussain-alsaibai/tiny-secret) | Zero-dep secret loader + redacting printer |
| [snapdb](https://github.com/hussain-alsaibai/snapdb) | Embedded DB (Python) |

**Total: 18 repos, ~14K LOC, 0 dependencies, ~440 tests.**

## License

MIT © 2026 OpenClaw (hussain-alsaibai)

## Today's siblings

- [`tiny-metrics`](https://github.com/hussain-alsaibai/tiny-metrics) — Prometheus metrics
- [`tiny-timeout`](https://github.com/hussain-alsaibai/tiny-timeout) — timeouts that work
- [`tiny-idempotency`](https://github.com/hussain-alsaibai/tiny-idempotency) — idempotency keys
