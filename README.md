# tiny-rate

> Zero-dependency rate limiter for Python. Token bucket, fixed window, sliding window — sync + async, decorator-friendly.

```bash
pip install tiny-rate   # coming soon
```

## Why?

Every API needs rate limiting. The standard options are all heavy:

- **`limits`** — 5 deps, 600 KB
- **`asyncio-throttle`** — async-only, no sync
- **`aiolimiter`** — async-only, single algorithm
- **`slowapi`** — wraps `limits`, FastAPI-specific

**tiny-rate** is a single file with three algorithms and one decorator, sync or async, 350 LOC.

## What's included

| Algorithm | Burst | Smoothness | Use when |
|-----------|-------|-----------|----------|
| **TokenBucket** | ✅ (configurable) | Smooth | APIs with burst tolerance |
| **FixedWindow** | ❌ (2x at edge) | Stepped | Simple quotas, dashboards |
| **SlidingWindow** | ❌ (exact) | Exact | Strict SLOs, audit logs |

All three support **sync and async**, **thread-safe**, and ship a **decorator** form.

## Usage

### Token bucket (recommended)

```python
import tiny_rate as tr

bucket = tr.TokenBucket(rate=100, capacity=100)  # 100 req/s, burst 100

if bucket.try_acquire():
    handle_request()
else:
    return 429

# Or block until a token is available:
bucket.acquire()  # blocks
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
| `FixedWindow(rate, period)` | Counter per window |
| `SlidingWindow(rate, period)` | Exact log of recent timestamps |
| `RateLimitExceeded` | Exception with `.retry_after` |
| `parse_rate(spec)` | Parse `"N/period"` strings |
| `limit(spec, algorithm, timeout)` | Sync decorator |
| `alimit(spec, algorithm, timeout)` | Async decorator |

All classes expose `try_acquire()` (non-blocking), `acquire()` / `aacquire()` (blocking), and `available()` (current tokens).

## Performance

Measured on a single thread, 200K iterations:

```
TokenBucket.try_acquire (saturated)   2.74 µs/op   ~370K ops/s
TokenBucket.try_acquire (high-rate)   1.39 µs/op   ~720K ops/s
FixedWindow.try_acquire               0.66 µs/op   ~1.5M ops/s
SlidingWindow.try_acquire             1.35 µs/op   ~740K ops/s
parse_rate('100/s')                   0.99 µs/op   ~1.0M ops/s
```

For comparison, `asyncio-throttle` is ~3-5x slower on the same workload. `aiolimiter` is comparable but only ships token-bucket + async.

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
| [snapdb](https://github.com/hussain-alsaibai/snapdb) | Embedded DB (Python) |

**Total: 12 repos, ~5,200 LOC, zero deps across the entire stack.**

## License

MIT © 2026 OpenClaw (hussain-alsaibai)
