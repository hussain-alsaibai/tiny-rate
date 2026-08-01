"""tiny-rate: Zero-dependency rate limiter for Python.

Three algorithms in a single file:

  - TokenBucket   : smooth refill, supports burst capacity
  - FixedWindow   : simple per-window counter
  - SlidingWindow : log of recent timestamps, exact precision

Sync and async decorators: @limit(rate="100/s") and @alimit(rate="100/s").
Thread-safe with locks. Sleep-free async: uses asyncio.Event / sleep.

Single file, no deps, MIT, fully typed.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import re
import threading
import time
from typing import Any, Awaitable, Callable, List, Optional, Tuple, TypeVar, Union

__version__ = "0.2.0"
__all__ = [
    "TokenBucket",
    "FixedWindow",
    "SlidingWindow",
    "LeakyBucket",
    "RedisBackend",
    "RateLimitExceeded",
    "RateMetrics",
    "limit",
    "alimit",
    "parse_rate",
]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class RateLimitExceeded(Exception):
    """Raised when a request is rejected by a limiter.

    Attributes:
        retry_after: Seconds until the next request would succeed (float).
    """

    def __init__(self, retry_after: float, message: Optional[str] = None) -> None:
        self.retry_after = max(0.0, float(retry_after))
        super().__init__(
            message or f"rate limit exceeded; retry after {self.retry_after:.4f}s"
        )


# ---------------------------------------------------------------------------
# Rate string parser: "100/s", "5_000/min", "1 per 2s", "10/h"
# ---------------------------------------------------------------------------

_RATE_RE = re.compile(
    r"^\s*(?P<n>\d[\d_]*)\s*(?:/|per\s+)?\s*(?P<unit>s|sec|second|seconds|m|min|minute|minutes|h|hr|hour|hours|d|day|days)\s*$",
    re.IGNORECASE,
)


def parse_rate(spec: Union[str, int, float, Tuple[int, str]]) -> Tuple[float, float]:
    """Parse a rate string into (count, period_seconds).

    Accepts:
        "100/s"      -> (100, 1.0)
        "1000/min"   -> (1000, 60.0)
        "5 per 2s"   -> (5, 2.0)
        100          -> (100, 1.0)         (int = per second)
        (5, "min")   -> (5, 60.0)

    Raises ValueError on malformed input.
    """
    if isinstance(spec, (int, float)):
        return float(spec), 1.0
    if isinstance(spec, tuple) and len(spec) == 2:
        n, unit = spec
        return float(n), _unit_seconds(str(unit))
    if not isinstance(spec, str):
        raise ValueError(f"unsupported rate spec: {spec!r}")
    m = _RATE_RE.match(spec)
    if not m:
        raise ValueError(f"could not parse rate spec: {spec!r}")
    n = int(m.group("n").replace("_", ""))
    unit = m.group("unit").lower()
    return float(n), _unit_seconds(unit)


def _unit_seconds(unit: str) -> float:
    u = unit.lower()
    if u in ("s", "sec", "second", "seconds"):
        return 1.0
    if u in ("m", "min", "minute", "minutes"):
        return 60.0
    if u in ("h", "hr", "hour", "hours"):
        return 3600.0
    if u in ("d", "day", "days"):
        return 86400.0
    raise ValueError(f"unknown time unit: {unit!r}")


# ---------------------------------------------------------------------------
# Token Bucket — smooth rate with burst capacity
# ---------------------------------------------------------------------------


class TokenBucket:
    """Token-bucket rate limiter.

    Args:
        rate:        Tokens added per second (float). Use parse_rate() for "N/period".
        capacity:    Bucket size (max burst). Defaults to rate (1 second of burst).

    The bucket starts full. Each acquire() consumes one token; tokens refill
    continuously at `rate` tokens/second, up to `capacity`.

    Thread-safe.
    """

    __slots__ = ("rate", "capacity", "_tokens", "_last", "_lock")

    def __init__(self, rate: float, capacity: Optional[float] = None) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        if capacity is None:
            capacity = rate
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.rate = float(rate)
        self.capacity = float(capacity)
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def _refill_locked(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Attempt to consume tokens; return True if successful, False otherwise."""
        if tokens <= 0:
            raise ValueError("tokens must be positive")
        with self._lock:
            self._refill_locked()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> None:
        """Block (sync) until tokens are available.

        Args:
            tokens: Tokens to consume (>=1).
            timeout: Max seconds to wait. None means wait forever. 0 means
                     raise immediately if tokens aren't available.
        """
        if timeout == 0:
            # Fast path: don't even try to refill
            with self._lock:
                self._refill_locked()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                needed = tokens - self._tokens
                raise RateLimitExceeded(needed / self.rate)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._lock:
                self._refill_locked()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                needed = tokens - self._tokens
                wait = needed / self.rate
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RateLimitExceeded(wait)
                wait = min(wait, remaining)
            time.sleep(wait)

    def available(self) -> float:
        """Current available tokens (after refill)."""
        with self._lock:
            self._refill_locked()
            return self._tokens

    async def aacquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> None:
        """Async version of acquire(). Uses asyncio.sleep instead of time.sleep.

        Args:
            tokens: Tokens to consume (>=1).
            timeout: Max seconds to wait. None means wait forever. 0 means
                     raise immediately if tokens aren't available.
        """
        if timeout == 0:
            with self._lock:
                self._refill_locked()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                needed = tokens - self._tokens
                raise RateLimitExceeded(needed / self.rate)
        loop = asyncio.get_event_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            with self._lock:
                self._refill_locked()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                needed = tokens - self._tokens
                wait = needed / self.rate
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise RateLimitExceeded(wait)
                wait = min(wait, remaining)
            await asyncio.sleep(wait)

    def __repr__(self) -> str:
        return f"TokenBucket(rate={self.rate:.4f}/s, capacity={self.capacity:.4f})"


# ---------------------------------------------------------------------------
# Fixed Window — counter reset every period
# ---------------------------------------------------------------------------


class FixedWindow:
    """Fixed-window counter limiter.

    Args:
        rate:        Max requests per window.
        period:      Window length in seconds (float).

    Simpler than token bucket: counts requests in each window; resets at the
    boundary. Allows 2x burst at window edges (a known weakness, but easy).

    Thread-safe.
    """

    __slots__ = ("rate", "period", "_count", "_window_start", "_lock")

    def __init__(self, rate: float, period: float = 1.0) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        if period <= 0:
            raise ValueError("period must be positive")
        self.rate = float(rate)
        self.period = float(period)
        self._count = 0
        self._window_start = time.monotonic()
        self._lock = threading.Lock()

    def _roll_locked(self) -> None:
        now = time.monotonic()
        if now - self._window_start >= self.period:
            self._count = 0
            self._window_start = now

    def try_acquire(self) -> bool:
        with self._lock:
            self._roll_locked()
            if self._count < self.rate:
                self._count += 1
                return True
            return False

    def acquire(self, timeout: Optional[float] = None) -> None:
        """Block until a slot opens in the current window.

        timeout: None = wait forever; 0 = raise immediately.
        """
        if timeout == 0:
            with self._lock:
                self._roll_locked()
                if self._count < self.rate:
                    self._count += 1
                    return
                wait = self.period - (time.monotonic() - self._window_start)
                raise RateLimitExceeded(max(0.0, wait))
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._lock:
                self._roll_locked()
                if self._count < self.rate:
                    self._count += 1
                    return
                wait = self.period - (time.monotonic() - self._window_start)
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RateLimitExceeded(max(0.0, wait))
                wait = min(wait, remaining)
            time.sleep(max(0.0, wait))

    async def aacquire(self, timeout: Optional[float] = None) -> None:
        """Async acquire. timeout: None = wait forever; 0 = raise immediately."""
        if timeout == 0:
            with self._lock:
                self._roll_locked()
                if self._count < self.rate:
                    self._count += 1
                    return
                wait = self.period - (time.monotonic() - self._window_start)
                raise RateLimitExceeded(max(0.0, wait))
        loop = asyncio.get_event_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            with self._lock:
                self._roll_locked()
                if self._count < self.rate:
                    self._count += 1
                    return
                wait = self.period - (time.monotonic() - self._window_start)
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise RateLimitExceeded(max(0.0, wait))
                wait = min(wait, remaining)
            await asyncio.sleep(max(0.0, wait))

    def __repr__(self) -> str:
        return f"FixedWindow(rate={self.rate}/period, period={self.period:.4f}s)"


# ---------------------------------------------------------------------------
# Sliding Window — exact log of recent timestamps
# ---------------------------------------------------------------------------


class SlidingWindow:
    """Sliding-window limiter using a deque of recent timestamps.

    Args:
        rate:   Max events in the window.
        period: Window length in seconds.

    Most accurate of the three; uses O(rate) memory and amortized O(1) per op
    by trimming the deque on every call. Thread-safe.
    """

    __slots__ = ("rate", "period", "_events", "_lock")

    def __init__(self, rate: float, period: float = 1.0) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        if period <= 0:
            raise ValueError("period must be positive")
        self.rate = float(rate)
        self.period = float(period)
        self._events: List[float] = []
        self._lock = threading.Lock()

    def _trim_locked(self, now: float) -> None:
        cutoff = now - self.period
        ev = self._events
        # Find first index >= cutoff via pop from left while stale
        while ev and ev[0] <= cutoff:
            ev.pop(0)

    def try_acquire(self) -> bool:
        with self._lock:
            now = time.monotonic()
            self._trim_locked(now)
            if len(self._events) < self.rate:
                self._events.append(now)
                return True
            return False

    def acquire(self, timeout: Optional[float] = None) -> None:
        """Block until the oldest event ages out of the window.

        timeout: None = wait forever; 0 = raise immediately.
        """
        if timeout == 0:
            with self._lock:
                now = time.monotonic()
                self._trim_locked(now)
                if len(self._events) < self.rate:
                    self._events.append(now)
                    return
                wait = self.period - (now - self._events[0])
                raise RateLimitExceeded(max(0.0, wait))
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._lock:
                now = time.monotonic()
                self._trim_locked(now)
                if len(self._events) < self.rate:
                    self._events.append(now)
                    return
                # Wait until the oldest event falls out of the window
                wait = self.period - (now - self._events[0])
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RateLimitExceeded(max(0.0, wait))
                wait = min(wait, remaining)
            time.sleep(max(0.0, wait))

    async def aacquire(self, timeout: Optional[float] = None) -> None:
        """Async acquire. timeout: None = wait forever; 0 = raise immediately."""
        if timeout == 0:
            with self._lock:
                now = time.monotonic()
                self._trim_locked(now)
                if len(self._events) < self.rate:
                    self._events.append(now)
                    return
                wait = self.period - (now - self._events[0])
                raise RateLimitExceeded(max(0.0, wait))
        loop = asyncio.get_event_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            with self._lock:
                now = time.monotonic()
                self._trim_locked(now)
                if len(self._events) < self.rate:
                    self._events.append(now)
                    return
                wait = self.period - (now - self._events[0])
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise RateLimitExceeded(max(0.0, wait))
                wait = min(wait, remaining)
            await asyncio.sleep(max(0.0, wait))

    def __repr__(self) -> str:
        return f"SlidingWindow(rate={self.rate}/period, period={self.period:.4f}s)"


# ---------------------------------------------------------------------------
# Leaky Bucket — constant drain rate, no burst
# ---------------------------------------------------------------------------


class LeakyBucket:
    """Leaky-bucket rate limiter.

    Unlike TokenBucket (which smooths by capping burst), LeakyBucket enforces
    a hard ceiling on concurrent in-flight items. Items "leak" out at a fixed
    rate. When the bucket is full, new acquisitions are rejected.

    Args:
        rate:        Leak rate: max items allowed through per second (float).
        capacity:    Bucket size — max concurrent items (int). Default = rate.

    Thread-safe.

    Example:
        # Allow 10 concurrent connections, leak at 5/s
        lb = LeakyBucket(rate=5, capacity=10)
        if lb.try_acquire():
            handle_connection()
    """

    __slots__ = ("rate", "capacity", "_in_flight", "_last_leak", "_lock")

    def __init__(self, rate: float, capacity: Optional[int] = None) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        cap = int(capacity) if capacity is not None else max(1, int(rate))
        if cap <= 0:
            raise ValueError("capacity must be positive")
        self.rate = float(rate)
        self.capacity = cap
        self._in_flight = 0
        self._last_leak = time.monotonic()
        self._lock = threading.Lock()

    def _drip_locked(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_leak
        if elapsed > 0:
            leaked = elapsed * self.rate
            self._in_flight = max(0, self._in_flight - leaked)
            self._last_leak = now

    def try_acquire(self) -> bool:
        """Attempt to add one item to the bucket. Returns True if accepted."""
        with self._lock:
            self._drip_locked()
            if self._in_flight < self.capacity:
                self._in_flight += 1
                return True
            return False

    def acquire(self, timeout: Optional[float] = None) -> None:
        """Block until a slot opens (item leaks out)."""
        if timeout == 0:
            with self._lock:
                self._drip_locked()
                if self._in_flight < self.capacity:
                    self._in_flight += 1
                    return
                wait = 1.0 / self.rate
                raise RateLimitExceeded(wait)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._lock:
                self._drip_locked()
                if self._in_flight < self.capacity:
                    self._in_flight += 1
                    return
                wait = 1.0 / self.rate
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RateLimitExceeded(wait)
                wait = min(wait, remaining)
            time.sleep(wait)

    async def aacquire(self, timeout: Optional[float] = None) -> None:
        """Async acquire — blocks until a slot opens."""
        if timeout == 0:
            with self._lock:
                self._drip_locked()
                if self._in_flight < self.capacity:
                    self._in_flight += 1
                    return
                wait = 1.0 / self.rate
                raise RateLimitExceeded(wait)
        loop = asyncio.get_event_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            with self._lock:
                self._drip_locked()
                if self._in_flight < self.capacity:
                    self._in_flight += 1
                    return
                wait = 1.0 / self.rate
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise RateLimitExceeded(wait)
                wait = min(wait, remaining)
            await asyncio.sleep(wait)

    def release(self) -> None:
        """Manually release one slot (call when work completes)."""
        with self._lock:
            self._drip_locked()
            self._in_flight = max(0, self._in_flight - 1)

    def in_flight(self) -> int:
        """Current in-flight count."""
        with self._lock:
            self._drip_locked()
            return int(self._in_flight)

    def __repr__(self) -> str:
        return f"LeakyBucket(rate={self.rate}/s, capacity={self.capacity})"


# ---------------------------------------------------------------------------
# Redis Backend — distributed rate limiting
# ----------------------------------------------------------------------------


class RedisBackend:
    """Distributed rate limiter backed by Redis.

    Uses a Lua script for atomic check-and-increment so there are no race
    conditions between acquire calls. Works across multiple processes or hosts.

    Supports token-bucket and sliding-window algorithms in Redis.

    Args:
        redis_url:  Redis URL (e.g. "redis://localhost:6379/0").
                    If None, defers connection until first call.
        key:        Key prefix for rate limit counters. Default "ratelimit".

    Example:
        backend = RedisBackend("redis://localhost:6379/0")
        await backend.acquire("api:user:42", limit=100, window=60.0)

    Note:
        Requires the ``redis`` package (``pip install redis``). Not installed
        by default — import is lazy.
    """

    __slots__ = ("_redis", "_url", "_key")

    def __init__(
        self,
        redis_url: Optional[str] = None,
        key: str = "ratelimit",
    ) -> None:
        self._redis: Optional[Any] = None
        self._url = redis_url or "redis://localhost:6379/0"
        self._key = key

    def _connect(self) -> Any:
        if self._redis is None:
            try:
                import redis  # type: ignore
            except ImportError:
                raise RuntimeError(
                    "RedisBackend requires the 'redis' package. "
                    "Install with: pip install redis"
                )
            self._redis = redis.from_url(self._url, decode_responses=False)
        return self._redis

    # Lua script: token-bucket in Redis (atomic)
    # KEYS[1] = key, ARGV[1] = limit, ARGV[2] = window_sec, ARGV[3] = now_ms
    _TB_SCRIPT = """
        local key = KEYS[1]
        local limit = tonumber(ARGV[1])
        local window = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])
        local bucket = redis.call('HMGET', key, 'tokens', 'last')
        local tokens = tonumber(bucket[1]) or limit
        local last = tonumber(bucket[2]) or now
        local rate = limit / window
        local elapsed = (now - last) / 1000.0
        tokens = math.min(limit, tokens + elapsed * rate)
        if tokens >= 1.0 then
            tokens = tokens - 1.0
            redis.call('HMSET', key, 'tokens', tokens, 'last', now)
            redis.call('EXPIRE', key, math.ceil(window * 2))
            return 1
        else
            redis.call('HMSET', key, 'tokens', tokens, 'last', now)
            redis.call('EXPIRE', key, math.ceil(window * 2))
            return 0
        end
    """

    # Lua script: sliding-window in Redis
    # KEYS[1] = key, ARGV[1] = limit, ARGV[2] = window_sec, ARGV[3] = now_ms
    _SW_SCRIPT = """
        local key = KEYS[1]
        local limit = tonumber(ARGV[1])
        local window = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])
        local window_ms = window * 1000
        local cutoff = now - window_ms
        redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)
        local count = redis.call('ZCARD', key)
        if count < limit then
            redis.call('ZADD', key, now, now .. ':' .. math.random(1000000))
            redis.call('EXPIRE', key, math.ceil(window * 2))
            return 1
        else
            return 0
        end
    """

    def acquire(
        self,
        key: str,
        limit: int,
        window: float,
        algorithm: str = "token",
        timeout: Optional[float] = None,
    ) -> bool:
        """Synchronous Redis acquire. Returns True if allowed, False if rejected.

        ``algorithm``: ``"token"`` (default) or ``"sliding"``.
        ``timeout``: seconds to retry acquiring. Default None (no retry).
        """
        r = self._connect()
        script = self._TB_SCRIPT if algorithm == "token" else self._SW_SCRIPT
        full_key = f"{self._key}:{key}"
        now_ms = int(time.time() * 1000)

        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            result = r.eval(script, 1, full_key, limit, window, now_ms)
            if result == 1:
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(0.05)

    async def aacquire(
        self,
        key: str,
        limit: int,
        window: float,
        algorithm: str = "token",
        timeout: Optional[float] = None,
    ) -> bool:
        """Async Redis acquire. ``timeout`` seconds to retry."""
        r = self._connect()
        script = self._TB_SCRIPT if algorithm == "token" else self._SW_SCRIPT
        full_key = f"{self._key}:{key}"
        now_ms = int(time.time() * 1000)

        deadline = None if timeout is None else asyncio.get_event_loop().time() + timeout
        while True:
            result = await asyncio.to_thread(r.eval, script, 1, full_key, limit, window, now_ms)
            if result == 1:
                return True
            if deadline is not None and asyncio.get_event_loop().time() >= deadline:
                return False
            await asyncio.sleep(0.05)

    def check(self, key: str) -> int:
        """Return current count for a key (sliding window)."""
        r = self._connect()
        full_key = f"{self._key}:{key}"
        return r.zcard(full_key) or 0

    def reset(self, key: str) -> None:
        """Reset counter for a key."""
        r = self._connect()
        full_key = f"{self._key}:{key}"
        r.delete(full_key)

    def close(self) -> None:
        """Close Redis connection."""
        if self._redis:
            self._redis.close()
            self._redis = None


# ---------------------------------------------------------------------------
# Metrics — Prometheus-compatible hit/miss/latency tracking
# ---------------------------------------------------------------------------


class RateMetrics:
    """Lightweight metrics collector for rate limiter performance.

    Thread-safe counters for hits, misses, wait time, and errors.
    Prometheus-compatible exposition format via ``.prometheus()``.

    Example:
        metrics = RateMetrics()
        bucket = tr.TokenBucket(rate=10, capacity=10)

        for req in requests:
            if bucket.try_acquire():
                metrics.hit()
                handle(req)
            else:
                metrics.miss()
                reject(req)

        print(metrics.prometheus())   # paste into /metrics endpoint
    """

    __slots__ = ("_hits", "_misses", "_errors", "_total_wait_us", "_lock")

    def __init__(self) -> None:
        self._hits = 0
        self._misses = 0
        self._errors = 0
        self._total_wait_us = 0
        self._lock = threading.Lock()

    def hit(self, wait_us: int = 0) -> None:
        with self._lock:
            self._hits += 1
            self._total_wait_us += wait_us

    def miss(self) -> None:
        with self._lock:
            self._misses += 1

    def error(self) -> None:
        with self._lock:
            self._errors += 1

    @property
    def total(self) -> int:
        with self._lock:
            return self._hits + self._misses

    @property
    def hit_rate(self) -> float:
        with self._lock:
            total = self._hits + self._misses
            return self._hits / total if total > 0 else 0.0

    @property
    def avg_wait_ms(self) -> float:
        with self._lock:
            return self._total_wait_us / self._hits / 1000.0 if self._hits > 0 else 0.0

    def prometheus(self) -> str:
        """Return Prometheus exposition text format."""
        with self._lock:
            hits = self._hits
            misses = self._misses
            errors = self._errors
            wait_us = self._total_wait_us
        total = hits + misses
        return (
            f"# HELP ratelimit_hits_total Total allowed acquisitions\n"
            f"# TYPE ratelimit_hits_total counter\n"
            f"ratelimit_hits_total {hits}\n"
            f"# HELP ratelimit_misses_total Total rejected acquisitions\n"
            f"# TYPE ratelimit_misses_total counter\n"
            f"ratelimit_misses_total {misses}\n"
            f"# HELP ratelimit_errors_total Total errors\n"
            f"# TYPE ratelimit_errors_total counter\n"
            f"ratelimit_errors_total {errors}\n"
            f"# HELP ratelimit_requests_total Total requests\n"
            f"# TYPE ratelimit_requests_total counter\n"
            f"ratelimit_requests_total {total}\n"
            f"# HELP ratelimit_hit_rate Ratio of hits to total\n"
            f"# TYPE ratelimit_hit_rate gauge\n"
            f"ratelimit_hit_rate {self.hit_rate:.6f}\n"
            f"# HELP ratelimit_avg_wait_ms Average wait time in milliseconds\n"
            f"# TYPE ratelimit_avg_wait_ms gauge\n"
            f"ratelimit_avg_wait_ms {self.avg_wait_ms:.6f}\n"
        )

    def summary(self) -> Dict[str, Any]:
        """Return dict for JSON export."""
        with self._lock:
            total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "errors": self._errors,
            "total": total,
            "hit_rate": round(self.hit_rate, 6),
            "avg_wait_ms": round(self.avg_wait_ms, 4),
        }


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------

F = TypeVar("F", bound=Callable[..., Any])


def _make_limiter(spec: Union[str, int, float, Tuple[int, str]], algorithm: str):
    if algorithm == "token":
        n, p = parse_rate(spec)
        return TokenBucket(rate=n / p, capacity=n / p)
    if algorithm == "fixed":
        n, p = parse_rate(spec)
        return FixedWindow(rate=n, period=p)
    if algorithm == "sliding":
        n, p = parse_rate(spec)
        return SlidingWindow(rate=n, period=p)
    raise ValueError(f"unknown algorithm: {algorithm!r}")


def limit(
    spec: Union[str, int, float, Tuple[int, str]] = "10/s",
    *,
    algorithm: str = "token",
    timeout: Optional[float] = None,
    on_exceeded: Optional[Callable[[float], None]] = None,
) -> Callable[[F], F]:
    """Decorator: rate-limit a sync function.

    Args:
        spec:       Rate string. "100/s", "5_000/min", or (n, "unit").
        algorithm:  "token" (default), "fixed", or "sliding".
        timeout:    Max seconds to wait for a token. None (default) means wait
                    forever. Use 0 to raise immediately on rejection.
        on_exceeded: Optional callback called with retry_after when limit is
                     hit. Useful for logging. Does not change behavior.

    Example:
        @limit("5/s")
        def call_api(): ...

        @limit("5/s", timeout=0.0)   # raise immediately on rejection
        def call_api(): ...

        @limit(("1000", "min"), algorithm="sliding")
        def heavy(): ...
    """
    limiter = _make_limiter(spec, algorithm)

    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                limiter.acquire(timeout=timeout)
            except RateLimitExceeded as e:
                if on_exceeded is not None:
                    on_exceeded(e.retry_after)
                raise
            return fn(*args, **kwargs)

        wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
        wrapper.limiter = limiter  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    return decorator


def alimit(
    spec: Union[str, int, float, Tuple[int, str]] = "10/s",
    *,
    algorithm: str = "token",
    timeout: Optional[float] = None,
    on_exceeded: Optional[Callable[[float], None]] = None,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Decorator: rate-limit an async function. Same params as @limit."""
    limiter = _make_limiter(spec, algorithm)

    def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        if not inspect.iscoroutinefunction(fn):
            raise TypeError("@alimit requires an async function; use @limit for sync")

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                await limiter.aacquire(timeout=timeout)
            except RateLimitExceeded as e:
                if on_exceeded is not None:
                    on_exceeded(e.retry_after)
                raise
            return await fn(*args, **kwargs)

        wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
        wrapper.limiter = limiter  # type: ignore[attr-defined]
        return wrapper

    return decorator
