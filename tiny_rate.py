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

__version__ = "0.1.0"
__all__ = [
    "TokenBucket",
    "FixedWindow",
    "SlidingWindow",
    "RateLimitExceeded",
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
