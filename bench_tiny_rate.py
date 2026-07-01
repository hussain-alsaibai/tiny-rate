"""Benchmarks for tiny-rate. Run with `python bench_tiny_rate.py`."""

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import tiny_rate as tr


def bench(name, fn, n=1_000_000):
    # warmup
    fn()
    t0 = time.perf_counter()
    for _ in range(n):
        fn()
    dt = (time.perf_counter() - t0) / n * 1e6
    print(f"  {name:35s} {dt:10.3f} µs/op  ({n/dt*1e6:,.0f} ops/s)")


async def abench(name, fn, n=10_000):
    for _ in range(100):
        await fn()
    t0 = time.perf_counter()
    for _ in range(n):
        await fn()
    dt = (time.perf_counter() - t0) / n * 1e6
    print(f"  {name:35s} {dt:10.3f} µs/op  ({n/dt*1e6:,.0f} ops/s)")


def main():
    print("== tiny-rate benchmarks ==")

    # Token bucket — saturated (always-available)
    b = tr.TokenBucket(rate=1_000_000, capacity=1_000_000)
    bench("TokenBucket.try_acquire (saturated)", lambda: b.try_acquire(), n=200_000)

    # Token bucket — contended (slower refill, no contention, but checks happen)
    b2 = tr.TokenBucket(rate=1, capacity=1_000_000)
    bench("TokenBucket.try_acquire (high-rate)", lambda: b2.try_acquire(), n=200_000)

    # Fixed window
    w = tr.FixedWindow(rate=1_000_000, period=1.0)
    bench("FixedWindow.try_acquire", lambda: w.try_acquire(), n=200_000)

    # Sliding window
    s = tr.SlidingWindow(rate=1_000_000, period=1.0)
    bench("SlidingWindow.try_acquire", lambda: s.try_acquire(), n=50_000)

    # parse_rate
    bench("parse_rate('100/s')", lambda: tr.parse_rate("100/s"), n=200_000)

    print()
    print("== Async acquire (n=10,000) ==")
    asyncio.run(abench("TokenBucket.aacquire (high-rate)", lambda: b2.aacquire()))


if __name__ == "__main__":
    main()
