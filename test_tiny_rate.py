"""Tests for tiny-rate — run with `python test_tiny_rate.py`. Stdlib only."""

import asyncio
import sys
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import tiny_rate as tr


# ---------------------------------------------------------------------------
# parse_rate
# ---------------------------------------------------------------------------


class TestParseRate(unittest.TestCase):
    def test_simple(self):
        self.assertEqual(tr.parse_rate("100/s"), (100.0, 1.0))
        self.assertEqual(tr.parse_rate("5_000/min"), (5000.0, 60.0))
        self.assertEqual(tr.parse_rate("10/h"), (10.0, 3600.0))
        self.assertEqual(tr.parse_rate("1/d"), (1.0, 86400.0))
        self.assertEqual(tr.parse_rate("100/sec"), (100.0, 1.0))
        self.assertEqual(tr.parse_rate("5/min"), (5.0, 60.0))

    def test_int(self):
        self.assertEqual(tr.parse_rate(50), (50.0, 1.0))

    def test_tuple(self):
        self.assertEqual(tr.parse_rate((5, "min")), (5.0, 60.0))

    def test_invalid(self):
        with self.assertRaises(ValueError):
            tr.parse_rate("abc")
        with self.assertRaises(ValueError):
            tr.parse_rate("")


# ---------------------------------------------------------------------------
# TokenBucket
# ---------------------------------------------------------------------------


class TestTokenBucket(unittest.TestCase):
    def test_starts_full_and_drains(self):
        b = tr.TokenBucket(rate=10, capacity=10)
        self.assertEqual(b.available(), 10)
        self.assertTrue(b.try_acquire())
        self.assertLess(b.available(), 10)

    def test_capacity_bounds_refill(self):
        b = tr.TokenBucket(rate=1, capacity=5)
        time.sleep(0.05)
        # Enough wall time to refill many times over; cap is the limit
        self.assertEqual(b.available(), 5)

    def test_rejects_when_empty(self):
        b = tr.TokenBucket(rate=10, capacity=1)
        self.assertTrue(b.try_acquire())
        self.assertFalse(b.try_acquire())

    def test_acquire_blocks(self):
        b = tr.TokenBucket(rate=100, capacity=1)
        b.try_acquire()
        start = time.monotonic()
        b.acquire()
        elapsed = time.monotonic() - start
        self.assertGreater(elapsed, 0.005)
        self.assertLess(elapsed, 0.5)

    def test_acquire_timeout_raises(self):
        b = tr.TokenBucket(rate=1, capacity=1)
        b.try_acquire()
        with self.assertRaises(tr.RateLimitExceeded) as cm:
            b.acquire(timeout=0.01)
        self.assertGreater(cm.exception.retry_after, 0)

    def test_acquire_uses_retry_after(self):
        b = tr.TokenBucket(rate=2, capacity=1)
        b.try_acquire()
        with self.assertRaises(tr.RateLimitExceeded) as cm:
            b.acquire(timeout=0.0)
        # 0.5s for one token at 2/s
        self.assertGreater(cm.exception.retry_after, 0.3)
        self.assertLess(cm.exception.retry_after, 1.0)

    def test_rejects_invalid_args(self):
        with self.assertRaises(ValueError):
            tr.TokenBucket(rate=0)
        with self.assertRaises(ValueError):
            tr.TokenBucket(rate=10, capacity=0)
        b = tr.TokenBucket(rate=10)
        with self.assertRaises(ValueError):
            b.try_acquire(tokens=0)
        with self.assertRaises(ValueError):
            b.try_acquire(tokens=-1)

    def test_aacquire(self):
        async def runner():
            b = tr.TokenBucket(rate=200, capacity=1)
            await b.aacquire()
            start = time.monotonic()
            await b.aacquire()
            return time.monotonic() - start

        elapsed = asyncio.run(runner())
        self.assertGreater(elapsed, 0.003)
        self.assertLess(elapsed, 0.5)

    def test_aacquire_timeout(self):
        async def runner():
            b = tr.TokenBucket(rate=1, capacity=1)
            await b.aacquire()
            with self.assertRaises(tr.RateLimitExceeded):
                await b.aacquire(timeout=0.01)

        asyncio.run(runner())

    def test_repr(self):
        b = tr.TokenBucket(rate=5)
        self.assertIn("5", repr(b))


# ---------------------------------------------------------------------------
# FixedWindow
# ---------------------------------------------------------------------------


class TestFixedWindow(unittest.TestCase):
    def test_allows_up_to_rate(self):
        w = tr.FixedWindow(rate=3, period=1.0)
        self.assertTrue(w.try_acquire())
        self.assertTrue(w.try_acquire())
        self.assertTrue(w.try_acquire())
        self.assertFalse(w.try_acquire())

    def test_resets(self):
        w = tr.FixedWindow(rate=2, period=0.05)
        self.assertTrue(w.try_acquire())
        self.assertTrue(w.try_acquire())
        self.assertFalse(w.try_acquire())
        time.sleep(0.07)
        self.assertTrue(w.try_acquire())

    def test_acquire_timeout(self):
        w = tr.FixedWindow(rate=1, period=0.5)
        w.try_acquire()
        with self.assertRaises(tr.RateLimitExceeded):
            w.acquire(timeout=0.01)

    def test_aacquire(self):
        async def runner():
            w = tr.FixedWindow(rate=2, period=0.05)
            await w.aacquire()
            await w.aacquire()
            with self.assertRaises(tr.RateLimitExceeded):
                await w.aacquire(timeout=0.01)

        asyncio.run(runner())

    def test_invalid(self):
        with self.assertRaises(ValueError):
            tr.FixedWindow(rate=0, period=1)
        with self.assertRaises(ValueError):
            tr.FixedWindow(rate=1, period=0)


# ---------------------------------------------------------------------------
# SlidingWindow
# ---------------------------------------------------------------------------


class TestSlidingWindow(unittest.TestCase):
    def test_allows_up_to_rate(self):
        w = tr.SlidingWindow(rate=2, period=1.0)
        self.assertTrue(w.try_acquire())
        self.assertTrue(w.try_acquire())
        self.assertFalse(w.try_acquire())

    def test_slides(self):
        w = tr.SlidingWindow(rate=2, period=0.05)
        self.assertTrue(w.try_acquire())
        time.sleep(0.03)
        self.assertTrue(w.try_acquire())
        self.assertFalse(w.try_acquire())
        time.sleep(0.04)
        self.assertTrue(w.try_acquire())

    def test_acquire(self):
        w = tr.SlidingWindow(rate=1, period=0.05)
        w.try_acquire()
        start = time.monotonic()
        w.acquire()
        elapsed = time.monotonic() - start
        self.assertGreater(elapsed, 0.04)
        self.assertLess(elapsed, 0.5)

    def test_aacquire(self):
        async def runner():
            w = tr.SlidingWindow(rate=1, period=0.05)
            await w.aacquire()
            with self.assertRaises(tr.RateLimitExceeded):
                await w.aacquire(timeout=0.01)

        asyncio.run(runner())

    def test_invalid(self):
        with self.assertRaises(ValueError):
            tr.SlidingWindow(rate=0, period=1)
        with self.assertRaises(ValueError):
            tr.SlidingWindow(rate=1, period=0)


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------


class TestDecorators(unittest.TestCase):
    def test_limit_sync(self):
        @tr.limit("2/s", timeout=0.0)
        def f():
            return "ok"

        self.assertEqual(f(), "ok")
        self.assertEqual(f(), "ok")
        with self.assertRaises(tr.RateLimitExceeded):
            f()

    def test_alimit_async(self):
        @tr.alimit("2/s", timeout=0.0)
        async def f():
            return "ok"

        async def runner():
            self.assertEqual(await f(), "ok")
            self.assertEqual(await f(), "ok")
            with self.assertRaises(tr.RateLimitExceeded):
                await f()

        asyncio.run(runner())

    def test_alimit_rejects_sync(self):
        with self.assertRaises(TypeError):
            tr.alimit("1/s")(lambda: None)

    def test_on_exceeded_callback(self):
        calls = []

        @tr.limit("1/s", timeout=0.0, on_exceeded=lambda ra: calls.append(ra))
        def f():
            return 1

        f()
        with self.assertRaises(tr.RateLimitExceeded):
            f()
        self.assertEqual(len(calls), 1)
        self.assertGreater(calls[0], 0)

    def test_limiter_attached(self):
        @tr.limit("5/s", algorithm="sliding")
        def f():
            return 1

        self.assertIsInstance(f.limiter, tr.SlidingWindow)

    def test_metadata_preserved_sync(self):
        @tr.limit("10/s")
        def my_function():
            """My docstring."""
            return 42

        self.assertEqual(my_function.__name__, "my_function")
        self.assertIn("docstring", my_function.__doc__)

    def test_metadata_preserved_async(self):
        @tr.alimit("10/s")
        async def my_async():
            """My async docstring."""
            return 42

        self.assertEqual(my_async.__name__, "my_async")
        self.assertIn("async docstring", my_async.__doc__)


# ---------------------------------------------------------------------------
# Thread safety + throughput smoke
# ---------------------------------------------------------------------------


class TestConcurrency(unittest.TestCase):
    def test_token_bucket_thread_safe(self):
        b = tr.TokenBucket(rate=1000, capacity=1000)
        consumed = []
        lock = threading.Lock()

        def worker():
            for _ in range(20):
                if b.try_acquire():
                    with lock:
                        consumed.append(1)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # No exceptions, count is sane
        self.assertGreater(len(consumed), 0)
        self.assertLessEqual(len(consumed), 1000)

    def test_token_bucket_throughput(self):
        b = tr.TokenBucket(rate=1_000_000, capacity=1_000_000)
        start = time.monotonic()
        n = 100_000
        for _ in range(n):
            b.try_acquire()
        elapsed = time.monotonic() - start
        ops = n / elapsed
        # Conservative floor; usually >1M ops/s
        self.assertGreater(ops, 50_000)


if __name__ == "__main__":
    unittest.main(verbosity=2)
