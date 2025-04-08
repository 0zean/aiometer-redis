from typing import Optional

import anyio
import redis.asyncio as redis

from .utils import check_strictly_positive


class MeterState:
    async def wait_task_can_start(self) -> None:
        raise NotImplementedError  # pragma: no cover

    async def notify_task_started(self) -> None:
        raise NotImplementedError  # pragma: no cover

    async def notify_task_finished(self) -> None:
        raise NotImplementedError  # pragma: no cover


class Meter:
    async def new_state(self) -> MeterState:
        raise NotImplementedError  # pragma: no cover


class HardLimitMeter(Meter):
    class State(MeterState):
        def __init__(self, max_at_once: int, redis_url: Optional[str] = None, key_prefix: Optional[str] = None) -> None:
            self.max_at_once = max_at_once
            self.redis_url = redis_url
            self.key_prefix = key_prefix
            self.redis = None
            if redis_url:
                self.key = f"{key_prefix}:active_tasks"

        async def _connect(self):
            if self.redis is None and self.redis_url:
                self.redis = await redis.from_url(self.redis_url)

        async def wait_task_can_start(self) -> None:
            if self.redis_url:
                await self._connect()
                while True:
                    active_tasks = await self.redis.get(self.key, encoding="utf-8")
                    active_tasks = int(active_tasks or 0)
                    if active_tasks < self.max_at_once:
                        break
                    await anyio.sleep(0.1)
            else:
                # Local semaphore fallback
                await self.semaphore.__aenter__()

        async def notify_task_started(self) -> None:
            if self.redis_url:
                await self._connect()
                await self.redis.incr(self.key)
            else:
                pass  # No-op for local semaphore

        async def notify_task_finished(self) -> None:
            if self.redis_url:
                await self._connect()
                await self.redis.decr(self.key)
            else:
                # Local semaphore fallback
                await self.semaphore.__aexit__(None, None, None)

    def __init__(self, max_at_once: int, redis_url: Optional[str] = None, key_prefix: Optional[str] = None) -> None:
        check_strictly_positive("max_at_once", max_at_once)
        self.max_at_once = max_at_once
        self.redis_url = redis_url
        self.key_prefix = key_prefix

    async def new_state(self) -> MeterState:
        return type(self).State(self.max_at_once, self.redis_url, self.key_prefix)


class RateLimitMeter(Meter):
    class State(MeterState):
        def __init__(
            self, max_per_second: float, redis_url: Optional[str] = None, key_prefix: Optional[str] = None
        ) -> None:
            self.period = 1 / max_per_second
            self.max_per_period = 1  # TODO: make configurable.
            self.redis_url = redis_url
            self.key_prefix = key_prefix
            self.redis = None
            if redis_url:
                self.key = f"{key_prefix}:tat"  # Redis key for TAT (Theoretical Arrival Time)
            else:
                self.next_start_time = 0.0  # Local fallback

        @property
        def task_delta(self) -> float:
            return self.period / self.max_per_period

        async def _connect(self):
            if self.redis is None and self.redis_url:
                self.redis = await redis.from_url(self.redis_url)

        async def wait_task_can_start(self) -> None:
            if self.redis_url:
                await self._connect()
                while True:
                    now = int(anyio.current_time() * 1000)  # Current time in ms
                    tat = await self.redis.get(self.key, encoding="utf-8")
                    tat = int(tat or 0)  # Default to 0 if no TAT is set
                    if now >= tat:
                        break
                    await anyio.sleep((tat - now) / 1000)  # Sleep until the TAT is reached
            else:
                # Local GCRA fallback
                while True:
                    now = anyio.current_time()
                    next_start_time = max(self.next_start_time, now)
                    time_until_start = next_start_time - now
                    threshold = self.period - self.task_delta
                    if time_until_start <= threshold:
                        break
                    await anyio.sleep(max(0, time_until_start - threshold))

        async def notify_task_started(self) -> None:
            if self.redis_url:
                await self._connect()
                now = int(anyio.current_time() * 1000)  # Current time in ms
                tat = await self.redis.get(self.key, encoding="utf-8")
                tat = int(tat or 0)  # Default to 0 if no TAT is set
                new_tat = max(tat, now) + int(self.task_delta * 1000)  # Update TAT
                await self.redis.set(self.key, new_tat)
            else:
                # Local GCRA fallback
                now = anyio.current_time()
                self.next_start_time = max(self.next_start_time, now) + self.task_delta

        async def notify_task_finished(self) -> None:
            pass  # No-op for both Redis and local

    def __init__(
        self, max_per_second: float, redis_url: Optional[str] = None, key_prefix: Optional[str] = None
    ) -> None:
        check_strictly_positive("max_per_second", max_per_second)
        self.max_per_second = max_per_second
        self.redis_url = redis_url
        self.key_prefix = key_prefix

    async def new_state(self) -> MeterState:
        return type(self).State(self.max_per_second, self.redis_url, self.key_prefix)
