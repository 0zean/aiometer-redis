from typing import Any, AsyncIterator, Awaitable, Callable, Optional, Sequence

import anyio

from .meters import RedisMeter  # Import RedisMeter
from .run_on_each import run_on_each
from .types import T


async def amap(
    async_fn: Callable[[Any], Awaitable[T]],
    args: Sequence[Any],
    *,
    max_at_once: Optional[int] = None,
    max_per_second: Optional[float] = None,
    redis_meter: Optional[RedisMeter] = None,  # Add redis_meter parameter
    _include_index: bool = False,
) -> AsyncIterator[T]:
    send_channel, receive_channel = anyio.create_memory_object_stream(max_at_once or 0)

    async def _run():
        await run_on_each(
            async_fn,
            args,
            max_at_once=max_at_once,
            max_per_second=max_per_second,
            redis_meter=redis_meter,  # Pass redis_meter to run_on_each
            _include_index=_include_index,
            _send_to=send_channel,
        )
        await send_channel.aclose()

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(_run)
        async with receive_channel:
            async for item in receive_channel:
                yield item
