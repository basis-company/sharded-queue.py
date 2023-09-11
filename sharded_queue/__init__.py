from asyncio import sleep
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import cache
from importlib import import_module
from json import dumps, loads
from typing import (Any, Generic, List, NamedTuple, Optional, Protocol,
                    Sequence, TypeVar, get_type_hints)

from pydantic import BaseSettings, Field

T = TypeVar('T')


class ShardedQueueSettings(BaseSettings):
    coordinator_delay: float = Field(
        default=1,
        title="Coordinator delay in seconds on empty queues"
    )

    default_order: str = Field(
        default='0',
        title='Default queue order'
    )

    default_thread: str = Field(
        default='0',
        title='Default queue thread'
    )

    worker_batch_size: int = Field(
        default=128,
        title='Worker batch processing size'
    )

    worker_empty_limit: int = Field(
        default=16,
        title="Worker empty queue attempt limit berfore queue rebind",
    )

    worker_empty_pause: float = Field(
        default=0.1,
        title="Worker pause in seconds on empty queue",
    )

    class Config:
        env_prefix = 'queue_'


settings = ShardedQueueSettings()


class Route(NamedTuple):
    thread: str = settings.default_thread
    order: str = settings.default_order


class RequestRoute(NamedTuple):
    request: Any
    route: Route


class Handler(Generic[T]):
    orders: Optional[list[str]] = None

    @classmethod
    async def create(cls):
        return cls()

    @classmethod
    def request_cls(cls) -> type[T]:
        return list(get_type_hints(cls.handle).values())[0]

    @classmethod
    async def route(cls, *requests: T) -> list[RequestRoute]:
        return [
            RequestRoute(
                request,
                Route(settings.default_thread, settings.default_order)
            )
            for request in requests
        ]

    async def start(self):
        pass

    async def handle(self, *requests: T):
        raise NotImplementedError()

    async def stop(self):
        pass


class Tube(NamedTuple):
    handler: type[Handler]
    route: Route

    @property
    def pipe(self) -> str:
        return '#'.join([
            self.handler.__module__,
            self.handler.__name__,
            str(self.route.thread),
            str(self.route.order),
        ])

    @asynccontextmanager
    async def context(self):
        instance = await self.handler.create()
        await instance.start()
        try:
            yield instance
        finally:
            await instance.stop()


@cache
def get_tube(tube: str) -> Tube:
    [*module, name, thread, order] = tube.split('#')
    handler = getattr(import_module("_".join(module)), name)
    return Tube(handler, Route(thread, order))


class RequestTube(NamedTuple, Generic[T]):
    request: T
    tube: Tube


class Serializer(Protocol[T]):
    def serialize(self, request: T) -> str:
        raise NotImplementedError

    def unserialize(self, cls: type[T], source: str) -> T:
        raise NotImplementedError


class Storage(Protocol):
    async def append(self, tube: str, *msgs: str) -> int:
        raise NotImplementedError

    async def length(self, tube: str) -> int:
        raise NotImplementedError

    async def pop(self, tube: str, max: int) -> list[str]:
        raise NotImplementedError

    async def pipes(self) -> list[str]:
        raise NotImplementedError

    async def range(self, tube: str, max: int) -> list[str]:
        raise NotImplementedError


@dataclass
class Queue(Generic[T]):
    serializer: Serializer
    storage: Storage

    async def register(self, handler: type[Handler], *requests: T):
        tubes: list[RequestTube] = [
            RequestTube(request, Tube(handler, route))
            for (request, route)
            in await handler.route(*requests)
        ]

        for pipe in set([tube.pipe for (_, tube) in tubes]):
            await self.storage.append(pipe, *[
                self.serializer.serialize(request)
                for (request, tube) in tubes
                if tube.pipe == pipe
            ])


class Coordinator(Protocol):
    async def acquire_tube(self, queue: Queue) -> Tube:
        all_pipes = False
        while True:
            for pipe in await queue.storage.pipes():
                if not await queue.storage.length(pipe):
                    continue
                tube = get_tube(pipe)
                if tube.handler.orders:
                    if tube.route.order != tube.handler.orders[0]:
                        if not all_pipes:
                            continue
                        tube = Tube(
                            handler=tube.handler,
                            route=Route(
                                thread=tube.route.thread,
                                order=tube.handler.orders[0]
                            )
                        )
                if not await self.bind(tube.pipe):
                    continue
                return tube

            if all_pipes:
                await sleep(settings.coordinator_delay)
            else:
                all_pipes = True

    async def bind(self, tube: str) -> bool:
        raise NotImplementedError

    async def unbind(self, tube: str):
        raise NotImplementedError


@dataclass
class Worker:
    coordinator: Coordinator
    queue: Queue

    def page_size(self, limit: int | None = None) -> int:
        if limit is None:
            return settings.worker_batch_size

        return min(limit, settings.worker_batch_size)

    async def loop(self, limit: Optional[int] = None):
        processed = 0
        while True and limit is None or limit > processed:
            tube = await self.coordinator.acquire_tube(self.queue)
            processed = processed + await self.process(tube, limit)

    async def process(self, tube: Tube, limit: Optional[int] = None) -> int:
        unserialize = self.queue.serializer.unserialize
        storage = self.queue.storage

        cls = tube.handler.request_cls()
        pipes = [tube.pipe]
        empty_counter = 0
        processed_counter = 0

        if tube.handler.orders:
            pipes = [
                Tube(tube.handler, Route(tube.route.thread, order)).pipe
                for order in tube.handler.orders
            ]

        async with tube.context() as instance:
            while limit is None or limit > processed_counter:
                page_size = self.page_size(limit)
                processed = False
                for pipe in pipes:
                    msgs = await storage.range(pipe, page_size)
                    if not len(msgs):
                        continue

                    await instance.handle(*[
                        unserialize(cls, msg) for msg in msgs
                    ])

                    await storage.pop(pipe, len(msgs))

                    processed = True
                    processed_counter = processed_counter + len(msgs)
                    empty_counter = 0
                    break

                if not processed:
                    empty_counter = empty_counter + 1
                    if empty_counter >= settings.worker_empty_limit:
                        break
                    await sleep(settings.worker_empty_pause)

        await self.coordinator.unbind(pipe)
        return processed_counter


class RuntimeCoordinator(Coordinator):
    def __init__(self) -> None:
        super().__init__()
        self.binds: dict[str, bool] = {}

    async def bind(self, pipe: str) -> bool:
        if pipe in self.binds:
            return False
        self.binds[pipe] = True
        return True

    async def unbind(self, pipe: str):
        del self.binds[pipe]


class RuntimeStorage(Storage):
    data: dict[str, List[str]]

    def __init__(self) -> None:
        self.data = {}

    async def append(self, tube: str, *msgs: str) -> int:
        if tube not in self.data:
            self.data[tube] = list(msgs)
        else:
            self.data[tube].extend(list(msgs))

        return len(self.data[tube])

    async def length(self, tube: str) -> int:
        return len(self.data[tube]) if tube in self.data else 0

    async def pop(self, tube: str, max: int) -> list[str]:
        res = await self.range(tube, max)
        if len(res):
            self.data[tube] = self.data[tube][len(res):]
        return res

    async def pipes(self) -> list[str]:
        return list(self.data.keys())

    async def range(self, tube: str, max: int) -> list[str]:
        return self.data[tube][0:max] if tube in self.data else []


class JsonTupleSerializer(Serializer):
    def serialize(self, request: T) -> str:
        if isinstance(request, Sequence):
            values = [k for k in request]
        else:
            values = list(request.__dict__)

        return dumps(values)

    def unserialize(self, cls: type[T], source: str) -> T:
        return cls(*loads(source))
