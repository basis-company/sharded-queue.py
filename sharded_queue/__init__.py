from asyncio import sleep
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import cache
from importlib import import_module
from typing import (AsyncGenerator, Generic, NamedTuple, Optional, Self,
                    TypeVar, get_type_hints)

from sharded_queue.drivers import JsonTupleSerializer
from sharded_queue.protocols import Lock, Serializer, Storage
from sharded_queue.settings import settings

T = TypeVar('T')


class Route(NamedTuple):
    thread: int = settings.default_thread
    priority: int = settings.default_priority


class Handler(Generic[T]):
    priorities: Optional[list[int]] = None

    @classmethod
    async def create(cls) -> Self:
        return cls()

    @classmethod
    def request_cls(cls) -> type[T]:
        return list(get_type_hints(cls.handle).values())[0]

    @classmethod
    async def route(cls, *requests: T) -> list[Route]:
        return [
            Route(settings.default_thread, settings.default_priority)
            for _ in requests
        ]

    async def start(self) -> None:
        pass

    async def handle(self, *requests: T) -> None:
        raise NotImplementedError()

    async def stop(self) -> None:
        pass


class Tube(NamedTuple):
    handler: type[Handler]
    route: Route

    @classmethod
    @cache
    def parse_pipe(cls, tube: str) -> Self:
        [*module, name, thread, priority] = tube.split('/')
        handler = getattr(import_module("_".join(module)), name)
        return cls(handler, Route(int(thread), int(priority)))

    @property
    def pipe(self) -> str:
        return '/'.join([
            self.handler.__module__,
            self.handler.__name__,
            str(self.route.thread),
            str(self.route.priority),
        ])

    @asynccontextmanager
    async def context(self) -> AsyncGenerator:
        instance = await self.handler.create()
        await instance.start()
        try:
            yield instance
        finally:
            await instance.stop()


@dataclass
class Queue(Generic[T]):
    def __init__(
        self, storage: Storage, serializer: Optional[Serializer] = None
    ):
        self.storage = storage
        self.serializer = serializer or JsonTupleSerializer()

    async def register(self, handler: type[Handler], *requests: T) -> None:
        routes = await handler.route(*requests)
        tubes: list[tuple[T, Tube]] = [
            (requests[n], Tube(handler, routes[n]))
            for n in range(len(routes))
        ]

        for pipe in set([tube.pipe for (_, tube) in tubes]):
            await self.storage.append(pipe, *[
                self.serializer.serialize(request)
                for (request, tube) in tubes
                if tube.pipe == pipe
            ])


@dataclass
class Worker:
    lock: Lock
    queue: Queue

    async def acquire_tube(self) -> Tube:
        all_pipes = False
        while True:
            for pipe in await self.queue.storage.pipes():
                if not await self.queue.storage.length(pipe):
                    continue
                tube: Tube = Tube.parse_pipe(pipe)
                if tube.handler.priorities:
                    if tube.route.priority != tube.handler.priorities[0]:
                        if not all_pipes:
                            continue
                        tube = Tube(
                            handler=tube.handler,
                            route=Route(
                                thread=tube.route.thread,
                                priority=tube.handler.priorities[0]
                            )
                        )
                if not await self.lock.acquire(tube.pipe):
                    continue
                return tube

            if all_pipes:
                await sleep(settings.worker_acquire_delay)
            else:
                all_pipes = True

    def page_size(self, limit: Optional[int] = None) -> int:
        if limit is None:
            return settings.worker_batch_size

        return min(limit, settings.worker_batch_size)

    async def loop(self, limit: Optional[int] = None) -> None:
        processed = 0
        while True and limit is None or limit > processed:
            tube = await self.acquire_tube()
            processed = processed + await self.process(tube, limit)

    async def process(self, tube: Tube, limit: Optional[int] = None) -> int:
        unserialize = self.queue.serializer.unserialize
        storage = self.queue.storage

        cls = tube.handler.request_cls()
        pipes = [tube.pipe]
        empty_counter = 0
        processed_counter = 0

        if tube.handler.priorities:
            pipes = [
                Tube(tube.handler, Route(tube.route.thread, priority)).pipe
                for priority in tube.handler.priorities
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

        await self.lock.release(tube.pipe)

        return processed_counter
