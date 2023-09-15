from asyncio import sleep
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cache
from importlib import import_module
from typing import (Any, AsyncGenerator, Generic, NamedTuple, Optional, Self,
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

    async def register(
        self, handler: type[Handler], *requests: T,
        delay: Optional[float | int | timedelta] = None,
        if_not_exists: bool = False
    ) -> None:
        routes = await handler.route(*requests)
        pipe_messages: list[tuple[str, Any]] = [
            (Tube(handler, routes[n]).pipe, requests[n])
            for n in range(len(routes))
        ]

        if delay:
            timestamp = BacklogHandler.get_delayed_timestamp(delay)
            pipe_messages = [
                (
                    Tube(BacklogHandler, Route()).pipe,
                    BacklogRequest(timestamp, pipe, values),
                )
                for (pipe, values)
                in [
                    (pipe, self.serializer.get_values(request))
                    for (pipe, request) in pipe_messages
                ]
            ]

        for pipe in set([pipe for (pipe, _) in pipe_messages]):
            await self.storage.append(pipe, *[
                msg for msg in
                [
                    self.serializer.serialize(request)
                    for (pipe_candidate, request) in pipe_messages
                    if pipe_candidate == pipe
                ]
                if not if_not_exists
                or not await self.storage.contains(pipe, msg)
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
        deserialize = self.queue.serializer.deserialize
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
            if isinstance(instance, BacklogHandler):
                instance.queue = self.queue

            while limit is None or limit > processed_counter:
                page_size = self.page_size(limit)
                processed = False
                for pipe in pipes:
                    msgs = await storage.range(pipe, page_size)
                    if not len(msgs):
                        continue

                    await instance.handle(*[
                        deserialize(cls, msg) for msg in msgs
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


class BacklogRequest(NamedTuple):
    timestamp: float
    pipe: str
    msg: list


class BacklogHandler(Handler):
    queue: Queue

    @classmethod
    def get_delayed_timestamp(cls, delay: float | int | timedelta) -> float:
        now: datetime = datetime.now()
        if isinstance(delay, timedelta):
            now = now + delay

        timestamp: float = now.timestamp()
        if not isinstance(delay, timedelta):
            timestamp = delay = delay

        return timestamp

    async def handle(self, *requests: BacklogRequest) -> None:
        now: float = datetime.now().timestamp()
        backlog = [
            request for request in requests
            if request.timestamp > now
        ]

        if len(backlog):
            await self.queue.register(BacklogHandler, *backlog)

        todo: list[tuple[str, list]] = [
            (request.pipe, request.msg)
            for request in requests
            if request.timestamp <= now
        ]

        for pipe in set([pipe for (pipe, _) in todo]):
            await self.queue.storage.append(pipe, *[
                msg for msg in
                [
                    self.queue.serializer.serialize(request)
                    for (candidate_pipe, request) in todo
                    if candidate_pipe == pipe
                ]
                if not await self.queue.storage.contains(pipe, msg)
            ])

        if len(backlog):
            await sleep(settings.backlog_retry_delay)
