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
        defer: Optional[float | int | timedelta] = None,
        if_not_exists: bool = False,
        recurrent: Optional[float | int | timedelta] = None,
    ) -> None:
        routes = await handler.route(*requests)
        pipe_messages: list[tuple[str, Any]] = [
            (Tube(handler, routes[n]).pipe, requests[n])
            for n in range(len(routes))
        ]

        if recurrent:
            if_not_exists = True
            pipe_messages = RecurrentHandler.transform(
                pipe_messages, recurrent, self.serializer
            )

        if defer:
            if_not_exists = True
            pipe_messages = DeferredHandler.transform(
                pipe_messages, defer, self.serializer
            )

        for pipe in set([pipe for (pipe, _) in pipe_messages]):
            msgs = [
                msg for msg in
                [
                    self.serializer.serialize(request)
                    for (pipe_candidate, request) in pipe_messages
                    if pipe_candidate == pipe
                ]
                if not if_not_exists
                or not await self.storage.contains(pipe, msg)
            ]
            if len(msgs):
                await self.storage.append(pipe, *msgs)


@dataclass
class Worker:
    lock: Lock
    queue: Queue

    async def acquire_tube(
        self, handler: Optional[type[Handler]] = None
    ) -> Tube:
        all_pipes = False
        while True:
            for pipe in await self.queue.storage.pipes():
                if not await self.queue.storage.length(pipe):
                    continue
                tube: Tube = Tube.parse_pipe(pipe)
                if handler and tube.handler is not handler:
                    continue
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

    async def loop(
        self,
        limit: Optional[int] = None,
        handler: Optional[type[Handler]] = None,
    ) -> None:
        processed = 0
        while True and limit is None or limit > processed:
            tube = await self.acquire_tube(handler)
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
            if isinstance(instance, DeferredHandler | RecurrentHandler):
                instance.queue = self.queue

            while limit is None or limit > processed_counter:
                if tube.handler is RecurrentHandler:
                    page_size = settings.recurrent_tasks_limit
                else:
                    page_size = self.page_size(limit)
                processed = False
                for pipe in pipes:
                    msgs = await storage.range(pipe, page_size)
                    if not len(msgs):
                        continue

                    await instance.handle(*[
                        deserialize(cls, msg) for msg in msgs
                    ])

                    if tube.handler is RecurrentHandler:
                        await self.lock.ttl(
                            key=tube.pipe,
                            ttl=settings.recurrent_check_interval
                        )
                        return len(msgs)

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

                if tube.handler is DeferredHandler:
                    await self.lock.ttl(
                        key=tube.pipe,
                        ttl=settings.deferred_retry_delay,
                    )
                    return processed_counter
                else:
                    await self.lock.ttl(tube.pipe, settings.lock_timeout)

        await self.lock.release(tube.pipe)

        return processed_counter


class DeferredRequest(NamedTuple):
    timestamp: float
    pipe: str
    msg: list

    @classmethod
    def calculate_timestamp(cls, delta: float | int | timedelta) -> float:
        now: datetime = datetime.now()
        if isinstance(delta, timedelta):
            now = now + delta

        timestamp: float = now.timestamp()
        if not isinstance(delta, timedelta):
            timestamp = delta = delta

        return timestamp


class DeferredHandler(Handler):
    queue: Queue

    async def handle(self, *requests: DeferredRequest) -> None:
        now: float = datetime.now().timestamp()
        pending: list[DeferredRequest] = [
            request for request in requests
            if request.timestamp > now
        ]

        if len(pending):
            await self.queue.register(DeferredHandler, *pending)

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

        if len(pending) and not len(todo):
            await sleep(settings.deferred_retry_delay)

    @classmethod
    def transform(
        cls,
        pipe_messages: list[tuple[str, T]],
        defer: float | int | timedelta,
        serializer: Serializer,
    ) -> list[tuple[str, DeferredRequest]]:
        timestamp = DeferredRequest.calculate_timestamp(defer)
        return [
            (
                Tube(DeferredHandler, Route()).pipe,
                DeferredRequest(timestamp, pipe, values),
            )
            for (pipe, values)
            in [
                (pipe, serializer.get_values(request))
                for (pipe, request) in pipe_messages
            ]
        ]


class RecurrentRequest(NamedTuple):
    interval: float
    pipe: str
    msg: list

    @classmethod
    def get_interval(cls, interval: int | float | timedelta) -> float:
        if isinstance(interval, timedelta):
            return float(interval.seconds)

        return float(interval)


class RecurrentHandler(Handler):
    queue: Queue

    async def handle(self, *requests: RecurrentRequest) -> None:
        deferred_pipe: str = Tube(DeferredHandler, Route()).pipe
        deferred_requests: list[tuple[str, str]] = [
            (request.pipe, request.msg)
            for request in [
                self.queue.serializer.deserialize(DeferredRequest, msg)
                for msg in await self.queue.storage.range(
                    deferred_pipe, settings.recurrent_tasks_limit
                )
            ]
        ]

        todo: list[DeferredRequest] = [
            DeferredRequest(
                DeferredRequest.calculate_timestamp(request.interval),
                request.pipe,
                request.msg,
            )
            for request in requests
            if (request.pipe, request.msg) not in deferred_requests
        ]

        if len(todo):
            await self.queue.register(
                DeferredHandler, *todo, if_not_exists=True
            )

    @classmethod
    def transform(
        cls,
        pipe_messages: list[tuple[str, T]],
        recurrent: float | int | timedelta,
        serializer: Serializer,
    ) -> list[tuple[str, RecurrentRequest]]:
        interval: float = RecurrentRequest.get_interval(recurrent)
        return [
            (
                Tube(RecurrentHandler, Route()).pipe,
                RecurrentRequest(interval, pipe, values)
            )
            for (pipe, values)
            in [
                (pipe, serializer.get_values(request))
                for (pipe, request) in pipe_messages
            ]
        ]
