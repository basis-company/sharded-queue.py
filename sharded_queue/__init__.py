from asyncio import get_event_loop, sleep
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import cache
from importlib import import_module
from signal import SIGTERM
from typing import (Any, AsyncGenerator, Generic, NamedTuple, Optional, Self,
                    TypeVar, get_type_hints)

from dateutil.rrule import rrule, rrulestr
from sharded_queue.drivers import JsonTupleSerializer
from sharded_queue.protocols import Lock, Serializer, Storage
from sharded_queue.settings import WorkerSettings

T = TypeVar('T')


class Route(NamedTuple):
    thread: int = 0
    priority: int = 0


class Handler(Generic[T]):
    priorities: Optional[list[int]] = None

    @classmethod
    async def create(cls) -> Self:
        return cls()

    @classmethod
    def request_cls(cls) -> type[T]:
        request_cls = list(get_type_hints(cls.handle).values())[0]
        if isinstance(request_cls, TypeVar):
            raise NotImplementedError(cls)
        return request_cls

    @classmethod
    async def route(cls, *requests: T) -> list[Route]:
        return [Route() for _ in requests]

    async def start(self) -> None:
        pass

    async def handle(self, *requests: T) -> None:
        raise NotImplementedError(self.__class__)

    async def stop(self) -> None:
        pass

    def batch_size(self) -> Optional[int]:
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
        self,
        storage: Storage,
        serializer: Optional[Serializer] = None,
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
            tube = Tube(RecurrentHandler, Route())
            length = await self.storage.length(tube.pipe)
            messages = await self.storage.range(tube.pipe, length)

            pipe_messages = RecurrentHandler.transform(
                pipe_messages, recurrent, self.serializer
            )

            recurrent_tuples = [
                (request.pipe, request.msg) for (_, request) in pipe_messages
            ]

            for msg in reversed(messages):
                request = self.serializer.deserialize(RecurrentRequest, msg)
                if (request.pipe, request.msg) in recurrent_tuples:
                    await self.storage.remove(tube.pipe, msg)

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
    pipe: Optional[str] = None
    settings: WorkerSettings = field(default_factory=WorkerSettings)

    async def acquire_tube(
        self, handler: Optional[type[Handler]] = None
    ) -> Optional[Tube]:
        all_pipes = False
        while get_event_loop().is_running():
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
                await sleep(self.settings.acquire_delay)
            else:
                all_pipes = True

        return None

    async def loop(
        self,
        limit: Optional[int] = None,
        handler: Optional[type[Handler]] = None,
    ) -> None:
        get_event_loop().add_signal_handler(SIGTERM, self.housekeep)
        processed = 0
        while get_event_loop().is_running() and (
            limit is None or limit > processed
        ):
            tube = await self.acquire_tube(handler)
            if not tube:
                break
            self.pipe = tube.pipe
            processed = processed + await self.process(tube, limit)
            self.pipe = None

        get_event_loop().remove_signal_handler(SIGTERM)

    def housekeep(self) -> None:
        if self.pipe:
            get_event_loop().create_task(self.lock.release(self.pipe))

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
                instance.worker = self

            while get_event_loop().is_running() and (
                limit is None or limit > processed_counter
            ):
                if tube.handler is RecurrentHandler:
                    batch_size = self.settings.recurrent_tasks_limit
                else:
                    batch_size = instance.batch_size()
                    if batch_size is None:
                        batch_size = self.settings.batch_size
                    if limit is not None:
                        batch_size = min(limit, batch_size)
                processed = False
                for pipe in pipes:
                    msgs = await storage.range(pipe, batch_size)
                    if not len(msgs):
                        continue

                    await instance.handle(*[
                        deserialize(cls, msg) for msg in msgs
                    ])

                    if tube.handler is RecurrentHandler:
                        await self.prolongate_lock(
                            self.settings.recurrent_check_interval
                        )
                        return len(msgs)

                    await storage.pop(pipe, len(msgs))

                    processed = True
                    processed_counter = processed_counter + len(msgs)
                    empty_counter = 0
                    break

                if not processed:
                    empty_counter = empty_counter + 1
                    if empty_counter >= self.settings.empty_limit:
                        break
                    await sleep(self.settings.empty_pause)

                if tube.handler is DeferredHandler:
                    await self.prolongate_lock(
                        self.settings.deferred_retry_delay
                    )
                    return processed_counter
                else:
                    await self.prolongate_lock()

        if self.pipe:
            await self.lock.release(self.pipe)

        return processed_counter

    async def prolongate_lock(self, ttl: Optional[int] = None):
        if not self.pipe:
            raise RuntimeError('No active pipe')
        if ttl is None:
            ttl = self.lock.settings.timeout
        await self.lock.ttl(self.pipe, ttl)


class DeferredRequest(NamedTuple):
    timestamp: float
    pipe: str
    msg: list

    @classmethod
    def calculate_timestamp(
        cls,
        delta: float | int | str | timedelta,
    ) -> float:
        if isinstance(delta, str):
            return rrulestr(delta).after(datetime.now()).timestamp()

        now: datetime = datetime.now()
        if isinstance(delta, timedelta):
            now = now + delta

        timestamp: float = now.timestamp()
        if not isinstance(delta, timedelta):
            timestamp = timestamp + delta

        return timestamp


class DeferredHandler(Handler):
    worker: Worker

    async def handle(self, *requests: DeferredRequest) -> None:
        now: float = datetime.now().timestamp()
        pending: list[DeferredRequest] = [
            request for request in requests
            if request.timestamp > now
        ]

        if len(pending):
            pending = sorted(pending, key=lambda r: r.timestamp)
            await self.worker.queue.register(DeferredHandler, *pending)

        todo: list[tuple[str, list]] = [
            (request.pipe, request.msg)
            for request in requests
            if request.timestamp <= now
        ]

        for pipe in set([pipe for (pipe, _) in todo]):
            ready: list[str] = [
                msg for msg in
                [
                    self.worker.queue.serializer.serialize(request)
                    for (candidate_pipe, request) in todo
                    if candidate_pipe == pipe
                ]
                if not await self.worker.queue.storage.contains(pipe, msg)
            ]

            ready = list(set(ready))
            if len(ready):
                await self.worker.queue.storage.append(pipe, *ready)

        if len(pending) and not len(todo):
            await sleep(self.worker.settings.deferred_retry_delay)

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
    interval: float | str
    pipe: str
    msg: list

    @classmethod
    def get_interval(
        cls,
        interval: int | float | timedelta | rrule
    ) -> float | str:
        if isinstance(interval, rrule):
            return str(interval)

        if isinstance(interval, timedelta):
            return float(int(interval.total_seconds()))

        return float(interval)


class RecurrentHandler(Handler):
    worker: Worker

    async def handle(self, *requests: RecurrentRequest) -> None:
        deferred_pipe: str = Tube(DeferredHandler, Route()).pipe
        deferred_requests: list[tuple[str, str]] = [
            (request.pipe, request.msg)
            for request in [
                self.worker.queue.serializer.deserialize(DeferredRequest, msg)
                for msg in await self.worker.queue.storage.range(
                    deferred_pipe, self.worker.settings.recurrent_tasks_limit
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
            await self.worker.queue.register(
                DeferredHandler, *todo, if_not_exists=True
            )

    @classmethod
    def transform(
        cls,
        pipe_messages: list[tuple[str, T]],
        recurrent: float | int | timedelta | rrule,
        serializer: Serializer,
    ) -> list[tuple[str, RecurrentRequest]]:
        interval: float | str = RecurrentRequest.get_interval(recurrent)
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
