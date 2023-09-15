from asyncio import gather
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator, NamedTuple

from pydantic_settings import BaseSettings
from pytest import mark

from sharded_queue import Handler, Queue, Route, Tube, Worker
from sharded_queue.drivers import RuntimeLock, RuntimeStorage
from sharded_queue.settings import settings


class BenchmarkSettings(BaseSettings):
    threads: int = 1
    requests: int = 100_000


class PerformaceMatrix:
    requests: list[int] = [100, 10_000]
    threads: list[int] = [1, 2, 5, 10, 20, 50]


benchmark = BenchmarkSettings()


class DummyRequest(NamedTuple):
    n: int


class DummyHandler(Handler):
    @classmethod
    async def route(cls, *requests: DummyRequest) -> list[Route]:
        return [Route(r.n % benchmark.threads) for r in requests]

    async def handle(self, *requests: DummyRequest) -> None:
        pass


@mark.asyncio
async def test_performance() -> None:
    print('start benchmark')
    settings.worker_batch_size = 512
    settings.worker_empty_limit = 0
    settings.worker_empty_pause = 0
    queue: Queue = Queue(RuntimeStorage())
    worker: Worker = Worker(RuntimeLock(), queue)
    for requests in PerformaceMatrix.requests:
        benchmark.requests = requests
        for threads in PerformaceMatrix.threads:
            benchmark.threads = threads
            print(f'test {requests} requests using {threads} thread(s)')
            async with measure('registration'):
                await queue.register(
                    DummyHandler,
                    *[DummyRequest(n) for n in range(1, requests)]
                )
            async with measure('one worker'):
                await worker.loop(requests-1)

            for thread in range(1, threads):
                pipe = Tube(DummyHandler, Route(thread)).pipe
                assert await queue.storage.length(pipe) == 0

            await queue.register(
                DummyHandler,
                *[DummyRequest(n) for n in range(1, requests)]
            )

            async with measure('worker threads'):
                await gather(*[
                    Worker(RuntimeLock(), queue).loop(
                        int(requests-1/threads) - 1
                    )
                ])


@asynccontextmanager
async def measure(label: str) -> AsyncGenerator:
    start = datetime.now().timestamp()
    yield
    time = datetime.now().timestamp() - start
    rps = round(benchmark.requests / time)
    time = round(time, 3)

    print(f'{label} [rps: {rps}]')
