from datetime import datetime
from typing import NamedTuple

from pytest import mark

from sharded_queue import (Handler, JsonTupleSerializer, Queue, RequestRoute,
                           Route, RuntimeCoordinator, RuntimeStorage, Tube,
                           Worker, settings)


class Request(NamedTuple):
    bucket: int


context = {
    'started': 0,
    'stopped': 0
}

synced: list[Request] = []


class SyncHandler(Handler):
    @classmethod
    async def route(cls, *requests: Request) -> list[RequestRoute]:
        return [
            RequestRoute(request, Route(thread=str(request.bucket % 2)))
            for request in requests
        ]

    async def start(self):
        context['started'] = datetime.now().timestamp()

    async def stop(self):
        context['stopped'] = datetime.now().timestamp()

    async def handle(self, *requests: Request):
        synced.extend(requests)


@mark.asyncio
async def test_queue():
    queue = Queue(JsonTupleSerializer(), RuntimeStorage())

    def get_pipe(thread: str):
        return Tube(SyncHandler, Route(thread=thread)).pipe

    await queue.register(
        SyncHandler,
        Request(1),
        Request(2),
        Request(3),
        Request(4),
        Request(5),
    )

    assert get_pipe(0) in queue.storage.data
    assert get_pipe(1) in queue.storage.data
    assert get_pipe(2) not in queue.storage.data

    assert await queue.storage.length(get_pipe(0)) == 2
    assert await queue.storage.length(get_pipe(1)) == 3
    assert await queue.storage.length(get_pipe(2)) == 0

    assert context['started'] == 0
    assert context['stopped'] == 0

    worker = Worker(coordinator=RuntimeCoordinator(), queue=queue)
    await worker.loop(2)

    assert context['started'] != 0
    assert context['stopped'] != 0
    assert context['started'] != context['stopped']

    assert len(synced) == 2
    thread = str(synced[0].bucket % 2)

    if thread == '0':
        assert await queue.storage.length(get_pipe(0)) == 0
        assert await queue.storage.length(get_pipe(1)) == 3
    else:
        assert await queue.storage.length(get_pipe(0)) == 2
        assert await queue.storage.length(get_pipe(1)) == 1

    # test worker switch
    settings.worker_empty_pause = 0
    (last_started, last_stopped) = tuple(context.values())
    await worker.loop(3)
    assert len(synced) == 5
    assert await queue.storage.length(get_pipe(0)) == 0
    assert await queue.storage.length(get_pipe(1)) == 0
    assert last_started != context['started']
    assert last_stopped != context['stopped']


@mark.asyncio
async def test_storage():
    storage = RuntimeStorage()
    await storage.append('tester', 'q')
    await storage.append('tester', 'w')
    await storage.append('tester', 'e')
    await storage.append('tester', 'r', 't', 'y')
    assert await storage.length('tester') == 6
    assert await storage.length('tester2') == 0
    assert await storage.range('tester', 1) == ['q']
    assert await storage.range('tester', 2) == ['q', 'w']
    assert await storage.range('tester', 3) == ['q', 'w', 'e']
    assert await storage.pop('tester', 1) == ['q']
    assert await storage.range('tester', 1) == ['w']
    assert await storage.range('tester', 2) == ['w', 'e']
    assert await storage.pop('tester', 2) == ['w', 'e']
    assert await storage.pop('tester', 10) == ['r', 't', 'y']
    assert await storage.pop('tester', 1) == []
