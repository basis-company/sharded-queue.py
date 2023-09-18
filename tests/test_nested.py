from asyncio import gather
from typing import NamedTuple

from pytest import mark

from sharded_queue import Handler, Queue, Route, Tube, Worker
from sharded_queue.drivers import RuntimeLock, RuntimeStorage
from sharded_queue.settings import settings


class ActionMessage(NamedTuple):
    id: int


class Action(NamedTuple):
    id: str


class BaseAction(Handler):
    async def handle(self, *requests: ActionMessage) -> None:
        return gather(*[
            self.perform(Action(request.id))
            for request in requests
        ])
    ...


class CreateUserAction(BaseAction):
    async def perform(self, action: Action):
        ...


class UpdateUserAction(BaseAction):
    async def perform(self, action: Action):
        ...


@mark.asyncio
async def test_nested() -> None:
    queue: Queue = Queue(RuntimeStorage())

    await queue.register(CreateUserAction, Action(1))
    await queue.register(UpdateUserAction, Action(2))
    create_user_pipe = Tube(CreateUserAction, Route(0, 0)).pipe
    update_user_pipe = Tube(UpdateUserAction, Route(0, 0)).pipe

    assert await queue.storage.length(create_user_pipe) == 1
    assert await queue.storage.length(update_user_pipe) == 1

    settings.worker_empty_pause = 0
    await Worker(RuntimeLock(), queue).loop(2)

    assert await queue.storage.length(create_user_pipe) == 0
    assert await queue.storage.length(update_user_pipe) == 0

