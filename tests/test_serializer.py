from dataclasses import dataclass
from typing import Any, NamedTuple

from pytest import mark

from sharded_queue.drivers import JsonTupleSerializer
from sharded_queue.protocols import Serializer


class ExampleNamedTuple(NamedTuple):
    name: str


@dataclass
class ExampleRequest:
    name: str


@mark.asyncio
async def test_named_tuple() -> None:
    serializer: Serializer = JsonTupleSerializer()
    request = ExampleRequest('nekufa')
    await runner(serializer, request)


@mark.asyncio
async def test_dataclass() -> None:
    serializer: Serializer = JsonTupleSerializer()
    request = ExampleNamedTuple('nekufa')
    await runner(serializer, request)


async def runner(serializer: Serializer, request: Any):
    serialized = serializer.serialize(request)
    parsed = serializer.deserialize(type(request), serialized)
    assert isinstance(parsed, type(request))
    assert parsed.name == request.name
