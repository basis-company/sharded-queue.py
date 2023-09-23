from dataclasses import dataclass
from typing import Any, NamedTuple

from pydantic import BaseModel

from sharded_queue.drivers import JsonTupleSerializer
from sharded_queue.protocols import Serializer


class NamedTupleRequest(NamedTuple):
    name: str


@dataclass
class DataclassRequest:
    name: str


class PydanticRequest(BaseModel):
    name: str


def test_dataclass() -> None:
    runner(JsonTupleSerializer(), DataclassRequest('nekufa'))


def test_named_tuple() -> None:
    runner(JsonTupleSerializer(), NamedTupleRequest('nekufa'))


def test_pydantic() -> None:
    runner(JsonTupleSerializer(), PydanticRequest(name='nekufa'))


def runner(serializer: Serializer, request: Any):
    serialized = serializer.serialize(request)
    parsed = serializer.deserialize(type(request), serialized)
    assert isinstance(parsed, type(request))
    assert parsed.name == request.name
