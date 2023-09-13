from typing import Any, Protocol, TypeVar

T = TypeVar('T')


class Lock(Protocol):
    async def acquire(self, tube: str) -> bool: ...
    async def release(self, tube: str) -> None: ...


class Serializer(Protocol[T]):
    def get_values(self, request) -> list[Any]: ...
    def serialize(self, request: T) -> str: ...
    def deserialize(self, cls: type[T], source: str) -> T: ...


class Storage(Protocol):
    async def append(self, tube: str, *msgs: str) -> int: ...
    async def contains(self, tube: str, msg: str) -> bool: ...
    async def length(self, tube: str) -> int: ...
    async def pipes(self) -> list[str]: ...
    async def pop(self, tube: str, max: int) -> list[str]: ...
    async def range(self, tube: str, max: int) -> list[str]: ...
