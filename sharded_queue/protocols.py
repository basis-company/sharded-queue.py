from typing import Protocol, TypeVar

T = TypeVar('T')


class Serializer(Protocol[T]):
    def serialize(self, request: T) -> str:
        raise NotImplementedError

    def unserialize(self, cls: type[T], source: str) -> T:
        raise NotImplementedError


class Storage(Protocol):
    async def append(self, tube: str, *msgs: str) -> int:
        raise NotImplementedError

    async def length(self, tube: str) -> int:
        raise NotImplementedError

    async def pipes(self) -> list[str]:
        raise NotImplementedError

    async def pop(self, tube: str, max: int) -> list[str]:
        raise NotImplementedError

    async def range(self, tube: str, max: int) -> list[str]:
        raise NotImplementedError


class Lock(Protocol):
    async def acquire(self, tube: str) -> bool:
        raise NotImplementedError

    async def release(self, tube: str) -> None:
        raise NotImplementedError
