from datetime import datetime, timedelta
from typing import Generic, NamedTuple, Optional, Self, TypeVar, get_type_hints

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
            timestamp = timestamp + delta

        return timestamp


class RecurrentRequest(NamedTuple):
    interval: float
    pipe: str
    msg: list

    @classmethod
    def get_interval(cls, interval: int | float | timedelta) -> float:
        if isinstance(interval, timedelta):
            return float(interval.seconds)

        return float(interval)
