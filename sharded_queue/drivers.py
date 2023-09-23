from json import dumps, loads
from typing import Any, List, Sequence

from redis.asyncio import Redis

from sharded_queue.protocols import Lock, Serializer, Storage
from sharded_queue.settings import settings


class JsonTupleSerializer(Serializer):
    def get_values(self, request) -> list[Any]:
        if isinstance(request, Sequence):
            return [k for k in request]
        return list(request.__dict__.values())

    def serialize(self, request: Any) -> str:
        return dumps(self.get_values(request))

    def deserialize(self, cls: type[Any], source: str) -> Any:
        values = loads(source)
        if hasattr(cls, 'model_fields'):
            return cls(**dict(zip(cls.model_fields, values)))

        return cls(*values)


class RuntimeLock(Lock):
    def __init__(self) -> None:
        self.storage: dict[str, bool] = {}

    async def acquire(self, key: str) -> bool:
        if key in self.storage:
            return False
        self.storage[key] = True
        return True

    async def exists(self, key: str) -> bool:
        return key in self.storage

    async def release(self, key: str) -> None:
        del self.storage[key]

    async def ttl(self, key: str, ttl: int) -> bool:
        if ttl == 0:
            await self.release(key)
            return True
        return await self.exists(key)


class RuntimeStorage(Storage):
    data: dict[str, List[str]]

    def __init__(self) -> None:
        self.data = {}

    async def append(self, tube: str, *msgs: str) -> int:
        if tube not in self.data:
            self.data[tube] = list(msgs)
        else:
            self.data[tube].extend(list(msgs))

        return len(self.data[tube])

    async def contains(self, tube: str, msg: str) -> bool:
        return tube in self.data and msg in self.data[tube]

    async def length(self, tube: str) -> int:
        return len(self.data[tube]) if tube in self.data else 0

    async def pop(self, tube: str, max: int) -> list[str]:
        res = await self.range(tube, max)
        if len(res):
            self.data[tube] = self.data[tube][len(res):]
        return res

    async def pipes(self) -> list[str]:
        return list(self.data.keys())

    async def range(self, tube: str, max: int) -> list[str]:
        return self.data[tube][0:max] if tube in self.data else []


class RedisLock(Lock):
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def acquire(self, key: str) -> bool:
        return None is not await self.redis.set(
            name=settings.lock_prefix + key,
            ex=settings.lock_timeout,
            nx=True,
            value=1,
        )

    async def exists(self, key: str) -> bool:
        checker = await self.redis.exists(
            settings.lock_prefix + key
        )
        return bool(checker)

    async def release(self, key: str) -> None:
        await self.redis.delete(settings.lock_prefix + key)

    async def ttl(self, key: str, ttl: int) -> bool:
        setter = await self.redis.set(
            settings.lock_prefix + key,
            value=key,
            ex=ttl,
            xx=True
        )
        return bool(setter)


class RedisStorage(Storage):
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def append(self, tube: str, *msgs: str) -> int:
        return await self.redis.rpush(self.key(tube), *msgs)

    async def contains(self, tube: str, msg: str) -> bool:
        return await self.redis.lpos(self.key(tube), msg) is not None

    def key(self, tube):
        return settings.tube_prefix + tube

    async def length(self, tube: str) -> int:
        return await self.redis.llen(self.key(tube))

    async def pipes(self) -> list[str]:
        return [
            key[len(settings.tube_prefix):]
            for key in await self.redis.keys(self.key('*'))
        ]

    async def pop(self, tube: str, max: int) -> list[str]:
        return await self.redis.lpop(self.key(tube), max) or []

    async def range(self, tube: str, max: int) -> list[str]:
        return await self.redis.lrange(self.key(tube), 0, max-1) or []
