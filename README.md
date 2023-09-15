# Sharded queue
[![CI](https://github.com/basis-company/sharded-queue.py/workflows/Test/badge.svg?event=push)](https://github.com/basis-company/sharded-queue.py/actions/workflows/test.yml?query=event%3Apush)
[![pypi](https://img.shields.io/pypi/v/sharded-queue.svg)](https://pypi.python.org/pypi/sharded-queue)
[![codecov](https://codecov.io/gh/basis-company/sharded-queue.py/graph/badge.svg?token=Y4EDPMQ6UG)](https://codecov.io/gh/basis-company/sharded-queue.py)
[![downloads](https://static.pepy.tech/badge/sharded-queue/month)](https://pepy.tech/project/sharded-queue)
[![license](https://img.shields.io/github/license/basis-company/sharded-queue.py.svg)](https://github.com/basis-company/sharded-queue.py/blob/master/LICENSE)

## Introduction

Imagine your job queue operates at very high rps and needs distribution over multiple workers. But you need to keep context-sensitive requests in same thread and manage thread request processing priority. In other words, sharded queue is a queue with sub-queues inside. Tasks are executed in FIFO order and you define how to route them correctly per handler basis.

## Installation
Install using pip
```
pip install sharded-queue
```

## Getting started
There are some roles that you need to understand:
- `request` a simple message that should be delivered to a handler
- `handler` request handler that performs the job
- `route` defines internal queue that is used for request distribution
    - `thread` a group of context-sensitive requests
    - `priority` can be used to sort requests inside the thread

Let's start with a simple notification task that is shared by 3 threads and there are no priorities. Notice, that handler methods are written using batch approach to reduce io latency per each message.
```py
from sharded_queue import Handler, Queue, Route

class NotifyRequest(NamedTuple):
    '''
    In this example we have simple notify request containing user identifier
    In addition, the value is used to shard requests over worker threads
    '''
    user_id: int

class NotifyHandler(Handler):
    @classmethod
    async def route(cls, *requests: NotifyRequest) -> list[Route]:
        '''
        Spread requests by 3 threads that can be concurrently processed
        '''
        return [
            Route(thread=request.user_id % 3)
            for request in requests
        ]

    async def perform(self, *requests: NotifyRequest) -> None:
        '''
        Perform is called using configurable batch size
        This allows you to reduce io per single request
        '''
        # users = await UserRepository.find_all([r.user_id for r in requests])
        # await mailer.send_all([construct_message(user) for user in users])
```

## Usage example

When a handler is described you can use queue and worker api to manage and process tasks. Let's describe runtime components:
- `lock` helps worker bind to the queue
- `queue` is used to register requests
- `storage` a database containing queue data
- `worker` performs requests using handler

```py
from asyncio import gather
from notifications import NotifyHandler, NotifyRequest
from sharded_queue import Queue, Worker
from sharded_queue.drivers import RuntimeLock, RuntimeStorage


async def example():
    '''
    let's register notification for first 9 users
    '''
    queue: Queue = Queue(RuntimeStorage())
    await queue.register(NotifyHandler, *[NotifyRequest(n) for n in range(1, 9)])
    '''
    now all requests are waiting for workers on 3 notify handler tubes
    they were distributed using route handler method
    first tube contains notify request for users 1, 4, 7
    second tube contains requests for 2, 5, 8 and other goes to third tube
    '''
    futures = [
        Worker(RuntimeLock(), queue).loop(3),
        Worker(RuntimeLock(), queue).loop(3),
        Worker(RuntimeLock(), queue).loop(3),
    ]
    '''
    we've just run three coroutines that will process messages
    workers will bind to each thread and process all messages
    '''
    await gather(*futures)
    '''
    now all emails were send in 3 threads
    '''
```

## Drivers
There are several implementations of components:
- `RedisLock` persist locks in redis using setnx api
- `RedisStorage` persist msgs using lists and lrange/lpop/rpush api
- `RuntimeLock` persist locks in memory (process in-memory distribution)
- `RuntimeStorage` persist msgs in dict (process in-memory distribution)

## Handler lifecycle

As you can notice, routing is made using static method, but perform is an instance method. When a worker start processing requests it can bootstrap and tear down the handler using `start` and `stop` methods

```py
class ParseEventRequest(NamedTuple):
    '''
    Event identifier should be enough to get it contents from storage
    '''
    event: int

class ParseEventHandler(Handler):
    @classmethod
    async def create(cls) -> Self:
        '''
        define your own handler and dependency factory
        '''
        return cls()

    @classmethod
    async def route(cls, *requests: ParseEventRequest) -> list[Route]:
        '''
        override default single thread tube
        '''
        return [
            Route(settings.default_thread, settings.default_priority)
            for request in requests
        ]

    async def start(self):
        '''
        run any code on worker is bind to the queue
        '''

    async def perform(self, *requests: ParseEventRequest):
        '''
        the handler
        '''

    async def handle(self, *requests: ParseEventRequest) -> None:
        '''
        process requests batch
        '''

    async def stop(self):
        '''
        run any code when queue is empty and worker stops processing thread
        '''
```
## Deduplication
There is an optional if_not_exists flag. If it is set, request will be registered only if not persent in a queue
```py
await queue.register(SycBucket, Bucket(7756527), if_not_exists=True)
await queue.register(SycBucket, Bucket(7756527), if_not_exists=True)
```
## Delay
You can use built-in task backlog to delay handler call
```py
await queue.register(Housekeep, Room(402), delay=5)  # numeric means seconds
await queue.register(Housekeep, Room(324), delay=timedelta(minutes=15))
```
## Performance
Performance dependends on many factors, we can only measure clean library overhead with in-memory storages. You can run performance on your hardware with `pytest -s`, with this option performance test will print result for different cases. Perfomance test on intel i5-4670K, Ubuntu 23.04 LTS using Python 3.11.4 gives us about `200_000` rps for batch request registration with sharding and about `600_000` requests for request handling in concurrent mode.

## Advanced queue configuration
You can configure sharded queue using env
- `QUEUE_BACKLOG_RETRY_DELAY = 1`\
Backlog retry delay
- `QUEUE_DEFAULT_PRIORITY = 0`\
Default queue priority
- `QUEUE_DEFAULT_THREAD = 0`\
Default queue thread
- `QUEUE_LOCK_PREFIX = 'lock_'`\
Lock key prefix
- `QUEUE_LOCK_TIMEOUT = 24 * 60 * 60`\
Lock key ttl
- `QUEUE_TUBE_PREFIX = 'tube_'`\
Default queue prefix
- `QUEUE_WORKER_ACQUIRE_DELAY = 1`\
Worker acquire delay in seconds on empty queues
- `QUEUE_WORKER_BATCH_SIZE = 128`\
Worker batch processing size
- `QUEUE_WORKER_EMPTY_LIMIT = 16`\
Worker empty queue attempt limit berfore queue rebind
- `QUEUE_WORKER_EMPTY_PAUSE = 0.1`\
Worker pause in seconds on empty queue

You can import and change settings manually
```py
from sharded_queue.settings import settings
from sharded_queue import Queue, Worker
from sharded_queue.drivers import RuntimeLock RuntimeStorage
settings.worker_acquire_delay = 5
settings.worker_batch_size = 64

worker = Worker(RuntimeLock(), Queue(RuntimeStorage()))
await worker.loop()

```