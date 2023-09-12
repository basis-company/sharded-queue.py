# Sharded queue

## Introduction

Imagine your job queue operates at very high rps and needs distribution over multiple workers. But you need to keep context-sensitive requests in same thread and manage thread request processing priority. In other words, sharded queue is a queue with sub-queues inside. Tasks are executed in FIFO order due priority and you define how to route them correctly per handler basis.

There are some roles that you need to understand:
- `request` a simple message that should be delivered to a handler
- `handler` request handler that performs the job
- `route` an object that defines request thread and priority

Runtime consist of several components:
- `queue` helps you to register requests to a handler
- `storage` a database that holds all queue data
- `serializer` converts your request to str and restore to objects
- `worker` performs requests using handler
- `coordinator` is mechanism that helps worker findout the queue

## Installation
Install using pip
```
pip install sharded-queue
```

## Getting started
First of all you need to define your handler. Handler methods are written using batch approach to reduce io latency per each message. Let's start with a simple notification task.
```py
from sharded_queue import Handler, Queue, Route

class NotifyRequest:
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
            Route(thread=str(request.user_id % 3))
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

When a handler is described you can use queue and worker api to manage and process tasks.
```py
from notifications import NotifyHandler, NotifyRequest


async def main():
    queue = Queue(RuntimeStorage())

    # let's register notification for first 9 users
    await queue.register(NotifyHandler, *[NotifyRequest(n) for n in range(1, 9)])

    # now all requests are waiting for workers on 3 notify handler tubes
    # first tube contains notify request for users 1, 4, 7
    # second tube contains requests for 2, 5, 8 and so on
    # they were distributed using route handler method

    worker = Worker(queue)
    # we can run worker with processed message limit
    # in this example we will run three coroutines that will process messages
    # workers will bind to any tube and process all 3 messages
    # in advance, you can run workers on a distributed system
    futures = [
        worker.loop(3),
        worker.loop(3),
        worker.loop(3),
    ]

    # now all emails were send
    await gather(*futures)
```

## Routes

`route` method returns an array of routes, each route is defind using:
- `thread` - requests pipe that uses strict order prcessing
- `priority` - define priority for you requests inside a thread

## Handlers

As you can notice, routing is made using static method, but perform is an instance method. When a worker start processing requests it can bootstrap and tear down the handler

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
        ```

    async def stop(self):
        '''
        run any code when queue is empty and worker stops processing thread
        '''
```
## Queue configuration
You can configure sharded queue using env.
- `QUEUE_COORDINATOR_DELAY=1` Coordinator delay in seconds on empty queues
- `QUEUE_DEFAULT_priority='0'` Default queue priority
- `QUEUE_DEFAULT_THREAD='0'` Default queue thread
- `QUEUE_TUBE_PREFIX='tube_'` Default queue prefix
- `QUEUE_WORKER_BATCH_SIZE=128` Worker batch processing size
- `QUEUE_WORKER_EMPTY_LIMIT=16` Worker empty queue attempt limit berfore queue rebind
- `QUEUE_WORKER_EMPTY_PAUSE=0.1` Worker pause in seconds on empty queue

Or import and change settings object:
```py
from sharded_queue import settings
settings.coordinator_delay = 5
settings.worker_batch_size = 64

worker = Worker(Queue(RuntimeStorage()))
await worker.loop()

```