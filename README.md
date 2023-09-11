# Sharded queue

## Introduction

This library can be used to distribute your job queue using sharding technique.\
Any handler consists of the handler and route method that defines your request routing.\
`Thread` meaning subqueue of a handler with strict fifo order.\
`Order` segment can be used for request priority management inside your thread subqueue.\
All code is written using batch approach to reduce io latency per each message.

## Getting started

Install library.
```pip install sharded-queue```

Describe your handler.
```py
from repositories import UserRepository
from sharded_queue import Handler, Queue, RequestRoute, Route
from services import construct_message, mailer

class NotifyRequest:
    '''
    This is your single handler request
    '''
    user_id: int

class NotifyHandler(Handler):
    @classmethod
    async def route(cls, *requests: NotifyRequest):
        '''
        Spread requests by 3 threads that can be concurrently processed
        '''
        return [
            RequestRoute(request, Route(thread=str(request.user_id % 3)))
            for request in requests
        ]

    async def perform(self, *requests: NotifyRequest):
        '''
        Perform is called using configurable batch size
        This allows you to reduce io per single request
        '''
        users = await UserRepository.find_all([r.user_id for r in requests])

        await mailer.send_all([construct_message(user) for user in users])


async def main():
    queue = Queue()
    await queue.register(
        NotifyHandler,
        NotifyRequest(1),
        NotifyRequest(2),
        NotifyRequest(3),
        NotifyRequest(4),
        NotifyRequest(5),
        NotifyRequest(6),
        NotifyRequest(7),
        NotifyRequest(8),
        NotifyRequest(9),
    )

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

## Handler boostrap

When a worker will bind to queue it created async context that is used to do bootstrap and shutdown routines.

```py
class BucketRequest:
    bucket: int

class SyncBucketHandler(Handler):
    async def start(self):
        '''
        perform any tasks before perform would be called
        '''
    async def perform(self, *requests: BucketRequest):
        pass

    async def stop(self):
        '''
        perform any tasks after perform would be called
        '''
```

## Queue configuration
You can configure sharded queue using env.
- `QUEUE_COORDINATOR_DELAY=1` Coordinator delay in seconds on empty queues
- `QUEUE_DEFAULT_ORDER='0'` Default queue order
- `QUEUE_DEFAULT_THREAD='0'` Default queue thread
- `QUEUE_WORKER_BATCH_SIZE=128` Worker batch processing size
- `QUEUE_WORKER_EMPTY_LIMIT=16` Worker empty queue attempt limit berfore queue rebind
- `QUEUE_WORKER_EMPTY_PAUSE=0.1` Worker pause in seconds on empty queue

Or import and settings object:
```py
from sharded_queue import settings
settings.coordinator_delay = 5
settings.worker_batch_size = 64

worker = Worker(Queue())
await worker.loop()

```