from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ShardedQueueSettings(BaseSettings):
    default_priority: int = Field(
        default='0',
        title='Default queue priority'
    )

    default_thread: int = Field(
        default='0',
        title='Default queue thread'
    )

    deferred_retry_delay: int = Field(
        default=1,
        title='Defereed tasks retry delay'
    )

    lock_prefix: str = Field(
        default="lock_",
        title="Lock key prefix"
    )

    lock_timeout: int = Field(
        default=24*60*60,
        title="Lock key ttl"
    )

    model_config = SettingsConfigDict(env_prefix='queue_')

    recurrent_check_interval: int = Field(
        default=30,
        title='Recurrent interval check in seconds'
    )

    recurrent_tasks_limit: int = Field(
        default=1024,
        title='Recurrent tasks limit count'
    )

    tube_prefix: str = Field(
        default="tube_",
        title="Queue prefix"
    )

    worker_acquire_delay: float = Field(
        default=1,
        title="Worker acquire delay in seconds on empty queues"
    )

    worker_batch_size: int = Field(
        default=128,
        title='Worker batch processing size'
    )

    worker_empty_limit: int = Field(
        default=16,
        title="Worker empty queue attempt limit berfore queue rebind",
    )

    worker_empty_pause: float = Field(
        default=0.1,
        title="Worker pause in seconds on empty queue",
    )


settings = ShardedQueueSettings()
