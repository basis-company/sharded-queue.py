from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LockSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='queue_lock_')

    prefix: str = Field(
        default="lock_",
        title="Lock key prefix"
    )

    timeout: int = Field(
        default=24*60*60,
        title="Lock key ttl"
    )


class StorageSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='queue_storage_')

    prefix: str = Field(
        default="tube_",
        title="Tube name prefix"
    )


class WorkerSettings(BaseSettings):

    acquire_delay: float = Field(
        default=1,
        title="Acquire delay in seconds on empty queues"
    )

    batch_size: int = Field(
        default=128,
        title='Batch processing size'
    )

    deferred_retry_delay: int = Field(
        default=1,
        title='Defereed tasks retry delay'
    )

    empty_limit: int = Field(
        default=16,
        title="Empty queue attempt limit berfore queue rebind",
    )

    empty_pause: float = Field(
        default=0.1,
        title="Pause in seconds on empty queue",
    )

    model_config = SettingsConfigDict(env_prefix='queue_worker_')

    recurrent_check_interval: int = Field(
        default=30,
        title='Recurrent interval check in seconds'
    )

    recurrent_tasks_limit: int = Field(
        default=1024,
        title='Recurrent tasks limit count'
    )
