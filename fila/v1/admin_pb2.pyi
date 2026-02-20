from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateQueueRequest(_message.Message):
    __slots__ = ("name", "config")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    name: str
    config: QueueConfig
    def __init__(self, name: _Optional[str] = ..., config: _Optional[_Union[QueueConfig, _Mapping]] = ...) -> None: ...

class QueueConfig(_message.Message):
    __slots__ = ("on_enqueue_script", "on_failure_script", "visibility_timeout_ms")
    ON_ENQUEUE_SCRIPT_FIELD_NUMBER: _ClassVar[int]
    ON_FAILURE_SCRIPT_FIELD_NUMBER: _ClassVar[int]
    VISIBILITY_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    on_enqueue_script: str
    on_failure_script: str
    visibility_timeout_ms: int
    def __init__(self, on_enqueue_script: _Optional[str] = ..., on_failure_script: _Optional[str] = ..., visibility_timeout_ms: _Optional[int] = ...) -> None: ...

class CreateQueueResponse(_message.Message):
    __slots__ = ("queue_id",)
    QUEUE_ID_FIELD_NUMBER: _ClassVar[int]
    queue_id: str
    def __init__(self, queue_id: _Optional[str] = ...) -> None: ...

class DeleteQueueRequest(_message.Message):
    __slots__ = ("queue",)
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    queue: str
    def __init__(self, queue: _Optional[str] = ...) -> None: ...

class DeleteQueueResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SetConfigRequest(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: str
    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class SetConfigResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetConfigRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class GetConfigResponse(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class ConfigEntry(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: str
    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class ListConfigRequest(_message.Message):
    __slots__ = ("prefix",)
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    def __init__(self, prefix: _Optional[str] = ...) -> None: ...

class ListConfigResponse(_message.Message):
    __slots__ = ("entries", "total_count")
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[ConfigEntry]
    total_count: int
    def __init__(self, entries: _Optional[_Iterable[_Union[ConfigEntry, _Mapping]]] = ..., total_count: _Optional[int] = ...) -> None: ...

class GetStatsRequest(_message.Message):
    __slots__ = ("queue",)
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    queue: str
    def __init__(self, queue: _Optional[str] = ...) -> None: ...

class PerFairnessKeyStats(_message.Message):
    __slots__ = ("key", "pending_count", "current_deficit", "weight")
    KEY_FIELD_NUMBER: _ClassVar[int]
    PENDING_COUNT_FIELD_NUMBER: _ClassVar[int]
    CURRENT_DEFICIT_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_FIELD_NUMBER: _ClassVar[int]
    key: str
    pending_count: int
    current_deficit: int
    weight: int
    def __init__(self, key: _Optional[str] = ..., pending_count: _Optional[int] = ..., current_deficit: _Optional[int] = ..., weight: _Optional[int] = ...) -> None: ...

class PerThrottleKeyStats(_message.Message):
    __slots__ = ("key", "tokens", "rate_per_second", "burst")
    KEY_FIELD_NUMBER: _ClassVar[int]
    TOKENS_FIELD_NUMBER: _ClassVar[int]
    RATE_PER_SECOND_FIELD_NUMBER: _ClassVar[int]
    BURST_FIELD_NUMBER: _ClassVar[int]
    key: str
    tokens: float
    rate_per_second: float
    burst: float
    def __init__(self, key: _Optional[str] = ..., tokens: _Optional[float] = ..., rate_per_second: _Optional[float] = ..., burst: _Optional[float] = ...) -> None: ...

class GetStatsResponse(_message.Message):
    __slots__ = ("depth", "in_flight", "active_fairness_keys", "active_consumers", "quantum", "per_key_stats", "per_throttle_stats")
    DEPTH_FIELD_NUMBER: _ClassVar[int]
    IN_FLIGHT_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FAIRNESS_KEYS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_CONSUMERS_FIELD_NUMBER: _ClassVar[int]
    QUANTUM_FIELD_NUMBER: _ClassVar[int]
    PER_KEY_STATS_FIELD_NUMBER: _ClassVar[int]
    PER_THROTTLE_STATS_FIELD_NUMBER: _ClassVar[int]
    depth: int
    in_flight: int
    active_fairness_keys: int
    active_consumers: int
    quantum: int
    per_key_stats: _containers.RepeatedCompositeFieldContainer[PerFairnessKeyStats]
    per_throttle_stats: _containers.RepeatedCompositeFieldContainer[PerThrottleKeyStats]
    def __init__(self, depth: _Optional[int] = ..., in_flight: _Optional[int] = ..., active_fairness_keys: _Optional[int] = ..., active_consumers: _Optional[int] = ..., quantum: _Optional[int] = ..., per_key_stats: _Optional[_Iterable[_Union[PerFairnessKeyStats, _Mapping]]] = ..., per_throttle_stats: _Optional[_Iterable[_Union[PerThrottleKeyStats, _Mapping]]] = ...) -> None: ...

class RedriveRequest(_message.Message):
    __slots__ = ("dlq_queue", "count")
    DLQ_QUEUE_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    dlq_queue: str
    count: int
    def __init__(self, dlq_queue: _Optional[str] = ..., count: _Optional[int] = ...) -> None: ...

class RedriveResponse(_message.Message):
    __slots__ = ("redriven",)
    REDRIVEN_FIELD_NUMBER: _ClassVar[int]
    redriven: int
    def __init__(self, redriven: _Optional[int] = ...) -> None: ...

class ListQueuesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class QueueInfo(_message.Message):
    __slots__ = ("name", "depth", "in_flight", "active_consumers")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DEPTH_FIELD_NUMBER: _ClassVar[int]
    IN_FLIGHT_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_CONSUMERS_FIELD_NUMBER: _ClassVar[int]
    name: str
    depth: int
    in_flight: int
    active_consumers: int
    def __init__(self, name: _Optional[str] = ..., depth: _Optional[int] = ..., in_flight: _Optional[int] = ..., active_consumers: _Optional[int] = ...) -> None: ...

class ListQueuesResponse(_message.Message):
    __slots__ = ("queues",)
    QUEUES_FIELD_NUMBER: _ClassVar[int]
    queues: _containers.RepeatedCompositeFieldContainer[QueueInfo]
    def __init__(self, queues: _Optional[_Iterable[_Union[QueueInfo, _Mapping]]] = ...) -> None: ...
