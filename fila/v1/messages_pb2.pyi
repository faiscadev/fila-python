import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Message(_message.Message):
    __slots__ = ("id", "headers", "payload", "metadata", "timestamps")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMPS_FIELD_NUMBER: _ClassVar[int]
    id: str
    headers: _containers.ScalarMap[str, str]
    payload: bytes
    metadata: MessageMetadata
    timestamps: MessageTimestamps
    def __init__(self, id: _Optional[str] = ..., headers: _Optional[_Mapping[str, str]] = ..., payload: _Optional[bytes] = ..., metadata: _Optional[_Union[MessageMetadata, _Mapping]] = ..., timestamps: _Optional[_Union[MessageTimestamps, _Mapping]] = ...) -> None: ...

class MessageMetadata(_message.Message):
    __slots__ = ("fairness_key", "weight", "throttle_keys", "attempt_count", "queue_id")
    FAIRNESS_KEY_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_FIELD_NUMBER: _ClassVar[int]
    THROTTLE_KEYS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_COUNT_FIELD_NUMBER: _ClassVar[int]
    QUEUE_ID_FIELD_NUMBER: _ClassVar[int]
    fairness_key: str
    weight: int
    throttle_keys: _containers.RepeatedScalarFieldContainer[str]
    attempt_count: int
    queue_id: str
    def __init__(self, fairness_key: _Optional[str] = ..., weight: _Optional[int] = ..., throttle_keys: _Optional[_Iterable[str]] = ..., attempt_count: _Optional[int] = ..., queue_id: _Optional[str] = ...) -> None: ...

class MessageTimestamps(_message.Message):
    __slots__ = ("enqueued_at", "leased_at")
    ENQUEUED_AT_FIELD_NUMBER: _ClassVar[int]
    LEASED_AT_FIELD_NUMBER: _ClassVar[int]
    enqueued_at: _timestamp_pb2.Timestamp
    leased_at: _timestamp_pb2.Timestamp
    def __init__(self, enqueued_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., leased_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
