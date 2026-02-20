from fila.v1 import messages_pb2 as _messages_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EnqueueRequest(_message.Message):
    __slots__ = ("queue", "headers", "payload")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    queue: str
    headers: _containers.ScalarMap[str, str]
    payload: bytes
    def __init__(self, queue: _Optional[str] = ..., headers: _Optional[_Mapping[str, str]] = ..., payload: _Optional[bytes] = ...) -> None: ...

class EnqueueResponse(_message.Message):
    __slots__ = ("message_id",)
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    message_id: str
    def __init__(self, message_id: _Optional[str] = ...) -> None: ...

class ConsumeRequest(_message.Message):
    __slots__ = ("queue",)
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    queue: str
    def __init__(self, queue: _Optional[str] = ...) -> None: ...

class ConsumeResponse(_message.Message):
    __slots__ = ("message",)
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: _messages_pb2.Message
    def __init__(self, message: _Optional[_Union[_messages_pb2.Message, _Mapping]] = ...) -> None: ...

class AckRequest(_message.Message):
    __slots__ = ("queue", "message_id")
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    queue: str
    message_id: str
    def __init__(self, queue: _Optional[str] = ..., message_id: _Optional[str] = ...) -> None: ...

class AckResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class NackRequest(_message.Message):
    __slots__ = ("queue", "message_id", "error")
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    queue: str
    message_id: str
    error: str
    def __init__(self, queue: _Optional[str] = ..., message_id: _Optional[str] = ..., error: _Optional[str] = ...) -> None: ...

class NackResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
