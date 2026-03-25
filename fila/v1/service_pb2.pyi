from fila.v1 import messages_pb2 as _messages_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EnqueueErrorCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ENQUEUE_ERROR_CODE_UNSPECIFIED: _ClassVar[EnqueueErrorCode]
    ENQUEUE_ERROR_CODE_QUEUE_NOT_FOUND: _ClassVar[EnqueueErrorCode]
    ENQUEUE_ERROR_CODE_STORAGE: _ClassVar[EnqueueErrorCode]
    ENQUEUE_ERROR_CODE_LUA: _ClassVar[EnqueueErrorCode]
    ENQUEUE_ERROR_CODE_PERMISSION_DENIED: _ClassVar[EnqueueErrorCode]

class AckErrorCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACK_ERROR_CODE_UNSPECIFIED: _ClassVar[AckErrorCode]
    ACK_ERROR_CODE_MESSAGE_NOT_FOUND: _ClassVar[AckErrorCode]
    ACK_ERROR_CODE_STORAGE: _ClassVar[AckErrorCode]
    ACK_ERROR_CODE_PERMISSION_DENIED: _ClassVar[AckErrorCode]

class NackErrorCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NACK_ERROR_CODE_UNSPECIFIED: _ClassVar[NackErrorCode]
    NACK_ERROR_CODE_MESSAGE_NOT_FOUND: _ClassVar[NackErrorCode]
    NACK_ERROR_CODE_STORAGE: _ClassVar[NackErrorCode]
    NACK_ERROR_CODE_PERMISSION_DENIED: _ClassVar[NackErrorCode]
ENQUEUE_ERROR_CODE_UNSPECIFIED: EnqueueErrorCode
ENQUEUE_ERROR_CODE_QUEUE_NOT_FOUND: EnqueueErrorCode
ENQUEUE_ERROR_CODE_STORAGE: EnqueueErrorCode
ENQUEUE_ERROR_CODE_LUA: EnqueueErrorCode
ENQUEUE_ERROR_CODE_PERMISSION_DENIED: EnqueueErrorCode
ACK_ERROR_CODE_UNSPECIFIED: AckErrorCode
ACK_ERROR_CODE_MESSAGE_NOT_FOUND: AckErrorCode
ACK_ERROR_CODE_STORAGE: AckErrorCode
ACK_ERROR_CODE_PERMISSION_DENIED: AckErrorCode
NACK_ERROR_CODE_UNSPECIFIED: NackErrorCode
NACK_ERROR_CODE_MESSAGE_NOT_FOUND: NackErrorCode
NACK_ERROR_CODE_STORAGE: NackErrorCode
NACK_ERROR_CODE_PERMISSION_DENIED: NackErrorCode

class EnqueueMessage(_message.Message):
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

class EnqueueRequest(_message.Message):
    __slots__ = ("messages",)
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[EnqueueMessage]
    def __init__(self, messages: _Optional[_Iterable[_Union[EnqueueMessage, _Mapping]]] = ...) -> None: ...

class EnqueueResult(_message.Message):
    __slots__ = ("message_id", "error")
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message_id: str
    error: EnqueueError
    def __init__(self, message_id: _Optional[str] = ..., error: _Optional[_Union[EnqueueError, _Mapping]] = ...) -> None: ...

class EnqueueError(_message.Message):
    __slots__ = ("code", "message")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: EnqueueErrorCode
    message: str
    def __init__(self, code: _Optional[_Union[EnqueueErrorCode, str]] = ..., message: _Optional[str] = ...) -> None: ...

class EnqueueResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[EnqueueResult]
    def __init__(self, results: _Optional[_Iterable[_Union[EnqueueResult, _Mapping]]] = ...) -> None: ...

class ConsumeRequest(_message.Message):
    __slots__ = ("queue",)
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    queue: str
    def __init__(self, queue: _Optional[str] = ...) -> None: ...

class ConsumeResponse(_message.Message):
    __slots__ = ("messages",)
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[_messages_pb2.Message]
    def __init__(self, messages: _Optional[_Iterable[_Union[_messages_pb2.Message, _Mapping]]] = ...) -> None: ...

class AckMessage(_message.Message):
    __slots__ = ("queue", "message_id")
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    queue: str
    message_id: str
    def __init__(self, queue: _Optional[str] = ..., message_id: _Optional[str] = ...) -> None: ...

class AckRequest(_message.Message):
    __slots__ = ("messages",)
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[AckMessage]
    def __init__(self, messages: _Optional[_Iterable[_Union[AckMessage, _Mapping]]] = ...) -> None: ...

class AckResult(_message.Message):
    __slots__ = ("success", "error")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    success: AckSuccess
    error: AckError
    def __init__(self, success: _Optional[_Union[AckSuccess, _Mapping]] = ..., error: _Optional[_Union[AckError, _Mapping]] = ...) -> None: ...

class AckSuccess(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class AckError(_message.Message):
    __slots__ = ("code", "message")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: AckErrorCode
    message: str
    def __init__(self, code: _Optional[_Union[AckErrorCode, str]] = ..., message: _Optional[str] = ...) -> None: ...

class AckResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[AckResult]
    def __init__(self, results: _Optional[_Iterable[_Union[AckResult, _Mapping]]] = ...) -> None: ...

class NackMessage(_message.Message):
    __slots__ = ("queue", "message_id", "error")
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    queue: str
    message_id: str
    error: str
    def __init__(self, queue: _Optional[str] = ..., message_id: _Optional[str] = ..., error: _Optional[str] = ...) -> None: ...

class NackRequest(_message.Message):
    __slots__ = ("messages",)
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[NackMessage]
    def __init__(self, messages: _Optional[_Iterable[_Union[NackMessage, _Mapping]]] = ...) -> None: ...

class NackResult(_message.Message):
    __slots__ = ("success", "error")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    success: NackSuccess
    error: NackError
    def __init__(self, success: _Optional[_Union[NackSuccess, _Mapping]] = ..., error: _Optional[_Union[NackError, _Mapping]] = ...) -> None: ...

class NackSuccess(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class NackError(_message.Message):
    __slots__ = ("code", "message")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: NackErrorCode
    message: str
    def __init__(self, code: _Optional[_Union[NackErrorCode, str]] = ..., message: _Optional[str] = ...) -> None: ...

class NackResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[NackResult]
    def __init__(self, results: _Optional[_Iterable[_Union[NackResult, _Mapping]]] = ...) -> None: ...

class StreamEnqueueRequest(_message.Message):
    __slots__ = ("messages", "sequence_number")
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[EnqueueMessage]
    sequence_number: int
    def __init__(self, messages: _Optional[_Iterable[_Union[EnqueueMessage, _Mapping]]] = ..., sequence_number: _Optional[int] = ...) -> None: ...

class StreamEnqueueResponse(_message.Message):
    __slots__ = ("sequence_number", "results")
    SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    sequence_number: int
    results: _containers.RepeatedCompositeFieldContainer[EnqueueResult]
    def __init__(self, sequence_number: _Optional[int] = ..., results: _Optional[_Iterable[_Union[EnqueueResult, _Mapping]]] = ...) -> None: ...
