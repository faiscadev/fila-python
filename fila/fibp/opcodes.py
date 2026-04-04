"""FIBP opcode constants, error codes, and frame header definition."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

# ---------------------------------------------------------------------------
# Protocol constants
# ---------------------------------------------------------------------------

PROTOCOL_VERSION: int = 1
DEFAULT_MAX_FRAME_SIZE: int = 16 * 1024 * 1024  # 16 MiB
FRAME_HEADER_SIZE: int = 6  # opcode(1) + flags(1) + request_id(4)
CONTINUATION_FLAG: int = 0x01


# ---------------------------------------------------------------------------
# Opcodes
# ---------------------------------------------------------------------------

class Opcode(IntEnum):
    """FIBP opcodes."""

    # Control
    HANDSHAKE = 0x01
    HANDSHAKE_OK = 0x02
    PING = 0x03
    PONG = 0x04
    DISCONNECT = 0x05

    # Hot-path
    ENQUEUE = 0x10
    ENQUEUE_RESULT = 0x11
    CONSUME = 0x12
    DELIVERY = 0x13
    CANCEL_CONSUME = 0x14
    ACK = 0x15
    ACK_RESULT = 0x16
    NACK = 0x17
    NACK_RESULT = 0x18
    CONSUME_OK = 0x19

    # Error
    ERROR = 0xFE

    # Admin
    CREATE_QUEUE = 0xFD
    CREATE_QUEUE_RESULT = 0xFC
    DELETE_QUEUE = 0xFB
    DELETE_QUEUE_RESULT = 0xFA
    GET_STATS = 0xF9
    GET_STATS_RESULT = 0xF8
    LIST_QUEUES = 0xF7
    LIST_QUEUES_RESULT = 0xF6
    SET_CONFIG = 0xF5
    SET_CONFIG_RESULT = 0xF4
    GET_CONFIG = 0xF3
    GET_CONFIG_RESULT = 0xF2
    LIST_CONFIG = 0xF1
    LIST_CONFIG_RESULT = 0xF0
    REDRIVE = 0xEF
    REDRIVE_RESULT = 0xEE

    # Auth
    CREATE_API_KEY = 0xED
    CREATE_API_KEY_RESULT = 0xEC
    REVOKE_API_KEY = 0xEB
    REVOKE_API_KEY_RESULT = 0xEA
    LIST_API_KEYS = 0xE9
    LIST_API_KEYS_RESULT = 0xE8
    SET_ACL = 0xE7
    SET_ACL_RESULT = 0xE6
    GET_ACL = 0xE5
    GET_ACL_RESULT = 0xE4


# ---------------------------------------------------------------------------
# Error codes
# ---------------------------------------------------------------------------

class ErrorCode(IntEnum):
    """FIBP error codes returned in Error frames and per-item results."""

    OK = 0x00
    QUEUE_NOT_FOUND = 0x01
    MESSAGE_NOT_FOUND = 0x02
    QUEUE_ALREADY_EXISTS = 0x03
    INVALID_ARGUMENT = 0x04
    DEADLINE_EXCEEDED = 0x05
    PERMISSION_DENIED = 0x06
    RESOURCE_EXHAUSTED = 0x07
    PRECONDITION_FAILED = 0x08
    ABORTED = 0x09
    UNAVAILABLE = 0x0A
    UNAUTHENTICATED = 0x0B
    NOT_LEADER = 0x0C
    LUA_ERROR = 0x0D
    CHANNEL_FULL = 0x0E
    FORBIDDEN = 0x0F
    API_KEY_NOT_FOUND = 0x10
    ACL_NOT_FOUND = 0x11
    INTERNAL_ERROR = 0xFF


# ---------------------------------------------------------------------------
# Frame header
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class FrameHeader:
    """Parsed 6-byte FIBP frame header."""

    opcode: int
    flags: int
    request_id: int

    @property
    def is_continuation(self) -> bool:
        return bool(self.flags & CONTINUATION_FLAG)
