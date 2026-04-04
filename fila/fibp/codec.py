"""Encode/decode functions for every FIBP opcode."""

from __future__ import annotations

from dataclasses import dataclass

from fila.fibp.primitives import Reader, Writer

# ---------------------------------------------------------------------------
# Data types used by decode functions
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class DeliveryMessage:
    """A single message within a Delivery frame."""

    message_id: str
    queue: str
    headers: dict[str, str]
    payload: bytes
    fairness_key: str
    weight: int
    throttle_keys: list[str]
    attempt_count: int
    enqueued_at: int
    leased_at: int


@dataclass(frozen=True, slots=True)
class EnqueueResultItem:
    """Per-message result within an EnqueueResult frame."""

    error_code: int
    message_id: str


@dataclass(frozen=True, slots=True)
class ErrorFrame:
    """Decoded Error frame."""

    code: int
    message: str
    metadata: dict[str, str]


@dataclass(frozen=True, slots=True)
class StatsResult:
    """Decoded GetStatsResult frame."""

    stats: dict[str, str]


@dataclass(frozen=True, slots=True)
class QueueInfo:
    """A single queue in ListQueuesResult."""

    name: str
    config: dict[str, str]


@dataclass(frozen=True, slots=True)
class ApiKeyInfo:
    """A single API key in ListApiKeysResult."""

    key_id: str
    prefix: str
    created_at: int


@dataclass(frozen=True, slots=True)
class AclEntry:
    """Decoded GetAclResult."""

    patterns: list[str]
    superadmin: bool


# ---------------------------------------------------------------------------
# Encode: Control
# ---------------------------------------------------------------------------

def encode_handshake(version: int, api_key: str | None = None) -> bytes:
    """Encode a Handshake frame body."""
    w = Writer()
    w.write_u16(version)
    w.write_optional_string(api_key)
    return w.finish()


def encode_pong() -> bytes:
    """Encode a Pong frame body (empty)."""
    return b""


def encode_disconnect() -> bytes:
    """Encode a Disconnect frame body (empty)."""
    return b""


# ---------------------------------------------------------------------------
# Decode: Control
# ---------------------------------------------------------------------------

def decode_handshake_ok(data: bytes) -> tuple[int, int, int]:
    """Decode a HandshakeOk frame body -> (version, node_id, max_frame_size)."""
    r = Reader(data)
    version = r.read_u16()
    node_id = r.read_u64()
    max_frame_size = r.read_u32()
    return version, node_id, max_frame_size


# ---------------------------------------------------------------------------
# Encode: Hot-path
# ---------------------------------------------------------------------------

def encode_enqueue(messages: list[dict[str, object]]) -> bytes:
    """Encode an Enqueue frame body.

    Each message dict has keys: queue (str), headers (dict[str,str]), payload (bytes).
    """
    w = Writer()
    w.write_u32(len(messages))
    for msg in messages:
        w.write_string(str(msg["queue"]))
        w.write_string_map(msg.get("headers") or {})  # type: ignore[arg-type]
        w.write_bytes(msg.get("payload", b"") or b"")  # type: ignore[arg-type]
    return w.finish()


def encode_consume(queue: str) -> bytes:
    """Encode a Consume frame body."""
    w = Writer()
    w.write_string(queue)
    return w.finish()


def encode_cancel_consume(consumer_id: str) -> bytes:
    """Encode a CancelConsume frame body."""
    w = Writer()
    w.write_string(consumer_id)
    return w.finish()


def encode_ack(items: list[dict[str, str]]) -> bytes:
    """Encode an Ack frame body. Each item: {queue, message_id}."""
    w = Writer()
    w.write_u32(len(items))
    for item in items:
        w.write_string(item["queue"])
        w.write_string(item["message_id"])
    return w.finish()


def encode_nack(items: list[dict[str, str]]) -> bytes:
    """Encode a Nack frame body. Each item: {queue, message_id, error}."""
    w = Writer()
    w.write_u32(len(items))
    for item in items:
        w.write_string(item["queue"])
        w.write_string(item["message_id"])
        w.write_string(item.get("error", ""))
    return w.finish()


# ---------------------------------------------------------------------------
# Decode: Hot-path
# ---------------------------------------------------------------------------

def decode_enqueue_result(data: bytes) -> list[EnqueueResultItem]:
    """Decode an EnqueueResult frame body."""
    r = Reader(data)
    count = r.read_u32()
    results: list[EnqueueResultItem] = []
    for _ in range(count):
        error_code = r.read_u8()
        message_id = r.read_string()
        results.append(EnqueueResultItem(error_code=error_code, message_id=message_id))
    return results


def decode_consume_ok(data: bytes) -> str:
    """Decode a ConsumeOk frame body -> consumer_id."""
    r = Reader(data)
    return r.read_string()


def decode_delivery(data: bytes) -> list[DeliveryMessage]:
    """Decode a Delivery frame body."""
    r = Reader(data)
    count = r.read_u32()
    messages: list[DeliveryMessage] = []
    for _ in range(count):
        msg_id = r.read_string()
        queue = r.read_string()
        headers = r.read_string_map()
        payload = r.read_bytes()
        fairness_key = r.read_string()
        weight = r.read_u32()
        throttle_keys = r.read_string_list()
        attempt_count = r.read_u32()
        enqueued_at = r.read_u64()
        leased_at = r.read_u64()
        messages.append(DeliveryMessage(
            message_id=msg_id,
            queue=queue,
            headers=headers,
            payload=payload,
            fairness_key=fairness_key,
            weight=weight,
            throttle_keys=throttle_keys,
            attempt_count=attempt_count,
            enqueued_at=enqueued_at,
            leased_at=leased_at,
        ))
    return messages


def decode_ack_result(data: bytes) -> list[int]:
    """Decode an AckResult frame body -> list of error codes."""
    r = Reader(data)
    count = r.read_u32()
    return [r.read_u8() for _ in range(count)]


def decode_nack_result(data: bytes) -> list[int]:
    """Decode a NackResult frame body -> list of error codes."""
    r = Reader(data)
    count = r.read_u32()
    return [r.read_u8() for _ in range(count)]


# ---------------------------------------------------------------------------
# Decode: Error
# ---------------------------------------------------------------------------

def decode_error(data: bytes) -> ErrorFrame:
    """Decode an Error frame body."""
    r = Reader(data)
    code = r.read_u8()
    message = r.read_string()
    metadata = r.read_string_map()
    return ErrorFrame(code=code, message=message, metadata=metadata)


# ---------------------------------------------------------------------------
# Encode: Admin
# ---------------------------------------------------------------------------

def encode_create_queue(name: str, config: dict[str, str] | None = None) -> bytes:
    """Encode a CreateQueue frame body."""
    w = Writer()
    w.write_string(name)
    w.write_string_map(config or {})
    return w.finish()


def encode_delete_queue(name: str) -> bytes:
    """Encode a DeleteQueue frame body."""
    w = Writer()
    w.write_string(name)
    return w.finish()


def encode_get_stats(queue: str) -> bytes:
    """Encode a GetStats frame body."""
    w = Writer()
    w.write_string(queue)
    return w.finish()


def encode_list_queues() -> bytes:
    """Encode a ListQueues frame body (empty)."""
    return b""


def encode_set_config(queue: str, config: dict[str, str]) -> bytes:
    """Encode a SetConfig frame body."""
    w = Writer()
    w.write_string(queue)
    w.write_string_map(config)
    return w.finish()


def encode_get_config(queue: str) -> bytes:
    """Encode a GetConfig frame body."""
    w = Writer()
    w.write_string(queue)
    return w.finish()


def encode_list_config(queue: str) -> bytes:
    """Encode a ListConfig frame body."""
    w = Writer()
    w.write_string(queue)
    return w.finish()


def encode_redrive(source_queue: str, dest_queue: str, count: int) -> bytes:
    """Encode a Redrive frame body."""
    w = Writer()
    w.write_string(source_queue)
    w.write_string(dest_queue)
    w.write_u32(count)
    return w.finish()


# ---------------------------------------------------------------------------
# Decode: Admin results
# ---------------------------------------------------------------------------

def _decode_simple_result(data: bytes) -> int:
    """Decode a simple result frame that contains just an error code."""
    r = Reader(data)
    return r.read_u8()


def decode_get_stats_result(data: bytes) -> StatsResult:
    """Decode a GetStatsResult frame body."""
    r = Reader(data)
    stats = r.read_string_map()
    return StatsResult(stats=stats)


def decode_list_queues_result(data: bytes) -> list[str]:
    """Decode a ListQueuesResult frame body -> list of queue names."""
    r = Reader(data)
    return r.read_string_list()


def decode_get_config_result(data: bytes) -> dict[str, str]:
    """Decode a GetConfigResult frame body -> config map."""
    r = Reader(data)
    return r.read_string_map()


def decode_list_config_result(data: bytes) -> dict[str, str]:
    """Decode a ListConfigResult frame body -> config map."""
    r = Reader(data)
    return r.read_string_map()


# ---------------------------------------------------------------------------
# Encode: Auth
# ---------------------------------------------------------------------------

def encode_create_api_key(name: str) -> bytes:
    """Encode a CreateApiKey frame body."""
    w = Writer()
    w.write_string(name)
    return w.finish()


def encode_revoke_api_key(key_id: str) -> bytes:
    """Encode a RevokeApiKey frame body."""
    w = Writer()
    w.write_string(key_id)
    return w.finish()


def encode_list_api_keys() -> bytes:
    """Encode a ListApiKeys frame body (empty)."""
    return b""


def encode_set_acl(key_id: str, patterns: list[str], superadmin: bool = False) -> bytes:
    """Encode a SetAcl frame body."""
    w = Writer()
    w.write_string(key_id)
    w.write_string_list(patterns)
    w.write_bool(superadmin)
    return w.finish()


def encode_get_acl(key_id: str) -> bytes:
    """Encode a GetAcl frame body."""
    w = Writer()
    w.write_string(key_id)
    return w.finish()


# ---------------------------------------------------------------------------
# Decode: Auth results
# ---------------------------------------------------------------------------

def decode_create_api_key_result(data: bytes) -> tuple[str, str]:
    """Decode a CreateApiKeyResult -> (key_id, raw_key)."""
    r = Reader(data)
    key_id = r.read_string()
    raw_key = r.read_string()
    return key_id, raw_key


def decode_list_api_keys_result(data: bytes) -> list[ApiKeyInfo]:
    """Decode a ListApiKeysResult -> list of ApiKeyInfo."""
    r = Reader(data)
    count = r.read_u16()
    keys: list[ApiKeyInfo] = []
    for _ in range(count):
        key_id = r.read_string()
        prefix = r.read_string()
        created_at = r.read_u64()
        keys.append(ApiKeyInfo(key_id=key_id, prefix=prefix, created_at=created_at))
    return keys


def decode_get_acl_result(data: bytes) -> AclEntry:
    """Decode a GetAclResult -> AclEntry."""
    r = Reader(data)
    patterns = r.read_string_list()
    superadmin = r.read_bool()
    return AclEntry(patterns=patterns, superadmin=superadmin)
