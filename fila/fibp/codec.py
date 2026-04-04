"""Encode/decode functions for every FIBP opcode."""

from __future__ import annotations

from dataclasses import dataclass, field

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
class CreateQueueResultFrame:
    """Decoded CreateQueueResult."""

    error_code: int
    queue_id: str


@dataclass(frozen=True, slots=True)
class FairnessKeyStat:
    """Per-fairness-key stats in GetStatsResult."""

    key: str
    pending_count: int
    current_deficit: int
    weight: int


@dataclass(frozen=True, slots=True)
class ThrottleKeyStat:
    """Per-throttle-key stats in GetStatsResult."""

    key: str
    tokens: float
    rate_per_second: float
    burst: float


@dataclass(frozen=True, slots=True)
class StatsResultFrame:
    """Decoded GetStatsResult frame."""

    error_code: int
    depth: int
    in_flight: int
    active_fairness_keys: int
    active_consumers: int
    quantum: int
    leader_node_id: int
    replication_count: int
    per_key_stats: list[FairnessKeyStat] = field(default_factory=list)
    per_throttle_stats: list[ThrottleKeyStat] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class ListQueuesQueueInfo:
    """A single queue in ListQueuesResult."""

    name: str
    depth: int
    in_flight: int
    active_consumers: int
    leader_node_id: int


@dataclass(frozen=True, slots=True)
class ListQueuesResultFrame:
    """Decoded ListQueuesResult."""

    error_code: int
    cluster_node_count: int
    queues: list[ListQueuesQueueInfo]


@dataclass(frozen=True, slots=True)
class ApiKeyInfoFrame:
    """A single API key in ListApiKeysResult."""

    key_id: str
    name: str
    created_at: int
    expires_at: int
    is_superadmin: bool


@dataclass(frozen=True, slots=True)
class AclPermission:
    """A single ACL permission."""

    kind: str
    pattern: str


@dataclass(frozen=True, slots=True)
class GetAclResultFrame:
    """Decoded GetAclResult."""

    error_code: int
    key_id: str
    is_superadmin: bool
    permissions: list[AclPermission]


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


def encode_create_queue(
    name: str,
    *,
    on_enqueue_script: str | None = None,
    on_failure_script: str | None = None,
    visibility_timeout_ms: int = 0,
) -> bytes:
    """Encode a CreateQueue frame body.

    Wire format: [string name][optional<string> on_enqueue_script]
                 [optional<string> on_failure_script][u64 visibility_timeout_ms]
    """
    w = Writer()
    w.write_string(name)
    w.write_optional_string(on_enqueue_script)
    w.write_optional_string(on_failure_script)
    w.write_u64(visibility_timeout_ms)
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


def encode_set_config(key: str, value: str) -> bytes:
    """Encode a SetConfig frame body. Wire: [string key][string value]."""
    w = Writer()
    w.write_string(key)
    w.write_string(value)
    return w.finish()


def encode_get_config(key: str) -> bytes:
    """Encode a GetConfig frame body. Wire: [string key]."""
    w = Writer()
    w.write_string(key)
    return w.finish()


def encode_list_config(prefix: str) -> bytes:
    """Encode a ListConfig frame body. Wire: [string prefix]."""
    w = Writer()
    w.write_string(prefix)
    return w.finish()


def encode_redrive(dlq_queue: str, count: int) -> bytes:
    """Encode a Redrive frame body. Wire: [string dlq_queue][u64 count]."""
    w = Writer()
    w.write_string(dlq_queue)
    w.write_u64(count)
    return w.finish()


# ---------------------------------------------------------------------------
# Decode: Admin results
# ---------------------------------------------------------------------------


def decode_create_queue_result(data: bytes) -> CreateQueueResultFrame:
    """Decode a CreateQueueResult. Wire: [u8 error_code][string queue_id]."""
    r = Reader(data)
    error_code = r.read_u8()
    queue_id = r.read_string()
    return CreateQueueResultFrame(error_code=error_code, queue_id=queue_id)


def decode_delete_queue_result(data: bytes) -> int:
    """Decode a DeleteQueueResult -> error_code."""
    r = Reader(data)
    return r.read_u8()


def decode_get_stats_result(data: bytes) -> StatsResultFrame:
    """Decode a GetStatsResult frame body."""
    r = Reader(data)
    error_code = r.read_u8()
    depth = r.read_u64()
    in_flight = r.read_u64()
    active_fairness_keys = r.read_u64()
    active_consumers = r.read_u32()
    quantum = r.read_u32()
    leader_node_id = r.read_u64()
    replication_count = r.read_u32()

    per_key_count = r.read_u16()
    per_key_stats: list[FairnessKeyStat] = []
    for _ in range(per_key_count):
        key = r.read_string()
        pending = r.read_u64()
        deficit = r.read_i64()
        weight = r.read_u32()
        per_key_stats.append(FairnessKeyStat(
            key=key, pending_count=pending, current_deficit=deficit, weight=weight
        ))

    per_throttle_count = r.read_u16()
    per_throttle_stats: list[ThrottleKeyStat] = []
    for _ in range(per_throttle_count):
        key = r.read_string()
        tokens = r.read_f64()
        rate = r.read_f64()
        burst = r.read_f64()
        per_throttle_stats.append(ThrottleKeyStat(
            key=key, tokens=tokens, rate_per_second=rate, burst=burst
        ))

    return StatsResultFrame(
        error_code=error_code,
        depth=depth,
        in_flight=in_flight,
        active_fairness_keys=active_fairness_keys,
        active_consumers=active_consumers,
        quantum=quantum,
        leader_node_id=leader_node_id,
        replication_count=replication_count,
        per_key_stats=per_key_stats,
        per_throttle_stats=per_throttle_stats,
    )


def decode_list_queues_result(data: bytes) -> ListQueuesResultFrame:
    """Decode a ListQueuesResult frame body."""
    r = Reader(data)
    error_code = r.read_u8()
    cluster_node_count = r.read_u32()
    queue_count = r.read_u16()
    queues: list[ListQueuesQueueInfo] = []
    for _ in range(queue_count):
        name = r.read_string()
        depth = r.read_u64()
        in_flight = r.read_u64()
        active_consumers = r.read_u32()
        leader_node_id = r.read_u64()
        queues.append(ListQueuesQueueInfo(
            name=name,
            depth=depth,
            in_flight=in_flight,
            active_consumers=active_consumers,
            leader_node_id=leader_node_id,
        ))
    return ListQueuesResultFrame(
        error_code=error_code,
        cluster_node_count=cluster_node_count,
        queues=queues,
    )


def decode_set_config_result(data: bytes) -> int:
    """Decode a SetConfigResult -> error_code."""
    r = Reader(data)
    return r.read_u8()


def decode_get_config_result(data: bytes) -> tuple[int, str]:
    """Decode a GetConfigResult -> (error_code, value)."""
    r = Reader(data)
    error_code = r.read_u8()
    value = r.read_string()
    return error_code, value


def decode_list_config_result(data: bytes) -> tuple[int, dict[str, str]]:
    """Decode a ListConfigResult -> (error_code, entries)."""
    r = Reader(data)
    error_code = r.read_u8()
    count = r.read_u16()
    entries: dict[str, str] = {}
    for _ in range(count):
        key = r.read_string()
        value = r.read_string()
        entries[key] = value
    return error_code, entries


def decode_redrive_result(data: bytes) -> tuple[int, int]:
    """Decode a RedriveResult -> (error_code, redriven_count)."""
    r = Reader(data)
    error_code = r.read_u8()
    redriven = r.read_u64()
    return error_code, redriven


# ---------------------------------------------------------------------------
# Encode: Auth
# ---------------------------------------------------------------------------


def encode_create_api_key(
    name: str, *, expires_at_ms: int = 0, is_superadmin: bool = False
) -> bytes:
    """Encode a CreateApiKey frame body.

    Wire: [string name][u64 expires_at_ms][bool is_superadmin]
    """
    w = Writer()
    w.write_string(name)
    w.write_u64(expires_at_ms)
    w.write_bool(is_superadmin)
    return w.finish()


def encode_revoke_api_key(key_id: str) -> bytes:
    """Encode a RevokeApiKey frame body."""
    w = Writer()
    w.write_string(key_id)
    return w.finish()


def encode_list_api_keys() -> bytes:
    """Encode a ListApiKeys frame body (empty)."""
    return b""


def encode_set_acl(
    key_id: str, permissions: list[tuple[str, str]]
) -> bytes:
    """Encode a SetAcl frame body.

    Wire: [string key_id][u16 count][per: string kind, string pattern]
    permissions is a list of (kind, pattern) tuples.
    """
    w = Writer()
    w.write_string(key_id)
    w.write_u16(len(permissions))
    for kind, pattern in permissions:
        w.write_string(kind)
        w.write_string(pattern)
    return w.finish()


def encode_get_acl(key_id: str) -> bytes:
    """Encode a GetAcl frame body."""
    w = Writer()
    w.write_string(key_id)
    return w.finish()


# ---------------------------------------------------------------------------
# Decode: Auth results
# ---------------------------------------------------------------------------


def decode_create_api_key_result(data: bytes) -> tuple[int, str, str, bool]:
    """Decode a CreateApiKeyResult -> (error_code, key_id, raw_key, is_superadmin)."""
    r = Reader(data)
    error_code = r.read_u8()
    key_id = r.read_string()
    raw_key = r.read_string()
    is_superadmin = r.read_bool()
    return error_code, key_id, raw_key, is_superadmin


def decode_revoke_api_key_result(data: bytes) -> int:
    """Decode a RevokeApiKeyResult -> error_code."""
    r = Reader(data)
    return r.read_u8()


def decode_list_api_keys_result(data: bytes) -> tuple[int, list[ApiKeyInfoFrame]]:
    """Decode a ListApiKeysResult -> (error_code, list of ApiKeyInfoFrame)."""
    r = Reader(data)
    error_code = r.read_u8()
    count = r.read_u16()
    keys: list[ApiKeyInfoFrame] = []
    for _ in range(count):
        key_id = r.read_string()
        name = r.read_string()
        created_at = r.read_u64()
        expires_at = r.read_u64()
        is_superadmin = r.read_bool()
        keys.append(ApiKeyInfoFrame(
            key_id=key_id, name=name, created_at=created_at,
            expires_at=expires_at, is_superadmin=is_superadmin,
        ))
    return error_code, keys


def decode_set_acl_result(data: bytes) -> int:
    """Decode a SetAclResult -> error_code."""
    r = Reader(data)
    return r.read_u8()


def decode_get_acl_result(data: bytes) -> GetAclResultFrame:
    """Decode a GetAclResult."""
    r = Reader(data)
    error_code = r.read_u8()
    key_id = r.read_string()
    is_superadmin = r.read_bool()
    perm_count = r.read_u16()
    permissions: list[AclPermission] = []
    for _ in range(perm_count):
        kind = r.read_string()
        pattern = r.read_string()
        permissions.append(AclPermission(kind=kind, pattern=pattern))
    return GetAclResultFrame(
        error_code=error_code,
        key_id=key_id,
        is_superadmin=is_superadmin,
        permissions=permissions,
    )
