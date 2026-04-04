"""Asynchronous Fila client."""

from __future__ import annotations

import ssl
from typing import TYPE_CHECKING

from fila.conn import AsyncConnection
from fila.errors import (
    NotLeaderError,
    _map_error_code,
    _map_per_item_error,
    _raise_from_error_frame,
)
from fila.fibp.codec import (
    decode_ack_result,
    decode_create_api_key_result,
    decode_create_queue_result,
    decode_delete_queue_result,
    decode_delivery,
    decode_enqueue_result,
    decode_error,
    decode_get_acl_result,
    decode_get_config_result,
    decode_get_stats_result,
    decode_list_api_keys_result,
    decode_list_config_result,
    decode_list_queues_result,
    decode_nack_result,
    decode_redrive_result,
    decode_revoke_api_key_result,
    decode_set_acl_result,
    decode_set_config_result,
    encode_ack,
    encode_create_api_key,
    encode_create_queue,
    encode_delete_queue,
    encode_enqueue,
    encode_get_acl,
    encode_get_config,
    encode_get_stats,
    encode_list_api_keys,
    encode_list_config,
    encode_list_queues,
    encode_nack,
    encode_redrive,
    encode_revoke_api_key,
    encode_set_acl,
    encode_set_config,
)
from fila.fibp.opcodes import ErrorCode, FrameHeader, Opcode
from fila.types import (
    AclEntry,
    AclPermission,
    ApiKeyInfo,
    ConsumeMessage,
    CreateApiKeyResult,
    EnqueueResult,
    FairnessKeyStat,
    QueueInfo,
    StatsResult,
    ThrottleKeyStat,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _parse_addr(addr: str) -> tuple[str, int]:
    if ":" not in addr:
        raise ValueError(f"invalid address (expected host:port): {addr}")
    host, port_str = addr.rsplit(":", 1)
    return host, int(port_str)


class AsyncClient:
    """Asynchronous client for the Fila message broker."""

    def __init__(
        self,
        addr: str,
        *,
        tls: bool = False,
        ca_cert: bytes | None = None,
        client_cert: bytes | None = None,
        client_key: bytes | None = None,
        api_key: str | None = None,
    ) -> None:
        self._addr = addr
        self._tls = tls
        self._ca_cert = ca_cert
        self._client_cert = client_cert
        self._client_key = client_key
        self._api_key = api_key
        self._conn: AsyncConnection | None = None

        use_tls = tls or ca_cert is not None
        if (client_cert is not None or client_key is not None) and not use_tls:
            raise ValueError(
                "client_cert and client_key require ca_cert or tls=True"
            )

        self._ssl_ctx = self._make_ssl_context() if use_tls else None

    def _make_ssl_context(self) -> ssl.SSLContext:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if self._ca_cert is not None:
            ctx.load_verify_locations(cadata=self._ca_cert.decode("ascii"))
        else:
            ctx.load_default_certs()
        if self._client_cert is not None and self._client_key is not None:
            import os
            import tempfile

            cert_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
            key_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
            try:
                cert_file.write(self._client_cert)
                cert_file.close()
                key_file.write(self._client_key)
                key_file.close()
                ctx.load_cert_chain(cert_file.name, key_file.name)
            finally:
                os.unlink(cert_file.name)
                os.unlink(key_file.name)
        return ctx

    async def _ensure_connected(self) -> AsyncConnection:
        if self._conn is None:
            host, port = _parse_addr(self._addr)
            self._conn = await AsyncConnection.connect(
                host, port, ssl_context=self._ssl_ctx, api_key=self._api_key
            )
        return self._conn

    async def _reconnect(self, addr: str) -> None:
        import contextlib

        if self._conn is not None:
            with contextlib.suppress(OSError):
                await self._conn.close()
        self._addr = addr
        self._conn = None
        await self._ensure_connected()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> AsyncClient:
        await self._ensure_connected()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # -- hot-path operations -------------------------------------------------

    async def enqueue(
        self,
        queue: str,
        headers: dict[str, str] | None,
        payload: bytes,
    ) -> str:
        """Enqueue a message. Returns the broker-assigned message ID."""
        await self._ensure_connected()
        msgs = [{"queue": queue, "headers": headers or {}, "payload": payload}]
        body = encode_enqueue(msgs)

        header, resp_body = await self._request_with_leader_retry(
            Opcode.ENQUEUE, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)

        items = decode_enqueue_result(resp_body)
        item = items[0]
        if item.error_code == ErrorCode.OK:
            return item.message_id
        raise _map_per_item_error(item.error_code, "enqueue")

    async def enqueue_many(
        self,
        messages: list[tuple[str, dict[str, str] | None, bytes]],
    ) -> list[EnqueueResult]:
        """Enqueue multiple messages in a single request."""
        await self._ensure_connected()
        msgs = [
            {"queue": q, "headers": h or {}, "payload": p}
            for q, h, p in messages
        ]
        body = encode_enqueue(msgs)

        header, resp_body = await self._request_with_leader_retry(
            Opcode.ENQUEUE, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)

        items = decode_enqueue_result(resp_body)
        results: list[EnqueueResult] = []
        for item in items:
            if item.error_code == ErrorCode.OK:
                results.append(EnqueueResult(message_id=item.message_id, error=None))
            else:
                err_msg = f"error 0x{item.error_code:02x}"
                results.append(EnqueueResult(message_id=None, error=err_msg))
        return results

    async def consume(self, queue: str) -> AsyncIterator[ConsumeMessage]:
        """Open a streaming consumer on the specified queue."""
        conn = await self._ensure_connected()
        try:
            _req_id, consumer_id = await conn.subscribe(queue)
        except NotLeaderError as e:
            if e.leader_addr is not None:
                await self._reconnect(e.leader_addr)
                conn = await self._ensure_connected()
                _req_id, consumer_id = await conn.subscribe(queue)
            else:
                raise

        return self._consume_iter(conn, consumer_id)

    async def _consume_iter(
        self, conn: AsyncConnection, consumer_id: str
    ) -> AsyncIterator[ConsumeMessage]:
        try:
            while True:
                header, body = await conn.read_frame()

                if header.opcode == Opcode.DELIVERY:
                    for msg in decode_delivery(body):
                        yield ConsumeMessage(
                            id=msg.message_id,
                            queue=msg.queue,
                            headers=msg.headers,
                            payload=msg.payload,
                            fairness_key=msg.fairness_key,
                            attempt_count=msg.attempt_count,
                            weight=msg.weight,
                            throttle_keys=msg.throttle_keys,
                            enqueued_at=msg.enqueued_at,
                            leased_at=msg.leased_at,
                        )
                elif header.opcode == Opcode.ERROR:
                    err = decode_error(body)
                    _raise_from_error_frame(err)
        except (ConnectionError, OSError):
            return

    async def ack(self, queue: str, msg_id: str) -> None:
        body = encode_ack([{"queue": queue, "message_id": msg_id}])
        header, resp_body = await self._request_with_leader_retry(Opcode.ACK, body)

        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)

        codes = decode_ack_result(resp_body)
        if codes and codes[0] != ErrorCode.OK:
            raise _map_per_item_error(codes[0], "ack")

    async def nack(self, queue: str, msg_id: str, error: str) -> None:
        body = encode_nack([{"queue": queue, "message_id": msg_id, "error": error}])
        header, resp_body = await self._request_with_leader_retry(Opcode.NACK, body)

        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)

        codes = decode_nack_result(resp_body)
        if codes and codes[0] != ErrorCode.OK:
            raise _map_per_item_error(codes[0], "nack")

    # -- admin operations ----------------------------------------------------

    async def create_queue(self, name: str) -> None:
        body = encode_create_queue(name)
        header, resp_body = await self._request_with_leader_retry(
            Opcode.CREATE_QUEUE, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        result = decode_create_queue_result(resp_body)
        if result.error_code != ErrorCode.OK:
            raise _map_error_code(result.error_code, "create_queue failed")

    async def delete_queue(self, name: str) -> None:
        body = encode_delete_queue(name)
        header, resp_body = await self._request_with_leader_retry(
            Opcode.DELETE_QUEUE, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        error_code = decode_delete_queue_result(resp_body)
        if error_code != ErrorCode.OK:
            raise _map_error_code(error_code, "delete_queue failed")

    async def get_stats(self, queue: str) -> StatsResult:
        body = encode_get_stats(queue)
        header, resp_body = await self._request_with_leader_retry(
            Opcode.GET_STATS, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        r = decode_get_stats_result(resp_body)
        if r.error_code != ErrorCode.OK:
            raise _map_error_code(r.error_code, "get_stats failed")
        return StatsResult(
            depth=r.depth, in_flight=r.in_flight,
            active_fairness_keys=r.active_fairness_keys,
            active_consumers=r.active_consumers, quantum=r.quantum,
            leader_node_id=r.leader_node_id,
            replication_count=r.replication_count,
            per_key_stats=[
                FairnessKeyStat(
                    key=s.key, pending_count=s.pending_count,
                    current_deficit=s.current_deficit, weight=s.weight,
                ) for s in r.per_key_stats
            ],
            per_throttle_stats=[
                ThrottleKeyStat(
                    key=s.key, tokens=s.tokens,
                    rate_per_second=s.rate_per_second, burst=s.burst,
                ) for s in r.per_throttle_stats
            ],
        )

    async def list_queues(self) -> list[QueueInfo]:
        body = encode_list_queues()
        header, resp_body = await self._request_with_leader_retry(
            Opcode.LIST_QUEUES, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        r = decode_list_queues_result(resp_body)
        if r.error_code != ErrorCode.OK:
            raise _map_error_code(r.error_code, "list_queues failed")
        return [
            QueueInfo(
                name=q.name, depth=q.depth, in_flight=q.in_flight,
                active_consumers=q.active_consumers,
                leader_node_id=q.leader_node_id,
            ) for q in r.queues
        ]

    async def set_config(self, key: str, value: str) -> None:
        body = encode_set_config(key, value)
        header, resp_body = await self._request_with_leader_retry(
            Opcode.SET_CONFIG, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        error_code = decode_set_config_result(resp_body)
        if error_code != ErrorCode.OK:
            raise _map_error_code(error_code, "set_config failed")

    async def get_config(self, key: str) -> str:
        body = encode_get_config(key)
        header, resp_body = await self._request_with_leader_retry(
            Opcode.GET_CONFIG, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        error_code, value = decode_get_config_result(resp_body)
        if error_code != ErrorCode.OK:
            raise _map_error_code(error_code, "get_config failed")
        return value

    async def list_config(self, prefix: str) -> dict[str, str]:
        body = encode_list_config(prefix)
        header, resp_body = await self._request_with_leader_retry(
            Opcode.LIST_CONFIG, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        error_code, entries = decode_list_config_result(resp_body)
        if error_code != ErrorCode.OK:
            raise _map_error_code(error_code, "list_config failed")
        return entries

    async def redrive(self, dlq_queue: str, count: int) -> int:
        body = encode_redrive(dlq_queue, count)
        header, resp_body = await self._request_with_leader_retry(
            Opcode.REDRIVE, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        error_code, redriven = decode_redrive_result(resp_body)
        if error_code != ErrorCode.OK:
            raise _map_error_code(error_code, "redrive failed")
        return redriven

    # -- auth operations -----------------------------------------------------

    async def create_api_key(self, name: str) -> CreateApiKeyResult:
        body = encode_create_api_key(name)
        header, resp_body = await self._request_with_leader_retry(
            Opcode.CREATE_API_KEY, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        ec, key_id, raw_key, is_superadmin = decode_create_api_key_result(resp_body)
        if ec != ErrorCode.OK:
            raise _map_error_code(ec, "create_api_key failed")
        return CreateApiKeyResult(
            key_id=key_id, raw_key=raw_key, is_superadmin=is_superadmin
        )

    async def revoke_api_key(self, key_id: str) -> None:
        body = encode_revoke_api_key(key_id)
        header, resp_body = await self._request_with_leader_retry(
            Opcode.REVOKE_API_KEY, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        error_code = decode_revoke_api_key_result(resp_body)
        if error_code != ErrorCode.OK:
            raise _map_error_code(error_code, "revoke_api_key failed")

    async def list_api_keys(self) -> list[ApiKeyInfo]:
        body = encode_list_api_keys()
        header, resp_body = await self._request_with_leader_retry(
            Opcode.LIST_API_KEYS, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        error_code, items = decode_list_api_keys_result(resp_body)
        if error_code != ErrorCode.OK:
            raise _map_error_code(error_code, "list_api_keys failed")
        return [
            ApiKeyInfo(
                key_id=k.key_id, name=k.name, created_at=k.created_at,
                expires_at=k.expires_at, is_superadmin=k.is_superadmin,
            ) for k in items
        ]

    async def set_acl(self, key_id: str, permissions: list[tuple[str, str]]) -> None:
        body = encode_set_acl(key_id, permissions)
        header, resp_body = await self._request_with_leader_retry(
            Opcode.SET_ACL, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        error_code = decode_set_acl_result(resp_body)
        if error_code != ErrorCode.OK:
            raise _map_error_code(error_code, "set_acl failed")

    async def get_acl(self, key_id: str) -> AclEntry:
        body = encode_get_acl(key_id)
        header, resp_body = await self._request_with_leader_retry(
            Opcode.GET_ACL, body
        )
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        result = decode_get_acl_result(resp_body)
        if result.error_code != ErrorCode.OK:
            raise _map_error_code(result.error_code, "get_acl failed")
        return AclEntry(
            key_id=result.key_id,
            is_superadmin=result.is_superadmin,
            permissions=[
                AclPermission(kind=p.kind, pattern=p.pattern)
                for p in result.permissions
            ],
        )

    # -- internal helpers ----------------------------------------------------

    async def _request_with_leader_retry(
        self, opcode: int, body: bytes
    ) -> tuple[FrameHeader, bytes]:
        conn = await self._ensure_connected()
        header, resp_body = await conn.request(opcode, body)

        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            if err.code == ErrorCode.NOT_LEADER:
                leader_addr = err.metadata.get("leader_addr")
                if leader_addr:
                    await self._reconnect(leader_addr)
                    conn = await self._ensure_connected()
                    return await conn.request(opcode, body)

        return header, resp_body
