"""Asynchronous Fila client."""

from __future__ import annotations

import ssl
from typing import TYPE_CHECKING

from fila.conn import AsyncConnection
from fila.errors import (
    NotLeaderError,
    _map_per_item_error,
    _raise_from_error_frame,
)
from fila.fibp.codec import (
    decode_ack_result,
    decode_create_api_key_result,
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
from fila.fibp.opcodes import ErrorCode, Opcode
from fila.types import (
    AclEntry,
    ApiKeyInfo,
    ConsumeMessage,
    CreateApiKeyResult,
    EnqueueResult,
    StatsResult,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _parse_addr(addr: str) -> tuple[str, int]:
    """Parse 'host:port' into (host, port)."""
    if ":" not in addr:
        raise ValueError(f"invalid address (expected host:port): {addr}")
    host, port_str = addr.rsplit(":", 1)
    return host, int(port_str)


class AsyncClient:
    """Asynchronous client for the Fila message broker.

    Wraps the hot-path FIBP operations: enqueue, enqueue_many, consume, ack, nack.

    Usage::

        client = await AsyncClient.create("localhost:5555")
        msg_id = await client.enqueue("my-queue", {"tenant": "acme"}, b"hello")
        async for msg in await client.consume("my-queue"):
            await client.ack("my-queue", msg.id)
        await client.close()

    Or as an async context manager::

        async with AsyncClient("localhost:5555") as client:
            await client.enqueue("my-queue", None, b"hello")

    TLS (system trust store)::

        client = AsyncClient("localhost:5555", tls=True)

    TLS (custom CA)::

        with open("ca.pem", "rb") as f:
            ca = f.read()
        client = AsyncClient("localhost:5555", ca_cert=ca)

    mTLS + API key::

        client = AsyncClient(
            "localhost:5555",
            ca_cert=ca,
            client_cert=cert,
            client_key=key,
            api_key="fila_...",
        )
    """

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
                "client_cert and client_key require ca_cert or tls=True to establish a TLS channel"
            )

        self._ssl_ctx = self._make_ssl_context() if use_tls else None

    def _make_ssl_context(self) -> ssl.SSLContext:
        """Create an SSL context from stored credentials."""
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
        """Ensure a connection exists, creating one if needed."""
        if self._conn is None:
            host, port = _parse_addr(self._addr)
            self._conn = await AsyncConnection.connect(
                host, port, ssl_context=self._ssl_ctx, api_key=self._api_key
            )
        return self._conn

    async def _reconnect(self, addr: str) -> None:
        """Reconnect to a different address (e.g. after leader hint)."""
        if self._conn is not None:
            import contextlib

            with contextlib.suppress(OSError):
                await self._conn.close()
        self._addr = addr
        self._conn = None
        await self._ensure_connected()

    async def close(self) -> None:
        """Close the underlying connection."""
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
        """Enqueue a message to the specified queue.

        Returns:
            Broker-assigned message ID (UUIDv7).
        """
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
                results.append(
                    EnqueueResult(message_id=None, error=f"error 0x{item.error_code:02x}")
                )
        return results

    async def consume(self, queue: str) -> AsyncIterator[ConsumeMessage]:
        """Open a streaming consumer on the specified queue.

        Returns an async iterator that yields messages as they arrive.
        """
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
        """Internal async generator reading Delivery frames."""
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
        """Acknowledge a successfully processed message."""
        body = encode_ack([{"queue": queue, "message_id": msg_id}])
        header, resp_body = await self._request_with_leader_retry(Opcode.ACK, body)

        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)

        codes = decode_ack_result(resp_body)
        if codes and codes[0] != ErrorCode.OK:
            raise _map_per_item_error(codes[0], "ack")

    async def nack(self, queue: str, msg_id: str, error: str) -> None:
        """Negatively acknowledge a message that failed processing."""
        body = encode_nack([{"queue": queue, "message_id": msg_id, "error": error}])
        header, resp_body = await self._request_with_leader_retry(Opcode.NACK, body)

        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)

        codes = decode_nack_result(resp_body)
        if codes and codes[0] != ErrorCode.OK:
            raise _map_per_item_error(codes[0], "nack")

    # -- admin operations ----------------------------------------------------

    async def create_queue(self, name: str, config: dict[str, str] | None = None) -> None:
        """Create a queue on the broker."""
        body = encode_create_queue(name, config)
        header, resp_body = await self._request_with_leader_retry(Opcode.CREATE_QUEUE, body)
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)

    async def delete_queue(self, name: str) -> None:
        """Delete a queue from the broker."""
        body = encode_delete_queue(name)
        header, resp_body = await self._request_with_leader_retry(Opcode.DELETE_QUEUE, body)
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)

    async def get_stats(self, queue: str) -> StatsResult:
        """Get statistics for a queue."""
        body = encode_get_stats(queue)
        header, resp_body = await self._request_with_leader_retry(Opcode.GET_STATS, body)
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        result = decode_get_stats_result(resp_body)
        return StatsResult(stats=result.stats)

    async def list_queues(self) -> list[str]:
        """List all queues on the broker."""
        body = encode_list_queues()
        header, resp_body = await self._request_with_leader_retry(Opcode.LIST_QUEUES, body)
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        return decode_list_queues_result(resp_body)

    async def set_config(self, queue: str, config: dict[str, str]) -> None:
        """Set configuration for a queue."""
        body = encode_set_config(queue, config)
        header, resp_body = await self._request_with_leader_retry(Opcode.SET_CONFIG, body)
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)

    async def get_config(self, queue: str) -> dict[str, str]:
        """Get configuration for a queue."""
        body = encode_get_config(queue)
        header, resp_body = await self._request_with_leader_retry(Opcode.GET_CONFIG, body)
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        return decode_get_config_result(resp_body)

    async def list_config(self, queue: str) -> dict[str, str]:
        """List all configuration for a queue."""
        body = encode_list_config(queue)
        header, resp_body = await self._request_with_leader_retry(Opcode.LIST_CONFIG, body)
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        return decode_list_config_result(resp_body)

    async def redrive(self, source_queue: str, dest_queue: str, count: int) -> None:
        """Redrive messages from one queue to another."""
        body = encode_redrive(source_queue, dest_queue, count)
        header, resp_body = await self._request_with_leader_retry(Opcode.REDRIVE, body)
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)

    # -- auth operations -----------------------------------------------------

    async def create_api_key(self, name: str) -> CreateApiKeyResult:
        """Create a new API key."""
        body = encode_create_api_key(name)
        header, resp_body = await self._request_with_leader_retry(Opcode.CREATE_API_KEY, body)
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        key_id, raw_key = decode_create_api_key_result(resp_body)
        return CreateApiKeyResult(key_id=key_id, raw_key=raw_key)

    async def revoke_api_key(self, key_id: str) -> None:
        """Revoke an API key."""
        body = encode_revoke_api_key(key_id)
        header, resp_body = await self._request_with_leader_retry(Opcode.REVOKE_API_KEY, body)
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)

    async def list_api_keys(self) -> list[ApiKeyInfo]:
        """List all API keys."""
        body = encode_list_api_keys()
        header, resp_body = await self._request_with_leader_retry(Opcode.LIST_API_KEYS, body)
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        items = decode_list_api_keys_result(resp_body)
        return [
            ApiKeyInfo(key_id=k.key_id, prefix=k.prefix, created_at=k.created_at)
            for k in items
        ]

    async def set_acl(
        self, key_id: str, patterns: list[str], superadmin: bool = False
    ) -> None:
        """Set ACL for an API key."""
        body = encode_set_acl(key_id, patterns, superadmin)
        header, resp_body = await self._request_with_leader_retry(Opcode.SET_ACL, body)
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)

    async def get_acl(self, key_id: str) -> AclEntry:
        """Get ACL for an API key."""
        body = encode_get_acl(key_id)
        header, resp_body = await self._request_with_leader_retry(Opcode.GET_ACL, body)
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            _raise_from_error_frame(err)
        result = decode_get_acl_result(resp_body)
        return AclEntry(patterns=result.patterns, superadmin=result.superadmin)

    # -- internal helpers ----------------------------------------------------

    async def _request_with_leader_retry(
        self, opcode: int, body: bytes
    ) -> tuple[object, bytes]:
        """Send a request, retrying once on NotLeader with leader hint."""
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
