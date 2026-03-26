"""Asynchronous Fila client (FIBP transport)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fila.errors import (
    _map_ack_error_code,
    _map_enqueue_error_code,
    _map_fibp_error,
    _map_nack_error_code,
)
from fila.fibp import (
    AsyncFibpConnection,
    FibpError,
    decode_ack_nack_response,
    decode_consume_push,
    decode_enqueue_response,
    encode_ack,
    encode_consume,
    encode_enqueue,
    encode_nack,
    make_ssl_context,
    parse_addr,
)
from fila.types import ConsumeMessage, EnqueueResult

if TYPE_CHECKING:
    import ssl
    from collections.abc import AsyncIterator

_log = logging.getLogger(__name__)


class AsyncClient:
    """Asynchronous client for the Fila message broker (FIBP transport).

    Usage::

        client = AsyncClient("localhost:5555")
        await client.connect()
        msg_id = await client.enqueue("my-queue", {"tenant": "acme"}, b"hello")
        async for msg in await client.consume("my-queue"):
            await client.ack("my-queue", msg.id)
        await client.close()

    Or as an async context manager (preferred — handles connect automatically)::

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
        """Prepare a connection to a Fila broker.

        Call ``await client.connect()`` (or use the async context manager)
        before making any requests.

        Args:
            addr: Broker address in ``"host:port"`` format.
            tls: Enable TLS using the OS system trust store.
            ca_cert: PEM-encoded CA certificate for server verification.
            client_cert: PEM-encoded client certificate for mTLS.
            client_key: PEM-encoded client private key for mTLS.
            api_key: API key sent as an AUTH frame on connect.
        """
        self._addr = addr
        self._tls = tls
        self._ca_cert = ca_cert
        self._client_cert = client_cert
        self._client_key = client_key
        self._api_key = api_key

        use_tls = tls or ca_cert is not None
        if (client_cert is not None or client_key is not None) and not use_tls:
            raise ValueError(
                "client_cert and client_key require ca_cert or tls=True"
            )

        self._conn = self._make_conn(addr)

    def _make_ssl_ctx(self) -> ssl.SSLContext | None:
        use_tls = self._tls or self._ca_cert is not None
        if not use_tls:
            return None
        return make_ssl_context(
            ca_cert=self._ca_cert,
            client_cert=self._client_cert,
            client_key=self._client_key,
        )

    def _make_conn(self, addr: str) -> AsyncFibpConnection:
        host, port = parse_addr(addr)
        ssl_ctx = self._make_ssl_ctx()
        return AsyncFibpConnection(host, port, ssl_ctx=ssl_ctx, api_key=self._api_key)

    async def connect(self) -> None:
        """Open the TCP connection and perform the FIBP handshake."""
        await self._conn.connect()

    async def close(self) -> None:
        """Close the underlying connection."""
        await self._conn.close()

    async def __aenter__(self) -> AsyncClient:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def enqueue(
        self,
        queue: str,
        headers: dict[str, str] | None,
        payload: bytes,
    ) -> str:
        """Enqueue a message to the specified queue.

        Args:
            queue: Target queue name.
            headers: Optional message headers.
            payload: Message payload bytes.

        Returns:
            Broker-assigned message ID (UUIDv7).

        Raises:
            QueueNotFoundError: If the queue does not exist.
            TransportError: For unexpected FIBP failures.
        """
        corr_id = self._conn.alloc_corr_id()
        frame = encode_enqueue(corr_id, [(queue, headers or {}, payload)])
        try:
            body = await self._conn.send_request(frame, corr_id)
        except FibpError as e:
            raise _map_fibp_error(e.code, e.message) from e

        results = decode_enqueue_response(body)
        ok, msg_id, err_code, err_msg = results[0]
        if ok:
            return msg_id
        raise _map_enqueue_error_code(err_code, err_msg)

    async def enqueue_many(
        self,
        messages: list[tuple[str, dict[str, str] | None, bytes]],
    ) -> list[EnqueueResult]:
        """Enqueue multiple messages, possibly targeting different queues.

        Per-queue FIBP requests are issued concurrently via ``asyncio.gather``.

        Args:
            messages: List of ``(queue, headers, payload)`` tuples.

        Returns:
            List of ``EnqueueResult`` objects, one per input message.

        Raises:
            TransportError: For unexpected FIBP failures.
        """
        import asyncio
        from collections import defaultdict

        by_queue: dict[str, list[tuple[int, dict[str, str], bytes]]] = defaultdict(list)
        order: list[tuple[str, int]] = []
        local_indices: dict[str, int] = defaultdict(int)

        for queue, hdrs, payload in messages:
            idx = local_indices[queue]
            local_indices[queue] += 1
            by_queue[queue].append((idx, hdrs or {}, payload))
            order.append((queue, idx))

        async def _send_one_queue(
            queue_name: str,
            items: list[tuple[int, dict[str, str], bytes]],
        ) -> tuple[str, list[EnqueueResult]]:
            corr_id = self._conn.alloc_corr_id()
            msgs = [(queue_name, h, p) for _, h, p in items]
            frame = encode_enqueue(corr_id, msgs)
            try:
                body = await self._conn.send_request(frame, corr_id)
            except FibpError as e:
                err = str(e)
                return queue_name, [
                    EnqueueResult(message_id=None, error=err) for _ in items
                ]
            decoded = decode_enqueue_response(body)
            per_queue: list[EnqueueResult] = []
            for ok, msg_id, _err_code, err_msg in decoded:
                if ok:
                    per_queue.append(EnqueueResult(message_id=msg_id, error=None))
                else:
                    per_queue.append(EnqueueResult(message_id=None, error=err_msg))
            return queue_name, per_queue

        coros = [
            _send_one_queue(queue_name, items)
            for queue_name, items in by_queue.items()
        ]
        gathered = await asyncio.gather(*coros)
        results_by_queue: dict[str, list[EnqueueResult]] = dict(gathered)

        per_queue_counters: dict[str, int] = defaultdict(int)
        final: list[EnqueueResult] = []
        for queue_name, _ in order:
            idx = per_queue_counters[queue_name]
            per_queue_counters[queue_name] += 1
            final.append(results_by_queue[queue_name][idx])
        return final

    async def consume(self, queue: str) -> AsyncIterator[ConsumeMessage]:
        """Open a streaming consumer on the specified queue.

        Returns an async iterator that yields messages as they arrive.

        Args:
            queue: Queue to consume from.

        Raises:
            QueueNotFoundError: If the queue does not exist.
            TransportError: For unexpected FIBP failures.
        """
        corr_id = self._conn.alloc_corr_id()
        frame = encode_consume(corr_id, queue)
        try:
            q = await self._conn.open_consume_stream(frame, corr_id)
        except FibpError as e:
            raise _map_fibp_error(e.code, e.message) from e

        return self._consume_iter(q, queue)

    async def _consume_iter(
        self,
        q: object,
        queue: str,
    ) -> AsyncIterator[ConsumeMessage]:
        import asyncio
        # q is an asyncio.Queue[bytes | None]
        assert isinstance(q, asyncio.Queue)
        while True:
            body: bytes | None = await q.get()
            if body is None:
                return
            try:
                messages = decode_consume_push(body)
            except Exception:
                _log.warning(
                    "failed to decode consume push frame; skipping",
                    exc_info=True,
                )
                continue
            for msg_id, headers, payload, fairness_key, attempt_count in messages:
                yield ConsumeMessage(
                    id=msg_id,
                    headers=headers,
                    payload=payload,
                    fairness_key=fairness_key,
                    attempt_count=attempt_count,
                    queue=queue,
                )

    async def ack(self, queue: str, msg_id: str) -> None:
        """Acknowledge a successfully processed message.

        Args:
            queue: Queue the message belongs to.
            msg_id: ID of the message to acknowledge.

        Raises:
            MessageNotFoundError: If the message does not exist.
            TransportError: For unexpected FIBP failures.
        """
        corr_id = self._conn.alloc_corr_id()
        frame = encode_ack(corr_id, [(queue, msg_id)])
        try:
            body = await self._conn.send_request(frame, corr_id)
        except FibpError as e:
            raise _map_fibp_error(e.code, e.message) from e

        results = decode_ack_nack_response(body)
        if results:
            ok, err_code, err_msg = results[0]
            if not ok:
                raise _map_ack_error_code(err_code, err_msg)

    async def nack(self, queue: str, msg_id: str, error: str) -> None:
        """Negatively acknowledge a message that failed processing.

        Args:
            queue: Queue the message belongs to.
            msg_id: ID of the message to nack.
            error: Description of the failure.

        Raises:
            MessageNotFoundError: If the message does not exist.
            TransportError: For unexpected FIBP failures.
        """
        corr_id = self._conn.alloc_corr_id()
        frame = encode_nack(corr_id, [(queue, msg_id, error)])
        try:
            body = await self._conn.send_request(frame, corr_id)
        except FibpError as e:
            raise _map_fibp_error(e.code, e.message) from e

        results = decode_ack_nack_response(body)
        if results:
            ok, err_code, err_msg = results[0]
            if not ok:
                raise _map_nack_error_code(err_code, err_msg)
