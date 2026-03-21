"""Asynchronous Fila client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import grpc
import grpc.aio

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from fila.errors import _map_ack_error, _map_consume_error, _map_enqueue_error, _map_nack_error
from fila.types import ConsumeMessage
from fila.v1 import service_pb2, service_pb2_grpc


class _AsyncClientCallDetails(
    grpc.aio.ClientCallDetails,  # type: ignore[misc]
):
    """Concrete ``ClientCallDetails`` for the async interceptor chain.

    ``grpc.aio.ClientCallDetails`` is a namedtuple with 5 fields (method,
    timeout, metadata, credentials, wait_for_ready).  We override ``__new__``
    so the namedtuple layer receives exactly those five, then set any extra
    attribute (``compression``) in ``__init__``.
    """

    def __new__(
        cls,
        method: str,
        timeout: float | None,
        metadata: grpc.aio.Metadata | None,
        credentials: grpc.CallCredentials | None,
        wait_for_ready: bool | None,
    ) -> _AsyncClientCallDetails:
        return super().__new__(cls, method, timeout, metadata, credentials, wait_for_ready)  # type: ignore[no-any-return]

    def __init__(
        self,
        method: str,
        timeout: float | None,
        metadata: grpc.aio.Metadata | None,
        credentials: grpc.CallCredentials | None,
        wait_for_ready: bool | None,
    ) -> None:
        # Fields are already set by __new__ (namedtuple).  Nothing extra to do.
        pass


class _AsyncApiKeyInterceptor(
    grpc.aio.UnaryUnaryClientInterceptor,  # type: ignore[misc]
    grpc.aio.UnaryStreamClientInterceptor,  # type: ignore[misc]
):
    """Injects ``authorization: Bearer <key>`` metadata into every async RPC."""

    def __init__(self, api_key: str) -> None:
        self._metadata = grpc.aio.Metadata(("authorization", f"Bearer {api_key}"))

    def _inject(
        self, metadata: grpc.aio.Metadata | None
    ) -> grpc.aio.Metadata:
        merged = grpc.aio.Metadata()
        if metadata is not None:
            for key, value in metadata:
                merged.add(key, value)
        for key, value in self._metadata:
            merged.add(key, value)
        return merged

    async def intercept_unary_unary(
        self,
        continuation: Any,
        client_call_details: grpc.aio.ClientCallDetails,
        request: Any,
    ) -> Any:
        new_details = _AsyncClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            self._inject(client_call_details.metadata),
            client_call_details.credentials,
            client_call_details.wait_for_ready,
        )
        return await continuation(new_details, request)

    async def intercept_unary_stream(
        self,
        continuation: Any,
        client_call_details: grpc.aio.ClientCallDetails,
        request: Any,
    ) -> Any:
        new_details = _AsyncClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            self._inject(client_call_details.metadata),
            client_call_details.credentials,
            client_call_details.wait_for_ready,
        )
        return await continuation(new_details, request)


class AsyncClient:
    """Asynchronous client for the Fila message broker.

    Wraps the hot-path gRPC operations: enqueue, consume, ack, nack.

    Usage::

        client = AsyncClient("localhost:5555")
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
        """Connect to a Fila broker at the given address.

        Args:
            addr: Broker address in "host:port" format (e.g., "localhost:5555").
            tls: Enable TLS using the OS system trust store for server
                 verification. Ignored when ``ca_cert`` is provided (which
                 implies TLS). Defaults to ``False``.
            ca_cert: PEM-encoded CA certificate for verifying the server.
                     When provided, a TLS channel is used instead of an insecure one.
            client_cert: PEM-encoded client certificate for mutual TLS (optional).
            client_key: PEM-encoded client private key for mutual TLS (optional).
            api_key: API key for authentication. When set, every RPC includes an
                     ``authorization: Bearer <key>`` metadata header.
        """
        use_tls = tls or ca_cert is not None

        if (client_cert is not None or client_key is not None) and not use_tls:
            raise ValueError(
                "client_cert and client_key require ca_cert or tls=True to establish a TLS channel"
            )

        interceptors: list[grpc.aio.ClientInterceptor] = []
        if api_key is not None:
            interceptors.append(_AsyncApiKeyInterceptor(api_key))

        if use_tls:
            creds = grpc.ssl_channel_credentials(
                root_certificates=ca_cert,
                private_key=client_key,
                certificate_chain=client_cert,
            )
            self._channel = grpc.aio.secure_channel(
                addr, creds, interceptors=interceptors or None
            )
        else:
            self._channel = grpc.aio.insecure_channel(
                addr, interceptors=interceptors or None
            )

        self._stub = service_pb2_grpc.FilaServiceStub(self._channel)  # type: ignore[no-untyped-call]

    async def close(self) -> None:
        """Close the underlying gRPC channel."""
        await self._channel.close()

    async def __aenter__(self) -> AsyncClient:
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
            RPCError: For unexpected gRPC failures.
        """
        try:
            resp = await self._stub.Enqueue(
                service_pb2.EnqueueRequest(
                    queue=queue,
                    headers=headers or {},
                    payload=payload,
                )
            )
        except grpc.RpcError as e:
            raise _map_enqueue_error(e) from e
        return str(resp.message_id)

    async def consume(self, queue: str) -> AsyncIterator[ConsumeMessage]:
        """Open a streaming consumer on the specified queue.

        Yields messages as they become available. The iterator ends when the
        server stream closes or an error occurs. Nil message frames (keepalive
        signals) are skipped automatically.

        Args:
            queue: Queue to consume from.

        Yields:
            ConsumeMessage objects as they arrive.

        Raises:
            QueueNotFoundError: If the queue does not exist.
            RPCError: For unexpected gRPC failures.
        """
        try:
            stream = self._stub.Consume(
                service_pb2.ConsumeRequest(queue=queue)
            )
        except grpc.RpcError as e:
            raise _map_consume_error(e) from e

        return self._consume_iter(stream)

    async def _consume_iter(
        self,
        stream: Any,
    ) -> AsyncIterator[ConsumeMessage]:
        """Internal async generator reading from the gRPC stream."""
        try:
            async for resp in stream:
                msg = resp.message
                if msg is None or not msg.ByteSize():
                    continue  # keepalive
                metadata = msg.metadata
                cm = ConsumeMessage(
                    id=msg.id,
                    headers=dict(msg.headers),
                    payload=bytes(msg.payload),
                    fairness_key=metadata.fairness_key if metadata else "",
                    attempt_count=metadata.attempt_count if metadata else 0,
                    queue=metadata.queue_id if metadata else "",
                )
                yield cm
        except grpc.RpcError:
            return

    async def ack(self, queue: str, msg_id: str) -> None:
        """Acknowledge a successfully processed message.

        The message is permanently removed from the queue.

        Args:
            queue: Queue the message belongs to.
            msg_id: ID of the message to acknowledge.

        Raises:
            MessageNotFoundError: If the message does not exist.
            RPCError: For unexpected gRPC failures.
        """
        try:
            await self._stub.Ack(
                service_pb2.AckRequest(queue=queue, message_id=msg_id)
            )
        except grpc.RpcError as e:
            raise _map_ack_error(e) from e

    async def nack(self, queue: str, msg_id: str, error: str) -> None:
        """Negatively acknowledge a message that failed processing.

        The message is requeued for retry or routed to the dead-letter queue
        based on the queue's on_failure Lua hook configuration.

        Args:
            queue: Queue the message belongs to.
            msg_id: ID of the message to nack.
            error: Description of the failure.

        Raises:
            MessageNotFoundError: If the message does not exist.
            RPCError: For unexpected gRPC failures.
        """
        try:
            await self._stub.Nack(
                service_pb2.NackRequest(
                    queue=queue, message_id=msg_id, error=error
                )
            )
        except grpc.RpcError as e:
            raise _map_nack_error(e) from e
