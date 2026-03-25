"""Asynchronous Fila client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import grpc
import grpc.aio

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from fila.client import _proto_enqueue_result_to_sdk, _proto_msg_to_consume_message
from fila.errors import (
    EnqueueError,
    _map_ack_error,
    _map_consume_error,
    _map_enqueue_error,
    _map_enqueue_result_error,
    _map_nack_error,
)
from fila.types import ConsumeMessage, EnqueueResult
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


_LEADER_HINT_KEY = "x-fila-leader-addr"


def _extract_leader_hint(err: grpc.RpcError) -> str | None:
    """Return the leader address from trailing metadata, if present."""
    if err.code() != grpc.StatusCode.UNAVAILABLE:
        return None
    trailing = err.trailing_metadata()
    if trailing is None:
        return None
    for key, value in trailing:
        if key == _LEADER_HINT_KEY:
            return str(value)
    return None


class AsyncClient:
    """Asynchronous client for the Fila message broker.

    Wraps the hot-path gRPC operations: enqueue, enqueue_many, consume, ack,
    nack.

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
        self._tls = tls
        self._ca_cert = ca_cert
        self._client_cert = client_cert
        self._client_key = client_key
        self._api_key = api_key

        use_tls = tls or ca_cert is not None
        if (client_cert is not None or client_key is not None) and not use_tls:
            raise ValueError(
                "client_cert and client_key require ca_cert or tls=True to establish a TLS channel"
            )

        self._channel = self._make_channel(addr)
        self._stub = service_pb2_grpc.FilaServiceStub(self._channel)  # type: ignore[no-untyped-call]

    def _make_channel(self, addr: str) -> grpc.aio.Channel:
        """Create an async gRPC channel to the given address using stored credentials."""
        use_tls = self._tls or self._ca_cert is not None

        interceptors: list[grpc.aio.ClientInterceptor] = []
        if self._api_key is not None:
            interceptors.append(_AsyncApiKeyInterceptor(self._api_key))

        if use_tls:
            creds = grpc.ssl_channel_credentials(
                root_certificates=self._ca_cert,
                private_key=self._client_key,
                certificate_chain=self._client_cert,
            )
            return grpc.aio.secure_channel(
                addr, creds, interceptors=interceptors or None
            )
        return grpc.aio.insecure_channel(
            addr, interceptors=interceptors or None
        )

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
                    messages=[
                        service_pb2.EnqueueMessage(
                            queue=queue,
                            headers=headers or {},
                            payload=payload,
                        )
                    ]
                )
            )
        except grpc.RpcError as e:
            raise _map_enqueue_error(e) from e

        result = resp.results[0]
        which = result.WhichOneof("result")
        if which == "message_id":
            return str(result.message_id)
        raise _map_enqueue_result_error(result.error.code, result.error.message)

    async def enqueue_many(
        self,
        messages: list[tuple[str, dict[str, str] | None, bytes]],
    ) -> list[EnqueueResult]:
        """Enqueue multiple messages in a single RPC.

        Args:
            messages: List of (queue, headers, payload) tuples.

        Returns:
            List of ``EnqueueResult`` objects, one per input message.
            Each result has either a ``message_id`` (success) or ``error``
            (per-message failure).

        Raises:
            QueueNotFoundError: If a referenced queue does not exist.
            RPCError: For unexpected gRPC failures.
        """
        proto_messages = [
            service_pb2.EnqueueMessage(
                queue=q,
                headers=h or {},
                payload=p,
            )
            for q, h, p in messages
        ]

        try:
            resp = await self._stub.Enqueue(
                service_pb2.EnqueueRequest(messages=proto_messages)
            )
        except grpc.RpcError as e:
            raise _map_enqueue_error(e) from e

        return [_proto_enqueue_result_to_sdk(r) for r in resp.results]

    async def consume(self, queue: str) -> AsyncIterator[ConsumeMessage]:
        """Open a streaming consumer on the specified queue.

        Yields messages as they become available. The iterator ends when the
        server stream closes or an error occurs. Nil message frames (keepalive
        signals) are skipped automatically.

        If the server returns UNAVAILABLE with an ``x-fila-leader-addr``
        trailing metadata entry, the client transparently reconnects to the
        leader address and retries the consume call once.

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
            leader_addr = _extract_leader_hint(e)
            if leader_addr is not None:
                stream = await self._reconnect_and_consume(leader_addr, queue)
            else:
                raise _map_consume_error(e) from e

        return self._consume_iter(stream)

    async def _reconnect_and_consume(self, leader_addr: str, queue: str) -> Any:
        """Create a new channel to *leader_addr* and retry the consume call."""
        await self._channel.close()
        self._channel = self._make_channel(leader_addr)
        self._stub = service_pb2_grpc.FilaServiceStub(self._channel)  # type: ignore[no-untyped-call]
        try:
            return self._stub.Consume(
                service_pb2.ConsumeRequest(queue=queue)
            )
        except grpc.RpcError as e:
            raise _map_consume_error(e) from e

    async def _consume_iter(
        self,
        stream: Any,
    ) -> AsyncIterator[ConsumeMessage]:
        """Internal async generator reading from the gRPC stream."""
        try:
            async for resp in stream:
                for msg in resp.messages:
                    if msg is not None and msg.ByteSize():
                        yield _proto_msg_to_consume_message(msg)
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
            resp = await self._stub.Ack(
                service_pb2.AckRequest(
                    messages=[service_pb2.AckMessage(queue=queue, message_id=msg_id)]
                )
            )
        except grpc.RpcError as e:
            raise _map_ack_error(e) from e

        # Check per-message result for errors.
        if resp.results:
            result = resp.results[0]
            which = result.WhichOneof("result")
            if which == "error":
                from fila.errors import MessageNotFoundError, RPCError as _RPCError

                ack_err = result.error
                if ack_err.code == service_pb2.ACK_ERROR_CODE_MESSAGE_NOT_FOUND:
                    raise MessageNotFoundError(f"ack: {ack_err.message}")
                raise _RPCError(grpc.StatusCode.INTERNAL, f"ack: {ack_err.message}")

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
            resp = await self._stub.Nack(
                service_pb2.NackRequest(
                    messages=[
                        service_pb2.NackMessage(
                            queue=queue, message_id=msg_id, error=error
                        )
                    ]
                )
            )
        except grpc.RpcError as e:
            raise _map_nack_error(e) from e

        # Check per-message result for errors.
        if resp.results:
            result = resp.results[0]
            which = result.WhichOneof("result")
            if which == "error":
                from fila.errors import MessageNotFoundError, RPCError as _RPCError

                nack_err = result.error
                if nack_err.code == service_pb2.NACK_ERROR_CODE_MESSAGE_NOT_FOUND:
                    raise MessageNotFoundError(f"nack: {nack_err.message}")
                raise _RPCError(grpc.StatusCode.INTERNAL, f"nack: {nack_err.message}")
