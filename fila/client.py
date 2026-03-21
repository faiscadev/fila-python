"""Synchronous Fila client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import grpc

from fila.errors import _map_ack_error, _map_consume_error, _map_enqueue_error, _map_nack_error
from fila.types import ConsumeMessage
from fila.v1 import service_pb2, service_pb2_grpc

if TYPE_CHECKING:
    from collections.abc import Iterator


class _ApiKeyInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
):
    """Injects ``authorization: Bearer <key>`` metadata into every RPC."""

    def __init__(self, api_key: str) -> None:
        self._metadata = (("authorization", f"Bearer {api_key}"),)

    def _inject(
        self, client_call_details: grpc.ClientCallDetails
    ) -> grpc.ClientCallDetails:
        metadata = list(client_call_details.metadata or [])
        metadata.extend(self._metadata)
        return grpc.ClientCallDetails(  # type: ignore[call-arg]
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )

    def intercept_unary_unary(  # type: ignore[override]
        self,
        continuation: Any,
        client_call_details: grpc.ClientCallDetails,
        request: Any,
    ) -> Any:
        return continuation(self._inject(client_call_details), request)

    def intercept_unary_stream(  # type: ignore[override]
        self,
        continuation: Any,
        client_call_details: grpc.ClientCallDetails,
        request: Any,
    ) -> Any:
        return continuation(self._inject(client_call_details), request)


class Client:
    """Synchronous client for the Fila message broker.

    Wraps the hot-path gRPC operations: enqueue, consume, ack, nack.

    Usage::

        client = Client("localhost:5555")
        msg_id = client.enqueue("my-queue", {"tenant": "acme"}, b"hello")
        for msg in client.consume("my-queue"):
            client.ack("my-queue", msg.id)
        client.close()

    Or as a context manager::

        with Client("localhost:5555") as client:
            client.enqueue("my-queue", None, b"hello")

    TLS::

        with open("ca.pem", "rb") as f:
            ca = f.read()
        client = Client("localhost:5555", ca_cert=ca)

    mTLS + API key::

        client = Client(
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
        ca_cert: bytes | None = None,
        client_cert: bytes | None = None,
        client_key: bytes | None = None,
        api_key: str | None = None,
    ) -> None:
        """Connect to a Fila broker at the given address.

        Args:
            addr: Broker address in "host:port" format (e.g., "localhost:5555").
            ca_cert: PEM-encoded CA certificate for verifying the server.
                     When provided, a TLS channel is used instead of an insecure one.
            client_cert: PEM-encoded client certificate for mutual TLS (optional).
            client_key: PEM-encoded client private key for mutual TLS (optional).
            api_key: API key for authentication. When set, every RPC includes an
                     ``authorization: Bearer <key>`` metadata header.
        """
        if ca_cert is not None:
            creds = grpc.ssl_channel_credentials(
                root_certificates=ca_cert,
                private_key=client_key,
                certificate_chain=client_cert,
            )
            self._channel = grpc.secure_channel(addr, creds)
        else:
            self._channel = grpc.insecure_channel(addr)

        if api_key is not None:
            interceptor = _ApiKeyInterceptor(api_key)
            self._channel = grpc.intercept_channel(self._channel, interceptor)

        self._stub = service_pb2_grpc.FilaServiceStub(self._channel)  # type: ignore[no-untyped-call]

    def close(self) -> None:
        """Close the underlying gRPC channel."""
        self._channel.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def enqueue(
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
            resp = self._stub.Enqueue(
                service_pb2.EnqueueRequest(
                    queue=queue,
                    headers=headers or {},
                    payload=payload,
                )
            )
        except grpc.RpcError as e:
            raise _map_enqueue_error(e) from e
        return str(resp.message_id)

    def consume(self, queue: str) -> Iterator[ConsumeMessage]:
        """Open a streaming consumer on the specified queue.

        Yields messages as they become available. The iterator ends when the
        server stream closes or an error occurs. Skip nil message frames
        (keepalive signals) automatically.

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

    def _consume_iter(
        self,
        stream: Any,
    ) -> Iterator[ConsumeMessage]:
        """Internal generator reading from the gRPC stream."""
        try:
            for resp in stream:
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

    def ack(self, queue: str, msg_id: str) -> None:
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
            self._stub.Ack(
                service_pb2.AckRequest(queue=queue, message_id=msg_id)
            )
        except grpc.RpcError as e:
            raise _map_ack_error(e) from e

    def nack(self, queue: str, msg_id: str, error: str) -> None:
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
            self._stub.Nack(
                service_pb2.NackRequest(
                    queue=queue, message_id=msg_id, error=error
                )
            )
        except grpc.RpcError as e:
            raise _map_nack_error(e) from e
