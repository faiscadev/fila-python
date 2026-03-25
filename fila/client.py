"""Synchronous Fila client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import grpc

from fila.batcher import AutoAccumulator, LingerAccumulator
from fila.errors import (
    EnqueueError,
    _map_ack_error,
    _map_consume_error,
    _map_enqueue_error,
    _map_enqueue_result_error,
    _map_nack_error,
)
from fila.types import AccumulatorMode, ConsumeMessage, EnqueueResult, Linger
from fila.v1 import service_pb2, service_pb2_grpc

if TYPE_CHECKING:
    from collections.abc import Iterator

_LEADER_HINT_KEY = "x-fila-leader-addr"


def _extract_leader_hint(err: grpc.RpcError) -> str | None:
    """Return the leader address from trailing metadata, if present.

    The server sets ``x-fila-leader-addr`` in trailing metadata alongside an
    UNAVAILABLE status when the node is not the leader for the requested queue.
    """
    if err.code() != grpc.StatusCode.UNAVAILABLE:
        return None
    trailing = err.trailing_metadata()
    if trailing is None:
        return None
    for key, value in trailing:
        if key == _LEADER_HINT_KEY:
            return str(value)
    return None


def _proto_msg_to_consume_message(msg: Any) -> ConsumeMessage:
    """Convert a protobuf Message to a ConsumeMessage."""
    metadata = msg.metadata
    return ConsumeMessage(
        id=msg.id,
        headers=dict(msg.headers),
        payload=bytes(msg.payload),
        fairness_key=metadata.fairness_key if metadata else "",
        attempt_count=metadata.attempt_count if metadata else 0,
        queue=metadata.queue_id if metadata else "",
    )


def _proto_enqueue_result_to_sdk(result: Any) -> EnqueueResult:
    """Convert a proto EnqueueResult to the SDK type."""
    which = result.WhichOneof("result")
    if which == "message_id":
        return EnqueueResult(message_id=str(result.message_id), error=None)
    return EnqueueResult(message_id=None, error=result.error.message)


class _ClientCallDetails(
    grpc.ClientCallDetails,  # type: ignore[misc]
):
    """Concrete ``ClientCallDetails`` that can be instantiated.

    ``grpc.ClientCallDetails`` is an abstract class with no ``__init__``, so we
    need our own subclass to carry the fields through the interceptor chain.
    """

    def __init__(
        self,
        method: str,
        timeout: float | None,
        metadata: list[tuple[str, str | bytes]] | None,
        credentials: grpc.CallCredentials | None,
        wait_for_ready: bool | None,
        compression: grpc.Compression | None,
    ) -> None:
        self.method = method
        self.timeout = timeout
        self.metadata = metadata
        self.credentials = credentials
        self.wait_for_ready = wait_for_ready
        self.compression = compression


class _ApiKeyInterceptor(
    grpc.UnaryUnaryClientInterceptor,  # type: ignore[misc]
    grpc.UnaryStreamClientInterceptor,  # type: ignore[misc]
):
    """Injects ``authorization: Bearer <key>`` metadata into every RPC."""

    def __init__(self, api_key: str) -> None:
        self._metadata = (("authorization", f"Bearer {api_key}"),)

    def _inject(
        self, client_call_details: grpc.ClientCallDetails
    ) -> _ClientCallDetails:
        metadata = list(client_call_details.metadata or [])
        metadata.extend(self._metadata)
        return _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )

    def intercept_unary_unary(
        self,
        continuation: Any,
        client_call_details: grpc.ClientCallDetails,
        request: Any,
    ) -> Any:
        return continuation(self._inject(client_call_details), request)

    def intercept_unary_stream(
        self,
        continuation: Any,
        client_call_details: grpc.ClientCallDetails,
        request: Any,
    ) -> Any:
        return continuation(self._inject(client_call_details), request)


class Client:
    """Synchronous client for the Fila message broker.

    Wraps the hot-path gRPC operations: enqueue, enqueue_many, consume, ack,
    nack.

    Usage::

        client = Client("localhost:5555")
        msg_id = client.enqueue("my-queue", {"tenant": "acme"}, b"hello")
        for msg in client.consume("my-queue"):
            client.ack("my-queue", msg.id)
        client.close()

    Or as a context manager::

        with Client("localhost:5555") as client:
            client.enqueue("my-queue", None, b"hello")

    Accumulator modes::

        # AUTO (default): opportunistic accumulation via background thread
        client = Client("localhost:5555")

        # DISABLED: each enqueue() is a direct RPC
        client = Client("localhost:5555", accumulator_mode=AccumulatorMode.DISABLED)

        # LINGER: timer-based forced accumulation
        client = Client("localhost:5555", accumulator_mode=Linger(linger_ms=10, max_messages=100))

    TLS (system trust store)::

        client = Client("localhost:5555", tls=True)

    TLS (custom CA)::

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
        tls: bool = False,
        ca_cert: bytes | None = None,
        client_cert: bytes | None = None,
        client_key: bytes | None = None,
        api_key: str | None = None,
        accumulator_mode: AccumulatorMode | Linger = AccumulatorMode.AUTO,
        max_accumulator_messages: int = 1000,
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
            accumulator_mode: Controls how ``enqueue()`` routes messages.
                              Defaults to ``AccumulatorMode.AUTO``
                              (opportunistic accumulation).
            max_accumulator_messages: Maximum number of messages per flush when
                                     using ``AccumulatorMode.AUTO``.
                                     Defaults to 1000.
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

        # Set up the accumulator based on the chosen mode.
        self._accumulator: AutoAccumulator | LingerAccumulator | None = None
        if isinstance(accumulator_mode, Linger):
            self._accumulator = LingerAccumulator(
                self._stub,
                linger_ms=accumulator_mode.linger_ms,
                max_messages=accumulator_mode.max_messages,
            )
        elif accumulator_mode is AccumulatorMode.AUTO:
            self._accumulator = AutoAccumulator(
                self._stub,
                max_messages=max_accumulator_messages,
            )
        # AccumulatorMode.DISABLED: self._accumulator stays None

    def _make_channel(self, addr: str) -> grpc.Channel:
        """Create a gRPC channel to the given address using stored credentials."""
        use_tls = self._tls or self._ca_cert is not None

        if use_tls:
            creds = grpc.ssl_channel_credentials(
                root_certificates=self._ca_cert,
                private_key=self._client_key,
                certificate_chain=self._client_cert,
            )
            channel: grpc.Channel = grpc.secure_channel(addr, creds)
        else:
            channel = grpc.insecure_channel(addr)

        if self._api_key is not None:
            interceptor = _ApiKeyInterceptor(self._api_key)
            channel = grpc.intercept_channel(channel, interceptor)

        return channel

    def close(self) -> None:
        """Drain pending accumulated messages and close the underlying gRPC channel."""
        if self._accumulator is not None:
            self._accumulator.close()
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

        When an accumulator is active (``AccumulatorMode.AUTO`` or ``Linger``),
        the message is submitted to the background accumulator and this call
        blocks until the flush completes and the result is available.

        When accumulation is disabled (``AccumulatorMode.DISABLED``), this call
        makes a direct synchronous RPC.

        Args:
            queue: Target queue name.
            headers: Optional message headers.
            payload: Message payload bytes.

        Returns:
            Broker-assigned message ID (UUIDv7).

        Raises:
            QueueNotFoundError: If the queue does not exist (DISABLED mode).
            EnqueueError: If the enqueue RPC fails (AUTO/LINGER mode).
            RPCError: For unexpected gRPC failures.
        """
        proto = service_pb2.EnqueueMessage(
            queue=queue,
            headers=headers or {},
            payload=payload,
        )

        if self._accumulator is not None:
            future = self._accumulator.submit(proto)
            return future.result()

        # Direct RPC (DISABLED mode).
        try:
            resp = self._stub.Enqueue(
                service_pb2.EnqueueRequest(messages=[proto])
            )
        except grpc.RpcError as e:
            raise _map_enqueue_error(e) from e

        result = resp.results[0]
        which = result.WhichOneof("result")
        if which == "message_id":
            return str(result.message_id)
        raise _map_enqueue_result_error(result.error.code, result.error.message)

    def enqueue_many(
        self,
        messages: list[tuple[str, dict[str, str] | None, bytes]],
    ) -> list[EnqueueResult]:
        """Enqueue multiple messages in a single RPC.

        This is an explicit multi-message operation that always uses the
        Enqueue RPC directly, regardless of the accumulator_mode setting.

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
            resp = self._stub.Enqueue(
                service_pb2.EnqueueRequest(messages=proto_messages)
            )
        except grpc.RpcError as e:
            raise _map_enqueue_error(e) from e

        return [_proto_enqueue_result_to_sdk(r) for r in resp.results]

    def consume(self, queue: str) -> Iterator[ConsumeMessage]:
        """Open a streaming consumer on the specified queue.

        Yields messages as they become available. The iterator ends when the
        server stream closes or an error occurs. Skip nil message frames
        (keepalive signals) automatically.

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
                stream = self._reconnect_and_consume(leader_addr, queue)
            else:
                raise _map_consume_error(e) from e

        return self._consume_iter(stream)

    def _reconnect_and_consume(self, leader_addr: str, queue: str) -> Any:
        """Create a new channel to *leader_addr* and retry the consume call."""
        self._channel.close()
        self._channel = self._make_channel(leader_addr)
        self._stub = service_pb2_grpc.FilaServiceStub(self._channel)  # type: ignore[no-untyped-call]
        if self._accumulator is not None:
            self._accumulator.update_stub(self._stub)
        try:
            return self._stub.Consume(
                service_pb2.ConsumeRequest(queue=queue)
            )
        except grpc.RpcError as e:
            raise _map_consume_error(e) from e

    def _consume_iter(
        self,
        stream: Any,
    ) -> Iterator[ConsumeMessage]:
        """Internal generator reading from the gRPC stream."""
        try:
            for resp in stream:
                for msg in resp.messages:
                    if msg is not None and msg.ByteSize():
                        yield _proto_msg_to_consume_message(msg)
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
            resp = self._stub.Ack(
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
            resp = self._stub.Nack(
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
