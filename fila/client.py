"""Synchronous Fila client (FIBP transport)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fila.batcher import AutoAccumulator, LingerAccumulator
from fila.errors import (
    _map_ack_error_code,
    _map_enqueue_error_code,
    _map_fibp_error,
    _map_nack_error_code,
)
from fila.fibp import (
    FibpConnection,
    FibpError,
    decode_ack_nack_response,
    decode_consume_message,
    decode_enqueue_response,
    encode_ack,
    encode_consume,
    encode_enqueue,
    encode_nack,
    make_ssl_context,
    parse_addr,
)
from fila.types import AccumulatorMode, ConsumeMessage, EnqueueResult, Linger

if TYPE_CHECKING:
    import ssl
    from collections.abc import Iterator


class Client:
    """Synchronous client for the Fila message broker (FIBP transport).

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

        # DISABLED: each enqueue() is a direct FIBP call
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
            addr: Broker address in ``"host:port"`` format (e.g., ``"localhost:5555"``).
            tls: Enable TLS using the OS system trust store for server
                 verification.  Ignored when ``ca_cert`` is provided (which
                 implies TLS).  Defaults to ``False``.
            ca_cert: PEM-encoded CA certificate for verifying the server.
                     When provided, a TLS channel is used instead of plain TCP.
            client_cert: PEM-encoded client certificate for mutual TLS (optional).
            client_key: PEM-encoded client private key for mutual TLS (optional).
            api_key: API key for authentication.  Sent as an AUTH frame on connect.
            accumulator_mode: Controls how ``enqueue()`` routes messages.
                              Defaults to ``AccumulatorMode.AUTO``.
            max_accumulator_messages: Maximum number of messages per flush when
                                      using ``AccumulatorMode.AUTO``.
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

        self._accumulator: AutoAccumulator | LingerAccumulator | None = None
        if isinstance(accumulator_mode, Linger):
            self._accumulator = LingerAccumulator(
                self._conn,
                linger_ms=accumulator_mode.linger_ms,
                max_messages=accumulator_mode.max_messages,
            )
        elif accumulator_mode is AccumulatorMode.AUTO:
            self._accumulator = AutoAccumulator(
                self._conn,
                max_messages=max_accumulator_messages,
            )

    def _make_ssl_ctx(self) -> ssl.SSLContext | None:
        use_tls = self._tls or self._ca_cert is not None
        if not use_tls:
            return None
        return make_ssl_context(
            ca_cert=self._ca_cert,
            client_cert=self._client_cert,
            client_key=self._client_key,
        )

    def _make_conn(self, addr: str) -> FibpConnection:
        host, port = parse_addr(addr)
        ssl_ctx = self._make_ssl_ctx()
        return FibpConnection(host, port, ssl_ctx=ssl_ctx, api_key=self._api_key)

    def close(self) -> None:
        """Drain pending accumulated messages and close the connection."""
        if self._accumulator is not None:
            self._accumulator.close()
        self._conn.close()

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
        blocks until the flush completes.

        When accumulation is disabled (``AccumulatorMode.DISABLED``), this call
        makes a direct synchronous FIBP request.

        Args:
            queue: Target queue name.
            headers: Optional message headers.
            payload: Message payload bytes.

        Returns:
            Broker-assigned message ID (UUIDv7).

        Raises:
            QueueNotFoundError: If the queue does not exist.
            EnqueueError: If the enqueue fails (AUTO/LINGER mode).
            TransportError: For unexpected FIBP failures.
        """
        if self._accumulator is not None:
            fut = self._accumulator.submit(queue, headers or {}, payload)
            return fut.result()

        # Direct FIBP call (DISABLED mode).
        corr_id = self._conn.alloc_corr_id()
        frame = encode_enqueue(corr_id, [(queue, headers or {}, payload)])
        try:
            body = self._conn.send_request(frame, corr_id).result()
        except FibpError as e:
            raise _map_fibp_error(e.code, e.message) from e

        results = decode_enqueue_response(body)
        ok, msg_id, err_code, err_msg = results[0]
        if ok:
            return msg_id
        raise _map_enqueue_error_code(err_code, err_msg)

    def enqueue_many(
        self,
        messages: list[tuple[str, dict[str, str] | None, bytes]],
    ) -> list[EnqueueResult]:
        """Enqueue multiple messages, possibly targeting different queues.

        Always issues FIBP requests directly (bypasses the accumulator).

        Args:
            messages: List of ``(queue, headers, payload)`` tuples.

        Returns:
            List of ``EnqueueResult`` objects, one per input message.

        Raises:
            TransportError: For unexpected FIBP failures.
        """
        # Group messages by queue so each FIBP frame targets one queue.
        from collections import defaultdict
        by_queue: dict[str, list[tuple[int, dict[str, str], bytes]]] = defaultdict(list)
        order: list[tuple[str, int]] = []  # (queue, local_index)
        local_indices: dict[str, int] = defaultdict(int)

        for queue, hdrs, payload in messages:
            idx = local_indices[queue]
            local_indices[queue] += 1
            by_queue[queue].append((idx, hdrs or {}, payload))
            order.append((queue, idx))

        # Send one FIBP ENQUEUE per queue.
        results_by_queue: dict[str, list[EnqueueResult]] = {}
        for queue_name, items in by_queue.items():
            corr_id = self._conn.alloc_corr_id()
            msgs = [(queue_name, h, p) for _, h, p in items]
            frame = encode_enqueue(corr_id, msgs)
            try:
                body = self._conn.send_request(frame, corr_id).result()
            except FibpError as e:
                err = str(e)
                results_by_queue[queue_name] = [
                    EnqueueResult(message_id=None, error=err) for _ in items
                ]
                continue
            decoded = decode_enqueue_response(body)
            per_queue_results: list[EnqueueResult] = []
            for ok, msg_id, _err_code, err_msg in decoded:
                if ok:
                    per_queue_results.append(EnqueueResult(message_id=msg_id, error=None))
                else:
                    per_queue_results.append(EnqueueResult(message_id=None, error=err_msg))
            results_by_queue[queue_name] = per_queue_results

        # Reconstruct in original input order.
        per_queue_counters: dict[str, int] = defaultdict(int)
        final: list[EnqueueResult] = []
        for queue_name, _ in order:
            idx = per_queue_counters[queue_name]
            per_queue_counters[queue_name] += 1
            final.append(results_by_queue[queue_name][idx])
        return final

    def consume(self, queue: str) -> Iterator[ConsumeMessage]:
        """Open a streaming consumer on the specified queue.

        Yields messages as they become available.  The iterator ends when the
        server closes the stream.

        If the server returns a leader-hint error, the client transparently
        reconnects to the leader address and retries once.

        Args:
            queue: Queue to consume from.

        Yields:
            ConsumeMessage objects as they arrive.

        Raises:
            QueueNotFoundError: If the queue does not exist.
            TransportError: For unexpected FIBP failures.
        """
        corr_id = self._conn.alloc_corr_id()
        frame = encode_consume(corr_id, queue)
        try:
            cq = self._conn.open_consume_stream(frame, corr_id)
        except FibpError as e:
            raise _map_fibp_error(e.code, e.message) from e

        return self._consume_iter(cq)

    def _consume_iter(self, cq: object) -> Iterator[ConsumeMessage]:
        from fila.fibp import _ConsumeQueue
        assert isinstance(cq, _ConsumeQueue)
        while True:
            body = cq.get()
            if body is None:
                return
            try:
                msg_id, queue, headers, payload, fairness_key, attempt_count = (
                    decode_consume_message(body)
                )
            except Exception:
                continue
            yield ConsumeMessage(
                id=msg_id,
                headers=headers,
                payload=payload,
                fairness_key=fairness_key,
                attempt_count=attempt_count,
                queue=queue,
            )

    def ack(self, queue: str, msg_id: str) -> None:
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
            body = self._conn.send_request(frame, corr_id).result()
        except FibpError as e:
            raise _map_fibp_error(e.code, e.message) from e

        results = decode_ack_nack_response(body)
        if results:
            ok, err_code, err_msg = results[0]
            if not ok:
                raise _map_ack_error_code(err_code, err_msg)

    def nack(self, queue: str, msg_id: str, error: str) -> None:
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
            body = self._conn.send_request(frame, corr_id).result()
        except FibpError as e:
            raise _map_fibp_error(e.code, e.message) from e

        results = decode_ack_nack_response(body)
        if results:
            ok, err_code, err_msg = results[0]
            if not ok:
                raise _map_nack_error_code(err_code, err_msg)
