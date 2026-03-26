"""FIBP (Fila Binary Protocol) transport layer.

Wire format
-----------
Every message is a length-prefixed frame::

    [4-byte big-endian total-payload-length][flags:u8][op:u8][corr_id:u32][body...]

The 4-byte prefix encodes the number of bytes that follow it (flags + op +
corr_id + body).  Minimum payload is 6 bytes (flags + op + corr_id with no
body).

Handshake
---------
Client sends ``FIBP\\x01\\x00`` (6 bytes).  Server echoes the same 6 bytes.

Op codes
--------
Hot path (binary body):
  0x01 ENQUEUE, 0x02 CONSUME, 0x03 ACK, 0x04 NACK

Admin (protobuf body):
  0x10 CREATE_QUEUE, 0x11 DELETE_QUEUE, 0x12 QUEUE_STATS,
  0x13 LIST_QUEUES,  0x14 PAUSE_QUEUE,  0x15 RESUME_QUEUE, 0x16 REDRIVE

Flow / control:
  0x20 FLOW, 0x21 HEARTBEAT

Auth:
  0x30 AUTH

Responses / errors:
  0xFE ERROR, 0xFF GOAWAY

Flag bits
---------
Bit 2 (0x04) — SERVER_PUSH: set by server on streamed consume frames.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import ssl
import struct
import threading
from concurrent.futures import Future

# ------------------------------------------------------------------
# Protocol constants
# ------------------------------------------------------------------

MAGIC = b"FIBP\x01\x00"

OP_ENQUEUE = 0x01
OP_CONSUME = 0x02
OP_ACK = 0x03
OP_NACK = 0x04

OP_CREATE_QUEUE = 0x10
OP_DELETE_QUEUE = 0x11
OP_QUEUE_STATS = 0x12
OP_LIST_QUEUES = 0x13
OP_PAUSE_QUEUE = 0x14
OP_RESUME_QUEUE = 0x15
OP_REDRIVE = 0x16

OP_FLOW = 0x20
OP_HEARTBEAT = 0x21

OP_AUTH = 0x30

OP_ERROR = 0xFE
OP_GOAWAY = 0xFF

FLAG_SERVER_PUSH = 0x04

# Frame header: flags(1) + op(1) + corr_id(4) = 6 bytes after the 4-byte length prefix.
_FRAME_HEADER_FMT = ">IBBI"  # length(4) + flags(1) + op(1) + corr_id(4)
_FRAME_HEADER_SIZE = struct.calcsize(_FRAME_HEADER_FMT)  # 10

# Error codes surfaced in 0xFE ERROR frames.
ERR_QUEUE_NOT_FOUND = 1
ERR_MESSAGE_NOT_FOUND = 2
ERR_PERMISSION_DENIED = 3
ERR_AUTH_REQUIRED = 4
ERR_INTERNAL = 255

# Consume initial credits (number of messages the server may push before
# the client must grant more).  Large enough for a long-running stream.
_DEFAULT_CONSUME_CREDITS = 1_000_000


# ------------------------------------------------------------------
# Encoding helpers
# ------------------------------------------------------------------

def _encode_str(s: str) -> bytes:
    """Encode a string as u16-prefixed UTF-8."""
    b = s.encode()
    return struct.pack(">H", len(b)) + b


def _encode_frame(flags: int, op: int, corr_id: int, body: bytes) -> bytes:
    """Build a complete FIBP frame."""
    payload = struct.pack(">BBI", flags, op, corr_id) + body
    return struct.pack(">I", len(payload)) + payload


def encode_enqueue(corr_id: int, messages: list[tuple[str, dict[str, str], bytes]]) -> bytes:
    """Encode an ENQUEUE request frame.

    Wire format for body::

        queue_len:u16 | queue:utf8 | msg_count:u16 | messages...

    Each message::

        header_count:u8 | (key_len:u16 key val_len:u16 val)... | payload_len:u32 | payload
    """
    # All messages in one ENQUEUE frame must target the same queue.
    queue = messages[0][0]
    parts: list[bytes] = [_encode_str(queue), struct.pack(">H", len(messages))]
    for _queue, headers, payload in messages:
        h = headers or {}
        parts.append(struct.pack(">B", len(h)))
        for k, v in h.items():
            parts.append(_encode_str(k))
            parts.append(_encode_str(v))
        parts.append(struct.pack(">I", len(payload)) + payload)
    return _encode_frame(0, OP_ENQUEUE, corr_id, b"".join(parts))


def encode_consume(
    corr_id: int,
    queue: str,
    initial_credits: int = _DEFAULT_CONSUME_CREDITS,
) -> bytes:
    """Encode a CONSUME request frame.

    Wire format for body::

        queue_len:u16 | queue:utf8 | initial_credits:u32
    """
    body = _encode_str(queue) + struct.pack(">I", initial_credits)
    return _encode_frame(0, OP_CONSUME, corr_id, body)


def encode_ack(corr_id: int, items: list[tuple[str, str]]) -> bytes:
    """Encode an ACK request frame.

    Wire format for body::

        item_count:u16 | (queue_len:u16 queue msg_id_len:u16 msg_id)...
    """
    parts: list[bytes] = [struct.pack(">H", len(items))]
    for queue, msg_id in items:
        parts.append(_encode_str(queue))
        parts.append(_encode_str(msg_id))
    return _encode_frame(0, OP_ACK, corr_id, b"".join(parts))


def encode_nack(corr_id: int, items: list[tuple[str, str, str]]) -> bytes:
    """Encode a NACK request frame.

    Wire format for body::

        item_count:u16 | (queue_len:u16 queue msg_id_len:u16 msg_id err_len:u16 err_msg)...
    """
    parts: list[bytes] = [struct.pack(">H", len(items))]
    for queue, msg_id, error in items:
        parts.append(_encode_str(queue))
        parts.append(_encode_str(msg_id))
        parts.append(_encode_str(error))
    return _encode_frame(0, OP_NACK, corr_id, b"".join(parts))


def encode_auth(corr_id: int, api_key: str) -> bytes:
    """Encode an AUTH frame carrying the API key."""
    return _encode_frame(0, OP_AUTH, corr_id, _encode_str(api_key))


def encode_admin(op: int, corr_id: int, proto_body: bytes) -> bytes:
    """Encode an admin frame with a protobuf-serialised body."""
    return _encode_frame(0, op, corr_id, proto_body)


# ------------------------------------------------------------------
# Decoding helpers
# ------------------------------------------------------------------

def _decode_str(data: bytes, offset: int) -> tuple[str, int]:
    """Read a u16-prefixed UTF-8 string; return (value, new_offset)."""
    (length,) = struct.unpack_from(">H", data, offset)
    offset += 2
    return data[offset: offset + length].decode(), offset + length


def decode_enqueue_response(body: bytes) -> list[tuple[bool, str, int, str]]:
    """Decode an ENQUEUE response body.

    Returns a list of ``(ok, message_id, error_code, error_message)`` tuples.
    When ``ok`` is True, ``message_id`` is set; otherwise ``error_code`` and
    ``error_message`` describe the failure.
    """
    (count,) = struct.unpack_from(">H", body, 0)
    offset = 2
    results: list[tuple[bool, str, int, str]] = []
    for _ in range(count):
        (ok,) = struct.unpack_from(">B", body, offset)
        offset += 1
        if ok:
            msg_id, offset = _decode_str(body, offset)
            results.append((True, msg_id, 0, ""))
        else:
            (err_code,) = struct.unpack_from(">H", body, offset)
            offset += 2
            err_msg, offset = _decode_str(body, offset)
            results.append((False, "", err_code, err_msg))
    return results


def decode_consume_message(body: bytes) -> tuple[str, str, dict[str, str], bytes, str, int]:
    """Decode a single server-pushed consume frame body.

    Returns ``(msg_id, queue, headers, payload, fairness_key, attempt_count)``.

    The consume push wire format is::

        msg_id_len:u16 | msg_id
        queue_len:u16 | queue
        fairness_key_len:u16 | fairness_key
        attempt_count:u32
        header_count:u8 | (key_len:u16 key val_len:u16 val)...
        payload_len:u32 | payload
    """
    offset = 0
    msg_id, offset = _decode_str(body, offset)
    queue, offset = _decode_str(body, offset)
    fairness_key, offset = _decode_str(body, offset)
    (attempt_count,) = struct.unpack_from(">I", body, offset)
    offset += 4
    (header_count,) = struct.unpack_from(">B", body, offset)
    offset += 1
    headers: dict[str, str] = {}
    for _ in range(header_count):
        k, offset = _decode_str(body, offset)
        v, offset = _decode_str(body, offset)
        headers[k] = v
    (payload_len,) = struct.unpack_from(">I", body, offset)
    offset += 4
    payload = body[offset: offset + payload_len]
    return msg_id, queue, headers, payload, fairness_key, attempt_count


def decode_ack_nack_response(body: bytes) -> list[tuple[bool, int, str]]:
    """Decode an ACK or NACK response body.

    Returns a list of ``(ok, error_code, error_message)`` tuples.
    """
    (count,) = struct.unpack_from(">H", body, 0)
    offset = 2
    results: list[tuple[bool, int, str]] = []
    for _ in range(count):
        (ok,) = struct.unpack_from(">B", body, offset)
        offset += 1
        if ok:
            results.append((True, 0, ""))
        else:
            (err_code,) = struct.unpack_from(">H", body, offset)
            offset += 2
            err_msg, offset = _decode_str(body, offset)
            results.append((False, err_code, err_msg))
    return results


def decode_error_frame(body: bytes) -> tuple[int, str]:
    """Decode a 0xFE ERROR frame body.  Returns ``(error_code, message)``."""
    (code,) = struct.unpack_from(">H", body, 0)
    msg, _ = _decode_str(body, 2)
    return code, msg


# ------------------------------------------------------------------
# Synchronous connection
# ------------------------------------------------------------------

class FibpError(Exception):
    """Transport-level FIBP error (op 0xFE or connection failure)."""

    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"fibp error ({code}): {message}")


class _ConsumeQueue:
    """Thread-safe queue for server-pushed consume frames on one correlation ID."""

    def __init__(self) -> None:
        import queue
        self._q: queue.Queue[bytes | None] = queue.Queue()

    def put(self, frame_body: bytes | None) -> None:
        self._q.put(frame_body)

    def get(self, timeout: float | None = None) -> bytes | None:
        import queue
        try:
            return self._q.get(timeout=timeout)
        except queue.Empty:
            return None

    def close(self) -> None:
        """Signal end-of-stream."""
        self._q.put(None)


class FibpConnection:
    """Synchronous FIBP connection over a raw TCP socket.

    A background reader thread receives frames and dispatches them to
    waiting callers via per-correlation-ID ``Future`` objects (for
    request/response ops) or ``_ConsumeQueue`` objects (for streaming
    consume ops).
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        ssl_ctx: ssl.SSLContext | None = None,
        api_key: str | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._ssl_ctx = ssl_ctx
        self._api_key = api_key

        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._next_corr_id: int = 1
        # corr_id → Future[bytes] for request/response ops
        self._pending: dict[int, Future[bytes]] = {}
        # corr_id → _ConsumeQueue for streaming consume ops
        self._consume_queues: dict[int, _ConsumeQueue] = {}

        self._sock = self._connect()
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    # ------------------------------------------------------------------
    # Connection setup
    # ------------------------------------------------------------------

    def _connect(self) -> socket.socket:
        raw = socket.create_connection((self._host, self._port))
        raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if self._ssl_ctx is not None:
            sock: socket.socket = self._ssl_ctx.wrap_socket(raw, server_hostname=self._host)
        else:
            sock = raw

        # Handshake.
        sock.sendall(MAGIC)
        echo = _recv_exactly(sock, len(MAGIC))
        if echo != MAGIC:
            sock.close()
            raise FibpError(0, f"handshake failed: expected {MAGIC!r}, got {echo!r}")

        # Auth frame (corr_id 0 — fire and forget, server sends no response).
        if self._api_key is not None:
            sock.sendall(encode_auth(0, self._api_key))

        return sock

    def close(self) -> None:
        """Close the connection."""
        with contextlib.suppress(OSError):
            self._sock.close()
        # Wake any blocked consume queues.
        with self._lock:
            for cq in self._consume_queues.values():
                cq.close()
            self._consume_queues.clear()
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(FibpError(0, "connection closed"))
            self._pending.clear()

    # ------------------------------------------------------------------
    # Correlation-ID allocation
    # ------------------------------------------------------------------

    def _alloc_corr_id(self) -> int:
        with self._lock:
            cid = self._next_corr_id
            self._next_corr_id = (self._next_corr_id + 1) & 0xFFFF_FFFF
            if self._next_corr_id == 0:
                self._next_corr_id = 1
            return cid

    # ------------------------------------------------------------------
    # Request/response send
    # ------------------------------------------------------------------

    def send_request(self, frame: bytes, corr_id: int) -> Future[bytes]:
        """Register a pending future and send *frame*; return the future."""
        fut: Future[bytes] = Future()
        with self._lock:
            self._pending[corr_id] = fut
        with self._send_lock:
            self._sock.sendall(frame)
        return fut

    def open_consume_stream(self, frame: bytes, corr_id: int) -> _ConsumeQueue:
        """Register a consume queue, send *frame*, and return the queue."""
        cq = _ConsumeQueue()
        with self._lock:
            self._consume_queues[corr_id] = cq
        with self._send_lock:
            self._sock.sendall(frame)
        return cq

    def alloc_corr_id(self) -> int:
        return self._alloc_corr_id()

    # ------------------------------------------------------------------
    # Background reader
    # ------------------------------------------------------------------

    def _read_loop(self) -> None:
        try:
            while True:
                # Read 4-byte length prefix.
                length_buf = _recv_exactly(self._sock, 4)
                if not length_buf:
                    break
                (payload_len,) = struct.unpack(">I", length_buf)

                # Read flags + op + corr_id + body.
                payload = _recv_exactly(self._sock, payload_len)
                if len(payload) < 6:
                    break
                flags, op, corr_id = struct.unpack_from(">BBI", payload, 0)
                body = payload[6:]

                self._dispatch(flags, op, corr_id, body)
        except (OSError, struct.error):
            pass
        finally:
            # Close all waiting callers.
            with self._lock:
                for cq in self._consume_queues.values():
                    cq.close()
                self._consume_queues.clear()
                for fut in self._pending.values():
                    if not fut.done():
                        fut.set_exception(FibpError(0, "connection lost"))
                self._pending.clear()

    def _dispatch(self, flags: int, op: int, corr_id: int, body: bytes) -> None:
        is_push = bool(flags & FLAG_SERVER_PUSH)

        if is_push:
            # Server-pushed consume frame.
            with self._lock:
                cq = self._consume_queues.get(corr_id)
            if cq is not None:
                cq.put(body)
            return

        if op == OP_GOAWAY:
            # Server is shutting down; wake all waiters.
            with self._lock:
                for cq in self._consume_queues.values():
                    cq.close()
                self._consume_queues.clear()
                for pending_fut in self._pending.values():
                    if not pending_fut.done():
                        pending_fut.set_exception(FibpError(0, "server sent GOAWAY"))
                self._pending.clear()
            return

        # Resolve a pending future.
        with self._lock:
            fut: Future[bytes] | None = self._pending.pop(corr_id, None)
            # Also check if this is the "end of consume stream" signal
            # (op == OP_CONSUME response with no push flag).
            cq = self._consume_queues.get(corr_id)

        if cq is not None and op == OP_CONSUME:
            # Server closed the consume stream.
            cq.close()
            with self._lock:
                self._consume_queues.pop(corr_id, None)
            return

        if fut is not None and not fut.done():
            if op == OP_ERROR:
                code, msg = decode_error_frame(body)
                fut.set_exception(FibpError(code, msg))
            else:
                fut.set_result(body)


# ------------------------------------------------------------------
# Async connection
# ------------------------------------------------------------------

class AsyncFibpConnection:
    """Asynchronous FIBP connection using asyncio streams.

    A background reader task dispatches frames to per-correlation-ID
    ``asyncio.Future`` objects or ``asyncio.Queue`` objects (for consume
    streams).
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        ssl_ctx: ssl.SSLContext | None = None,
        api_key: str | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._ssl_ctx = ssl_ctx
        self._api_key = api_key

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._next_corr_id: int = 1
        self._write_lock: asyncio.Lock | None = None
        # corr_id → asyncio.Future[bytes]
        self._pending: dict[int, asyncio.Future[bytes]] = {}
        # corr_id → asyncio.Queue[bytes | None]
        self._consume_queues: dict[int, asyncio.Queue[bytes | None]] = {}
        self._reader_task: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        """Open the TCP connection, perform FIBP handshake, and start reader."""
        self._loop = asyncio.get_event_loop()
        self._write_lock = asyncio.Lock()

        ssl_arg: ssl.SSLContext | bool | None = self._ssl_ctx
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port, ssl=ssl_arg
        )

        # Handshake.
        self._writer.write(MAGIC)
        await self._writer.drain()
        echo = await self._reader.readexactly(len(MAGIC))
        if echo != MAGIC:
            self._writer.close()
            raise FibpError(0, f"handshake failed: expected {MAGIC!r}, got {echo!r}")

        # Auth.
        if self._api_key is not None:
            self._writer.write(encode_auth(0, self._api_key))
            await self._writer.drain()

        self._reader_task = asyncio.ensure_future(self._read_loop())

    async def close(self) -> None:
        """Close the connection and wake all pending waiters."""
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except OSError:
                pass
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._reader_task
        self._wake_all(FibpError(0, "connection closed"))

    def _wake_all(self, exc: Exception) -> None:
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()
        for q in self._consume_queues.values():
            q.put_nowait(None)
        self._consume_queues.clear()

    def _alloc_corr_id(self) -> int:
        cid = self._next_corr_id
        self._next_corr_id = (self._next_corr_id + 1) & 0xFFFF_FFFF
        if self._next_corr_id == 0:
            self._next_corr_id = 1
        return cid

    async def send_request(self, frame: bytes, corr_id: int) -> bytes:
        """Send *frame* and await the response body."""
        assert self._loop is not None
        assert self._write_lock is not None
        assert self._writer is not None
        fut: asyncio.Future[bytes] = self._loop.create_future()
        self._pending[corr_id] = fut
        async with self._write_lock:
            self._writer.write(frame)
            await self._writer.drain()
        return await fut

    async def open_consume_stream(
        self, frame: bytes, corr_id: int
    ) -> asyncio.Queue[bytes | None]:
        """Send *frame* and return a queue that receives pushed bodies."""
        assert self._write_lock is not None
        assert self._writer is not None
        q: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._consume_queues[corr_id] = q
        async with self._write_lock:
            self._writer.write(frame)
            await self._writer.drain()
        return q

    def alloc_corr_id(self) -> int:
        return self._alloc_corr_id()

    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
            while True:
                length_buf = await self._reader.readexactly(4)
                (payload_len,) = struct.unpack(">I", length_buf)
                payload = await self._reader.readexactly(payload_len)
                if len(payload) < 6:
                    break
                flags, op, corr_id = struct.unpack_from(">BBI", payload, 0)
                body = payload[6:]
                self._dispatch(flags, op, corr_id, body)
        except (asyncio.IncompleteReadError, OSError, struct.error, asyncio.CancelledError):
            pass
        finally:
            self._wake_all(FibpError(0, "connection lost"))

    def _dispatch(self, flags: int, op: int, corr_id: int, body: bytes) -> None:
        is_push = bool(flags & FLAG_SERVER_PUSH)

        if is_push:
            q = self._consume_queues.get(corr_id)
            if q is not None:
                q.put_nowait(body)
            return

        if op == OP_GOAWAY:
            self._wake_all(FibpError(0, "server sent GOAWAY"))
            return

        # End of consume stream (server sends a non-push CONSUME frame to close).
        if op == OP_CONSUME and corr_id in self._consume_queues:
            q = self._consume_queues.pop(corr_id)
            q.put_nowait(None)
            return

        fut = self._pending.pop(corr_id, None)
        if fut is not None and not fut.done():
            if op == OP_ERROR:
                code, msg = decode_error_frame(body)
                fut.set_exception(FibpError(code, msg))
            else:
                fut.set_result(body)


# ------------------------------------------------------------------
# Shared helpers
# ------------------------------------------------------------------

def _recv_exactly(sock: socket.socket, n: int) -> bytes:
    """Read exactly *n* bytes from *sock*, raising on EOF."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise OSError("connection closed by peer")
        buf.extend(chunk)
    return bytes(buf)


def parse_addr(addr: str) -> tuple[str, int]:
    """Parse ``"host:port"`` into ``(host, port)``."""
    host, _, port_str = addr.rpartition(":")
    return host, int(port_str)


def make_ssl_context(
    *,
    ca_cert: bytes | None = None,
    client_cert: bytes | None = None,
    client_key: bytes | None = None,
) -> ssl.SSLContext:
    """Build an ``ssl.SSLContext`` from PEM bytes.

    When *ca_cert* is ``None``, the OS default trust store is used.
    """
    import os
    import tempfile

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    if ca_cert is not None:
        # Pass PEM bytes directly via cadata to avoid writing a temp file.
        ctx.load_verify_locations(cadata=ca_cert.decode())
    else:
        ctx.load_default_certs()

    if client_cert is not None and client_key is not None:
        with (
            tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as cf,
            tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as kf,
        ):
            cf.write(client_cert)
            kf.write(client_key)
            cert_path = cf.name
            key_path = kf.name
        try:
            ctx.load_cert_chain(cert_path, key_path)
        finally:
            os.unlink(cert_path)
            os.unlink(key_path)

    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = True
    return ctx
