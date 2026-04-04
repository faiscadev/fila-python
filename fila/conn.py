"""FIBP connection manager — synchronous and asynchronous."""

from __future__ import annotations

import asyncio
import socket
import struct
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import ssl

from fila.fibp.codec import (
    decode_error,
    decode_handshake_ok,
    encode_handshake,
    encode_pong,
)
from fila.fibp.opcodes import (
    DEFAULT_MAX_FRAME_SIZE,
    FRAME_HEADER_SIZE,
    PROTOCOL_VERSION,
    FrameHeader,
    Opcode,
)


def _parse_header(data: bytes) -> FrameHeader:
    """Parse 6 bytes into a FrameHeader."""
    opcode = data[0]
    flags = data[1]
    request_id = struct.unpack_from("!I", data, 2)[0]
    return FrameHeader(opcode=opcode, flags=flags, request_id=request_id)


def _build_frame(opcode: int, request_id: int, body: bytes, flags: int = 0) -> bytes:
    """Build a length-prefixed FIBP frame."""
    frame_body = struct.pack("!BBI", opcode, flags, request_id) + body
    return struct.pack("!I", len(frame_body)) + frame_body


# ---------------------------------------------------------------------------
# Synchronous connection
# ---------------------------------------------------------------------------

class Connection:
    """Synchronous FIBP connection over a TCP socket."""

    def __init__(self, sock: socket.socket, max_frame_size: int = DEFAULT_MAX_FRAME_SIZE) -> None:
        self._sock = sock
        self._max_frame_size = max_frame_size
        self._req_counter = 0
        self._lock = threading.Lock()
        self._pushback: tuple[FrameHeader, bytes] | None = None

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        *,
        ssl_context: ssl.SSLContext | None = None,
        api_key: str | None = None,
        timeout: float = 10.0,
    ) -> Connection:
        """Open a TCP connection and perform the FIBP handshake."""
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        if ssl_context is not None:
            sock = ssl_context.wrap_socket(sock, server_hostname=host)

        conn = cls(sock)
        conn._handshake(api_key)
        return conn

    def _next_request_id(self) -> int:
        self._req_counter += 1
        return self._req_counter & 0xFFFFFFFF

    def _handshake(self, api_key: str | None) -> None:
        """Perform the FIBP handshake."""
        body = encode_handshake(PROTOCOL_VERSION, api_key)
        req_id = self._next_request_id()
        self.write_frame(Opcode.HANDSHAKE, req_id, body)

        header, resp_body = self.read_frame()
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            from fila.errors import _raise_from_error_frame
            _raise_from_error_frame(err)

        if header.opcode != Opcode.HANDSHAKE_OK:
            raise ConnectionError(
                f"expected HandshakeOk (0x02), got 0x{header.opcode:02x}"
            )

        version, _node_id, max_frame_size = decode_handshake_ok(resp_body)
        if max_frame_size > 0:
            self._max_frame_size = max_frame_size

    def write_frame(self, opcode: int, request_id: int, body: bytes, flags: int = 0) -> None:
        """Write a single length-prefixed FIBP frame."""
        frame = _build_frame(opcode, request_id, body, flags)
        self._sock.sendall(frame)

    def read_frame(self) -> tuple[FrameHeader, bytes]:
        """Read a single length-prefixed FIBP frame.

        Handles Ping by responding with Pong automatically.
        Handles continuation frames by concatenating bodies.
        """
        if self._pushback is not None:
            frame = self._pushback
            self._pushback = None
            return frame

        while True:
            header, body = self._read_single_frame()

            # Auto-reply to Ping.
            if header.opcode == Opcode.PING:
                self.write_frame(Opcode.PONG, header.request_id, encode_pong())
                continue

            # Handle continuation frames.
            if header.is_continuation:
                parts = [body]
                while True:
                    cont_header, cont_body = self._read_single_frame()
                    if cont_header.opcode == Opcode.PING:
                        self.write_frame(Opcode.PONG, cont_header.request_id, encode_pong())
                        continue
                    parts.append(cont_body)
                    if not cont_header.is_continuation:
                        break
                return header, b"".join(parts)

            return header, body

    def _read_single_frame(self) -> tuple[FrameHeader, bytes]:
        """Read one frame from the wire (no continuation handling)."""
        length_bytes = self._recv_exact(4)
        frame_len = struct.unpack("!I", length_bytes)[0]
        if frame_len > self._max_frame_size:
            raise ConnectionError(
                f"frame size {frame_len} exceeds max {self._max_frame_size}"
            )
        frame_data = self._recv_exact(frame_len)
        header = _parse_header(frame_data[:FRAME_HEADER_SIZE])
        body = frame_data[FRAME_HEADER_SIZE:]
        return header, body

    def _recv_exact(self, n: int) -> bytes:
        """Read exactly n bytes from the socket."""
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("connection closed by remote")
            buf.extend(chunk)
        return bytes(buf)

    def request(self, opcode: int, body: bytes) -> tuple[FrameHeader, bytes]:
        """Send a request frame and read the response (synchronous request-response)."""
        req_id = self._next_request_id()
        with self._lock:
            self.write_frame(opcode, req_id, body)
            return self.read_frame()

    def subscribe(self, queue: str) -> tuple[int, str]:
        """Send a Consume request and wait for ConsumeOk.

        Returns (request_id, consumer_id).
        """
        from fila.fibp.codec import decode_consume_ok, encode_consume

        req_id = self._next_request_id()
        self.write_frame(Opcode.CONSUME, req_id, encode_consume(queue))

        header, body = self.read_frame()
        if header.opcode == Opcode.ERROR:
            err = decode_error(body)
            from fila.errors import _raise_from_error_frame
            _raise_from_error_frame(err)

        if header.opcode == Opcode.CONSUME_OK:
            consumer_id = decode_consume_ok(body)
            return req_id, consumer_id

        if header.opcode == Opcode.DELIVERY:
            # Server may send Delivery directly (older binaries without ConsumeOk).
            # Push the frame back so the consume iterator can read it.
            self._pushback = (header, body)
            return req_id, ""

        raise ConnectionError(
            f"expected ConsumeOk (0x{Opcode.CONSUME_OK:02x}) or "
            f"Delivery (0x{Opcode.DELIVERY:02x}), "
            f"got 0x{header.opcode:02x}"
        )

    def cancel_consume(self, consumer_id: str) -> None:
        """Send a CancelConsume frame."""
        from fila.fibp.codec import encode_cancel_consume

        if not consumer_id:
            return
        req_id = self._next_request_id()
        self.write_frame(Opcode.CANCEL_CONSUME, req_id, encode_cancel_consume(consumer_id))

    def close(self) -> None:
        """Send Disconnect and close the socket."""
        import contextlib

        with contextlib.suppress(OSError):
            from fila.fibp.codec import encode_disconnect
            req_id = self._next_request_id()
            self.write_frame(Opcode.DISCONNECT, req_id, encode_disconnect())

        with contextlib.suppress(OSError):
            self._sock.close()


# ---------------------------------------------------------------------------
# Async connection
# ---------------------------------------------------------------------------

class AsyncConnection:
    """Asynchronous FIBP connection over asyncio streams."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._max_frame_size = max_frame_size
        self._req_counter = 0
        self._lock = asyncio.Lock()
        self._pushback: tuple[FrameHeader, bytes] | None = None

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        ssl_context: ssl.SSLContext | None = None,
        api_key: str | None = None,
        timeout: float = 10.0,
    ) -> AsyncConnection:
        """Open a TCP connection and perform the FIBP handshake."""
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=ssl_context),
            timeout=timeout,
        )

        # Set TCP_NODELAY on the underlying socket.
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        conn = cls(reader, writer)
        await conn._handshake(api_key)
        return conn

    def _next_request_id(self) -> int:
        self._req_counter += 1
        return self._req_counter & 0xFFFFFFFF

    async def _handshake(self, api_key: str | None) -> None:
        """Perform the FIBP handshake."""
        body = encode_handshake(PROTOCOL_VERSION, api_key)
        req_id = self._next_request_id()
        await self.write_frame(Opcode.HANDSHAKE, req_id, body)

        header, resp_body = await self.read_frame()
        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            from fila.errors import _raise_from_error_frame
            _raise_from_error_frame(err)

        if header.opcode != Opcode.HANDSHAKE_OK:
            raise ConnectionError(
                f"expected HandshakeOk (0x02), got 0x{header.opcode:02x}"
            )

        version, _node_id, max_frame_size = decode_handshake_ok(resp_body)
        if max_frame_size > 0:
            self._max_frame_size = max_frame_size

    async def write_frame(
        self, opcode: int, request_id: int, body: bytes, flags: int = 0
    ) -> None:
        """Write a single length-prefixed FIBP frame."""
        frame = _build_frame(opcode, request_id, body, flags)
        self._writer.write(frame)
        await self._writer.drain()

    async def read_frame(self) -> tuple[FrameHeader, bytes]:
        """Read a single length-prefixed FIBP frame.

        Handles Ping by responding with Pong automatically.
        Handles continuation frames by concatenating bodies.
        """
        if self._pushback is not None:
            frame = self._pushback
            self._pushback = None
            return frame

        while True:
            header, body = await self._read_single_frame()

            if header.opcode == Opcode.PING:
                await self.write_frame(Opcode.PONG, header.request_id, encode_pong())
                continue

            if header.is_continuation:
                parts = [body]
                while True:
                    cont_header, cont_body = await self._read_single_frame()
                    if cont_header.opcode == Opcode.PING:
                        await self.write_frame(
                            Opcode.PONG, cont_header.request_id, encode_pong()
                        )
                        continue
                    parts.append(cont_body)
                    if not cont_header.is_continuation:
                        break
                return header, b"".join(parts)

            return header, body

    async def _read_single_frame(self) -> tuple[FrameHeader, bytes]:
        """Read one frame from the wire (no continuation handling)."""
        length_bytes = await self._reader.readexactly(4)
        frame_len = struct.unpack("!I", length_bytes)[0]
        if frame_len > self._max_frame_size:
            raise ConnectionError(
                f"frame size {frame_len} exceeds max {self._max_frame_size}"
            )
        frame_data = await self._reader.readexactly(frame_len)
        header = _parse_header(frame_data[:FRAME_HEADER_SIZE])
        body = frame_data[FRAME_HEADER_SIZE:]
        return header, body

    async def request(self, opcode: int, body: bytes) -> tuple[FrameHeader, bytes]:
        """Send a request frame and read the response."""
        req_id = self._next_request_id()
        async with self._lock:
            await self.write_frame(opcode, req_id, body)
            return await self.read_frame()

    async def subscribe(self, queue: str) -> tuple[int, str]:
        """Send a Consume request and wait for ConsumeOk.

        Returns (request_id, consumer_id).
        """
        from fila.fibp.codec import decode_consume_ok, encode_consume

        req_id = self._next_request_id()
        await self.write_frame(Opcode.CONSUME, req_id, encode_consume(queue))

        header, body = await self.read_frame()
        if header.opcode == Opcode.ERROR:
            err = decode_error(body)
            from fila.errors import _raise_from_error_frame
            _raise_from_error_frame(err)

        if header.opcode == Opcode.CONSUME_OK:
            consumer_id = decode_consume_ok(body)
            return req_id, consumer_id

        if header.opcode == Opcode.DELIVERY:
            # Server may send Delivery directly (older binaries without ConsumeOk).
            self._pushback = (header, body)
            return req_id, ""

        raise ConnectionError(
            f"expected ConsumeOk (0x{Opcode.CONSUME_OK:02x}) or "
            f"Delivery (0x{Opcode.DELIVERY:02x}), "
            f"got 0x{header.opcode:02x}"
        )

    async def cancel_consume(self, consumer_id: str) -> None:
        """Send a CancelConsume frame."""
        from fila.fibp.codec import encode_cancel_consume

        if not consumer_id:
            return

        req_id = self._next_request_id()
        await self.write_frame(
            Opcode.CANCEL_CONSUME, req_id, encode_cancel_consume(consumer_id)
        )

    async def close(self) -> None:
        """Send Disconnect and close the stream."""
        try:
            from fila.fibp.codec import encode_disconnect
            req_id = self._next_request_id()
            await self.write_frame(Opcode.DISCONNECT, req_id, encode_disconnect())
        except OSError:
            pass
        finally:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except OSError:
                pass
