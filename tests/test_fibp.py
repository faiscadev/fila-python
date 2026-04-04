"""Unit tests for the FIBP codec and primitives."""

from __future__ import annotations

from fila.fibp.codec import (
    decode_ack_result,
    decode_delivery,
    decode_enqueue_result,
    decode_error,
    decode_handshake_ok,
    encode_ack,
    encode_consume,
    encode_enqueue,
    encode_handshake,
    encode_nack,
)
from fila.fibp.opcodes import ErrorCode, FrameHeader, Opcode
from fila.fibp.primitives import Reader, Writer


class TestPrimitives:
    """Test Writer/Reader round-trip for all primitive types."""

    def test_u8(self) -> None:
        w = Writer()
        w.write_u8(42)
        r = Reader(w.finish())
        assert r.read_u8() == 42

    def test_u16(self) -> None:
        w = Writer()
        w.write_u16(1234)
        r = Reader(w.finish())
        assert r.read_u16() == 1234

    def test_u32(self) -> None:
        w = Writer()
        w.write_u32(0xDEADBEEF)
        r = Reader(w.finish())
        assert r.read_u32() == 0xDEADBEEF

    def test_u64(self) -> None:
        w = Writer()
        w.write_u64(0xDEADBEEFCAFE0001)
        r = Reader(w.finish())
        assert r.read_u64() == 0xDEADBEEFCAFE0001

    def test_i64(self) -> None:
        w = Writer()
        w.write_i64(-42)
        r = Reader(w.finish())
        assert r.read_i64() == -42

    def test_f64(self) -> None:
        w = Writer()
        w.write_f64(3.14)
        r = Reader(w.finish())
        assert abs(r.read_f64() - 3.14) < 1e-10

    def test_bool(self) -> None:
        w = Writer()
        w.write_bool(True)
        w.write_bool(False)
        r = Reader(w.finish())
        assert r.read_bool() is True
        assert r.read_bool() is False

    def test_string(self) -> None:
        w = Writer()
        w.write_string("hello")
        r = Reader(w.finish())
        assert r.read_string() == "hello"

    def test_string_empty(self) -> None:
        w = Writer()
        w.write_string("")
        r = Reader(w.finish())
        assert r.read_string() == ""

    def test_bytes(self) -> None:
        w = Writer()
        w.write_bytes(b"\x00\x01\x02")
        r = Reader(w.finish())
        assert r.read_bytes() == b"\x00\x01\x02"

    def test_string_map(self) -> None:
        w = Writer()
        w.write_string_map({"a": "1", "b": "2"})
        r = Reader(w.finish())
        m = r.read_string_map()
        assert m == {"a": "1", "b": "2"}

    def test_string_list(self) -> None:
        w = Writer()
        w.write_string_list(["x", "y", "z"])
        r = Reader(w.finish())
        assert r.read_string_list() == ["x", "y", "z"]

    def test_optional_string_present(self) -> None:
        w = Writer()
        w.write_optional_string("present")
        r = Reader(w.finish())
        assert r.read_optional_string() == "present"

    def test_optional_string_absent(self) -> None:
        w = Writer()
        w.write_optional_string(None)
        r = Reader(w.finish())
        assert r.read_optional_string() is None


class TestCodec:
    """Test encode/decode round-trips for key opcodes."""

    def test_handshake_encode(self) -> None:
        """Handshake encodes version + optional API key."""
        data = encode_handshake(1, "my-key")
        r = Reader(data)
        assert r.read_u16() == 1
        assert r.read_optional_string() == "my-key"

    def test_handshake_no_key(self) -> None:
        data = encode_handshake(1, None)
        r = Reader(data)
        assert r.read_u16() == 1
        assert r.read_optional_string() is None

    def test_handshake_ok_decode(self) -> None:
        w = Writer()
        w.write_u16(1)  # version
        w.write_u64(42)  # node_id
        w.write_u32(16 * 1024 * 1024)  # max_frame_size
        version, node_id, mfs = decode_handshake_ok(w.finish())
        assert version == 1
        assert node_id == 42
        assert mfs == 16 * 1024 * 1024

    def test_enqueue_encode_decode(self) -> None:
        msgs = [
            {"queue": "q1", "headers": {"k": "v"}, "payload": b"hello"},
            {"queue": "q2", "headers": {}, "payload": b"world"},
        ]
        data = encode_enqueue(msgs)
        r = Reader(data)
        count = r.read_u32()
        assert count == 2
        # First message
        assert r.read_string() == "q1"
        assert r.read_string_map() == {"k": "v"}
        assert r.read_bytes() == b"hello"
        # Second message
        assert r.read_string() == "q2"
        assert r.read_string_map() == {}
        assert r.read_bytes() == b"world"

    def test_enqueue_result_decode(self) -> None:
        w = Writer()
        w.write_u32(2)  # count
        w.write_u8(ErrorCode.OK)
        w.write_string("msg-001")
        w.write_u8(ErrorCode.QUEUE_NOT_FOUND)
        w.write_string("")
        items = decode_enqueue_result(w.finish())
        assert len(items) == 2
        assert items[0].error_code == ErrorCode.OK
        assert items[0].message_id == "msg-001"
        assert items[1].error_code == ErrorCode.QUEUE_NOT_FOUND

    def test_consume_encode(self) -> None:
        data = encode_consume("my-queue")
        r = Reader(data)
        assert r.read_string() == "my-queue"

    def test_delivery_decode(self) -> None:
        w = Writer()
        w.write_u32(1)  # count
        w.write_string("msg-123")  # msg_id
        w.write_string("test-q")  # queue
        w.write_string_map({"h": "v"})  # headers
        w.write_bytes(b"payload")  # payload
        w.write_string("fk")  # fairness_key
        w.write_u32(10)  # weight
        w.write_string_list(["tk1"])  # throttle_keys
        w.write_u32(2)  # attempt_count
        w.write_u64(1000)  # enqueued_at
        w.write_u64(2000)  # leased_at

        msgs = decode_delivery(w.finish())
        assert len(msgs) == 1
        m = msgs[0]
        assert m.message_id == "msg-123"
        assert m.queue == "test-q"
        assert m.headers == {"h": "v"}
        assert m.payload == b"payload"
        assert m.fairness_key == "fk"
        assert m.weight == 10
        assert m.throttle_keys == ["tk1"]
        assert m.attempt_count == 2
        assert m.enqueued_at == 1000
        assert m.leased_at == 2000

    def test_ack_encode(self) -> None:
        data = encode_ack([{"queue": "q", "message_id": "id-1"}])
        r = Reader(data)
        assert r.read_u32() == 1
        assert r.read_string() == "q"
        assert r.read_string() == "id-1"

    def test_ack_result_decode(self) -> None:
        w = Writer()
        w.write_u32(2)
        w.write_u8(ErrorCode.OK)
        w.write_u8(ErrorCode.MESSAGE_NOT_FOUND)
        codes = decode_ack_result(w.finish())
        assert codes == [ErrorCode.OK, ErrorCode.MESSAGE_NOT_FOUND]

    def test_nack_encode(self) -> None:
        data = encode_nack([{"queue": "q", "message_id": "id-1", "error": "bad"}])
        r = Reader(data)
        assert r.read_u32() == 1
        assert r.read_string() == "q"
        assert r.read_string() == "id-1"
        assert r.read_string() == "bad"

    def test_error_decode(self) -> None:
        w = Writer()
        w.write_u8(ErrorCode.NOT_LEADER)
        w.write_string("not leader")
        w.write_string_map({"leader_addr": "10.0.0.1:5555"})
        err = decode_error(w.finish())
        assert err.code == ErrorCode.NOT_LEADER
        assert err.message == "not leader"
        assert err.metadata["leader_addr"] == "10.0.0.1:5555"


class TestFrameHeader:
    """Test FrameHeader dataclass."""

    def test_continuation_flag(self) -> None:
        h = FrameHeader(opcode=Opcode.DELIVERY, flags=0x01, request_id=1)
        assert h.is_continuation is True

    def test_no_continuation(self) -> None:
        h = FrameHeader(opcode=Opcode.DELIVERY, flags=0x00, request_id=1)
        assert h.is_continuation is False


class TestErrorMapping:
    """Test error code -> exception mapping."""

    def test_queue_not_found(self) -> None:
        from fila.errors import QueueNotFoundError, _map_error_code
        err = _map_error_code(ErrorCode.QUEUE_NOT_FOUND, "missing")
        assert isinstance(err, QueueNotFoundError)

    def test_message_not_found(self) -> None:
        from fila.errors import MessageNotFoundError, _map_error_code
        err = _map_error_code(ErrorCode.MESSAGE_NOT_FOUND, "gone")
        assert isinstance(err, MessageNotFoundError)

    def test_unauthenticated(self) -> None:
        from fila.errors import UnauthorizedError, _map_error_code
        err = _map_error_code(ErrorCode.UNAUTHENTICATED, "no key")
        assert isinstance(err, UnauthorizedError)

    def test_not_leader(self) -> None:
        from fila.errors import NotLeaderError, _map_error_code
        err = _map_error_code(ErrorCode.NOT_LEADER, "redirect")
        assert isinstance(err, NotLeaderError)

    def test_internal_error(self) -> None:
        from fila.errors import ProtocolError, _map_error_code
        err = _map_error_code(ErrorCode.INTERNAL_ERROR, "boom")
        assert isinstance(err, ProtocolError)

    def test_raise_from_error_frame_not_leader(self) -> None:
        import pytest

        from fila.errors import NotLeaderError, _raise_from_error_frame
        from fila.fibp.codec import ErrorFrame

        err = ErrorFrame(
            code=ErrorCode.NOT_LEADER,
            message="not leader",
            metadata={"leader_addr": "10.0.0.1:5555"},
        )
        with pytest.raises(NotLeaderError) as exc_info:
            _raise_from_error_frame(err)
        assert exc_info.value.leader_addr == "10.0.0.1:5555"
