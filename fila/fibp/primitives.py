"""Low-level encoding/decoding primitives for the FIBP wire format.

All multi-byte integers are big-endian.  Strings are length-prefixed with
a u16 byte count followed by UTF-8 bytes.  Byte slices use a u32 length
prefix.
"""

from __future__ import annotations

import struct

# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

class Writer:
    """Accumulates bytes for a FIBP frame body."""

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    # -- scalars -------------------------------------------------------------

    def write_u8(self, v: int) -> None:
        self._buf.append(v & 0xFF)

    def write_u16(self, v: int) -> None:
        self._buf.extend(struct.pack("!H", v))

    def write_u32(self, v: int) -> None:
        self._buf.extend(struct.pack("!I", v))

    def write_u64(self, v: int) -> None:
        self._buf.extend(struct.pack("!Q", v))

    def write_i64(self, v: int) -> None:
        self._buf.extend(struct.pack("!q", v))

    def write_f64(self, v: float) -> None:
        self._buf.extend(struct.pack("!d", v))

    def write_bool(self, v: bool) -> None:
        self._buf.append(1 if v else 0)

    # -- composites ----------------------------------------------------------

    def write_string(self, s: str) -> None:
        encoded = s.encode("utf-8")
        self.write_u16(len(encoded))
        self._buf.extend(encoded)

    def write_bytes(self, b: bytes) -> None:
        self.write_u32(len(b))
        self._buf.extend(b)

    def write_string_map(self, m: dict[str, str]) -> None:
        self.write_u16(len(m))
        for k, v in m.items():
            self.write_string(k)
            self.write_string(v)

    def write_string_list(self, items: list[str]) -> None:
        self.write_u16(len(items))
        for s in items:
            self.write_string(s)

    def write_optional_string(self, s: str | None) -> None:
        if s is None:
            self.write_u8(0)
        else:
            self.write_u8(1)
            self.write_string(s)

    # -- access --------------------------------------------------------------

    def finish(self) -> bytes:
        return bytes(self._buf)


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

class Reader:
    """Reads primitive values from a FIBP frame body with position tracking."""

    __slots__ = ("_data", "_pos")

    def __init__(self, data: bytes | bytearray | memoryview) -> None:
        self._data = bytes(data)
        self._pos = 0

    @property
    def remaining(self) -> int:
        return len(self._data) - self._pos

    # -- scalars -------------------------------------------------------------

    def read_u8(self) -> int:
        v = self._data[self._pos]
        self._pos += 1
        return v

    def read_u16(self) -> int:
        v: int = struct.unpack_from("!H", self._data, self._pos)[0]
        self._pos += 2
        return v

    def read_u32(self) -> int:
        v: int = struct.unpack_from("!I", self._data, self._pos)[0]
        self._pos += 4
        return v

    def read_u64(self) -> int:
        v: int = struct.unpack_from("!Q", self._data, self._pos)[0]
        self._pos += 8
        return v

    def read_i64(self) -> int:
        v: int = struct.unpack_from("!q", self._data, self._pos)[0]
        self._pos += 8
        return v

    def read_f64(self) -> float:
        v: float = struct.unpack_from("!d", self._data, self._pos)[0]
        self._pos += 8
        return v

    def read_bool(self) -> bool:
        return self.read_u8() != 0

    # -- composites ----------------------------------------------------------

    def read_string(self) -> str:
        length = self.read_u16()
        end = self._pos + length
        if end > len(self._data):
            raise ValueError(
                f"string length {length} exceeds remaining buffer "
                f"({len(self._data) - self._pos} bytes at offset {self._pos})"
            )
        s = self._data[self._pos:end].decode("utf-8")
        self._pos = end
        return s

    def read_bytes(self) -> bytes:
        length = self.read_u32()
        end = self._pos + length
        if end > len(self._data):
            raise ValueError(
                f"bytes length {length} exceeds remaining buffer "
                f"({len(self._data) - self._pos} bytes at offset {self._pos})"
            )
        b = self._data[self._pos:end]
        self._pos = end
        return b

    def read_string_map(self) -> dict[str, str]:
        count = self.read_u16()
        m: dict[str, str] = {}
        for _ in range(count):
            k = self.read_string()
            v = self.read_string()
            m[k] = v
        return m

    def read_string_list(self) -> list[str]:
        count = self.read_u16()
        return [self.read_string() for _ in range(count)]

    def read_optional_string(self) -> str | None:
        present = self.read_u8()
        if present:
            return self.read_string()
        return None
