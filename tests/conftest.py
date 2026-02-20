"""Pytest fixtures for Fila integration tests."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

import grpc
import pytest

if TYPE_CHECKING:
    from collections.abc import Generator

from fila.v1 import admin_pb2, admin_pb2_grpc

FILA_SERVER_BIN = os.environ.get(
    "FILA_SERVER_BIN",
    str(
        Path(__file__).resolve().parent.parent.parent
        / "fila" / "target" / "release" / "fila-server"
    ),
)

FILA_SERVER_AVAILABLE = os.path.isfile(FILA_SERVER_BIN) and os.access(FILA_SERVER_BIN, os.X_OK)


def _find_free_port() -> int:
    """Find an available TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestServer:
    """Manages a fila-server subprocess for integration tests."""

    def __init__(self, addr: str, process: subprocess.Popen[bytes], data_dir: str) -> None:
        self.addr = addr
        self._process = process
        self._data_dir = data_dir

    def stop(self) -> None:
        """Kill the server and clean up."""
        self._process.kill()
        self._process.wait()
        shutil.rmtree(self._data_dir, ignore_errors=True)

    def create_queue(self, name: str) -> None:
        """Create a queue on the test server via admin gRPC."""
        channel = grpc.insecure_channel(self.addr)
        stub = admin_pb2_grpc.FilaAdminStub(channel)
        stub.CreateQueue(
            admin_pb2.CreateQueueRequest(
                name=name,
                config=admin_pb2.QueueConfig(),
            )
        )
        channel.close()


@pytest.fixture()
def server() -> Generator[TestServer, None, None]:
    """Start a fila-server for the test, yield it, then shut down."""
    if not FILA_SERVER_AVAILABLE:
        pytest.skip(f"fila-server binary not found at {FILA_SERVER_BIN}")

    port = _find_free_port()
    addr = f"127.0.0.1:{port}"

    data_dir = tempfile.mkdtemp(prefix="fila-test-")

    # Write config file for the server.
    config_path = os.path.join(data_dir, "fila.toml")
    with open(config_path, "w") as f:
        f.write(f'[server]\nlisten_addr = "{addr}"\n')

    env = {**os.environ, "FILA_DATA_DIR": os.path.join(data_dir, "db")}
    process = subprocess.Popen(
        [FILA_SERVER_BIN],
        cwd=data_dir,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    ts = TestServer(addr, process, data_dir)

    # Wait for server to be ready.
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        try:
            channel = grpc.insecure_channel(addr)
            stub = admin_pb2_grpc.FilaAdminStub(channel)
            stub.ListQueues(admin_pb2.ListQueuesRequest())
            channel.close()
            break
        except grpc.RpcError:
            time.sleep(0.05)
    else:
        ts.stop()
        pytest.fail("fila-server did not become ready within 10s")

    yield ts

    ts.stop()
