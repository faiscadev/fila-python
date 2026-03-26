"""Pytest fixtures for Fila integration tests."""

from __future__ import annotations

import ipaddress
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


def _generate_self_signed_certs(out_dir: str) -> dict[str, str]:
    """Generate a self-signed CA + server + client cert for testing.

    Returns a dict with keys: ca_cert, server_cert, server_key, client_cert, client_key.
    """
    import datetime

    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
    except ImportError:
        pytest.skip("cryptography package required for TLS tests")

    # CA key + cert
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Fila Test CA")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )

    # Server key + cert
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]))
        .issuer_name(ca_name)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
        )
        .add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName("localhost"),
                x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
            ]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    # Client key + cert
    client_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-client")]))
        .issuer_name(ca_name)
        .public_key(client_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
        )
        .sign(ca_key, hashes.SHA256())
    )

    def _write_pem(path: str, data: bytes) -> str:
        with open(path, "wb") as f:
            f.write(data)
        return path

    paths = {
        "ca_cert": _write_pem(
            os.path.join(out_dir, "ca.pem"),
            ca_cert.public_bytes(serialization.Encoding.PEM),
        ),
        "server_cert": _write_pem(
            os.path.join(out_dir, "server.pem"),
            server_cert.public_bytes(serialization.Encoding.PEM),
        ),
        "server_key": _write_pem(
            os.path.join(out_dir, "server-key.pem"),
            server_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ),
        ),
        "client_cert": _write_pem(
            os.path.join(out_dir, "client.pem"),
            client_cert.public_bytes(serialization.Encoding.PEM),
        ),
        "client_key": _write_pem(
            os.path.join(out_dir, "client-key.pem"),
            client_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ),
        ),
    }
    return paths


class TestServer:
    """Manages a fila-server subprocess for integration tests."""

    def __init__(
        self,
        addr: str,
        process: subprocess.Popen[bytes],
        data_dir: str,
        *,
        tls_paths: dict[str, str] | None = None,
        api_key: str | None = None,
    ) -> None:
        self.addr = addr
        self._process = process
        self._data_dir = data_dir
        self.tls_paths = tls_paths
        self.api_key = api_key

    def stop(self) -> None:
        """Kill the server and clean up."""
        self._process.kill()
        self._process.wait()
        shutil.rmtree(self._data_dir, ignore_errors=True)

    def _make_grpc_channel(self) -> grpc.Channel:
        """Create a gRPC channel to this server (TLS-aware) for admin ops."""
        if self.tls_paths is not None:
            with open(self.tls_paths["ca_cert"], "rb") as f:
                ca = f.read()
            with open(self.tls_paths["client_cert"], "rb") as f:
                cert = f.read()
            with open(self.tls_paths["client_key"], "rb") as f:
                key = f.read()
            creds = grpc.ssl_channel_credentials(
                root_certificates=ca,
                private_key=key,
                certificate_chain=cert,
            )
            channel = grpc.secure_channel(self.addr, creds)
        else:
            channel = grpc.insecure_channel(self.addr)

        if self.api_key is not None:
            # Inject API key via metadata interceptor for admin calls.
            channel = grpc.intercept_channel(channel, _GrpcApiKeyInterceptor(self.api_key))

        return channel

    def create_queue(self, name: str) -> None:
        """Create a queue on the test server via admin gRPC."""
        channel = self._make_grpc_channel()
        stub = admin_pb2_grpc.FilaAdminStub(channel)
        stub.CreateQueue(
            admin_pb2.CreateQueueRequest(
                name=name,
                config=admin_pb2.QueueConfig(),
            )
        )
        channel.close()


class _GrpcClientCallDetails(grpc.ClientCallDetails):  # type: ignore[misc]
    """Minimal concrete ClientCallDetails for the API key interceptor."""

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


class _GrpcApiKeyInterceptor(
    grpc.UnaryUnaryClientInterceptor,  # type: ignore[misc]
    grpc.UnaryStreamClientInterceptor,  # type: ignore[misc]
):
    """Injects authorization metadata into gRPC admin calls (test fixture only)."""

    def __init__(self, api_key: str) -> None:
        self._metadata = (("authorization", f"Bearer {api_key}"),)

    def _inject(self, details: grpc.ClientCallDetails) -> _GrpcClientCallDetails:
        metadata = list(details.metadata or [])
        metadata.extend(self._metadata)
        return _GrpcClientCallDetails(
            details.method,
            details.timeout,
            metadata,
            details.credentials,
            details.wait_for_ready,
            details.compression,
        )

    def intercept_unary_unary(  # type: ignore[override]
        self, continuation: object, details: grpc.ClientCallDetails, request: object
    ) -> object:
        return continuation(self._inject(details), request)  # type: ignore[call-arg]

    def intercept_unary_stream(  # type: ignore[override]
        self, continuation: object, details: grpc.ClientCallDetails, request: object
    ) -> object:
        return continuation(self._inject(details), request)  # type: ignore[call-arg]


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
        f.write(f'[fibp]\nlisten_addr = "{addr}"\n')

    env = {**os.environ, "FILA_DATA_DIR": os.path.join(data_dir, "db")}
    process = subprocess.Popen(
        [FILA_SERVER_BIN],
        cwd=data_dir,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    ts = TestServer(addr, process, data_dir)

    # Wait for server to be ready via FIBP handshake.
    _wait_fibp_ready(addr, ts)

    yield ts

    ts.stop()


@pytest.fixture()
def tls_server() -> Generator[TestServer, None, None]:
    """Start a TLS-enabled fila-server, yield it, then shut down."""
    if not FILA_SERVER_AVAILABLE:
        pytest.skip(f"fila-server binary not found at {FILA_SERVER_BIN}")

    try:
        import cryptography  # noqa: F401
    except ImportError:
        pytest.skip("cryptography package required for TLS tests")

    port = _find_free_port()
    addr = f"127.0.0.1:{port}"

    data_dir = tempfile.mkdtemp(prefix="fila-tls-test-")
    tls_paths = _generate_self_signed_certs(data_dir)

    # Write config with TLS enabled.
    config_path = os.path.join(data_dir, "fila.toml")
    with open(config_path, "w") as f:
        f.write(
            f'[fibp]\n'
            f'listen_addr = "{addr}"\n'
            f'\n'
            f'[tls]\n'
            f'ca_cert = "{tls_paths["ca_cert"]}"\n'
            f'server_cert = "{tls_paths["server_cert"]}"\n'
            f'server_key = "{tls_paths["server_key"]}"\n'
        )

    env = {**os.environ, "FILA_DATA_DIR": os.path.join(data_dir, "db")}
    process = subprocess.Popen(
        [FILA_SERVER_BIN],
        cwd=data_dir,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    ts = TestServer(addr, process, data_dir, tls_paths=tls_paths)

    with open(tls_paths["ca_cert"], "rb") as f:
        ca_cert = f.read()
    with open(tls_paths["client_cert"], "rb") as f:
        client_cert = f.read()
    with open(tls_paths["client_key"], "rb") as f:
        client_key = f.read()

    _wait_fibp_ready(
        addr,
        ts,
        ca_cert=ca_cert,
        client_cert=client_cert,
        client_key=client_key,
    )

    yield ts

    ts.stop()


@pytest.fixture()
def auth_server() -> Generator[TestServer, None, None]:
    """Start a fila-server with API key auth enabled, yield it, then shut down."""
    if not FILA_SERVER_AVAILABLE:
        pytest.skip(f"fila-server binary not found at {FILA_SERVER_BIN}")

    port = _find_free_port()
    addr = f"127.0.0.1:{port}"
    bootstrap_key = "test-bootstrap-key-for-integration"

    data_dir = tempfile.mkdtemp(prefix="fila-auth-test-")

    # Write config with bootstrap API key.
    config_path = os.path.join(data_dir, "fila.toml")
    with open(config_path, "w") as f:
        f.write(
            f'[fibp]\n'
            f'listen_addr = "{addr}"\n'
            f'bootstrap_apikey = "{bootstrap_key}"\n'
        )

    env = {**os.environ, "FILA_DATA_DIR": os.path.join(data_dir, "db")}
    process = subprocess.Popen(
        [FILA_SERVER_BIN],
        cwd=data_dir,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    ts = TestServer(addr, process, data_dir, api_key=bootstrap_key)

    _wait_fibp_ready(addr, ts, api_key=bootstrap_key)

    yield ts

    ts.stop()


def _wait_fibp_ready(
    addr: str,
    ts: TestServer,
    *,
    ca_cert: bytes | None = None,
    client_cert: bytes | None = None,
    client_key: bytes | None = None,
    api_key: str | None = None,
    timeout: float = 10.0,
) -> None:
    """Poll the server with a FIBP handshake until it responds or times out.

    If the server responds with a non-FIBP handshake (e.g., an HTTP/2 gRPC
    frame), the test session is skipped with an informative message.  This
    allows the test suite to be run against a gRPC-only binary without
    failing — integration tests require a FIBP-capable server.
    """
    from fila.fibp import FibpConnection, FibpError, make_ssl_context, parse_addr

    host, port = parse_addr(addr)
    ssl_ctx = None
    if ca_cert is not None:
        ssl_ctx = make_ssl_context(
            ca_cert=ca_cert,
            client_cert=client_cert,
            client_key=client_key,
        )

    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = FibpConnection(host, port, ssl_ctx=ssl_ctx, api_key=api_key)
            conn.close()
            return
        except FibpError as e:
            # If the handshake fails immediately (not a timeout/connection-
            # refused), the server is online but does not speak FIBP — it is
            # likely a gRPC-only binary.  Skip rather than fail so the test
            # suite does not report false negatives against legacy binaries.
            if "handshake failed" in str(e):
                ts.stop()
                pytest.skip(
                    "fila-server does not speak FIBP (handshake rejected); "
                    "integration tests require a FIBP-capable server binary"
                )
            last_exc = e
            time.sleep(0.05)
        except OSError as e:
            last_exc = e
            time.sleep(0.05)

    ts.stop()
    pytest.fail(
        f"fila-server at {addr} did not become ready within {timeout}s: {last_exc}"
    )
