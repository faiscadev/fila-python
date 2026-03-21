"""Integration tests for the Fila Python SDK."""

from __future__ import annotations

import pytest

import fila


class TestSyncClient:
    """Integration tests for the synchronous Client."""

    def test_enqueue_consume_ack(self, server: object) -> None:
        """Full lifecycle: enqueue -> consume -> verify fields -> ack."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-sync-eca")

        with fila.Client(server.addr) as client:
            # Enqueue a message.
            headers = {"tenant": "acme"}
            payload = b"hello world"
            msg_id = client.enqueue("test-sync-eca", headers, payload)
            assert msg_id != ""

            # Consume the message.
            stream = client.consume("test-sync-eca")
            msg = next(stream)

            assert msg.id == msg_id
            assert msg.headers["tenant"] == "acme"
            assert msg.payload == b"hello world"

            # Ack the message.
            client.ack("test-sync-eca", msg.id)

    def test_enqueue_consume_nack_redeliver(self, server: object) -> None:
        """Nack triggers redelivery on the same stream with incremented attempt count."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-sync-nack")

        with fila.Client(server.addr) as client:
            msg_id = client.enqueue("test-sync-nack", None, b"retry-me")

            # Open consume stream.
            stream = client.consume("test-sync-nack")

            # First delivery.
            msg = next(stream)
            assert msg.id == msg_id
            assert msg.attempt_count == 0

            # Nack the message.
            client.nack("test-sync-nack", msg.id, "transient failure")

            # Redelivery on the same stream.
            msg2 = next(stream)
            assert msg2.id == msg_id
            assert msg2.attempt_count == 1

            # Ack to clean up.
            client.ack("test-sync-nack", msg2.id)

    def test_enqueue_nonexistent_queue(self, server: object) -> None:
        """Enqueue to a missing queue raises QueueNotFoundError."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)

        with fila.Client(server.addr) as client, pytest.raises(fila.QueueNotFoundError):
            client.enqueue("does-not-exist", None, b"test")


class TestAsyncClient:
    """Integration tests for the asynchronous AsyncClient."""

    @pytest.mark.asyncio
    async def test_async_enqueue_consume_ack(self, server: object) -> None:
        """Full async lifecycle: enqueue -> consume -> ack."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-async-eca")

        async with fila.AsyncClient(server.addr) as client:
            # Enqueue a message.
            msg_id = await client.enqueue(
                "test-async-eca", {"tenant": "acme"}, b"hello async"
            )
            assert msg_id != ""

            # Consume the message.
            stream = await client.consume("test-async-eca")
            msg = await stream.__anext__()

            assert msg.id == msg_id
            assert msg.headers["tenant"] == "acme"
            assert msg.payload == b"hello async"

            # Ack the message.
            await client.ack("test-async-eca", msg.id)


class TestTlsClient:
    """Integration tests for TLS connections."""

    def test_tls_enqueue_consume_ack(self, tls_server: object) -> None:
        """Full lifecycle over TLS: enqueue -> consume -> ack."""
        from tests.conftest import TestServer

        assert isinstance(tls_server, TestServer)
        assert tls_server.tls_paths is not None

        tls_server.create_queue("test-tls")

        with open(tls_server.tls_paths["ca_cert"], "rb") as f:
            ca_cert = f.read()
        with open(tls_server.tls_paths["client_cert"], "rb") as f:
            client_cert = f.read()
        with open(tls_server.tls_paths["client_key"], "rb") as f:
            client_key = f.read()

        with fila.Client(
            tls_server.addr,
            ca_cert=ca_cert,
            client_cert=client_cert,
            client_key=client_key,
        ) as client:
            msg_id = client.enqueue("test-tls", {"secure": "true"}, b"tls payload")
            assert msg_id != ""

            stream = client.consume("test-tls")
            msg = next(stream)

            assert msg.id == msg_id
            assert msg.payload == b"tls payload"

            client.ack("test-tls", msg.id)

    @pytest.mark.asyncio
    async def test_async_tls_enqueue_consume_ack(self, tls_server: object) -> None:
        """Full async lifecycle over TLS."""
        from tests.conftest import TestServer

        assert isinstance(tls_server, TestServer)
        assert tls_server.tls_paths is not None

        tls_server.create_queue("test-async-tls")

        with open(tls_server.tls_paths["ca_cert"], "rb") as f:
            ca_cert = f.read()
        with open(tls_server.tls_paths["client_cert"], "rb") as f:
            client_cert = f.read()
        with open(tls_server.tls_paths["client_key"], "rb") as f:
            client_key = f.read()

        async with fila.AsyncClient(
            tls_server.addr,
            ca_cert=ca_cert,
            client_cert=client_cert,
            client_key=client_key,
        ) as client:
            msg_id = await client.enqueue("test-async-tls", None, b"async tls")
            assert msg_id != ""

            stream = await client.consume("test-async-tls")
            msg = await stream.__anext__()

            assert msg.id == msg_id
            assert msg.payload == b"async tls"

            await client.ack("test-async-tls", msg.id)


class TestApiKeyAuth:
    """Integration tests for API key authentication."""

    def test_api_key_enqueue_consume_ack(self, auth_server: object) -> None:
        """Full lifecycle with API key auth: enqueue -> consume -> ack."""
        from tests.conftest import TestServer

        assert isinstance(auth_server, TestServer)
        assert auth_server.api_key is not None

        auth_server.create_queue("test-auth")

        with fila.Client(auth_server.addr, api_key=auth_server.api_key) as client:
            msg_id = client.enqueue("test-auth", None, b"authenticated")
            assert msg_id != ""

            stream = client.consume("test-auth")
            msg = next(stream)

            assert msg.id == msg_id
            assert msg.payload == b"authenticated"

            client.ack("test-auth", msg.id)

    def test_missing_api_key_rejected(self, auth_server: object) -> None:
        """Requests without API key are rejected when auth is enabled."""
        from tests.conftest import TestServer

        assert isinstance(auth_server, TestServer)

        # Connect without API key — should fail with UNAUTHENTICATED.
        with fila.Client(auth_server.addr) as client:
            with pytest.raises(fila.RPCError) as exc_info:
                client.enqueue("test-auth", None, b"no-key")
            import grpc
            assert exc_info.value.code == grpc.StatusCode.UNAUTHENTICATED

    @pytest.mark.asyncio
    async def test_async_api_key_enqueue(self, auth_server: object) -> None:
        """Async client with API key can enqueue successfully."""
        from tests.conftest import TestServer

        assert isinstance(auth_server, TestServer)
        assert auth_server.api_key is not None

        auth_server.create_queue("test-async-auth")

        async with fila.AsyncClient(
            auth_server.addr, api_key=auth_server.api_key
        ) as client:
            msg_id = await client.enqueue("test-async-auth", None, b"async auth")
            assert msg_id != ""
