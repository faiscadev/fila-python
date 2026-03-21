# fila-python

Python client SDK for the [Fila](https://github.com/faiscadev/fila) message broker.

## Installation

```bash
pip install fila-python
```

## Usage (Sync)

```python
from fila import Client

client = Client("localhost:5555")

# Enqueue a message.
msg_id = client.enqueue("my-queue", {"tenant": "acme"}, b"hello world")
print(f"Enqueued: {msg_id}")

# Consume messages.
for msg in client.consume("my-queue"):
    print(f"Received: {msg.id} (attempt {msg.attempt_count})")

    # Acknowledge successful processing.
    client.ack("my-queue", msg.id)

client.close()
```

## Usage (Async)

```python
import asyncio
from fila import AsyncClient

async def main():
    async with AsyncClient("localhost:5555") as client:
        msg_id = await client.enqueue("my-queue", {"tenant": "acme"}, b"hello")

        async for msg in await client.consume("my-queue"):
            print(f"Received: {msg.id}")
            await client.ack("my-queue", msg.id)

asyncio.run(main())
```

## TLS

For servers using certificates from a public CA (e.g., Let's Encrypt), enable TLS
with the system trust store:

```python
from fila import Client

# TLS using OS system trust store.
client = Client("localhost:5555", tls=True)
```

For servers using a private CA, provide the CA certificate explicitly:

```python
from fila import Client

# Read certificates.
with open("ca.pem", "rb") as f:
    ca_cert = f.read()
with open("client.pem", "rb") as f:
    client_cert = f.read()
with open("client-key.pem", "rb") as f:
    client_key = f.read()

# TLS with custom CA (server verification).
client = Client("localhost:5555", ca_cert=ca_cert)

# Mutual TLS (client + server verification).
client = Client(
    "localhost:5555",
    ca_cert=ca_cert,
    client_cert=client_cert,
    client_key=client_key,
)
```

Note: `ca_cert` implies `tls=True` -- you don't need to pass both.

## API Key Authentication

When the server has API key auth enabled, pass the key to the client:

```python
from fila import Client

client = Client("localhost:5555", api_key="fila_your_api_key_here")

# Combined with TLS:
client = Client(
    "localhost:5555",
    ca_cert=ca_cert,
    api_key="fila_your_api_key_here",
)
```

The API key is sent as `authorization: Bearer <key>` metadata on every RPC.

## API

### `Client(addr, *, tls=False, ca_cert=None, client_cert=None, client_key=None, api_key=None)` / `AsyncClient(...)`

Connect to a Fila broker. Both support context manager protocol.

### `client.enqueue(queue, headers, payload) -> str`

Enqueue a message. Returns the broker-assigned message ID.

### `client.consume(queue) -> Iterator[ConsumeMessage]`

Open a streaming consumer. Returns an iterator (sync) or async iterator (async) that yields messages as they become available.

### `client.ack(queue, msg_id)`

Acknowledge a successfully processed message. The message is permanently removed.

### `client.nack(queue, msg_id, error)`

Negatively acknowledge a failed message. The message is requeued or routed to the dead-letter queue based on the queue's configuration.

## Error Handling

Per-operation exception classes:

```python
from fila import QueueNotFoundError, MessageNotFoundError

try:
    client.enqueue("missing-queue", None, b"test")
except QueueNotFoundError:
    print("Queue does not exist")

try:
    client.ack("my-queue", "missing-id")
except MessageNotFoundError:
    print("Message does not exist")
```

## License

AGPLv3
