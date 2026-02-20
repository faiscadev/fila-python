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

## API

### `Client(addr)` / `AsyncClient(addr)`

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
