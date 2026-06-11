# litestar-events

Event Emitter Backends for [Litestar](https://litestar.dev).

Litestar ships with an in-process event emitter (`SimpleEventEmitter`) that runs
listeners in the same process that emitted the event. That is great for
side-effects that can tolerate "best-effort, same-process" delivery, but it
falls short as soon as you need:

- **Cross-process fanout** — multiple app instances reacting to the same event.
- **Durability** — events that survive a worker crash or restart.
- **Backpressure / decoupling** — producers that should not wait on slow
  listeners.

`litestar-events` provides drop-in `BaseEventEmitterBackend` implementations
backed by common messaging systems, so you can keep Litestar's `@listener`
ergonomics while swapping the transport underneath.

## Backends

| Backend    | Extra         | Client library                              |
|------------|---------------|---------------------------------------------|
| RabbitMQ   | `rabbit`      | [`aio-pika`](https://aio-pika.readthedocs.io) |
| Kafka      | `kafka`       | [`aiokafka`](https://aiokafka.readthedocs.io) |
| Kafka (C)  | `confluent`   | [`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python) |
| NATS       | `nats`        | [`nats-py`](https://github.com/nats-io/nats.py) |
| Redis      | `redis`       | [`redis`](https://github.com/redis/redis-py) (async) |
| MQTT       | `mqtt`        | [`aiomqtt`](https://github.com/empicano/aiomqtt) |
| PostgreSQL | `postgres`    | [`psycopg`](https://www.psycopg.org/psycopg3/) (`LISTEN/NOTIFY`) |

## Installation

Install the core package plus the extra for the backend(s) you want:

```bash
# RabbitMQ backend
pip install "litestar-events[rabbit]"

# Multiple backends at once
pip install "litestar-events[rabbit,redis,nats]"
```

Using [`uv`](https://docs.astral.sh/uv/):

```bash
uv add "litestar-events[rabbit]"
```

## Quick start

The same Litestar app, three different transports. The app logic — your
route handler, your listeners, your emitted events — is identical. Only
the `event_emitter_backend=` line changes.

### 1. Define your event and listeners

```python
# app.py
from __future__ import annotations

from typing import Any

from litestar import Request, post
from litestar.events import listener

USER_REGISTERED = "user_registered"


@listener(USER_REGISTERED)
async def send_welcome_email(*, email: str, **_: Any) -> None:
    print(f"[email] welcome to {email}")


@listener(USER_REGISTERED)
async def record_analytics(*, email: str, **_: Any) -> None:
    print(f"[analytics] new signup: {email}")


@post("/users")
async def register_user(request: Request, data: dict[str, str]) -> dict[str, str]:
    email = data["email"]
    request.app.emit(USER_REGISTERED, email=email)
    return {"status": "queued", "email": email}
```

Two listeners react concurrently to one event. Notice the listeners use
`**_: Any` — that's the [Litestar convention][events-docs] for keeping
listener signatures forwards-compatible when multiple listeners share an
event payload.

[events-docs]: https://docs.litestar.dev/2/usage/events.html

### 2. Pick a backend

Every backend constructor accepts `listeners: Sequence[EventListener]` plus
its own keyword arguments. Use `functools.partial` to bake in the kwargs
and hand the result to Litestar.

#### RabbitMQ (durable, at-least-once)

```python
from functools import partial

from litestar import Litestar

from litestar_events.contrib.rabbit import RabbitEventEmitter

app = Litestar(
    route_handlers=[register_user],
    listeners=[send_welcome_email, record_analytics],
    event_emitter_backend=partial(
        RabbitEventEmitter,
        amqp_url="amqp://guest:guest@localhost/",
        # queue_name="my-app",   # set for work-queue semantics across replicas
    ),
)
```

#### Redis (fire-and-forget pub/sub)

```python
from functools import partial

from litestar import Litestar

from litestar_events.contrib.redis import RedisEventEmitter

app = Litestar(
    route_handlers=[register_user],
    listeners=[send_welcome_email, record_analytics],
    event_emitter_backend=partial(
        RedisEventEmitter,
        redis_url="redis://localhost:6379/0",
    ),
)
```

#### PostgreSQL (`LISTEN`/`NOTIFY`)

```python
from functools import partial

from litestar import Litestar

from litestar_events.contrib.postgres import PostgresEventEmitter

app = Litestar(
    route_handlers=[register_user],
    listeners=[send_welcome_email, record_analytics],
    event_emitter_backend=partial(
        PostgresEventEmitter,
        dsn="postgresql://postgres:postgres@localhost:5432/events",
    ),
)
```

### 3. Trigger an event

```bash
curl -X POST http://localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"email": "ada@example.com"}'
```

Both listeners run concurrently for every emit, regardless of which backend
you picked. The choice of backend changes **how** the event gets from
emitter to listener (and what happens when things go wrong), not whether
the listener fires.

## Examples

Runnable examples for each backend live under [`examples/`](./examples):

- [`examples/rabbit`](./examples/rabbit) — RabbitMQ via `aio-pika`
- [`examples/kafka`](./examples/kafka) — Kafka via `aiokafka`
- [`examples/confluent`](./examples/confluent) — Kafka via `confluent-kafka`
- [`examples/nats`](./examples/nats) — NATS
- [`examples/redis`](./examples/redis) — Redis Pub/Sub
- [`examples/mqtt`](./examples/mqtt) — MQTT
- [`examples/postgres`](./examples/postgres) — PostgreSQL `LISTEN/NOTIFY`

## Delivery semantics

Different backends offer different guarantees. The library tries to give each
backend the **strongest reasonable default** for its transport, while letting
you weaken or tune those guarantees via constructor arguments:

- **RabbitMQ** — at-least-once with per-listener error isolation and a
  dead-letter exchange for unparseable messages.
- **Kafka / Confluent** — at-least-once with consumer-group offsets.
- **NATS** — at-most-once by default; JetStream-backed at-least-once when
  configured.
- **Redis / MQTT / Postgres `LISTEN/NOTIFY`** — fire-and-forget pub/sub; events
  emitted while no subscriber is connected are lost.

See each backend's docstring for the exact knobs.

## License

MIT
