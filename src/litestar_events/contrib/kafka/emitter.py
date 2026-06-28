from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from litestar.events import BaseEventEmitterBackend, EventListener
from typing_extensions import Self

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

_VALID_KAFKA_TOPIC = re.compile(r"^[A-Za-z0-9._-]{1,249}$")


def _validate_topic(topic: str) -> None:
    if not _VALID_KAFKA_TOPIC.match(topic):
        msg = (
            f"Invalid Kafka topic {topic!r}. Topics must match "
            "[A-Za-z0-9._-] and be 1..249 chars."
        )
        raise ValueError(
            msg,
        )


class KafkaEventEmitter(BaseEventEmitterBackend):
    """A Litestar event emitter backend backed by ``aiokafka`` (pure-Python Kafka).

    Delivery semantics:
      - At-least-once with consumer-group offsets. Auto-commit is disabled;
        offsets are committed only after listener dispatch returns, so a
        worker crash mid-dispatch causes the broker to redeliver.
      - Listener exceptions are caught per-listener; siblings still complete.
        A listener that raises does not block the commit — the contract is
        at-least-once at the transport layer, not at the listener layer.
      - ``group_id`` controls fanout vs work-queue:
          * Unique per instance (default, random UUID): every app instance
            sees every event (broadcast/fanout).
          * Shared across instances: exactly one instance handles each event
            (work-queue semantics).

    Topic layout:
      - One Kafka topic per registered event id: ``f"{topic_prefix}{event_id}"``.
      - Validated at ``__aenter__`` against Kafka's topic-name rules.
      - Topic auto-creation depends on broker config
        (``auto.create.topics.enable``). For production, pre-create topics
        with explicit partitions/replication.

    Pairs with ``ConfluentEventEmitter`` (``confluent-kafka``) as a faster
    C-backed alternative. Same semantics, same wire protocol.
    """

    def __init__(
        self,
        listeners: Sequence[EventListener],
        *,
        bootstrap_servers: str = "localhost:9092",
        group_id: str | None = None,
        topic_prefix: str = "litestar.events.",
        auto_offset_reset: str = "latest",
    ) -> None:
        super().__init__(listeners)
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id or f"litestar-events-{uuid.uuid4()}"
        self._topic_prefix = topic_prefix
        self._auto_offset_reset = auto_offset_reset

        self._by_event: dict[str, list[EventListener]] = defaultdict(list)
        for listener in listeners:
            for event_id in listener.event_ids:
                self._by_event[event_id].append(listener)

        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._publish_queue: (
            asyncio.Queue[tuple[str, tuple[Any, ...], dict[str, Any]]] | None
        ) = None
        self._publisher_task: asyncio.Task[None] | None = None
        self._consumer_task: asyncio.Task[None] | None = None

    def _topic(self, event_id: str) -> str:
        return f"{self._topic_prefix}{event_id}"

    async def __aenter__(self) -> Self:
        for event_id in self._by_event:
            try:
                _validate_topic(self._topic(event_id))
            except ValueError as exc:
                msg = f"event_id {event_id!r} produces an invalid Kafka topic. {exc}"
                raise ValueError(
                    msg,
                ) from exc

        self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
        await self._producer.start()

        if self._by_event:
            topics = [self._topic(event_id) for event_id in self._by_event]
            self._consumer = AIOKafkaConsumer(
                *topics,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                auto_offset_reset=self._auto_offset_reset,
                enable_auto_commit=False,
            )
            await self._consumer.start()
            self._consumer_task = asyncio.create_task(self._consumer_loop())

        self._publish_queue = asyncio.Queue()
        self._publisher_task = asyncio.create_task(self._publisher_loop())
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        for task in (self._publisher_task, self._consumer_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        if self._consumer is not None:
            await self._consumer.stop()
        if self._producer is not None:
            await self._producer.stop()

    def emit(self, event_id: str, *args: Any, **kwargs: Any) -> None:
        if self._publish_queue is None:
            msg = "Emitter used outside its async context"
            raise RuntimeError(msg)
        self._publish_queue.put_nowait((event_id, args, kwargs))

    async def _publisher_loop(self) -> None:
        assert self._publish_queue is not None
        assert self._producer is not None
        while True:
            event_id, args, kwargs = await self._publish_queue.get()
            try:
                body = json.dumps({"args": list(args), "kwargs": kwargs}).encode()
                await self._producer.send_and_wait(self._topic(event_id), body)
            except Exception:
                logger.exception("Failed to publish event %s", event_id)

    async def _consumer_loop(self) -> None:
        assert self._consumer is not None
        prefix_len = len(self._topic_prefix)
        async for msg in self._consumer:
            topic = msg.topic
            event_id = topic[prefix_len:]
            value = msg.value
            if value is None:  # tombstone / empty record
                continue
            try:
                payload = json.loads(value)
                args = payload.get("args", [])
                kwargs = payload.get("kwargs", {})
            except Exception:
                logger.exception("Dropping unparseable message on topic %s", topic)
                continue

            listeners = self._by_event.get(event_id, [])
            if not listeners:
                continue

            async def _run_one(listener: EventListener) -> None:
                try:
                    await listener.fn(*args, **kwargs)
                except Exception:
                    logger.exception(
                        "Listener %s failed for event %s",
                        getattr(listener.fn, "__name__", repr(listener.fn)),
                        event_id,
                    )

            await asyncio.gather(
                *(_run_one(listener) for listener in listeners),
                return_exceptions=True,
            )

            try:
                await self._consumer.commit()
            except Exception:
                logger.exception(
                    "Failed to commit offset for event %s; message may redeliver",
                    event_id,
                )
