from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from collections import defaultdict
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from litestar.events import BaseEventEmitterBackend, EventListener
from typing_extensions import Self

if TYPE_CHECKING:
    from collections.abc import Sequence

    from gcloud.aio.pubsub import (
        PublisherClient,
        SubscriberClient,
        SubscriberMessage,
    )

logger = logging.getLogger(__name__)


class PubSubEventEmitter(BaseEventEmitterBackend):
    """A Litestar event emitter backend backed by GCP Pub/Sub via gcloud-aio.

    Delivery semantics:
      - At-least-once + durable. A message is acked only after all matching
        in-process listeners run (the subscribe handler returns), so a crash
        mid-dispatch leaves the message to be redelivered.
      - Listener exceptions are caught per-listener; siblings still complete.
        The handler never re-raises, so a permanently-failing listener does not
        cause infinite redelivery (matching the other durable backends).
      - Unparseable messages are acked and dropped with a logged error.

    Topology:
      - A single topic (``topic_id``) carries every event id. The originating
        ``event_id`` travels in the ``event_id`` message attribute; the body is
        the same ``{"args": [...], "kwargs": {...}}`` JSON contract used by the
        other backends.
      - ``subscription_name=None`` (default): a unique per-instance subscription
        is created (and deleted on exit). Pub/Sub delivers every message to
        every subscription, so each app instance sees every event -> broadcast
        fanout.
      - ``subscription_name="..."``: a shared, durable subscription. Instances
        sharing it are competing consumers -> work-queue semantics.

    Credentials:
      - Production uses the standard GCP credential chain
        (``GOOGLE_APPLICATION_CREDENTIALS`` etc.).
      - ``emulator_host`` (``host:port``) targets a local Pub/Sub emulator; it
        sets ``PUBSUB_EMULATOR_HOST`` so gcloud-aio skips auth. Mainly for dev
        and tests.
    """

    def __init__(
        self,
        listeners: Sequence[EventListener],
        *,
        project_id: str,
        topic_id: str = "litestar-events",
        subscription_name: str | None = None,
        create_resources: bool = True,
        emulator_host: str | None = None,
        ack_deadline: float | None = None,
    ) -> None:
        super().__init__(listeners)
        self._project_id = project_id
        self._topic_id = topic_id
        self._subscription_name = subscription_name
        self._create_resources = create_resources
        self._emulator_host = emulator_host
        self._ack_deadline = ack_deadline

        self._by_event: dict[str, list[EventListener]] = defaultdict(list)
        for listener in listeners:
            for event_id in listener.event_ids:
                self._by_event[event_id].append(listener)

        self._publisher: PublisherClient | None = None
        self._subscriber: SubscriberClient | None = None
        self._topic = ""
        self._subscription = ""
        self._owns_subscription = subscription_name is None
        self._publish_queue: (
            asyncio.Queue[tuple[str, tuple[Any, ...], dict[str, Any]]] | None
        ) = None
        self._publisher_task: asyncio.Task[None] | None = None
        self._consumer_task: asyncio.Task[None] | None = None

    async def _ensure_topic(self) -> None:
        from aiohttp import ClientResponseError

        assert self._publisher is not None
        try:
            await self._publisher.create_topic(self._topic)
        except ClientResponseError as exc:
            if exc.status != 409:  # 409 ALREADY_EXISTS is expected
                raise

    async def _ensure_subscription(self) -> None:
        from aiohttp import ClientResponseError

        assert self._subscriber is not None
        try:
            await self._subscriber.create_subscription(
                self._subscription,
                self._topic,
            )
        except ClientResponseError as exc:
            if exc.status != 409:
                raise

    async def __aenter__(self) -> Self:
        from gcloud.aio.pubsub import PublisherClient, SubscriberClient

        if self._emulator_host:
            os.environ.setdefault("PUBSUB_EMULATOR_HOST", self._emulator_host)

        self._publisher = PublisherClient()
        self._topic = f"projects/{self._project_id}/topics/{self._topic_id}"
        if self._create_resources:
            await self._ensure_topic()

        sub_id = self._subscription_name or f"{self._topic_id}-{uuid4().hex}"
        self._subscription = f"projects/{self._project_id}/subscriptions/{sub_id}"

        self._publish_queue = asyncio.Queue()
        self._publisher_task = asyncio.create_task(self._publisher_loop())

        if self._by_event:
            self._subscriber = SubscriberClient()
            if self._create_resources:
                await self._ensure_subscription()
            self._consumer_task = asyncio.create_task(self._run_subscribe())
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        for task in (self._publisher_task, self._consumer_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        if (
            self._owns_subscription
            and self._create_resources
            and self._subscriber is not None
        ):
            try:
                await self._subscriber.delete_subscription(self._subscription)
            except Exception:
                logger.exception(
                    "Failed to delete per-instance subscription %s",
                    self._subscription,
                )

        if self._subscriber is not None:
            await self._subscriber.close()
        if self._publisher is not None:
            await self._publisher.close()

    def emit(self, event_id: str, *args: Any, **kwargs: Any) -> None:
        if self._publish_queue is None:
            msg = "Emitter used outside its async context"
            raise RuntimeError(msg)
        self._publish_queue.put_nowait((event_id, args, kwargs))

    async def _publisher_loop(self) -> None:
        from gcloud.aio.pubsub import PubsubMessage

        assert self._publish_queue is not None
        assert self._publisher is not None
        while True:
            event_id, args, kwargs = await self._publish_queue.get()
            try:
                body = json.dumps({"args": list(args), "kwargs": kwargs}).encode()
                await self._publisher.publish(
                    self._topic,
                    [PubsubMessage(body, event_id=event_id)],
                )
            except Exception:
                logger.exception("Failed to publish event %s", event_id)

    async def _run_subscribe(self) -> None:
        from gcloud.aio.pubsub import subscribe

        assert self._subscriber is not None
        await subscribe(
            self._subscription,
            self._handle_message,
            self._subscriber,
            num_producers=1,
            max_messages_per_producer=10,
            ack_deadline=self._ack_deadline,
        )

    async def _handle_message(self, message: SubscriberMessage) -> None:
        event_id = (message.attributes or {}).get("event_id", "")
        if message.data is None:
            logger.warning(
                "Dropping Pub/Sub message with no data (event_id=%s)", event_id
            )
            return
        try:
            payload = json.loads(message.data)
            args = payload.get("args", [])
            kwargs = payload.get("kwargs", {})
        except Exception:
            logger.exception("Dropping unparseable Pub/Sub message")
            return

        listeners = self._by_event.get(event_id, [])
        if not listeners:
            return

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
