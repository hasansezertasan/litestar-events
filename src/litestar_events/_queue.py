"""Shared utilities for queue-backed event emitter backends."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncio


class QueuedEmitterMixin:
    """Provides the synchronous ``emit`` for queue-backed emitters.

    Backends that buffer events on an in-process ``asyncio.Queue`` (drained by
    a background publisher task started in ``__aenter__``) share this method.
    Subclasses must set ``self._publish_queue`` in ``__init__`` and must list
    this mixin before ``BaseEventEmitterBackend`` so this ``emit`` satisfies the
    abstract method.
    """

    _publish_queue: asyncio.Queue[tuple[str, tuple[Any, ...], dict[str, Any]]] | None

    def emit(self, event_id: str, *args: Any, **kwargs: Any) -> None:
        if self._publish_queue is None:
            msg = "Emitter used outside its async context"
            raise RuntimeError(msg)
        self._publish_queue.put_nowait((event_id, args, kwargs))
