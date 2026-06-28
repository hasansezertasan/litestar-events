"""Unit test for the shared ``QueuedEmitterMixin.emit`` lifecycle guard.

``emit`` is the synchronous entry point every queue-backed backend exposes.
Calling it before ``__aenter__`` (or after ``__aexit__``) has no live queue to
push onto, so the mixin must raise a clear ``RuntimeError`` rather than fail
opaquely. We exercise it via the rabbit emitter (cheapest to import); the
behavior is identical across every backend that mixes in ``QueuedEmitterMixin``.
"""

from __future__ import annotations

import pytest


def _emitter():
    """Build a rabbit emitter without entering its async context (no broker)."""
    from litestar_events.contrib.rabbit.emitter import RabbitEventEmitter

    return RabbitEventEmitter([])


def test_emit_outside_async_context_raises() -> None:
    emitter = _emitter()
    with pytest.raises(RuntimeError, match="outside its async context"):
        emitter.emit("user_registered", 1, name="ada")


def test_publish_queue_defaults_to_none() -> None:
    # The class-level default backs the guard even if __init__ never assigns it,
    # turning a would-be AttributeError into the intended RuntimeError.
    from litestar_events._queue import QueuedEmitterMixin

    assert QueuedEmitterMixin._publish_queue is None
    assert _emitter()._publish_queue is None


def test_wrong_mro_order_rejected() -> None:
    # The mixin must precede the ABC; the wrong order is caught at definition
    # time by __init_subclass__ rather than failing opaquely later.
    from litestar.events import BaseEventEmitterBackend

    from litestar_events._queue import QueuedEmitterMixin

    with pytest.raises(TypeError, match="before BaseEventEmitterBackend"):

        class _Bad(BaseEventEmitterBackend, QueuedEmitterMixin):
            pass
