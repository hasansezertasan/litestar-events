"""Shared utilities for queue-backed event emitter backends."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    import asyncio

_T = TypeVar("_T")


def require(value: _T | None, name: str) -> _T:
    """Return ``value`` if set, else raise -- an ``assert`` that survives ``-O``.

    Background publisher/consumer loops narrow attributes established in
    ``__aenter__`` (queues, clients, pools, producers). ``assert x is not None``
    narrows for the type checkers but is stripped under ``python -O``, which
    would defer a stray ``None`` to an opaque ``AttributeError`` deep inside a
    detached task. This helper raises a clear ``RuntimeError`` unconditionally
    and returns the non-``None`` value (so the caller binds a narrowed local).

    Raises:
        RuntimeError: if ``value`` is ``None``.
    """
    if value is None:
        msg = f"{name} is unavailable; emitter used outside its async context"
        raise RuntimeError(msg)
    return value


class QueuedEmitterMixin:
    """Provides the synchronous ``emit`` for queue-backed emitters.

    Backends that buffer events on an in-process ``asyncio.Queue`` (drained by
    a background publisher task started in ``__aenter__``) share this method.
    Subclasses set ``self._publish_queue`` to a live queue in ``__aenter__``;
    outside that window it is ``None`` (the class default below). Subclasses
    must list this mixin before ``BaseEventEmitterBackend`` so this ``emit``
    satisfies the abstract method.

    The class-level ``= None`` default is deliberate: it guarantees the
    ``emit`` guard raises a clear ``RuntimeError`` even for a subclass that
    forgets to initialize the attribute, rather than an opaque ``AttributeError``.
    """

    # Class default so the guard below holds even before any ``__init__`` /
    # ``__aenter__`` assignment; per-instance assignment shadows it normally.
    _publish_queue: (
        asyncio.Queue[tuple[str, tuple[Any, ...], dict[str, Any]]] | None
    ) = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        # The MRO ordering (mixin before the ABC) is not type-checkable, so
        # enforce it at class-definition time: a wrong order would otherwise
        # let the ABC's abstract ``emit`` win silently.
        super().__init_subclass__(**kwargs)
        from litestar.events import BaseEventEmitterBackend

        mro = cls.__mro__
        if BaseEventEmitterBackend in mro and mro.index(
            QueuedEmitterMixin,
        ) > mro.index(BaseEventEmitterBackend):
            msg = (
                f"{cls.__name__} must list QueuedEmitterMixin before "
                "BaseEventEmitterBackend so the mixin's concrete `emit` "
                "satisfies the abstract method."
            )
            raise TypeError(msg)

    def emit(self, event_id: str, *args: Any, **kwargs: Any) -> None:
        if self._publish_queue is None:
            msg = "Emitter used outside its async context"
            raise RuntimeError(msg)
        self._publish_queue.put_nowait((event_id, args, kwargs))
