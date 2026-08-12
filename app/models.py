"""Core models shared by the Queuemaxxing queue engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TypeAlias


JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]


class QueueOrder(str, Enum):
    """How messages with the same effective priority are ordered."""

    FIFO = "fifo"
    LIFO = "lifo"


class MessageState(str, Enum):
    """A message's current queue lifecycle state.

    Leasing will extend this enum with ``in_flight`` and ``completed`` states
    in a later step.
    """

    DELAYED = "delayed"
    READY = "ready"


@dataclass(frozen=True, slots=True)
class QueueConfig:
    """Ordering behavior for one queue."""

    order: QueueOrder = QueueOrder.FIFO
    priority_enabled: bool = False

    def __post_init__(self) -> None:
        try:
            normalized_order = QueueOrder(self.order)
        except (TypeError, ValueError) as exc:
            supported = ", ".join(order.value for order in QueueOrder)
            raise ValueError(f"unsupported queue order; expected one of: {supported}") from exc

        if type(self.priority_enabled) is not bool:
            raise TypeError("priority_enabled must be a boolean")

        object.__setattr__(self, "order", normalized_order)


@dataclass(slots=True)
class Message:
    """A unit of JSON-compatible work owned by a queue."""

    id: str
    payload: JSONValue
    priority: int
    sequence: int
    created_at: datetime
    available_at: datetime
    state: MessageState
