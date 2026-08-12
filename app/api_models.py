"""Pydantic contracts for the Queuemaxxing HTTP API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FiniteFloat,
    StrictBool,
    StrictInt,
    field_validator,
)

from app.models import Message, MessageState, QueueOrder


class ConfigUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order: QueueOrder
    priority_enabled: StrictBool


class ConfigResponse(BaseModel):
    order: QueueOrder
    priority_enabled: bool


class EnqueueRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payload: dict[str, Any]
    priority: StrictInt = 0
    delay_seconds: FiniteFloat = Field(default=0, ge=0)


class ReceiveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str
    visibility_timeout_seconds: FiniteFloat = Field(default=30, gt=0)

    @field_validator("worker_id")
    @classmethod
    def validate_worker_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("worker_id must be non-blank")
        return value.strip()


class AckRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    receipt_handle: str

    @field_validator("receipt_handle")
    @classmethod
    def validate_receipt_handle(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("receipt_handle must be non-blank")
        return value


class NackRequest(AckRequest):
    retry_delay_seconds: FiniteFloat = Field(default=0, ge=0)


class MessageResponse(BaseModel):
    """Public message state that deliberately omits its receipt handle."""

    id: str
    payload: Any
    priority: int
    sequence: int
    created_at: datetime
    available_at: datetime
    state: MessageState
    delivery_attempts: int
    leased_by: str | None
    lease_expires_at: datetime | None
    completed_at: datetime | None

    @classmethod
    def from_message(cls, message: Message) -> "MessageResponse":
        return cls(
            id=message.id,
            payload=message.payload,
            priority=message.priority,
            sequence=message.sequence,
            created_at=message.created_at,
            available_at=message.available_at,
            state=message.state,
            delivery_attempts=message.delivery_attempts,
            leased_by=message.leased_by,
            lease_expires_at=message.lease_expires_at,
            completed_at=message.completed_at,
        )


class LeasedMessageResponse(MessageResponse):
    receipt_handle: str

    @classmethod
    def from_message(cls, message: Message) -> "LeasedMessageResponse":
        if message.receipt_handle is None:
            raise ValueError("leased message has no receipt handle")
        public = MessageResponse.from_message(message).model_dump()
        return cls(**public, receipt_handle=message.receipt_handle)


class ReceiveResponse(BaseModel):
    message: LeasedMessageResponse | None


class ClearCompletedResponse(BaseModel):
    cleared: int


class StatsResponse(BaseModel):
    total: int
    delayed: int
    ready: int
    in_flight: int
    completed: int
    total_delivery_attempts: int
    redelivery_count: int
    active_worker_count: int


class EventResponse(BaseModel):
    record_number: int
    event_type: str
    message_id: str | None


class EventsResponse(BaseModel):
    ordering: Literal["oldest_first"] = "oldest_first"
    events: list[EventResponse]
