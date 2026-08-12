"""FastAPI application factory for the durable Queuemaxxing service."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Query, Request, status
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.api_models import (
    AckRequest,
    ClearCompletedResponse,
    ConfigResponse,
    ConfigUpdateRequest,
    EnqueueRequest,
    EventsResponse,
    LeasedMessageResponse,
    MessageResponse,
    NackRequest,
    ReceiveRequest,
    ReceiveResponse,
    StatsResponse,
)
from app.models import MessageState, QueueConfig
from app.queue_engine import (
    Clock,
    InvalidReceiptHandleError,
    InvalidRetryDelayError,
    InvalidVisibilityTimeoutError,
    InvalidWorkerIDError,
    MessageNotFoundError,
    MessageNotInFlightError,
    QueueEngine,
    QueueEngineError,
)
from app.wal import WALWriteError


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = PROJECT_ROOT / "static"
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"


def create_app(
    *,
    engine: QueueEngine | None = None,
    wal_path: str | Path | None = None,
    clock: Clock | None = None,
) -> FastAPI:
    """Create an app; injected engines remain owned by their caller."""

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        owns_engine = engine is None
        active_engine = engine
        if active_engine is None:
            configured_path = Path(wal_path) if wal_path is not None else _default_wal_path()
            active_engine = QueueEngine.open_durable(configured_path, clock=clock)
        application.state.queue_engine = active_engine
        try:
            yield
        finally:
            if owns_engine:
                active_engine.close()
            application.state.queue_engine = None

    application = FastAPI(
        title="Queuemaxxing",
        version="0.7.0",
        description="A durable, configurable HTTP queue.",
        lifespan=lifespan,
    )
    application.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    _register_exception_handlers(application)
    _register_routes(application)
    return application


def _default_wal_path() -> Path:
    data_dir = Path(os.getenv("QUEUEMAXXING_DATA_DIR", str(DEFAULT_DATA_DIR)))
    return data_dir / "queue.wal"


def _get_engine(request: Request) -> QueueEngine:
    active_engine = getattr(request.app.state, "queue_engine", None)
    if not isinstance(active_engine, QueueEngine):
        raise WALWriteError("queue engine is unavailable")
    return active_engine


EngineDependency = Annotated[QueueEngine, Depends(_get_engine)]


def _register_exception_handlers(application: FastAPI) -> None:
    @application.exception_handler(MessageNotFoundError)
    async def message_not_found(_: Request, exc: MessageNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @application.exception_handler(InvalidReceiptHandleError)
    @application.exception_handler(MessageNotInFlightError)
    async def transition_conflict(_: Request, exc: QueueEngineError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @application.exception_handler(InvalidRetryDelayError)
    @application.exception_handler(InvalidVisibilityTimeoutError)
    @application.exception_handler(InvalidWorkerIDError)
    async def domain_validation(_: Request, exc: QueueEngineError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @application.exception_handler(WALWriteError)
    async def storage_unavailable(_: Request, __: WALWriteError) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"detail": "queue storage is temporarily unavailable"},
        )


def _register_routes(application: FastAPI) -> None:
    @application.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "queuemaxxing"}

    @application.get("/", response_class=FileResponse)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @application.get("/api/config", response_model=ConfigResponse)
    async def get_config(queue: EngineDependency) -> ConfigResponse:
        config = queue.get_config()
        return ConfigResponse(
            order=config.order,
            priority_enabled=config.priority_enabled,
        )

    @application.put("/api/config", response_model=ConfigResponse)
    async def update_config(
        request_body: ConfigUpdateRequest, queue: EngineDependency
    ) -> ConfigResponse:
        config = queue.update_config(
            QueueConfig(
                order=request_body.order,
                priority_enabled=request_body.priority_enabled,
            )
        )
        return ConfigResponse(
            order=config.order,
            priority_enabled=config.priority_enabled,
        )

    @application.post(
        "/api/messages",
        response_model=MessageResponse,
        status_code=status.HTTP_201_CREATED,
    )
    async def enqueue(
        request_body: EnqueueRequest, queue: EngineDependency
    ) -> MessageResponse:
        message = queue.enqueue(
            request_body.payload,
            priority=request_body.priority,
            delay_seconds=request_body.delay_seconds,
        )
        return MessageResponse.from_message(message)

    @application.post(
        "/api/messages/receive",
        response_model=ReceiveResponse,
    )
    async def receive(
        request_body: ReceiveRequest, queue: EngineDependency
    ) -> ReceiveResponse:
        message = queue.receive(
            request_body.worker_id,
            visibility_timeout_seconds=request_body.visibility_timeout_seconds,
        )
        return ReceiveResponse(
            message=(
                LeasedMessageResponse.from_message(message)
                if message is not None
                else None
            )
        )

    @application.post(
        "/api/messages/{message_id}/ack",
        response_model=MessageResponse,
    )
    async def ack(
        message_id: str, request_body: AckRequest, queue: EngineDependency
    ) -> MessageResponse:
        return MessageResponse.from_message(
            queue.ack(message_id, request_body.receipt_handle)
        )

    @application.post(
        "/api/messages/{message_id}/nack",
        response_model=MessageResponse,
    )
    async def nack(
        message_id: str, request_body: NackRequest, queue: EngineDependency
    ) -> MessageResponse:
        return MessageResponse.from_message(
            queue.nack(
                message_id,
                request_body.receipt_handle,
                retry_delay_seconds=request_body.retry_delay_seconds,
            )
        )

    @application.get("/api/messages", response_model=list[MessageResponse])
    async def list_messages(
        queue: EngineDependency,
        state: MessageState | None = Query(default=None),
    ) -> list[MessageResponse]:
        messages = queue.messages()
        if state is not None:
            messages = [message for message in messages if message.state is state]
        return [MessageResponse.from_message(message) for message in messages]

    @application.delete(
        "/api/messages/completed",
        response_model=ClearCompletedResponse,
    )
    async def clear_completed(queue: EngineDependency) -> ClearCompletedResponse:
        return ClearCompletedResponse(cleared=queue.clear_completed())

    @application.get("/api/stats", response_model=StatsResponse)
    async def get_stats(queue: EngineDependency) -> StatsResponse:
        return StatsResponse(**queue.statistics())

    @application.get("/api/events", response_model=EventsResponse)
    async def get_events(
        queue: EngineDependency,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> EventsResponse:
        return EventsResponse(events=queue.recent_events(limit))


app = create_app()
