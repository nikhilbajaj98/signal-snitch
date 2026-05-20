from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
import json
import logging
import os
import time
from typing import Any

import clickhouse_connect
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aggregator")

DEFAULT_PACKET_TYPE = "kafka_tcp"


class TelemetryEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    source_ip: str
    payload: str
    packet_type: str | None = Field(default=None)
    captured_at: datetime | None = Field(default=None)
    meta: dict[str, Any] | str | None = Field(default=None)

    def normalized_packet_type(self) -> str:
        if self.packet_type and self.packet_type.strip():
            return self.packet_type.strip()
        return DEFAULT_PACKET_TYPE

    def normalized_event_time(self) -> datetime:
        if self.captured_at:
            return self.captured_at.astimezone(UTC)
        return datetime.now(tz=UTC)

    def normalized_meta_json(self) -> str:
        if isinstance(self.meta, dict):
            return json.dumps(self.meta)
        if isinstance(self.meta, str) and self.meta.strip():
            return self.meta
        return "{}"


def get_clickhouse_client() -> Any:
    client_kwargs: dict[str, Any] = {
        "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
    }
    if os.getenv("CLICKHOUSE_USER"):
        client_kwargs["username"] = os.getenv("CLICKHOUSE_USER")
    if os.getenv("CLICKHOUSE_PASSWORD"):
        client_kwargs["password"] = os.getenv("CLICKHOUSE_PASSWORD")
    if os.getenv("CLICKHOUSE_DATABASE"):
        client_kwargs["database"] = os.getenv("CLICKHOUSE_DATABASE")

    return clickhouse_connect.get_client(**client_kwargs)


def ensure_telemetry_table(ch_client: Any) -> None:
    ch_client.command(
        """
CREATE TABLE IF NOT EXISTS telemetry (
    event_time DateTime64(3),
    ingested_at DateTime64(3) DEFAULT now64(),
    packet_type LowCardinality(String),
    source_ip String,
    payload String,
    meta_json String DEFAULT '{}'
) ENGINE = MergeTree()
PARTITION BY toDate(event_time)
ORDER BY (packet_type, event_time, source_ip);
"""
    )
    # Keep compatibility with older local tables from previous schema versions.
    ch_client.command("ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS event_time DateTime64(3)")
    ch_client.command(
        "ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS ingested_at DateTime64(3) DEFAULT now64()"
    )
    ch_client.command(
        "ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS packet_type LowCardinality(String)"
    )
    ch_client.command("ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS meta_json String DEFAULT '{}'")


def create_app(ch_client_override: Any | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.ch_client = ch_client_override
        if app.state.ch_client is None:
            app.state.ch_client = get_clickhouse_client()
        ensure_telemetry_table(app.state.ch_client)
        logger.info("Connected to ClickHouse and telemetry schema is ready")
        yield

    app = FastAPI(lifespan=lifespan)
    app.state.ch_client = ch_client_override

    @app.post("/telemetry")
    async def telemetry(event: TelemetryEvent) -> dict[str, str]:
        started_at = time.perf_counter()
        packet_type = event.normalized_packet_type()
        event_time = event.normalized_event_time()
        meta_json = event.normalized_meta_json()

        if app.state.ch_client is None:
            raise HTTPException(status_code=503, detail="clickhouse client is not initialized")

        try:
            app.state.ch_client.insert(
                "telemetry",
                [[event_time, packet_type, event.source_ip, event.payload, meta_json]],
                column_names=["event_time", "packet_type", "source_ip", "payload", "meta_json"],
            )
        except Exception as exc:
            latency_ms = (time.perf_counter() - started_at) * 1000
            logger.exception(
                "Failed to persist telemetry event packet_type=%s source_ip=%s latency_ms=%.2f error=%s",
                packet_type,
                event.source_ip,
                latency_ms,
                exc,
            )
            raise HTTPException(status_code=503, detail="failed to persist telemetry event") from exc

        latency_ms = (time.perf_counter() - started_at) * 1000
        logger.info(
            "Persisted telemetry event packet_type=%s source_ip=%s latency_ms=%.2f",
            packet_type,
            event.source_ip,
            latency_ms,
        )
        return {"status": "ok", "packet_type": packet_type}

    return app


app = create_app()
