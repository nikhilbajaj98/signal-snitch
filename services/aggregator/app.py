from __future__ import annotations

from contextlib import asynccontextmanager
import json
import logging
import os
import time
from typing import Any

import clickhouse_connect
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field, model_validator

try:
    from google import genai
    from google.genai import types
except ImportError:
    genai = None
    types = None


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aggregator")

DEFAULT_PROTOCOL = "KAFKA"
DEFAULT_ROUTE = ""


class IncidentReport(BaseModel):
    is_anomaly: bool = Field(description="True if there is sudden schema drift, missing keys, structural errors, or system issues.")
    suspected_cause: str = Field(description="Concise system analysis explaining what changed or broke across the stream.")
    suggested_fix: str = Field(description="Highly practical, terminal-ready remediation instructions.")


def _heuristic_analysis(topic: str, payloads: list[str], error_msg: str | None = None) -> IncidentReport:
    """Fallback deterministic inspection when LLM engine is unreachable or offline."""
    error_count = 0
    invalid_json = 0
    sample_keys: set[str] = set()

    for p in payloads:
        try:
            data = json.loads(p)
            if isinstance(data, dict):
                sample_keys.update(str(k) for k in data.keys())
                status = str(data.get("status", "")).upper()
                if status in ("FAILED", "ERROR", "CRITICAL", "500"):
                    error_count += 1
        except Exception:
            invalid_json += 1

    if invalid_json > 0:
        return IncidentReport(
            is_anomaly=True,
            suspected_cause=f"Schema corruption detected: {invalid_json}/{len(payloads)} packets contain malformed or unparseable JSON frames on stream '{topic}'.",
            suggested_fix="Check producer serialization formatting (`docker compose logs producer amqp-producer --tail 50`) and verify utf-8 encoding."
        )
    if error_count > 0:
        return IncidentReport(
            is_anomaly=True,
            suspected_cause=f"High failure rate detected: {error_count}/{len(payloads)} payloads report an explicit FAILED or ERROR runtime status in stream '{topic}'.",
            suggested_fix="Inspect upstream service error handlers (`docker compose logs producer amqp-producer --tail 50`) and check database transaction constraints."
        )

    notice = f" (Offline fallback evaluation used: {error_msg})" if error_msg else ""
    keys_str = ", ".join(sorted(sample_keys)[:5]) if sample_keys else "raw_payload"
    return IncidentReport(
        is_anomaly=False,
        suspected_cause=f"Stream '{topic}' evaluated across {len(payloads)} packets. Schema keys ({keys_str}) conform to expected telemetry contracts.{notice}",
        suggested_fix="No remediation required. All monitored microservice pipelines are operating within nominal thresholds."
    )



class TelemetryEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    protocol: str | None = Field(default=None)
    route: str | None = Field(default=None)
    sender: str | None = Field(default=None)
    receiver: str | None = Field(default=None)
    payload: str
    bytes_size: int | None = Field(default=None)

    # Legacy fields from earlier agents (mapped during validation).
    packet_type: str | None = Field(default=None)
    source_ip: str | None = Field(default=None)

    @model_validator(mode="after")
    def apply_legacy_aliases(self) -> TelemetryEvent:
        if not self.sender and self.source_ip:
            self.sender = self.source_ip
        if not self.protocol and self.packet_type:
            legacy = self.packet_type.strip().lower()
            if legacy in {"kafka", "kafka_tcp"}:
                self.protocol = "KAFKA"
            else:
                self.protocol = legacy.upper()
        return self

    def normalized_protocol(self) -> str:
        if self.protocol and self.protocol.strip():
            return self.protocol.strip().upper()
        return DEFAULT_PROTOCOL

    def normalized_route(self) -> str:
        if self.route is not None:
            return self.route.strip()
        return DEFAULT_ROUTE

    def normalized_sender(self) -> str:
        if self.sender and self.sender.strip():
            return self.sender.strip()
        raise ValueError("sender is required")

    def normalized_receiver(self) -> str:
        if self.receiver is not None:
            return self.receiver.strip()
        return ""

    def normalized_bytes_size(self) -> int:
        if self.bytes_size is not None and self.bytes_size >= 0:
            return self.bytes_size
        return len(self.payload.encode("utf-8"))


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
    timestamp DateTime64(3) DEFAULT now64(),
    protocol LowCardinality(String),
    route String,
    sender String,
    receiver String,
    payload String,
    bytes_size UInt32
) ENGINE = MergeTree()
ORDER BY (protocol, route, timestamp);
"""
    )
    ch_client.command(
        "ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS protocol LowCardinality(String)"
    )
    ch_client.command("ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS route String")
    ch_client.command("ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS sender String")
    ch_client.command("ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS receiver String")
    ch_client.command("ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS bytes_size UInt32")


def normalize_node_id(endpoint: str) -> str:
    if not endpoint:
        return ""
    if ":" in endpoint:
        parts = endpoint.rsplit(":", 1)
        if parts[1].isdigit():
            return parts[0]
    return endpoint


def create_app(ch_client_override: Any | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.ch_client = ch_client_override
        if app.state.ch_client is None:
            app.state.ch_client = get_clickhouse_client()
        ensure_telemetry_table(app.state.ch_client)
        logger.info("Connected to ClickHouse and telemetry schema is ready")
        yield
        close_fn = getattr(app.state.ch_client, "close", None)
        if callable(close_fn):
            close_fn()
            logger.info("Closed ClickHouse connection")

    app = FastAPI(lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state.ch_client = ch_client_override

    @app.post("/telemetry")
    async def telemetry(event: TelemetryEvent) -> dict[str, str | int]:
        started_at = time.perf_counter()
        try:
            protocol = event.normalized_protocol()
            route = event.normalized_route()
            sender = event.normalized_sender()
            receiver = event.normalized_receiver()
            bytes_size = event.normalized_bytes_size()
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        if app.state.ch_client is None:
            raise HTTPException(status_code=503, detail="clickhouse client is not initialized")

        try:
            app.state.ch_client.insert(
                "telemetry",
                [[protocol, route, sender, receiver, event.payload, bytes_size]],
                column_names=["protocol", "route", "sender", "receiver", "payload", "bytes_size"],
            )
        except Exception as exc:
            latency_ms = (time.perf_counter() - started_at) * 1000
            logger.exception(
                "Failed to persist telemetry protocol=%s route=%s sender=%s latency_ms=%.2f error=%s",
                protocol,
                route,
                sender,
                latency_ms,
                exc,
            )
            raise HTTPException(status_code=503, detail="failed to persist telemetry event") from exc

        latency_ms = (time.perf_counter() - started_at) * 1000
        logger.info(
            "Persisted telemetry protocol=%s route=%s sender=%s bytes_size=%d latency_ms=%.2f",
            protocol,
            route,
            sender,
            bytes_size,
            latency_ms,
        )
        return {
            "status": "ok",
            "protocol": protocol,
            "route": route,
            "bytes_size": bytes_size,
        }

    @app.get("/telemetry/topology")
    async def topology(window_seconds: int | None = 900) -> dict[str, Any]:
        if app.state.ch_client is None:
            raise HTTPException(status_code=503, detail="clickhouse client is not initialized")

        query_params = {}
        if window_seconds is not None and window_seconds > 0:
            query = """
SELECT
    protocol,
    route,
    sender,
    receiver,
    count() AS message_count,
    sum(bytes_size) AS total_bytes
FROM telemetry
WHERE timestamp >= now64() - toIntervalSecond(%(window)s)
  AND route NOT IN ('telemetry.events', 'signal-snitch-internal')
GROUP BY protocol, route, sender, receiver
"""
            query_params["window"] = window_seconds
        else:
            query = """
SELECT
    protocol,
    route,
    sender,
    receiver,
    count() AS message_count,
    sum(bytes_size) AS total_bytes
FROM telemetry
WHERE route NOT IN ('telemetry.events', 'signal-snitch-internal')
GROUP BY protocol, route, sender, receiver
"""

        try:
            result = app.state.ch_client.query(query, parameters=query_params)
        except Exception as exc:
            logger.exception("Failed to query telemetry topology from ClickHouse: %s", exc)
            raise HTTPException(status_code=503, detail="failed to query telemetry topology") from exc

        nodes: dict[str, dict[str, Any]] = {}
        edges: dict[tuple[str, str, str], dict[str, Any]] = {}

        for row in result.result_rows:
            # row: [protocol, route, sender, receiver, message_count, total_bytes]
            protocol = str(row[0])
            route_name = str(row[1])
            sender = str(row[2])
            receiver = str(row[3])
            msg_count = int(row[4])
            total_bytes = int(row[5])

            sender_id = normalize_node_id(sender)
            receiver_id = normalize_node_id(receiver) if receiver else f"broker:{protocol.lower()}"
            route_id = f"route:{protocol.lower()}:{route_name}"

            # Sender node
            if sender_id not in nodes:
                nodes[sender_id] = {
                    "id": sender_id,
                    "label": sender_id,
                    "type": "client",
                    "message_count": 0,
                    "bytes_size": 0,
                    "protocols": set(),
                }
            nodes[sender_id]["message_count"] += msg_count
            nodes[sender_id]["bytes_size"] += total_bytes
            nodes[sender_id]["protocols"].add(protocol)

            # Route node
            if route_id not in nodes:
                nodes[route_id] = {
                    "id": route_id,
                    "label": route_name,
                    "type": "route",
                    "protocol": protocol,
                    "message_count": 0,
                    "bytes_size": 0,
                    "protocols": set(),
                }
            nodes[route_id]["message_count"] += msg_count
            nodes[route_id]["bytes_size"] += total_bytes
            nodes[route_id]["protocols"].add(protocol)

            # Receiver node
            if receiver_id not in nodes:
                nodes[receiver_id] = {
                    "id": receiver_id,
                    "label": receiver if receiver else f"{protocol} Broker",
                    "type": "broker",
                    "protocol": protocol,
                    "message_count": 0,
                    "bytes_size": 0,
                    "protocols": set(),
                }
            else:
                if nodes[receiver_id]["type"] == "client":
                    nodes[receiver_id]["type"] = "broker"
                if receiver and ":" in receiver and ":" not in nodes[receiver_id]["label"]:
                    nodes[receiver_id]["label"] = receiver
                if "protocol" not in nodes[receiver_id]:
                    nodes[receiver_id]["protocol"] = protocol
            nodes[receiver_id]["message_count"] += msg_count
            nodes[receiver_id]["bytes_size"] += total_bytes
            nodes[receiver_id]["protocols"].add(protocol)

            # Edge from Sender to Route
            edge1_key = (sender_id, route_id, protocol)
            if edge1_key not in edges:
                edges[edge1_key] = {
                    "source": sender_id,
                    "target": route_id,
                    "protocol": protocol,
                    "message_count": 0,
                    "bytes_size": 0,
                }
            edges[edge1_key]["message_count"] += msg_count
            edges[edge1_key]["bytes_size"] += total_bytes

            # Edge from Route to Receiver
            edge2_key = (route_id, receiver_id, protocol)
            if edge2_key not in edges:
                edges[edge2_key] = {
                    "source": route_id,
                    "target": receiver_id,
                    "protocol": protocol,
                    "message_count": 0,
                    "bytes_size": 0,
                }
            edges[edge2_key]["message_count"] += msg_count
            edges[edge2_key]["bytes_size"] += total_bytes

        # Format protocols from set to list
        nodes_list = []
        for node_id, node_data in nodes.items():
            data = node_data.copy()
            if "protocols" in data:
                data["protocols"] = sorted(list(data["protocols"]))
            nodes_list.append(data)

        edges_list = list(edges.values())

        return {
            "nodes": nodes_list,
            "edges": edges_list,
        }

    @app.post("/telemetry/analyze", response_model=IncidentReport)
    async def analyze_telemetry(topic: str) -> IncidentReport | dict[str, Any]:
        if not topic:
            raise HTTPException(status_code=400, detail="topic parameter is required")

        clean_topic = topic
        if clean_topic.startswith("route:"):
            parts = clean_topic.split(":", 2)
            if len(parts) == 3:
                clean_topic = parts[2]

        if app.state.ch_client is None:
            return IncidentReport(
                is_anomaly=False,
                suspected_cause="ClickHouse database client is not initialized in the aggregator runtime.",
                suggested_fix="Check backend startup logs and ClickHouse environment credentials."
            )

        try:
            query = """
SELECT payload
FROM telemetry
WHERE route IN (%(clean)s, %(raw)s)
ORDER BY timestamp DESC
LIMIT 15
"""
            result = app.state.ch_client.query(query, parameters={"clean": clean_topic, "raw": topic})
            payloads = [str(row[0]) for row in result.result_rows if row and row[0]]
        except Exception as exc:
            logger.exception("Failed to query ClickHouse for topic=%s: %s", topic, exc)
            return IncidentReport(
                is_anomaly=False,
                suspected_cause=f"Database connectivity or query execution failed while inspecting stream '{clean_topic}': {exc}",
                suggested_fix="Check ClickHouse database health (`docker compose ps clickhouse`) and verify network availability."
            )

        if not payloads:
            return IncidentReport(
                is_anomaly=False,
                suspected_cause=f"No active traffic or payload logs found for stream '{clean_topic}' in the telemetry database.",
                suggested_fix="Ensure the producer container is running (`docker compose ps`) and actively broadcasting messages to this route."
            )

        try:
            if genai is None or types is None:
                raise RuntimeError("google-genai package is not imported or installed.")
            api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
            if not api_key:
                return _heuristic_analysis(clean_topic, payloads, error_msg="No GEMINI_API_KEY configured in environment")

            client = genai.Client(api_key=api_key)
            prompt = (
                f"You are an automated On-Call Site Reliability Engineer (SRE) inspecting live network packet payloads for topic/route '{clean_topic}'.\n"
                "Analyze the following 15 recent telemetry payload JSON strings captured by our packet sniffer.\n"
                "Determine if there is any sudden schema drift, structural anomalies, missing keys, error statuses, or stream degradation.\n"
                "Provide a structured IncidentReport with actionable, terminal-ready remediation steps.\n\n"
                "Recent Payloads:\n" + "\n".join(f"- {p}" for p in payloads)
            )
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=IncidentReport,
                    temperature=0.1,
                ),
            )
            if hasattr(response, "parsed") and response.parsed is not None:
                if isinstance(response.parsed, IncidentReport):
                    return response.parsed
                if isinstance(response.parsed, dict):
                    return IncidentReport(**response.parsed)
            if response.text:
                return IncidentReport.model_validate_json(response.text)
            raise RuntimeError("Empty response from Gemini AI model")
        except Exception as exc:
            logger.exception("AI diagnostic engine failed for topic=%s: %s", clean_topic, exc)
            return _heuristic_analysis(clean_topic, payloads, error_msg=str(exc))

    return app


app = create_app()
