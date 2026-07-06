from __future__ import annotations

from fastapi.testclient import TestClient

from aggregator.app import create_app


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.inserts: list[dict[str, object]] = []
        self.query_results: list[list[object]] = []
        self.queries_run: list[tuple[str, dict[str, object] | None]] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def insert(self, table: str, rows: list[list[object]], column_names: list[str]) -> None:
        self.inserts.append({"table": table, "rows": rows, "column_names": column_names})

    def query(self, sql: str, parameters: dict[str, object] | None = None) -> object:
        self.queries_run.append((sql, parameters))
        class FakeQueryResult:
            def __init__(self, rows: list[list[object]]) -> None:
                self.result_rows = rows
        return FakeQueryResult(self.query_results)


def test_telemetry_persists_new_schema_fields() -> None:
    fake_client = FakeClickHouseClient()
    app = create_app(ch_client_override=fake_client)
    client = TestClient(app)

    response = client.post(
        "/telemetry",
        json={
            "protocol": "KAFKA",
            "route": "orders",
            "sender": "10.0.0.5:54321",
            "receiver": "10.0.0.10:9092",
            "payload": '{"message":"hello"}',
            "bytes_size": 128,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["protocol"] == "KAFKA"
    assert body["route"] == "orders"
    assert body["bytes_size"] == 128

    insert = fake_client.inserts[0]
    assert insert["table"] == "telemetry"
    assert insert["column_names"] == [
        "protocol",
        "route",
        "sender",
        "receiver",
        "payload",
        "bytes_size",
    ]
    assert insert["rows"][0] == [
        "KAFKA",
        "orders",
        "10.0.0.5:54321",
        "10.0.0.10:9092",
        '{"message":"hello"}',
        128,
    ]


def test_telemetry_maps_legacy_packet_type_and_source_ip() -> None:
    fake_client = FakeClickHouseClient()
    app = create_app(ch_client_override=fake_client)
    client = TestClient(app)

    response = client.post(
        "/telemetry",
        json={
            "packet_type": "kafka_tcp",
            "source_ip": "10.0.0.1",
            "payload": '{"message":"legacy"}',
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["protocol"] == "KAFKA"

    insert = fake_client.inserts[0]
    assert insert["rows"][0][0] == "KAFKA"
    assert insert["rows"][0][2] == "10.0.0.1"
    assert insert["rows"][0][5] == len('{"message":"legacy"}'.encode("utf-8"))


def test_telemetry_requires_sender() -> None:
    fake_client = FakeClickHouseClient()
    app = create_app(ch_client_override=fake_client)
    client = TestClient(app)

    response = client.post(
        "/telemetry",
        json={
            "protocol": "AMQP",
            "payload": '{"message":"missing-sender"}',
        },
    )

    assert response.status_code == 422


def test_topology_endpoint() -> None:
    fake_client = FakeClickHouseClient()
    # Mock database rows:
    # [protocol, route, sender, receiver, message_count, total_bytes]
    fake_client.query_results = [
        ["KAFKA", "orders", "10.0.0.5:54321", "10.0.0.10:9092", 10, 1024],
        ["KAFKA", "orders", "10.0.0.5:54322", "10.0.0.10:9092", 5, 512],
        ["AMQP", "my-exchange/pulse", "10.0.0.6:12345", "10.0.0.20:5672", 20, 2048],
        ["KAFKA", "orders", "10.0.0.5:54321", "", 1, 100],  # empty receiver fallback test
    ]

    app = create_app(ch_client_override=fake_client)
    client = TestClient(app)

    response = client.get("/telemetry/topology?window_seconds=300")
    assert response.status_code == 200
    data = response.json()

    # Assert query execution and parameters
    assert len(fake_client.queries_run) == 1
    sql, params = fake_client.queries_run[0]
    assert "timestamp >=" in sql
    assert params == {"window": 300}

    # Assert Nodes
    # Expected Nodes:
    # 1. 10.0.0.5 (Sender, Client type, message_count = 10 + 5 + 1 = 16, bytes = 1536 + 100 = 1636, protocols = ["KAFKA"])
    # 2. route:kafka:orders (Route type, message_count = 16, bytes = 1636)
    # 3. 10.0.0.10 (Receiver, Broker type, message_count = 15, bytes = 1536)
    # 4. broker:kafka (Receiver, Broker type, message_count = 1, bytes = 100, fallback)
    # 5. 10.0.0.6 (Sender, Client type, message_count = 20, bytes = 2048, protocols = ["AMQP"])
    # 6. route:amqp:my-exchange/pulse (Route type, message_count = 20, bytes = 2048)
    # 7. 10.0.0.20 (Receiver, Broker type, message_count = 20, bytes = 2048)
    nodes = {n["id"]: n for n in data["nodes"]}
    assert len(nodes) == 7

    # Check Client 10.0.0.5 (dynamic port stripped!)
    assert nodes["10.0.0.5"]["type"] == "client"
    assert nodes["10.0.0.5"]["message_count"] == 16
    assert nodes["10.0.0.5"]["bytes_size"] == 1636
    assert nodes["10.0.0.5"]["protocols"] == ["KAFKA"]

    # Check Route kafka:orders
    assert nodes["route:kafka:orders"]["type"] == "route"
    assert nodes["route:kafka:orders"]["label"] == "orders"
    assert nodes["route:kafka:orders"]["protocol"] == "KAFKA"
    assert nodes["route:kafka:orders"]["message_count"] == 16
    assert nodes["route:kafka:orders"]["bytes_size"] == 1636

    # Check Receiver 10.0.0.10
    assert nodes["10.0.0.10"]["type"] == "broker"
    assert nodes["10.0.0.10"]["label"] == "10.0.0.10:9092"
    assert nodes["10.0.0.10"]["message_count"] == 15
    assert nodes["10.0.0.10"]["bytes_size"] == 1536

    # Check Fallback Receiver broker:kafka
    assert nodes["broker:kafka"]["type"] == "broker"
    assert nodes["broker:kafka"]["label"] == "KAFKA Broker"
    assert nodes["broker:kafka"]["message_count"] == 1
    assert nodes["broker:kafka"]["bytes_size"] == 100

    # Check Client 10.0.0.6
    assert nodes["10.0.0.6"]["type"] == "client"
    assert nodes["10.0.0.6"]["message_count"] == 20
    assert nodes["10.0.0.6"]["bytes_size"] == 2048
    assert nodes["10.0.0.6"]["protocols"] == ["AMQP"]

    # Assert Edges
    edges = data["edges"]
    assert len(edges) == 5

    edge_map = {(e["source"], e["target"]): e for e in edges}

    assert edge_map[("10.0.0.5", "route:kafka:orders")]["message_count"] == 16
    assert edge_map[("10.0.0.5", "route:kafka:orders")]["bytes_size"] == 1636
    assert edge_map[("10.0.0.5", "route:kafka:orders")]["protocol"] == "KAFKA"

    assert edge_map[("route:kafka:orders", "10.0.0.10")]["message_count"] == 15
    assert edge_map[("route:kafka:orders", "10.0.0.10")]["bytes_size"] == 1536
    assert edge_map[("route:kafka:orders", "10.0.0.10")]["protocol"] == "KAFKA"

    assert edge_map[("route:kafka:orders", "broker:kafka")]["message_count"] == 1
    assert edge_map[("route:kafka:orders", "broker:kafka")]["bytes_size"] == 100
    assert edge_map[("route:kafka:orders", "broker:kafka")]["protocol"] == "KAFKA"

    assert edge_map[("10.0.0.6", "route:amqp:my-exchange/pulse")]["message_count"] == 20
    assert edge_map[("10.0.0.6", "route:amqp:my-exchange/pulse")]["bytes_size"] == 2048
    assert edge_map[("10.0.0.6", "route:amqp:my-exchange/pulse")]["protocol"] == "AMQP"

    assert edge_map[("route:amqp:my-exchange/pulse", "10.0.0.20")]["message_count"] == 20
    assert edge_map[("route:amqp:my-exchange/pulse", "10.0.0.20")]["bytes_size"] == 2048
    assert edge_map[("route:amqp:my-exchange/pulse", "10.0.0.20")]["protocol"] == "AMQP"


def test_topology_endpoint_no_window() -> None:
    fake_client = FakeClickHouseClient()
    fake_client.query_results = [
        ["KAFKA", "orders", "10.0.0.5", "10.0.0.10", 5, 500]
    ]

    app = create_app(ch_client_override=fake_client)
    client = TestClient(app)

    response = client.get("/telemetry/topology?window_seconds=0")
    assert response.status_code == 200
    data = response.json()

    assert len(fake_client.queries_run) == 1
    sql, params = fake_client.queries_run[0]
    assert "timestamp >=" not in sql
    assert params == {}


def test_analyze_endpoint_offline_fallback() -> None:
    fake_client = FakeClickHouseClient()
    fake_client.query_results = [
        ['{"timestamp": 123, "status": "SUCCESS", "message": "Test"}'],
        ['{"timestamp": 124, "status": "FAILED", "message": "Error"}'],
    ]

    app = create_app(ch_client_override=fake_client)
    client = TestClient(app)

    response = client.post("/telemetry/analyze?topic=route:kafka:orders")
    assert response.status_code == 200
    data = response.json()
    assert "is_anomaly" in data
    assert "suspected_cause" in data
    assert "suggested_fix" in data
    assert data["is_anomaly"] is True
    assert "High failure rate detected" in data["suspected_cause"]


