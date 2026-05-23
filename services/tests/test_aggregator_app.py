from fastapi.testclient import TestClient

from aggregator.app import create_app


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.inserts: list[dict[str, object]] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def insert(self, table: str, rows: list[list[object]], column_names: list[str]) -> None:
        self.inserts.append({"table": table, "rows": rows, "column_names": column_names})


def test_telemetry_defaults_packet_type_for_legacy_payload() -> None:
    fake_client = FakeClickHouseClient()
    app = create_app(ch_client_override=fake_client)
    client = TestClient(app)

    response = client.post(
        "/telemetry",
        json={
            "source_ip": "10.0.0.1",
            "payload": '{"message":"legacy"}',
        },
    )

    assert response.status_code == 200
    assert response.json()["packet_type"] == "kafka_tcp"
    assert len(fake_client.inserts) == 1
    insert = fake_client.inserts[0]
    assert insert["table"] == "telemetry"
    assert insert["column_names"] == [
        "event_time",
        "packet_type",
        "source_ip",
        "payload",
        "meta_json",
    ]
    assert insert["rows"][0][1] == "kafka_tcp"


def test_telemetry_stores_explicit_packet_type_and_meta() -> None:
    fake_client = FakeClickHouseClient()
    app = create_app(ch_client_override=fake_client)
    client = TestClient(app)

    response = client.post(
        "/telemetry",
        json={
            "packet_type": "dns_udp",
            "source_ip": "10.0.0.2",
            "payload": '{"message":"dns-event"}',
            "meta": {"port": 53, "parser_version": "v1"},
        },
    )

    assert response.status_code == 200
    assert response.json()["packet_type"] == "dns_udp"
    assert len(fake_client.inserts) == 1
    insert = fake_client.inserts[0]
    assert insert["rows"][0][1] == "dns_udp"
    assert insert["rows"][0][4] == '{"port": 53, "parser_version": "v1"}'
