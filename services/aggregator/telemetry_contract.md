# Telemetry Event Contract

`POST /telemetry` accepts an event envelope that supports Kafka traffic today and non-Kafka
packet types over time.

## Request fields

- `source_ip` (string, required): source IP observed by the sniffer.
- `payload` (string, required): raw extracted payload.
- `packet_type` (string, optional): packet label such as `kafka_tcp`, `dns_udp`, `http_tls`.
- `captured_at` (ISO8601 datetime, optional): packet capture timestamp from agent.
- `meta` (object or string, optional): extra metadata (interface, parser version, port, etc).

## Backward compatibility

When `packet_type` is missing or empty, the aggregator defaults it to `kafka_tcp`. This keeps
older agents compatible while enabling newer packet categories.

## ClickHouse mapping

Events are persisted in `telemetry` with these fields:

- `event_time`: uses `captured_at` when provided, otherwise server receive time.
- `ingested_at`: server insertion time (`now64()` default in ClickHouse).
- `packet_type`: normalized packet type label.
- `source_ip`: incoming source IP.
- `payload`: incoming payload.
- `meta_json`: JSON-serialized `meta` payload (or `{}` when omitted).
