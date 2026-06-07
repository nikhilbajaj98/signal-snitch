# Telemetry Event Contract

`POST /telemetry` accepts a protocol-agnostic event envelope for Kafka today and other
protocols (AMQP, Redis, etc.) over time.

## Request fields

- `protocol` (string, required for new agents): e.g. `KAFKA`, `AMQP`, `REDIS`.
- `route` (string, optional): topic, exchange, queue, or channel name.
- `sender` (string, required): source endpoint such as `IP:Port` or client ID.
- `receiver` (string, optional): destination broker/server endpoint.
- `payload` (string, required): extracted raw payload (JSON/text).
- `bytes_size` (integer, optional): packet size for throughput metrics; defaults to UTF-8
  byte length of `payload` when omitted.

## Agent parsing

The Go agent uses a parser factory (`agent/parsers`) with one sniffer sidecar per broker:

- `go-agent-kafka` — BPF `tcp port 9092`, parses **Produce** requests for topic name
- `go-agent-amqp` — BPF `tcp port 5672`, parses **`basic.publish`** for `exchange/routing_key`

### Route semantics

| Protocol | `route` value | Example |
|----------|----------------|---------|
| KAFKA | Kafka topic name | `orders` |
| AMQP | `exchange/routing_key` | `telemetry.events/pulse` |

Env fallbacks when wire parse fails: `KAFKA_DEFAULT_ROUTE`, `AMQP_DEFAULT_ROUTE`.

## Legacy compatibility

Older agents may still send:

- `packet_type` (mapped to `protocol`, with `kafka_tcp` -> `KAFKA`)
- `source_ip` (mapped to `sender`)

## ClickHouse mapping

Events are persisted in `telemetry` with these fields:

- `timestamp`: server insertion time (`now64()` default in ClickHouse).
- `protocol`: normalized uppercase protocol label.
- `route`: topic / exchange / queue name.
- `sender`: source node identifier.
- `receiver`: destination node identifier.
- `payload`: extracted payload.
- `bytes_size`: packet size (`UInt32`).

Table ordering key: `(protocol, route, timestamp)`.
