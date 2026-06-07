package main

import (
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/nikhilbajaj98/signal-snitch/agent/parsers"
)

func TestTelemetryJSONIncludesProtocolAndRoute(t *testing.T) {
	telemetry := Telemetry{
		Protocol:  "KAFKA",
		Route:     "orders",
		Sender:    "127.0.0.1:54321",
		Receiver:  "127.0.0.1:9092",
		Payload:   `{"ok":true}`,
		BytesSize: 42,
	}

	encoded, err := json.Marshal(telemetry)
	if err != nil {
		t.Fatalf("expected marshal to succeed, got error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("expected unmarshal to succeed, got error: %v", err)
	}

	if decoded["protocol"] != "KAFKA" {
		t.Fatalf("expected protocol KAFKA, got: %v", decoded["protocol"])
	}
	if decoded["route"] != "orders" {
		t.Fatalf("expected route orders, got: %v", decoded["route"])
	}
}

func TestParserFactoryKafkaWithJSON(t *testing.T) {
	f := parsers.NewFactory()
	event, ok := f.Parse(parsers.ParseInput{
		SrcPort:    54321,
		DstPort:    9092,
		TCPPayload: []byte(`{"ok":true}`),
	})
	if !ok || event.Protocol != "KAFKA" {
		t.Fatalf("expected kafka event, got ok=%v protocol=%s", ok, event.Protocol)
	}
}

func TestKafkaParserExtractsTopicFromProduceRequest(t *testing.T) {
	// Minimal Produce v3-style header + topic "orders".
	var buf []byte
	writeI16 := func(v int16) { b := make([]byte, 2); binary.BigEndian.PutUint16(b, uint16(v)); buf = append(buf, b...) }
	writeI32 := func(v int32) { b := make([]byte, 4); binary.BigEndian.PutUint32(b, uint32(v)); buf = append(buf, b...) }
	writeStr := func(s string) {
		writeI16(int16(len(s)))
		buf = append(buf, []byte(s)...)
	}

	writeI16(0)    // api_key Produce
	writeI16(3)    // api_version
	writeI32(1)    // correlation_id
	writeStr("c")  // client_id
	writeI16(-1)   // transactional_id null
	writeI16(1)    // required_acks
	writeI32(1000) // timeout
	writeStr("orders")

	p := parsers.NewKafkaParser()
	event, ok := p.Parse(parsers.ParseInput{
		DstPort:    9092,
		TCPPayload: append([]byte{0, 0, 0, 0}, buf...), // frame size prefix
	})
	if !ok {
		t.Fatal("expected parse ok")
	}
	if event.Route != "orders" {
		t.Fatalf("expected route orders, got %s", event.Route)
	}
}

func TestAmqpParserBasicPublishAndBody(t *testing.T) {
	p := parsers.NewAmqpParser()

	exchange := "telemetry.events"
	routingKey := "pulse"
	methodPayload := make([]byte, 0, 64)
	methodPayload = append(methodPayload, 0, 60, 0, 40) // class 60 method 40
	methodPayload = append(methodPayload, 0, 0)          // reserved
	methodPayload = append(methodPayload, byte(len(exchange)))
	methodPayload = append(methodPayload, []byte(exchange)...)
	methodPayload = append(methodPayload, byte(len(routingKey)))
	methodPayload = append(methodPayload, []byte(routingKey)...)

	methodFrame := buildAmqpFrame(1, methodPayload)
	in := parsers.ParseInput{SrcPort: 54321, DstPort: 5672, TCPPayload: methodFrame}

	_, ok := p.Parse(in)
	if ok {
		t.Fatal("method frame should not emit event")
	}

	body := []byte(`{"timestamp":1,"status":"SUCCESS","message":"hi"}`)
	bodyFrame := buildAmqpFrame(3, body)
	in.TCPPayload = bodyFrame
	event, ok := p.Parse(in)
	if !ok {
		t.Fatal("expected body frame event")
	}
	if event.Route != "telemetry.events/pulse" {
		t.Fatalf("unexpected route: %s", event.Route)
	}
	if event.Payload != string(body) {
		t.Fatalf("unexpected payload: %s", event.Payload)
	}
}

func buildAmqpFrame(frameType byte, payload []byte) []byte {
	size := len(payload)
	frame := make([]byte, 0, 7+size+1)
	frame = append(frame, frameType, 0, 1)
	frame = append(frame, byte(size>>24), byte(size>>16), byte(size>>8), byte(size))
	frame = append(frame, payload...)
	frame = append(frame, 0xce)
	return frame
}
