package main

import (
	"encoding/json"
	"testing"
)

func TestJSONRegexExtractsProducerPayload(t *testing.T) {
	// Simulate Kafka TCP payload having extra framing/bytes around JSON.
	payload := []byte{0x00, 0x01, 0x02, 'x', 'y', 'z', '{',
		'"', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '"', ':', '1', '2', '3', ',',
		'"', 's', 't', 'a', 't', 'u', 's', '"', ':', '"', 'S', 'U', 'C', 'C', 'E', 'S', 'S', '"', ',',
		'"', 'm', 'e', 's', 's', 'a', 'g', 'e', '"', ':', '"', 'H', 'e', 'l', 'l', 'o', ',', ' ', 'w', 'o', 'r', 'l', 'd', '!', '"',
		'}', 0x00, 0xff}

	match := jsonRegex.Find(payload)
	if len(match) == 0 {
		t.Fatalf("expected JSON match, got none")
	}

	want := `{"timestamp":123,"status":"SUCCESS","message":"Hello, world!"}`
	if string(match) != want {
		t.Fatalf("unexpected match.\nwant: %s\ngot:  %s", want, string(match))
	}
}

func TestTelemetryJSONIncludesPacketType(t *testing.T) {
	telemetry := Telemetry{
		PacketType: "kafka_tcp",
		SourceIP:   "127.0.0.1",
		Payload:    `{"ok":true}`,
	}

	encoded, err := json.Marshal(telemetry)
	if err != nil {
		t.Fatalf("expected marshal to succeed, got error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("expected unmarshal to succeed, got error: %v", err)
	}

	if decoded["packet_type"] != "kafka_tcp" {
		t.Fatalf("expected packet_type to be kafka_tcp, got: %v", decoded["packet_type"])
	}
}

