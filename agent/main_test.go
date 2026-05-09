package main

import "testing"

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

