package parsers

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"regexp"
)

const kafkaPort = 9092

const kafkaAPIKeyProduce int16 = 0

var jsonRegex = regexp.MustCompile(`\{[^{}]*\}`)

type KafkaParser struct {
	defaultRoute string
}

func NewKafkaParser() *KafkaParser {
	route := os.Getenv("KAFKA_DEFAULT_ROUTE")
	if route == "" {
		route = "orders"
	}
	return &KafkaParser{defaultRoute: route}
}

func (p *KafkaParser) Protocol() string {
	return "KAFKA"
}

func (p *KafkaParser) Matches(in ParseInput) bool {
	return in.SrcPort == kafkaPort || in.DstPort == kafkaPort
}

func (p *KafkaParser) Parse(in ParseInput) (Event, bool) {
	payload := in.TCPPayload
	if len(payload) == 0 {
		return Event{}, false
	}

	route := extractKafkaTopic(payload)
	if route == "" {
		route = p.defaultRoute
	}

	match := jsonRegex.Find(payload)
	if len(match) == 0 || !json.Valid(match) {
		// Emit produce metadata even when JSON value is not visible in this segment.
		if route != p.defaultRoute || hasProduceRequest(payload) {
			return Event{
				Route:     route,
				Payload:   "",
				BytesSize: len(payload),
			}, route != ""
		}
		return Event{}, false
	}

	return Event{
		Route:     route,
		Payload:   string(match),
		BytesSize: len(payload),
	}, true
}

func hasProduceRequest(payload []byte) bool {
	return extractKafkaTopic(payload) != ""
}

func extractKafkaTopic(payload []byte) string {
	for _, start := range []int{0, 4} {
		if topic, ok := tryParseProduceAt(payload, start); ok {
			return topic
		}
	}
	if topic, ok := scanLengthPrefixedTopic(payload); ok {
		return topic
	}
	return ""
}

func tryParseProduceAt(payload []byte, start int) (string, bool) {
	if start+10 > len(payload) {
		return "", false
	}
	apiKey := int16(binary.BigEndian.Uint16(payload[start : start+2]))
	if apiKey != kafkaAPIKeyProduce {
		return "", false
	}
	apiVersion := int16(binary.BigEndian.Uint16(payload[start+2 : start+4]))
	off := start + 4 + 4 // correlation_id

	clientIDLen := int16(binary.BigEndian.Uint16(payload[off : off+2]))
	off += 2
	if clientIDLen < 0 || off+int(clientIDLen) > len(payload) {
		return "", false
	}
	off += int(clientIDLen)

	return parseProduceBody(payload, off, apiVersion)
}

func parseProduceBody(payload []byte, off int, apiVersion int16) (string, bool) {
	switch {
	case apiVersion <= 2:
		return parseProduceBodyV0to2(payload, off)
	case apiVersion >= 3 && apiVersion <= 9:
		return parseProduceBodyV3Plus(payload, off)
	default:
		return parseProduceBodyV0to2(payload, off)
	}
}

func parseProduceBodyV0to2(payload []byte, off int) (string, bool) {
	if off+6 > len(payload) {
		return "", false
	}
	off += 2 // required_acks
	off += 4 // timeout
	return readKafkaString(payload, off)
}

func parseProduceBodyV3Plus(payload []byte, off int) (string, bool) {
	if off+6 > len(payload) {
		return "", false
	}
	// transactional_id nullable string (int16 length, -1 = null)
	txLen := int16(binary.BigEndian.Uint16(payload[off : off+2]))
	off += 2
	if txLen >= 0 {
		off += int(txLen)
	}
	if off+6 > len(payload) {
		return "", false
	}
	off += 2 // required_acks
	off += 4 // timeout
	return readKafkaString(payload, off)
}

func readKafkaString(payload []byte, off int) (string, bool) {
	if off+2 > len(payload) {
		return "", false
	}
	strLen := int16(binary.BigEndian.Uint16(payload[off : off+2]))
	if strLen < 0 {
		return "", false
	}
	off += 2
	if off+int(strLen) > len(payload) {
		return "", false
	}
	return string(payload[off : off+int(strLen)]), true
}

func scanLengthPrefixedTopic(payload []byte) (string, bool) {
	// Heuristic: find int16(len) + "orders" style topic names (3..64 chars).
	for i := 0; i+4 < len(payload); i++ {
		strLen := int(binary.BigEndian.Uint16(payload[i : i+2]))
		if strLen < 3 || strLen > 64 || i+2+strLen > len(payload) {
			continue
		}
		candidate := string(payload[i+2 : i+2+strLen])
		if isValidTopicName(candidate) {
			return candidate, true
		}
	}
	return "", false
}

func isValidTopicName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '.' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}
