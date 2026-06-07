package parsers

import (
	"encoding/json"
	"os"
)

const amqpPort = 5672

const (
	amqpClassBasic  = 60
	amqpMethodPublish = 40

	amqpFrameMethod    = 1
	amqpFrameBody      = 3
	amqpFrameHeartbeat = 4
)

type AmqpParser struct {
	defaultRoute string
	flows        *FlowCache
}

func NewAmqpParser() *AmqpParser {
	route := os.Getenv("AMQP_DEFAULT_ROUTE")
	if route == "" {
		route = "unknown_exchange"
	}
	return &AmqpParser{
		defaultRoute: route,
		flows:        NewFlowCache(),
	}
}

func (p *AmqpParser) Protocol() string {
	return "AMQP"
}

func (p *AmqpParser) Matches(in ParseInput) bool {
	if in.SrcPort == amqpPort || in.DstPort == amqpPort {
		return true
	}
	return len(in.TCPPayload) >= 4 && string(in.TCPPayload[:4]) == "AMQP"
}

func (p *AmqpParser) Parse(in ParseInput) (Event, bool) {
	payload := in.TCPPayload
	if len(payload) == 0 {
		return Event{}, false
	}

	// Protocol header — cache only, do not emit telemetry noise.
	if len(payload) >= 4 && string(payload[:4]) == "AMQP" {
		return Event{}, false
	}

	if len(payload) < 8 {
		return Event{}, false
	}

	frameType := payload[0]
	frameSize := int(payload[3])<<24 | int(payload[4])<<16 | int(payload[5])<<8 | int(payload[6])
	if frameSize < 0 || 7+frameSize > len(payload) {
		return p.fallbackEvent(payload)
	}

	framePayload := payload[7 : 7+frameSize]

	switch frameType {
	case amqpFrameMethod:
		exchange, routingKey, ok := parseBasicPublish(framePayload)
		if !ok {
			return Event{}, false
		}
		route := formatAmqpRoute(exchange, routingKey)
		p.flows.Set(in, route)
		return Event{}, false

	case amqpFrameBody:
		route, ok := p.flows.Get(in)
		if !ok {
			route = p.defaultRoute
		}
		body := string(framePayload)
		if !isPrintablePayload(body) {
			return Event{}, false
		}
		return Event{
			Route:     route,
			Payload:   body,
			BytesSize: len(payload),
		}, true

	case amqpFrameHeartbeat:
		return Event{}, false
	}

	return p.fallbackEvent(payload)
}

func (p *AmqpParser) fallbackEvent(payload []byte) (Event, bool) {
	if len(payload) == 0 {
		return Event{}, false
	}
	return Event{}, false
}

func formatAmqpRoute(exchange, routingKey string) string {
	if exchange == "" && routingKey == "" {
		return ""
	}
	if exchange == "" {
		return routingKey
	}
	if routingKey == "" {
		return exchange
	}
	return exchange + "/" + routingKey
}

func parseBasicPublish(framePayload []byte) (exchange, routingKey string, ok bool) {
	if len(framePayload) < 4 {
		return "", "", false
	}
	classID := int(framePayload[0])<<8 | int(framePayload[1])
	methodID := int(framePayload[2])<<8 | int(framePayload[3])
	if classID != amqpClassBasic || methodID != amqpMethodPublish {
		return "", "", false
	}

	off := 4
	off += 2 // reserved

	exchange, off, ok = readAMQPShortStr(framePayload, off)
	if !ok {
		return "", "", false
	}
	routingKey, _, ok = readAMQPShortStr(framePayload, off)
	if !ok {
		return "", "", false
	}
	return exchange, routingKey, true
}

func readAMQPShortStr(b []byte, off int) (string, int, bool) {
	if off >= len(b) {
		return "", off, false
	}
	ln := int(b[off])
	off++
	if off+ln > len(b) {
		return "", off, false
	}
	return string(b[off : off+ln]), off + ln, true
}

func isPrintablePayload(s string) bool {
	if s == "" {
		return false
	}
	if json.Valid([]byte(s)) {
		return true
	}
	for _, r := range s {
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			return false
		}
	}
	return true
}
