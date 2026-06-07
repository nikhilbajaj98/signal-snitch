package parsers

import "net"

// ParseInput is the normalized L4 payload passed to protocol parsers.
type ParseInput struct {
	SrcIP      net.IP
	DstIP      net.IP
	SrcPort    uint16
	DstPort    uint16
	TCPPayload []byte
}

// Event is a parsed telemetry record before endpoint enrichment in main.
type Event struct {
	Protocol  string
	Route     string
	Payload   string
	BytesSize int
}
