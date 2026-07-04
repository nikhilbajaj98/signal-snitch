package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/nikhilbajaj98/signal-snitch/agent/parsers"
)

type Telemetry struct {
	Protocol  string `json:"protocol"`
	Route     string `json:"route"`
	Sender    string `json:"sender"`
	Receiver  string `json:"receiver"`
	Payload   string `json:"payload"`
	BytesSize int    `json:"bytes_size"`
}

var parserFactory = parsers.NewFactory()

func reportToBrain(event Telemetry) {
	url := os.Getenv("AGGREGATOR_URL")
	if url == "" {
		url = "http://localhost:8000/telemetry"
	}

	jsonData, err := json.Marshal(event)
	if err != nil {
		log.Println("Error marshalling data:", err)
		return
	}

	response, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Println("Error sending request:", err)
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		log.Printf("Error: %s\n", response.Status)
		return
	}

	log.Println("Data sent successfully")
}

func endpoint(ip net.IP, port uint16) string {
	return net.JoinHostPort(ip.String(), strconv.Itoa(int(port)))
}

func defaultBPFFilter() string {
	if filter := os.Getenv("SNIFF_BPF_FILTER"); filter != "" {
		return filter
	}
	return "tcp port 9092 or tcp port 5672"
}

func main() {
	fmt.Println("Finding all devices...")
	devices, err := pcap.FindAllDevs()
	if err != nil {
		log.Fatal(err)
	}
	targetInterface := os.Getenv("SNIFF_INTERFACE")
	if targetInterface == "" {
		targetInterface = "lo0"
	}
	var sniffInterface *pcap.Interface
	for i := range devices {
		if devices[i].Name == targetInterface {
			sniffInterface = &devices[i]
			break
		}
	}
	if sniffInterface == nil {
		if len(devices) == 0 {
			log.Fatal("No network devices found for packet sniffing")
		}
		log.Printf("Interface %s not found; falling back to %s", targetInterface, devices[0].Name)
		sniffInterface = &devices[0]
	}
	fmt.Printf("Using device: %s, Device description: %s\n", sniffInterface.Name, sniffInterface.Description)

	snaplen := 65535
	if v := os.Getenv("SNIFF_SNAPLEN"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			snaplen = n
		}
	}

	handle, err := pcap.OpenLive(sniffInterface.Name, int32(snaplen), true, 30*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer handle.Close()

	bpfFilter := defaultBPFFilter()
	if err := handle.SetBPFFilter(bpfFilter); err != nil {
		log.Fatalf("Failed to set BPF filter %q: %v", bpfFilter, err)
	}
	log.Printf("Sniffing with BPF filter: %s", bpfFilter)

	fmt.Println("Sniffing packets...")
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	for packet := range packetSource.Packets() {
		processPacket(packet)
	}
}

func processPacket(packet gopacket.Packet) {
	tcpLayer := packet.Layer(layers.LayerTypeTCP)
	ipLayer := packet.Layer(layers.LayerTypeIPv4)
	if tcpLayer == nil || ipLayer == nil {
		return
	}

	tcp, _ := tcpLayer.(*layers.TCP)
	ip, _ := ipLayer.(*layers.IPv4)
	if len(tcp.Payload) == 0 {
		return
	}

	in := parsers.ParseInput{
		SrcIP:      ip.SrcIP,
		DstIP:      ip.DstIP,
		SrcPort:    uint16(tcp.SrcPort),
		DstPort:    uint16(tcp.DstPort),
		TCPPayload: tcp.Payload,
	}

	event, ok := parserFactory.Parse(in)
	if !ok {
		return
	}

	sender := endpoint(ip.SrcIP, uint16(tcp.SrcPort))
	receiver := endpoint(ip.DstIP, uint16(tcp.DstPort))

	if event.Protocol == "KAFKA" {
		dstPortStr := strconv.Itoa(int(tcp.DstPort))
		if !strings.HasSuffix(receiver, ":"+dstPortStr) {
			receiver = fmt.Sprintf("%s:%s", ip.DstIP.String(), dstPortStr)
		}
		if tcp.DstPort == 9092 || tcp.DstPort == 29092 {
			receiver = fmt.Sprintf("%s:%d", ip.DstIP.String(), tcp.DstPort)
		} else if tcp.SrcPort == 9092 || tcp.SrcPort == 29092 {
			sender = fmt.Sprintf("%s:%d", ip.SrcIP.String(), tcp.SrcPort)
		}
	}

	fmt.Printf("🎯 [%s] route=%s payload=%s\n", event.Protocol, event.Route, event.Payload)
	fmt.Printf("🎯 [FLOW DETECTED]: %s -> %s\n", sender, receiver)

	reportToBrain(Telemetry{
		Protocol:  event.Protocol,
		Route:     event.Route,
		Sender:    sender,
		Receiver:  receiver,
		Payload:   event.Payload,
		BytesSize: event.BytesSize,
	})
}
