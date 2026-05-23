package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
)

var jsonRegex = regexp.MustCompile(`\{[^{}]*\}`)

type Telemetry struct {
	PacketType string            `json:"packet_type,omitempty"`
	SourceIP   string            `json:"source_ip"`
	Payload    string            `json:"payload"`
	CapturedAt string            `json:"captured_at,omitempty"`
	Meta       map[string]string `json:"meta,omitempty"`
}

func reportToBrain(sourceIp string, payload string, packetType string) {
	url := os.Getenv("AGGREGATOR_URL")
	if url == "" {
		url = "http://localhost:8000/telemetry"
	}

	data := Telemetry{
		PacketType: packetType,
		SourceIP:   sourceIp,
		Payload:    payload,
		CapturedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Meta: map[string]string{
			"sniffer": "go-agent",
		},
	}

	jsonData, err := json.Marshal(data)
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

	handle, err := pcap.OpenLive(sniffInterface.Name, 1024, true, 30*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer handle.Close()

	handle.SetBPFFilter("tcp port 9092")

	fmt.Println("Sniffing packets...")
	packet_source := gopacket.NewPacketSource(handle, handle.LinkType())
	for packet := range packet_source.Packets() {
		processPacket(packet)
	}
}

func processPacket(packet gopacket.Packet) {

	var sourceIp string
	var payload string
	tcpLayer := packet.Layer(layers.LayerTypeTCP)
	if tcpLayer != nil {
		tcp, _ := tcpLayer.(*layers.TCP)
		tcpPayload := tcp.Payload

		match := jsonRegex.Find(tcpPayload)
		if len(match) > 0 && json.Valid(match) {
			fmt.Printf("🎯 [DATA DETECTED]: %s\n", string(match))
			payload = string(match)
		}
	}

	ipLayer := packet.Layer(layers.LayerTypeIPv4)
	if ipLayer != nil {
		ip, _ := ipLayer.(*layers.IPv4)
		fmt.Printf("🎯 [IP DETECTED]: %s\n", ip.SrcIP.String())
		sourceIp = ip.SrcIP.String()
	}

	if sourceIp != "" && payload != "" {
		reportToBrain(sourceIp, payload, "kafka_tcp")
	}
}
