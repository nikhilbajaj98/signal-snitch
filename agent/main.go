package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
)

var jsonRegex = regexp.MustCompile(`\{[^{}]*\}`)

type Telemetry struct {
	SourceIP string `json:"source_ip"`
	Payload  string `json:"payload"`
}

func reportToBrain(sourceIp string, payload string) {
	url := "http://localhost:8000/telemetry"

	data := Telemetry{
		SourceIP: sourceIp,
		Payload:  payload,
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
	var wifi *pcap.Interface
	for i := range devices {
		if devices[i].Name == "lo0" {
			wifi = &devices[i]
			break
		}
	}
	if wifi == nil {
		log.Fatal("No WiFi device found")
	}
	fmt.Printf("Using device: %s, Device description: %s\n", wifi.Name, wifi.Description)

	handle, err := pcap.OpenLive(wifi.Name, 1024, true, 30*time.Second)
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
		reportToBrain(sourceIp, payload)
	}
}
