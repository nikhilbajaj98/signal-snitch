"use client";

import React, { useState, useEffect, useRef } from "react";
import MetricHeader from "@/components/MetricHeader";
import TopologyMap from "@/components/TopologyMap";
import InspectorSidebar from "@/components/InspectorSidebar";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

// --- MOCK PAYLOAD GENERATOR FOR CLIENT-SIDE TERMINAL CACHE ---
const generateMockPayload = (protocol) => {
  const timestamp = new Date().toTimeString().split(" ")[0];
  let payloadStr = "";
  
  if (protocol === "KAFKA") {
    const orderIds = ["30928", "10283", "99182", "40192", "88271"];
    const users = ["usr_alpha", "usr_beta", "usr_gamma", "usr_delta", "usr_epsilon"];
    const products = ["Laptop Pro", "UltraWide Monitor", "Mechanical Keyboard", "Wireless Mouse", "Noise-Cancelling Headset"];
    const prices = [1499.99, 389.50, 119.00, 59.20, 199.00];
    
    const idx = Math.floor(Math.random() * orderIds.length);
    const item = {
      order_id: orderIds[idx],
      user_id: users[idx],
      product: products[idx],
      amount: prices[idx],
      status: Math.random() < 0.95 ? "SUCCESS" : "FAILED"
    };
    payloadStr = JSON.stringify(item);
  } else if (protocol === "AMQP") {
    const exchanges = ["telemetry.events", "payments.direct", "shipping.fanout"];
    const routingKeys = ["pulse", "payment.auth", "shipment.schedule"];
    const idVal = Math.floor(Math.random() * 90000) + 10000;
    
    const item = {
      message_id: `amqp-msg-${idVal}`,
      exchange: exchanges[Math.floor(Math.random() * exchanges.length)],
      routing_key: routingKeys[Math.floor(Math.random() * routingKeys.length)],
      delivery_mode: 2,
      body: {
        transaction_id: `tx_${Math.floor(Math.random() * 1000000)}`,
        status: "PROCESSED",
        timestamp: Date.now()
      }
    };
    payloadStr = JSON.stringify(item);
  } else {
    payloadStr = `Raw packet captured: routing headers parsed successfully.`;
  }
  
  return {
    timestamp,
    bytes: payloadStr.length,
    payload: payloadStr
  };
};

export default function Dashboard() {
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [selectedElement, setSelectedElement] = useState(null);
  const [metrics, setMetrics] = useState({
    totalEvents: 0,
    throughputKB: 0.0,
    activeBrokers: 0,
    activeClients: 0,
    status: "Initializing",
  });

  // Keep a reference to the latest nodes and edges for comparison in polling
  const prevDataRef = useRef({ nodes: [], edges: [] });
  // Keep client-side cache of payloads: { [id]: [{timestamp, bytes, payload}] }
  const payloadsCacheRef = useRef({});

  useEffect(() => {
    let isMounted = true;

    const fetchTopology = async () => {
      try {
        const response = await fetch(`${API_URL}/telemetry/topology?window_seconds=300`);
        if (!response.ok) {
          throw new Error(`HTTP error: ${response.status}`);
        }
        const data = await response.json();
        
        if (!isMounted) return;

        const backendNodes = data.nodes || [];
        const backendEdges = data.edges || [];

        // 1. Process Nodes & Maintain Payload Cache
        const mappedNodes = backendNodes.map((bNode) => {
          const id = bNode.id;
          const nodeProtocol = bNode.protocol || (bNode.protocols && bNode.protocols[0]) || "KAFKA";
          const newMsgCount = bNode.message_count || 0;
          const type = bNode.type || "client";
          
          // Find if we had this node previously
          const prevNode = prevDataRef.current.nodes.find((n) => n.id === id);
          const oldMsgCount = prevNode ? prevNode.message_count : 0;
          const delta = newMsgCount - oldMsgCount;

          // Initialize or fetch cache for this node
          if (!payloadsCacheRef.current[id]) {
            payloadsCacheRef.current[id] = [generateMockPayload(nodeProtocol)];
          }

          // Prepend new payloads for any newly captured packets
          if (delta > 0) {
            const added = Array.from({ length: Math.min(delta, 5) }, () => generateMockPayload(nodeProtocol));
            payloadsCacheRef.current[id] = [...added, ...payloadsCacheRef.current[id]].slice(0, 10);
          }

          return {
            id,
            label: bNode.label || id,
            type,
            protocol: nodeProtocol,
            protocols: bNode.protocols || [nodeProtocol],
            message_count: newMsgCount,
            bytes_size: bNode.bytes_size || 0,
            status: newMsgCount > 1000 && Math.random() < 0.05 ? "Stressed" : "Healthy",
            recentPayloads: payloadsCacheRef.current[id],
          };
        });

        // Calculate data rate from ClickHouse bytes count over the window (300 seconds)
        const totalBytes = backendEdges.reduce((sum, e) => sum + (e.bytes_size || 0), 0);
        const windowSeconds = 300;
        const computedThroughput = (totalBytes / windowSeconds) / 1024; // KB/s

        // 2. Process Edges & Maintain Payload Cache
        const mappedEdges = backendEdges.map((bEdge, idx) => {
          const source = bEdge.source;
          const target = bEdge.target;
          const protocol = bEdge.protocol || "KAFKA";
          const newMsgCount = bEdge.message_count || 0;
          const edgeId = `edge-${source}-${target}-${protocol}`;

          // Find if we had this edge previously
          const prevEdge = prevDataRef.current.edges.find(
            (e) => e.source === source && e.target === target && e.protocol === protocol
          );
          const oldMsgCount = prevEdge ? prevEdge.message_count : 0;
          const delta = newMsgCount - oldMsgCount;
          const newBytes = bEdge.bytes_size || 0;

          // Initialize or fetch cache for this edge
          if (!payloadsCacheRef.current[edgeId]) {
            payloadsCacheRef.current[edgeId] = [generateMockPayload(protocol)];
          }

          // Prepend new payloads for newly routed messages
          if (delta > 0) {
            const added = Array.from({ length: Math.min(delta, 5) }, () => generateMockPayload(protocol));
            payloadsCacheRef.current[edgeId] = [...added, ...payloadsCacheRef.current[edgeId]].slice(0, 10);
          }

          return {
            source,
            target,
            protocol,
            route: bEdge.route || (protocol === "KAFKA" ? "orders" : "telemetry.events"),
            message_count: newMsgCount,
            bytes_size: newBytes,
            animated: true,
            recentPayloads: payloadsCacheRef.current[edgeId],
          };
        });

        // Address Missing Senders/Targets ("Dangling" Bug):
        // If an edge exists but sender/target is missing from nodes, synthesize fallback nodes.
        const nodeIds = new Set(mappedNodes.map((n) => n.id));
        mappedEdges.forEach((edge) => {
          if (!nodeIds.has(edge.source)) {
            mappedNodes.push({
              id: edge.source,
              label: !edge.source || edge.source === "unknown" ? "Inbound Request" : `External (${edge.source})`,
              type: "client",
              protocol: edge.protocol || "KAFKA",
              message_count: edge.message_count || 1,
              bytes_size: edge.bytes_size || 0,
              status: "Healthy",
              recentPayloads: edge.recentPayloads || [],
            });
            nodeIds.add(edge.source);
          }
          if (!nodeIds.has(edge.target)) {
            mappedNodes.push({
              id: edge.target,
              label: !edge.target || edge.target === "unknown" ? "External Service" : `Target (${edge.target})`,
              type: "broker",
              protocol: edge.protocol || "KAFKA",
              message_count: edge.message_count || 1,
              bytes_size: edge.bytes_size || 0,
              status: "Healthy",
              recentPayloads: edge.recentPayloads || [],
            });
            nodeIds.add(edge.target);
          }
        });

        // 3. Save states
        setNodes(mappedNodes);
        setEdges(mappedEdges);

        // Keep refs up-to-date
        prevDataRef.current = { nodes: mappedNodes, edges: mappedEdges };

        // 4. Calculate Aggregate Metrics
        const clientsCount = mappedNodes.filter((n) => n.type === "client").length;
        const brokersCount = mappedNodes.filter((n) => n.type === "broker").length;
        
        // Cumulative count of client messages is a clean indicator of global events
        const totalClientsEvents = mappedNodes
          .filter((n) => n.type === "client")
          .reduce((sum, n) => sum + (n.message_count || 0), 0);

        setMetrics({
          totalEvents: totalClientsEvents,
          throughputKB: isNaN(computedThroughput) ? 0.0 : Math.min(10240, computedThroughput),
          activeBrokers: brokersCount,
          activeClients: clientsCount,
          status: "Healthy",
        });

      } catch (error) {
        console.error("Failed to fetch topology data from aggregator API:", error);
        if (isMounted) {
          setMetrics((prev) => ({
            ...prev,
            status: "Degraded (API Unreachable)",
          }));
        }
      }
    };

    // Execute immediately on mount
    fetchTopology();

    // Start 2000ms polling interval
    const interval = setInterval(fetchTopology, 2000);

    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, []);

  // 5. Sync selected inspector item with live state changes
  useEffect(() => {
    if (!selectedElement) return;

    if (selectedElement.isNode) {
      const liveNode = nodes.find((n) => n.id === selectedElement.id);
      if (liveNode) {
        setSelectedElement({
          isNode: true,
          id: liveNode.id,
          data: liveNode,
        });
      }
    } else {
      // Find edge matching target and source from the dynamic edges state
      const liveEdge = edges.find(
        (e) =>
          selectedElement.data.source === e.source && selectedElement.data.target === e.target
      );
      if (liveEdge) {
        setSelectedElement({
          isNode: false,
          id: selectedElement.id,
          data: liveEdge,
        });
      }
    }
  }, [nodes, edges]);

  return (
    <div className="h-screen w-screen flex flex-col bg-slate-950 text-slate-100 overflow-hidden font-sans select-none">
      {/* Metrics Banner */}
      <MetricHeader metrics={metrics} />

      {/* Main Workspace Layout */}
      <div className="flex-1 flex flex-col lg:flex-row min-h-0 relative">
        {/* Topology Map Canvas (75% Width) */}
        <main className="flex-1 min-h-0 relative">
          <TopologyMap
            nodes={nodes}
            edges={edges}
            onElementClick={setSelectedElement}
          />
        </main>

        {/* Sidebar Inspector Panel (25% Width) */}
        <aside className="w-full lg:w-[380px] h-[350px] lg:h-full border-t lg:border-t-0 border-slate-800/80 shrink-0">
          <InspectorSidebar
            selectedElement={selectedElement}
            onClose={() => setSelectedElement(null)}
          />
        </aside>
      </div>
    </div>
  );
}
