"use client";

import React, { useMemo, useEffect, useState } from "react";
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  Handle,
  Position,
  getSmoothStepPath,
  ConnectionLineType,
  useNodesState,
  useEdgesState,
} from "reactflow";
import "reactflow/dist/style.css";
import { Server, User, GitBranch } from "lucide-react";

// --- CUSTOM NODE RENDERING (Matte Charcoal Datadog/Vercel Vibe) ---
const CustomNodeComponent = ({ data }) => {
  const { label, type, protocol, status = "Healthy", message_count = 0 } = data;

  let icon = <User className="w-3.5 h-3.5 text-sky-400" />;
  let typeLabel = "Client";
  let bgClass = "bg-sky-500/10 border-sky-500/20";
  let glowColor = "bg-sky-400 shadow-[0_0_8px_#38bdf8]";

  if (type === "broker") {
    icon = <Server className="w-3.5 h-3.5 text-emerald-400" />;
    typeLabel = `${protocol || "System"} Broker`;
    bgClass = status === "Healthy" ? "bg-emerald-500/10 border-emerald-500/20" : "bg-rose-500/10 border-rose-500/20";
    glowColor = status === "Healthy" ? "bg-emerald-500 shadow-[0_0_8px_#10b981]" : "bg-rose-500 shadow-[0_0_8px_#f43f5e]";
  } else if (type === "route") {
    icon = <GitBranch className="w-3.5 h-3.5 text-purple-400" />;
    typeLabel = `${protocol || "System"} Route`;
    bgClass = "bg-purple-500/10 border-purple-500/20";
    glowColor = "bg-purple-400 shadow-[0_0_8px_#c084fc]";
  }

  return (
    <div className="bg-[#111726]/80 backdrop-blur-md border border-[#1f293d] hover:border-slate-700 rounded-xl p-4 shadow-2xl w-64 transition-all duration-300 group">
      {/* Left target handle for incoming messages */}
      <Handle 
        type="target" 
        position={Position.Left} 
        style={{ background: "#334155", border: "2px solid #090d16", width: 10, height: 10, left: -5 }} 
      />
      
      {/* Right source handle for outgoing messages */}
      <Handle 
        type="source" 
        position={Position.Right} 
        style={{ background: "#334155", border: "2px solid #090d16", width: 10, height: 10, right: -5 }} 
      />
 
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <span className={`w-2 h-2 rounded-full animate-pulse ${glowColor}`} />
          <span className="text-[10px] font-mono tracking-wider uppercase text-slate-400 font-semibold">
            {typeLabel}
          </span>
        </div>
        <div className="flex items-center gap-1">
          {(data.protocols && data.protocols.length > 0 ? data.protocols : [protocol || "SYSTEM"]).map((p, idx) => (
            <span key={idx} className="text-[9px] font-mono px-1.5 py-0.5 rounded bg-[#1f293d]/80 text-slate-300 border border-slate-800 uppercase">
              {p}
            </span>
          ))}
        </div>
      </div>

      <div className="flex items-center gap-2.5 mb-3">
        <div className={`p-2 rounded-lg border ${bgClass}`}>
          {icon}
        </div>
        <div className="overflow-hidden flex-1">
          <h4 className="text-xs font-bold font-mono tracking-tight text-slate-200 truncate max-w-[170px]" title={label}>
            {label}
          </h4>
          <span className="text-[10px] text-slate-500 font-mono tracking-wide truncate block">
            {label.includes(":") ? label.split(":")[0] : label}
          </span>
        </div>
      </div>

      <div className="pt-2 border-t border-[#1f293d]/80 flex items-center justify-between text-[11px] font-mono">
        <span className="text-slate-500">Throughput</span>
        <div className="flex items-baseline gap-1">
          <span className="text-slate-200 font-semibold">{message_count.toLocaleString()}</span>
          <span className="text-[9px] text-slate-500">msgs</span>
        </div>
      </div>
    </div>
  );
};

// --- CUSTOM EDGE RENDERING (Fiber-Optic Photon Pipelines) ---
const CustomEdgeComponent = ({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style = {},
  markerEnd,
  data = {},
}) => {
  const [edgePath] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    borderRadius: 8,
  });

  // Colors based on protocol
  let pulseColor = "#00f0ff"; // Cyan for AMQP/default
  let filterGlow = "drop-shadow(0 0 6px rgba(0, 240, 255, 0.8))";
  
  if (data.protocol === "KAFKA") {
    pulseColor = "#d946ef"; // Purple/Violet for KAFKA
    filterGlow = "drop-shadow(0 0 6px rgba(217, 70, 239, 0.8))";
  } else if (data.protocol === "AMQP") {
    pulseColor = "#00f0ff"; // Cyan for AMQP
    filterGlow = "drop-shadow(0 0 6px rgba(0, 240, 255, 0.8))";
  }

  return (
    <>
      {/* Path 1: Thin, dark, elegant base connection line */}
      <path
        id={id}
        style={{
          ...style,
          strokeWidth: 1.5,
          stroke: "#334155",
        }}
        className="react-flow__edge-path cursor-pointer"
        d={edgePath}
        markerEnd={markerEnd}
      />
      {/* Path 2: Animated glowing photon pip moving through fiber-optic cable */}
      <path
        id={`${id}-pulse`}
        style={{
          ...style,
          strokeWidth: 2,
          stroke: pulseColor,
          filter: filterGlow,
        }}
        className="react-flow__edge-path edge-pulse pointer-events-none"
        d={edgePath}
      />
    </>
  );
};

const nodeTypes = {
  customNode: CustomNodeComponent,
};

const edgeTypes = {
  customEdge: CustomEdgeComponent,
};

export default function TopologyMap({ nodes = [], edges = [], onElementClick }) {
  const [rfNodes, setRfNodes, onNodesChange] = useNodesState([]);
  const [rfEdges, setRfEdges, onEdgesChange] = useEdgesState([]);
  const [isMounted, setIsMounted] = useState(false);

  // Safely mount to prevent SSR mismatch
  useEffect(() => {
    setIsMounted(true);
  }, []);

  // Compute positions dynamically in a layout
  useEffect(() => {
    if (nodes.length === 0) return;

    // Filter Fragmented Noise: Ensure any route/edge with volume < 3 is filtered out
    // First let's find active edges with message_count >= 3
    const activeEdgeList = edges.filter(e => (e.message_count || 0) >= 3);
    const activeConnectedNodeIds = new Set();
    activeEdgeList.forEach(e => {
      activeConnectedNodeIds.add(e.source);
      activeConnectedNodeIds.add(e.target);
    });

    // Filter out route nodes that have < 3 messages or have no remaining active connections
    const filteredNodes = nodes.filter(n => {
      if (n.type === "route") {
        return (n.message_count || 0) >= 3 && activeConnectedNodeIds.has(n.id);
      }
      return true; // Keep all client and broker nodes so they can act as anchors
    });

    // Enforce a strict grid-lane layout mapping:
    // Column 1 (X: 50) = Clients
    // Column 2 (X: 400) = Routes / Topics / Exchanges
    // Column 3 (X: 750) = Brokers
    const clients = filteredNodes.filter(n => n.type === "client");
    const routes = filteredNodes.filter(n => n.type === "route");
    const brokers = filteredNodes.filter(n => n.type === "broker");

    // Sort within columns by protocol (KAFKA first, then AMQP) to prevent line crossing chaos
    const sortByProto = (a, b) => {
      const pA = a.protocol || (a.protocols ? a.protocols[0] : "") || "";
      const pB = b.protocol || (b.protocols ? b.protocols[0] : "") || "";
      return pB.localeCompare(pA); // KAFKA before AMQP
    };
    clients.sort(sortByProto);
    routes.sort(sortByProto);
    brokers.sort(sortByProto);

    const stepY = 160; // Clean vertical row height per node
    const maxCount = Math.max(clients.length, routes.length, brokers.length, 1);

    // Align Y coordinates neatly by counting nodes per column (centered vertically)
    const getStartY = (colLen) => {
      return 50 + ((maxCount - colLen) * stepY) / 2;
    };

    const layoutNodes = [];

    // 1. Clients (Column 1 - X: 80)
    const clientStartY = getStartY(clients.length);
    clients.forEach((node, idx) => {
      layoutNodes.push({
        id: node.id,
        type: "customNode",
        position: node.position || { x: 80, y: clientStartY + (idx * stepY) },
        data: { ...node },
      });
    });

    // 2. Routes (Column 2 - X: 400)
    const routeStartY = getStartY(routes.length);
    routes.forEach((node, idx) => {
      layoutNodes.push({
        id: node.id,
        type: "customNode",
        position: node.position || { x: 400, y: routeStartY + (idx * stepY) },
        data: { ...node },
      });
    });

    // 3. Brokers (Column 3 - X: 720)
    const brokerStartY = getStartY(brokers.length);
    brokers.forEach((node, idx) => {
      layoutNodes.push({
        id: node.id,
        type: "customNode",
        position: node.position || { x: 720, y: brokerStartY + (idx * stepY) },
        data: { ...node },
      });
    });

    setRfNodes(layoutNodes);
  }, [nodes, edges, setRfNodes]);

  // Sync and map Edges to React Flow Edges
  useEffect(() => {
    // Filter Fragmented Noise: Only display edges with message volume >= 3
    const activeEdges = edges.filter(edge => (edge.message_count || 0) >= 3);

    const layoutEdges = activeEdges.map((edge, idx) => {
      return {
        id: `e-${edge.source}-${edge.target}-${idx}`,
        source: edge.source,
        target: edge.target,
        type: "customEdge",
        animated: true,
        data: {
          ...edge,
        },
      };
    });

    setRfEdges(layoutEdges);
  }, [edges, setRfEdges]);

  // Click handlers
  const handleNodeClick = (event, node) => {
    if (onElementClick) {
      onElementClick({
        isNode: true,
        id: node.id,
        data: node.data,
      });
    }
  };

  const handleEdgeClick = (event, edge) => {
    if (onElementClick) {
      onElementClick({
        isNode: false,
        id: edge.id,
        data: edge.data,
      });
    }
  };

  if (!isMounted) {
    return (
      <div className="w-full h-full bg-[#090d16] flex items-center justify-center text-slate-500 font-mono text-xs">
        Initializing Canvas Map...
      </div>
    );
  }

  return (
    <div className="w-full h-full bg-[#090d16] relative overflow-hidden">
      <ReactFlow
        nodes={rfNodes}
        edges={rfEdges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={handleNodeClick}
        onEdgeClick={handleEdgeClick}
        fitView
        fitViewOptions={{ padding: 0.15 }}
        minZoom={0.2}
        maxZoom={1.5}
      >
        <Background color="#1e293b" gap={20} size={1} style={{ opacity: 0.4 }} />
        <Controls 
          className="bg-[#111726] border border-[#1f293d] text-slate-300 [&>button]:border-[#1f293d] [&>button]:bg-[#111726] [&>button]:text-slate-300 hover:[&>button]:bg-slate-800"
        />
        <MiniMap 
          nodeColor={(node) => {
            if (node.data?.type === "broker") return "#10b981";
            if (node.data?.type === "route") return "#a855f7";
            return "#00f0ff";
          }}
          maskColor="rgba(9, 13, 22, 0.7)"
          style={{ backgroundColor: "#111726", border: "1px solid #1f293d", borderRadius: "0.75rem" }}
          className="hidden sm:block shadow-2xl overflow-hidden"
        />
      </ReactFlow>
    </div>
  );
}
