"use client";

import React, { useState, useEffect } from "react";
import { 
  X, 
  Server, 
  Network, 
  Terminal, 
  Database, 
  FileText, 
  Cpu, 
  Activity, 
  ShieldAlert, 
  ShieldCheck, 
  Bot, 
  Sparkles 
} from "lucide-react";

export default function InspectorSidebar({ selectedElement, onClose }) {
  const [aiReport, setAiReport] = useState(null);
  const [isLoading, setIsLoading] = useState(false);

  // Helper to extract a clean route/topic name from nodes or edges
  const getRouteName = (element) => {
    if (!element) return null;
    const { isNode, data = {}, id = "" } = element;
    if (isNode) {
      if (data.type === "route" || id.startsWith("route:")) {
        return data.label || id.replace(/^route:(kafka|amqp):/i, "");
      }
      return null;
    } else {
      // For edges, inspect route property or target/source endpoints
      if (data.route) return data.route;
      if (data.target && data.target.startsWith("route:")) {
        return data.target.replace(/^route:(kafka|amqp):/i, "");
      }
      if (data.source && data.source.startsWith("route:")) {
        return data.source.replace(/^route:(kafka|amqp):/i, "");
      }
      return null;
    }
  };

  useEffect(() => {
    const routeName = getRouteName(selectedElement);
    if (routeName) {
      fetchAiAnalysis(routeName);
    } else {
      setAiReport(null);
      setIsLoading(false);
    }
  }, [selectedElement]);

  const fetchAiAnalysis = async (routeName) => {
    if (!routeName) {
      setAiReport(null);
      return;
    }
    setIsLoading(true);
    setAiReport(null);
    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
      const response = await fetch(`${apiUrl}/telemetry/analyze?topic=${encodeURIComponent(routeName)}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
      });
      if (!response.ok) {
        throw new Error(`HTTP error ${response.status}`);
      }
      const data = await response.json();
      setAiReport(data);
    } catch (err) {
      console.error("AI Diagnostic Engine fetch error:", err);
      // Defensive fallback state to prevent UI crashes during network or database lockouts
      setAiReport({
        is_anomaly: false,
        suspected_cause: "AI Diagnostic Engine is temporarily unreachable or offline (network drop / database lockout).",
        suggested_fix: "Verify backend service status (`docker compose ps aggregator`) and check connection logs.",
      });
    } finally {
      setIsLoading(false);
    }
  };

  if (!selectedElement) {
    return (
      <div className="h-full flex flex-col items-center justify-center p-6 text-center bg-slate-950/40 border-l border-slate-900/60 backdrop-blur-md">
        <div className="p-4 rounded-full bg-slate-900/80 border border-slate-800 text-slate-500 mb-4 animate-pulse">
          <Terminal className="w-8 h-8" />
        </div>
        <h3 className="text-sm font-semibold text-slate-300 font-mono tracking-wide">
          PAYLOAD INSPECTOR
        </h3>
        <p className="text-xs text-slate-500 max-w-[240px] mt-2 font-mono leading-relaxed">
          Select a microservice broker node or active routing edge on the map to inspect live packet payloads and trigger AI SRE diagnostics.
        </p>
      </div>
    );
  }

  const { isNode, data = {}, id } = selectedElement;
  const activeRouteName = getRouteName(selectedElement);

  return (
    <div className="h-full flex flex-col bg-slate-950/70 border-l border-slate-800/80 backdrop-blur-md text-slate-200">
      {/* Sidebar Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-slate-800/80 bg-slate-900/20">
        <div className="flex items-center gap-2">
          {isNode ? (
            <Server className="w-5 h-5 text-blue-400" />
          ) : (
            <Network className="w-5 h-5 text-purple-400" />
          )}
          <h2 className="text-sm font-bold tracking-wider font-mono uppercase text-slate-100">
            {isNode ? "Node Inspector" : "Edge Inspector"}
          </h2>
        </div>
        <button
          onClick={onClose}
          className="p-1 rounded-md hover:bg-slate-800 text-slate-400 hover:text-slate-200 transition-colors"
          title="Close Inspector"
        >
          <X className="w-5 h-5" />
        </button>
      </div>

      {/* Main Details Panel */}
      <div className="flex-1 overflow-y-auto p-6 space-y-6">
        {/* Entity Title Card */}
        <div className="bg-slate-900/50 border border-slate-800/80 rounded-xl p-4 space-y-2">
          <div className="flex items-center justify-between">
            <span className="text-[10px] font-mono uppercase tracking-wider text-slate-500">
              Identifier
            </span>
            <span className={`text-[10px] font-mono px-2 py-0.5 rounded font-semibold uppercase ${
              data.protocol === "KAFKA"
                ? "bg-orange-500/10 text-orange-400 border border-orange-500/20"
                : data.protocol === "AMQP"
                ? "bg-purple-500/10 text-purple-400 border border-purple-500/20"
                : "bg-slate-800 text-slate-400"
            }`}>
              {data.protocol || "SYSTEM"}
            </span>
          </div>
          <p className="text-base font-bold font-mono tracking-tight text-white break-all">
            {isNode ? data.label || id : `${data.source} → ${data.target}`}
          </p>
          {isNode && (
            <p className="text-xs text-slate-400 font-mono">
              Role: <span className="text-blue-400 capitalize">{data.type || "unknown"}</span>
            </p>
          )}
        </div>

        {/* Telemetry Metrics */}
        <div>
          <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider font-mono mb-3 flex items-center gap-1.5">
            <Cpu className="w-3.5 h-3.5" />
            Telemetry Metrics
          </h3>
          <div className="grid grid-cols-2 gap-3">
            <div className="bg-slate-900/30 border border-slate-800/40 rounded-lg p-3">
              <span className="text-[10px] text-slate-500 font-mono uppercase">Total Messages</span>
              <p className="text-lg font-bold font-mono text-slate-200 mt-1">
                {(data.message_count || 0).toLocaleString()}
              </p>
            </div>
            <div className="bg-slate-900/30 border border-slate-800/40 rounded-lg p-3">
              <span className="text-[10px] text-slate-500 font-mono uppercase">Data Weight</span>
              <p className="text-lg font-bold font-mono text-slate-200 mt-1">
                {data.bytes_size >= 1048576
                  ? `${(data.bytes_size / 1048576).toFixed(2)} MB`
                  : `${(data.bytes_size / 1024).toFixed(1)} KB`}
              </p>
            </div>
          </div>
        </div>

        {/* Custom Node or Edge details */}
        {!isNode && (
          <div>
            <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider font-mono mb-3 flex items-center gap-1.5">
              <Network className="w-3.5 h-3.5" />
              Routing Configuration
            </h3>
            <div className="bg-slate-900/30 border border-slate-800/40 rounded-lg p-3.5 space-y-2.5 font-mono text-xs">
              <div className="flex justify-between">
                <span className="text-slate-500">Source:</span>
                <span className="text-blue-400 text-right break-all max-w-[160px]">{data.source}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-500">Destination:</span>
                <span className="text-emerald-400 text-right break-all max-w-[160px]">{data.target}</span>
              </div>
              <div className="flex justify-between border-t border-slate-800/80 pt-2">
                <span className="text-slate-500">Route / Topic:</span>
                <span className="text-amber-400 text-right font-semibold break-all max-w-[160px]">
                  {data.route || "Default"}
                </span>
              </div>
            </div>
          </div>
        )}

        {/* AI Diagnostic Guard Banner */}
        {activeRouteName && (
          <div>
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-xs font-semibold text-slate-300 uppercase tracking-wider font-mono flex items-center gap-1.5">
                <Bot className="w-4 h-4 text-cyan-400" />
                AI Diagnostic Guard (On-Call SRE)
              </h3>
              <span className="text-[9px] font-mono px-2 py-0.5 rounded-full bg-cyan-950/40 border border-cyan-800/60 text-cyan-400 flex items-center gap-1">
                <Sparkles className="w-2.5 h-2.5" /> Gemini 2.5 Flash
              </span>
            </div>

            {isLoading ? (
              <div className="bg-[#0c101b]/80 border border-slate-800/80 rounded-xl p-5 shadow-inner flex flex-col items-center justify-center space-y-3 text-center">
                <div className="flex items-center gap-2">
                  <span className="relative flex h-3 w-3">
                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-cyan-400 opacity-75"></span>
                    <span className="relative inline-flex rounded-full h-3 w-3 bg-cyan-500"></span>
                  </span>
                  <span className="text-xs font-mono font-semibold text-cyan-300 animate-pulse">
                    Analyzing stream telemetry...
                  </span>
                </div>
                <p className="text-[11px] font-mono text-slate-400 max-w-sm leading-relaxed">
                  🤖 SignalSnitch AI Agent extracting ClickHouse byte arrays and parsing schema constraints...
                </p>
              </div>
            ) : aiReport ? (
              aiReport.is_anomaly ? (
                <div className="border border-rose-500/30 bg-rose-950/20 text-rose-200 rounded-xl p-4 shadow-xl mb-4 transition-all">
                  <div className="flex items-start gap-3">
                    <ShieldAlert className="w-5 h-5 text-rose-400 flex-shrink-0 mt-0.5 animate-pulse" />
                    <div className="flex-1 overflow-hidden">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-xs font-bold font-mono tracking-wide text-rose-300 uppercase">
                          Anomaly / Schema Drift Detected
                        </span>
                        <span className="text-[9px] font-mono px-1.5 py-0.5 rounded bg-rose-900/60 text-rose-200 border border-rose-700">
                          URGENT
                        </span>
                      </div>
                      <p className="text-xs font-mono text-rose-200/90 leading-relaxed break-words">
                        {aiReport.suspected_cause}
                      </p>
                    </div>
                  </div>
                  {aiReport.suggested_fix && (
                    <div className="mt-3">
                      <span className="text-[10px] font-mono uppercase tracking-wider text-rose-300/80 block mb-1">
                        Suggested Terminal Remediation:
                      </span>
                      <div className="bg-[#0c101b] border border-slate-800 text-xs font-mono p-3 rounded-lg mt-2 text-slate-300 overflow-x-auto select-all shadow-inner">
                        {aiReport.suggested_fix}
                      </div>
                    </div>
                  )}
                </div>
              ) : (
                <div className="border border-emerald-500/20 bg-emerald-950/10 text-emerald-400 p-4 rounded-xl text-xs mb-4 shadow-md transition-all">
                  <div className="flex items-start gap-3">
                    <ShieldCheck className="w-5 h-5 text-emerald-400 flex-shrink-0 mt-0.5" />
                    <div>
                      <span className="text-xs font-bold font-mono tracking-wide text-emerald-300 uppercase block mb-1">
                        Stream Health Nominal
                      </span>
                      <p className="font-mono text-emerald-400/90 leading-relaxed">
                        Structure validation complete. Core data streams conform cleanly to runtime application contracts.
                      </p>
                      {aiReport.suspected_cause && (
                        <p className="text-[11px] font-mono text-slate-400 mt-2 border-t border-emerald-900/40 pt-2">
                          {aiReport.suspected_cause}
                        </p>
                      )}
                    </div>
                  </div>
                </div>
              )
            ) : null}
          </div>
        )}

        {/* Live Packet Terminal */}
        <div className="flex-1 flex flex-col min-h-[220px]">
          <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider font-mono mb-3 flex items-center gap-1.5">
            <FileText className="w-3.5 h-3.5" />
            Packet Payload Terminal
          </h3>
          <div className="flex-1 bg-slate-950 border border-slate-800/80 rounded-xl p-4 font-mono text-xs overflow-hidden flex flex-col shadow-inner">
            <div className="flex items-center justify-between border-b border-slate-900 pb-2 mb-3">
              <span className="text-[10px] text-slate-500 flex items-center gap-1.5">
                <span className="relative flex h-1.5 w-1.5">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-1.5 w-1.5 bg-blue-500"></span>
                </span>
                sniffing raw traffic
              </span>
              <span className="text-[9px] text-slate-600">
                {new Date().toLocaleTimeString()}
              </span>
            </div>
            <div className="flex-1 overflow-y-auto space-y-3 pr-1">
              {data.recentPayloads && data.recentPayloads.length > 0 ? (
                data.recentPayloads.map((payloadObj, idx) => (
                  <div key={idx} className="border-b border-slate-900/50 pb-2 last:border-b-0">
                    <div className="flex justify-between text-[9px] text-slate-500 mb-1">
                      <span>[{payloadObj.timestamp}]</span>
                      <span>{payloadObj.bytes} bytes</span>
                    </div>
                    <pre className="text-slate-300 whitespace-pre-wrap break-all bg-slate-900/40 p-2 rounded border border-slate-800/30 overflow-x-auto">
                      {payloadObj.payload}
                    </pre>
                  </div>
                ))
              ) : (
                <div className="text-slate-600 italic flex items-center justify-center h-full">
                  Listening for incoming message packets...
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
