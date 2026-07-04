"use client";

import React from "react";
import { X, Server, Network, Terminal, Database, FileText, Cpu, Activity } from "lucide-react";

export default function InspectorSidebar({ selectedElement, onClose }) {
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
          Select a microservice broker node or active routing edge on the map to inspect live packet payloads.
        </p>
      </div>
    );
  }

  const { isNode, data = {}, id } = selectedElement;

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
