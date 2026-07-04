"use client";

import React from "react";
import { Activity, Server, Shield, RefreshCw } from "lucide-react";

export default function MetricHeader({ metrics = {} }) {
  const {
    totalEvents = 0,
    throughputKB = 0.0,
    activeBrokers = 0,
    activeClients = 0,
    status = "Healthy", // "Healthy" or "Degraded"
  } = metrics;

  return (
    <header className="border-b border-slate-800/80 bg-slate-900/40 backdrop-blur-md px-6 py-4 flex flex-col md:flex-row md:items-center md:justify-between gap-4">
      {/* Brand & Connection State */}
      <div className="flex items-center gap-3">
        <div className="p-2.5 rounded-lg bg-blue-500/10 border border-blue-500/20 text-blue-400">
          <Activity className="w-6 h-6 animate-pulse" />
        </div>
        <div>
          <div className="flex items-center gap-2">
            <h1 className="text-xl font-bold tracking-tight text-white font-mono">
              SIGNAL<span className="text-blue-500">SNITCH</span>
            </h1>
            <span className="text-[10px] uppercase font-mono px-2 py-0.5 rounded bg-blue-950 text-blue-400 border border-blue-800/50">
              v1.0.0
            </span>
          </div>
          <p className="text-xs text-slate-400 font-mono flex items-center gap-1.5 mt-0.5">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
            </span>
            LIVE telemetry flow active
          </p>
        </div>
      </div>

      {/* Metrics Row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 flex-1 max-w-4xl">
        {/* Status Card */}
        <div className="bg-slate-900/60 border border-slate-800/60 rounded-xl p-3 flex items-center gap-3">
          <div className={`p-2 rounded-lg ${
            status === "Healthy"
              ? "bg-emerald-500/10 text-emerald-400 border border-emerald-500/20"
              : "bg-rose-500/10 text-rose-400 border border-rose-500/20"
          }`}>
            <Shield className="w-4 h-4" />
          </div>
          <div>
            <p className="text-[10px] text-slate-500 uppercase tracking-wider font-mono">System Health</p>
            <p className={`text-sm font-semibold font-mono ${
              status === "Healthy" ? "text-emerald-400" : "text-rose-400"
            }`}>
              {status}
            </p>
          </div>
        </div>

        {/* Total Events Card */}
        <div className="bg-slate-900/60 border border-slate-800/60 rounded-xl p-3 flex items-center gap-3">
          <div className="p-2 rounded-lg bg-indigo-500/10 text-indigo-400 border border-indigo-500/20">
            <RefreshCw className="w-4 h-4 animate-spin-slow" />
          </div>
          <div>
            <p className="text-[10px] text-slate-500 uppercase tracking-wider font-mono">Events Sniffed</p>
            <p className="text-sm font-semibold font-mono text-slate-200">
              {totalEvents.toLocaleString()}
            </p>
          </div>
        </div>

        {/* Throughput Card */}
        <div className="bg-slate-900/60 border border-slate-800/60 rounded-xl p-3 flex items-center gap-3">
          <div className="p-2 rounded-lg bg-amber-500/10 text-amber-400 border border-amber-500/20">
            <Activity className="w-4 h-4" />
          </div>
          <div>
            <p className="text-[10px] text-slate-500 uppercase tracking-wider font-mono">Data Rate</p>
            <p className="text-sm font-semibold font-mono text-slate-200">
              {throughputKB.toFixed(2)} KB/s
            </p>
          </div>
        </div>

        {/* Active Nodes Card */}
        <div className="bg-slate-900/60 border border-slate-800/60 rounded-xl p-3 flex items-center gap-3">
          <div className="p-2 rounded-lg bg-sky-500/10 text-sky-400 border border-sky-500/20">
            <Server className="w-4 h-4" />
          </div>
          <div>
            <p className="text-[10px] text-slate-500 uppercase tracking-wider font-mono">Infrastructure</p>
            <p className="text-sm font-semibold font-mono text-slate-200">
              {activeBrokers} B / {activeClients} C
            </p>
          </div>
        </div>
      </div>
    </header>
  );
}
