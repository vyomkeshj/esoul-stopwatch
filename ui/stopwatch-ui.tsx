"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import { useAppCanEdit, usePluginEventDispatch, usePluginRealtime } from "esoul-sdk/react";
import { kickPluginTask, nanoid, pluginRouteUrl } from "esoul-sdk";
import {
  APP_TYPE,
  abandonedEvent,
  clearedEvent,
  formatMs,
  sourceSetEvent,
  startRequestedEvent,
  stopRequestedEvent,
  stopwatchChannel,
  uiLatencyEvent,
  type StopwatchData,
  type StopwatchRun,
} from "../app";
import { EMPTY_FACE, foldClockMessage, shownMs, type ClockFace } from "./clock-display";

/**
 * Stopwatch UI — a mechanical instrument drawn in SVG.
 *
 * The clock MOVES ONLY WHEN A TICK ARRIVES (ui/clock-display.ts, the pure rule):
 * the face shows the last tick the clock's authority published — the Inngest
 * task or the app's own server route — or the final time once a stop landed.
 * No frame-rate sweep, no local extrapolation, not even for the tab that
 * pressed Start: a stalled clock shows as a stalled hand, which is what an
 * instrument owes its reader. That one number drives the seconds hand, the
 * 30-minute sub-dial and the digital window; nothing on the face has a clock.
 *
 * Two physical pushers: the CROWN (top) starts and stops; the SIDE button
 * clears the history. They depress under the pointer (a real button travels,
 * then springs back) and are real <button>s for the keyboard and readers.
 * Start/Stop kick the tasks; the browser measures tap → first message heard
 * on its own clock and records it on the timeline. Read-only viewers watch.
 *
 * No library: the import wall admits only esoul-sdk, and an instrument face is
 * gradients, arcs and a rotate() — SVG has all of it.
 */

type TickMsg = { runId: string; elapsedMs: number; serverNow: number; tick: number; readMs?: number; publishMs?: number; importMs?: number; foldMs?: number; pingMs?: number; region?: string };
type StoppedMsg = { runId: string; elapsedMs: number; taskEndedAt: number; stopRequestedAt?: number };

const CX = 200;
const CY = 230;
const R_DIAL = 150;
/** A run stuck in starting/stopping this long gets an escape hatch — longer in a preview,
 * whose first request compiles the route it needs (up to a minute). */
const STUCK_MS = typeof window !== "undefined" && window.location.pathname.includes("/internal/plugin-preview") ? 60_000 : 10_000;
const IN_PREVIEW = typeof window !== "undefined" && window.location.pathname.includes("/internal/plugin-preview");

export function StopwatchUi({ state }: { state: StopwatchData }) {
  const dispatch = usePluginEventDispatch();
  const canEdit = useAppCanEdit();
  const current = state?.current ?? null;
  const runs = state?.runs ?? [];
  const clockSource = state?.clockSource ?? "task";
  const identifier = {
    workspaceId: state?.workspaceId ?? "",
    nodeId: state?.nodeId ?? "",
    applicationId: state?.nodeId ?? "",
    instanceName: state?.instanceName ?? "",
  };
  // SVG ids are document-global: two stopwatches on one workspace must not share a bezel.
  const uid = useMemo(() => `sw${(identifier.nodeId || "x").replace(/[^a-zA-Z0-9]/g, "").slice(0, 8)}`, [identifier.nodeId]);

  // Subscribe only while a run exists: a preview has no task runtime, and an
  // idle stopwatch has nothing to hear.
  const live = usePluginRealtime<TickMsg | StoppedMsg>({
    channel: stopwatchChannel,
    workspaceId: identifier.workspaceId,
    nodeId: identifier.nodeId,
    topics: stopwatchChannel.topicNames,
    enabled: !!current && current.source !== "stream" && !!identifier.workspaceId,
  });

  // The stream arm: the app's own route, one request, ticks a second. Same face,
  // same measurements — the run records which clock drove it.
  const [streamMsgs, setStreamMsgs] = useState<{ topic: string; data: unknown }[]>([]);
  const streamOn = !!current && current.source === "stream" && !!identifier.nodeId;
  useEffect(() => {
    if (!streamOn) return;
    const url = pluginRouteUrl("stopwatch-gh", "ticks", identifier.nodeId);
    const es = new EventSource(url);
    const on = (topic: string) => (e: Event) => {
      let data: unknown = null;
      try {
        data = JSON.parse((e as MessageEvent).data);
      } catch {
        data = null;
      }
      setStreamMsgs((m) => [...m.slice(-200), { topic, data }]);
    };
    for (const t of ["tick", "stopped", "stopping", "idle", "task-run", "error"]) es.addEventListener(t, on(t));
    // The route did not answer (a box without route support, or no session): say so, do not spin.
    es.onerror = () => setNotice("The ticks route is not answering — in a preview that means the box predates routes-in-the-preview (reopen it); when installed, reload the page.");
    return () => es.close();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [streamOn, identifier.nodeId]);

  // Per-device: the face (last tick / final time) and the tap times we measure against.
  const [face, setFace] = useState<ClockFace>(EMPTY_FACE);
  const [notice, setNotice] = useState<string | null>(null);
  const [pressed, setPressed] = useState<"crown" | "side" | null>(null);
  const tapStart = useRef<{ runId: string; at: number } | null>(null);
  const tapStop = useRef<{ runId: string; at: number } | null>(null);
  const seen = useRef(0);

  // Fold every new realtime message into the face / measurements.
  const seenStream = useRef(0);
  useEffect(() => {
    const msgs = [
      ...(live.data ?? []).slice(seen.current).map((m) => ({ topic: m.topic, data: m.data })),
      ...streamMsgs.slice(seenStream.current),
    ];
    seen.current = (live.data ?? []).length;
    seenStream.current = streamMsgs.length;
    for (let i = 0; i < msgs.length; i++) {
      const m = msgs[i];
      const heardAt = Date.now();
      setFace((f) => foldClockMessage(f, m));
      if (m.topic === "tick") {
        const t = m.data as TickMsg;
        if (tapStart.current?.runId === t.runId) {
          dispatch(uiLatencyEvent.dataCreator({ ...identifier, runId: t.runId, uiStartLatencyMs: heardAt - tapStart.current.at }));
          tapStart.current = null;
        }
      } else if (m.topic === "stopped") {
        const s = m.data as StoppedMsg;
        if (tapStop.current?.runId === s.runId) {
          dispatch(uiLatencyEvent.dataCreator({ ...identifier, runId: s.runId, uiStopLatencyMs: heardAt - tapStop.current.at }));
          tapStop.current = null;
        }
      }
    }
    // identifier is derived from state; dispatch is stable.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [live.data, streamMsgs]);

  // A run stuck in starting/stopping gets an escape hatch after a while.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!current || current.status === "running") return;
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [current]);
  const stuck =
    !!current &&
    ((current.status === "starting" && now - current.kickedAt > STUCK_MS) ||
      (current.status === "stopping" && current.stopRequestedAt !== undefined && now - current.stopRequestedAt > STUCK_MS));

  // The face: the fold's recorded time once a stop landed, else the last tick heard for this
  // run, else 0 (nothing arrived yet), else the final time heard, else the last finished run.
  const display = shownMs(face, current, runs.find((r) => r.status === "finished")?.elapsedMs);

  async function start() {
    if (!canEdit || current) return;
    const runId = nanoid();
    const at = Date.now();
    setFace(EMPTY_FACE);
    setNotice(null);
    if (clockSource === "stream") {
      tapStart.current = { runId, at };
      dispatch(startRequestedEvent.dataCreator({ ...identifier, runId, kickedAt: at, source: "stream" }));
      return;
    }
    let kicked: { ok: boolean; status: number };
    try {
      kicked = await kickPluginTask({ applicationType: APP_TYPE, taskName: "tick", identifier, data: { runId, kickedAt: at } });
    } catch (e) {
      setNotice(`The clock could not be started from here (${(e as Error).message}). A preview has no task runtime — once the app is installed, Start ticks.`);
      return;
    }
    if (!kicked.ok) {
      // The preview page has no session and no task runtime; say which surface this is.
      const inPreview = typeof window !== "undefined" && window.location.pathname.includes("/internal/plugin-preview");
      setNotice(
        inPreview
          ? `This is the Forge preview — it has no task runtime (HTTP ${kicked.status}). The clock runs in the installed app: open Stopwatch from the workspace, not from the board.`
          : kicked.status === 401
            ? "The kick route did not see your session (HTTP 401). Reload the page; if it persists, sign in again — and tell the owner, that is a bug."
            : `The clock was refused (HTTP ${kicked.status}).`,
      );
      return;
    }
    tapStart.current = { runId, at };
    dispatch(startRequestedEvent.dataCreator({ ...identifier, runId, kickedAt: at, source: "task" }));
  }

  function stop() {
    if (!canEdit || !current) return;
    const at = Date.now();
    tapStop.current = { runId: current.runId, at };
    dispatch(stopRequestedEvent.dataCreator({ ...identifier, runId: current.runId, stopRequestedAt: at }));
  }

  function abandon() {
    if (!canEdit || !current) return;
    dispatch(abandonedEvent.dataCreator({ ...identifier, runId: current.runId, reason: current.status === "starting" ? "the clock never answered" : "the clock never stopped" }));
    setFace(EMPTY_FACE);
    setNotice(null);
  }

  function clear() {
    if (!canEdit || runs.length === 0) return;
    dispatch(clearedEvent.dataCreator({ ...identifier }));
  }

  const running = !!current;
  const crownDisabled = !canEdit || (running && current!.status === "stopping");
  const crownLabel = !running ? "Start" : current!.status === "stopping" ? "Stopping…" : "Stop";
  const status = current ? current.status : face.finalMs !== null ? "finished" : "idle";
  const heardThisRun = !!current && face.lastTick?.runId === current.runId;
  const liveNote =
    current && !heardThisRun
      ? current.status === "starting"
        ? current.source === "stream"
          ? IN_PREVIEW
            ? "connecting to the ticks route… (a preview compiles it on first use — up to a minute)"
            : "connecting to the ticks route…"
          : IN_PREVIEW
            ? "starting the clock… (a preview compiles its runtime on first use — up to a minute)"
            : "starting the durable clock…"
        : "listening for the server clock…"
      : current
        ? (() => {
            const t = live.latestData?.data as TickMsg | undefined;
            const cost =
              t?.readMs !== undefined
                ? ` · fold read ${t.readMs} ms${t.pingMs !== undefined ? ` (db ping ${t.pingMs}${t.region ? ` from ${t.region}` : ""})` : ""}${t.publishMs !== undefined && t.publishMs >= 0 ? `, publish ${t.publishMs} ms` : ""}`
                : "";
            return `server tick ${t?.tick ?? 0}${cost}`;
          })()
        : null;

  // Geometry from the one number.
  const secAngle = ((display % 60_000) / 60_000) * 360;
  const minAngle = ((display % 1_800_000) / 1_800_000) * 360;
  const crownDy = pressed === "crown" ? 9 : 0;
  const sideDx = pressed === "side" ? 7 : 0;

  return (
    <div
      className={[
        "stopwatch-root flex flex-col items-center min-h-full w-full p-3 sm:p-5 gap-3",
        "bg-[#efe8dc] dark:bg-[#171513] text-[#3d2f1e] dark:text-white/90 rounded-lg",
      ].join(" ")}
      style={{ minHeight: 320 }}
    >
      <div className="relative w-full" style={{ maxWidth: 400 }}>
        <svg viewBox="0 0 400 420" className="w-full h-auto select-none" role="img" aria-label={`Stopwatch ${identifier.instanceName}: ${formatMs(display)}, ${status}`}>
          <defs>
            <linearGradient id={`${uid}-steel`} x1="0" y1="0" x2="1" y2="1">
              <stop offset="0" stopColor="#f4f4f2" />
              <stop offset="0.25" stopColor="#b9bcc0" />
              <stop offset="0.5" stopColor="#eceeef" />
              <stop offset="0.75" stopColor="#9da2a8" />
              <stop offset="1" stopColor="#d7d9db" />
            </linearGradient>
            <linearGradient id={`${uid}-steelV`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0" stopColor="#f6f6f4" />
              <stop offset="0.5" stopColor="#a9adb2" />
              <stop offset="1" stopColor="#e2e4e6" />
            </linearGradient>
            <radialGradient id={`${uid}-face`} cx="0.5" cy="0.42" r="0.65">
              <stop offset="0" stopColor="#fffdf7" />
              <stop offset="0.85" stopColor="#f3eee2" />
              <stop offset="1" stopColor="#dcd4c3" />
            </radialGradient>
            <radialGradient id={`${uid}-well`} cx="0.5" cy="0.35" r="0.7">
              <stop offset="0" stopColor="#3a3632" />
              <stop offset="1" stopColor="#0f0e0d" />
            </radialGradient>
            <linearGradient id={`${uid}-glass`} x1="0" y1="0" x2="0.3" y2="1">
              <stop offset="0" stopColor="#ffffff" stopOpacity="0.55" />
              <stop offset="0.45" stopColor="#ffffff" stopOpacity="0.04" />
              <stop offset="1" stopColor="#ffffff" stopOpacity="0" />
            </linearGradient>
            <linearGradient id={`${uid}-red`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0" stopColor="#ff6b60" />
              <stop offset="1" stopColor="#a3231a" />
            </linearGradient>
            <linearGradient id={`${uid}-green`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0" stopColor="#5fc99a" />
              <stop offset="1" stopColor="#1f6b4d" />
            </linearGradient>
            <filter id={`${uid}-drop`} x="-20%" y="-20%" width="140%" height="150%">
              <feDropShadow dx="0" dy="6" stdDeviation="6" floodColor="#000" floodOpacity="0.35" />
            </filter>
            <filter id={`${uid}-hand`} x="-50%" y="-50%" width="200%" height="200%">
              <feDropShadow dx="1" dy="2" stdDeviation="1.2" floodColor="#000" floodOpacity="0.35" />
            </filter>
            <filter id={`${uid}-inset`} x="-20%" y="-20%" width="140%" height="140%">
              <feGaussianBlur in="SourceAlpha" stdDeviation="3" result="b" />
              <feOffset dx="0" dy="3" result="o" />
              <feComposite in="o" in2="SourceAlpha" operator="arithmetic" k2="-1" k3="1" result="i" />
              <feColorMatrix in="i" type="matrix" values="0 0 0 0 0  0 0 0 0 0  0 0 0 0 0  0 0 0 0.45 0" result="s" />
              <feMerge>
                <feMergeNode in="SourceGraphic" />
                <feMergeNode in="s" />
              </feMerge>
            </filter>
          </defs>

          {/* ── the crown (Start / Stop): stem, then the cap that travels ── */}
          <g transform={`translate(${CX} 0)`}>
            <rect x="-14" y="30" width="28" height="60" rx="4" fill={`url(#${uid}-steelV)`} stroke="#6f7378" strokeWidth="1" />
            <g transform={`translate(0 ${crownDy})`} style={{ transition: "transform 90ms cubic-bezier(.2,.8,.2,1)" }}>
              <rect x="-30" y="2" width="60" height="42" rx="10" fill={`url(#${uid}-steel)`} stroke="#5b5f64" strokeWidth="1.2" filter={`url(#${uid}-drop)`} />
              {/* knurling */}
              {Array.from({ length: 9 }, (_, i) => (
                <rect key={i} x={-24 + i * 6} y="8" width="2.2" height="30" rx="1" fill="#6f7378" opacity="0.55" />
              ))}
              <rect x="-22" y="45" width="44" height="7" rx="2" fill={running ? `url(#${uid}-red)` : `url(#${uid}-green)`} />
            </g>
          </g>

          {/* ── the side pusher (Clear): on the rim at 45°, the cap proud of the bezel, travels inward ── */}
          <g transform={`translate(${CX + 127} ${CY - 127}) rotate(-45)`}>
            <rect x="-6" y="-8" width="40" height="16" rx="3" fill={`url(#${uid}-steelV)`} stroke="#6f7378" strokeWidth="1" />
            <g transform={`translate(${-sideDx} 0)`} style={{ transition: "transform 90ms cubic-bezier(.2,.8,.2,1)" }}>
              <rect x="26" y="-13" width="36" height="26" rx="7" fill={`url(#${uid}-steel)`} stroke="#5b5f64" strokeWidth="1.2" filter={`url(#${uid}-drop)`} />
              {Array.from({ length: 5 }, (_, i) => (
                <rect key={i} x={31 + i * 6} y="-8" width="2" height="16" rx="1" fill="#6f7378" opacity="0.55" />
              ))}
            </g>
          </g>

          {/* ── the case ── */}
          <circle cx={CX} cy={CY} r={R_DIAL + 30} fill={`url(#${uid}-steel)`} stroke="#4f5358" strokeWidth="1.5" filter={`url(#${uid}-drop)`} />
          <circle cx={CX} cy={CY} r={R_DIAL + 22} fill="none" stroke="#ffffff" strokeOpacity="0.6" strokeWidth="1" />
          <circle cx={CX} cy={CY} r={R_DIAL + 12} fill={`url(#${uid}-well)`} />
          <circle cx={CX} cy={CY} r={R_DIAL + 4} fill={`url(#${uid}-face)`} filter={`url(#${uid}-inset)`} />

          {/* ── the dial: 60 seconds, a mark every fifth of a second ── */}
          {Array.from({ length: 300 }, (_, i) => {
            const a = (i / 300) * 2 * Math.PI;
            const major = i % 25 === 0;
            const mid = !major && i % 5 === 0;
            const len = major ? 16 : mid ? 9 : 4;
            const r1 = R_DIAL - 6;
            const r0 = r1 - len;
            return (
              <line
                key={i}
                x1={CX + r0 * Math.sin(a)}
                y1={CY - r0 * Math.cos(a)}
                x2={CX + r1 * Math.sin(a)}
                y2={CY - r1 * Math.cos(a)}
                stroke={major ? "#2b2420" : "#4a4038"}
                strokeWidth={major ? 2.4 : mid ? 1.6 : 0.8}
                strokeLinecap="round"
              />
            );
          })}
          {Array.from({ length: 12 }, (_, i) => {
            const a = (i / 12) * 2 * Math.PI;
            const r = R_DIAL - 36;
            return (
              <text
                key={i}
                x={CX + r * Math.sin(a)}
                y={CY - r * Math.cos(a) + 6}
                textAnchor="middle"
                fontSize="17"
                fontWeight="700"
                fontFamily="ui-sans-serif, system-ui, sans-serif"
                fill="#2b2420"
              >
                {i === 0 ? 60 : i * 5}
              </text>
            );
          })}

          {/* ── the 30-minute sub-dial ── */}
          <g transform={`translate(${CX} ${CY - 62})`}>
            <circle r="40" fill="#f7f2e7" stroke="#8a8073" strokeWidth="1" />
            {Array.from({ length: 30 }, (_, i) => {
              const a = (i / 30) * 2 * Math.PI;
              const major = i % 5 === 0;
              const r1 = 36;
              const r0 = r1 - (major ? 7 : 3.5);
              return <line key={i} x1={r0 * Math.sin(a)} y1={-r0 * Math.cos(a)} x2={r1 * Math.sin(a)} y2={-r1 * Math.cos(a)} stroke="#4a4038" strokeWidth={major ? 1.6 : 0.8} strokeLinecap="round" />;
            })}
            {[10, 20, 30].map((n) => {
              const a = (n / 30) * 2 * Math.PI;
              return (
                <text key={n} x={25 * Math.sin(a)} y={-25 * Math.cos(a) + 3.5} textAnchor="middle" fontSize="9" fontWeight="700" fontFamily="ui-sans-serif, system-ui, sans-serif" fill="#2b2420">
                  {n}
                </text>
              );
            })}
            <g transform={`rotate(${minAngle})`} filter={`url(#${uid}-hand)`}>
              <path d="M -1.6 6 L 0 -31 L 1.6 6 Z" fill="#2b2420" />
              <circle r="2.6" fill="#2b2420" />
            </g>
          </g>

          {/* ── the digital window ── */}
          <g transform={`translate(${CX} ${CY + 66})`}>
            <rect x="-66" y="-18" width="132" height="36" rx="6" fill="#1b1917" stroke="#6b6157" strokeWidth="1" />
            <text x="0" y="8" textAnchor="middle" fontSize="24" fontWeight="700" fontFamily="ui-monospace, SFMono-Regular, Menlo, monospace" fill="#e9f5ec" style={{ fontVariantNumeric: "tabular-nums" }}>
              {formatMs(display)}
            </text>
          </g>
          <text x={CX} y={CY + 40} textAnchor="middle" fontSize="8.5" letterSpacing="2.5" fontFamily="ui-sans-serif, system-ui, sans-serif" fill="#6b6157">
            {(identifier.instanceName || "STOPWATCH").toUpperCase().slice(0, 22)} · {status.toUpperCase()}
          </text>

          {/* ── the seconds hand: sweeps from the one interpolated number ── */}
          <g transform={`rotate(${secAngle} ${CX} ${CY})`} filter={`url(#${uid}-hand)`}>
            <path d={`M ${CX - 2.4} ${CY + 30} L ${CX} ${CY - R_DIAL + 14} L ${CX + 2.4} ${CY + 30} Z`} fill="#c8271c" />
            <path d={`M ${CX - 2.4} ${CY + 30} L ${CX} ${CY - R_DIAL + 14} L ${CX + 2.4} ${CY + 30} Z`} fill="none" stroke="#7d160f" strokeWidth="0.6" />
            <circle cx={CX} cy={CY + 30} r="5" fill="#c8271c" />
          </g>
          <circle cx={CX} cy={CY} r="6" fill={`url(#${uid}-steelV)`} stroke="#3a3632" strokeWidth="1" />
          <circle cx={CX} cy={CY} r="2" fill="#2b2420" />

          {/* ── the crystal ── */}
          <ellipse cx={CX - 40} cy={CY - 70} rx="118" ry="78" fill={`url(#${uid}-glass)`} transform={`rotate(-25 ${CX - 40} ${CY - 70})`} pointerEvents="none" />
        </svg>

        {/* Real buttons over the pushers: keyboard, readers, and the press that moves the metal. */}
        <button
          type="button"
          aria-label={crownLabel}
          disabled={crownDisabled}
          onPointerDown={() => !crownDisabled && setPressed("crown")}
          onPointerUp={() => setPressed(null)}
          onPointerLeave={() => setPressed(null)}
          onPointerCancel={() => setPressed(null)}
          onClick={() => (running ? stop() : start())}
          className="absolute rounded-xl focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[#b87333] disabled:cursor-not-allowed"
          style={{ left: "40%", width: "20%", top: 0, height: "13%", background: "transparent" }}
        />
        <button
          type="button"
          aria-label="Clear history"
          disabled={!canEdit || runs.length === 0}
          onPointerDown={() => canEdit && runs.length > 0 && setPressed("side")}
          onPointerUp={() => setPressed(null)}
          onPointerLeave={() => setPressed(null)}
          onPointerCancel={() => setPressed(null)}
          onClick={clear}
          className="absolute rounded-xl focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[#b87333] disabled:cursor-not-allowed"
          style={{ left: "80%", width: "16%", top: "11%", height: "14%", background: "transparent" }}
        />
      </div>

      <p className="text-xs opacity-60 select-none -mt-1">
        {liveNote ?? (runs.length ? `${runs.length} run${runs.length === 1 ? "" : "s"} recorded` : "crown: start · side button: clear")}
      </p>
      <div className="flex items-center gap-1 text-[11px] select-none" role="radiogroup" aria-label="Clock source">
        <span className="opacity-50 mr-1">clock:</span>
        {(["task", "stream"] as const).map((src) => (
          <button
            key={src}
            role="radio"
            aria-checked={clockSource === src}
            disabled={!canEdit || !!current}
            onClick={() => dispatch(sourceSetEvent.dataCreator({ ...identifier, source: src }))}
            title={src === "task" ? "The durable Inngest task: survives everything, seconds per hop." : "The app's own server route streams ticks while this tab is connected: no hops, nothing outlives the request."}
            className={[
              "px-2.5 py-1 rounded-full border transition-colors",
              clockSource === src ? "bg-[#3d2f1e] text-[#efe8dc] border-[#3d2f1e] dark:bg-white/85 dark:text-[#171513] dark:border-white/85" : "border-black/15 dark:border-white/20 opacity-70 hover:opacity-100",
              "disabled:cursor-not-allowed disabled:opacity-40",
            ].join(" ")}
          >
            {src === "task" ? "durable task" : "server stream"}
          </button>
        ))}
      </div>

      {notice && (
        <p className="text-xs text-center max-w-md opacity-80 select-none" role="status">
          {notice}
        </p>
      )}
      {stuck && (
        <button
          onClick={abandon}
          className="px-4 py-2 rounded-lg text-xs font-semibold bg-white/60 dark:bg-white/10 hover:bg-white/80 dark:hover:bg-white/20 active:scale-95 transition-transform"
        >
          {current?.status === "starting" ? "The clock never answered — abandon this run" : "The clock never stopped — abandon this run"}
        </button>
      )}

      {runs.length > 0 && (
        <div className="w-full max-w-xl overflow-x-auto">
          <table className="w-full text-xs sm:text-sm" style={{ fontVariantNumeric: "tabular-nums" }}>
            <thead className="opacity-60 text-left">
              <tr>
                <th className="py-1 pr-3 font-medium">Run</th>
                <th className="py-1 pr-3 font-medium">Clock</th>
                <th className="py-1 pr-3 font-medium">Elapsed</th>
                <th className="py-1 pr-3 font-medium" title="Start sent → task ticking (kicker's clock → server clock)">Start→task</th>
                <th className="py-1 pr-3 font-medium" title="Stop sent → task ended">Stop→end</th>
                <th className="py-1 pr-3 font-medium" title="Browser: tap → first tick heard">Start heard</th>
                <th className="py-1 font-medium" title="Browser: tap → stopped heard">Stop heard</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((r: StopwatchRun) => (
                <tr key={r.runId} className="border-t border-black/5 dark:border-white/10">
                  <td className="py-1 pr-3 font-mono opacity-70">{r.runId.slice(0, 6)}</td>
                  <td className="py-1 pr-3">{r.source === "stream" ? "stream" : "task"}</td>
                  <td className="py-1 pr-3 font-mono">{r.status === "abandoned" ? "abandoned" : r.elapsedMs !== undefined ? formatMs(r.elapsedMs) : "—"}</td>
                  <td className="py-1 pr-3">{ms(r.startLatencyMs)}</td>
                  <td className="py-1 pr-3">{ms(r.stopLatencyMs)}</td>
                  <td className="py-1 pr-3">{ms(r.uiStartLatencyMs)}</td>
                  <td className="py-1">{ms(r.uiStopLatencyMs)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!canEdit && <p className="text-xs opacity-40 select-none">view only</p>}
    </div>
  );
}

const ms = (v: number | undefined) => (v === undefined ? "—" : `${v} ms`);
