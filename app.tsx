// Stopwatch — the clock runs on the server.
//
// NOT "use client": this is the SCHEMA module, evaluated in the server bundle.
// The UI lives in ./ui/stopwatch-ui.tsx.
//
// Start kicks the `tick` task. The task records `started` on the timeline,
// then runs in ~25-second chunks (one durable step each): twice a second it
// reads the FOLD, and once a second publishes a tick on the app's channel;
// when the fold says the run is stopping (or gone) it records `finished`
// and returns. Stop is therefore just an EVENT — the UI or a tool appends
// `stop_requested` straight onto the timeline and the loop observes it. The
// first build waited for a stop with `step.waitForEvent` between ticks; on
// this stack a tick step takes seconds (publish + scheduling), so the loop
// was outside its wait window most of the time and a stop landing then was
// lost forever (2026-09-10, run g7V7… ticked for 20 minutes). A loop that
// observes the timeline cannot miss anything the timeline holds.
// The UI hears ticks over realtime and interpolates between them; what
// survives a refresh is only ever the timeline. Each run also records how
// long Start and Stop took end to end — on the kicker's clock (a tool → the
// server's, both ends on one clock; the UI → the browser's, measured from tap
// to the first realtime message heard).
import { z } from "zod";
import {
  incompleteStateNotice,
  EventTypes,
  definePluginChannel,
  kickPluginTask,
  nanoid,
  type ApplicationIdentifier,
  type ApplicationSchema,
  type ApplicationPort,
  type EventData,
  type EventDefinition,
} from "esoul-sdk";
import { StopwatchUi } from "./ui/stopwatch-ui";

export const APP_TYPE = "plugin_stopwatch_gh";
/** One authoritative tick a second; the UI runs at frame rate between them. */
export const TICK_MS = 1000;
/** The fold is read this often: a stop is seen within half a second. */
export const POLL_MS = 500;
/** One durable step runs this long before the loop takes the next; well inside the route's budget. */
export const CHUNK_MS = 25_000;
/** Chunks per run before the loop hands itself to a fresh run (≈40 min; the step budget is 1000). */
export const CHUNKS_PER_RUN = 96;
export const MAX_RUNS = 50;

export interface StopwatchRun {
  runId: string;
  /** The kicker's clock when Start was sent. */
  kickedAt: number;
  /** Server clock when the task began ticking. */
  startedAt?: number;
  /** The kicker's clock when Stop was sent. */
  stopRequestedAt?: number;
  /** Server clock when the task saw the stop. */
  taskEndedAt?: number;
  /** The interval between the two presses, on the kicker's clock (known the moment Stop lands). */
  elapsedMs?: number;
  /** The task's own interval: taskEndedAt − startedAt (server clock; includes both hops). */
  taskElapsedMs?: number;
  /** startedAt − kickedAt. Exact when Start came from a tool (both ends server clock). */
  startLatencyMs?: number;
  /** taskEndedAt − stopRequestedAt. Exact when Stop came from a tool. */
  stopLatencyMs?: number;
  /** Browser: Start tapped → first tick heard over realtime. One clock. */
  uiStartLatencyMs?: number;
  /** Browser: Stop tapped → "stopped" heard over realtime. One clock. */
  uiStopLatencyMs?: number;
  /** How long the fold read that saw the stop took, inside the task. */
  readMs?: number;
  status: "starting" | "running" | "stopping" | "finished" | "abandoned";
  /** Which clock drove this run. */
  source: ClockSource;
  /** Set when the kicker gave up on a run the clock never answered. */
  abandonedAt?: number;
  reason?: string;
}

/** Which clock drives a run: the durable task (Inngest, seconds per hop) or the app's own
 * server route streaming ticks (one request, no hops — nothing outlives the request). */
export type ClockSource = "task" | "stream";

export interface StopwatchData extends ApplicationIdentifier {
  /** The source the next Start uses. */
  clockSource: ClockSource;
  current: StopwatchRun | null;
  /** Finished runs, newest first, capped. */
  runs: StopwatchRun[];
}

/** The app's realtime channel — one per instance. Tasks publish, the UI listens. */
export const stopwatchChannel = definePluginChannel({
  applicationType: APP_TYPE,
  topics: {
    tick: {
      schema: z.object({
        runId: z.string(),
        elapsedMs: z.number(),
        serverNow: z.number(),
        tick: z.number(),
        /** How long the fold read before this tick took, inside the task. */
        readMs: z.number().optional(),
        /** How long the previous tick's publish took (the broker's latency as the task sees it). */
        publishMs: z.number().optional(),
        /** The read's split, from the task context: importing the fold module vs the fold itself. */
        importMs: z.number().optional(),
        foldMs: z.number().optional(),
        /** A bare SELECT 1 beside the read: seconds here = waiting for a pooled connection. */
        pingMs: z.number().optional(),
        /** Where the task ran (VERCEL_REGION). */
        region: z.string().optional(),
        /** How late this tick fired past its boundary — seconds here = the loop's timers starved (a blocking read). */
        lagMs: z.number().optional(),
        /** How long the fold read beside this tick had been in flight when it fired. */
        readInflightMs: z.number().optional(),
      }),
    },
    stopped: {
      schema: z.object({
        runId: z.string(),
        elapsedMs: z.number(),
        taskEndedAt: z.number(),
        stopRequestedAt: z.number().optional(),
        readMs: z.number().optional(),
      }),
    },
  },
});

const envelope = (eventName: string, args: Record<string, any>, eventData: unknown): EventData<any> => ({
  eventName,
  eventData,
  timestamp: Date.now(),
  workspaceId: args.workspaceId,
  applicationId: args.applicationId || args.nodeId,
  instanceName: args.instanceName,
  chatIdSource: args.chatIdSource,
});

const num = (v: unknown): number | undefined => (typeof v === "number" && Number.isFinite(v) ? v : undefined);
const str = (v: unknown): string => (typeof v === "string" ? v : "");

function findRun(state: StopwatchData, runId: string): StopwatchRun | undefined {
  if (state.current?.runId === runId) return state.current;
  return (state.runs ?? []).find((r) => r.runId === runId);
}

function patchRun(state: StopwatchData, runId: string, patch: Partial<StopwatchRun>): StopwatchData {
  if (state.current?.runId === runId) return { ...state, current: { ...state.current, ...patch } };
  const runs = state.runs ?? [];
  if (!runs.some((r) => r.runId === runId)) return state;
  return { ...state, runs: runs.map((r) => (r.runId === runId ? { ...r, ...patch } : r)) };
}

/** Start was sent. Idempotent by runId; one run at a time. */
export const startRequestedEvent: EventDefinition<StopwatchData> = {
  eventName: "plugin_stopwatch_gh_start_requested",
  type: EventTypes.Client,
  dataCreator: (args) =>
    envelope("plugin_stopwatch_gh_start_requested", args, {
      runId: args.runId ?? nanoid(),
      kickedAt: args.kickedAt ?? Date.now(),
      source: args.source,
    }),
  processor: (state, event) => {
    const d = event.eventData || {};
    const runId = str(d.runId);
    const kickedAt = num(d.kickedAt);
    if (!runId || kickedAt === undefined) return state;
    if (findRun(state, runId)) return state; // idempotent
    if (state.current && state.current.status !== "finished") return state; // one at a time
    const source: ClockSource = d.source === "stream" || d.source === "task" ? d.source : state.clockSource ?? "task";
    return { ...state, current: { runId, kickedAt, status: "starting", source } };
  },
};

/** The task began ticking (server). Creates the run if the request never folded. */
export const startedEvent: EventDefinition<StopwatchData> = {
  eventName: "plugin_stopwatch_gh_started",
  type: EventTypes.Client,
  dataCreator: (args) =>
    envelope("plugin_stopwatch_gh_started", args, {
      runId: args.runId,
      startedAt: args.startedAt,
      kickedAt: args.kickedAt,
      source: args.source,
    }),
  processor: (state, event) => {
    const d = event.eventData || {};
    const runId = str(d.runId);
    const startedAt = num(d.startedAt);
    if (!runId || startedAt === undefined) return state;
    const existing = findRun(state, runId);
    if (existing?.startedAt !== undefined || existing?.status === "abandoned") return state; // idempotent; an abandoned run stays abandoned
    if (existing) {
      return patchRun(state, runId, {
        startedAt,
        status: existing.status === "stopping" ? "stopping" : "running",
        startLatencyMs: Math.max(0, startedAt - existing.kickedAt),
      });
    }
    if (state.current && state.current.status !== "finished") return state;
    const kickedAt = num(d.kickedAt) ?? startedAt;
    return {
      ...state,
      current: { runId, kickedAt, startedAt, startLatencyMs: Math.max(0, startedAt - kickedAt), status: "running", source: d.source === "stream" ? "stream" : "task" },
    };
  },
};

/** Stop was sent (recorded by the `stop` task). runId "" means "whatever is running". */
export const stopRequestedEvent: EventDefinition<StopwatchData> = {
  eventName: "plugin_stopwatch_gh_stop_requested",
  type: EventTypes.Client,
  dataCreator: (args) =>
    envelope("plugin_stopwatch_gh_stop_requested", args, {
      runId: args.runId ?? "",
      stopRequestedAt: args.stopRequestedAt ?? Date.now(),
    }),
  processor: (state, event) => {
    const d = event.eventData || {};
    const stopRequestedAt = num(d.stopRequestedAt);
    if (stopRequestedAt === undefined) return state;
    const runId = str(d.runId) || state.current?.runId || "";
    if (!runId) return state;
    const run = findRun(state, runId);
    if (!run || run.stopRequestedAt !== undefined) return state; // idempotent
    const patch: Partial<StopwatchRun> = { stopRequestedAt, elapsedMs: Math.max(0, stopRequestedAt - run.kickedAt) };
    if (run.status !== "finished") patch.status = "stopping";
    else if (run.taskEndedAt !== undefined) patch.stopLatencyMs = Math.max(0, run.taskEndedAt - stopRequestedAt);
    return patchRun(state, runId, patch);
  },
};

/** The task saw the stop and returned. The authoritative elapsed time. */
export const finishedEvent: EventDefinition<StopwatchData> = {
  eventName: "plugin_stopwatch_gh_finished",
  type: EventTypes.Client,
  triggerMeta: {
    displayName: "Stopwatch stopped",
    description: "Fires when a run of this stopwatch finishes. eventData: {runId, elapsedMs, taskEndedAt}.",
    sampleVariables: ["event.elapsedMs"],
  },
  dataCreator: (args) =>
    envelope("plugin_stopwatch_gh_finished", args, {
      runId: args.runId,
      elapsedMs: args.elapsedMs,
      taskEndedAt: args.taskEndedAt,
      stopRequestedAt: args.stopRequestedAt,
      // A route finishes through emitPluginAppEvent, which mints via THIS creator: what is
      // not named here is dropped (the stream arm's first run lost its readMs, 2026-09-10).
      kickedAt: args.kickedAt,
      readMs: args.readMs,
      source: args.source,
    }),
  processor: (state, event) => {
    const d = event.eventData || {};
    const runId = str(d.runId);
    const elapsedMs = num(d.elapsedMs);
    const taskEndedAt = num(d.taskEndedAt);
    if (!runId || elapsedMs === undefined || taskEndedAt === undefined) return state;
    if ((state.runs ?? []).some((r) => r.runId === runId)) return state; // idempotent, and an abandoned run stays abandoned
    const base: StopwatchRun =
      state.current?.runId === runId
        ? state.current
        : { runId, kickedAt: num(d.kickedAt) ?? taskEndedAt - elapsedMs, status: "running", source: d.source === "stream" ? "stream" : "task" };
    const stopRequestedAt = base.stopRequestedAt ?? num(d.stopRequestedAt);
    const finished: StopwatchRun = {
      ...base,
      startedAt: base.startedAt ?? taskEndedAt - elapsedMs,
      // The user's interval wins when both presses were stamped; the task's is the fallback
      // (a run stopped by the task alone — abandon by the owner, or a stop the fold carried
      // without a kickedAt), and is always kept beside it.
      elapsedMs:
        base.elapsedMs ??
        (state.current?.runId === runId && stopRequestedAt !== undefined ? Math.max(0, stopRequestedAt - base.kickedAt) : elapsedMs),
      taskElapsedMs: elapsedMs,
      taskEndedAt,
      stopRequestedAt,
      stopLatencyMs: stopRequestedAt !== undefined ? Math.max(0, taskEndedAt - stopRequestedAt) : undefined,
      readMs: num(d.readMs),
      status: "finished",
    };
    return {
      ...state,
      current: state.current?.runId === runId ? null : state.current,
      runs: [finished, ...(state.runs ?? [])].slice(0, MAX_RUNS),
    };
  },
};

/** What the browser measured on its own clock. Merged into the run. */
export const uiLatencyEvent: EventDefinition<StopwatchData> = {
  eventName: "plugin_stopwatch_gh_ui_latency",
  type: EventTypes.Client,
  dataCreator: (args) =>
    envelope("plugin_stopwatch_gh_ui_latency", args, {
      runId: args.runId,
      uiStartLatencyMs: args.uiStartLatencyMs,
      uiStopLatencyMs: args.uiStopLatencyMs,
    }),
  processor: (state, event) => {
    const d = event.eventData || {};
    const runId = str(d.runId);
    if (!runId) return state;
    const patch: Partial<StopwatchRun> = {};
    const s = num(d.uiStartLatencyMs);
    const t = num(d.uiStopLatencyMs);
    if (s !== undefined) patch.uiStartLatencyMs = s;
    if (t !== undefined) patch.uiStopLatencyMs = t;
    if (!Object.keys(patch).length) return state;
    return patchRun(state, runId, patch);
  },
};

/** The kicker gave up on a run the clock never answered (or never stopped). Recorded as
 * abandoned — never as a finished time. Idempotent by runId. */
export const abandonedEvent: EventDefinition<StopwatchData> = {
  eventName: "plugin_stopwatch_gh_abandoned",
  type: EventTypes.Client,
  dataCreator: (args) =>
    envelope("plugin_stopwatch_gh_abandoned", args, {
      runId: args.runId,
      abandonedAt: args.abandonedAt ?? Date.now(),
      reason: args.reason ?? "",
    }),
  processor: (state, event) => {
    const d = event.eventData || {};
    const runId = str(d.runId);
    const abandonedAt = num(d.abandonedAt);
    if (!runId || abandonedAt === undefined) return state;
    if (state.current?.runId !== runId) return state; // gone already, or never ours
    const run: StopwatchRun = { ...state.current, status: "abandoned", abandonedAt, reason: str(d.reason) || undefined };
    return { ...state, current: null, runs: [run, ...(state.runs ?? [])].slice(0, MAX_RUNS) };
  },
};

/** Choose the clock the next Start uses. Collapsible: the last choice wins. */
export const sourceSetEvent: EventDefinition<StopwatchData> = {
  eventName: "plugin_stopwatch_gh_source_set",
  type: EventTypes.Client,
  dataCreator: (args) => envelope("plugin_stopwatch_gh_source_set", args, { source: args.source }),
  collapseConfig: {
    collapseKeyFn: (_eventData, context) => `${context.applicationId}:stopwatch-source`,
    collapseWindowMs: 2000,
  },
  processor: (state, event) => {
    const src = event.eventData?.source;
    if (src !== "task" && src !== "stream") return state;
    return { ...state, clockSource: src };
  },
};

/** Clear the history. Whole-replace — keyed by applicationId. The current run is kept. */
export const clearedEvent: EventDefinition<StopwatchData> = {
  eventName: "plugin_stopwatch_gh_cleared",
  type: EventTypes.Client,
  dataCreator: (args) => envelope("plugin_stopwatch_gh_cleared", args, { at: args.at ?? Date.now() }),
  collapseConfig: {
    collapseKeyFn: (_eventData, context) => `${context.applicationId}:stopwatch-clear`,
    collapseWindowMs: 4000,
  },
  processor: (state) => ({ ...state, runs: [] }),
};

export function formatMs(ms: number): string {
  const total = Math.max(0, Math.floor(ms));
  const m = Math.floor(total / 60000);
  const s = Math.floor((total % 60000) / 1000);
  const cs = Math.floor((total % 1000) / 10);
  return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}.${String(cs).padStart(2, "0")}`;
}

function describeRun(r: StopwatchRun): string {
  const bits: string[] = [r.source === "stream" ? "server stream" : "durable task"];
  if (r.status === "abandoned") bits.push(`abandoned${r.reason ? ` — ${r.reason}` : ""}`);
  if (r.elapsedMs !== undefined) bits.push(`elapsed ${formatMs(r.elapsedMs)}`);
  if (r.taskElapsedMs !== undefined && r.taskElapsedMs !== r.elapsedMs) bits.push(`task saw ${formatMs(r.taskElapsedMs)}`);
  if (r.startLatencyMs !== undefined) bits.push(`start→task ${r.startLatencyMs} ms`);
  if (r.stopLatencyMs !== undefined) bits.push(`stop→task end ${r.stopLatencyMs} ms`);
  if (r.readMs !== undefined) bits.push(`fold read ${r.readMs} ms`);
  if (r.uiStartLatencyMs !== undefined) bits.push(`start→first tick heard ${r.uiStartLatencyMs} ms`);
  if (r.uiStopLatencyMs !== undefined) bits.push(`stop→stopped heard ${r.uiStopLatencyMs} ms`);
  return `${r.runId} (${r.status}): ${bits.join(", ") || "no measurements yet"}`;
}

export function describeStopwatch(s: Pick<StopwatchData, "current" | "runs" | "instanceName">): string {
  const lines = [`Stopwatch "${s.instanceName}" (next Start uses the ${(s as Partial<StopwatchData>).clockSource === "stream" ? "server stream" : "durable task"}).`];
  lines.push(s.current ? `Current run — ${describeRun(s.current)}.` : "Not running.");
  const runs = s.runs ?? [];
  lines.push(
    runs.length
      ? `${runs.length} finished run(s), newest first: ${runs.slice(0, 5).map(describeRun).join("; ")}.`
      : "No finished runs.",
  );
  lines.push(
    "Elapsed is the interval between the two presses on the presser's clock; 'task saw' is the durable task's own interval (it includes the start and stop hops). start→task and stop→task end are exact when Start/Stop came from a tool (server clock both ends); the *heard* figures are the browser's own clock, tap to realtime message.",
  );
  return lines.join(" ");
}

export const pluginSchema: ApplicationSchema<StopwatchData> = {
  applicationType: APP_TYPE,
  description:
    "A stopwatch whose clock runs on the server: Start kicks a durable task that publishes a tick a second over realtime; Stop is an event that task waits for. Each run records its elapsed time and how long start and stop took end to end.",
  reactNode: StopwatchUi,
  reconstructStateFromEventLog: true,
  events: [startRequestedEvent, startedEvent, stopRequestedEvent, finishedEvent, uiLatencyEvent, abandonedEvent, sourceSetEvent, clearedEvent],
  getPorts: (): ApplicationPort[] => [],
  stateCreator: (identifier) => ({ ...identifier, clockSource: "task", current: null, runs: [] }),
  channel: stopwatchChannel,

  getStateDescription: (state: StopwatchData) => {
    const notice = incompleteStateNotice({
      title: "Stopwatch",
      instanceName: state?.instanceName,
      shape: { lists: { runs: state?.runs } },
    });
    if (notice) return notice;
    return describeStopwatch(state);
  },

  tasks: [
    {
      taskName: "tick",
      description:
        "The clock. Records `started`, then in 25-second chunks ticks once a second on the server clock with a fold read always in flight beside it; when the fold says the run is stopping or gone it records `finished` and returns. Fresh run every 96 chunks.",
      concurrency: { limit: 1, scope: "per-app" },
      handler: async (ctx) => {
        const runId = str(ctx.eventData.runId);
        if (!runId) return;
        const kickedAt = num(ctx.eventData.kickedAt);
        const pollMs = num(ctx.eventData.pollMs) ?? POLL_MS;
        const tickMs = num(ctx.eventData.tickMs) ?? TICK_MS;
        const chunkMs = num(ctx.eventData.chunkMs) ?? CHUNK_MS;
        // The clock is minted inside the FIRST chunk (one Inngest hop from the kick,
        // not two: every step boundary is a fresh invocation of the platform's
        // function — measured 7 s between `start` and `chunk-0` on 2026-09-10).
        // A continuation carries the original clock.
        const from = num(ctx.eventData.chunkFrom) ?? 0;
        let tick = num(ctx.eventData.tickFrom) ?? 0;
        let startedAt = num(ctx.eventData.startedAt);
        for (let c = from; c < from + CHUNKS_PER_RUN; c++) {
          const out = (await ctx.step.run(`chunk-${c}`, async () => {
            let began = startedAt;
            // A chunk that mints `started` itself knows the run is live — it may tick before its
            // first read lands. A continuation (startedAt carried in) must not: the fold may no
            // longer hold the run (abandoned), and a run nobody holds must not hear a tick.
            let confirmed = began !== undefined ? false : true;
            let startedWrite: Promise<void> | null = null;
            const settle = async () => { if (startedWrite) { await startedWrite; startedWrite = null; } };
            const until = Date.now() + chunkMs;
            let n = tick;
            let lastTickAt = 0;
            let publishMs: number | undefined;
            let readMs: number | undefined;
            if (began === undefined) {
              // Idempotent on retry: the processor keeps the first startedAt it saw.
              began = Date.now();
              // The first tick goes out BEFORE `started` is recorded: recording it is a
              // timeline write from the task's region (~4 s from iad1 — the battery's first
              // tick sat at 5.9–8.0 s after the kick with 1.9–4.0 s of that the hop, 2026-09-10).
              // The run exists — its own start event kicked this task — and a face is allowed
              // to move before the fold says `running`.
              lastTickAt = began;
              n += 1;
              const p0 = Date.now();
              try {
                await ctx.notify("tick", { runId, elapsedMs: 0, serverNow: began, tick: n, lagMs: 0, readInflightMs: 0 });
                publishMs = Date.now() - p0;
              } catch {
                publishMs = -1;
              }
              // The record runs BESIDE the ticks (a ~4 s timeline write from iad1 froze the
              // hand for one interval after the first tick); it is awaited before any return,
              // never left behind by its step.
              startedWrite = ctx.dispatchEvent("plugin_stopwatch_gh_started", { runId, startedAt: began, kickedAt });
            }
            // The tick and the read are two cadences, not one loop: a tick is the server clock
            // (elapsed = now − began, published in ~30 ms) and needs no fold; the fold is read
            // only to see a stop, and on this stack a read is ~3.4 s from the task (iad1 → Neon
            // eu-central, §5). Chained, the clock ticked once per read — every 4 s, the first at
            // 7 s (2026-09-10). Decoupled: a read is always in flight, ticks land every tickMs
            // regardless once the first read has confirmed the run, and a stop is seen within one read plus
            // pollMs, as before.
            let inflight: Promise<StopwatchData> | null = null;
            let readStartedAt = 0;
            let nextReadAt = 0;
            for (;;) {
              const now = Date.now();
              if (!inflight && now >= nextReadAt) {
                readStartedAt = now;
                inflight = ctx.getState() as Promise<StopwatchData>;
              }
              // The cadence is the clock's once the run is confirmed live — by this chunk having
              // minted `started`, or by the first read landing (a continuation). On this stack the
              // first read is ~3.4 s from the task (§5) and the kick→task hop 3–14 s: a 10 s run
              // waiting on both ended before its first tick in 3 of 5 battery runs (2026-09-10).
              if (confirmed && now - lastTickAt >= tickMs) {
                const lagMs = n > tick ? Math.max(0, now - lastTickAt - tickMs) : 0;
                const readInflightMs = inflight ? now - readStartedAt : 0;
                lastTickAt = now;
                n += 1;
                const p0 = Date.now();
                // AWAITED, never fire-and-forget: a publish left dangling inside a step is
                // orphaned when the step returns — on production not one tick reached the
                // channel while the awaited `stopped` always did, and the function log
                // showed the orphans timing out in bursts after every run (2026-09-10).
                // The host's publish is time-boxed (publishWithTimeout), so a slow broker
                // costs this tick at most that box, never the step; a failure is -1 on the
                // next tick, and the host warns.
                const split = ctx.timing?.lastGetState;
                try {
                  await ctx.notify("tick", { runId, elapsedMs: now - began, serverNow: now, tick: n, readMs, publishMs, importMs: split?.importMs, foldMs: split?.foldMs, pingMs: split?.pingMs, region: split?.region, lagMs, readInflightMs });
                  publishMs = Date.now() - p0;
                } catch {
                  publishMs = -1;
                }
              }
              if (Date.now() >= until) { await settle(); return { done: "chunk" as const, tick: n, startedAt: began }; }
              // Wait for whichever comes first: the read landing, or the next tick boundary.
              const untilTick = Math.max(0, tickMs - (Date.now() - lastTickAt));
              const landed = await Promise.race([
                inflight ? inflight.then((state) => ({ state })) : new Promise<{ state?: StopwatchData }>((r) => setTimeout(() => r({}), Math.min(untilTick, pollMs))),
                new Promise<{ state?: StopwatchData }>((r) => setTimeout(() => r({}), untilTick)),
              ]);
              if (landed.state) {
                readMs = Date.now() - readStartedAt;
                confirmed = true;
                inflight = null;
                nextReadAt = Date.now() + pollMs;
                const cur = landed.state.current;
                if (!cur || cur.runId !== runId) { await settle(); return { done: "gone" as const, tick: n, startedAt: began }; }
                if (cur.status === "stopping" || cur.status === "abandoned") {
                  await settle();
                  return { done: "stop" as const, tick: n, startedAt: began, stopRequestedAt: cur.stopRequestedAt, taskEndedAt: Date.now(), readMs };
                }
              }
            }
          })) as { done: "gone" | "stop" | "chunk"; tick: number; startedAt: number; stopRequestedAt?: number; taskEndedAt?: number; readMs?: number };
          tick = out.tick;
          startedAt = out.startedAt;
          if (out.done === "gone") return; // abandoned, or never ours — the fold already says so
          if (out.done === "stop") {
            const began = out.startedAt;
            await ctx.step.run("finish", async () => {
              const taskEndedAt = out.taskEndedAt ?? Date.now();
              const elapsedMs = taskEndedAt - began;
              await ctx.notify("stopped", { runId, elapsedMs, taskEndedAt, stopRequestedAt: out.stopRequestedAt, readMs: out.readMs }).catch(() => {});
              await ctx.dispatchEvent("plugin_stopwatch_gh_finished", { runId, elapsedMs, taskEndedAt, stopRequestedAt: out.stopRequestedAt, kickedAt, readMs: out.readMs });
            });
            return;
          }
        }
        await ctx.step.sendEvent("continue", {
          name: `${APP_TYPE}/tick`,
          data: { ...ctx.eventData, runId, startedAt, chunkFrom: from + CHUNKS_PER_RUN, tickFrom: tick },
        });
      },
    },
  ],

  toolkitCreator: (identifier, forChatId, eventCallback) => {
    const base = identifier.instanceName.replace(/[^a-zA-Z0-9]/g, "_");
    const idArgs = { ...identifier, applicationId: identifier.nodeId, chatIdSource: forChatId };
    const tools: Record<string, any> = {
      [`read_stopwatch_${base}`]: {
        description: `Read "${identifier.instanceName}": the current run, finished runs and their measured latencies.`,
        parameters: z.object({}),
        readOnly: true,
        publicSafe: true,
        execute: async () => {
          const { callPluginOp } = await import("esoul-sdk");
          const s = await callPluginOp<Pick<StopwatchData, "current" | "runs" | "instanceName">>("stopwatch-gh", "read-state", identifier.nodeId);
          return describeStopwatch(s);
        },
      },
      [`start_stopwatch_${base}`]: {
        description: `Start "${identifier.instanceName}". source "task" (default: the durable Inngest task, survives everything, seconds per hop) or "stream" (the app's own server route streams ticks while someone is connected — no hops, nothing outlives the request). One run at a time.`,
        parameters: z.object({ source: z.enum(["task", "stream"]).optional() }),
        execute: async (args) => {
          const runId = nanoid();
          const kickedAt = Date.now();
          if (args.source === "stream") {
            eventCallback(startRequestedEvent.dataCreator({ ...idArgs, runId, kickedAt, source: "stream" }));
            return `Started run ${runId} on the server stream; the clock ticks for whoever is connected to the ticks route, and the run is finished by that route when Stop lands.`;
          }
          let kicked: { ok: boolean; status: number };
          try {
            kicked = await kickPluginTask({ applicationType: APP_TYPE, taskName: "tick", identifier, data: { runId, kickedAt } });
          } catch (e) {
            return `Not started: the clock task could not be kicked from here (${(e as Error).message}). A preview has no task runtime — once installed, Start ticks.`;
          }
          if (!kicked.ok) return `Not started: the clock task was refused (HTTP ${kicked.status}).`;
          eventCallback(startRequestedEvent.dataCreator({ ...idArgs, runId, kickedAt, source: "task" }));
          return `Started run ${runId}; the durable clock is ticking. Stop it with stop_stopwatch.`;
        },
      },
      [`set_clock_source_${base}`]: {
        description: `Choose the clock the next Start of "${identifier.instanceName}" uses: "task" (durable) or "stream" (the server route).`,
        parameters: z.object({ source: z.enum(["task", "stream"]) }),
        execute: async (args) => {
          eventCallback(sourceSetEvent.dataCreator({ ...idArgs, source: args.source }));
          return `Next Start uses the ${args.source === "stream" ? "server stream" : "durable task"}.`;
        },
      },
      [`stop_stopwatch_${base}`]: {
        description: `Stop "${identifier.instanceName}". Appends the stop to the timeline; the running clock observes it and records the final time.`,
        parameters: z.object({}),
        execute: async () => {
          eventCallback(stopRequestedEvent.dataCreator({ ...idArgs, runId: "", stopRequestedAt: Date.now() }));
          return "Stop recorded; the clock records the final time as plugin_stopwatch_gh_finished within a second.";
        },
      },
      [`abandon_stopwatch_run_${base}`]: {
        description: `Give up on the current run of "${identifier.instanceName}" when the clock never answered or never stopped. Recorded as abandoned, never as a time. Pass the runId from read_stopwatch.`,
        parameters: z.object({ runId: z.string().min(1), reason: z.string().max(200).optional() }),
        execute: async (args) => {
          eventCallback(abandonedEvent.dataCreator({ ...idArgs, runId: args.runId, reason: args.reason }));
          return `Run ${args.runId} recorded as abandoned.`;
        },
      },
      [`clear_stopwatch_history_${base}`]: {
        description: `Clear the finished runs of "${identifier.instanceName}" (the current run is kept). Requires confirm:true.`,
        parameters: z.object({ confirm: z.literal(true).describe("Must be true — this clears the history") }),
        execute: async (args) => {
          if (args.confirm !== true) return "Not cleared — pass confirm:true.";
          eventCallback(clearedEvent.dataCreator({ ...idArgs }));
          return "History cleared.";
        },
      },
    };
    // Browser surfaces (voice, WebMCP) run a tool through `onClient` — the same work as
    // `execute`; never a stub the voice runtime would report as success.
    for (const t of Object.values(tools)) t.onClient = t.execute;
    return tools;
  },
};
