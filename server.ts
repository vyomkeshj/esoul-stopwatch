// The one server op: the folded state for read_stopwatch (chat/voice/MCP read the same fold the UI renders).
// And the app's own backend: `ticks`, a clock streamed straight from a server request.
import "server-only";
import { emitPluginAppEvent, readAppState, sseStream } from "esoul-sdk/server";
import type { PluginOpContext, PluginRouteContext, PluginServerModule } from "esoul-sdk/server";
import { APP_TYPE, type StopwatchData } from "./app";

async function readState(ctx: PluginOpContext): Promise<Pick<StopwatchData, "current" | "runs" | "instanceName" | "clockSource">> {
  const folded = await readAppState(ctx.nodeId);
  if (!folded) throw new Error(`no Stopwatch app ${ctx.nodeId}`);
  const s = folded.state as Partial<StopwatchData>;
  if (!Array.isArray(s.runs)) throw new Error("stopwatch did not fold (runs missing)");
  return { current: s.current ?? null, runs: s.runs, instanceName: String(s.instanceName ?? ""), clockSource: s.clockSource ?? "task" };
}

/**
 * The comparison arm to the durable task: one Node request, a fold read a second,
 * a tick a second, no scheduler hops. When the fold says a STREAM run is stopping,
 * this route finishes it (a writer only) — and if nobody is connected, nobody does:
 * that is the trade, stated on the run.
 */
async function ticks(ctx: PluginRouteContext): Promise<Response> {
  const pollMs = Math.max(250, Number(ctx.searchParams.get("pollMs") ?? 1000));
  return sseStream(
    async (send, signal) => {
      const openedAt = Date.now();
      let n = 0;
      while (!signal.aborted && Date.now() - openedAt < 280_000) {
        const t0 = Date.now();
        const app = await readAppState(ctx.nodeId);
        const readMs = Date.now() - t0;
        const s = (app?.state ?? {}) as Partial<StopwatchData>;
        const cur = s.current ?? null;
        if (!cur) {
          send("idle", { readMs, runs: (s.runs ?? []).length });
        } else if (cur.source === "stream" && (cur.status === "stopping" || cur.status === "abandoned")) {
          if (ctx.canWrite && cur.status === "stopping") {
            const taskEndedAt = Date.now();
            const elapsedMs = taskEndedAt - cur.kickedAt;
            await emitPluginAppEvent({
              source: { pluginId: ctx.pluginId, workspaceId: ctx.workspaceId, nodeId: ctx.nodeId, applicationType: APP_TYPE },
              targetNodeId: ctx.nodeId,
              eventName: "plugin_stopwatch_gh_finished",
              eventData: { runId: cur.runId, elapsedMs, taskEndedAt, stopRequestedAt: cur.stopRequestedAt, kickedAt: cur.kickedAt, readMs, source: "stream" },
            });
            send("stopped", { runId: cur.runId, elapsedMs, taskEndedAt, stopRequestedAt: cur.stopRequestedAt, readMs });
          } else {
            send("stopping", { runId: cur.runId, readMs, note: ctx.canWrite ? "abandoned" : "a viewer cannot finish a run" });
          }
        } else if (cur.source === "stream") {
          n += 1;
          const now = Date.now();
          if (cur.status === "starting" && ctx.canWrite) {
            // The route IS the clock for a stream run: its first tick is the start. (Only the
            // task recorded `started` before — a stream run stayed "starting" while ticking.)
            await emitPluginAppEvent({
              source: { pluginId: ctx.pluginId, workspaceId: ctx.workspaceId, nodeId: ctx.nodeId, applicationType: APP_TYPE },
              targetNodeId: ctx.nodeId,
              eventName: "plugin_stopwatch_gh_started",
              eventData: { runId: cur.runId, startedAt: now, kickedAt: cur.kickedAt },
            });
          }
          send("tick", { runId: cur.runId, elapsedMs: now - cur.kickedAt, serverNow: now, tick: n, readMs });
        } else {
          send("task-run", { runId: cur.runId, status: cur.status, readMs });
        }
        await new Promise((r) => setTimeout(r, pollMs));
      }
    },
    { signal: ctx.request.signal },
  );
}

export const pluginServer: PluginServerModule = {
  ops: { "read-state": readState },
  routes: { ticks },
};
