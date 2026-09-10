/**
 * The face's one rule: THE CLOCK MOVES ONLY WHEN A TICK ARRIVES.
 *
 * No frame-rate interpolation, no wall-clock extrapolation from startedAt, not even for the tab
 * that pressed Start. What the face shows is the last tick the clock's authority published (the Inngest task
 * or the app's own server route), or the final time once a stop has been recorded. A stalled
 * task therefore shows as a stalled hand — which is the point of an instrument: the face is the
 * server's truth, never a local guess that keeps sweeping while the clock behind it is dead
 * (2026-09-10, the user: "clock only moves when the tick arrives").
 *
 * Pure so the rule is a unit test: this module takes no clock and reads no time.
 */
export type ClockTick = { runId: string; elapsedMs: number; tick: number };
export type ClockFace = {
  /** The last tick heard, for the run it names. */
  lastTick: ClockTick | null;
  /** The final time the clock's authority published for the last finished run. */
  finalMs: number | null;
};
export const EMPTY_FACE: ClockFace = { lastTick: null, finalMs: null };

/** Fold one realtime/stream message into the face. Anything not a tick or a stop leaves it alone. */
export function foldClockMessage(face: ClockFace, msg: { topic: string; data: unknown }): ClockFace {
  const d = msg.data as Partial<ClockTick> & { taskEndedAt?: number } | null;
  if (!d || typeof d.runId !== "string" || typeof d.elapsedMs !== "number") return face;
  if (msg.topic === "tick") {
    // Ticks arrive in order per run; a late duplicate never moves the hand backwards.
    if (face.lastTick && face.lastTick.runId === d.runId && (d.tick ?? 0) < face.lastTick.tick) return face;
    return { lastTick: { runId: d.runId, elapsedMs: d.elapsedMs, tick: d.tick ?? 0 }, finalMs: null };
  }
  if (msg.topic === "stopped") return { lastTick: null, finalMs: d.elapsedMs };
  return face;
}

export type ClockRun = { runId: string; status: string; elapsedMs?: number };

/**
 * What the face shows. Precedence: the fold's recorded time for the current run (a stop that
 * has folded) → the last tick heard, for the current run or for a run this tab has not learned
 * yet → a known run with no tick yet shows 0 (the hand has not moved: nothing has arrived) → the
 * final time of the last stop heard → the last finished run in the fold → 0.
 */
export function shownMs(face: ClockFace, current: ClockRun | null | undefined, lastFinishedMs: number | undefined): number {
  if (current?.elapsedMs !== undefined) return current.elapsedMs;
  // A tick moves the hand whether or not this tab's fold has learned the run yet: a VIEWER
  // (a tab that did not press, a run started by a tool or another device) hears the tick on
  // the channel before the fold's invalidation lands. Only a tick for a run this tab knows
  // has been superseded is ignored. (The first cut required the fold to know the run — a
  // viewer's hand never moved while tools drove the clock, 2026-09-10.)
  if (face.lastTick && (!current || face.lastTick.runId === current.runId)) return face.lastTick.elapsedMs;
  if (current) return 0;
  if (face.finalMs !== null) return face.finalMs;
  return lastFinishedMs ?? 0;
}
