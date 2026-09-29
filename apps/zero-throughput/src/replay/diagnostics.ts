import {mkdir, writeFile} from 'node:fs/promises';
import {Session} from 'node:inspector/promises';
import {join} from 'node:path';
import type {Phase} from './recorder.ts';

/**
 * Captures CPU profiles of every zero-cache process at once through its
 * `/profz` endpoint (dispatcher, syncer workers, and so on), and writes one
 * `.cpuprofile` per process. Returns the files written.
 */
export async function captureProfiles(options: {
  readonly cacheURL: string;
  readonly seconds: number;
  readonly adminPassword: string | undefined;
  readonly outDir: string;
  readonly label: string;
}): Promise<string[]> {
  const {cacheURL, seconds, adminPassword, outDir, label} = options;
  const headers: Record<string, string> = {};
  if (adminPassword !== undefined) {
    headers.authorization = `Basic ${Buffer.from(`admin:${adminPassword}`).toString('base64')}`;
  }
  const response = await fetch(
    new URL(`/profz?duration=${seconds}`, cacheURL),
    {headers},
  );
  if (!response.ok) {
    throw new Error(`/profz returned HTTP ${response.status}`);
  }
  const body = (await response.json()) as Record<string, unknown>;
  await mkdir(outDir, {recursive: true});
  // One profile, or {[process]: profile}.
  const profiles =
    'nodes' in body && Array.isArray(body.nodes)
      ? {process: body}
      : (body as Record<string, unknown>);
  const files: string[] = [];
  for (const [name, profile] of Object.entries(profiles)) {
    const file = join(outDir, `${label}-${name}.cpuprofile`);
    await writeFile(file, JSON.stringify(profile));
    files.push(file);
  }
  return files;
}

export type ResetSummary = {
  readonly total: number;
  readonly byPhase: Readonly<Record<string, number>>;
  readonly byReason: Readonly<Record<string, number>>;
};

const RESET = /resetting pipelines: (.*)$/;
// zero-cache's text logs start each entry with its timestamp but can wrap a
// long context over several lines, so a message line may not carry one.
const TIMESTAMP = /^(\d{4}-\d\d-\d\dT\S+)\s/;

/**
 * Counts zero-cache's pipeline resets (`resetting pipelines: <message>`)
 * by run phase and by the message's leading words.
 */
export function summarizeResets(
  logText: string,
  runStartWallMs: number,
  phases: readonly Phase[],
): ResetSummary {
  const byPhase: Record<string, number> = {};
  const byReason: Record<string, number> = {};
  let total = 0;
  let lastTimestampMs = Number.NaN;
  for (const line of logText.split('\n')) {
    const stamp = TIMESTAMP.exec(line);
    if (stamp !== null) {
      lastTimestampMs = Date.parse(stamp[1]);
    }
    const match = RESET.exec(line);
    if (match === null) {
      continue;
    }
    total++;
    const t = lastTimestampMs - runStartWallMs;
    const phase =
      phases.find(p => t >= p.startMs && t < p.endMs)?.label ??
      (Number.isNaN(t) ? 'unknown' : t < 0 ? 'before run' : 'after run');
    byPhase[phase] = (byPhase[phase] ?? 0) + 1;
    const reason = match[1].split(' ').slice(0, 4).join(' ');
    byReason[reason] = (byReason[reason] ?? 0) + 1;
  }
  return {total, byPhase, byReason};
}

/**
 * Now, on the clock V8 stamps CPU profile samples with, in microseconds.
 * It is not process.hrtime's clock: on macOS the two differ by the time the
 * machine has slept. Reads it from a momentary profile of this process.
 */
export async function profileClockMicros(): Promise<number> {
  const session = new Session();
  session.connect();
  try {
    await session.post('Profiler.enable');
    await session.post('Profiler.start');
    const {profile} = await session.post('Profiler.stop');
    return profile.endTime;
  } finally {
    session.disconnect();
  }
}
