import type {QueryKind} from './sync-client.ts';

export type Distribution = {
  readonly count: number;
  readonly p50: number;
  readonly p95: number;
  readonly p99: number;
  readonly max: number;
};

export function distribution(values: readonly number[]): Distribution {
  if (values.length === 0) {
    return {count: 0, p50: 0, p95: 0, p99: 0, max: 0};
  }
  const sorted = values.toSorted((a, b) => a - b);
  const at = (p: number) =>
    sorted[Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1)] ?? 0;
  return {
    count: sorted.length,
    p50: round(at(0.5)),
    p95: round(at(0.95)),
    p99: round(at(0.99)),
    max: round(sorted.at(-1) ?? 0),
  };
}

type Sample = {readonly t: number; readonly ms: number};

type HydrationSample = Sample & {
  readonly name: string;
  readonly kind: QueryKind;
  readonly returning: boolean;
};

type FailureKind = 'unexpectedClose' | 'serverError' | 'queryError';

type Counters = {
  sessionsStarted: number;
  sessionsEnded: number;
  unexpectedCloses: number;
  serverErrors: number;
  queryErrors: number;
  pokes: number;
  pokeRows: number;
  pokeBytes: number;
  backfillPages: number;
  backfillRowsRead: number;
  backfillRowsWritten: number;
  appWrites: number;
  appWriteErrors: number;
};

type Gauges = {
  activeSessions: number;
  pendingQueries: number;
  eventLoopDelayP99Ms: number;
  cloudzero?: Readonly<Record<string, number>> | undefined;
};

type Bucket = {
  readonly index: number;
  readonly counters: Counters;
  gauges: Gauges | undefined;
};

export type Phase = {
  readonly label: string;
  readonly startMs: number;
  readonly endMs: number;
};

export type BucketSummary = {
  readonly startS: number;
  readonly phase: string | undefined;
  readonly hydration: Distribution;
  readonly sessionHydration: Distribution;
  readonly screenHydration: Distribution;
  readonly connect: Distribution;
  readonly firstPoke: Distribution;
  readonly heldRowDelivery: Distribution;
  /** Time for a fresh probe group to hydrate every session query. */
  readonly probeHydration: Distribution;
  readonly backfillPage: Distribution;
  readonly appWrite: Distribution;
  readonly pingRtt: Distribution;
} & Counters &
  Partial<Gauges>;

export type PhaseSummary = {
  readonly label: string;
  readonly startS: number;
  readonly endS: number;
  readonly hydration: Distribution;
  /** Hydration over the last quarter of the phase: where a climb shows. */
  readonly hydrationTail: Distribution;
  /** Least-squares slope of per-bucket hydration p95, in ms per minute. */
  readonly hydrationP95SlopeMsPerMin: number;
  readonly firstPoke: Distribution;
  readonly heldRowDelivery: Distribution;
  /** Time for a fresh probe group to hydrate every session query. */
  readonly probeHydration: Distribution;
  readonly pingRtt: Distribution;
  readonly backfillRowsPerSecond: number;
  readonly appWritesPerSecond: number;
  readonly unexpectedCloses: number;
  readonly serverErrors: number;
  readonly queryErrors: number;
};

const NO_COUNTERS: Counters = {
  sessionsStarted: 0,
  sessionsEnded: 0,
  unexpectedCloses: 0,
  serverErrors: 0,
  queryErrors: 0,
  pokes: 0,
  pokeRows: 0,
  pokeBytes: 0,
  backfillPages: 0,
  backfillRowsRead: 0,
  backfillRowsWritten: 0,
  appWrites: 0,
  appWriteErrors: 0,
};

/**
 * Collects what happens during a run into fixed-width time buckets. Times
 * are milliseconds on the `now()` clock, relative to `startMs`.
 */
export class Recorder {
  readonly #bucketMs: number;
  readonly #now: () => number;
  #startMs: number;
  readonly #buckets: Bucket[] = [];
  readonly #hydrations: HydrationSample[] = [];
  readonly #connects: Sample[] = [];
  readonly #firstPokes: Sample[] = [];
  readonly #heldRows: Sample[] = [];
  readonly #probes: Sample[] = [];
  readonly #backfillPages: (Sample & {readonly written: number})[] = [];
  readonly #failures: {readonly t: number; readonly kind: FailureKind}[] = [];
  readonly #appWrites: Sample[] = [];
  readonly #pings: Sample[] = [];
  readonly #errors = new Map<string, number>();

  constructor(bucketMs: number, now: () => number) {
    this.#bucketMs = bucketMs;
    this.#now = now;
    this.#startMs = now();
  }

  /** Discards nothing, but makes `now()` time zero for the timeline. */
  restart(): void {
    this.#startMs = this.#now();
  }

  elapsedMs(): number {
    return this.#now() - this.#startMs;
  }

  sessionStarted(): void {
    this.#counters().sessionsStarted++;
  }

  sessionEnded(expected: boolean, reason: string): void {
    const c = this.#counters();
    c.sessionsEnded++;
    if (!expected) {
      c.unexpectedCloses++;
      this.#failures.push({t: this.elapsedMs(), kind: 'unexpectedClose'});
      this.#error(`close: ${reason}`);
    }
  }

  connected(ms: number): void {
    this.#connects.push({t: this.elapsedMs(), ms});
  }

  firstPoke(ms: number): void {
    this.#firstPokes.push({t: this.elapsedMs(), ms});
  }

  hydrated(
    name: string,
    kind: QueryKind,
    ms: number,
    returning: boolean,
  ): void {
    this.#hydrations.push({t: this.elapsedMs(), ms, name, kind, returning});
  }

  queryError(name: string, message: string): void {
    this.#counters().queryErrors++;
    this.#failures.push({t: this.elapsedMs(), kind: 'queryError'});
    this.#error(`query ${name}: ${message}`);
  }

  serverError(kind: string, message: string): void {
    this.#counters().serverErrors++;
    this.#failures.push({t: this.elapsedMs(), kind: 'serverError'});
    this.#error(`${kind}: ${message}`);
  }

  poke(rows: number, bytes: number): void {
    const c = this.#counters();
    c.pokes++;
    c.pokeRows += rows;
    c.pokeBytes += bytes;
  }

  pong(rttMs: number): void {
    this.#pings.push({t: this.elapsedMs(), ms: rttMs});
  }

  probeHydrated(ms: number): void {
    this.#probes.push({t: this.elapsedMs(), ms});
  }

  probeFailed(reason: string): void {
    this.#error(`probe: ${reason}`);
  }

  recentProbe(windowMs: number): Distribution {
    const since = this.elapsedMs() - windowMs;
    return distribution(this.#probes.filter(s => s.t >= since).map(s => s.ms));
  }

  heldRowDelivered(ms: number): void {
    this.#heldRows.push({t: this.elapsedMs(), ms});
  }

  backfillPage(read: number, written: number, pageMs: number): void {
    const c = this.#counters();
    c.backfillPages++;
    c.backfillRowsRead += read;
    c.backfillRowsWritten += written;
    this.#backfillPages.push({t: this.elapsedMs(), ms: pageMs, written});
  }

  appWrite(ms: number, error: string | undefined): void {
    const c = this.#counters();
    if (error === undefined) {
      c.appWrites++;
      this.#appWrites.push({t: this.elapsedMs(), ms});
    } else {
      c.appWriteErrors++;
      this.#error(`app write: ${error}`);
    }
  }

  /** Records the gauges for the current bucket; the last call wins. */
  gauges(gauges: Gauges): void {
    this.#bucket(this.elapsedMs()).gauges = gauges;
  }

  errors(): ReadonlyMap<string, number> {
    return this.#errors;
  }

  /** Hydration over the trailing `windowMs`, for progress lines. */
  recentHydration(windowMs: number): Distribution {
    const since = this.elapsedMs() - windowMs;
    return distribution(
      this.#hydrations.filter(s => s.t >= since).map(s => s.ms),
    );
  }

  recentHeldRowDelivery(windowMs: number): Distribution {
    const since = this.elapsedMs() - windowMs;
    return distribution(
      this.#heldRows.filter(s => s.t >= since).map(s => s.ms),
    );
  }

  recentPing(windowMs: number): Distribution {
    const since = this.elapsedMs() - windowMs;
    return distribution(this.#pings.filter(s => s.t >= since).map(s => s.ms));
  }

  timeline(phases: readonly Phase[]): BucketSummary[] {
    const count = Math.max(
      this.#buckets.length,
      Math.floor(this.elapsedMs() / this.#bucketMs) + 1,
    );
    const summaries: BucketSummary[] = [];
    for (let index = 0; index < count; index++) {
      const startMs = index * this.#bucketMs;
      const endMs = startMs + this.#bucketMs;
      const inBucket = (s: Sample) => s.t >= startMs && s.t < endMs;
      const hydrations = this.#hydrations.filter(inBucket);
      const bucket = this.#buckets[index];
      summaries.push({
        startS: startMs / 1000,
        phase: phases.find(p => startMs >= p.startMs && startMs < p.endMs)
          ?.label,
        hydration: distribution(hydrations.map(s => s.ms)),
        sessionHydration: distribution(
          hydrations.filter(s => s.kind === 'session').map(s => s.ms),
        ),
        screenHydration: distribution(
          hydrations.filter(s => s.kind === 'screen').map(s => s.ms),
        ),
        connect: msOf(this.#connects, inBucket),
        firstPoke: msOf(this.#firstPokes, inBucket),
        heldRowDelivery: msOf(this.#heldRows, inBucket),
        probeHydration: msOf(this.#probes, inBucket),
        backfillPage: msOf(this.#backfillPages, inBucket),
        appWrite: msOf(this.#appWrites, inBucket),
        pingRtt: msOf(this.#pings, inBucket),
        ...(bucket?.counters ?? NO_COUNTERS),
        ...bucket?.gauges,
      });
    }
    return summaries;
  }

  phaseSummaries(phases: readonly Phase[]): PhaseSummary[] {
    const timeline = this.timeline(phases);
    return phases.map(phase => {
      const inPhase = (s: {readonly t: number}) =>
        s.t >= phase.startMs && s.t < phase.endMs;
      const tailStart = phase.endMs - (phase.endMs - phase.startMs) / 4;
      const buckets = timeline.filter(b => b.phase === phase.label);
      const seconds = (phase.endMs - phase.startMs) / 1000;
      const perSecond = (n: number) => (seconds > 0 ? round(n / seconds) : 0);
      const failures = (kind: FailureKind) =>
        this.#failures.filter(f => f.kind === kind && inPhase(f)).length;
      return {
        label: phase.label,
        startS: phase.startMs / 1000,
        endS: phase.endMs / 1000,
        hydration: msOf(this.#hydrations, inPhase),
        hydrationTail: msOf(
          this.#hydrations,
          s => s.t >= tailStart && s.t < phase.endMs,
        ),
        hydrationP95SlopeMsPerMin: slope(
          buckets
            .filter(b => b.hydration.count > 0)
            .map(b => [b.startS / 60, b.hydration.p95] as const),
        ),
        firstPoke: msOf(this.#firstPokes, inPhase),
        heldRowDelivery: msOf(this.#heldRows, inPhase),
        probeHydration: msOf(this.#probes, inPhase),
        pingRtt: msOf(this.#pings, inPhase),
        backfillRowsPerSecond: perSecond(
          this.#backfillPages
            .filter(inPhase)
            .reduce((total, p) => total + p.written, 0),
        ),
        appWritesPerSecond: perSecond(this.#appWrites.filter(inPhase).length),
        unexpectedCloses: failures('unexpectedClose'),
        serverErrors: failures('serverError'),
        queryErrors: failures('queryError'),
      };
    });
  }

  /** Hydration by query name and kind over `[startMs, endMs)`. */
  hydrationByName(
    startMs: number,
    endMs: number,
  ): {name: string; kind: QueryKind; returning: boolean; ms: Distribution}[] {
    const groups = new Map<
      string,
      {name: string; kind: QueryKind; returning: boolean; values: number[]}
    >();
    for (const s of this.#hydrations) {
      if (s.t < startMs || s.t >= endMs) {
        continue;
      }
      const key = `${s.name}\u0000${s.kind}\u0000${s.returning}`;
      let g = groups.get(key);
      if (g === undefined) {
        g = {name: s.name, kind: s.kind, returning: s.returning, values: []};
        groups.set(key, g);
      }
      g.values.push(s.ms);
    }
    return Array.from(groups.values(), g => ({
      name: g.name,
      kind: g.kind,
      returning: g.returning,
      ms: distribution(g.values),
    })).sort((a, b) => b.ms.p95 - a.ms.p95);
  }

  #counters(): Counters {
    return this.#bucket(this.elapsedMs()).counters;
  }

  #bucket(elapsedMs: number): Bucket {
    const index = Math.max(0, Math.floor(elapsedMs / this.#bucketMs));
    for (let i = this.#buckets.length; i <= index; i++) {
      this.#buckets.push({
        index: i,
        counters: {...NO_COUNTERS},
        gauges: undefined,
      });
    }
    return this.#buckets[index];
  }

  #error(key: string): void {
    const trimmed = key.length > 200 ? `${key.slice(0, 200)}...` : key;
    this.#errors.set(trimmed, (this.#errors.get(trimmed) ?? 0) + 1);
  }
}

function msOf(
  samples: readonly Sample[],
  filter: (s: Sample) => boolean,
): Distribution {
  return distribution(samples.filter(filter).map(s => s.ms));
}

/** Least-squares slope of y over x; 0 with fewer than two points. */
export function slope(points: readonly (readonly [number, number])[]): number {
  if (points.length < 2) {
    return 0;
  }
  const n = points.length;
  const mx = points.reduce((s, [x]) => s + x, 0) / n;
  const my = points.reduce((s, [, y]) => s + y, 0) / n;
  let num = 0;
  let den = 0;
  for (const [x, y] of points) {
    num += (x - mx) * (y - my);
    den += (x - mx) ** 2;
  }
  return den === 0 ? 0 : round(num / den);
}

function round(n: number): number {
  return Math.round(n * 10) / 10;
}
