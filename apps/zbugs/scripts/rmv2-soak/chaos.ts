import {stat} from 'node:fs/promises';
import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {ChangeStreamerHttpClient} from '../../../../packages/zero-cache/src/services/change-streamer/change-streamer-http.ts';
import type {SnapshotStatus} from '../../../../packages/zero-cache/src/services/change-streamer/snapshot.ts';
import type {TrafficDriver} from '../change-log-traffic.ts';
import type {SoakCluster} from './cluster.ts';
import type {SoakConfig} from './config.ts';
import {backupGenerations, sleep, startMinio, stopMinio} from './infra.ts';
import type {SoakEvent, SoakLog} from './logs.ts';
import {readBackfillState, readTableRowCount} from './oracle.ts';
import type {MetricStore} from './otlp.ts';
import type {ResourceSample, ResourceSampler} from './resources.ts';

/**
 * The chaos matrix of plan section 6.
 *
 * `GRACEFUL_SHUTDOWN = ['SIGTERM','SIGINT']` and
 * `FORCEFUL_SHUTDOWN = ['SIGQUIT','SIGABRT']` are genuinely different paths
 * in `life-cycle.ts`, which is why C1 and C2 are two actions and not the same
 * test twice.
 *
 * Every action returns what it observed; the orchestrator runs a
 * quiesce-and-compare after each one (section 7.2), because a final-only
 * comparison is nearly worthless: a later restore heals a divergence.
 */

export type ChaosContext = {
  readonly config: SoakConfig;
  readonly cluster: SoakCluster;
  /** For the oracle's replica readers, which open SQLite directly. */
  readonly lc: LogContext;
  readonly log: SoakLog;
  readonly metrics: MetricStore;
  readonly sql: postgres.Sql;
  readonly traffic: TrafficDriver;
  readonly sampler: ResourceSampler;
  readonly note: (message: string) => void;
};

export type ChaosOutcome = {
  readonly id: string;
  readonly title: string;
  readonly startedMs: number;
  readonly finishedMs: number;
  readonly notes: string[];
  /** Route counter deltas for the action's window, `source/reason` keyed. */
  readonly census: Readonly<Record<string, number>>;
  readonly findings: string[];
  readonly measurements: Readonly<Record<string, number | string>>;
};

export type ChaosAction = {
  readonly id: string;
  readonly title: string;
  readonly expected: string;
  readonly run: (ctx: ChaosContext, out: MutableOutcome) => Promise<void>;
};

export type MutableOutcome = {
  /** When the action started; the lower bound for its own event window. */
  startedMs: number;
  notes: string[];
  findings: string[];
  measurements: Record<string, number | string>;
};

export type C9ResourceMeasurements = {
  readonly changeLogLiveBytesBefore: number;
  readonly changeLogLiveBytesDuring: number;
  readonly changeLogLiveBytesAfter: number;
  readonly slotRetainedBytesBefore: number;
  readonly slotRetainedBytesDuring: number;
  readonly slotRetainedBytesAfter: number;
};

/**
 * C9 starts after the fat-payload phase, so its pre-outage live-page count can
 * exceed the count during the outage. Recovery only requires the pinned live
 * pages and retained WAL to drain after MinIO returns.
 */
export function c9ResourceFindings({
  changeLogLiveBytesBefore,
  changeLogLiveBytesDuring,
  changeLogLiveBytesAfter,
  slotRetainedBytesBefore,
  slotRetainedBytesDuring,
  slotRetainedBytesAfter,
}: C9ResourceMeasurements): string[] {
  const findings: string[] = [];
  if (
    [
      changeLogLiveBytesBefore,
      changeLogLiveBytesDuring,
      changeLogLiveBytesAfter,
    ].some(bytes => bytes < 0)
  ) {
    findings.push('C9: change-log live-page usage was not measurable');
  } else if (changeLogLiveBytesAfter >= changeLogLiveBytesDuring) {
    findings.push(
      'C9: live change-log pages did not drain after the backup recovered',
    );
  }
  if (slotRetainedBytesDuring <= slotRetainedBytesBefore) {
    findings.push('C9: the minio outage did not grow retained WAL');
  }
  if (
    slotRetainedBytesAfter >= slotRetainedBytesDuring &&
    slotRetainedBytesDuring > 0
  ) {
    findings.push('C9: retained WAL did not drain after the backup recovered');
  }
  return findings;
}

const CENSUS_METRIC = 'sqlite_change_log.catchup_routes';

/**
 * `SoakEvent.detail` is `Record<string, unknown>` by construction -- it is
 * whatever the JSON log line carried -- so every read of it goes through
 * here rather than through `String()`, which would stringify an object as
 * `[object Object]`.
 */
function str(value: unknown, dflt = 'unknown'): string {
  if (value === undefined || value === null) {
    return dflt;
  }
  switch (typeof value) {
    case 'string':
      return value;
    case 'number':
    case 'boolean':
    case 'bigint':
      return String(value);
    default:
      return JSON.stringify(value) ?? dflt;
  }
}

/** A numeric field of an event's detail, or `dflt` when it is not one. */
function num(value: unknown, dflt = 0): number {
  return typeof value === 'number' ? value : dflt;
}

async function fileSize(path: string): Promise<number> {
  try {
    return (await stat(path)).size;
  } catch {
    return 0;
  }
}

function census(metrics: MetricStore): Record<string, number> {
  return metrics.byAttributes(CENSUS_METRIC, 'source', 'reason');
}

function censusDelta(
  before: Record<string, number>,
  after: Record<string, number>,
): Record<string, number> {
  const out: Record<string, number> = {};
  for (const key of new Set([...Object.keys(before), ...Object.keys(after)])) {
    const delta = (after[key] ?? 0) - (before[key] ?? 0);
    if (delta !== 0) {
      out[key] = delta;
    }
  }
  return out;
}

/**
 * The most recent `seedWatermark` the replication-manager reported before
 * `beforeMs`, from the startup line or from any coverage payload.
 *
 * Plan section 1.5: `#confirmReservations` demotes on
 * `coverage.minWatermark > backupWatermark`, but `minWatermark` conflates
 * "history was purged away" with "nothing has been written since the seed".
 * The exact condition is `seedWatermark <= backupWatermark`, so every
 * demotion and every delayed confirmation is reported with both, and a
 * population where the exact condition consistently held is the evidence that
 * the check can be tightened.
 */
function latestSeedWatermark(
  log: SoakLog,
  beforeMs: number,
): string | undefined {
  let seed: string | undefined;
  for (const event of log.events) {
    if (event.tsMs > beforeMs) {
      break;
    }
    if (event.kind === 'change-log-startup') {
      const value = event.detail.seedWatermark;
      if (typeof value === 'string') {
        seed = value;
      }
    } else if (event.kind === 'change-log-reseed') {
      const value = event.detail.head;
      if (typeof value === 'string') {
        seed = value;
      }
    } else {
      const coverage = event.detail.coverage as
        | {seedWatermark?: string}
        | undefined;
      if (coverage?.seedWatermark) {
        seed = coverage.seedWatermark;
      }
    }
  }
  return seed;
}

/** Annotates each demotion / delayed confirmation with the section 1.5 test. */
function recordConfirmationEvidence(
  log: SoakLog,
  sinceMs: number,
  out: MutableOutcome,
): void {
  const relevant = log.events.filter(
    e =>
      e.tsMs >= sinceMs &&
      (e.kind === 'reservation-demoted' || e.kind === 'reservation-delayed'),
  );
  let coveredBySeed = 0;
  for (const event of relevant) {
    const backupWatermark = event.detail.backupWatermark;
    const seedWatermark = latestSeedWatermark(log, event.tsMs);
    const exact =
      typeof backupWatermark === 'string' && seedWatermark !== undefined
        ? seedWatermark <= backupWatermark
        : undefined;
    if (exact) {
      coveredBySeed++;
    }
    out.notes.push(
      `${event.kind}: minWatermark=${str(event.detail.minWatermark)} ` +
        `backupWatermark=${str(backupWatermark)} ` +
        `seedWatermark=${seedWatermark ?? 'unknown'} ` +
        `seedWatermark<=backupWatermark=${exact ?? 'unknown'}`,
    );
  }
  if (relevant.length > 0) {
    out.measurements['confirmationHolds'] = relevant.length;
    out.measurements['confirmationHoldsCoveredBySeedWatermark'] = coveredBySeed;
  }
}

/**
 * Asserts that no follower was demoted to PG in this window.
 *
 * A warm restart leaves the local LTX chain in place, so litestream re-uploads
 * it into the freshly minted generation one file per transaction. While that
 * runs the backup genuinely trails the replica, and `BackupMonitor` holds
 * `firstBackupReceived` until a watermark actually covers it. If that gate
 * regresses to releasing on the first watermark of any value, the RM serves
 * against a backup that does not cover it and demotes the follower.
 */
function expectNoDemotions(
  ctx: ChaosContext,
  sinceMs: number,
  id: string,
  out: MutableOutcome,
): void {
  const demotions = ctx.log.events.filter(
    e => e.tsMs >= sinceMs && e.kind === 'reservation-demoted',
  );
  out.measurements['demotions'] = demotions.length;
  if (demotions.length > 0) {
    out.findings.push(
      `${id} demoted ${demotions.length} follower(s) to PG while the new ` +
        'backup generation was still backfilling; the initial-backup gate ' +
        'released before the backup covered the replica',
    );
  }
}

function firstAfter(
  log: SoakLog,
  sinceMs: number,
  kind: SoakEvent['kind'],
  predicate: (e: SoakEvent) => boolean = () => true,
): SoakEvent | undefined {
  return log.events.find(
    e => e.tsMs >= sinceMs && e.kind === kind && predicate(e),
  );
}

function lastBefore(
  log: SoakLog,
  beforeMs: number,
  kind: SoakEvent['kind'],
): SoakEvent | undefined {
  let found: SoakEvent | undefined;
  for (const event of log.events) {
    if (event.tsMs > beforeMs) {
      break;
    }
    if (event.kind === kind) {
      found = event;
    }
  }
  return found;
}

/** Reservation open -> confirm, i.e. how long the follower waited to restore. */
function measureReservationHold(
  log: SoakLog,
  sinceMs: number,
  taskID: string,
  out: MutableOutcome,
): void {
  const opened = firstAfter(
    log,
    sinceMs,
    'reservation-opened',
    e => e.detail.taskID === taskID,
  );
  const confirmed = firstAfter(
    log,
    sinceMs,
    'reservation-confirmed',
    e => e.detail.taskID === taskID,
  );
  if (opened && confirmed) {
    out.measurements[`reservationHoldMs.${taskID}`] =
      confirmed.tsMs - opened.tsMs;
  }
  if (confirmed) {
    // Reseed -> confirm, for reference only. It is *not* the stall: it also
    // contains the replication-manager's own restart and however long the
    // harness took to bring a follower back to the door. The follower-visible
    // wait is `reservationHoldMs` above; the product-intrinsic window is
    // `reseedToCoveringBackupMs` (see `measureReseedWindow`).
    const reseed = lastBefore(log, confirmed.tsMs, 'change-log-reseed');
    if (reseed && reseed.tsMs >= out.startedMs) {
      out.measurements['reseedToConfirmMs'] = confirmed.tsMs - reseed.tsMs;
    }
  }
}

/**
 * The matrix names three view-syncers so that a demotion of one is visibly
 * *not* a demotion of the others, but a smaller cluster is a legitimate way
 * to run it; the index wraps rather than reaching past the end.
 */
function viewSyncerAt(ctx: ChaosContext, index: number) {
  const {viewSyncers} = ctx.cluster;
  return viewSyncers[index % viewSyncers.length];
}

/**
 * Section 1.4's window, measured without a follower in it: from the reseed to
 * the first backup the vfs poller observes at or above the seed point.
 *
 * That is the interval during which a restoring follower *would* be held,
 * because until such a backup exists the log cannot cover any backup the
 * follower could restore from. A follower that arrives after it waits zero,
 * which is why the reservation hold alone understates the exposure while
 * reseed-to-confirm overstates it -- the latter is mostly restart latency.
 *
 * Its floor is one litestream `monitor-interval` plus one vfs poll interval,
 * so it scales with those settings rather than being a fixed cost.
 */
function measureReseedWindow(log: SoakLog, out: MutableOutcome): void {
  const reseed = log.events.find(
    e => e.kind === 'change-log-reseed' && e.tsMs >= out.startedMs,
  );
  const seed = reseed?.detail.head;
  if (!reseed || typeof seed !== 'string') {
    return;
  }
  const covering = log.events.find(
    e =>
      e.kind === 'backup-watermark' &&
      e.tsMs >= reseed.tsMs &&
      typeof e.detail.watermark === 'string' &&
      e.detail.watermark >= seed,
  );
  out.measurements['reseedSeedWatermark'] = seed;
  out.measurements['reseedToCoveringBackupMs'] = covering
    ? covering.tsMs - reseed.tsMs
    : -1;
  if (covering) {
    out.measurements['reseedCoveringBackupWatermark'] = str(
      covering.detail.watermark,
    );
  }
}

async function restartViewSyncer(
  ctx: ChaosContext,
  index: number,
  signal: NodeJS.Signals,
  out: MutableOutcome,
  opts: {deleteReplica?: boolean | undefined; downMs?: number | undefined} = {},
): Promise<void> {
  const vs = viewSyncerAt(ctx, index);
  const since = Date.now();
  out.notes.push(`${signal} ${vs.name}`);
  await vs.stop(signal);
  if (opts.downMs) {
    out.notes.push(`leaving ${vs.name} down for ${opts.downMs}ms`);
    await sleep(opts.downMs);
  }
  if (opts.deleteReplica) {
    await vs.deleteReplica();
    out.notes.push(`deleted ${vs.name}'s replica`);
  }
  await vs.start();
  out.measurements[`restartMs.${vs.name}`] = Date.now() - since;
  measureReservationHold(ctx.log, since, vs.name, out);
  recordConfirmationEvidence(ctx.log, since, out);
}

/**
 * C15's fixture size. Backfill transactions are cut at 8MB
 * (`COMMIT_THRESHOLD_BYTES` in `backfill-manager.ts`), so the table has to be
 * several times that for the run to have committed a mark *and* still have
 * work left when the RM is killed.
 *
 * Measured, not guessed: 100k rows at 200 bytes (~20MB, ~2.5 transactions) ran
 * to completion before the action could interrupt it at all. 400k at 400 bytes
 * is ~160MB, i.e. about twenty transactions, which is a window measured in
 * seconds rather than in poll intervals.
 *
 * `scale` shrinks it for a smoke run, but only to a floor: below a handful of
 * commit thresholds there is no mark to resume from and the action can only
 * report that its own fixture was too small.
 */
const BACKFILL_FIXTURE_MAX_ROWS = 400_000;
const BACKFILL_FIXTURE_MIN_ROWS = 250_000;
const BACKFILL_FIXTURE_PAYLOAD_BYTES = 400;
/** Tight, because the whole point is to catch the run before it ends. */
const BACKFILL_PROGRESS_POLL_MS = 50;
/** The column C15 adds, and whose values the run has to carry. */
const BACKFILL_COLUMN = 'payload';

function backfillFixtureRows(config: SoakConfig): number {
  return Math.max(
    BACKFILL_FIXTURE_MIN_ROWS,
    Math.round(BACKFILL_FIXTURE_MAX_ROWS * config.scale),
  );
}
const BACKFILL_START_TIMEOUT_MS = 120_000;
const BACKFILL_RESUME_TIMEOUT_MS = 120_000;
const BACKFILL_COMPLETION_TIMEOUT_MS = 300_000;
/**
 * The table whose column C15 will have backfilled, with a key the change
 * source can actually resume from.
 *
 * The key is `int4`, which is on the resumable-literal allowlist, and the rows
 * are inserted in key order and then `ANALYZE`d, so `pg_stats.correlation` is
 * 1 and the run clears the `backfillResumeMinCorrelation` gate. The zbugs
 * `issue` table satisfies neither -- its ids are
 * `change-log-traffic-<run>-<n>`, whose text order and insertion order diverge
 * as soon as the sequence crosses a decade -- so C15 brings its own table
 * rather than borrowing the workload's.
 *
 * Only the key is created here. The rows replicate as ordinary inserts, which
 * is *not* a backfill; a backfill is what carries the values a column already
 * had when it was published, and those are not in the WAL. {@link addBackfillColumn}
 * is what triggers the run.
 */
async function createBackfillFixture(
  ctx: ChaosContext,
  table: string,
  out: MutableOutcome,
): Promise<number> {
  const rows = backfillFixtureRows(ctx.config);
  const started = Date.now();
  await ctx.sql.unsafe(`CREATE TABLE "${table}" ("id" int4 PRIMARY KEY)`);
  await ctx.sql.unsafe(`
    INSERT INTO "${table}" ("id")
      SELECT g FROM generate_series(1, ${rows}) g`);
  await ctx.sql.unsafe(`ANALYZE "${table}"`);
  out.measurements['fixtureRows'] = rows;
  out.measurements['fixtureBuildMs'] = Date.now() - started;
  // Recorded rather than asserted: if the gate ever stops admitting a
  // perfectly correlated key, the resume verdict says so, and this says why.
  const stats = await ctx.sql.unsafe<{correlation: number | null}[]>(`
    SELECT correlation FROM pg_stats
      WHERE schemaname = 'public' AND tablename = '${table}'
        AND attname = 'id'`);
  const correlation = stats[0]?.correlation ?? 'unknown';
  out.measurements['fixtureKeyCorrelation'] = correlation;
  out.notes.push(
    `created ${table} with ${rows} rows (key correlation ${correlation})`,
  );
  return rows;
}

/**
 * Adds the column whose existing values the change source has to backfill.
 *
 * A constant `DEFAULT` is metadata-only in PG11+, so every existing row gains
 * a value without a single WAL row change -- which is exactly the situation
 * the backfill exists for, and why C15 triggers its run this way rather than
 * by creating a populated table (whose rows would simply replicate).
 */
async function addBackfillColumn(
  ctx: ChaosContext,
  table: string,
  out: MutableOutcome,
): Promise<void> {
  const started = Date.now();
  await ctx.sql.unsafe(`
    ALTER TABLE "${table}"
      ADD COLUMN "payload" text NOT NULL
      DEFAULT repeat('x', ${BACKFILL_FIXTURE_PAYLOAD_BYTES})`);
  out.measurements['addColumnMs'] = Date.now() - started;
  out.notes.push(`added ${table}.payload, which needs a backfill`);
}

async function dropBackfillFixture(
  ctx: ChaosContext,
  table: string,
): Promise<void> {
  await ctx.sql.unsafe(`DROP TABLE IF EXISTS "${table}"`);
}

/** One replica's row count for the fixture table. */
export type FixtureRowCount = {readonly node: string; readonly rows: number};

export type C15Observations = {
  readonly table: string;
  readonly fixtureRows: number;
  /** Did every replica hold the fixture's rows before the column was added? */
  readonly fixtureRowsSettled: boolean;
  /** Did any run announce itself for the table at all? */
  readonly runAnnounced: boolean;
  /** Had the replica recorded a mark when the RM was killed? */
  readonly markedBeforeRestart: boolean;
  /** Rows whose backfilled column had a value when the RM was killed. */
  readonly rowsFilledBeforeRestart: number;
  /** `zero`, `resumed`, or `none-observed`. */
  readonly resumedStart: string;
  readonly demotions: number;
  readonly restores: number;
  readonly rowsAfterResume: readonly FixtureRowCount[];
};

/**
 * C15's verdict, separated from the driving so that each way it can fail is
 * a case rather than a run.
 *
 * The ordering is deliberate: a run that never announced itself, or a fixture
 * too small to interrupt, makes the resume verdict meaningless, so those are
 * reported *instead of* it rather than alongside it. Only the demotion,
 * restore and completeness checks always apply -- they are about the rest of
 * the system's reaction, which is worth judging even when the backfill itself
 * did something unexpected.
 */
export function c15Findings({
  table,
  fixtureRows,
  fixtureRowsSettled,
  runAnnounced,
  markedBeforeRestart,
  rowsFilledBeforeRestart,
  resumedStart,
  demotions,
  restores,
  rowsAfterResume,
}: C15Observations): string[] {
  const findings: string[] = [];
  if (!fixtureRowsSettled) {
    findings.push(
      `C15's ${fixtureRows} fixture rows did not reach every replica before ` +
        'the column was added; the run it measured started from an ' +
        'unsettled table',
    );
  } else if (!runAnnounced) {
    findings.push(
      `C15 created ${table} with ${fixtureRows} rows but no backfill run ` +
        'announced itself; either the change source did not pick the table ' +
        'up, or run announcements are not being logged',
    );
  } else if (!markedBeforeRestart) {
    // Two causes, and they are worth naming together, because the harness
    // cannot tell them apart from here and one of them is a *product*
    // condition rather than a fixture problem. An unordered run carries no
    // `lastKey`, so no mark ever appears no matter how long the run takes --
    // which looks identical to a run that finished too fast.
    findings.push(
      `C15 never saw a mark for ${table} ` +
        `(${rowsFilledBeforeRestart} of ${fixtureRows} rows backfilled). ` +
        'Either the run was not ordered -- check that ' +
        '`ZERO_CHANGE_STREAMER_BACKFILL_RESUME=on` reached the RM and that ' +
        'the key cleared the correlation gate -- or it finished before the ' +
        'restart, which makes the fixture too small. The restart proved ' +
        'nothing either way.',
    );
  } else if (resumedStart === 'none-observed') {
    findings.push(
      `C15 restarted the RM mid-backfill but ${table} never announced ` +
        'another run; the backfill was dropped rather than resumed',
    );
  } else if (resumedStart !== 'resumed') {
    findings.push(
      `C15's restarted run of ${table} started from ${resumedStart} rather ` +
        "than resuming from the replica's mark; the whole table is being " +
        'copied again',
    );
  }
  if (demotions > 0) {
    findings.push(
      `C15 demoted ${demotions} follower(s) to PG after a backfill was ` +
        'interrupted; a backfill restart is not a replication gap',
    );
  }
  if (restores > 0) {
    findings.push(
      `C15 sent ${restores} follower(s) back to a litestream restore after ` +
        'a backfill was interrupted; a backfill restart is not a ' +
        'replication gap',
    );
  }
  const short = rowsAfterResume.filter(c => c.rows !== fixtureRows);
  if (short.length > 0) {
    // A resume that skipped its suffix would satisfy every check above.
    findings.push(
      `C15's resumed backfill of ${table} left ${short.length} replica(s) ` +
        `short of ${fixtureRows} rows: ` +
        short.map(c => `${c.node}=${c.rows}`).join(', '),
    );
  }
  return findings;
}

/** The run announcement for `table` at or after `sinceMs`, if one arrives. */
function waitForRunAnnouncement(
  ctx: ChaosContext,
  table: string,
  sinceMs: number,
  timeoutMs: number,
): Promise<SoakEvent | undefined> {
  return ctx.log
    .waitFor(
      `a backfill run announcement for ${table}`,
      e => e.kind === 'backfill-run-started' && e.detail.table === table,
      timeoutMs,
      sinceMs,
    )
    .catch(() => undefined);
}

/**
 * Waits until every replica holds the fixture's rows, before the column that
 * needs backfilling is added. The rows arrive as ordinary inserts, so this is
 * setup rather than the thing under test -- but the backfill has to start from
 * a settled table, or "the column is not filled in yet" and "the row is not
 * here yet" become the same observation.
 */
async function waitForFixtureRows(
  ctx: ChaosContext,
  table: string,
  fixtureRows: number,
): Promise<boolean> {
  const deadline = Date.now() + BACKFILL_START_TIMEOUT_MS;
  const handles = ctx.cluster.replicaHandles();
  for (;;) {
    const counts = handles.map(h =>
      readTableRowCount(ctx.lc, h.replicaFile, table),
    );
    if (counts.every(rows => rows === fixtureRows)) {
      return true;
    }
    if (Date.now() >= deadline) {
      return false;
    }
    await sleep(500);
  }
}

/**
 * Waits until the RM's own replica has recorded a mark for `table` -- which is
 * what a restarted manager resumes from -- while the run still has rows left.
 *
 * Progress is the mark, not the row count: every row is already there, and it
 * is the *column* that is being filled in.
 */
async function waitForBackfillProgress(
  ctx: ChaosContext,
  table: string,
  fixtureRows: number,
  out: MutableOutcome,
): Promise<{marked: boolean; filled: number}> {
  const replicaFile = ctx.cluster.rm.replicaFile;
  const deadline = Date.now() + BACKFILL_START_TIMEOUT_MS;
  let filled = -1;
  for (;;) {
    const state = readBackfillState(ctx.lc, replicaFile, table);
    // The row exists only while the backfill is in flight -- a completion
    // deletes it -- so a real mark on it *is* "still running, and resumable".
    // `filled` is read for the record afterwards, never as the condition: it
    // is a second query, and racing it against the first is one of the ways
    // the earlier version managed to miss a run entirely.
    // (`mark` is `null` for an unordered run and `undefined` when the row is
    // gone; neither is a mark.)
    if (state?.mark !== null && state?.mark !== undefined) {
      filled = readTableRowCount(ctx.lc, replicaFile, table, BACKFILL_COLUMN);
      out.measurements['rowsFilledBeforeRestart'] = filled;
      out.measurements['markBeforeRestart'] = state.mark;
      out.measurements['runIDBeforeRestart'] = str(state.runID);
      return {marked: true, filled};
    }
    filled = readTableRowCount(ctx.lc, replicaFile, table, BACKFILL_COLUMN);
    // No row beside a filled column means the run finished before we could
    // interrupt it.
    if (state === undefined && filled >= fixtureRows) {
      break;
    }
    if (Date.now() >= deadline) {
      break;
    }
    await sleep(BACKFILL_PROGRESS_POLL_MS);
  }
  out.measurements['rowsFilledBeforeRestart'] = filled;
  return {marked: false, filled};
}

/**
 * Every replica's count of rows that have the backfilled column, once they
 * settle or time out. A resume that skipped its suffix shows up here and
 * nowhere else.
 */
async function waitForBackfillCompletion(
  ctx: ChaosContext,
  table: string,
  fixtureRows: number,
): Promise<FixtureRowCount[]> {
  const deadline = Date.now() + BACKFILL_COMPLETION_TIMEOUT_MS;
  const handles = ctx.cluster.replicaHandles();
  for (;;) {
    const counts = handles.map(h => ({
      node: h.node,
      rows: readTableRowCount(ctx.lc, h.replicaFile, table, BACKFILL_COLUMN),
    }));
    if (counts.every(c => c.rows === fixtureRows) || Date.now() >= deadline) {
      return counts;
    }
    await sleep(500);
  }
}

/**
 * The task id C16 reserves under. Deliberately not one of the cluster's node
 * names: `SnapshotReservations.open()` closes whatever reservation the task
 * already holds, so reusing a view-syncer's id would tear down that follower's
 * own reservation instead of adding one.
 */
const C16_TASK_ID = 'c16-slow-restore';

/** How long to wait for the change-streamer to confirm C16's reservation. */
const RESERVATION_CONFIRM_TIMEOUT_MS = 120_000;

/**
 * How long to wait for the purger to run once C16 releases its reservation.
 * The coordinator is level-triggered on a `CLEANUP_DELAY_MS` (30s) cadence, so
 * a pass is not immediate even though the floor is free.
 */
const RESERVATION_DRAIN_TIMEOUT_MS = 180_000;

export type C16Observations = {
  readonly reservedWatermark: string | undefined;
  readonly rowsPurgedDuringHold: number;
  readonly passesAboveReservedWatermark: number;
  readonly backupWatermarksDuringHold: number;
  readonly changeLogLiveBytesBefore: number;
  readonly changeLogLiveBytesDuring: number;
  readonly changeLogLiveBytesAfter: number;
  readonly rowsPurgedAfterRelease: number;
};

/**
 * C16's gate: what a held reservation must and must not do.
 *
 * The `must not` is the interesting half. A reservation is the mechanism that
 * makes a slow restore safe -- the follower is promised catchup from
 * `minWatermark`, and the floor
 * (`min(backupWatermark, ...acks, ...reservations)`) may not pass it while the
 * socket is open -- so a purge that ran anyway is a follower that would meet
 * `WatermarkTooOld` at the end of its restore.
 *
 * The retained/drained pair is what keeps the action honest: without growth
 * during the hold there was nothing to purge, and "no rows purged" would pass
 * for the wrong reason.
 */
export function c16Findings({
  reservedWatermark,
  rowsPurgedDuringHold,
  passesAboveReservedWatermark,
  backupWatermarksDuringHold,
  changeLogLiveBytesBefore,
  changeLogLiveBytesDuring,
  changeLogLiveBytesAfter,
  rowsPurgedAfterRelease,
}: C16Observations): string[] {
  const findings: string[] = [];
  if (reservedWatermark === undefined) {
    // Every other check is a statement about a reservation that was actually
    // held, so none of them mean anything here.
    return ['C16: the snapshot reservation was never confirmed'];
  }
  if (rowsPurgedDuringHold > 0) {
    findings.push(
      `C16: ${rowsPurgedDuringHold} change-log row(s) were purged while a ` +
        'snapshot reservation was open',
    );
  }
  if (passesAboveReservedWatermark > 0) {
    findings.push(
      `C16: ${passesAboveReservedWatermark} purge pass(es) ran with a floor ` +
        `above the reserved watermark ${reservedWatermark}`,
    );
  }
  if (
    [
      changeLogLiveBytesBefore,
      changeLogLiveBytesDuring,
      changeLogLiveBytesAfter,
    ].some(bytes => bytes < 0)
  ) {
    findings.push('C16: change-log live-page usage was not measurable');
  } else {
    if (changeLogLiveBytesDuring <= changeLogLiveBytesBefore) {
      findings.push(
        'C16: the held reservation retained no change-log pages, so nothing ' +
          'was asked of the floor',
      );
    }
    if (changeLogLiveBytesAfter >= changeLogLiveBytesDuring) {
      findings.push(
        'C16: live change-log pages did not drain after the reservation was ' +
          'released',
      );
    }
  }
  if (rowsPurgedAfterRelease === 0) {
    findings.push(
      'C16: no change-log rows were purged after the reservation was released',
    );
  }
  if (backupWatermarksDuringHold === 0) {
    findings.push(
      'C16: no backup watermark arrived while the reservation was held, so ' +
        'the upstream ACK path was never observed',
    );
  }
  return findings;
}

type HeldReservation = {
  /** The bounds the change-streamer advertised, or `undefined` if it never did. */
  readonly status: SnapshotStatus | undefined;
  readonly confirmMs: number;
  /** Ends the reservation, the way subscribing to `/changes` would. */
  readonly release: () => Promise<void>;
};

/**
 * Opens a snapshot reservation and holds it, the way a view-syncer holds one
 * for the whole of a litestream restore.
 *
 * The reservation lives exactly as long as its WebSocket, so the stream is
 * *iterated* rather than read once and broken out of -- breaking cancels the
 * subscription, which is precisely the release this is trying to defer. The
 * real client does the same thing (`reserveAndGetSnapshotStatus`), and the
 * change-streamer is what normally ends the stream, when the task subscribes.
 */
async function holdSnapshotReservation(
  ctx: ChaosContext,
  taskID: string,
): Promise<HeldReservation> {
  const client = new ChangeStreamerHttpClient(
    ctx.lc,
    // Only used for change-streamer discovery through the change DB, which an
    // explicit URI skips.
    {appID: ctx.config.appID, shardNum: 0},
    ctx.config.upstreamDB,
    `ws://127.0.0.1:${ctx.config.rmPort + 1}/`,
  );
  const openedAt = Date.now();
  const stream = await client.reserveSnapshot(taskID);

  let confirm: (status: SnapshotStatus | undefined) => void = () => {};
  const confirmed = new Promise<SnapshotStatus | undefined>(resolve => {
    confirm = resolve;
  });
  const ended = (async () => {
    try {
      for await (const msg of stream) {
        confirm(msg[1]);
      }
    } finally {
      // A stream that ends without a status leaves the wait unresolved
      // otherwise; resolving twice is a no-op.
      confirm(undefined);
    }
  })().catch(() => {});

  const timer = setTimeout(confirm, RESERVATION_CONFIRM_TIMEOUT_MS, undefined);
  const status = await confirmed;
  clearTimeout(timer);

  return {
    status,
    confirmMs: Date.now() - openedAt,
    release: async () => {
      stream.cancel();
      await ended;
    },
  };
}

export const CHAOS_ACTIONS: readonly ChaosAction[] = [
  {
    id: 'C1',
    title: 'SIGTERM a view-syncer (graceful drain), restart, replica intact',
    expected: 'sqlite / selected',
    run: (ctx, out) => restartViewSyncer(ctx, 0, 'SIGTERM', out),
  },
  {
    id: 'C2',
    title: 'SIGQUIT a view-syncer (abrupt), restart, replica intact',
    expected: 'sqlite / selected',
    run: (ctx, out) => restartViewSyncer(ctx, 1, 'SIGQUIT', out),
  },
  {
    id: 'C3',
    title: 'Kill a view-syncer, delete its replica, restart',
    expected: 'litestream restore, then sqlite -- and no demotion',
    async run(ctx, out) {
      const since = Date.now();
      await restartViewSyncer(ctx, 2, 'SIGQUIT', out, {deleteReplica: true});
      const restored = firstAfter(ctx.log, since, 'restore-started');
      out.measurements['restoreObserved'] = restored ? 'yes' : 'no';
      // Invariant 14: the purge floor is capped at the backup watermark, so a
      // log that held the history covers any backup a follower can restore
      // from. A `backup-uncovered` demotion here, with no recent reseed, is a
      // finding rather than an expected outcome.
      const target = viewSyncerAt(ctx, 2);
      const demoted = ctx.log.events.filter(
        e =>
          e.tsMs >= since &&
          e.kind === 'reservation-demoted' &&
          e.detail.taskID === target.name,
      );
      const reseeded = firstAfter(ctx.log, since - 60_000, 'change-log-reseed');
      if (demoted.length > 0 && !reseeded) {
        out.findings.push(
          `C3 demoted ${target.name} to PG without a ` +
            `recent reseed: invariant 14 (minWatermark <= backupWatermark) ` +
            `did not hold`,
        );
      }
    },
  },
  {
    id: 'C4',
    title:
      'Kill a view-syncer mid-burst, leave it down past a backup interval, restart',
    expected:
      'sqlite / selected over a short gap; over a long one, the snapshot gate discards the stale replica and restores',
    async run(ctx, out) {
      const burst = ctx.traffic.runStage({
        rate: 250,
        durationSeconds: 20,
        label: 'C4-burst',
      });
      await sleep(3_000);
      // Outage A: shorter than the retention window. The purge floor should
      // have held the history for a disconnected subscriber, so the follower
      // catches up from SQLite across the gap.
      await restartViewSyncer(ctx, 0, 'SIGQUIT', out, {
        downMs: ctx.config.backupIntervalSeconds * 3_000 + 5_000,
      });
      const result = await burst;
      out.measurements['burstTransactions'] = result.transactions;

      // Outage B: comfortably longer than the retention window, so the purge
      // floor outruns the follower's ack.
      //
      // What this asserts is *not* `pg / watermark-uncovered`. A restarting
      // view-syncer can never take that route, however long the gap:
      // `restoreReplica` runs on every start and `reserveAndGetSnapshotStatus`
      // hands it the log's `minWatermark` first, so a replica below the
      // minimum is discarded and re-restored from the backup and the
      // subscriber always reaches `/changes` at or above the minimum. The
      // snapshot gate converts the uncovered case into a restore before the
      // route exists.
      //
      // So the assertion is that conversion: the follower comes back, throws
      // its stale replica away, and is then served from SQLite.
      const downMs = ctx.config.changeLog.retentionMs * 2 + 15_000;
      const load = ctx.traffic.runStage({
        rate: 25,
        durationSeconds: Math.ceil(downMs / 1000) + 25,
        label: 'C4-long-gap',
      });
      const since = Date.now();
      const target = viewSyncerAt(ctx, 0);
      await restartViewSyncer(ctx, 0, 'SIGQUIT', out, {downMs});
      await load;
      const discarded = ctx.log.events.some(
        e =>
          e.tsMs >= since &&
          e.kind === 'replica-discarded' &&
          e.node === target.name,
      );
      out.measurements['longGapOutcome'] = discarded
        ? 'stale-replica-discarded-and-restored'
        : 'replica-still-covered';
      const uncovered = ctx.log.events.some(
        e =>
          e.tsMs >= since &&
          e.kind === 'served-from-pg' &&
          e.detail.reason === 'watermark-uncovered',
      );
      out.measurements['longGapWatermarkUncovered'] = uncovered ? 'yes' : 'no';
      if (uncovered) {
        out.findings.push(
          'C4 routed a restarting follower through pg/watermark-uncovered; ' +
            'the snapshot gate should discard and restore a stale replica ' +
            'before it subscribes',
        );
      }
    },
  },
  {
    id: 'C5',
    title: 'SIGTERM the replication-manager, restart',
    expected: 'valid log resumes from its own head; view-syncers reconnect',
    async run(ctx, out) {
      const since = Date.now();
      await ctx.cluster.rm.stop('SIGTERM');
      await ctx.cluster.startReplicationManager();
      out.measurements['rmRestartMs'] = Date.now() - since;
      const reconcile = firstAfter(ctx.log, since, 'change-log-reconcile');
      out.measurements['reconcileAction'] = str(
        reconcile?.detail.action,
        'none-observed',
      );
      if (reconcile?.detail.action === 'reseeded') {
        out.findings.push(
          `C5 reseeded the change log (reason=${str(
            reconcile.detail.reason,
          )}); a valid log should have resumed from its own head`,
        );
      }
      recordConfirmationEvidence(ctx.log, since, out);
    },
  },
  {
    id: 'C6',
    title:
      'Stop the RM, delete only the change log, restart, then immediately wipe a view-syncer replica',
    expected:
      'forced `created` reseed; RM readiness waits for a covering backup, then the restore uses SQLite',
    async run(ctx, out) {
      const since = Date.now();
      await ctx.cluster.rm.stop('SIGTERM');
      await ctx.cluster.rm.deleteChangeLog();
      out.notes.push('deleted only replica.db-change-log*');
      await ctx.cluster.startReplicationManager();
      const reseed = firstAfter(ctx.log, since, 'change-log-reseed');
      out.measurements['reseedReason'] = str(
        reseed?.detail.reason,
        'none-observed',
      );
      if (!reseed) {
        out.findings.push(
          'C6 deleted the change log but no reseed was observed',
        );
      }
      // The RM reseeded at the replica head while the newest backup may still
      // sit behind it. The readiness gate must absorb that window so the
      // follower is never offered an uncovering backup and demoted to PG.
      const backSince = Date.now();
      await restartViewSyncer(ctx, 2, 'SIGQUIT', out, {deleteReplica: true});
      expectNoDemotions(ctx, backSince, 'C6', out);
    },
  },
  {
    id: 'C7',
    title: 'SIGSTOP the replication-manager for 30s, then SIGCONT',
    expected: 'view-syncer disconnect and reconnect, no data gap',
    async run(ctx, out) {
      const since = Date.now();
      ctx.cluster.rm.signal('SIGSTOP');
      out.notes.push('SIGSTOP rm');
      await sleep(30_000);
      ctx.cluster.rm.signal('SIGCONT');
      out.notes.push('SIGCONT rm');
      // Give the view-syncers a chance to notice and reconnect.
      await sleep(15_000);
      out.measurements['pausedMs'] = Date.now() - since;
      recordConfirmationEvidence(ctx.log, since, out);
    },
  },
  {
    id: 'C8',
    title: 'SIGKILL the replication-manager mid-burst, restart',
    expected: 'reconcile by truncation, not by reseed',
    async run(ctx, out) {
      const burst = ctx.traffic.runStage({
        rate: 250,
        durationSeconds: 15,
        label: 'C8-burst',
      });
      await sleep(4_000);
      const since = Date.now();
      await ctx.cluster.rm.stop('SIGKILL');
      out.notes.push('SIGKILL rm mid-burst');
      // The burst cannot commit while the RM is down, but it writes to PG,
      // not through the RM, so let it finish.
      const result = await burst.catch(e => {
        out.notes.push(`burst reported ${String(e)}`);
        return undefined;
      });
      await ctx.cluster.startReplicationManager();
      const reconcile = firstAfter(ctx.log, since, 'change-log-reconcile');
      const action = str(reconcile?.detail.action, 'none-observed');
      out.measurements['reconcileAction'] = action;
      out.measurements['burstTransactions'] = result?.transactions ?? -1;
      if (action === 'reseeded') {
        // If every hard crash reseeds, section 1.4's window is paid on every
        // crash rather than only on a schema bump.
        out.findings.push(
          `C8: a hard crash reseeded the log (reason=${str(
            reconcile?.detail.reason,
          )}) rather than truncating a bounded suffix`,
        );
      }
      recordConfirmationEvidence(ctx.log, since, out);
    },
  },
  {
    id: 'C9',
    title: 'Stop minio under sustained writes, then restart it',
    expected: 'backup watermark freezes, then catches up; both bounded',
    async run(ctx, out) {
      const downSeconds = Math.max(60, Math.round(300 * ctx.config.scale));
      const load = ctx.traffic.runStage({
        rate: 25,
        durationSeconds: downSeconds + 60,
        label: 'C9-sustained',
      });
      const before = await ctx.sampler.sample();
      await stopMinio();
      out.notes.push(`stopped minio for ${downSeconds}s`);
      await sleep(downSeconds * 1000);
      const during = await ctx.sampler.sample();
      await startMinio(ctx.config);
      const recoveredAt = Date.now();
      out.notes.push('restarted minio');
      await load;
      // Let the backup catch up and the acker walk the slot forward.
      await sleep(30_000);
      const after = await ctx.sampler.sample();
      const slotBytes = (s: typeof before) =>
        (s?.slots ?? []).reduce((acc, slot) => acc + slot.retainedBytes, 0);
      const liveBytes = (s: ResourceSample | undefined) =>
        s?.changeLogLiveBytes ?? -1;
      out.measurements['changeLogBytesBefore'] = before?.changeLogBytes ?? -1;
      out.measurements['changeLogBytesDuring'] = during?.changeLogBytes ?? -1;
      out.measurements['changeLogBytesAfter'] = after?.changeLogBytes ?? -1;
      out.measurements['changeLogLiveBytesBefore'] = liveBytes(before);
      out.measurements['changeLogLiveBytesDuring'] = liveBytes(during);
      out.measurements['changeLogLiveBytesAfter'] = liveBytes(after);
      out.measurements['changeLogFreeBytesAfter'] =
        after?.changeLogFreeBytes ?? -1;
      out.measurements['slotRetainedBytesBefore'] = slotBytes(before);
      out.measurements['slotRetainedBytesDuring'] = slotBytes(during);
      out.measurements['slotRetainedBytesAfter'] = slotBytes(after);
      const backupRecoveries = ctx.log.events.filter(
        e => e.tsMs >= recoveredAt && e.kind === 'backup-watermark',
      ).length;
      const recoveryPurges = ctx.log.events.filter(
        e => e.tsMs >= recoveredAt && e.kind === 'purge-pass',
      );
      const purgedRowsAfterRecovery = recoveryPurges.reduce(
        (sum, event) =>
          sum +
          (typeof event.detail.deletedRows === 'number'
            ? event.detail.deletedRows
            : 0),
        0,
      );
      out.measurements['backupRecoveries'] = backupRecoveries;
      out.measurements['purgePassesAfterRecovery'] = recoveryPurges.length;
      out.measurements['purgedRowsAfterRecovery'] = purgedRowsAfterRecovery;

      out.findings.push(
        ...c9ResourceFindings({
          changeLogLiveBytesBefore: liveBytes(before),
          changeLogLiveBytesDuring: liveBytes(during),
          changeLogLiveBytesAfter: liveBytes(after),
          slotRetainedBytesBefore: slotBytes(before),
          slotRetainedBytesDuring: slotBytes(during),
          slotRetainedBytesAfter: slotBytes(after),
        }),
      );
      if (backupRecoveries === 0) {
        out.findings.push(
          'C9: no backup watermark was observed after minio recovered',
        );
      }
      if (purgedRowsAfterRecovery === 0) {
        out.findings.push(
          'C9: no change-log rows were purged after minio recovered',
        );
      }
    },
  },
  {
    id: 'C10',
    title: 'Roll the read percentage back from 100 to 0 and restart the RM',
    expected: 'every route becomes pg / percentage',
    async run(ctx, out) {
      const before = census(ctx.metrics);
      await ctx.cluster.rm.stop('SIGTERM');
      await ctx.cluster.startReplicationManager({
        readPercent: 0,
        coldReadPercent: 0,
      });
      // Force every view-syncer to re-register so the new routing is
      // exercised rather than assumed.
      for (const vs of ctx.cluster.viewSyncers) {
        await vs.stop('SIGTERM');
        await vs.start();
      }
      await sleep(ctx.config.metricExportIntervalMs * 3);
      const delta = censusDelta(before, census(ctx.metrics));
      out.measurements['routesAfterRollback'] = JSON.stringify(delta);
      const servedFromSQLite = Object.entries(delta)
        .filter(([key]) => key.startsWith('sqlite/'))
        .reduce((acc, [, value]) => acc + value, 0);
      if (servedFromSQLite > 0) {
        out.findings.push(
          `C10: ${servedFromSQLite} catchup(s) still routed to SQLite after ` +
            `the read percentage was rolled back to 0`,
        );
      }
    },
  },
  {
    id: 'C11',
    title: 'Walk the mode ladder back: serve -> compare -> write',
    expected: 'the writer stays, reads stop, then comparison stops',
    async run(ctx, out) {
      for (const mode of ['compare', 'write'] as const) {
        const since = Date.now();
        await ctx.cluster.rm.stop('SIGTERM');
        await ctx.cluster.startReplicationManager({
          mode,
          readPercent: 0,
          coldReadPercent: 0,
          comparePercent: mode === 'compare' ? 100 : 0,
        });
        await sleep(ctx.config.metricExportIntervalMs * 3);
        const startup = firstAfter(ctx.log, since, 'change-log-startup');
        out.notes.push(
          `mode=${mode}: change log ${startup ? 'still open' : 'not opened'}`,
        );
        out.measurements[`headAfter.${mode}`] = str(
          startup?.detail.headWatermark,
        );
        if (!startup) {
          out.findings.push(
            `C11: no change-log startup line after rolling back to ${mode}; ` +
              `the writer should still be running`,
          );
        }
      }
    },
  },
  {
    id: 'C12',
    title: 'Turn the change log off and confirm the file is reclaimed',
    expected: 'replicatorDeletesStaleChangeLog removes the file',
    async run(ctx, out) {
      const before = await fileSize(ctx.cluster.changeLogFile);
      await ctx.cluster.rm.stop('SIGTERM');
      await ctx.cluster.startReplicationManager({
        mode: 'off',
        readPercent: 0,
        coldReadPercent: 0,
        comparePercent: 0,
      });
      // The delete happens in a replicator worker at startup. The
      // replication-manager runs none (NUM_SYNC_WORKERS=0), so the reclaim is
      // observed on a view-syncer's tree; restart one to make it happen.
      for (const vs of ctx.cluster.viewSyncers) {
        await vs.stop('SIGTERM');
        await vs.start();
      }
      const after = await fileSize(ctx.cluster.changeLogFile);
      out.measurements['changeLogBytesBeforeOff'] = before;
      out.measurements['changeLogBytesAfterOff'] = after;
      if (before > 0 && after >= before) {
        // The reclaim is `deleteStaleChangeLog`, called at the top of a
        // replicator worker. A replication-manager runs no *syncing*
        // replicator (`NUM_SYNC_WORKERS=0`), but it does run a
        // backup-replicator over the same replica path whenever a backup URL
        // is configured, and that is what normally reclaims the file. Report
        // the bytes rather than a cause: "does turning it off actually free
        // the disk" is the class-5 question, and a no here is the answer.
        out.findings.push(
          `C12: the change log still holds ${after} bytes (was ${before}) ` +
            `after the mode was rolled back to off`,
        );
      }
    },
  },
  {
    id: 'C13',
    title: 'A view-syncer already behind, meeting a reseed (C4 and C6)',
    expected:
      'stale replica discarded -> restore; RM readiness waits for a backup that passes the seed',
    async run(ctx, out) {
      const vs = viewSyncerAt(ctx, 1);
      const load = ctx.traffic.runStage({
        rate: 50,
        durationSeconds: Math.max(30, Math.round(90 * ctx.config.scale)),
        label: 'C13-load',
      });
      await sleep(2_000);
      await vs.stop('SIGQUIT');
      out.notes.push(`${vs.name} down and falling behind`);
      await sleep(ctx.config.backupIntervalSeconds * 4_000 + 10_000);

      const since = Date.now();
      await ctx.cluster.rm.stop('SIGTERM');
      await ctx.cluster.rm.deleteChangeLog();
      await ctx.cluster.startReplicationManager();
      const reseed = firstAfter(ctx.log, since, 'change-log-reseed');
      out.measurements['reseedReason'] = str(
        reseed?.detail.reason,
        'none-observed',
      );

      const backSince = Date.now();
      await vs.start();
      measureReservationHold(ctx.log, backSince, vs.name, out);
      recordConfirmationEvidence(ctx.log, backSince, out);
      expectNoDemotions(ctx, backSince, 'C13', out);
      await load;
    },
  },
  {
    id: 'C14',
    title: 'Kill the replication-manager and wipe its whole volume, restart',
    expected:
      'restore from the backup into a fresh generation, and no follower demoted while it backfills',
    async run(ctx, out) {
      // Every other action leaves the RM's disk intact, so the local LTX chain
      // is always there to be re-uploaded. This is the other case: the RM comes
      // back on an empty volume and has to restore from S3, which is what a
      // node replacement looks like in production.
      const load = ctx.traffic.runStage({
        rate: 50,
        durationSeconds: Math.max(30, Math.round(60 * ctx.config.scale)),
        label: 'C14-load',
      });
      await sleep(2_000);

      const before = await backupGenerations(ctx.config);
      out.measurements['generationsBefore'] = before.length;

      const since = Date.now();
      await ctx.cluster.rm.stop('SIGTERM');
      // A lost volume loses everything beside the replica too. `deleteReplica`
      // takes the `.replica.db-litestream` directory but not the change log,
      // which lives at `${replicaFile}-change-log`.
      await ctx.cluster.rm.deleteReplica();
      await ctx.cluster.rm.deleteChangeLog();
      out.notes.push("wiped rm's replica, litestream state and change log");

      await ctx.cluster.startReplicationManager();
      out.measurements['rmRestartMs'] = Date.now() - since;

      // Did it come back from the backup, or fall all the way back to a fresh
      // initial sync from Postgres? The difference is minutes of downtime in
      // production, so it is worth naming rather than inferring from timing.
      const restore = firstAfter(ctx.log, since, 'restore-started');
      out.measurements['restoreObserved'] = restore ? 'yes' : 'no';
      if (!restore) {
        out.findings.push(
          'C14 wiped the RM volume but no litestream restore was observed; ' +
            'the RM likely fell back to a full initial sync from Postgres',
        );
      }

      // The change log went with the volume, so this must be a `created`
      // reseed rather than a resume.
      const reseed = firstAfter(ctx.log, since, 'change-log-reseed');
      out.measurements['reseedReason'] = str(
        reseed?.detail.reason,
        'none-observed',
      );

      const after = await backupGenerations(ctx.config);
      out.measurements['generationsAfter'] = after.length;
      const minted = after.filter(g => !before.includes(g));
      out.measurements['generationsMinted'] = minted.length;
      if (minted.length === 0) {
        out.findings.push(
          'C14 expected the restarted RM to mint a new backup generation, ' +
            'but the bucket gained no new top-level prefix',
        );
      }

      // Losing the volume is *cheaper* than keeping it, which is worth
      // measuring rather than assuming. A warm restart still has the local LTX
      // chain, so litestream re-uploads it into the new generation one file per
      // transaction and the backup trails the replica while that runs. A
      // restored volume has no chain, so litestream starts a fresh one at the
      // restored state and the new generation is covered almost immediately.
      // The demotion assertion therefore lives on C6/C13, not here; this one
      // only records that the cheap path stayed cheap.
      const backSince = Date.now();
      await restartViewSyncer(ctx, 0, 'SIGTERM', out);
      const demotions = ctx.log.events.filter(
        e => e.tsMs >= backSince && e.kind === 'reservation-demoted',
      );
      out.measurements['demotionsAfterVolumeLoss'] = demotions.length;
      if (demotions.length > 0) {
        out.findings.push(
          `C14 demoted ${demotions.length} follower(s) to PG after the RM ` +
            'restored onto a fresh volume, which should be the cheap path: ' +
            'a restored replica has no local LTX chain to re-upload, so its ' +
            'new generation is covered almost immediately',
        );
      }
      await load;
    },
  },
  {
    id: 'C15',
    title: 'Interrupt a backfill mid-run by restarting the replication-manager',
    expected:
      "the run resumes from the replica's mark rather than starting over, and nobody is demoted or restored",
    async run(ctx, out) {
      // The other actions all interrupt *replication*. This one interrupts a
      // *backfill*, which is the one piece of RM state a restart cannot resume
      // from the change log: the run lives in a snapshot transaction that dies
      // with the process. What survives is the mark the replica recorded, and
      // the whole point of R7 is that the next run picks up from it.
      const table = `c15_backfill_${ctx.config.runID.replace(/[^a-z0-9]/gi, '')}`;
      const fixtureRows = await createBackfillFixture(ctx, table, out);
      try {
        // Settle the rows first. They replicate as ordinary inserts -- adding
        // a column is what needs a backfill, because the values it already
        // has for every existing row are not in the WAL.
        const settled = await waitForFixtureRows(ctx, table, fixtureRows);
        out.measurements['fixtureRowsSettled'] = settled ? 'yes' : 'no';

        const windowStart = Date.now();
        await addBackfillColumn(ctx, table, out);
        const announced = await waitForRunAnnouncement(
          ctx,
          table,
          windowStart,
          BACKFILL_START_TIMEOUT_MS,
        );
        out.measurements['firstRunStart'] = str(
          announced?.detail.start,
          'none-observed',
        );

        // A mark exists only once a backfill transaction has landed, so this
        // is what makes the restart interesting rather than a no-op.
        const progress = await waitForBackfillProgress(
          ctx,
          table,
          fixtureRows,
          out,
        );

        const since = Date.now();
        await ctx.cluster.rm.stop('SIGTERM');
        out.notes.push('SIGTERM rm mid-backfill');
        await ctx.cluster.startReplicationManager();
        out.measurements['rmRestartMs'] = Date.now() - since;

        const resumed = await waitForRunAnnouncement(
          ctx,
          table,
          since,
          BACKFILL_RESUME_TIMEOUT_MS,
        );
        out.measurements['resumedRunStart'] = str(
          resumed?.detail.start,
          'none-observed',
        );
        out.measurements['resumedFrom'] = JSON.stringify(
          resumed?.detail.resumeFrom ?? null,
        );

        const rowsAfter = await waitForBackfillCompletion(
          ctx,
          table,
          fixtureRows,
        );
        out.measurements['rowsAfterResume'] = rowsAfter
          .map(c => `${c.node}=${c.rows}`)
          .join(',');

        // A backfill restart must not look like a replication gap to anyone:
        // no follower demoted to PG, and none thrown back to a restore.
        const demotions = ctx.log.events.filter(
          e => e.tsMs >= since && e.kind === 'reservation-demoted',
        ).length;
        // Followers only. The replication-manager restores its own replica on
        // every start -- that is the restart path C5 exercises, not a
        // subscriber being thrown back to the backup.
        const rmNode = ctx.cluster.rm.name;
        const restores = ctx.log.events.filter(
          e =>
            e.tsMs >= since &&
            e.kind === 'restore-started' &&
            e.node !== rmNode,
        ).length;
        out.measurements['demotions'] = demotions;
        out.measurements['restoresAfterBackfillRestart'] = restores;

        out.findings.push(
          ...c15Findings({
            table,
            fixtureRows,
            fixtureRowsSettled: settled,
            runAnnounced: announced !== undefined,
            markedBeforeRestart: progress.marked,
            rowsFilledBeforeRestart: progress.filled,
            resumedStart: resumed
              ? str(resumed.detail.start, 'unknown')
              : 'none-observed',
            demotions,
            restores,
            rowsAfterResume: rowsAfter,
          }),
        );
      } finally {
        await dropBackfillFixture(ctx, table);
      }
    },
  },
  {
    id: 'C16',
    title: 'Hold a snapshot reservation open under sustained writes',
    expected:
      'the change log is retained for the whole hold and drains after it; the upstream ACK keeps advancing',
    async run(ctx, out) {
      // Every other action leaves a reservation open for milliseconds -- the
      // holds observed in a soak are 1-11ms, because the follower restores a
      // small local replica. A production restore of a large replica out of S3
      // is minutes, and for all of them the purge floor is pinned at the
      // `minWatermark` the follower was promised. It is more than pinned:
      // `startSnapshotReservation` takes a `pause(taskID)` on the purge
      // scheduler that is only released when the reservation closes, so no
      // pass deletes anything at all while one is open.
      //
      // This holds a reservation without restoring anything, which is the only
      // way to ask the question without a replica big enough to take minutes
      // to download.
      const holdSeconds = Math.max(60, Math.round(300 * ctx.config.scale));
      const load = ctx.traffic.runStage({
        rate: 25,
        durationSeconds: holdSeconds + 60,
        label: 'C16-sustained',
      });
      const before = await ctx.sampler.sample();
      const reservation = await holdSnapshotReservation(ctx, C16_TASK_ID);
      const heldFrom = Date.now();
      const reservedWatermark = reservation.status?.minWatermark;
      out.notes.push(
        `holding a snapshot reservation as ${C16_TASK_ID} for ${holdSeconds}s`,
      );
      out.measurements['holdSeconds'] = holdSeconds;
      out.measurements['reservationConfirmMs'] = reservation.confirmMs;
      out.measurements['reservedWatermark'] =
        reservedWatermark ?? 'unconfirmed';

      let during: ResourceSample | undefined;
      let releasedAt = heldFrom;
      try {
        await sleep(holdSeconds * 1000);
        during = await ctx.sampler.sample();
      } finally {
        // A reservation left open would pin the floor for the rest of the run,
        // so it is released even if the hold itself failed. The boundary is
        // taken *before* the release rather than after it: a purge in the
        // milliseconds it takes the socket to close would then be read as an
        // ordinary post-release pass rather than as a violation, and a
        // spurious finding is worse here than an unobservable one.
        releasedAt = Date.now();
        await reservation.release();
        out.notes.push('released the snapshot reservation');
      }

      const passes = ctx.log.events.filter(
        e =>
          e.kind === 'purge-pass' && e.tsMs >= heldFrom && e.tsMs < releasedAt,
      );
      const rowsPurgedDuringHold = passes.reduce(
        (sum, e) => sum + num(e.detail.deletedRows),
        0,
      );
      // A floor above the reservation would mean the floor formula dropped it;
      // an absent floor field cannot be above anything.
      const passesAboveReservedWatermark =
        reservedWatermark === undefined
          ? 0
          : passes.filter(e => str(e.detail.floor, '') > reservedWatermark)
              .length;
      const backupWatermarksDuringHold = ctx.log.events.filter(
        e =>
          e.kind === 'backup-watermark' &&
          e.tsMs >= heldFrom &&
          e.tsMs < releasedAt,
      ).length;
      out.measurements['purgePassesDuringHold'] = passes.length;
      out.measurements['purgePassesPausedDuringHold'] = passes.filter(
        e => e.detail.stopped === 'paused',
      ).length;
      out.measurements['rowsPurgedDuringHold'] = rowsPurgedDuringHold;
      out.measurements['passesAboveReservedWatermark'] =
        passesAboveReservedWatermark;
      out.measurements['backupWatermarksDuringHold'] =
        backupWatermarksDuringHold;

      // Let the writes stop before measuring the drain, so that the recovery
      // is the purger catching up rather than a race with the workload.
      await load;
      const drainPass = await ctx.log
        .waitFor(
          'a purge pass after the reservation was released',
          e => e.kind === 'purge-pass' && num(e.detail.deletedRows) > 0,
          RESERVATION_DRAIN_TIMEOUT_MS,
          releasedAt,
        )
        .catch(() => undefined);
      const after = await ctx.sampler.sample();
      const rowsPurgedAfterRelease = ctx.log.events
        .filter(e => e.kind === 'purge-pass' && e.tsMs >= releasedAt)
        .reduce((sum, e) => sum + num(e.detail.deletedRows), 0);
      out.measurements['drainPassObserved'] = drainPass ? 'yes' : 'no';
      out.measurements['rowsPurgedAfterRelease'] = rowsPurgedAfterRelease;

      const liveBytes = (s: ResourceSample | undefined) =>
        s?.changeLogLiveBytes ?? -1;
      const slotBytes = (s: ResourceSample | undefined) =>
        (s?.slots ?? []).reduce((acc, slot) => acc + slot.retainedBytes, 0);
      out.measurements['changeLogBytesBefore'] = before?.changeLogBytes ?? -1;
      out.measurements['changeLogBytesDuring'] = during?.changeLogBytes ?? -1;
      out.measurements['changeLogBytesAfter'] = after?.changeLogBytes ?? -1;
      out.measurements['changeLogLiveBytesBefore'] = liveBytes(before);
      out.measurements['changeLogLiveBytesDuring'] = liveBytes(during);
      out.measurements['changeLogLiveBytesAfter'] = liveBytes(after);
      // The slot is recorded rather than gated. A reservation pins the purge
      // floor but not the upstream ACK -- that is
      // `min(pgChangeLogWatermark, backupWatermark)`, which no reservation
      // enters, and which is the whole difference between this and C9. WAL is
      // retained in segments, though, so a short run's slot numbers are too
      // lumpy to assert on; the ACK-path evidence that *is* gated is
      // `backupWatermarksDuringHold`.
      out.measurements['slotRetainedBytesBefore'] = slotBytes(before);
      out.measurements['slotRetainedBytesDuring'] = slotBytes(during);
      out.measurements['slotRetainedBytesAfter'] = slotBytes(after);

      out.findings.push(
        ...c16Findings({
          reservedWatermark,
          rowsPurgedDuringHold,
          passesAboveReservedWatermark,
          backupWatermarksDuringHold,
          changeLogLiveBytesBefore: liveBytes(before),
          changeLogLiveBytesDuring: liveBytes(during),
          changeLogLiveBytesAfter: liveBytes(after),
          rowsPurgedAfterRelease,
        }),
      );
    },
  },
];

export async function runChaosAction(
  action: ChaosAction,
  ctx: ChaosContext,
): Promise<ChaosOutcome> {
  const startedMs = Date.now();
  const before = census(ctx.metrics);
  const out: MutableOutcome = {
    startedMs,
    notes: [],
    findings: [],
    measurements: {},
  };
  ctx.note(`${action.id}: ${action.title}`);
  try {
    await action.run(ctx, out);
    measureReseedWindow(ctx.log, out);
  } catch (e) {
    out.findings.push(
      `${action.id} threw: ${e instanceof Error ? e.message : String(e)}`,
    );
  }
  // One export interval so the counters for this window have landed.
  await sleep(ctx.config.metricExportIntervalMs * 2);
  return {
    id: action.id,
    title: action.title,
    startedMs,
    finishedMs: Date.now(),
    notes: out.notes,
    census: censusDelta(before, census(ctx.metrics)),
    findings: out.findings,
    measurements: out.measurements,
  };
}
