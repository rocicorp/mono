import {stat} from 'node:fs/promises';
import type postgres from 'postgres';
import type {TrafficDriver} from '../change-log-traffic.ts';
import type {SoakCluster} from './cluster.ts';
import type {SoakConfig} from './config.ts';
import {sleep, startMinio, stopMinio} from './infra.ts';
import type {SoakEvent, SoakLog} from './logs.ts';
import type {MetricStore} from './otlp.ts';
import type {ResourceSampler} from './resources.ts';

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
      'forced `created` reseed, then a restore held until a backup passes the seed',
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
      // The restore that has to be held: the RM reseeded at the replica head
      // while the newest backup still sits behind it, so the log alone cannot
      // bridge the gap. Today this ends in a free demotion to PG; after PG is
      // retired it becomes a wait.
      await restartViewSyncer(ctx, 2, 'SIGQUIT', out, {deleteReplica: true});
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
      const downSeconds = Math.max(60, Math.round(120 * ctx.config.scale));
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
      out.notes.push('restarted minio');
      await load;
      // Let the backup catch up and the acker walk the slot forward.
      await sleep(30_000);
      const after = await ctx.sampler.sample();
      const slotBytes = (s: typeof before) =>
        (s?.slots ?? []).reduce((acc, slot) => acc + slot.retainedBytes, 0);
      out.measurements['changeLogBytesBefore'] = before?.changeLogBytes ?? -1;
      out.measurements['changeLogBytesDuring'] = during?.changeLogBytes ?? -1;
      out.measurements['changeLogBytesAfter'] = after?.changeLogBytes ?? -1;
      out.measurements['slotRetainedBytesBefore'] = slotBytes(before);
      out.measurements['slotRetainedBytesDuring'] = slotBytes(during);
      out.measurements['slotRetainedBytesAfter'] = slotBytes(after);
      if (
        after &&
        during &&
        after.changeLogBytes > during.changeLogBytes &&
        during.changeLogBytes > 0
      ) {
        out.findings.push(
          'C9: the change log did not shrink after the backup recovered',
        );
      }
      if (slotBytes(after) > slotBytes(during) && slotBytes(during) > 0) {
        out.findings.push(
          'C9: retained WAL did not drain after the backup recovered',
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
      'watermark-uncovered -> forced restore -> held until a backup passes the seed',
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
      await load;
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
