import {writeFile} from 'node:fs/promises';
import type {StageResult} from '../change-log-traffic.ts';
import type {ChaosOutcome} from './chaos.ts';
import type {SoakConfig} from './config.ts';
import type {SoakLog, Tripwire} from './logs.ts';
import type {OracleResult} from './oracle.ts';
import type {MetricStore} from './otlp.ts';
import {purgeStreaks, type ResourceSample} from './resources.ts';

/**
 * Plan sections 7.5 (tripwires), 7.6 (coverage) and 8 (measured quantities).
 *
 * A soak that quietly routes everything to PG passes the oracle and the
 * tripwires while proving nothing, so coverage is a *positive* requirement:
 * at least one of each route, or an explicit "not exercised" row.
 */

/** Drawn from `ChangeLogReadRouteReason`, the closed set of route reasons. */
export const REQUIRED_ROUTES: readonly {
  route: string;
  triggeredBy: string;
}[] = [
  {route: 'sqlite/selected', triggeredBy: 'C1, C2, C4'},
  {
    route: 'sqlite/selected-cold',
    triggeredBy: 'C6, and reconnects within retentionMs of a seed',
  },
  {route: 'pg/backup-uncovered', triggeredBy: 'C6 inside the reseed window'},
  {
    // Structurally hard to reach, and measured to be so. A *restarting*
    // follower cannot take it at any gap length: `restoreReplica` runs on
    // every view-syncer start and the snapshot reservation hands it the log's
    // `minWatermark`, so a replica below the minimum is discarded and
    // re-restored before the subscriber reaches `/changes` (C4's long gap
    // asserts exactly that conversion). It belongs instead to a follower that
    // survives a *stream* disconnect long enough for the purge floor to pass
    // its ack -- and SIGSTOP does not produce one, because the change-streamer
    // keeps a frozen subscriber registered and lets it drain on resume.
    route: 'pg/watermark-uncovered',
    triggeredBy:
      'a stream disconnect outliving the purge floor; not reachable from a restart',
  },
];

export type PhaseRecord = {
  readonly name: string;
  readonly startedMs: number;
  readonly finishedMs: number;
  readonly stages: StageResult[];
};

export type SoakReport = {
  readonly runID: string;
  readonly startedMs: number;
  readonly finishedMs: number;
  readonly config: Record<string, unknown>;
  readonly phases: PhaseRecord[];
  readonly oracle: OracleResult[];
  readonly chaos: ChaosOutcome[];
  readonly census: Record<string, number>;
  readonly coverage: {route: string; count: number; triggeredBy: string}[];
  readonly tripwires: Tripwire[];
  readonly measurements: Record<string, unknown>;
  readonly resources: Record<string, unknown>;
  readonly baseline?: Record<string, unknown> | undefined;
  readonly findings: string[];
  readonly pass: boolean;
};

function stats(values: readonly number[]): {
  n: number;
  min?: number | undefined;
  p50?: number | undefined;
  p95?: number | undefined;
  max?: number | undefined;
  mean?: number | undefined;
} {
  if (values.length === 0) {
    return {n: 0};
  }
  const sorted = values.toSorted((a, b) => a - b);
  const at = (p: number) =>
    sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * p) - 1)];
  return {
    n: sorted.length,
    min: sorted[0],
    p50: at(0.5),
    p95: at(0.95),
    max: sorted.at(-1),
    mean: sorted.reduce((a, b) => a + b, 0) / sorted.length,
  };
}

/**
 * Every numeric measurement whose key is `key` or starts with `${key}.`
 * (the per-task form, e.g. `reservationHoldMs.vs-0`), optionally restricted
 * to a set of chaos action ids.
 */
function numbersFromChaos(
  chaos: readonly ChaosOutcome[],
  key: string,
  ids?: readonly string[],
): number[] {
  const out: number[] = [];
  for (const outcome of chaos) {
    if (ids && !ids.includes(outcome.id)) {
      continue;
    }
    for (const [name, value] of Object.entries(outcome.measurements)) {
      if (
        (name === key || name.startsWith(`${key}.`)) &&
        typeof value === 'number'
      ) {
        out.push(value);
      }
    }
  }
  return out;
}

export function buildReport(args: {
  config: SoakConfig;
  startedMs: number;
  phases: PhaseRecord[];
  oracle: OracleResult[];
  chaos: ChaosOutcome[];
  log: SoakLog;
  metrics: MetricStore;
  resources: readonly ResourceSample[];
  baseline?: Record<string, unknown> | undefined;
}): SoakReport {
  const {config, log, metrics, chaos, oracle, resources} = args;

  const census = metrics.byAttributes(
    'sqlite_change_log.catchup_routes',
    'source',
    'reason',
  );
  const coverage = REQUIRED_ROUTES.map(({route, triggeredBy}) => ({
    route,
    count: census[route] ?? 0,
    triggeredBy,
  }));

  const backupLags = log.events
    .filter(e => e.kind === 'backup-watermark')
    .map(e => e.detail.lagMs)
    .filter((v): v is number => typeof v === 'number');

  const reseedWindows = numbersFromChaos(chaos, 'reseedToCoveringBackupMs');
  const holds = numbersFromChaos(chaos, 'reservationHoldMs');

  const measurements: Record<string, unknown> = {
    // Section 8's headline, in the only two forms that mean anything.
    //
    // `reseedToCoveringBackupMs` is the window itself: reseed -> the first
    // backup observed at or above the seed. Nothing can be served from the
    // log before it, so it is the exposure, and it holds whether or not a
    // follower happens to be waiting on it.
    //
    // `reservationHoldMs` is what a follower actually waited, which is ~0
    // whenever it arrived after that backup had already landed.
    reseedToCoveringBackupMs: stats(reseedWindows),
    reservationHoldMs: stats(holds),
    // C3 restores against a *live* log, which invariant 14 says is covered
    // by construction; this is the number that should read ~0.
    reservationHoldMsC3: stats(
      numbersFromChaos(chaos, 'reservationHoldMs', ['C3']),
    ),
    // C6 and C13 restore against a log that just reseeded, which is the
    // window section 1.4 is about.
    reseedToCoveringBackupMsC6C13: stats(
      numbersFromChaos(chaos, 'reseedToCoveringBackupMs', ['C6', 'C13']),
    ),
    backupLagMs: stats(backupLags),
    reservationConfirmDelays: metrics.total(
      'sqlite_change_log.reservation_confirm_delays',
    ),
    reservationDemotions: metrics.total(
      'sqlite_change_log.reservation_demotions',
    ),
    barrierTimeouts: metrics.total('sqlite_change_log.barrier_timeouts'),
    compareResults: metrics.byAttribute(
      'sqlite_change_log.compare_result',
      'outcome',
    ),
    initCompareResults: metrics.byAttribute(
      'sqlite_change_log.init_compare_result',
      'result',
    ),
    floorProbes: metrics.byAttribute(
      'sqlite_change_log.purge_floor_probe',
      'outcome',
    ),
    reconcileWipes: metrics.byAttribute(
      'sqlite_change_log.reconcile_wipes',
      'reason',
    ),
    purgeDeclines: metrics.byAttribute(
      'sqlite_change_log.purge_declined',
      'reason',
    ),
    catchupRows: metrics.total('sqlite_change_log.catchup_rows'),
    catchupBytes: metrics.total('sqlite_change_log.catchup_bytes'),
    catchupDurationSeconds: metrics.total('sqlite_change_log.catchup_duration'),
    catchupResults: metrics.byAttributes(
      'sqlite_change_log.catchup_results',
      'classification',
      'log_warm',
    ),
    litestreamRestores: metrics.byAttribute(
      'litestream.restore.runs',
      'result',
    ),
    purgedRows: metrics.total('sqlite_change_log.purged_rows'),
  };

  const changeLogBytes = resources.map(s => s.changeLogBytes);
  const slotRetained = resources.map(s =>
    s.slots.reduce((acc, slot) => acc + slot.retainedBytes, 0),
  );
  const resourceReport: Record<string, unknown> = {
    changeLogBytes: stats(changeLogBytes),
    changeLogBytesFinal: changeLogBytes.at(-1) ?? 0,
    slotRetainedBytes: stats(slotRetained),
    slotRetainedBytesFinal: slotRetained.at(-1) ?? 0,
    backupBytesFinal: resources.at(-1)?.backupBytes ?? -1,
    backupObjectsFinal: resources.at(-1)?.backupObjects ?? -1,
    purge: purgeStreaks(log),
    samples: resources.length,
  };

  const findings: string[] = [];
  for (const outcome of chaos) {
    findings.push(...outcome.findings);
  }
  for (const result of oracle) {
    if (!result.ok) {
      findings.push(
        `oracle "${result.label}" diverged (${result.verdict}): ` +
          result.comparisons
            .filter(c => !c.ok)
            .map(
              c =>
                `${c.node}${c.error ? ` error=${c.error}` : ''} ` +
                c.diffs
                  .map(
                    d =>
                      `${d.table}[pg=${d.pgRows} replica=${d.replicaRows} ` +
                      `missing=${d.missingFromReplica.length} ` +
                      `extra=${d.extraInReplica.length} ` +
                      `mismatched=${d.mismatched.length}]`,
                  )
                  .join(' '),
            )
            .join('; '),
      );
    }
  }
  const namedTripwires = log.tripwires.filter(t => t.name !== 'error-log');
  for (const tripwire of namedTripwires) {
    findings.push(
      `tripwire ${tripwire.name} on ${tripwire.node}: ${tripwire.message}`,
    );
  }
  const uncovered = coverage.filter(c => c.count === 0);
  for (const row of uncovered) {
    findings.push(
      `coverage: ${row.route} was not exercised (expected from ${row.triggeredBy})`,
    );
  }
  const purge = purgeStreaks(log);
  if (purge.longestBatchLimitStreak > 20) {
    findings.push(
      `resource bound: the purger hit its batch limit ${purge.longestBatchLimitStreak} ` +
        `passes in a row, which is a purger losing to the write rate`,
    );
  }

  // Coverage gaps are reported but do not fail the run: a smoke-scale run
  // legitimately skips the chaos actions that produce them. Divergence,
  // tripwires and chaos findings do.
  const pass =
    oracle.every(r => r.ok) &&
    namedTripwires.length === 0 &&
    chaos.every(c => c.findings.length === 0);

  return {
    runID: config.runID,
    startedMs: args.startedMs,
    finishedMs: Date.now(),
    config: {
      viewSyncers: config.viewSyncers,
      changeLog: config.changeLog,
      backupIntervalSeconds: config.backupIntervalSeconds,
      vfsPollIntervalMs: config.vfsPollIntervalMs,
      startupDelayMs: config.startupDelayMs,
      snapshotBackupIntervalHours: config.snapshotBackupIntervalHours,
      chaos: config.chaos,
      scale: config.scale,
      baseline: config.baseline,
      root: config.root,
    },
    phases: args.phases,
    oracle,
    chaos,
    census,
    coverage,
    tripwires: log.tripwires,
    measurements,
    resources: resourceReport,
    baseline: args.baseline,
    findings,
    pass,
  };
}

export async function writeReport(
  path: string,
  report: SoakReport,
): Promise<void> {
  await writeFile(path, JSON.stringify(report, null, 2), 'utf8');
}

function fmt(value: unknown): string {
  if (value === undefined) {
    return '-';
  }
  if (typeof value === 'number') {
    return Number.isInteger(value) ? String(value) : value.toFixed(1);
  }
  return typeof value === 'string' ? value : JSON.stringify(value);
}

export function summarize(report: SoakReport): string {
  const lines: string[] = [];
  const seconds = ((report.finishedMs - report.startedMs) / 1000).toFixed(0);
  lines.push('');
  lines.push(
    `RMv2 local soak ${report.runID} -- ${report.pass ? 'PASS' : 'FAIL'} (${seconds}s)`,
  );
  lines.push('');

  lines.push('Phases');
  for (const phase of report.phases) {
    const tx = phase.stages.reduce((acc, s) => acc + s.transactions, 0);
    const mutations = phase.stages.reduce((acc, s) => acc + s.mutations, 0);
    lines.push(
      `  ${phase.name.padEnd(12)} ${((phase.finishedMs - phase.startedMs) / 1000).toFixed(0)}s ` +
        `tx=${tx} mutations=${mutations}`,
    );
  }

  lines.push('');
  lines.push('Route census (source/reason)');
  const censusKeys = Object.keys(report.census).sort();
  if (censusKeys.length === 0) {
    lines.push('  (no catchup routes recorded)');
  }
  for (const key of censusKeys) {
    lines.push(`  ${key.padEnd(32)} ${report.census[key]}`);
  }

  lines.push('');
  lines.push('Coverage (section 7.6)');
  for (const row of report.coverage) {
    lines.push(
      `  ${row.route.padEnd(28)} ${
        row.count > 0 ? String(row.count) : 'NOT EXERCISED'
      }  (${row.triggeredBy})`,
    );
  }

  lines.push('');
  lines.push('Measured quantities (section 8)');
  for (const [key, value] of Object.entries(report.measurements)) {
    lines.push(`  ${key.padEnd(32)} ${fmt(value)}`);
  }

  lines.push('');
  lines.push('Resource bounds (section 7.7)');
  for (const [key, value] of Object.entries(report.resources)) {
    lines.push(`  ${key.padEnd(32)} ${fmt(value)}`);
  }

  lines.push('');
  lines.push('Chaos');
  for (const outcome of report.chaos) {
    const census = Object.entries(outcome.census)
      .map(([k, v]) => `${k}=${v}`)
      .join(' ');
    lines.push(
      `  ${outcome.id} ${outcome.title} (${(
        (outcome.finishedMs - outcome.startedMs) /
        1000
      ).toFixed(0)}s)`,
    );
    if (census) {
      lines.push(`      routes: ${census}`);
    }
    for (const [key, value] of Object.entries(outcome.measurements)) {
      lines.push(`      ${key}=${fmt(value)}`);
    }
    for (const note of outcome.notes) {
      lines.push(`      - ${note}`);
    }
  }

  lines.push('');
  lines.push('Oracle');
  for (const result of report.oracle) {
    lines.push(
      `  ${result.ok ? 'ok  ' : 'FAIL'} ${result.label} ` +
        `(quiesce ${result.quiesceMs}ms, ${result.verdict})`,
    );
  }

  if (report.baseline) {
    lines.push('');
    lines.push('Baseline A/B (class 6, section 9 slice L9)');
    for (const [key, value] of Object.entries(report.baseline)) {
      lines.push(`  ${key.padEnd(32)} ${fmt(value)}`);
    }
  }

  lines.push('');
  if (report.findings.length === 0) {
    lines.push('Findings: none');
  } else {
    lines.push('Findings');
    for (const finding of report.findings) {
      lines.push(`  ! ${finding}`);
    }
  }
  lines.push('');
  return lines.join('\n');
}
