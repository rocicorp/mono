/* oxlint-disable no-console */

import '../../../packages/shared/src/dotenv.ts';

import {existsSync, mkdirSync} from 'node:fs';
import {createServer} from 'node:net';
import {join} from 'node:path';
import {argv} from 'node:process';
import {parseArgs} from 'node:util';
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {pgClient} from '../../../packages/zero-cache/src/types/pg.ts';
import {TrafficDriver, type StageResult} from './change-log-traffic.ts';
import {
  CHAOS_ACTIONS,
  runChaosAction,
  type ChaosContext,
  type ChaosOutcome,
} from './rmv2-soak/chaos.ts';
import {SoakCluster} from './rmv2-soak/cluster.ts';
import {
  APP_ROOT,
  BIN_DIR,
  makeRunDir,
  vsPort,
  type SoakConfig,
} from './rmv2-soak/config.ts';
import {resetBackup, run, sleep, startInfra} from './rmv2-soak/infra.ts';
import {SoakLog} from './rmv2-soak/logs.ts';
import {runOracle, type OracleResult} from './rmv2-soak/oracle.ts';
import {OtlpMetricsReceiver, type MetricStore} from './rmv2-soak/otlp.ts';
import {
  buildReport,
  summarize,
  writeReport,
  type PhaseRecord,
} from './rmv2-soak/report.ts';
import {ResourceSampler} from './rmv2-soak/resources.ts';
import {mixedPhase, workloadPhases} from './rmv2-soak/workload.ts';

/** Slice L10: the rollback drills, which must run after everything else. */
const ROLLBACK_DRILLS = new Set(['C10', 'C11', 'C12']);

/**
 * Everything except C9 (a five-minute minio outage) and the rollback drills,
 * which leave the change log rolled back to `off`. `--chaos all` adds them.
 */
const DEFAULT_CHAOS = 'C1,C2,C3,C4,C5,C6,C7,C8,C13';

const USAGE = `
A local, reproducible end-to-end exercise of the SQLite change log in serve
mode against a real litestream v5 backup (minio/S3), with real view-syncer
restores, disconnects and reconnects.

Usage:
  node scripts/rmv2-soak.ts [options]

Prerequisites:
  scripts/build-litestream.sh          builds litestream v3/v5 and vfs-query
  docker                               postgres and minio come up from
                                       docker/docker-compose{,.minio}.yml

Options:
  --view-syncers <n>        Number of view-syncers. Default: 3
  --mode <mode>             off | write | compare | serve. Default: serve
  --read-percent <n>        Default: 100
  --cold-read-percent <n>   Default: 100 (zeroes the warm-up wait; the
                            residual wait is a reseed window, not a restore
                            window)
  --compare-percent <n>     Default: 100
  --retention-ms <n>        Default: 60000. Must be > 0; it is the purger's
                            retention floor, not a routing knob.
  --scale <f>               Multiplies every phase duration. Default: 1
  --chaos <list>            Comma-separated action ids, or "none", or "all".
                            Default: C1-C8 and C13. "all" adds C9 (a long
                            minio outage) and the C10-C12 rollback drills,
                            which run last and leave the log turned off.
  --baseline                Also run the mode=off A/B control pass first
  --backup-interval-seconds <n>  litestream monitor-interval. Default: 2
  --vfs-poll-ms <n>         Default: 1000
  --startup-delay-ms <n>    change-streamer startup delay. Default: 1000
  --snapshot-hours <n>      litestream snapshot interval. Default: 4
  --seed                    Run db-migrate and db-seed first
  --keep                    Leave processes and directories in place on exit
  --run-id <id>             Default: a timestamp
  --help
`;

function numOpt(name: string, value: string | undefined, dflt: number): number {
  if (value === undefined) {
    return dflt;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`--${name} must be a number`);
  }
  return parsed;
}

function parseConfig(): SoakConfig {
  const {values} = parseArgs({
    options: {
      'view-syncers': {type: 'string'},
      'mode': {type: 'string'},
      'read-percent': {type: 'string'},
      'cold-read-percent': {type: 'string'},
      'compare-percent': {type: 'string'},
      'retention-ms': {type: 'string'},
      'scale': {type: 'string'},
      'chaos': {type: 'string'},
      'baseline': {type: 'boolean', default: false},
      'backup-interval-seconds': {type: 'string'},
      'vfs-poll-ms': {type: 'string'},
      'startup-delay-ms': {type: 'string'},
      'snapshot-hours': {type: 'string'},
      'seed': {type: 'boolean', default: false},
      'keep': {type: 'boolean', default: false},
      'run-id': {type: 'string'},
      'help': {type: 'boolean', default: false},
    },
    strict: true,
  });

  if (values.help) {
    console.log(USAGE.trim());
    process.exit(0);
  }

  const mode = (values.mode ?? 'serve') as SoakConfig['changeLog']['mode'];
  if (!['off', 'write', 'compare', 'serve'].includes(mode)) {
    throw new Error(`--mode must be off | write | compare | serve`);
  }
  const readPercent = numOpt(
    'read-percent',
    values['read-percent'],
    mode === 'serve' ? 100 : 0,
  );
  const coldReadPercent = numOpt(
    'cold-read-percent',
    values['cold-read-percent'],
    mode === 'serve' ? 100 : 0,
  );
  const retentionMs = numOpt('retention-ms', values['retention-ms'], 60_000);
  if (retentionMs <= 0) {
    // `sqlite-change-log-purge-scheduler.ts` asserts retentionMs > 0. It is
    // the purger's retention floor; zeroing it would be a request to purge
    // everything, not a request to serve early.
    throw new Error('--retention-ms must be a positive integer');
  }

  const runID =
    values['run-id'] ?? new Date().toISOString().replace(/[:.]/g, '-');
  const root = join(APP_ROOT, '.soak', runID);
  const {logsDir} = makeRunDir(root);

  const chaosArg = values.chaos ?? DEFAULT_CHAOS;
  const chaos =
    chaosArg === 'none'
      ? []
      : chaosArg === 'all'
        ? CHAOS_ACTIONS.map(a => a.id)
        : chaosArg.split(',').map(s => s.trim().toUpperCase());
  for (const id of chaos) {
    if (!CHAOS_ACTIONS.some(a => a.id === id)) {
      throw new Error(
        `unknown chaos action ${id}; known: ${CHAOS_ACTIONS.map(a => a.id).join(', ')}`,
      );
    }
  }

  const upstreamDB = process.env.ZERO_UPSTREAM_DB;
  if (!upstreamDB) {
    throw new Error('ZERO_UPSTREAM_DB is required (apps/zbugs/.env sets it)');
  }

  return {
    runID,
    root,
    logsDir,
    reportPath: join(root, 'report.json'),
    upstreamDB,
    viewSyncers: numOpt('view-syncers', values['view-syncers'], 3),
    rmPort: 4850,
    vsBasePort: 4860,
    otlpPort: 4899,
    backupURL: 's3://zero-replica/zbugs',
    s3Endpoint: 'http://127.0.0.1:9000',
    s3Region: 'us-east-1',
    accessKeyID: 'minioadmin',
    secretAccessKey: 'minioadmin',
    changeLog: {
      mode,
      readPercent,
      coldReadPercent,
      comparePercent: numOpt(
        'compare-percent',
        values['compare-percent'],
        mode === 'off' || mode === 'write' ? 0 : 100,
      ),
      retentionMs,
    },
    backupIntervalSeconds: numOpt(
      'backup-interval-seconds',
      values['backup-interval-seconds'],
      2,
    ),
    vfsPollIntervalMs: numOpt('vfs-poll-ms', values['vfs-poll-ms'], 1000),
    startupDelayMs: numOpt(
      'startup-delay-ms',
      values['startup-delay-ms'],
      1000,
    ),
    snapshotBackupIntervalHours: numOpt(
      'snapshot-hours',
      values['snapshot-hours'],
      4,
    ),
    // Debug on the replication-manager is what makes the SQLite route
    // visible: `serving <id> from SQLite catchup` is a debug line. The
    // view-syncers stay at info so the burst phase's log volume does not
    // perturb the timings it is measuring.
    rmLogLevel: 'debug',
    vsLogLevel: 'info',
    chaos,
    baseline: values.baseline,
    scale: numOpt('scale', values.scale, 1),
    seed: values.seed,
    keep: values.keep,
    metricExportIntervalMs: 2_000,
  };
}

async function runOrThrow(
  command: string,
  args: readonly string[],
): Promise<void> {
  const {code, stdout, stderr} = await run(command, args, APP_ROOT);
  if (code !== 0) {
    throw new Error(
      `${command} ${args.join(' ')} exited with ${code}\n${stdout}\n${stderr}`,
    );
  }
}

/**
 * A port still held by a previous run's orphaned worker is the most confusing
 * way for a soak to fail: the new replication-manager cannot take the
 * replica's journal-mode lock and dies with `SQLITE_BUSY` several layers down.
 * Fail up front, by name.
 */
async function assertPortsFree(config: SoakConfig): Promise<void> {
  const ports = [
    config.rmPort,
    config.rmPort + 1,
    config.rmPort + 2,
    config.otlpPort,
    ...Array.from({length: config.viewSyncers}, (_, i) => vsPort(config, i)),
    ...Array.from(
      {length: config.viewSyncers},
      (_, i) => vsPort(config, i) + 2,
    ),
  ];
  const busy: number[] = [];
  for (const port of ports) {
    const free = await new Promise<boolean>(resolve => {
      const probe = createServer();
      probe.once('error', () => resolve(false));
      probe.listen(port, '127.0.0.1', () => probe.close(() => resolve(true)));
    });
    if (!free) {
      busy.push(port);
    }
  }
  if (busy.length > 0) {
    throw new Error(
      `port(s) ${busy.join(', ')} are in use. A previous soak may have left ` +
        `workers running; check with ` +
        `\`ps -Ao pid=,command= | grep zero-cache/src/server\``,
    );
  }
}

function assertPrerequisites(): void {
  const missing = ['litestream-v3', 'litestream-v5', 'vfs-query'].filter(
    bin => !existsSync(join(BIN_DIR, bin)),
  );
  if (missing.length > 0) {
    throw new Error(
      `missing ${missing.join(', ')} in ${BIN_DIR}. Run scripts/build-litestream.sh`,
    );
  }
}

type ThroughputSnapshot = {
  transactions: number;
  forwardDurationSeconds: number;
  messageProcessingSeconds: number;
  logCommitSeconds: number;
};

function throughput(metrics: MetricStore): ThroughputSnapshot {
  return {
    transactions: metrics.total('replication.transactions'),
    forwardDurationSeconds: metrics.total('transaction_forward_duration'),
    messageProcessingSeconds: metrics.total(
      'sqlite_change_log.message_processing_duration',
    ),
    logCommitSeconds: metrics.total('sqlite_change_log.log_commit_duration'),
  };
}

function throughputDelta(
  before: ThroughputSnapshot,
  after: ThroughputSnapshot,
): ThroughputSnapshot {
  return {
    transactions: after.transactions - before.transactions,
    forwardDurationSeconds:
      after.forwardDurationSeconds - before.forwardDurationSeconds,
    messageProcessingSeconds:
      after.messageProcessingSeconds - before.messageProcessingSeconds,
    logCommitSeconds: after.logCommitSeconds - before.logCommitSeconds,
  };
}

function stageSummary(stages: readonly StageResult[]): Record<string, number> {
  const transactions = stages.reduce((a, s) => a + s.transactions, 0);
  const elapsed = stages.reduce((a, s) => a + s.elapsedSeconds, 0);
  const writing = stages.filter(s => s.targetTransactionsPerSecond > 0);
  return {
    transactions,
    elapsedSeconds: Number(elapsed.toFixed(1)),
    actualTransactionsPerSecond: Number(
      (transactions / Math.max(1, elapsed)).toFixed(2),
    ),
    p95LatencyMs: Number(
      (
        writing.reduce((a, s) => a + s.latencyMs.p95, 0) /
        Math.max(1, writing.length)
      ).toFixed(1),
    ),
  };
}

async function main(): Promise<void> {
  const config = parseConfig();
  assertPrerequisites();
  await assertPortsFree(config);
  const lc = new LogContext('warn', {}, consoleLogSink);
  const startedMs = Date.now();

  const say = (message: string) =>
    console.log(
      `[soak +${((Date.now() - startedMs) / 1000).toFixed(0)}s] ${message}`,
    );

  say(`run ${config.runID}`);
  say(`root ${config.root}`);

  const receiver = new OtlpMetricsReceiver();
  await receiver.listen(config.otlpPort);
  const metrics = receiver.store;

  say('bringing up postgres and minio');
  await startInfra(config);

  if (config.seed) {
    say('running db-migrate');
    await runOrThrow('pnpm', ['run', 'db-migrate']);
    say('running db-seed');
    await runOrThrow('pnpm', ['run', 'db-seed']);
  }

  const sql = pgClient(lc, config.upstreamDB, 'rmv2-soak', {max: 8});
  const [fixture] = await sql<{projectID: string; creatorID: string}[]>`
    SELECT u.id AS "creatorID", p.id AS "projectID"
      FROM public."user" u CROSS JOIN public.project p
     ORDER BY u.id, p.id LIMIT 1`;
  if (!fixture) {
    throw new Error(
      'zbugs needs at least one user and project. Re-run with --seed.',
    );
  }

  const phases: PhaseRecord[] = [];
  const oracle: OracleResult[] = [];
  const chaosOutcomes: ChaosOutcome[] = [];
  let baseline: Record<string, unknown> | undefined;
  let quiesceCount = 0;

  const soakLog = new SoakLog();
  soakLog.onTripwire(t => {
    if (t.name !== 'error-log') {
      say(`TRIPWIRE ${t.name} on ${t.node}: ${t.message}`);
    }
  });

  let cluster: SoakCluster | undefined;
  let traffic: TrafficDriver | undefined;
  let sampler: ResourceSampler | undefined;

  const shutdown = async () => {
    sampler?.stop();
    await cluster?.stopAll('SIGTERM').catch(() => undefined);
    await traffic?.close().catch(() => undefined);
    await receiver.close().catch(() => undefined);
  };
  const onSignal = () => {
    say('interrupted; shutting down');
    void shutdown().finally(() => process.exit(130));
  };
  process.on('SIGINT', onSignal);
  process.on('SIGTERM', onSignal);

  try {
    // Phase 0 (slice L9): the A/B control for failure class 6. Without it a
    // replication-path regression is indistinguishable from the workload.
    if (config.baseline) {
      say('phase 0: baseline with the change log off');
      await resetBackup(config);
      const baselineLogsDir = join(config.logsDir, 'baseline');
      mkdirSync(baselineLogsDir, {recursive: true});
      const control = new SoakCluster(
        {
          ...config,
          logsDir: baselineLogsDir,
          changeLog: {
            mode: 'off',
            readPercent: 0,
            coldReadPercent: 0,
            comparePercent: 0,
            retentionMs: config.changeLog.retentionMs,
          },
        },
        soakLog,
      );
      // Tracked so that an interrupt or a failure during the control pass
      // still tears the control cluster down.
      cluster = control;
      const before = throughput(metrics);
      await control.startReplicationManager();
      await control.startViewSyncers();
      const controlTraffic = await TrafficDriver.connect(config.upstreamDB);
      const controlStages: StageResult[] = [];
      for (const phase of workloadPhases(config)) {
        for (const stage of phase.stages) {
          controlStages.push(await controlTraffic.runStage(stage));
        }
      }
      await sleep(config.metricExportIntervalMs * 3);
      const after = throughput(metrics);
      await controlTraffic.deleteResidue();
      await controlTraffic.close();
      await control.stopAll('SIGTERM');
      baseline = {
        stages: stageSummary(controlStages),
        replication: throughputDelta(before, after),
      };
      cluster = undefined;
      say('baseline complete; wiping replicas and the backup for the real run');
      for (const node of control.nodes) {
        await node.deleteReplica();
        await node.deleteChangeLog();
      }
      await resetBackup(config);
    } else {
      await resetBackup(config);
    }

    const soakCluster = new SoakCluster(config, soakLog);
    cluster = soakCluster;
    const mainBefore = throughput(metrics);

    say(
      'starting the replication-manager (phase 1: initial sync + first backup)',
    );
    await soakCluster.startReplicationManager();
    say('starting view-syncers (each does its own litestream restore)');
    await soakCluster.startViewSyncers();

    traffic = await TrafficDriver.connect(config.upstreamDB, {
      runID: `soak-${config.runID}`,
    });
    sampler = new ResourceSampler(
      config,
      sql,
      soakCluster.changeLogFile,
      soakCluster.nodes.map(n => ({node: n.name, replicaFile: n.replicaFile})),
    );
    sampler.start();

    const quiesceAndCompare = async (label: string) => {
      quiesceCount++;
      const result = await runOracle(
        lc,
        sql,
        // Not `replicaHandles()`: a node that is deliberately down has no
        // replica to read, and waiting for it would hang the sentinel.
        soakCluster.nodes
          .filter(n => n.running)
          .map(n => ({node: n.name, replicaFile: n.replicaFile})),
        fixture,
        label,
        `rmv2-soak-quiesce-${config.runID}-${quiesceCount}`,
      );
      oracle.push(result);
      say(
        `oracle "${label}": ${result.ok ? 'ok' : 'DIVERGED'} ` +
          `(quiesce ${result.quiesceMs}ms, ${result.verdict})`,
      );
      return result;
    };

    // Phases 2-5, each followed by a quiesce-and-compare. Evaluating at every
    // quiescent point rather than only at the end is what keeps a later
    // restore from healing -- and hiding -- a divergence.
    for (const phase of workloadPhases(config)) {
      say(`phase ${phase.name}: ${phase.stresses}`);
      sampler.phase = phase.name;
      const phaseStartedMs = Date.now();
      const stages: StageResult[] = [];
      for (const stage of phase.stages) {
        stages.push(await traffic.runStage(stage));
      }
      phases.push({
        name: phase.name,
        startedMs: phaseStartedMs,
        finishedMs: Date.now(),
        stages,
      });
      await quiesceAndCompare(`after phase ${phase.name}`);
    }

    // Phase 6: the mixed phase, with the chaos matrix interleaved. Every
    // action is followed by its own quiesce-and-compare, and no view-syncer
    // is re-restored between an action and its check.
    if (config.chaos.length > 0) {
      say(`phase mixed: chaos ${config.chaos.join(', ')}`);
      sampler.phase = 'mixed';
      const mixed = mixedPhase(config);
      const mixedStartedMs = Date.now();
      const mixedStages: StageResult[] = [];
      const ctx: ChaosContext = {
        config,
        cluster: soakCluster,
        log: soakLog,
        metrics,
        sql,
        traffic,
        sampler,
        note: say,
      };
      // The rollback drills leave the change log in `write` and then `off`,
      // which is not a state the rest of the run can continue from, so they
      // always go last regardless of the order they were requested in.
      const ordered = config.chaos.toSorted(
        (a, b) =>
          Number(ROLLBACK_DRILLS.has(a)) - Number(ROLLBACK_DRILLS.has(b)),
      );
      for (const id of ordered) {
        const action = CHAOS_ACTIONS.find(a => a.id === id);
        if (!action) {
          continue;
        }
        // Give the action a workload to land in, rather than hitting an idle
        // system: the stages cycle underneath the chaos matrix.
        const stage = mixed.stages[mixedStages.length % mixed.stages.length];
        const load = traffic.runStage(stage);
        const outcome = await runChaosAction(action, ctx);
        mixedStages.push(await load);
        chaosOutcomes.push(outcome);
        for (const finding of outcome.findings) {
          say(`FINDING ${finding}`);
        }
        await quiesceAndCompare(`after ${action.id}`);
      }
      phases.push({
        name: mixed.name,
        startedMs: mixedStartedMs,
        finishedMs: Date.now(),
        stages: mixedStages,
      });
    }

    await sleep(config.metricExportIntervalMs * 3);
    const mainAfter = throughput(metrics);
    if (baseline) {
      baseline['soakReplication'] = throughputDelta(mainBefore, mainAfter);
      baseline['soakStages'] = stageSummary(phases.flatMap(p => p.stages));
    }

    await sampler.sample();
    sampler.stop();

    const report = buildReport({
      config,
      startedMs,
      phases,
      oracle,
      chaos: chaosOutcomes,
      log: soakLog,
      metrics,
      resources: sampler.samples,
      baseline,
    });
    await writeReport(config.reportPath, report);
    console.log(summarize(report));
    say(`report written to ${config.reportPath}`);
    if (!report.pass) {
      process.exitCode = 1;
    }
  } finally {
    process.off('SIGINT', onSignal);
    process.off('SIGTERM', onSignal);
    sampler?.stop();
    if (!config.keep) {
      await traffic?.deleteResidue().catch(() => undefined);
      // A run interrupted mid-quiesce can leave its sentinel row behind, and
      // a stray row would show up as a diff in the *next* run.
      await sql`
        DELETE FROM issue WHERE id LIKE ${`rmv2-soak-quiesce-${config.runID}-%`}
      `.catch(() => undefined);
    }
    await cluster?.stopAll('SIGTERM').catch(() => undefined);
    await traffic?.close().catch(() => undefined);
    await sql.end().catch(() => undefined);
    await receiver.close().catch(() => undefined);
    if (!config.keep) {
      // The replicas and the change log; the logs and the report stay.
      for (const node of cluster?.nodes ?? []) {
        await node.deleteReplica().catch(() => undefined);
        await node.deleteChangeLog().catch(() => undefined);
      }
    }
  }
}

if (argv[1]?.endsWith('rmv2-soak.ts')) {
  main().catch(error => {
    console.error(
      error instanceof Error ? (error.stack ?? error.message) : error,
    );
    process.exitCode = 1;
  });
}
