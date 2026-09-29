/**
 * Replays client groups from a CVR export against a Zero deployment while
 * optionally running a column backfill and synthetic app writes against its
 * upstream database, and records what clients experience.
 *
 * See `apps/zero-throughput/src/replay/README.md`.
 */
import '../../../../packages/shared/src/dotenv.ts';

import {mkdir, readFile, writeFile} from 'node:fs/promises';
import {dirname} from 'node:path';
import {monitorEventLoopDelay, performance} from 'node:perf_hooks';
import {parseOptions} from '../../../../packages/shared/src/options.ts';
import * as v from '../../../../packages/shared/src/valita.ts';
import {
  CloudZeroMetricsPoller,
  type CloudZeroMetricsSummary,
} from '../cloudzero-metrics.ts';
import {appPath, DEFAULT_PG_URL} from '../config.ts';
import {connectBenchmarkDB} from '../db.ts';
import {startPostgres} from '../processes.ts';
import {formatDuration, log, warn} from '../util.ts';
import {
  AppWriteDriver,
  loadAppWritesSpec,
  type AppWritesSpec,
} from './app-writes.ts';
import {
  parseArgRewrites,
  rewriteArgs,
  type ArgRewrite,
} from './arg-rewrites.ts';
import {
  BackfillDriver,
  HeldRowTracker,
  loadBackfillSpec,
  parseSchedule,
  scheduleSeconds,
  type BackfillSpec,
  type ScheduleSegment,
} from './backfill.ts';
import {
  captureProfiles,
  profileClockMicros,
  summarizeResets,
} from './diagnostics.ts';
import {startLocalTarget, type LocalTarget} from './local-target.ts';
import {HydrationProbe} from './probe.ts';
import {Recorder, type Distribution, type Phase} from './recorder.ts';
import {loadReplicaSeedPlan} from './replica-seed.ts';
import {SessionDriver, seededRandom} from './sessions.ts';
import {loadWorkload, type WorkloadGroup} from './workload.ts';

const options = {
  workload: v.string(),
  target: v.literalUnion('remote', 'local').default('remote'),
  /** zero-cache URL; a comma-separated list spreads devices across them. */
  cacheURL: v.string().optional(),
  /** Tokens are `<authSecret>:<userID>`. */
  authSecret: v.string().optional(),
  /** Upstream database, for the backfill and app writes. */
  pgURL: v.string().optional(),
  protocolVersion: v.number().default(52),

  /** Concurrent sessions. */
  groups: v.number().default(110),
  /** Devices sessions are drawn from; defaults to twice `groups`. */
  devices: v.number().optional(),
  rampSeconds: v.number().default(120),
  /** Measured time after the ramp; defaults to the backfill schedule's length. */
  durationSeconds: v.number().optional(),
  /** Defaults to the workload's estimate. */
  meanSessionSeconds: v.number().optional(),
  screenQueriesPerMinute: v.number().default(1),
  meanScreenDwellSeconds: v.number().default(60),
  maxScreenQueries: v.number().default(5),
  pingIntervalMs: v.number().default(5_000),
  maxHeaderLength: v.number().default(8 * 1024),
  reconnectDelayMs: v.number().default(5_000),
  /** Reuse a prefix across runs to keep the server's CVRs warm. */
  clientGroupPrefix: v.string().optional(),
  seed: v.number().default(1),

  backfillSpec: v.string().optional(),
  /** Subset and order of the spec's tables. */
  backfillTables: v.array(v.string()).optional(),
  backfillDirection: v.literalUnion('forward', 'reverse').default('forward'),
  backfillPageSize: v.number().default(100),
  /** `rowsPerSecond:seconds,...` starting after the ramp, e.g. `0:600,24:1200`. */
  backfillSchedule: v.string().optional(),
  backfillSuppressTriggers: v.boolean().default(true),
  backfillSkipInvalidRows: v.boolean().default(true),
  /** Required to write to a database that is not on this machine. */
  backfillConfirm: v.boolean().default(false),

  appWritesSpec: v.string().optional(),
  appWritesPerSecond: v.number().default(0),
  appWritesMaxInFlight: v.number().default(8),

  /**
   * `name.key=now|minute|hour|day`: replace a recorded clock-derived argument
   * with the current time (floored to the unit) whenever a session registers
   * the query. Without it the snapshot's values are replayed as-is.
   */
  argRewrite: v.array(v.string()).default([]),

  /**
   * Seconds into the run (the ramp included) at which to capture CPU
   * profiles of every zero-cache process through `/profz`.
   */
  profileAt: v.array(v.string()).default([]),
  profileSeconds: v.number().default(20),
  /** For `/profz` on a deployment; not needed for the local target. */
  adminPassword: v.string().optional(),

  /**
   * Every N seconds after the ramp, a fresh client group for one fixed
   * workload group connects and times hydrating all its session queries.
   * 0 turns the probe off.
   */
  probeEverySeconds: v.number().default(0),
  /** The workload group to probe with: an index or a source client group ID. */
  probeGroup: v.string().default('0'),
  probeTimeoutSeconds: v.number().default(180),

  /** Free text copied into the result, e.g. known fidelity gaps. */
  note: v.array(v.string()).default([]),

  bucketSeconds: v.number().default(60),
  progressSeconds: v.number().default(15),
  output: v.string().default('results/replay/latest.json'),
  logsDir: v.string().default('results/replay/logs'),

  cloudzeroApiKey: v.string().optional(),
  cloudzeroMetricsUrl: v
    .string()
    .default('https://console.cloudzero.fun/api/v1/metrics'),
  cloudzeroStackId: v.string().optional(),

  local: {
    /** Postgres server; the harness's Docker Postgres by default. */
    pgURL: v.string().default(DEFAULT_PG_URL),
    startPostgres: v.boolean().default(true),
    database: v.string().default('replay'),
    reset: v.boolean().default(false),
    seedSQL: v.array(v.string()).default([]),
    /** A Zero replica file to copy rows from before the seed SQL runs. */
    seedReplica: v.string().optional(),
    /** JSON plan for the copy: per-table `skip` or a SQLite `where`. */
    seedReplicaPlan: v.string().optional(),
    /** zero-cache's first start copies the whole database. */
    readyTimeoutMinutes: v.number().default(180),
    /**
     * Profile every zero-cache process for its whole life (`--cpu-prof`) into
     * `<output>.profiles/`. The result's `profileClockStartMicros` lines the
     * run's phases up with the profiles' sample times.
     */
    cpuProf: v.boolean().default(false),
    queryServerCommand: v.string().optional(),
    queryServerPort: v.number().default(3_100),
    queryPath: v.string().default('/api/query'),
    zeroPort: v.number().default(4_858),
    numSyncWorkers: v.number().default(4),
    replicaFile: v.string().default('/tmp/zero-replay-replica.db'),
  },
};

type RunConfig = ReturnType<typeof parseConfig>;

function parseConfig() {
  const argv = process.argv.slice(2);
  return parseOptions(options, {
    argv: argv[0] === '--' ? argv.slice(1) : argv,
    envNamePrefix: 'REPLAY_',
  });
}

async function main(): Promise<void> {
  const config = parseConfig();
  const runID = new Date().toISOString().replace(/[:.]/g, '-');
  const workload = await loadWorkload(config.workload);
  const backfillSpec =
    config.backfillSpec === undefined
      ? undefined
      : await loadBackfillSpec(config.backfillSpec);
  const schedule =
    config.backfillSchedule === undefined
      ? undefined
      : parseSchedule(config.backfillSchedule);
  if ((backfillSpec === undefined) !== (schedule === undefined)) {
    throw new Error('--backfill-spec and --backfill-schedule go together');
  }
  // Parsed before anything starts, so a typo fails fast.
  const argRewrites = parseArgRewrites(config.argRewrite);
  const appWrites =
    config.appWritesSpec === undefined || config.appWritesPerSecond <= 0
      ? undefined
      : await loadAppWritesSpec(config.appWritesSpec);

  const stops: (() => Promise<void>)[] = [];
  let interrupted = false;
  const onSignal = () => {
    warn('Interrupted; finishing the run and writing results...');
    interrupted = true;
  };
  process.once('SIGINT', onSignal);
  process.once('SIGTERM', onSignal);

  try {
    const {cacheURLs, pgURL, authSecret, zeroCacheLog} = await resolveTarget(
      config,
      runID,
      workload.clientSchema,
      [...new Set(workload.groups.map(g => g.userID))],
      stops,
    );
    await execute({
      config,
      runID,
      workload,
      backfillSpec,
      schedule,
      appWrites,
      argRewrites,
      cacheURLs,
      pgURL,
      authSecret,
      zeroCacheLog,
      stops,
      isInterrupted: () => interrupted,
    });
  } finally {
    for (const stop of stops.reverse()) {
      await stop().catch(e => warn(`cleanup failed: ${String(e)}`));
    }
  }
}

async function resolveTarget(
  config: RunConfig,
  runID: string,
  clientSchema: Awaited<ReturnType<typeof loadWorkload>>['clientSchema'],
  userIDs: readonly string[],
  stops: (() => Promise<void>)[],
): Promise<{
  cacheURLs: string[];
  pgURL: string | undefined;
  authSecret: string;
  zeroCacheLog?: string | undefined;
}> {
  if (config.target === 'remote') {
    if (config.cacheURL === undefined || config.authSecret === undefined) {
      throw new Error('--target remote needs --cache-url and --auth-secret');
    }
    return {
      cacheURLs: config.cacheURL.split(',').map(s => s.trim()),
      pgURL: config.pgURL,
      authSecret: config.authSecret,
    };
  }
  const local = config.local;
  if (local.queryServerCommand === undefined) {
    throw new Error('--target local needs --local-query-server-command');
  }
  if (local.startPostgres && local.pgURL === DEFAULT_PG_URL) {
    log('Starting PostgreSQL (docker compose)...');
    await startPostgres();
  }
  const authSecret = config.authSecret ?? 'local-secret';
  const target: LocalTarget = await startLocalTarget({
    pgServerURL: local.pgURL,
    database: local.database,
    reset: local.reset,
    clientSchema,
    userIDs,
    seedReplica:
      local.seedReplica === undefined
        ? undefined
        : {
            file: local.seedReplica,
            plan: await loadReplicaSeedPlan(local.seedReplicaPlan),
          },
    seedSQLFiles: local.seedSQL,
    readyTimeoutMs: local.readyTimeoutMinutes * 60_000,
    queryServerCommand: local.queryServerCommand,
    queryServerPort: local.queryServerPort,
    queryPath: local.queryPath,
    authSecret,
    zeroPort: local.zeroPort,
    numSyncWorkers: local.numSyncWorkers,
    replicaFile: local.replicaFile,
    cpuProfileDir: local.cpuProf
      ? appPath(config.output).replace(JSON_EXTENSION, '') + '.profiles'
      : undefined,
    logsDir: appPath(config.logsDir),
    runID,
    log,
  });
  stops.push(() => target.stop());
  return {
    cacheURLs: [target.cacheURL],
    pgURL: target.pgURL,
    authSecret,
    zeroCacheLog: target.zeroCacheLog,
  };
}

async function execute(args: {
  readonly config: RunConfig;
  readonly runID: string;
  readonly workload: Awaited<ReturnType<typeof loadWorkload>>;
  readonly backfillSpec: BackfillSpec | undefined;
  readonly schedule: readonly ScheduleSegment[] | undefined;
  readonly appWrites: AppWritesSpec | undefined;
  readonly argRewrites: readonly ArgRewrite[];
  readonly cacheURLs: readonly string[];
  readonly pgURL: string | undefined;
  readonly authSecret: string;
  readonly zeroCacheLog: string | undefined;
  readonly stops: (() => Promise<void>)[];
  readonly isInterrupted: () => boolean;
}): Promise<void> {
  const {config, runID, workload, backfillSpec, schedule, appWrites} = args;
  const random = seededRandom(config.seed);
  const now = () => performance.now();
  const recorder = new Recorder(config.bucketSeconds * 1000, now);
  const startedAtWallMs = Date.now();
  // Where run time zero falls on the clock of --cpu-prof samples.
  const profileClockStartMicros = await profileClockMicros();
  const rampMs = config.rampSeconds * 1000;
  const measuredMs =
    (config.durationSeconds ??
      (schedule === undefined ? 600 : scheduleSeconds(schedule))) * 1000;
  const phases = runPhases(rampMs, measuredMs, schedule);

  const needsPG = backfillSpec !== undefined || appWrites !== undefined;
  if (needsPG && args.pgURL === undefined) {
    throw new Error('Backfill and app writes need --pg-url');
  }
  const sql =
    needsPG && args.pgURL !== undefined
      ? connectBenchmarkDB(args.pgURL, config.appWritesMaxInFlight + 2)
      : undefined;
  if (sql !== undefined) {
    args.stops.push(() => sql.end({timeout: 5}));
  }
  if (backfillSpec !== undefined && args.pgURL !== undefined) {
    const host = new URL(args.pgURL).hostname;
    if (
      !['localhost', '127.0.0.1', '::1'].includes(host) &&
      !config.backfillConfirm
    ) {
      throw new Error(
        `The backfill would write to ${host}; pass --backfill-confirm to allow it`,
      );
    }
  }

  const tracker =
    backfillSpec === undefined
      ? undefined
      : new HeldRowTracker({
          spec: backfillSpec,
          to:
            config.backfillDirection === 'forward'
              ? backfillSpec.to
              : backfillSpec.from,
          now,
          ttlMs: 15 * 60_000,
          onDelivered: ms => recorder.heldRowDelivered(ms),
        });
  const backfill =
    backfillSpec === undefined || schedule === undefined || sql === undefined
      ? undefined
      : new BackfillDriver({
          sql,
          spec: backfillSpec,
          tables: selectTables(backfillSpec, config.backfillTables),
          direction: config.backfillDirection,
          pageSize: config.backfillPageSize,
          schedule,
          suppressTriggers: config.backfillSuppressTriggers,
          skipInvalidRows: config.backfillSkipInvalidRows,
          recorder,
          tracker: tracker as HeldRowTracker,
          log,
        });
  const remainingBefore = await backfill?.remaining();
  if (remainingBefore !== undefined) {
    for (const {table, rows} of remainingBefore) {
      log(`backfill: ${table} holds ${rows ?? '?'} rows to rename`);
    }
  }

  const devices = config.devices ?? Math.min(2 * config.groups, 100_000);
  const {argRewrites} = args;
  const sessions = new SessionDriver({
    groups: workload.groups,
    screenQueries: workload.screenQueries,
    clientSchema: workload.clientSchema,
    cacheURLs: args.cacheURLs,
    protocolVersion: config.protocolVersion,
    authForUser: userID => `${args.authSecret}:${userID}`,
    devices,
    clientGroupPrefix: config.clientGroupPrefix ?? `replay-${runID}`,
    meanSessionMs:
      (config.meanSessionSeconds ??
        (workload.stats.meanSessionSeconds || 1_129)) * 1000,
    screenQueriesPerMinute: config.screenQueriesPerMinute,
    meanScreenDwellMs: config.meanScreenDwellSeconds * 1000,
    maxScreenQueriesPerSession: config.maxScreenQueries,
    reconnectDelayMs: config.reconnectDelayMs,
    pingIntervalMs: config.pingIntervalMs,
    maxHeaderLength: config.maxHeaderLength,
    random,
    prepareQuery: q => rewriteArgs(q, argRewrites, Date.now()),
    recorder,
    onRow: (table, row, caughtUpAtMs) =>
      tracker?.observe(table, row, caughtUpAtMs),
    log: warn,
  });

  const cloudzero = startCloudZero(config);
  const loopDelay = monitorEventLoopDelay({resolution: 10});
  loopDelay.enable();
  const gaugeTimer = setInterval(() => {
    recorder.gauges({
      activeSessions: sessions.activeSessions,
      pendingQueries: sessions.pendingQueries,
      eventLoopDelayP99Ms: Math.round(loopDelay.percentile(99) / 1e6),
      cloudzero: flattenCloudZero(cloudzero?.latest ?? null),
    });
    loopDelay.reset();
    tracker?.expire();
  }, 5_000);
  const progressTimer = setInterval(
    () => log(progressLine(recorder, sessions, backfill, phases)),
    config.progressSeconds * 1000,
  );

  log(
    `replay ${runID}: ${config.groups} sessions over ${devices} devices ` +
      `(${workload.groups.length} workload groups) -> ${args.cacheURLs.join(', ')}; ` +
      `ramp ${formatDuration(rampMs)}, then ${formatDuration(measuredMs)}`,
  );
  sessions.setConcurrency(config.groups, rampMs);
  const profilesDir =
    appPath(config.output).replace(JSON_EXTENSION, '') + '.profiles';
  const profileFiles: string[] = [];
  const profiling = config.profileAt
    .flatMap(s => s.split(','))
    .map(Number)
    .filter(s => Number.isFinite(s))
    .map(
      atSeconds =>
        new Promise<void>(resolve => {
          setTimeout(
            () => {
              if (args.isInterrupted()) {
                resolve();
                return;
              }
              log(
                `profiling every zero-cache process for ${config.profileSeconds}s (t=${atSeconds}s)`,
              );
              captureProfiles({
                cacheURL: args.cacheURLs[0],
                seconds: config.profileSeconds,
                adminPassword: config.adminPassword,
                outDir: profilesDir,
                label: `t${atSeconds}s`,
              })
                .then(files => {
                  profileFiles.push(...files);
                  log(`saved ${files.length} profiles to ${profilesDir}`);
                })
                .catch(e => {
                  recorder.serverError('profz', String(e));
                  warn(`profiling at t=${atSeconds}s failed: ${String(e)}`);
                })
                .finally(resolve);
            },
            Math.max(0, atSeconds * 1000 - recorder.elapsedMs()),
          );
        }),
    );
  let backfillDone: Promise<void> | undefined;
  let writesDone: Promise<void> | undefined;
  const appWriter =
    appWrites === undefined || sql === undefined
      ? undefined
      : new AppWriteDriver({
          sql,
          spec: appWrites,
          writesPerSecond: config.appWritesPerSecond,
          maxInFlight: config.appWritesMaxInFlight,
          activeUserIDs: () => sessions.activeUserIDs(),
          random,
          recorder,
        });

  const probe =
    config.probeEverySeconds > 0
      ? new HydrationProbe({
          group: pickGroup(workload.groups, config.probeGroup),
          clientSchema: workload.clientSchema,
          cacheURL: args.cacheURLs[0],
          protocolVersion: config.protocolVersion,
          auth: `${args.authSecret}:${pickGroup(workload.groups, config.probeGroup).userID}`,
          prepareQuery: q => rewriteArgs(q, argRewrites, Date.now()),
          intervalMs: config.probeEverySeconds * 1000,
          timeoutMs: config.probeTimeoutSeconds * 1000,
          clientGroupPrefix: config.clientGroupPrefix ?? `replay-${runID}`,
          pingIntervalMs: config.pingIntervalMs,
          maxHeaderLength: config.maxHeaderLength,
          recorder,
        })
      : undefined;
  let probing = false;

  const endMs = rampMs + measuredMs;
  while (recorder.elapsedMs() < endMs && !args.isInterrupted()) {
    if (recorder.elapsedMs() >= rampMs) {
      backfillDone ??= backfill?.run() ?? Promise.resolve();
      writesDone ??= appWriter?.run() ?? Promise.resolve();
      if (probe !== undefined && !probing) {
        probing = true;
        probe.start();
      }
    }
    await new Promise(resolve => setTimeout(resolve, 200));
  }

  backfill?.stop();
  appWriter?.stop();
  await probe?.stop();
  await Promise.all(profiling);
  await backfillDone;
  await writesDone;
  const endedAtMs = recorder.elapsedMs();
  clearInterval(progressTimer);
  clearInterval(gaugeTimer);
  loopDelay.disable();
  await sessions.stop();
  await cloudzero?.stop();
  const remainingAfter = await backfill?.remaining();

  const closedPhases = phases.map(p => ({
    ...p,
    endMs: Math.min(p.endMs, endedAtMs),
  }));
  const result = {
    runID,
    profileClockStartMicros,
    notes: config.note,
    interrupted: args.isInterrupted(),
    config: {
      ...config,
      authSecret: config.authSecret ? '<redacted>' : undefined,
      pgURL: redactPassword(config.pgURL),
      cloudzeroApiKey: config.cloudzeroApiKey ? '<redacted>' : undefined,
      adminPassword: config.adminPassword ? '<redacted>' : undefined,
      local: {...config.local, pgURL: redactPassword(config.local.pgURL)},
    },
    target: {cacheURLs: args.cacheURLs},
    workload: {
      path: config.workload,
      groups: workload.groups.length,
      screenQueries: workload.screenQueries.length,
      stats: {...workload.stats, names: undefined},
    },
    backfill:
      backfill === undefined
        ? undefined
        : {
            rowsWritten: backfill.rowsWritten,
            remainingBefore,
            remainingAfter,
          },
    phases: recorder.phaseSummaries(closedPhases),
    hydrationByName: recorder.hydrationByName(rampMs, endedAtMs),
    errors: Object.fromEntries(recorder.errors()),
    resets:
      args.zeroCacheLog === undefined
        ? undefined
        : summarizeResets(
            await readFile(args.zeroCacheLog, 'utf8'),
            startedAtWallMs,
            closedPhases,
          ),
    profiles: profileFiles,
    timeline: recorder.timeline(closedPhases),
  };
  const output = appPath(config.output);
  await mkdir(dirname(output), {recursive: true});
  await writeFile(output, JSON.stringify(result, null, 2));
  await writeFile(
    output.replace(JSON_EXTENSION, '') + '.timeline.csv',
    timelineCSV(result.timeline),
  );
  log('');
  for (const note of config.note) {
    log(`note: ${note}`);
  }
  log(summaryTable(result.phases));
  if (result.resets !== undefined) {
    log(
      `pipeline resets: ${result.resets.total} ` +
        JSON.stringify({
          byPhase: result.resets.byPhase,
          byReason: result.resets.byReason,
        }),
    );
  }
  const errors = [...recorder.errors()].toSorted((a, b) => b[1] - a[1]);
  if (errors.length > 0) {
    log('errors:');
    for (const [message, count] of errors.slice(0, 15)) {
      log(`  ${String(count).padStart(6)}  ${message}`);
    }
  }
  log(`results: ${output}`);
}

const JSON_EXTENSION = /\.json$/;

function redactPassword(url: string | undefined): string | undefined {
  if (url === undefined) {
    return undefined;
  }
  try {
    const parsed = new URL(url);
    if (parsed.password !== '') {
      parsed.password = 'redacted';
    }
    return parsed.toString();
  } catch {
    return '<unparseable>';
  }
}

function runPhases(
  rampMs: number,
  measuredMs: number,
  schedule: readonly ScheduleSegment[] | undefined,
): Phase[] {
  const phases: Phase[] = [{label: 'ramp', startMs: 0, endMs: rampMs}];
  const endMs = rampMs + measuredMs;
  if (schedule === undefined) {
    phases.push({label: 'steady', startMs: rampMs, endMs});
    return phases;
  }
  let t = rampMs;
  schedule.forEach((segment, i) => {
    const end = Math.min(endMs, t + segment.seconds * 1000);
    if (end > t) {
      phases.push({
        label: `${i + 1}: ${segment.rowsPerSecond === 0 ? 'no backfill' : `backfill ${segment.rowsPerSecond}/s`}`,
        startMs: t,
        endMs: end,
      });
    }
    t = end;
  });
  if (t < endMs) {
    phases.push({label: 'after schedule', startMs: t, endMs});
  }
  return phases;
}

function pickGroup(
  groups: readonly WorkloadGroup[],
  selector: string,
): WorkloadGroup {
  const byID = groups.find(g => g.sourceClientGroupID === selector);
  const group = byID ?? groups[Number(selector)];
  if (group === undefined) {
    throw new Error(`--probe-group ${selector} matches no workload group`);
  }
  return group;
}

function selectTables(
  spec: BackfillSpec,
  names: readonly string[] | undefined,
): BackfillSpec['tables'] {
  if (names === undefined || names.length === 0) {
    return spec.tables;
  }
  return names
    .flatMap(n => n.split(','))
    .map(name => {
      const table = spec.tables.find(
        t => t.table === name || t.table.endsWith(`.${name}`),
      );
      if (table === undefined) {
        throw new Error(
          `--backfill-tables: ${name} is not in the spec (${spec.tables.map(t => t.table).join(', ')})`,
        );
      }
      return table;
    });
}

function startCloudZero(config: RunConfig): CloudZeroMetricsPoller | undefined {
  const apiKey = config.cloudzeroApiKey ?? process.env.CLOUDZERO_API_KEY;
  if (apiKey === undefined || config.cloudzeroStackId === undefined) {
    return undefined;
  }
  const poller = new CloudZeroMetricsPoller({
    metricsUrl: config.cloudzeroMetricsUrl,
    apiKey,
    stackId: config.cloudzeroStackId,
  });
  poller.start();
  return poller;
}

function flattenCloudZero(
  s: CloudZeroMetricsSummary | null,
): Record<string, number> | undefined {
  if (s === null) {
    return undefined;
  }
  const out: Record<string, number> = {
    vsPods: s.vsSummary.podCount,
    vsTotalCpuCores: s.vsSummary.totalCpuCores,
    vsMaxCpuCores: s.vsSummary.maxCpuCores,
    vsMaxMemoryMB: s.vsSummary.maxMemoryMB,
    vsPipelines: s.vsSummary.totalPipelines,
  };
  if (s.rmPod !== undefined) {
    out.rmCpuCores = s.rmPod.cpuCores;
  }
  if (s.replicationLagMs !== null) {
    out.replicationLagP95Ms = s.replicationLagMs.p95 ?? s.replicationLagMs.max;
  }
  if (s.viewSyncerLagMs !== null) {
    out.viewSyncerLagP95Ms = s.viewSyncerLagMs.p95 ?? s.viewSyncerLagMs.max;
  }
  for (const pod of s.vsPods) {
    out[`cpu:${pod.pod}`] = pod.cpuCores;
  }
  return out;
}

function progressLine(
  recorder: Recorder,
  sessions: SessionDriver,
  backfill: BackfillDriver | undefined,
  phases: readonly Phase[],
): string {
  const t = recorder.elapsedMs();
  const phase = phases.find(p => t >= p.startMs && t < p.endMs)?.label ?? '';
  const h = recorder.recentHydration(60_000);
  const held = recorder.recentHeldRowDelivery(60_000);
  const ping = recorder.recentPing(60_000);
  const probe = recorder.recentProbe(60_000);
  const parts = [
    `+${formatDuration(Math.round(t))}`,
    `[${phase}]`,
    `sessions=${sessions.activeSessions}`,
    `pending=${sessions.pendingQueries}`,
    `hydrated/1m=${h.count} p50=${ms(h.p50)} p95=${ms(h.p95)} max=${ms(h.max)}`,
    `ping p95=${ms(ping.p95)}`,
  ];
  if (probe.count > 0) {
    parts.push(
      `probe/1m=${probe.count} p50=${ms(probe.p50)} max=${ms(probe.max)}`,
    );
  }
  if (backfill !== undefined) {
    parts.push(
      `backfill total=${backfill.rowsWritten}`,
      `held/1m=${held.count} p95=${ms(held.p95)}`,
    );
  }
  const errors = [...recorder.errors().values()].reduce((a, b) => a + b, 0);
  if (errors > 0) {
    parts.push(`errors=${errors}`);
  }
  return parts.join(' ');
}

function ms(value: number): string {
  return value >= 1000
    ? `${(value / 1000).toFixed(1)}s`
    : `${Math.round(value)}ms`;
}

function summaryTable(phases: ReturnType<Recorder['phaseSummaries']>): string {
  const header = [
    'phase',
    'backfill/s',
    'hydrations',
    'p50',
    'p95',
    'p99',
    'tail p95',
    'p95 slope/min',
    'caught up p95',
    'held p95',
    'probe p50',
    'probe max',
    'ping p95',
    'errors',
  ];
  const d = (x: Distribution, key: 'p50' | 'p95' | 'p99') =>
    x.count === 0 ? '-' : ms(x[key]);
  const rows = phases.map(p => [
    p.label,
    String(p.backfillRowsPerSecond),
    String(p.hydration.count),
    d(p.hydration, 'p50'),
    d(p.hydration, 'p95'),
    d(p.hydration, 'p99'),
    d(p.hydrationTail, 'p95'),
    ms(p.hydrationP95SlopeMsPerMin),
    d(p.firstPoke, 'p95'),
    d(p.heldRowDelivery, 'p95'),
    d(p.probeHydration, 'p50'),
    p.probeHydration.count === 0 ? '-' : ms(p.probeHydration.max),
    d(p.pingRtt, 'p95'),
    String(p.unexpectedCloses + p.serverErrors + p.queryErrors),
  ]);
  const widths = header.map((h, i) =>
    Math.max(h.length, ...rows.map(r => r[i].length)),
  );
  const line = (cells: readonly string[]) =>
    cells.map((c, i) => c.padEnd(widths[i])).join('  ');
  return [line(header), ...rows.map(line)].join('\n');
}

function timelineCSV(timeline: ReturnType<Recorder['timeline']>): string {
  const columns = [
    'startS',
    'phase',
    'activeSessions',
    'pendingQueries',
    'hydrations',
    'hydrationP50',
    'hydrationP95',
    'hydrationP99',
    'hydrationMax',
    'sessionHydrationP95',
    'screenHydrationP95',
    'firstPokeP95',
    'heldRowDeliveries',
    'heldRowP95',
    'probeCount',
    'probeP50',
    'probeMax',
    'pingP95',
    'backfillRowsWritten',
    'backfillPageP95',
    'appWrites',
    'pokes',
    'pokeRows',
    'sessionsStarted',
    'unexpectedCloses',
    'serverErrors',
    'queryErrors',
    'eventLoopDelayP99Ms',
    'vsTotalCpuCores',
    'vsMaxCpuCores',
    'rmCpuCores',
    'replicationLagP95Ms',
  ];
  const rows = timeline.map(b =>
    [
      b.startS,
      b.phase ?? '',
      b.activeSessions ?? '',
      b.pendingQueries ?? '',
      b.hydration.count,
      b.hydration.p50,
      b.hydration.p95,
      b.hydration.p99,
      b.hydration.max,
      b.sessionHydration.p95,
      b.screenHydration.p95,
      b.firstPoke.p95,
      b.heldRowDelivery.count,
      b.heldRowDelivery.p95,
      b.probeHydration.count,
      b.probeHydration.p50,
      b.probeHydration.max,
      b.pingRtt.p95,
      b.backfillRowsWritten,
      b.backfillPage.p95,
      b.appWrites,
      b.pokes,
      b.pokeRows,
      b.sessionsStarted,
      b.unexpectedCloses,
      b.serverErrors,
      b.queryErrors,
      b.eventLoopDelayP99Ms ?? '',
      b.cloudzero?.vsTotalCpuCores ?? '',
      b.cloudzero?.vsMaxCpuCores ?? '',
      b.cloudzero?.rmCpuCores ?? '',
      b.cloudzero?.replicationLagP95Ms ?? '',
    ]
      .map(v => (typeof v === 'string' && v.includes(',') ? `"${v}"` : v))
      .join(','),
  );
  return [columns.join(','), ...rows].join('\n') + '\n';
}

try {
  await main();
  process.exit(0);
} catch (e) {
  warn(e instanceof Error ? (e.stack ?? e.message) : String(e));
  process.exit(1);
}
