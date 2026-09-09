import {appendFile, mkdir, writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import type {
  BenchmarkModel,
  BenchmarkProfile,
  BenchmarkTopology,
} from './config.ts';
import {appPath} from './config.ts';
import {startPostgres, stopPostgres} from './processes.ts';
import {
  attemptFromResult,
  type BaseSweepConfig,
  benchmarkCommand,
  DEFAULT_MODELS,
  DEFAULT_PROFILES,
  DEFAULT_ROWS_PER_QUERY,
  DEFAULT_SYNC_WORKERS,
  DEFAULT_USERS,
  fileExists,
  formatDuration,
  parseModels,
  parseNonNegativeInteger,
  parseOption,
  parsePositiveInteger,
  parsePositiveIntegerList,
  parseProfiles,
  pointID,
  pointLabel,
  readBenchmarkResult,
  readBooleanOption,
  readOptionValue,
  runCommandToLog,
  stderr,
  stdout,
  type SweepAttempt,
  type SweepPoint,
  sweepPoints,
} from './sweep-common.ts';

export type LinearSweepConfig = BaseSweepConfig & {
  readonly writeRates: readonly number[];
  readonly viewSyncers?: readonly number[] | undefined;
};

export async function runLinearSweep(
  config: LinearSweepConfig,
): Promise<readonly SweepAttempt[]> {
  const outputDir = appPath(config.outputDir);
  const runsDir = join(outputDir, 'runs');
  const childLogsDir = join(outputDir, 'child-logs');
  const attemptsPath = join(outputDir, 'attempts.jsonl');
  const points = sweepPoints(config);
  const writeRates =
    config.writeRates.length > 0
      ? config.writeRates
      : [100, 250, 500, 1000, 2000];
  const vsCounts =
    config.viewSyncers && config.viewSyncers.length > 0
      ? config.viewSyncers
      : config.numViewSyncers !== undefined
        ? [config.numViewSyncers]
        : [1];

  await mkdir(runsDir, {recursive: true});
  await mkdir(childLogsDir, {recursive: true});
  await writeFile(attemptsPath, '');

  let postgresStarted = false;
  const onSigint = () => {
    stderr('\nInterrupted. Cleaning up sweep PostgreSQL if needed...\n');
    void (async () => {
      if (postgresStarted) {
        await stopPostgres();
      }
      process.exit(130);
    })();
  };
  process.once('SIGINT', onSigint);

  const attempts: SweepAttempt[] = [];

  try {
    stdout(`zero-throughput linear sweep ${config.runID}\n`);
    stdout(`output: ${outputDir}\n`);

    if (config.pgURL === undefined) {
      stdout('Starting sweep PostgreSQL...\n');
      await startPostgres();
      postgresStarted = true;
    }

    for (const point of points) {
      for (const vs of vsCounts) {
        for (const rate of writeRates) {
          const vsTag = vs > 1 || vsCounts.length > 1 ? `[${vs} VS] ` : '';
          stdout(
            `\n--> Running ${pointLabel(point)} ${vsTag}@ ${rate} writes/s...\n`,
          );
          const attempt = await runAttempt({
            config,
            point,
            writeRate: rate,
            numViewSyncers: vs,
            runsDir,
            childLogsDir,
          });
          attempts.push(attempt);
          await appendFile(attemptsPath, `${JSON.stringify(attempt)}\n`);

          const s = attempt.summary;
          const status = attempt.status === 'pass' ? 'PASS' : 'FAIL';
          stdout(
            `    Result: ${status} | achieved=${s?.achievedWriteRate.toFixed(1) ?? '0'} w/s | p95Lag=${s?.p95ClientVisibleLagMs.toFixed(1) ?? 'N/A'}ms\n`,
          );

          if (attempt.status === 'error' && !config.continueOnError) {
            throw new Error(
              `Benchmark attempt failed for ${pointLabel(point)} at ${rate} writes/s. See ${attempt.logPath}`,
            );
          }
        }
      }
    }
  } finally {
    process.off('SIGINT', onSigint);
    if (postgresStarted) {
      stdout('\nStopping sweep PostgreSQL...\n');
      await stopPostgres();
    }
  }

  printLinearSweepSummaryTable(attempts);
  return attempts;
}

export function printLinearSweepSummaryTable(
  attempts: readonly SweepAttempt[],
): void {
  const showVS = attempts.some(a => (a.numViewSyncers ?? 1) > 1);

  stdout(
    '\n========================================================================================================================\n',
  );
  stdout(
    '                                LINEAR THROUGHPUT SWEEP RESULTS                                                         \n',
  );
  stdout(
    '========================================================================================================================\n',
  );
  if (showVS) {
    stdout(
      ' Profile     | Model     | Users | VS | Rate  | Actual | E2E Lag (p50/p95) | IVM Adv (p50/p95) | RM Lag (p50/p95) | Status   \n',
    );
    stdout(
      '-------------+-----------+-------+----+-------+--------+-------------------+-------------------+------------------+----------\n',
    );
  } else {
    stdout(
      ' Profile     | Model     | Users | Rate  | Actual | E2E Lag (p50/p95) | IVM Adv (p50/p95) | RM Lag (p50/p95) | Status   \n',
    );
    stdout(
      '-------------+-----------+-------+-------+--------+-------------------+-------------------+------------------+----------\n',
    );
  }

  for (const a of attempts) {
    const s = a.summary;
    const p = a.point;
    const e2eStr = s?.e2eServingLagMs
      ? `${s.e2eServingLagMs.p50.toFixed(1)} / ${s.e2eServingLagMs.p95.toFixed(1)}ms`
      : s?.p95ClientVisibleLagMs !== undefined
        ? `${s.p50ClientVisibleLagMs.toFixed(1)} / ${s.p95ClientVisibleLagMs.toFixed(1)}ms`
        : 'N/A';
    const ivmStr = s?.advancementLatencyMs
      ? `${s.advancementLatencyMs.p50.toFixed(1)} / ${s.advancementLatencyMs.p95.toFixed(1)}ms`
      : 'N/A';
    const rmStr = s?.replicationLagMs
      ? `${s.replicationLagMs.p50.toFixed(1)} / ${s.replicationLagMs.p95.toFixed(1)}ms`
      : 'N/A';
    const status = a.status === 'pass' ? 'HEALTHY' : 'COLLAPSED';

    const vsCol = showVS ? `${String(a.numViewSyncers ?? 1).padEnd(2)} | ` : '';
    stdout(
      ` ${p.profile.padEnd(11)} | ` +
        `${p.model.padEnd(9)} | ` +
        `${String(p.users).padEnd(5)} | ` +
        vsCol +
        `${String(a.writeRate).padEnd(5)} | ` +
        `${String(s?.achievedWriteRate.toFixed(1) ?? '0').padEnd(6)} | ` +
        `${e2eStr.padEnd(17)} | ` +
        `${ivmStr.padEnd(17)} | ` +
        `${rmStr.padEnd(16)} | ` +
        `${status}\n`,
    );
  }
  stdout(
    '========================================================================================================================\n\n',
  );
}

async function runAttempt(args: {
  readonly config: LinearSweepConfig;
  readonly point: SweepPoint;
  readonly writeRate: number;
  readonly numViewSyncers: number;
  readonly runsDir: string;
  readonly childLogsDir: string;
}): Promise<SweepAttempt> {
  const suffix =
    args.numViewSyncers > 1 || args.config.topology === 'distributed'
      ? `-vs${args.numViewSyncers}`
      : '';
  const outputPath = join(
    args.runsDir,
    `${pointID(args.point)}${suffix}-rate${args.writeRate}.json`,
  );
  const logPath = join(
    args.childLogsDir,
    `${pointID(args.point)}${suffix}-rate${args.writeRate}.log`,
  );

  if (args.config.resume && (await fileExists(outputPath))) {
    const result = await readBenchmarkResult(outputPath);
    return attemptFromResult({
      point: args.point,
      writeRate: args.writeRate,
      repetition: 0,
      outputPath,
      logPath,
      reused: true,
      result,
      numViewSyncers: args.numViewSyncers,
    });
  }

  const command = benchmarkCommand(
    args.config,
    args.point,
    args.writeRate,
    {
      outputPath,
      logsDir: join(args.config.outputDir, 'logs'),
    },
    {
      numViewSyncers: args.numViewSyncers,
      topology: args.numViewSyncers > 1 ? 'distributed' : args.config.topology,
    },
  );
  const exitCode = await runCommandToLog({
    command: command[0],
    args: command.slice(1),
    cwd: process.cwd(),
    logPath,
    verbose: args.config.verboseChildLogs,
  });

  if (!(await fileExists(outputPath))) {
    return {
      point: args.point,
      writeRate: args.writeRate,
      repetition: 0,
      outputPath,
      logPath,
      reused: false,
      status: 'error',
      exitCode,
      error: `Benchmark exited ${exitCode} without writing ${outputPath}`,
      summary: undefined,
      numViewSyncers: args.numViewSyncers,
    };
  }

  const result = await readBenchmarkResult(outputPath);
  return attemptFromResult({
    point: args.point,
    writeRate: args.writeRate,
    repetition: 0,
    outputPath,
    logPath,
    reused: false,
    result,
    exitCode,
    numViewSyncers: args.numViewSyncers,
  });
}

function printDryRun(config: LinearSweepConfig): void {
  const points = sweepPoints(config);
  const rates = config.writeRates;
  const vsCounts =
    config.viewSyncers && config.viewSyncers.length > 0
      ? config.viewSyncers
      : config.numViewSyncers !== undefined
        ? [config.numViewSyncers]
        : [1];
  stdout(`zero-throughput linear sweep dry run\n`);
  stdout(
    `points: ${points.length}, vs pods: ${vsCounts.join(', ')}, write rates: ${rates.join(', ')}\n`,
  );
  stdout(
    `total benchmark runs: ${points.length * vsCounts.length * rates.length}\n`,
  );
  stdout(`duration per benchmark: ${formatDuration(config.durationMs)}\n\n`);
  for (const point of points) {
    for (const vs of vsCounts) {
      for (const rate of rates) {
        const vsTag = vs > 1 || vsCounts.length > 1 ? `[${vs} VS] ` : '';
        stdout(`${pointLabel(point)} ${vsTag}@ ${rate} writes/s\n`);
      }
    }
  }
}

export function parseLinearSweepArgs(
  argv: readonly string[],
): LinearSweepConfig {
  const runID = new Date().toISOString().replace(/[:.]/g, '-');
  let writeRates: readonly number[] = [100, 250, 500, 1000, 2000];
  let topology: BenchmarkTopology | undefined;
  let numViewSyncers: number | undefined;
  let viewSyncers: readonly number[] | undefined;
  let numSyncWorkers: number | undefined;
  let writeConcurrency: number | undefined;
  let profileRM = false;
  let profileVS = false;
  let profiles: readonly BenchmarkProfile[] = DEFAULT_PROFILES;
  let models: readonly BenchmarkModel[] = DEFAULT_MODELS;
  let users: readonly number[] = DEFAULT_USERS;
  let rowsPerQuery: readonly number[] = DEFAULT_ROWS_PER_QUERY;
  let syncWorkers: readonly number[] = DEFAULT_SYNC_WORKERS;
  let queriesPerUser = 3;
  let durationMs = 30_000;
  let warmupMs = 5_000;
  let settleMs = 3_000;
  let sampleIntervalMs = 1_000;
  let progressIntervalMs = 5_000;
  let sloP99LagMs = 2_000;
  let batchSize = 1;
  let payloadBytes = 256;
  let outputDir = join('results', 'sweeps', runID);
  let zeroPort = 4_848;
  let cacheURL: string | undefined;
  let cacheURLs: string | undefined;
  let pgURL: string | undefined;
  let resume = true;
  let continueOnError = false;
  let dryRun = false;
  let limit: number | undefined;
  let verboseChildLogs = false;
  let help = false;

  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--') {
      continue;
    }
    const option = parseOption(argv[i]);
    switch (option.name) {
      case '--help':
      case '-h':
        help = true;
        break;
      case '--write-rates': {
        writeRates = parsePositiveIntegerList(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      }
      case '--profiles':
        profiles = parseProfiles(readOptionValue(argv, option, i));
        i += option.value === undefined ? 1 : 0;
        break;
      case '--models':
        models = parseModels(readOptionValue(argv, option, i));
        i += option.value === undefined ? 1 : 0;
        break;
      case '--users':
        users = parsePositiveIntegerList(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--rows-per-query':
        rowsPerQuery = parsePositiveIntegerList(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--sync-workers':
        syncWorkers = parsePositiveIntegerList(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--queries-per-user':
        queriesPerUser = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--duration-ms':
        durationMs = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--warmup-ms':
        warmupMs = parseNonNegativeInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--settle-ms':
        settleMs = parseNonNegativeInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--sample-interval-ms':
        sampleIntervalMs = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--progress-interval-ms':
        progressIntervalMs = parseNonNegativeInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--slo-p99lag-ms':
        sloP99LagMs = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--batch-size':
        batchSize = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--payload-bytes':
        payloadBytes = parseNonNegativeInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--output-dir':
        outputDir = readOptionValue(argv, option, i);
        i += option.value === undefined ? 1 : 0;
        break;
      case '--zero-port':
        zeroPort = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--cache-url':
        cacheURL = readOptionValue(argv, option, i);
        i += option.value === undefined ? 1 : 0;
        break;
      case '--cache-urls':
        cacheURLs = readOptionValue(argv, option, i);
        i += option.value === undefined ? 1 : 0;
        break;
      case '--pg-url':
        pgURL = readOptionValue(argv, option, i);
        i += option.value === undefined ? 1 : 0;
        break;
      case '--resume': {
        const parsed = readBooleanOption(argv, option, i, true);
        resume = parsed.value;
        i += parsed.consumed;
        break;
      }
      case '--continue-on-error': {
        const parsed = readBooleanOption(argv, option, i, true);
        continueOnError = parsed.value;
        i += parsed.consumed;
        break;
      }
      case '--dry-run': {
        const parsed = readBooleanOption(argv, option, i, true);
        dryRun = parsed.value;
        i += parsed.consumed;
        break;
      }
      case '--limit':
        limit = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--verbose-child-logs': {
        const parsed = readBooleanOption(argv, option, i, true);
        verboseChildLogs = parsed.value;
        i += parsed.consumed;
        break;
      }
      case '--topology': {
        const value = readOptionValue(argv, option, i);
        if (value !== 'single' && value !== 'distributed') {
          throw new Error('--topology must be "single" or "distributed"');
        }
        topology = value;
        i += option.value === undefined ? 1 : 0;
        break;
      }
      case '--num-view-syncers':
      case '--view-syncers': {
        const raw = readOptionValue(argv, option, i);
        viewSyncers = parsePositiveIntegerList(option.name, raw);
        if (viewSyncers.length > 1 || viewSyncers[0] > 1) {
          topology ??= 'distributed';
        }
        numViewSyncers = viewSyncers[0];
        i += option.value === undefined ? 1 : 0;
        break;
      }
      case '--num-sync-workers': {
        numSyncWorkers = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      }
      case '--write-concurrency': {
        writeConcurrency = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      }
      case '--profile-rm': {
        const parsed = readBooleanOption(argv, option, i, true);
        profileRM = parsed.value;
        i += parsed.consumed;
        break;
      }
      case '--profile-vs': {
        const parsed = readBooleanOption(argv, option, i, true);
        profileVS = parsed.value;
        i += parsed.consumed;
        break;
      }
      default:
        throw new Error(`Unknown linear sweep option ${option.name}`);
    }
  }

  if (help) {
    printUsage();
    process.exit(0);
  }

  return {
    runID,
    writeRates,
    viewSyncers,
    topology,
    numViewSyncers,
    numSyncWorkers,
    writeConcurrency,
    profileRM,
    profileVS,
    profiles,
    models,
    users,
    rowsPerQuery,
    syncWorkers,
    queriesPerUser,
    durationMs,
    warmupMs,
    settleMs,
    sampleIntervalMs,
    progressIntervalMs,
    sloP99LagMs,
    batchSize,
    payloadBytes,
    outputDir,
    zeroPort,
    cacheURL,
    cacheURLs,
    pgURL,
    resume,
    continueOnError,
    dryRun,
    limit,
    verboseChildLogs,
  };
}

function printUsage(): void {
  stdout(`Usage:
  pnpm --filter zero-throughput run sweep:linear -- [options]

Evaluates a specific set of target write rates and prints a side-by-side comparative table
displaying throughput, E2E lag, IVM pipeline advancement, and RM replication lag.

Options:
  --write-rates <r1,r2,...>     Target write rates to evaluate (default: 100,250,500,1000,2000)
  --profiles <p1,p2,...>        Profiles: feed-append, email, forum, relational (default: relational,email,forum)
  --models <m1,m2,...>          Models: hot, realistic (default: hot)
  --users <u1,u2,...>           Active users count (default: 50,100,200,400)
  --rows-per-query <n1,...>     Row limit per query (default: 50)
  --sync-workers <n1,...>       View-syncer worker processes (default: 1,2,4)
  --duration-ms <ms>            Benchmark duration per test point (default: 30000)
  --topology <single|distributed> Process architecture (default: single)
  --num-view-syncers <n1,...>   Number of View-Syncer pods (e.g. 1,2,3 sweeps across pod counts)
  --num-sync-workers <N>        Number of sync workers per View-Syncer
  --write-concurrency <N>       Concurrent writer database connections
  --batch-size <N>              Rows inserted per transaction
  --profile-rm                  Collect V8 CPU profile on Replication Manager (rm)
  --profile-vs                  Collect V8 CPU profile on View-Syncer (vs-0)
  --dry-run                     List test points without executing
  --output-dir <path>           Directory for benchmark artifacts
  --cache-url <url>             Connect to existing Zero cache instead of launching locally
  --cache-urls <u1,u2,...>      Comma-separated View-Syncer URLs for partitioned clients
  --pg-url <url>                Connect to existing PostgreSQL instead of launching local Docker

Examples:
  pnpm --filter zero-throughput run sweep:write-rates
  pnpm --filter zero-throughput run sweep:num-view-syncers
  pnpm --filter zero-throughput run sweep:users
  pnpm --filter zero-throughput run sweep:linear -- --profiles feed-append --write-rates 500,1000,2000
`);
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const argv = process.argv.slice(2);
  const config = parseLinearSweepArgs(argv[0] === '--' ? argv.slice(1) : argv);
  if (config.dryRun) {
    printDryRun(config);
  } else {
    await runLinearSweep(config);
  }
}
