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
  CSV_ESCAPE_PATTERN,
  DEFAULT_MODELS,
  DEFAULT_PROFILES,
  DEFAULT_ROWS_PER_QUERY,
  DEFAULT_SEARCH_STEPS,
  DEFAULT_SYNC_WORKERS,
  DEFAULT_USERS,
  DEFAULT_WRITE_RATE_MAX,
  fileExists,
  formatDuration,
  gitCommit,
  parseModels,
  parseNonNegativeInteger,
  parseOption,
  parsePositiveInteger,
  parsePositiveIntegerList,
  parseProfiles,
  pointID,
  pointLabel,
  type PointResult,
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

export type BinarySweepConfig = BaseSweepConfig & {
  readonly writeRateMin: number;
  readonly writeRateMax: number;
  readonly searchSteps: number;
  readonly repetitions: number;
};

export async function runBinarySweep(config: BinarySweepConfig): Promise<void> {
  const outputDir = appPath(config.outputDir);
  const runsDir = join(outputDir, 'runs');
  const childLogsDir = join(outputDir, 'child-logs');
  const manifestPath = join(outputDir, 'manifest.json');
  const attemptsPath = join(outputDir, 'attempts.jsonl');
  const pointsPath = join(outputDir, 'points.jsonl');
  const summaryPath = join(outputDir, 'summary.csv');
  const points = sweepPoints(config);

  await mkdir(runsDir, {recursive: true});
  await mkdir(childLogsDir, {recursive: true});

  if (!config.resume || !(await fileExists(manifestPath))) {
    await writeFile(
      manifestPath,
      `${JSON.stringify(
        {
          gitCommit: gitCommit(),
          config,
          points,
        },
        null,
        2,
      )}\n`,
    );
  }

  if (!config.resume || !(await fileExists(attemptsPath))) {
    await writeFile(attemptsPath, '');
  }
  if (!config.resume || !(await fileExists(pointsPath))) {
    await writeFile(pointsPath, '');
  }

  const completedPointIDs = new Set<string>();
  if (config.resume && (await fileExists(pointsPath))) {
    const text = await readBenchmarkResultText(pointsPath);
    for (const line of text.split('\n')) {
      if (line.trim().length === 0) {
        continue;
      }
      const parsed = JSON.parse(line) as {readonly point: SweepPoint};
      completedPointIDs.add(pointID(parsed.point));
    }
  }

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

  const results: PointResult[] = [];
  try {
    stdout(`zero-throughput binary search sweep ${config.runID}\n`);
    stdout(`output: ${outputDir}\n`);
    stdout(`total points: ${points.length}\n`);

    if (config.pgURL === undefined) {
      stdout('Starting sweep PostgreSQL...\n');
      await startPostgres();
      postgresStarted = true;
    }

    for (let i = 0; i < points.length; i++) {
      const point = points[i];
      const id = pointID(point);
      stdout(`\n[${i + 1}/${points.length}] ${pointLabel(point)}\n`);
      if (completedPointIDs.has(id)) {
        stdout('  already completed, skipping\n');
        continue;
      }

      const result = await runPointSearch({
        config,
        point,
        runsDir,
        childLogsDir,
        attemptsPath,
      });
      results.push(result);
      completedPointIDs.add(id);

      await appendFile(pointsPath, `${JSON.stringify(result)}\n`);
      await writeSummaryCSV(summaryPath, points, results, outputDir);

      if (result.bestWriteRate !== undefined) {
        stdout(
          `  best sustainable write rate: ${result.bestWriteRate} logical writes/s\n`,
        );
      } else {
        stdout(
          `  no sustainable write rate found in [${config.writeRateMin}, ${config.writeRateMax}]\n`,
        );
      }
    }
  } finally {
    process.off('SIGINT', onSigint);
    if (postgresStarted) {
      stdout('\nStopping sweep PostgreSQL...\n');
      await stopPostgres();
    }
  }

  stdout(`\nsweep finished. summary written to ${summaryPath}\n`);
}

async function runPointSearch(args: {
  readonly config: BinarySweepConfig;
  readonly point: SweepPoint;
  readonly runsDir: string;
  readonly childLogsDir: string;
  readonly attemptsPath: string;
}): Promise<PointResult> {
  let low = args.config.writeRateMin;
  let high = args.config.writeRateMax;
  let bestWriteRate: number | undefined;
  let bestAttempt: SweepAttempt | undefined;
  const attempts: SweepAttempt[] = [];

  for (let step = 0; step < args.config.searchSteps && low <= high; step++) {
    const writeRate = Math.floor((low + high) / 2);
    const rateAttempts: SweepAttempt[] = [];
    let ratePassed = true;

    stdout(`  step ${step + 1}: ${writeRate} logical writes/s\n`);
    for (
      let repetition = 0;
      repetition < args.config.repetitions;
      repetition++
    ) {
      const attempt = await runAttempt({
        config: args.config,
        point: args.point,
        writeRate,
        repetition,
        runsDir: args.runsDir,
        childLogsDir: args.childLogsDir,
      });
      attempts.push(attempt);
      rateAttempts.push(attempt);
      await appendFile(args.attemptsPath, `${JSON.stringify(attempt)}\n`);

      if (attempt.status === 'error' && !args.config.continueOnError) {
        throw new Error(
          `Benchmark attempt failed for ${pointLabel(args.point)} at ${writeRate} writes/s. See ${attempt.logPath}`,
        );
      }
      if (attempt.status !== 'pass') {
        ratePassed = false;
        break;
      }
    }

    if (ratePassed) {
      bestWriteRate = writeRate;
      bestAttempt = rateAttempts.at(-1);
      low = writeRate + 1;
    } else {
      high = writeRate - 1;
    }
  }

  return {
    point: args.point,
    bestWriteRate,
    lowerBoundWriteRate: low,
    upperBoundWriteRate: high,
    attempts,
    bestAttempt,
  };
}

async function runAttempt(args: {
  readonly config: BinarySweepConfig;
  readonly point: SweepPoint;
  readonly writeRate: number;
  readonly repetition: number;
  readonly runsDir: string;
  readonly childLogsDir: string;
}): Promise<SweepAttempt> {
  const outputPath = join(
    args.runsDir,
    `${pointID(args.point)}-rate${args.writeRate}-rep${args.repetition + 1}.json`,
  );
  const logPath = join(
    args.childLogsDir,
    `${pointID(args.point)}-rate${args.writeRate}-rep${args.repetition + 1}.log`,
  );

  if (args.config.resume && (await fileExists(outputPath))) {
    const result = await readBenchmarkResult(outputPath);
    return attemptFromResult({
      point: args.point,
      writeRate: args.writeRate,
      repetition: args.repetition,
      outputPath,
      logPath,
      reused: true,
      result,
    });
  }

  const command = benchmarkCommand(args.config, args.point, args.writeRate, {
    outputPath,
    logsDir: join(args.config.outputDir, 'logs'),
  });
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
      repetition: args.repetition,
      outputPath,
      logPath,
      reused: false,
      status: 'error',
      exitCode,
      error: `Benchmark exited ${exitCode} without writing ${outputPath}`,
      summary: undefined,
    };
  }

  const result = await readBenchmarkResult(outputPath);
  return attemptFromResult({
    point: args.point,
    writeRate: args.writeRate,
    repetition: args.repetition,
    outputPath,
    logPath,
    reused: false,
    result,
    exitCode,
  });
}

async function writeSummaryCSV(
  summaryPath: string,
  allPoints: readonly SweepPoint[],
  results: readonly PointResult[],
  outputDir: string,
): Promise<void> {
  const resultMap = new Map<string, PointResult>();
  for (const r of results) {
    resultMap.set(pointID(r.point), r);
  }

  const pointsPath = join(outputDir, 'points.jsonl');
  if (await fileExists(pointsPath)) {
    const text = await readBenchmarkResultText(pointsPath);
    for (const line of text.split('\n')) {
      if (line.trim().length === 0) {
        continue;
      }
      const parsed = JSON.parse(line) as PointResult;
      resultMap.set(pointID(parsed.point), parsed);
    }
  }

  const lines: string[] = [csvHeader()];
  for (const point of allPoints) {
    const result = resultMap.get(pointID(point));
    if (result !== undefined) {
      lines.push(csvRow(result));
    }
  }

  await writeFile(summaryPath, `${lines.join('\n')}\n`);
}

function csvHeader(): string {
  return [
    'profile',
    'model',
    'users',
    'queriesPerUser',
    'rowsPerQuery',
    'zeroNumSyncWorkers',
    'bestWriteRate',
    'attemptedWriteRates',
    'bestP99ClientVisibleLagMs',
    'bestMaxSeqLag',
    'bestLagSlopeSeqPerSec',
    'bestAffectedActiveClientGroupWriteRatio',
    'bestOutputPath',
    'bestFailureReasons',
  ].join(',');
}

function csvRow(result: PointResult): string {
  const best = result.bestAttempt;
  return [
    result.point.profile,
    result.point.model,
    result.point.users,
    result.point.queriesPerUser,
    result.point.rowsPerQuery,
    result.point.zeroNumSyncWorkers,
    result.bestWriteRate ?? '',
    result.attempts.map(attempt => attempt.writeRate).join('|'),
    best?.summary?.p99ClientVisibleLagMs ?? '',
    best?.summary?.maxSeqLag ?? '',
    best?.summary?.lagSlopeSeqPerSec ?? '',
    best?.summary?.writeImpact.affectedActiveClientGroupWriteRatio ?? '',
    best?.outputPath ?? '',
    best?.summary?.failureReasons.join('|') ?? '',
  ]
    .map(csvCell)
    .join(',');
}

function csvCell(value: unknown): string {
  const text = String(value);
  if (CSV_ESCAPE_PATTERN.test(text)) {
    return `"${text.replaceAll('"', '""')}"`;
  }
  return text;
}

async function readBenchmarkResultText(path: string): Promise<string> {
  const {readFile} = await import('node:fs/promises');
  return await readFile(path, 'utf8');
}

function printDryRun(config: BinarySweepConfig): void {
  const points = sweepPoints(config);
  stdout(`zero-throughput binary search sweep dry run\n`);
  stdout(`points: ${points.length}\n`);
  stdout(
    `max benchmark runs: ${points.length * config.searchSteps * config.repetitions}\n`,
  );
  stdout(`duration per benchmark: ${formatDuration(config.durationMs)}\n`);
  stdout(
    `write-rate search: ${config.writeRateMin}-${config.writeRateMax} logical writes/s, ${config.searchSteps} steps\n\n`,
  );
  for (const point of points) {
    stdout(`${pointLabel(point)}\n`);
  }
}

export function parseBinarySweepArgs(
  argv: readonly string[],
): BinarySweepConfig {
  const runID = new Date().toISOString().replace(/[:.]/g, '-');
  let topology: BenchmarkTopology | undefined;
  let numViewSyncers: number | undefined;
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
  let durationMs = 300_000;
  let warmupMs = 30_000;
  let settleMs = 5_000;
  let sampleIntervalMs = 1_000;
  let progressIntervalMs = 5_000;
  let sloP99LagMs = 2_000;
  let batchSize = 1;
  let payloadBytes = 256;
  let writeRateMin = 1;
  let writeRateMax = DEFAULT_WRITE_RATE_MAX;
  let searchSteps = DEFAULT_SEARCH_STEPS;
  let repetitions = 1;
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
      case '--write-rate-min':
        writeRateMin = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--write-rate-max':
        writeRateMax = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--search-steps':
        searchSteps = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        break;
      case '--repetitions':
        repetitions = parsePositiveInteger(
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
      case '--num-view-syncers': {
        numViewSyncers = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
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
        throw new Error(`Unknown sweep option ${option.name}`);
    }
  }

  if (help) {
    printUsage();
    process.exit(0);
  }

  if (writeRateMin > writeRateMax) {
    throw new Error('--write-rate-min must be <= --write-rate-max');
  }

  return {
    runID,
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
    writeRateMin,
    writeRateMax,
    searchSteps,
    repetitions,
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
  pnpm --filter zero-throughput run sweep -- [options]

Executes an automated binary search across write rates to discover the maximum sustainable
write capacity envelope for each matrix point, writing results to attempts.jsonl and summary.csv.

Matrix Dimensions:
  --profiles <p1,p2,...>        Profiles: feed-append, email, forum, relational (default: relational,email,forum)
  --models <m1,m2,...>          Models: hot, realistic (default: hot)
  --users <u1,u2,...>           Client user counts (default: 50,100,200,400)
  --rows-per-query <n1,...>     Row limit per query (default: 50)
  --sync-workers <n1,...>       View-syncer worker processes (default: 1,2,4)
  --queries-per-user <N>        Queries per client user (default: 3)

Binary Search Controls:
  --write-rate-min <N>          Minimum write rate to test (default: 1)
  --write-rate-max <N>          Maximum write rate to test (default: 100)
  --search-steps <N>            Number of binary search steps (default: 7)
  --repetitions <N>             Repetitions per candidate rate (default: 1)
  --duration-ms <ms>            Benchmark duration per attempt (default: 300000)

Architecture & Tuning:
  --topology <single|distributed> Process architecture (default: single)
  --num-view-syncers <N>        Number of View-Syncer pods when topology=distributed
  --num-sync-workers <N>        Number of sync workers per View-Syncer
  --write-concurrency <N>       Concurrent writer database connections
  --batch-size <N>              Rows inserted per transaction
  --profile-rm                  Collect V8 CPU profile on Replication Manager (rm)
  --profile-vs                  Collect V8 CPU profile on View-Syncer (vs-0)

Execution & Output:
  --output-dir <path>           Results directory (default: results/sweeps/<runID>)
  --dry-run                     List matrix points without executing
  --limit <N>                   Limit execution to first N matrix points
  --resume <true|false>         Resume prior interrupted sweep (default: true)
  --cache-url <url>             Connect to existing Zero cache instead of launching locally
  --cache-urls <u1,u2,...>      Comma-separated View-Syncer URLs for partitioned clients
  --pg-url <url>                Connect to existing PostgreSQL instead of launching local Docker
  --verbose-child-logs          Stream full child benchmark logs to stdout

Examples:
  pnpm --filter zero-throughput run sweep -- --dry-run
  pnpm --filter zero-throughput run sweep -- --limit 1 --write-rate-max 50
  pnpm --filter zero-throughput run sweep -- --profiles relational --users 50,100
`);
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const argv = process.argv.slice(2);
  const config = parseBinarySweepArgs(argv[0] === '--' ? argv.slice(1) : argv);
  if (config.dryRun) {
    printDryRun(config);
  } else {
    await runBinarySweep(config);
  }
}
