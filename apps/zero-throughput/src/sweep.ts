import {execFileSync, spawn} from 'node:child_process';
import {once} from 'node:events';
import {createWriteStream, type WriteStream} from 'node:fs';
import {access, appendFile, mkdir, readFile, writeFile} from 'node:fs/promises';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import type {BenchmarkProfile} from './config.ts';
import {appPath, appRoot, repoRoot} from './config.ts';
import {startPostgres, stopPostgres} from './processes.ts';
import type {BenchmarkResult} from './results.ts';
import {formatDuration} from './util.ts';

const DEFAULT_PROFILES = ['relational', 'email', 'forum'] as const;
const DEFAULT_USERS = [50, 100, 200, 400] as const;
const DEFAULT_ROWS_PER_QUERY = [50] as const;
const DEFAULT_SYNC_WORKERS = [1, 2, 4] as const;
const DEFAULT_WRITE_RATE_MAX = 100;
const DEFAULT_SEARCH_STEPS = 7;
const CSV_ESCAPE_PATTERN = /[",\n]/;
const PROFILE_VALUES = new Set<BenchmarkProfile>([
  'feed-append',
  'email',
  'forum',
  'relational',
]);

type SweepConfig = {
  readonly runID: string;
  readonly profiles: readonly BenchmarkProfile[];
  readonly users: readonly number[];
  readonly rowsPerQuery: readonly number[];
  readonly syncWorkers: readonly number[];
  readonly queriesPerUser: number;
  readonly durationMs: number;
  readonly warmupMs: number;
  readonly settleMs: number;
  readonly sampleIntervalMs: number;
  readonly progressIntervalMs: number;
  readonly sloP99LagMs: number;
  readonly batchSize: number;
  readonly payloadBytes: number;
  readonly writeRateMin: number;
  readonly writeRateMax: number;
  readonly searchSteps: number;
  readonly repetitions: number;
  readonly outputDir: string;
  readonly zeroPort: number;
  readonly pgStart: boolean;
  readonly pgURL: string | undefined;
  readonly resume: boolean;
  readonly continueOnError: boolean;
  readonly dryRun: boolean;
  readonly limit: number | undefined;
  readonly verboseChildLogs: boolean;
};

type SweepPoint = {
  readonly profile: BenchmarkProfile;
  readonly users: number;
  readonly queriesPerUser: number;
  readonly rowsPerQuery: number;
  readonly zeroNumSyncWorkers: number;
};

type AttemptStatus = 'pass' | 'fail' | 'error';

type SweepAttempt = {
  readonly point: SweepPoint;
  readonly writeRate: number;
  readonly repetition: number;
  readonly outputPath: string;
  readonly logPath: string;
  readonly reused: boolean;
  readonly status: AttemptStatus;
  readonly exitCode: number | undefined;
  readonly error: string | undefined;
  readonly summary: BenchmarkResult['summary'] | undefined;
};

type PointResult = {
  readonly point: SweepPoint;
  readonly bestWriteRate: number | undefined;
  readonly lowerBoundWriteRate: number;
  readonly upperBoundWriteRate: number;
  readonly attempts: readonly SweepAttempt[];
  readonly bestAttempt: SweepAttempt | undefined;
};

const argv = process.argv.slice(2);
const config = parseArgs(argv[0] === '--' ? argv.slice(1) : argv);

if (config.dryRun) {
  printDryRun(config);
} else {
  await runSweep(config);
}

async function runSweep(config: SweepConfig): Promise<void> {
  const outputDir = appPath(config.outputDir);
  const runsDir = join(outputDir, 'runs');
  const childLogsDir = join(outputDir, 'child-logs');
  const attemptsPath = join(outputDir, 'attempts.jsonl');
  const pointsPath = join(outputDir, 'points.jsonl');
  const summaryPath = join(outputDir, 'summary.csv');
  const manifestPath = join(outputDir, 'manifest.json');
  const points = sweepPoints(config);
  const results: PointResult[] = [];
  let postgresStarted = false;

  await mkdir(runsDir, {recursive: true});
  await mkdir(childLogsDir, {recursive: true});
  await writeFile(
    manifestPath,
    `${JSON.stringify(
      {
        runID: config.runID,
        startedAt: new Date().toISOString(),
        gitCommit: gitCommit(),
        config,
        points,
        maxBenchmarkRuns:
          points.length * config.searchSteps * config.repetitions,
      },
      null,
      2,
    )}\n`,
  );
  await writeFile(attemptsPath, '');
  await writeFile(pointsPath, '');
  await writeFile(summaryPath, `${csvHeader()}\n`);

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

  try {
    stdout(`zero-throughput sweep ${config.runID}\n`);
    stdout(`output: ${outputDir}\n`);
    stdout(
      `points: ${points.length}, max benchmark runs: ${
        points.length * config.searchSteps * config.repetitions
      }\n`,
    );

    if (config.pgStart) {
      stdout('Starting sweep PostgreSQL...\n');
      await startPostgres();
      postgresStarted = true;
    }

    for (const [index, point] of points.entries()) {
      stdout(
        `\n[${index + 1}/${points.length}] ${pointLabel(point)} ` +
          `searching ${config.writeRateMin}-${config.writeRateMax} writes/s\n`,
      );
      const result = await runPointSearch({
        config,
        point,
        runsDir,
        childLogsDir,
        attemptsPath,
      });
      results.push(result);
      await appendFile(pointsPath, `${JSON.stringify(result)}\n`);
      await appendFile(summaryPath, `${csvRow(result)}\n`);
      stdout(
        `best sustainable write rate: ${
          result.bestWriteRate === undefined ? 'none' : result.bestWriteRate
        } logical writes/s\n`,
      );
    }

    await writeFile(
      join(outputDir, 'sweep.json'),
      `${JSON.stringify(
        {
          runID: config.runID,
          finishedAt: new Date().toISOString(),
          config,
          results,
        },
        null,
        2,
      )}\n`,
    );
    stdout(`\nsummary: ${summaryPath}\n`);
    stdout(`attempts: ${attemptsPath}\n`);
  } finally {
    process.off('SIGINT', onSigint);
    if (postgresStarted) {
      stdout('Stopping sweep PostgreSQL...\n');
      await stopPostgres();
    }
  }
}

async function runPointSearch(args: {
  readonly config: SweepConfig;
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
  readonly config: SweepConfig;
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
    cwd: repoRoot,
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

function attemptFromResult(args: {
  readonly point: SweepPoint;
  readonly writeRate: number;
  readonly repetition: number;
  readonly outputPath: string;
  readonly logPath: string;
  readonly reused: boolean;
  readonly result: BenchmarkResult;
  readonly exitCode?: number | undefined;
}): SweepAttempt {
  const pass = args.result.summary.pass;
  return {
    point: args.point,
    writeRate: args.writeRate,
    repetition: args.repetition,
    outputPath: args.outputPath,
    logPath: args.logPath,
    reused: args.reused,
    status: pass ? 'pass' : 'fail',
    exitCode: args.exitCode,
    error: undefined,
    summary: args.result.summary,
  };
}

function benchmarkCommand(
  config: SweepConfig,
  point: SweepPoint,
  writeRate: number,
  paths: {readonly outputPath: string; readonly logsDir: string},
): readonly string[] {
  const main = fileURLToPath(new URL('main.ts', import.meta.url));
  const command = [
    process.execPath,
    main,
    '--profile',
    point.profile,
    '--users',
    String(point.users),
    '--queries-per-user',
    String(point.queriesPerUser),
    '--rows-per-query',
    String(point.rowsPerQuery),
    '--write-rate',
    String(writeRate),
    '--duration-ms',
    String(config.durationMs),
    '--warmup-ms',
    String(config.warmupMs),
    '--settle-ms',
    String(config.settleMs),
    '--sample-interval-ms',
    String(config.sampleIntervalMs),
    '--progress-interval-ms',
    String(config.progressIntervalMs),
    '--slo-p99lag-ms',
    String(config.sloP99LagMs),
    '--batch-size',
    String(config.batchSize),
    '--payload-bytes',
    String(config.payloadBytes),
    '--zero-num-sync-workers',
    String(point.zeroNumSyncWorkers),
    '--zero-port',
    String(config.zeroPort),
    '--pg-start',
    'false',
    '--pg-stop-after-run',
    'false',
    '--output',
    paths.outputPath,
    '--logs-dir',
    paths.logsDir,
  ];
  if (config.pgURL !== undefined) {
    command.push('--pg-url', config.pgURL);
  }
  return command;
}

async function runCommandToLog(args: {
  readonly command: string;
  readonly args: readonly string[];
  readonly cwd: string;
  readonly logPath: string;
  readonly verbose: boolean;
}): Promise<number> {
  await mkdir(dirname(args.logPath), {recursive: true});
  const log = createWriteStream(args.logPath, {flags: 'w'});
  await writeLog(log, `$ ${args.command} ${args.args.join(' ')}\n`);
  try {
    return await new Promise<number>((resolve, reject) => {
      const child = spawn(args.command, args.args, {
        cwd: args.cwd,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      child.stdout?.on('data', chunk => {
        log.write(chunk);
        if (args.verbose) {
          process.stdout.write(chunk);
        }
      });
      child.stderr?.on('data', chunk => {
        log.write(chunk);
        if (args.verbose) {
          process.stderr.write(chunk);
        }
      });
      child.once('error', reject);
      child.once('exit', code => {
        resolve(code ?? 1);
      });
    });
  } finally {
    await closeLog(log);
  }
}

function sweepPoints(config: SweepConfig): readonly SweepPoint[] {
  const points: SweepPoint[] = [];
  for (const profile of config.profiles) {
    for (const users of config.users) {
      for (const rowsPerQuery of config.rowsPerQuery) {
        for (const zeroNumSyncWorkers of config.syncWorkers) {
          points.push({
            profile,
            users,
            queriesPerUser: config.queriesPerUser,
            rowsPerQuery,
            zeroNumSyncWorkers,
          });
          if (config.limit !== undefined && points.length >= config.limit) {
            return points;
          }
        }
      }
    }
  }
  return points;
}

function parseArgs(argv: readonly string[]): SweepConfig {
  const runID = new Date().toISOString().replace(/[:.]/g, '-');
  let profiles: readonly BenchmarkProfile[] = DEFAULT_PROFILES;
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
  let pgStart = true;
  let pgURL: string | undefined;
  let resume = true;
  let continueOnError = false;
  let dryRun = false;
  let limit: number | undefined;
  let verboseChildLogs = false;
  let help = false;

  for (let i = 0; i < argv.length; i++) {
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
      case '--zero-num-sync-workers':
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
      case '--slo-p99-lag-ms':
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
      case '--pg-start': {
        const parsed = readBooleanOption(argv, option, i, true);
        pgStart = parsed.value;
        i += parsed.consumed;
        break;
      }
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
    profiles,
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
    pgStart,
    pgURL,
    resume,
    continueOnError,
    dryRun,
    limit,
    verboseChildLogs,
  };
}

function parseOption(arg: string): {
  readonly name: string;
  readonly value: string | undefined;
} {
  const equals = arg.indexOf('=');
  if (equals === -1) {
    return {name: arg, value: undefined};
  }
  return {name: arg.slice(0, equals), value: arg.slice(equals + 1)};
}

function readOptionValue(
  argv: readonly string[],
  option: {readonly name: string; readonly value: string | undefined},
  index: number,
): string {
  if (option.value !== undefined) {
    return option.value;
  }
  const value = argv[index + 1];
  if (value === undefined || value.startsWith('--')) {
    throw new Error(`${option.name} requires a value`);
  }
  return value;
}

function readBooleanOption(
  argv: readonly string[],
  option: {readonly name: string; readonly value: string | undefined},
  index: number,
  defaultValue: boolean,
): {readonly value: boolean; readonly consumed: number} {
  if (option.value !== undefined) {
    return {value: parseBoolean(option.value), consumed: 0};
  }
  const value = argv[index + 1];
  if (value === 'true' || value === 'false') {
    return {value: parseBoolean(value), consumed: 1};
  }
  return {value: defaultValue, consumed: 0};
}

function parseProfiles(value: string): readonly BenchmarkProfile[] {
  const profiles = value.split(',').map(part => {
    const trimmed = part.trim();
    if (!PROFILE_VALUES.has(trimmed as BenchmarkProfile)) {
      throw new Error(`Invalid profile "${trimmed}"`);
    }
    return trimmed as BenchmarkProfile;
  });
  if (profiles.length === 0) {
    throw new Error('--profiles must not be empty');
  }
  return profiles;
}

function parsePositiveIntegerList(
  name: string,
  value: string,
): readonly number[] {
  const values = value
    .split(',')
    .map(part => parsePositiveInteger(name, part.trim()));
  if (values.length === 0) {
    throw new Error(`${name} must not be empty`);
  }
  return values;
}

function parsePositiveInteger(name: string, value: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function parseNonNegativeInteger(name: string, value: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return parsed;
}

function parseBoolean(value: string): boolean {
  switch (value) {
    case 'true':
      return true;
    case 'false':
      return false;
    default:
      throw new Error(`Expected boolean value, got "${value}"`);
  }
}

function printDryRun(config: SweepConfig): void {
  const points = sweepPoints(config);
  stdout(`zero-throughput recommended sweep dry run\n`);
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

function printUsage(): void {
  stdout(`Usage:
  pnpm --filter zero-throughput run sweep -- [options]

Default matrix:
  --profiles relational,email,forum
  --users 50,100,200,400
  --rows-per-query 50
  --sync-workers 1,2,4
  --queries-per-user 3
  --duration-ms 300000

Write-rate search:
  --write-rate-min 1
  --write-rate-max 100
  --search-steps 7

Useful:
  --dry-run
  --limit 1
  --output-dir results/sweeps/my-run
  --pg-start false
  --verbose-child-logs
`);
}

async function readBenchmarkResult(path: string): Promise<BenchmarkResult> {
  const parsed = JSON.parse(await readFile(path, 'utf8')) as unknown;
  if (
    parsed === null ||
    typeof parsed !== 'object' ||
    !('summary' in parsed) ||
    parsed.summary === null ||
    typeof parsed.summary !== 'object' ||
    !('pass' in parsed.summary) ||
    typeof parsed.summary.pass !== 'boolean'
  ) {
    throw new Error(`${path} is not a benchmark result`);
  }
  return parsed as BenchmarkResult;
}

async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

async function writeLog(stream: WriteStream, text: string): Promise<void> {
  if (!stream.write(text)) {
    await once(stream, 'drain');
  }
}

async function closeLog(stream: WriteStream): Promise<void> {
  stream.end();
  await once(stream, 'finish');
}

function csvHeader(): string {
  return [
    'profile',
    'users',
    'queriesPerUser',
    'rowsPerQuery',
    'zeroNumSyncWorkers',
    'bestWriteRate',
    'attemptedWriteRates',
    'bestP99ClientVisibleLagMs',
    'bestMaxSeqLag',
    'bestLagSlopeSeqPerSec',
    'bestOutputPath',
    'bestFailureReasons',
  ].join(',');
}

function csvRow(result: PointResult): string {
  const best = result.bestAttempt;
  return [
    result.point.profile,
    result.point.users,
    result.point.queriesPerUser,
    result.point.rowsPerQuery,
    result.point.zeroNumSyncWorkers,
    result.bestWriteRate ?? '',
    result.attempts.map(attempt => attempt.writeRate).join('|'),
    best?.summary?.p99ClientVisibleLagMs ?? '',
    best?.summary?.maxSeqLag ?? '',
    best?.summary?.lagSlopeSeqPerSec ?? '',
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

function pointID(point: SweepPoint): string {
  return [
    point.profile,
    `${point.users}u`,
    `${point.queriesPerUser}q`,
    `${point.rowsPerQuery}rows`,
    `${point.zeroNumSyncWorkers}sync`,
  ].join('-');
}

function pointLabel(point: SweepPoint): string {
  return `${point.profile} users=${point.users} queriesPerUser=${point.queriesPerUser} rowsPerQuery=${point.rowsPerQuery} syncWorkers=${point.zeroNumSyncWorkers}`;
}

function gitCommit(): string | undefined {
  try {
    return execFileSync('git', ['rev-parse', 'HEAD'], {
      cwd: appRoot,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim();
  } catch {
    return undefined;
  }
}

function stdout(message: string): void {
  process.stdout.write(message);
}

function stderr(message: string): void {
  process.stderr.write(message);
}
