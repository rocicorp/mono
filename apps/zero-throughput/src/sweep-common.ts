import {execFileSync, spawn} from 'node:child_process';
import {once} from 'node:events';
import {createWriteStream, type WriteStream} from 'node:fs';
import {access, mkdir, readFile} from 'node:fs/promises';
import {dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import type {
  BenchmarkModel,
  BenchmarkProfile,
  BenchmarkTopology,
} from './config.ts';
import {appRoot, DEFAULT_PG_URL} from './config.ts';
import type {BenchmarkResult} from './results.ts';
export {formatDuration} from './util.ts';

export const DEFAULT_PROFILES = ['relational', 'email', 'forum'] as const;
export const DEFAULT_MODELS = ['hot'] as const;
export const DEFAULT_USERS = [50, 100, 200, 400] as const;
export const DEFAULT_ROWS_PER_QUERY = [50] as const;
export const DEFAULT_SYNC_WORKERS = [1, 2, 4] as const;
export const DEFAULT_WRITE_RATE_MAX = 100;
export const DEFAULT_SEARCH_STEPS = 7;
export const CSV_ESCAPE_PATTERN = /[",\n]/;
export const PROFILE_VALUES = new Set<BenchmarkProfile>([
  'feed-append',
  'email',
  'forum',
  'relational',
]);
export const MODEL_VALUES = new Set<BenchmarkModel>(['hot', 'realistic']);

export type BaseSweepConfig = {
  readonly runID: string;
  readonly topology?: BenchmarkTopology | undefined;
  readonly numViewSyncers?: number | undefined;
  readonly numSyncWorkers?: number | undefined;
  readonly writeConcurrency?: number | undefined;
  readonly profileRM?: boolean | undefined;
  readonly profileVS?: boolean | undefined;
  readonly profiles: readonly BenchmarkProfile[];
  readonly models: readonly BenchmarkModel[];
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
  readonly outputDir: string;
  readonly zeroPort: number;
  readonly cacheURL?: string | undefined;
  readonly cacheURLs?: string | undefined;
  readonly pgURL: string | undefined;
  readonly resume: boolean;
  readonly continueOnError: boolean;
  readonly dryRun: boolean;
  readonly limit: number | undefined;
  readonly verboseChildLogs: boolean;
};

export type SweepPoint = {
  readonly profile: BenchmarkProfile;
  readonly model: BenchmarkModel;
  readonly users: number;
  readonly queriesPerUser: number;
  readonly rowsPerQuery: number;
  readonly zeroNumSyncWorkers: number;
};

export type AttemptStatus = 'pass' | 'fail' | 'error';

export type SweepAttempt = {
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
  readonly numViewSyncers?: number | undefined;
};

export type PointResult = {
  readonly point: SweepPoint;
  readonly bestWriteRate: number | undefined;
  readonly lowerBoundWriteRate: number;
  readonly upperBoundWriteRate: number;
  readonly attempts: readonly SweepAttempt[];
  readonly bestAttempt: SweepAttempt | undefined;
};

export function sweepPoints(config: {
  readonly profiles: readonly BenchmarkProfile[];
  readonly models: readonly BenchmarkModel[];
  readonly users: readonly number[];
  readonly rowsPerQuery: readonly number[];
  readonly syncWorkers: readonly number[];
  readonly queriesPerUser: number;
  readonly limit?: number | undefined;
}): readonly SweepPoint[] {
  const points: SweepPoint[] = [];
  for (const profile of config.profiles) {
    for (const model of config.models) {
      for (const users of config.users) {
        for (const rowsPerQuery of config.rowsPerQuery) {
          for (const zeroNumSyncWorkers of config.syncWorkers) {
            points.push({
              profile,
              model,
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
  }
  return points;
}

export function pointID(point: SweepPoint): string {
  return [
    point.profile,
    point.model,
    `${point.users}u`,
    `${point.queriesPerUser}q`,
    `${point.rowsPerQuery}rows`,
    `${point.zeroNumSyncWorkers}sync`,
  ].join('-');
}

export function pointLabel(point: SweepPoint): string {
  return `${point.profile}:${point.model} users=${point.users} queriesPerUser=${point.queriesPerUser} rowsPerQuery=${point.rowsPerQuery} syncWorkers=${point.zeroNumSyncWorkers}`;
}

export function gitCommit(): string | undefined {
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

export function stdout(message: string): void {
  process.stdout.write(message);
}

export function stderr(message: string): void {
  process.stderr.write(message);
}

export function parseOption(arg: string): {
  readonly name: string;
  readonly value: string | undefined;
} {
  const equals = arg.indexOf('=');
  if (equals === -1) {
    return {name: arg, value: undefined};
  }
  return {name: arg.slice(0, equals), value: arg.slice(equals + 1)};
}

export function readOptionValue(
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

export function readBooleanOption(
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

export function parseProfiles(value: string): readonly BenchmarkProfile[] {
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

export function parseModels(value: string): readonly BenchmarkModel[] {
  const models = value.split(',').map(part => {
    const trimmed = part.trim();
    if (!MODEL_VALUES.has(trimmed as BenchmarkModel)) {
      throw new Error(`Invalid model "${trimmed}"`);
    }
    return trimmed as BenchmarkModel;
  });
  if (models.length === 0) {
    throw new Error('--models must not be empty');
  }
  return models;
}

export function parsePositiveIntegerList(
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

export function parsePositiveInteger(name: string, value: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

export function parseNonNegativeInteger(name: string, value: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return parsed;
}

export function parseBoolean(value: string): boolean {
  switch (value) {
    case 'true':
      return true;
    case 'false':
      return false;
    default:
      throw new Error(`Expected boolean value, got "${value}"`);
  }
}

export function benchmarkCommand(
  config: BaseSweepConfig,
  point: SweepPoint,
  writeRate: number,
  paths: {readonly outputPath: string; readonly logsDir: string},
  override?: {
    readonly numViewSyncers?: number | undefined;
    readonly topology?: BenchmarkTopology | undefined;
  },
): readonly string[] {
  const main = fileURLToPath(new URL('main.ts', import.meta.url));
  const command = [
    process.execPath,
    main,
    '--profile',
    point.profile,
    '--model',
    point.model,
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
    '--output',
    paths.outputPath,
    '--logs-dir',
    paths.logsDir,
    '--pg-url',
    config.pgURL ?? DEFAULT_PG_URL,
  ];
  if (config.cacheURLs !== undefined) {
    command.push('--cache-urls', config.cacheURLs);
  } else if (config.cacheURL !== undefined) {
    command.push('--cache-url', config.cacheURL);
  }
  const topology = override?.topology ?? config.topology;
  if (topology !== undefined) {
    command.push('--topology', topology);
  }
  const numViewSyncers = override?.numViewSyncers ?? config.numViewSyncers;
  if (numViewSyncers !== undefined) {
    command.push('--num-view-syncers', String(numViewSyncers));
  }
  if (config.numSyncWorkers !== undefined) {
    command.push('--num-sync-workers', String(config.numSyncWorkers));
  }
  if (config.writeConcurrency !== undefined) {
    command.push('--write-concurrency', String(config.writeConcurrency));
  }
  if (config.profileRM) {
    command.push('--profile-rm');
  }
  if (config.profileVS) {
    command.push('--profile-vs');
  }
  return command;
}

export async function runCommandToLog(args: {
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

export function attemptFromResult(args: {
  readonly point: SweepPoint;
  readonly writeRate: number;
  readonly repetition: number;
  readonly outputPath: string;
  readonly logPath: string;
  readonly reused: boolean;
  readonly result: BenchmarkResult;
  readonly exitCode?: number | undefined;
  readonly numViewSyncers?: number | undefined;
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
    numViewSyncers: args.numViewSyncers,
  };
}

export async function readBenchmarkResult(
  path: string,
): Promise<BenchmarkResult> {
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

export async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

export async function writeLog(
  stream: WriteStream,
  text: string,
): Promise<void> {
  if (!stream.write(text)) {
    await once(stream, 'drain');
  }
}

export async function closeLog(stream: WriteStream): Promise<void> {
  stream.end();
  await once(stream, 'finish');
}
