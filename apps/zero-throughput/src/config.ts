import '../../../packages/shared/src/dotenv.ts';

import {isAbsolute, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {parseOptions} from '../../../packages/shared/src/options.ts';
import * as v from '../../../packages/shared/src/valita.ts';

export const appRoot = fileURLToPath(new URL('..', import.meta.url));
export const repoRoot = fileURLToPath(new URL('../../..', import.meta.url));

export const DEFAULT_PG_URL =
  'postgresql://user:password@127.0.0.1:6436/postgres';
const APP_ID_PATTERN = /^[a-z0-9_]+$/;

const options = {
  profile: v
    .literalUnion('feed-append', 'email', 'forum', 'relational')
    .default('feed-append'),
  model: v.literalUnion('hot', 'realistic').default('hot'),

  users: v.number().default(1),
  queriesPerUser: v.number().default(1),
  rowsPerQuery: v.number().default(100),
  writeRate: v.number().default(100),
  batchSize: v.number().default(1),
  rowsPerTx: v.number().default(1),
  writeConcurrency: v.number().default(1),
  payloadBytes: v.number().default(256),
  durationMs: v.number().default(30_000),
  warmupMs: v.number().default(10_000),
  settleMs: v.number().default(5_000),
  sampleIntervalMs: v.number().default(1_000),
  progressIntervalMs: v.number().default(5_000),
  sloP99LagMs: v.number().default(2_000),
  output: v.string().default('results/latest.json'),
  logsDir: v.string().default('results/logs'),
  profileDir: v.string().default('results/profiles'),
  processLogMode: v.literalUnion('file', 'inherit', 'ignore').default('file'),
  reset: v.boolean().default(true),
  cacheURL: v.string().optional(),
  cacheURLs: v.string().optional(),

  topology: v.literalUnion('single', 'distributed').default('single'),
  numViewSyncers: v.number().default(1),
  numSyncWorkers: v.number().optional(),
  profileRM: v.boolean().default(false),
  profileVS: v.boolean().default(false),

  pg: {
    url: v.string().optional(),
    stopAfterRun: v.boolean().default(true),
    readyTimeoutMs: v.number().default(60_000),
  },

  zero: {
    port: v.number().default(4_848),
    readyTimeoutMs: v.number().default(120_000),
    appID: v.string().default('zero_throughput'),
    replicaFile: v.string().default('/tmp/zero-throughput-replica.db'),
    logLevel: v.literalUnion('debug', 'info', 'warn', 'error').default('info'),
    numSyncWorkers: v.number().default(1),
    upstreamMaxConns: v.number().default(10),
    cvrMaxConns: v.number().default(10),
    changeMaxConns: v.number().default(5),
  },
};

export type BenchmarkProfile = 'feed-append' | 'email' | 'forum' | 'relational';
export type BenchmarkModel = 'hot' | 'realistic';
export type BenchmarkTopology = 'single' | 'distributed';

export type BenchmarkConfig = {
  readonly runID: string;
  readonly profile: BenchmarkProfile;
  readonly model: BenchmarkModel;
  readonly topology: BenchmarkTopology;
  readonly numViewSyncers: number;
  readonly numSyncWorkers: number;
  readonly users: number;
  readonly queriesPerUser: number;
  readonly rowsPerQuery: number;
  readonly writeRate: number;
  readonly batchSize: number;
  readonly rowsPerTx: number;
  readonly writeConcurrency: number;
  readonly payloadBytes: number;
  readonly durationMs: number;
  readonly warmupMs: number;
  readonly settleMs: number;
  readonly sampleIntervalMs: number;
  readonly progressIntervalMs: number;
  readonly sloP99LagMs: number;
  readonly outputPath: string;
  readonly logsDir: string;
  readonly profileDir: string;
  readonly profileRM: boolean;
  readonly profileVS: boolean;
  readonly processLogMode: 'file' | 'inherit' | 'ignore';
  readonly reset: boolean;
  readonly cacheURL: string;
  readonly cacheURLs: readonly string[];
  readonly pg: {
    readonly url: string;
    readonly start: boolean;
    readonly stopAfterRun: boolean;
    readonly readyTimeoutMs: number;
  };
  readonly zero: {
    readonly start: boolean;
    readonly port: number;
    readonly readyTimeoutMs: number;
    readonly appID: string;
    readonly replicaFile: string;
    readonly logLevel: 'debug' | 'info' | 'warn' | 'error';
    readonly numSyncWorkers: number;
    readonly upstreamMaxConns: number;
    readonly cvrMaxConns: number;
    readonly changeMaxConns: number;
  };
};

export function loadConfig(): BenchmarkConfig {
  const argv = process.argv.slice(2);
  const parsed = parseOptions(options, {
    argv: argv[0] === '--' ? argv.slice(1) : argv,
    envNamePrefix: 'ZERO_THROUGHPUT_',
  });
  assertPositiveInteger('users', parsed.users);
  assertPositiveInteger('queriesPerUser', parsed.queriesPerUser);
  assertPositiveInteger('rowsPerQuery', parsed.rowsPerQuery);
  assertPositiveNumber('writeRate', parsed.writeRate);
  assertPositiveInteger('batchSize', parsed.batchSize);
  assertPositiveInteger('rowsPerTx', parsed.rowsPerTx);
  assertPositiveInteger('writeConcurrency', parsed.writeConcurrency);
  assertPositiveInteger('numViewSyncers', parsed.numViewSyncers);
  const numSyncWorkers = parsed.numSyncWorkers ?? parsed.zero.numSyncWorkers;
  assertPositiveInteger('numSyncWorkers', numSyncWorkers);
  assertNonNegativeInteger('payloadBytes', parsed.payloadBytes);
  assertPositiveInteger('durationMs', parsed.durationMs);
  assertNonNegativeInteger('warmupMs', parsed.warmupMs);
  assertNonNegativeInteger('settleMs', parsed.settleMs);
  assertPositiveInteger('sampleIntervalMs', parsed.sampleIntervalMs);
  assertNonNegativeInteger('progressIntervalMs', parsed.progressIntervalMs);
  assertPositiveInteger('sloP99LagMs', parsed.sloP99LagMs);
  assertValidAppID(parsed.zero.appID);

  const effectiveBatchSize = Math.max(parsed.batchSize, parsed.rowsPerTx);
  const basePort = parsed.zero.port;
  const rawCacheURLs = parsed.cacheURLs ?? parsed.cacheURL;
  const explicitURLs = rawCacheURLs
    ? rawCacheURLs
        .split(',')
        .map(s => s.trim())
        .filter(Boolean)
    : undefined;

  const cacheURLs =
    explicitURLs && explicitURLs.length > 0
      ? explicitURLs
      : parsed.topology === 'distributed'
        ? Array.from(
            {length: parsed.numViewSyncers},
            (_, i) => `http://127.0.0.1:${basePort + 10 + i}`,
          )
        : [`http://127.0.0.1:${basePort}`];

  const isZeroManaged = explicitURLs === undefined || explicitURLs.length === 0;
  const isPgManaged = parsed.pg.url === undefined;
  const pgURL = parsed.pg.url ?? DEFAULT_PG_URL;

  return {
    runID: new Date().toISOString().replace(/[:.]/g, '-'),
    profile: parsed.profile,
    model: parsed.model,
    topology: parsed.topology,
    numViewSyncers: parsed.numViewSyncers,
    numSyncWorkers,
    users: parsed.users,
    queriesPerUser: parsed.queriesPerUser,
    rowsPerQuery: parsed.rowsPerQuery,
    writeRate: parsed.writeRate,
    batchSize: effectiveBatchSize,
    rowsPerTx: effectiveBatchSize,
    writeConcurrency: parsed.writeConcurrency,
    payloadBytes: parsed.payloadBytes,
    durationMs: parsed.durationMs,
    warmupMs: parsed.warmupMs,
    settleMs: parsed.settleMs,
    sampleIntervalMs: parsed.sampleIntervalMs,
    progressIntervalMs: parsed.progressIntervalMs,
    sloP99LagMs: parsed.sloP99LagMs,
    outputPath: parsed.output,
    logsDir: parsed.logsDir,
    profileDir: parsed.profileDir,
    profileRM: parsed.profileRM,
    profileVS: parsed.profileVS,
    processLogMode: parsed.processLogMode,
    reset: parsed.reset,
    cacheURL: cacheURLs[0],
    cacheURLs,
    pg: {
      url: pgURL,
      start: isPgManaged,
      stopAfterRun: isPgManaged && parsed.pg.stopAfterRun,
      readyTimeoutMs: parsed.pg.readyTimeoutMs,
    },
    zero: {
      ...parsed.zero,
      start: isZeroManaged,
      numSyncWorkers,
    },
  };
}

export function appPath(path: string): string {
  return isAbsolute(path) ? path : join(appRoot, path);
}

function assertPositiveNumber(name: string, value: number): void {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive number`);
  }
}

function assertPositiveInteger(name: string, value: number): void {
  assertPositiveNumber(name, value);
  if (!Number.isInteger(value)) {
    throw new Error(`${name} must be an integer`);
  }
}

function assertNonNegativeInteger(name: string, value: number): void {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
}

function assertValidAppID(appID: string): void {
  if (!APP_ID_PATTERN.test(appID)) {
    throw new Error(
      'zero.appID must contain only lowercase letters, digits, and underscores',
    );
  }
}
