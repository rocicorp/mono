import '../../../packages/shared/src/dotenv.ts';

import {isAbsolute, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {parseOptions} from '../../../packages/shared/src/options.ts';
import * as v from '../../../packages/shared/src/valita.ts';

export const appRoot = fileURLToPath(new URL('..', import.meta.url));
export const repoRoot = fileURLToPath(new URL('../../..', import.meta.url));

const DEFAULT_PG_URL = 'postgresql://user:password@127.0.0.1:6436/postgres';
const APP_ID_PATTERN = /^[a-z0-9_]+$/;

const options = {
  benchmark: v.literalUnion('throughput', 'recovery').default('throughput'),
  profile: v
    .literalUnion('feed-append', 'email', 'forum', 'relational')
    .default('feed-append'),
  model: v.literalUnion('hot', 'realistic').default('hot'),

  users: v.number().default(1),
  queriesPerUser: v.number().default(1),
  rowsPerQuery: v.number().default(100),
  writeRate: v.number().default(100),
  batchSize: v.number().default(1),
  payloadBytes: v.number().default(256),
  durationMs: v.number().default(30_000),
  warmupMs: v.number().default(10_000),
  settleMs: v.number().default(5_000),
  sampleIntervalMs: v.number().default(1_000),
  progressIntervalMs: v.number().default(5_000),
  sloP99LagMs: v.number().default(2_000),
  output: v.string().default('results/latest.json'),
  logsDir: v.string().default('results/logs'),
  processLogMode: v.literalUnion('file', 'inherit', 'ignore').default('file'),
  reset: v.boolean().default(true),
  cacheURL: v.string().optional(),

  recovery: {
    timeoutMs: v.number().default(60_000),
    stableMs: v.number().default(2_000),
    pollMs: v.number().default(100),
    minSeqLag: v.number().default(1),
    minPipelineResets: v.number().default(1),
  },

  pg: {
    url: v.string().default(DEFAULT_PG_URL),
    start: v.boolean().default(true),
    stopAfterRun: v.boolean().default(true),
    readyTimeoutMs: v.number().default(60_000),
  },

  zero: {
    start: v.boolean().default(true),
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
export type BenchmarkKind = 'throughput' | 'recovery';

export type BenchmarkConfig = {
  readonly runID: string;
  readonly benchmark: BenchmarkKind;
  readonly profile: BenchmarkProfile;
  readonly model: BenchmarkModel;
  readonly users: number;
  readonly queriesPerUser: number;
  readonly rowsPerQuery: number;
  readonly writeRate: number;
  readonly batchSize: number;
  readonly payloadBytes: number;
  readonly durationMs: number;
  readonly warmupMs: number;
  readonly settleMs: number;
  readonly sampleIntervalMs: number;
  readonly progressIntervalMs: number;
  readonly sloP99LagMs: number;
  readonly outputPath: string;
  readonly logsDir: string;
  readonly processLogMode: 'file' | 'inherit' | 'ignore';
  readonly reset: boolean;
  readonly cacheURL: string;
  readonly recovery: {
    readonly timeoutMs: number;
    readonly stableMs: number;
    readonly pollMs: number;
    readonly minSeqLag: number;
    readonly minPipelineResets: number;
  };
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
  assertNonNegativeInteger('payloadBytes', parsed.payloadBytes);
  assertPositiveInteger('durationMs', parsed.durationMs);
  assertNonNegativeInteger('warmupMs', parsed.warmupMs);
  assertNonNegativeInteger('settleMs', parsed.settleMs);
  assertPositiveInteger('sampleIntervalMs', parsed.sampleIntervalMs);
  assertNonNegativeInteger('progressIntervalMs', parsed.progressIntervalMs);
  assertPositiveInteger('sloP99LagMs', parsed.sloP99LagMs);
  assertPositiveInteger('recovery.timeoutMs', parsed.recovery.timeoutMs);
  assertNonNegativeInteger('recovery.stableMs', parsed.recovery.stableMs);
  assertPositiveInteger('recovery.pollMs', parsed.recovery.pollMs);
  assertNonNegativeInteger('recovery.minSeqLag', parsed.recovery.minSeqLag);
  assertNonNegativeInteger(
    'recovery.minPipelineResets',
    parsed.recovery.minPipelineResets,
  );
  assertValidAppID(parsed.zero.appID);
  if (parsed.benchmark === 'recovery' && parsed.model !== 'hot') {
    throw new Error(
      'recovery benchmark currently requires --model hot so global seq lag represents every client group',
    );
  }

  return {
    runID: new Date().toISOString().replace(/[:.]/g, '-'),
    benchmark: parsed.benchmark,
    profile: parsed.profile,
    model: parsed.model,
    users: parsed.users,
    queriesPerUser: parsed.queriesPerUser,
    rowsPerQuery: parsed.rowsPerQuery,
    writeRate: parsed.writeRate,
    batchSize: parsed.batchSize,
    payloadBytes: parsed.payloadBytes,
    durationMs: parsed.durationMs,
    warmupMs: parsed.warmupMs,
    settleMs: parsed.settleMs,
    sampleIntervalMs: parsed.sampleIntervalMs,
    progressIntervalMs: parsed.progressIntervalMs,
    sloP99LagMs: parsed.sloP99LagMs,
    outputPath: parsed.output,
    logsDir: parsed.logsDir,
    processLogMode: parsed.processLogMode,
    reset: parsed.reset,
    cacheURL: parsed.cacheURL ?? `http://127.0.0.1:${parsed.zero.port}`,
    recovery: parsed.recovery,
    pg: parsed.pg,
    zero: parsed.zero,
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
