import {describe, expect, test} from 'vitest';
import type {BenchmarkConfig} from './config.ts';
import type {BenchmarkDB} from './db.ts';
import {sleep} from './util.ts';
import {effectiveWriteConcurrency, FixedRateWriter} from './writer.ts';

function createMockConfig(
  overrides: Partial<BenchmarkConfig> = {},
): BenchmarkConfig {
  return {
    runID: 'test-run',
    profile: 'forum',
    model: 'hot',
    users: 1,
    queriesPerUser: 1,
    rowsPerQuery: 100,
    writeRate: 200,
    batchSize: 10,
    rowsPerTx: 10,
    writeConcurrency: 4,
    payloadBytes: 16,
    durationMs: 500,
    warmupMs: 0,
    settleMs: 0,
    sampleIntervalMs: 100,
    progressIntervalMs: 0,
    sloP99LagMs: 2000,
    sloMetric: 'client-visible',
    outputPath: 'results/test.json',
    logsDir: 'results/logs',
    profileDir: 'results/profiles',
    profileRM: false,
    profileVS: false,
    profileDurationSec: 5,
    processLogMode: 'ignore',
    reset: false,
    cleanup: false,
    resetMode: 'none',
    appServerPort: 3000,
    cacheURL: 'http://localhost:4848',
    cacheURLs: ['http://localhost:4848'],
    topology: 'single',
    numViewSyncers: 1,
    pg: {
      url: 'postgresql://localhost/test',
      start: false,
      stopAfterRun: false,
      readyTimeoutMs: 1000,
    },
    zero: {
      start: false,
      port: 4848,
      readyTimeoutMs: 1000,
      appID: 'test',
      replicaFile: 'test.db',
      logLevel: 'error',
      numSyncWorkers: 1,
      upstreamMaxConns: 10,
      cvrMaxConns: 10,
      changeMaxConns: 10,
    },
    ...overrides,
  } as unknown as BenchmarkConfig;
}

function createMockDB(
  options: {
    readonly latencyMs?: number | undefined;
    readonly onBegin?: (() => void) | undefined;
    readonly shouldFailAtTx?: number | undefined;
  } = {},
): BenchmarkDB {
  let txCount = 0;
  const mock = {
    begin: async (fn: (tx: unknown) => Promise<unknown>) => {
      txCount++;
      if (
        options.shouldFailAtTx !== undefined &&
        txCount === options.shouldFailAtTx
      ) {
        throw new Error(`Simulated DB failure at tx ${txCount}`);
      }
      if (options.latencyMs !== undefined && options.latencyMs > 0) {
        await sleep(options.latencyMs);
      }
      options.onBegin?.();
      const mockTx = Object.assign(
        (_strings: TemplateStringsArray, ..._values: unknown[]) =>
          Promise.resolve([]),
        {},
      );
      return fn(mockTx);
    },
  };
  return mock as unknown as BenchmarkDB;
}

describe('effectiveWriteConcurrency', () => {
  test('returns 1 if writeRate <= 0', () => {
    expect(effectiveWriteConcurrency(createMockConfig({writeRate: 0}))).toBe(1);
    expect(effectiveWriteConcurrency(createMockConfig({writeRate: -10}))).toBe(
      1,
    );
  });

  test('respects writeConcurrency floor when targetTxRate is low', () => {
    // 200 w/s with batch 50 = 4 tx/s. rateBased = ceil(4 * 1.0) = 4.
    // config.writeConcurrency = 16 -> max(16, 4) = 16.
    expect(
      effectiveWriteConcurrency(
        createMockConfig({writeRate: 200, batchSize: 50, writeConcurrency: 16}),
      ),
    ).toBe(16);
  });

  test('scales up concurrency using Little Law headroom when targetTxRate is high', () => {
    // 4600 w/s with batch 50 = 92 tx/s. rateBased = ceil(92 * 1.0) = 92.
    // config.writeConcurrency = 32 -> max(32, 92) = 92.
    expect(
      effectiveWriteConcurrency(
        createMockConfig({
          writeRate: 4600,
          batchSize: 50,
          writeConcurrency: 32,
        }),
      ),
    ).toBe(92);
  });

  test('respects user-supplied writeConcurrency when higher than targetTxRate', () => {
    expect(
      effectiveWriteConcurrency(
        createMockConfig({
          writeRate: 4600,
          batchSize: 50,
          writeConcurrency: 128,
        }),
      ),
    ).toBe(128);
  });

  test('clamps at 256 for extreme transaction rates', () => {
    expect(
      effectiveWriteConcurrency(
        createMockConfig({
          writeRate: 50000,
          batchSize: 1,
          writeConcurrency: 32,
        }),
      ),
    ).toBe(256);
  });
});

describe('FixedRateWriter', () => {
  test('handles zero writeRate immediately', async () => {
    const db = createMockDB();
    const config = createMockConfig({writeRate: 0});
    const writer = new FixedRateWriter(db, config);

    const stats = await writer.run(10);
    expect(stats.committedRows).toBe(0);
    expect(stats.committedTransactions).toBe(0);
    expect(stats.highestCommittedSeq).toBe(0);
    expect(writer.highestCommittedSeq).toBe(0);
  });

  test('dispatches batches at target rate and tracks sequence numbers', async () => {
    const db = createMockDB();
    // 200 w/s, batch 20 = 10 tx/s (1 tx every 100ms)
    // 350ms duration -> tx 0 (0ms), tx 1 (100ms), tx 2 (200ms), tx 3 (300ms) = 4 txs = 80 rows
    const config = createMockConfig({
      writeRate: 200,
      batchSize: 20,
      writeConcurrency: 4,
    });
    const writer = new FixedRateWriter(db, config);

    const stats = await writer.run(350);
    expect(stats.committedTransactions).toBeGreaterThanOrEqual(3);
    expect(stats.committedTransactions).toBeLessThanOrEqual(5);
    expect(stats.committedRows).toBe(stats.committedTransactions * 20);
    expect(stats.highestCommittedSeq).toBe(stats.committedRows);
    expect(writer.highestCommittedSeq).toBe(stats.committedRows);
    expect(stats.transactionLatencyMs.length).toBe(stats.committedTransactions);
  });

  test('maintains target dispatch rate even with non-trivial transaction latency', async () => {
    // 50ms simulated DB latency
    // 100 w/s, batch 10 = 10 tx/s (1 tx every 100ms)
    // 450ms duration -> ~4-5 transactions
    const db = createMockDB({latencyMs: 50});
    const config = createMockConfig({
      writeRate: 100,
      batchSize: 10,
      writeConcurrency: 4,
    });
    const writer = new FixedRateWriter(db, config);

    const stats = await writer.run(450);
    expect(stats.committedTransactions).toBeGreaterThanOrEqual(4);
    expect(stats.committedRows).toBe(stats.committedTransactions * 10);
    expect(stats.transactionLatencyMs.every(lat => lat >= 45)).toBe(true);
  });

  test('propagates errors and cleans up in-flight transactions on failure', async () => {
    const db = createMockDB({shouldFailAtTx: 2});
    const config = createMockConfig({
      writeRate: 500,
      batchSize: 10,
      writeConcurrency: 4,
    });
    const writer = new FixedRateWriter(db, config);

    await expect(writer.run(500)).rejects.toThrow(
      'Simulated DB failure at tx 2',
    );
  });
});
