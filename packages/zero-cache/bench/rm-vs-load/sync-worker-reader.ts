import {performance} from 'node:perf_hooks';
import {parentPort, workerData} from 'node:worker_threads';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {sleep} from './perf-utils.ts';
import type {SyncWorkerData, SyncWorkerStats} from './types.ts';

if (parentPort === null) {
  throw new Error('sync-worker-reader must run inside a worker_thread');
}

const workerPort = parentPort;
const config = workerData as SyncWorkerData;
const lc = createSilentLogContext();
let active = true;
let reads = 0;
let totalReadMs = 0;
let maxReadMs = 0;
let errors = 0;

workerPort.on('message', msg => {
  if (typeof msg === 'object' && msg !== null && 'type' in msg) {
    switch (msg.type) {
      case 'stop':
        active = false;
        break;
      case 'stats':
        postStats();
        break;
    }
  }
});

try {
  await run();
  workerPort.postMessage({type: 'done', stats: stats()});
  workerPort.close();
} catch (e) {
  const error = e instanceof Error ? e : new Error(String(e));
  workerPort.postMessage({
    type: 'error',
    error: {message: error.message, stack: error.stack},
  });
  workerPort.close();
}

async function run() {
  const replica = new Database(lc, config.sqlitePath);
  try {
    replica.pragma('query_only = ON');
    replica.pragma('busy_timeout = 30000');
    const read = replica.prepare(
      'SELECT count(*) AS count, max(tx) AS maxTx FROM "bench_rows"',
    );
    const interval = setInterval(postStats, 25);
    interval.unref();
    workerPort.postMessage({type: 'ready'});

    try {
      while (active) {
        const start = performance.now();
        try {
          read.get<{count: number; maxTx: number | null}>();
        } catch {
          errors++;
        }
        const elapsed = performance.now() - start;
        reads++;
        totalReadMs += elapsed;
        maxReadMs = Math.max(maxReadMs, elapsed);
        if (config.readDelayMs > 0) {
          await sleep(config.readDelayMs);
        }
      }
    } finally {
      clearInterval(interval);
    }
  } finally {
    replica.close();
  }
}

function postStats() {
  workerPort.postMessage({type: 'stats', stats: stats()});
}

function stats(): SyncWorkerStats {
  return {
    reads,
    totalReadMs,
    maxReadMs,
    errors,
  };
}
