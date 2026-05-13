/* oxlint-disable no-console */
import {performance} from 'node:perf_hooks';
import {deepEqual} from '../../shared/src/json.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {
  RowID,
  RowRecord,
} from '../src/services/view-syncer/schema/types.ts';

type Result = {
  readonly mode: string;
  readonly rows: number;
  readonly queuedRows: number;
  readonly clearedRows: number;
  readonly elapsedMs: number;
  readonly rowsPerSec: number;
};

type Summary = {
  readonly name: 'cvr-noop-row-suppression';
  readonly generatedAt: string;
  readonly rows: number;
  readonly results: Result[];
};

const rows = envInt('ZERO_CVR_NOOP_ROWS', 100_000);
const existing = Array.from({length: rows}, (_, i) => makeRowRecord(i));
const candidates = existing.map(row => ({...row}));

const results = [
  runBaseline(existing, candidates),
  runEarlySuppression(existing, candidates),
];

for (const result of results) {
  console.log(
    `${result.mode}: ${formatRate(result.rowsPerSec)} rows/s | ` +
      `${result.elapsedMs.toFixed(1)} ms | queued=${result.queuedRows} | ` +
      `cleared=${result.clearedRows}`,
  );
}

const summary: Summary = {
  name: 'cvr-noop-row-suppression',
  generatedAt: new Date().toISOString(),
  rows,
  results,
};
console.log(JSON.stringify(summary));

function runBaseline(
  existing: readonly RowRecord[],
  candidates: readonly RowRecord[],
): Result {
  const pending = new Map<string, RowRecord>();
  const start = performance.now();
  for (let i = 0; i < candidates.length; i++) {
    const candidate = candidates[i];
    pending.set(rowKey(candidate.id), candidate);
  }
  let queuedRows = pending.size;
  for (let i = 0; i < existing.length; i++) {
    const id = rowKey(existing[i].id);
    const candidate = pending.get(id);
    if (
      candidate &&
      deepEqual(
        candidate as ReadonlyJSONValue,
        existing[i] as ReadonlyJSONValue,
      )
    ) {
      pending.delete(id);
    }
  }
  queuedRows -= pending.size;
  return result('baseline enqueue then flush-drop', rows, queuedRows, 0, start);
}

function runEarlySuppression(
  existing: readonly RowRecord[],
  candidates: readonly RowRecord[],
): Result {
  const pending = new Map<string, RowRecord>();
  let clearedRows = 0;
  const start = performance.now();
  for (let i = 0; i < candidates.length; i++) {
    const candidate = candidates[i];
    if (
      !deepEqual(
        candidate as ReadonlyJSONValue,
        existing[i] as ReadonlyJSONValue,
      )
    ) {
      pending.set(rowKey(candidate.id), candidate);
    } else {
      pending.delete(rowKey(candidate.id));
      clearedRows++;
    }
  }
  return result(
    'early no-op suppression',
    rows,
    pending.size,
    clearedRows,
    start,
  );
}

function makeRowRecord(i: number): RowRecord {
  return {
    id: {
      schema: 'public',
      table: 'issues',
      rowKey: {id: String(i)},
    },
    rowVersion: '03',
    patchVersion: {stateVersion: '1a0'},
    refCounts: {oneHash: 1},
  };
}

function rowKey(id: RowID): string {
  return `${id.schema}.${id.table}:${JSON.stringify(id.rowKey)}`;
}

function result(
  mode: string,
  rows: number,
  queuedRows: number,
  clearedRows: number,
  start: number,
): Result {
  const elapsedMs = performance.now() - start;
  return {
    mode,
    rows,
    queuedRows,
    clearedRows,
    elapsedMs,
    rowsPerSec: rows / (elapsedMs / 1000),
  };
}

function envInt(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined || value === '') {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid integer ${name}=${value}`);
  }
  return parsed;
}

function formatRate(value: number): string {
  return value.toLocaleString('en-US', {maximumFractionDigits: 1});
}
