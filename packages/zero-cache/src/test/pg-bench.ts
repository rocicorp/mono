import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../zqlite/src/db.ts';
import type {PostgresDB, PostgresTransaction} from '../types/pg.ts';
import type {RowValue} from '../types/row-key.ts';
import {id} from '../types/sql.ts';

export const BENCHMARK_FIXTURE_PUBLICATION = 'zero_bench';
export const BENCHMARK_FIXTURE_TABLES = [
  'bench_rows',
  'bench_lookup',
  'bench_wide',
  'bench_composite',
] as const;
export const BENCHMARK_FIXTURE_TABLE_KEYS = {
  bench_rows: 'id',
  bench_lookup: 'id',
  bench_wide: 'id',
  bench_composite: ['account_id', 'seq'],
} satisfies Record<string, string | string[]>;

export type BenchmarkFixtureTable = (typeof BENCHMARK_FIXTURE_TABLES)[number];

export type BenchmarkFixture = {
  publication: string;
  totalRows: number;
};

type BenchRowsRow = RowValue & {
  id: number;
  payload: string;
};

type BenchLookupRow = RowValue & {
  id: number;
  label: string;
  active: boolean;
};

type BenchWideRow = RowValue & {
  id: number;
  group_id: number;
  title: string;
  body: string;
  extra: string;
};

type BenchCompositeRow = RowValue & {
  account_id: number;
  seq: number;
  payload: string;
  amount: number;
};

type BenchmarkFixtureRowsByTable = {
  bench_rows: BenchRowsRow;
  bench_lookup: BenchLookupRow;
  bench_wide: BenchWideRow;
  bench_composite: BenchCompositeRow;
};

export type BenchmarkFixtureRow = {
  [Table in BenchmarkFixtureTable]: {
    table: Table;
    row: BenchmarkFixtureRowsByTable[Table];
  };
}[BenchmarkFixtureTable];

type RowBatch = {
  benchRows: BenchRowsRow[];
  lookupRows: BenchLookupRow[];
  wideRows: BenchWideRow[];
  compositeRows: BenchCompositeRow[];
};

// Creates deterministic variable-size ASCII text. Lengths are approximately
// uniform across the inclusive byte range for each payload column.
function makePayload(id: number, minBytes: number, maxBytes: number) {
  const range = maxBytes - minBytes + 1;
  const length = minBytes + ((id * 9973) % range);
  const seed = Math.imul(id, 2654435761) >>> 0;
  const chunk = `${id.toString(36)}:${seed.toString(36)}:abcdefghijklmnopqrstuvwxyz0123456789:`;
  return chunk.repeat(Math.ceil(length / chunk.length)).slice(0, length);
}

// The fixture uses id % 10 so every benchmark run gets the same table mix:
// 50% bench_rows: id primary key, payload 256-2048 B, ~1152 B average.
// 20% bench_wide: id primary key, group_id 0-127 indexed, short title,
// body 2048-8192 B (~5120 B average), extra 512-2048 B (~1280 B average).
// 20% bench_composite: (account_id, seq) primary key, payload 128-1024 B,
// ~576 B average, amount 0-99_999 indexed.
// 10% bench_lookup: id primary key, label 8-11 B, active boolean indexed.
function makeBenchmarkFixtureRow(id: number): BenchmarkFixtureRow {
  const kind = id % 10;

  if (kind < 5) {
    return {
      table: 'bench_rows',
      row: {id, payload: makePayload(id, 256, 2048)},
    };
  }
  if (kind < 7) {
    return {
      table: 'bench_wide',
      row: {
        id,
        group_id: id % 128,
        title: `wide row ${id}`,
        body: makePayload(id, 2048, 8192),
        extra: makePayload(id + 17, 512, 2048),
      },
    };
  }
  if (kind < 9) {
    return {
      table: 'bench_composite',
      row: {
        account_id: id % 1_000,
        seq: Math.floor(id / 1_000),
        payload: makePayload(id, 128, 1024),
        amount: id % 100_000,
      },
    };
  }
  return {
    table: 'bench_lookup',
    row: {
      id,
      label: `lookup-${id % 10_000}`,
      active: id % 2 === 0,
    },
  };
}

function makeBatch(startID: number, count: number): RowBatch {
  const batch: RowBatch = {
    benchRows: [],
    lookupRows: [],
    wideRows: [],
    compositeRows: [],
  };

  for (let i = 0; i < count; i++) {
    const fixtureRow = makeBenchmarkFixtureRow(startID + i);
    switch (fixtureRow.table) {
      case 'bench_rows':
        batch.benchRows.push(fixtureRow.row);
        break;
      case 'bench_lookup':
        batch.lookupRows.push(fixtureRow.row);
        break;
      case 'bench_wide':
        batch.wideRows.push(fixtureRow.row);
        break;
      case 'bench_composite':
        batch.compositeRows.push(fixtureRow.row);
        break;
    }
  }

  return batch;
}

export function makeBenchmarkFixtureRows(
  startID: number,
  count: number,
): BenchmarkFixtureRow[] {
  return Array.from({length: count}, (_, i) =>
    makeBenchmarkFixtureRow(startID + i),
  );
}

export function makeBenchmarkFixtureRowBatches(
  startID: number,
  count: number,
  rowsPerTransaction: number,
): RowBatch[] {
  const batches: RowBatch[] = [];
  for (let offset = 0; offset < count; offset += rowsPerTransaction) {
    batches.push(
      makeBatch(startID + offset, Math.min(rowsPerTransaction, count - offset)),
    );
  }
  return batches;
}

export async function createBenchmarkFixtureSchema(
  upstream: PostgresDB,
  publication = BENCHMARK_FIXTURE_PUBLICATION,
) {
  await upstream.unsafe(`
    CREATE TABLE bench_rows(
      id INTEGER PRIMARY KEY,
      payload TEXT NOT NULL
    );

    CREATE TABLE bench_lookup(
      id INTEGER PRIMARY KEY,
      label TEXT NOT NULL,
      active BOOLEAN NOT NULL
    );

    CREATE TABLE bench_wide(
      id INTEGER PRIMARY KEY,
      group_id INTEGER NOT NULL,
      title TEXT NOT NULL,
      body TEXT NOT NULL,
      extra TEXT NOT NULL
    );

    CREATE TABLE bench_composite(
      account_id INTEGER NOT NULL,
      seq INTEGER NOT NULL,
      payload TEXT NOT NULL,
      amount INTEGER NOT NULL,
      PRIMARY KEY (account_id, seq)
    );

    CREATE INDEX bench_lookup_active_idx ON bench_lookup(active);
    CREATE INDEX bench_wide_group_idx ON bench_wide(group_id);
    CREATE INDEX bench_composite_amount_idx ON bench_composite(amount);

    CREATE PUBLICATION ${id(publication)}
      FOR TABLE bench_rows, bench_lookup, bench_wide, bench_composite;
  `);
}

export async function insertBenchmarkFixtureRows(
  upstream: PostgresDB,
  startID: number,
  count: number,
  rowsPerTransaction: number,
) {
  for (let offset = 0; offset < count; offset += rowsPerTransaction) {
    const batch = makeBatch(
      startID + offset,
      Math.min(rowsPerTransaction, count - offset),
    );
    await upstream.begin(async tx => {
      await insertBenchmarkFixtureBatch(tx, batch);
    });
  }
}

export async function insertBenchmarkFixtureRowBatches(
  upstream: PostgresDB,
  batches: readonly RowBatch[],
) {
  for (const batch of batches) {
    await upstream.begin(async tx => {
      await insertBenchmarkFixtureBatch(tx, batch);
    });
  }
}

async function insertBenchmarkFixtureBatch(
  tx: PostgresTransaction,
  batch: RowBatch,
) {
  if (batch.benchRows.length > 0) {
    await tx`INSERT INTO bench_rows ${tx(batch.benchRows)}`;
  }
  if (batch.lookupRows.length > 0) {
    await tx`INSERT INTO bench_lookup ${tx(batch.lookupRows)}`;
  }
  if (batch.wideRows.length > 0) {
    await tx`INSERT INTO bench_wide ${tx(batch.wideRows)}`;
  }
  if (batch.compositeRows.length > 0) {
    await tx`INSERT INTO bench_composite ${tx(batch.compositeRows)}`;
  }
}

export async function setupBenchmarkFixture(
  upstream: PostgresDB,
  {
    publication = BENCHMARK_FIXTURE_PUBLICATION,
    rows = 0,
    rowsPerTransaction = 500,
  }: {
    publication?: string;
    rows?: number;
    rowsPerTransaction?: number;
  } = {},
): Promise<BenchmarkFixture> {
  await createBenchmarkFixtureSchema(upstream, publication);
  if (rows > 0) {
    await insertBenchmarkFixtureRows(upstream, 1, rows, rowsPerTransaction);
  }
  return {publication, totalRows: rows};
}

export function benchmarkFixtureReplicaRowCount(
  lc: LogContext,
  replicaPath: string,
): number {
  const db = new Database(lc, replicaPath, {readonly: true});
  try {
    let total = 0;
    for (const table of BENCHMARK_FIXTURE_TABLES) {
      const row = db
        .prepare(`SELECT count(*) AS n FROM ${id(table)}`)
        .get<{n: number}>();
      total += row.n;
    }
    return total;
  } finally {
    db.close();
  }
}
