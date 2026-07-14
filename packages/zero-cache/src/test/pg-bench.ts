import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../zqlite/src/db.ts';
import {listIndexes} from '../db/lite-tables.ts';
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
export const BENCHMARK_FIXTURE_BYTES_PER_MB = 1_000_000;

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

export type InitialSyncBenchmarkFixture =
  | {fixture: 'mixed'; rows: number}
  | {
      fixture: 'wide-text' | 'large-payload';
      rows: number;
      payloadBytes: number;
    };

export type InitialSyncBenchmarkValidation = {
  rowCounts: Readonly<Record<string, number>>;
  totalRows: number;
  indexes: readonly NormalizedBenchmarkIndex[];
  samplesValidated: number;
};

type NormalizedBenchmarkIndex = {
  name: string;
  tableName: string;
  unique: boolean;
  columns: readonly (readonly [string, 'ASC' | 'DESC'])[];
};

// Creates deterministic variable-size ASCII text. Lengths are approximately
// uniform across the inclusive byte range for each payload column.
function makePayload(id: number, minBytes: number, maxBytes: number) {
  const length = payloadLength(id, minBytes, maxBytes);
  const seed = Math.imul(id, 2654435761) >>> 0;
  const chunk = `${id.toString(36)}:${seed.toString(36)}:abcdefghijklmnopqrstuvwxyz0123456789:`;
  return chunk.repeat(Math.ceil(length / chunk.length)).slice(0, length);
}

function payloadLength(id: number, minBytes: number, maxBytes: number) {
  const range = maxBytes - minBytes + 1;
  return minBytes + ((id * 9973) % range);
}

function benchmarkFixtureRowPayloadBytes(id: number) {
  const kind = id % 10;

  if (kind < 5) {
    return payloadLength(id, 256, 2048);
  }
  if (kind < 7) {
    return (
      `wide row ${id}`.length +
      payloadLength(id, 2048, 8192) +
      payloadLength(id + 17, 512, 2048)
    );
  }
  if (kind < 9) {
    return payloadLength(id, 128, 1024);
  }
  return `lookup-${id % 10_000}`.length;
}

export function benchmarkFixturePayloadBytes(startID: number, count: number) {
  let total = 0;
  for (let i = 0; i < count; i++) {
    total += benchmarkFixtureRowPayloadBytes(startID + i);
  }
  return total;
}

// Decimal MB of generated text payload, not logical-replication or WAL bytes.
export function benchmarkFixturePayloadMB(startID: number, count: number) {
  return (
    benchmarkFixturePayloadBytes(startID, count) /
    BENCHMARK_FIXTURE_BYTES_PER_MB
  );
}

const PRODUCTION_PAYLOAD_PATTERN =
  'production-shaped-copy-payload-0123456789abcdef';

export function benchmarkProductionPayload(
  fixture: 'wide-text' | 'large-payload',
  row: number,
  payloadBytes: number,
) {
  const prefix = `${fixture}:${row}:`;
  return (
    prefix +
    PRODUCTION_PAYLOAD_PATTERN.repeat(
      Math.max(
        0,
        Math.ceil(
          (payloadBytes - prefix.length) / PRODUCTION_PAYLOAD_PATTERN.length,
        ),
      ),
    )
  ).slice(0, payloadBytes);
}

export function initialSyncBenchmarkPayloadBytes(
  fixture: InitialSyncBenchmarkFixture,
) {
  return fixture.fixture === 'mixed'
    ? benchmarkFixturePayloadBytes(1, fixture.rows)
    : fixture.rows * fixture.payloadBytes;
}

export function initialSyncBenchmarkPayloadMB(
  fixture: InitialSyncBenchmarkFixture,
) {
  return (
    initialSyncBenchmarkPayloadBytes(fixture) / BENCHMARK_FIXTURE_BYTES_PER_MB
  );
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

export async function setupInitialSyncBenchmarkFixture(
  upstream: PostgresDB,
  fixture: InitialSyncBenchmarkFixture,
  publication = BENCHMARK_FIXTURE_PUBLICATION,
) {
  if (fixture.fixture === 'mixed') {
    await setupBenchmarkFixture(upstream, {
      publication,
      rows: fixture.rows,
    });
    return;
  }

  await upstream.unsafe(/* sql */ `
    CREATE TABLE bench_payload(payload TEXT NOT NULL);
    INSERT INTO bench_payload
    SELECT left(
      repeat('${PRODUCTION_PAYLOAD_PATTERN}',
        ceil(${fixture.payloadBytes}::numeric / ${PRODUCTION_PAYLOAD_PATTERN.length})::integer),
      ${fixture.payloadBytes}
    );
  `);

  if (fixture.fixture === 'wide-text') {
    await createWideTextBenchmarkFixture(
      upstream,
      fixture.rows,
      fixture.payloadBytes,
    );
    await upstream.unsafe(
      `CREATE PUBLICATION ${id(publication)} FOR TABLE public.bench_wide_text`,
    );
  } else {
    await createLargePayloadBenchmarkFixture(
      upstream,
      fixture.rows,
      fixture.payloadBytes,
    );
    await upstream.unsafe(
      `CREATE PUBLICATION ${id(publication)} FOR TABLE benchmark.bench_large_payload`,
    );
  }
}

async function createWideTextBenchmarkFixture(
  upstream: PostgresDB,
  rows: number,
  payloadBytes: number,
) {
  await upstream.unsafe(/* sql */ `
    CREATE TABLE bench_wide_text(
      attributes JSONB,
      labels JSONB,
      tags JSONB,
      created_at TIMESTAMPTZ NOT NULL,
      event_at TIMESTAMPTZ NOT NULL,
      properties JSONB,
      formatted_text TEXT,
      id TEXT PRIMARY KEY,
      is_inactive BOOLEAN NOT NULL,
      external_id TEXT,
      reference_a TEXT,
      reference_b TEXT,
      summary TEXT,
      large_text TEXT,
      source_type TEXT,
      title TEXT,
      plain_text TEXT,
      group_id TEXT NOT NULL,
      targets JSONB,
      category TEXT,
      unique_ref TEXT,
      updated_at TIMESTAMPTZ NOT NULL,
      bucket_id TEXT NOT NULL,
      version BIGINT NOT NULL,
      partition_id TEXT NOT NULL
    );
    ALTER TABLE bench_wide_text ALTER COLUMN large_text SET STORAGE EXTENDED;
    ALTER TABLE bench_wide_text ALTER COLUMN large_text SET COMPRESSION pglz;

    INSERT INTO bench_wide_text
    SELECT
      jsonb_build_array(jsonb_build_object('name', 'attribute-' || g)),
      jsonb_build_array('label-' || g),
      jsonb_build_array('tag-' || g),
      timestamptz '2025-01-01' + g * interval '1 second',
      timestamptz '2025-01-01' + g * interval '1 second',
      jsonb_build_object('key', 'value-' || g),
      '<p>Record ' || g || '</p>',
      'wide-text-' || g,
      g % 17 = 0,
      'external-' || g,
      'reference-a-' || g,
      'reference-b-' || g,
      'summary-' || g,
      left('wide-text:' || g || ':' || p.payload, ${payloadBytes}),
      (ARRAY['stream', 'batch'])[1 + (g % 2)],
      'Title ' || g,
      'Plain text ' || g,
      'group-' || (g % 100),
      jsonb_build_array('target-' || g),
      (ARRAY['primary', 'secondary'])[1 + (g % 2)],
      'unique-ref-' || g,
      timestamptz '2025-01-01' + g * interval '1 second',
      'bucket-' || (g % 100),
      g,
      'partition-' || (g % 10)
    FROM generate_series(1, ${rows}) g
    CROSS JOIN bench_payload p;

    CREATE INDEX bench_wide_text_group_created_id_idx
      ON bench_wide_text(group_id, created_at, id);
    CREATE INDEX bench_wide_text_group_id_idx
      ON bench_wide_text(group_id, id);
    CREATE INDEX bench_wide_text_partition_id_idx
      ON bench_wide_text(partition_id, id);
  `);
}

async function createLargePayloadBenchmarkFixture(
  upstream: PostgresDB,
  rows: number,
  payloadBytes: number,
) {
  await upstream.unsafe(/* sql */ `
    CREATE SCHEMA benchmark;
    CREATE TABLE benchmark.bench_large_payload(
      created_at TIMESTAMPTZ NOT NULL,
      id TEXT PRIMARY KEY,
      payload JSONB NOT NULL,
      format TEXT NOT NULL,
      source_type TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      partition_id TEXT NOT NULL
    );
    ALTER TABLE benchmark.bench_large_payload
      ALTER COLUMN payload SET STORAGE EXTENDED;
    ALTER TABLE benchmark.bench_large_payload
      ALTER COLUMN payload SET COMPRESSION pglz;

    INSERT INTO benchmark.bench_large_payload
    SELECT
      timestamptz '2025-01-01' + g * interval '1 second',
      'payload-' || g,
      jsonb_build_object(
        'data', left('large-payload:' || g || ':' || p.payload, ${payloadBytes})
      ),
      'v' || (1 + g % 2),
      CASE g % 3 WHEN 0 THEN 'stream' WHEN 1 THEN 'batch' ELSE 'snapshot' END,
      timestamptz '2025-01-01' + g * interval '1 second',
      'partition-' || (g % 100)
    FROM generate_series(1, ${rows}) g
    CROSS JOIN bench_payload p;

    CREATE INDEX bench_large_payload_format_idx
      ON benchmark.bench_large_payload(format);
    CREATE INDEX bench_large_payload_partition_idx
      ON benchmark.bench_large_payload(partition_id);
    CREATE INDEX bench_large_payload_partition_id_idx
      ON benchmark.bench_large_payload(partition_id, id);
    CREATE INDEX bench_large_payload_partition_source_idx
      ON benchmark.bench_large_payload(partition_id, source_type);
    CREATE INDEX bench_large_payload_partition_source_created_idx
      ON benchmark.bench_large_payload(partition_id, source_type, created_at DESC);
  `);
}

export function validateInitialSyncBenchmarkReplica(
  lc: LogContext,
  replicaPath: string,
  fixture: InitialSyncBenchmarkFixture,
): InitialSyncBenchmarkValidation {
  const db = new Database(lc, replicaPath, {readonly: true});
  try {
    const expectedCounts = expectedRowCounts(fixture);
    const rowCounts = Object.fromEntries(
      Object.keys(expectedCounts).map(table => [
        table,
        db.prepare(`SELECT count(*) AS n FROM ${id(table)}`).get<{n: number}>()
          .n,
      ]),
    );
    assertExact('row counts', rowCounts, expectedCounts);

    const tableNames = new Set(Object.keys(expectedCounts));
    const indexes = listIndexes(db)
      .filter(index => tableNames.has(index.tableName))
      .map(({name, tableName, unique, columns}) => ({
        name,
        tableName,
        unique,
        columns: Object.entries(columns),
      }))
      .sort((a, b) => a.name.localeCompare(b.name));
    assertExact('indexes', indexes, expectedIndexes(fixture));

    const sampleRows =
      fixture.fixture === 'mixed'
        ? mixedSampleRows(fixture.rows)
        : [...new Set([1, Math.ceil(fixture.rows / 2), fixture.rows])];
    for (const row of sampleRows) {
      if (fixture.fixture === 'mixed') {
        validateMixedSample(db, row);
      } else {
        validateLargeSample(db, fixture, row);
      }
    }

    return {
      rowCounts,
      totalRows: Object.values(rowCounts).reduce(
        (sum, count) => sum + count,
        0,
      ),
      indexes,
      samplesValidated: sampleRows.length,
    };
  } finally {
    db.close();
  }
}

function expectedRowCounts(
  fixture: InitialSyncBenchmarkFixture,
): Readonly<Record<string, number>> {
  if (fixture.fixture === 'wide-text') {
    return {bench_wide_text: fixture.rows};
  }
  if (fixture.fixture === 'large-payload') {
    return {'benchmark.bench_large_payload': fixture.rows};
  }

  const counts: Record<BenchmarkFixtureTable, number> = {
    bench_rows: 0,
    bench_lookup: 0,
    bench_wide: 0,
    bench_composite: 0,
  };
  for (let row = 1; row <= fixture.rows; row++) {
    counts[makeBenchmarkFixtureRow(row).table]++;
  }
  return Object.fromEntries(
    Object.entries(counts).sort(([a], [b]) => a.localeCompare(b)),
  );
}

function expectedIndexes(fixture: InitialSyncBenchmarkFixture) {
  switch (fixture.fixture) {
    case 'mixed':
      return [
        benchmarkIndex('bench_composite_amount_idx', 'bench_composite', false, [
          ['amount', 'ASC'],
        ]),
        benchmarkIndex('bench_composite_pkey', 'bench_composite', true, [
          ['account_id', 'ASC'],
          ['seq', 'ASC'],
        ]),
        benchmarkIndex('bench_lookup_active_idx', 'bench_lookup', false, [
          ['active', 'ASC'],
        ]),
        benchmarkIndex('bench_lookup_pkey', 'bench_lookup', true, [
          ['id', 'ASC'],
        ]),
        benchmarkIndex('bench_rows_pkey', 'bench_rows', true, [['id', 'ASC']]),
        benchmarkIndex('bench_wide_group_idx', 'bench_wide', false, [
          ['group_id', 'ASC'],
        ]),
        benchmarkIndex('bench_wide_pkey', 'bench_wide', true, [['id', 'ASC']]),
      ];
    case 'wide-text':
      return [
        benchmarkIndex(
          'bench_wide_text_group_created_id_idx',
          'bench_wide_text',
          false,
          [
            ['group_id', 'ASC'],
            ['created_at', 'ASC'],
            ['id', 'ASC'],
          ],
        ),
        benchmarkIndex(
          'bench_wide_text_group_id_idx',
          'bench_wide_text',
          false,
          [
            ['group_id', 'ASC'],
            ['id', 'ASC'],
          ],
        ),
        benchmarkIndex(
          'bench_wide_text_partition_id_idx',
          'bench_wide_text',
          false,
          [
            ['partition_id', 'ASC'],
            ['id', 'ASC'],
          ],
        ),
        benchmarkIndex('bench_wide_text_pkey', 'bench_wide_text', true, [
          ['id', 'ASC'],
        ]),
      ];
    case 'large-payload':
      return [
        benchmarkIndex(
          'benchmark.bench_large_payload_format_idx',
          'benchmark.bench_large_payload',
          false,
          [['format', 'ASC']],
        ),
        benchmarkIndex(
          'benchmark.bench_large_payload_partition_id_idx',
          'benchmark.bench_large_payload',
          false,
          [
            ['partition_id', 'ASC'],
            ['id', 'ASC'],
          ],
        ),
        benchmarkIndex(
          'benchmark.bench_large_payload_partition_idx',
          'benchmark.bench_large_payload',
          false,
          [['partition_id', 'ASC']],
        ),
        benchmarkIndex(
          'benchmark.bench_large_payload_partition_source_created_idx',
          'benchmark.bench_large_payload',
          false,
          [
            ['partition_id', 'ASC'],
            ['source_type', 'ASC'],
            ['created_at', 'DESC'],
          ],
        ),
        benchmarkIndex(
          'benchmark.bench_large_payload_partition_source_idx',
          'benchmark.bench_large_payload',
          false,
          [
            ['partition_id', 'ASC'],
            ['source_type', 'ASC'],
          ],
        ),
        benchmarkIndex(
          'benchmark.bench_large_payload_pkey',
          'benchmark.bench_large_payload',
          true,
          [['id', 'ASC']],
        ),
      ];
  }
}

function benchmarkIndex(
  name: string,
  tableName: string,
  unique: boolean,
  columns: readonly (readonly [string, 'ASC' | 'DESC'])[],
): NormalizedBenchmarkIndex {
  return {name, tableName, unique, columns};
}

function mixedSampleRows(rows: number) {
  const samples = new Set<number>();
  for (const table of BENCHMARK_FIXTURE_TABLES) {
    for (const target of [1, Math.ceil(rows / 2), rows]) {
      samples.add(mixedSampleRow(rows, table, target));
    }
  }
  return [...samples];
}

function mixedSampleRow(
  rows: number,
  table: BenchmarkFixtureTable,
  target: number,
) {
  for (let distance = 0; distance < 10; distance++) {
    for (const candidate of [target - distance, target + distance]) {
      if (
        candidate >= 1 &&
        candidate <= rows &&
        makeBenchmarkFixtureRow(candidate).table === table
      ) {
        return candidate;
      }
    }
  }
  throw new Error(`could not select ${table} sample near row ${target}`);
}

function validateMixedSample(db: Database, rowID: number) {
  const {table, row} = makeBenchmarkFixtureRow(rowID);
  const columns = Object.keys(row);
  const where =
    table === 'bench_composite' ? 'account_id = ? AND seq = ?' : 'id = ?';
  const params =
    table === 'bench_composite'
      ? [row.account_id, row.seq]
      : [(row as {id: number}).id];
  const actual = db
    .prepare(
      `SELECT ${columns.map(id).join(', ')} FROM ${id(table)} WHERE ${where}`,
    )
    .get<Record<string, unknown>>(...params);
  const expected = Object.fromEntries(
    Object.entries(row).map(([key, value]) => [
      key,
      typeof value === 'boolean' ? Number(value) : value,
    ]),
  );
  assertExact(`${table} sample ${rowID}`, actual, expected);
}

function validateLargeSample(
  db: Database,
  fixture: Exclude<InitialSyncBenchmarkFixture, {fixture: 'mixed'}>,
  row: number,
) {
  if (fixture.fixture === 'wide-text') {
    const rowID = `wide-text-${row}`;
    const actual = db
      .prepare(
        `SELECT attributes, labels, tags, created_at, event_at, properties,
                formatted_text, id, is_inactive, external_id, reference_a,
                reference_b, summary, large_text, source_type, title,
                plain_text, group_id, targets, category, unique_ref,
                updated_at, bucket_id, version, partition_id
           FROM bench_wide_text WHERE id = ?`,
      )
      .get<Record<string, unknown>>(rowID);
    const timestamp = Date.UTC(2025, 0, 1) + row * 1_000;
    assertExact(`bench_wide_text sample ${row}`, actual, {
      attributes: `[{"name": "attribute-${row}"}]`,
      labels: `["label-${row}"]`,
      tags: `["tag-${row}"]`,
      created_at: timestamp,
      event_at: timestamp,
      properties: `{"key": "value-${row}"}`,
      formatted_text: `<p>Record ${row}</p>`,
      id: rowID,
      is_inactive: row % 17 === 0 ? 1 : 0,
      external_id: `external-${row}`,
      reference_a: `reference-a-${row}`,
      reference_b: `reference-b-${row}`,
      summary: `summary-${row}`,
      large_text: benchmarkProductionPayload(
        'wide-text',
        row,
        fixture.payloadBytes,
      ),
      source_type: row % 2 === 0 ? 'stream' : 'batch',
      title: `Title ${row}`,
      plain_text: `Plain text ${row}`,
      group_id: `group-${row % 100}`,
      targets: `["target-${row}"]`,
      category: row % 2 === 0 ? 'primary' : 'secondary',
      unique_ref: `unique-ref-${row}`,
      updated_at: timestamp,
      bucket_id: `bucket-${row % 100}`,
      version: row,
      partition_id: `partition-${row % 10}`,
    });
    return;
  }

  const rowID = `payload-${row}`;
  const actual = db
    .prepare(
      `SELECT created_at, id, payload, format, source_type, updated_at,
              partition_id
         FROM "benchmark.bench_large_payload" WHERE id = ?`,
    )
    .get<Record<string, unknown>>(rowID);
  const timestamp = Date.UTC(2025, 0, 1) + row * 1_000;
  const sources = ['stream', 'batch', 'snapshot'] as const;
  assertExact(`benchmark.bench_large_payload sample ${row}`, actual, {
    created_at: timestamp,
    id: rowID,
    payload: `{"data": "${benchmarkProductionPayload(
      'large-payload',
      row,
      fixture.payloadBytes,
    )}"}`,
    format: `v${1 + (row % 2)}`,
    source_type: sources[row % 3],
    updated_at: timestamp,
    partition_id: `partition-${row % 100}`,
  });
}

function assertExact(label: string, actual: unknown, expected: unknown) {
  const actualJSON = JSON.stringify(actual);
  const expectedJSON = JSON.stringify(expected);
  if (actualJSON !== expectedJSON) {
    throw new Error(
      `${label} mismatch:\nexpected ${expectedJSON}\nactual   ${actualJSON}`,
    );
  }
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
