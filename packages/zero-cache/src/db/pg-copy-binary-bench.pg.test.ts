import {PostgreSqlContainer, type StartedPostgreSqlContainer} from '@testcontainers/postgresql';
import {Writable, type Readable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import postgres from 'postgres';
import {afterAll, beforeAll, describe, expect, inject, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {postgresTypeConfig, type PostgresDB, type PostgresValueType} from '../types/pg.ts';
import {
  BinaryCopyParser,
  makeBinaryDecoder,
  type BinaryDecoder,
} from './pg-copy-binary.ts';
import {TsvParser} from './pg-copy.ts';
import {getTypeParsers} from './pg-type-parser.ts';
import {
  JSON_STRINGIFIED,
  liteValue,
  type LiteValueType,
} from '../types/lite.ts';

// ---- Config ----
const ROW_COUNTS = [10_000, 100_000];
const LATENCIES_MS = [0, 2];
const INSERT_BATCH_SIZE = 50;

// ---- Container setup ----

let container: StartedPostgreSqlContainer;
let sql: PostgresDB;

beforeAll(async () => {
  const pgImage = inject('pgImage') ?? 'postgres:18-alpine';
  container = await new PostgreSqlContainer(pgImage)
    .withCommand([
      'postgres',
      '-c', 'wal_level=logical',
      '-c', 'timezone=UTC',
    ])
    .withPrivilegedMode()
    .start();

  // Install iproute2 for tc netem.
  await container.exec(['apk', 'add', '--no-cache', '-q', 'iproute2']);

  sql = postgres(container.getConnectionUri(), {
    connection: {TimeZone: 'UTC'},
    ...postgresTypeConfig(),
  });
}, 120_000);

afterAll(async () => {
  await sql?.end();
  await container?.stop();
}, 60_000);

async function addLatency(ms: number) {
  if (ms > 0) {
    await container.exec([
      'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'delay', `${ms}ms`,
    ]);
  }
}

async function removeLatency() {
  try {
    await container.exec(['tc', 'qdisc', 'del', 'dev', 'eth0', 'root']);
  } catch {
    // Ignore if no qdisc to delete.
  }
}

// ---- Copy implementations ----

type CopyResult = {
  rows: number;
  parseMs: number;
  flushMs: number;
  totalMs: number;
};

type ColumnInfo = {
  name: string;
  typeOID: number;
  dataType: string;
  pgTypeClass: string;
  elemPgTypeClass?: string | null | undefined;
};

async function copyWithText(
  db: PostgresDB,
  pgTable: string,
  columns: ColumnInfo[],
  liteDb: Database,
  liteTable: string,
): Promise<CopyResult> {
  const pgParsers = await getTypeParsers(db, {returnJsonAsString: true});
  const parsers = columns.map(c => {
    const pgParse = pgParsers.getTypeParser(c.typeOID);
    return (val: string) =>
      liteValue(pgParse(val) as PostgresValueType, c.dataType, JSON_STRINGIFIED);
  });

  const colList = columns.map(c => `"${c.name}"`).join(',');
  const ph = `(${'?,'.repeat(columns.length - 1)}?)`;
  const insertStmt = liteDb.prepare(
    `INSERT INTO "${liteTable}" (${colList}) VALUES ${ph}`,
  );
  const insertBatchStmt = liteDb.prepare(
    `INSERT INTO "${liteTable}" (${colList}) VALUES ${ph}` +
      `,${ph}`.repeat(INSERT_BATCH_SIZE - 1),
  );

  const vpr = columns.length;
  const vpb = vpr * INSERT_BATCH_SIZE;
  const maxBuf = 10_000;
  const pending: LiteValueType[] = Array.from({length: maxBuf * vpr});
  let totalFlushedRows = 0;
  let pendingRows = 0;
  let flushMs = 0;

  function flush() {
    const flushedRows = pendingRows;
    const t0 = performance.now();
    let l = 0;
    for (; pendingRows > INSERT_BATCH_SIZE; pendingRows -= INSERT_BATCH_SIZE) {
      insertBatchStmt.run(pending.slice(l, (l += vpb)));
    }
    for (; pendingRows > 0; pendingRows--) {
      insertStmt.run(pending.slice(l, (l += vpr)));
    }
    flushMs += performance.now() - t0;
    totalFlushedRows += flushedRows;
  }

  const tsvParser = new TsvParser();
  let col = 0;
  const start = performance.now();

  const readable: Readable = await db
    .unsafe(`COPY ${pgTable} (${colList}) TO STDOUT`)
    .readable();

  await pipeline(
    readable,
    new Writable({
      highWaterMark: 8 * 1024 * 1024,
      write(chunk: Buffer, _encoding, callback) {
        try {
          for (const text of tsvParser.parse(chunk)) {
            pending[pendingRows * vpr + col] =
              text === null ? null : parsers[col](text);
            if (++col === parsers.length) {
              col = 0;
              if (++pendingRows >= maxBuf) {
                flush();
              }
            }
          }
          callback();
        } catch (e) {
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },
      final(callback) {
        try {
          flush();
          callback();
        } catch (e) {
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },
    }),
  );

  const totalMs = performance.now() - start;
  // parse time = total - flush (includes network I/O + parsing)
  return {rows: totalFlushedRows, parseMs: totalMs - flushMs, flushMs, totalMs};
}

async function copyWithBinary(
  db: PostgresDB,
  pgTable: string,
  columns: ColumnInfo[],
  liteDb: Database,
  liteTable: string,
): Promise<CopyResult> {
  const decoders: BinaryDecoder[] = columns.map(c =>
    makeBinaryDecoder(c as Parameters<typeof makeBinaryDecoder>[0]),
  );

  const colList = columns.map(c => `"${c.name}"`).join(',');
  const ph = `(${'?,'.repeat(columns.length - 1)}?)`;
  const insertStmt = liteDb.prepare(
    `INSERT INTO "${liteTable}" (${colList}) VALUES ${ph}`,
  );
  const insertBatchStmt = liteDb.prepare(
    `INSERT INTO "${liteTable}" (${colList}) VALUES ${ph}` +
      `,${ph}`.repeat(INSERT_BATCH_SIZE - 1),
  );

  const vpr = columns.length;
  const vpb = vpr * INSERT_BATCH_SIZE;
  const maxBuf = 10_000;
  const pending: LiteValueType[] = Array.from({length: maxBuf * vpr});
  let totalFlushedRows = 0;
  let pendingRows = 0;
  let flushMs = 0;

  function flush() {
    const flushedRows = pendingRows;
    const t0 = performance.now();
    let l = 0;
    for (; pendingRows > INSERT_BATCH_SIZE; pendingRows -= INSERT_BATCH_SIZE) {
      insertBatchStmt.run(pending.slice(l, (l += vpb)));
    }
    for (; pendingRows > 0; pendingRows--) {
      insertStmt.run(pending.slice(l, (l += vpr)));
    }
    flushMs += performance.now() - t0;
    totalFlushedRows += flushedRows;
  }

  const binaryParser = new BinaryCopyParser();
  let col = 0;
  const start = performance.now();

  const readable: Readable = await db
    .unsafe(
      `COPY ${pgTable} (${colList}) TO STDOUT WITH (FORMAT binary)`,
    )
    .readable();

  await pipeline(
    readable,
    new Writable({
      highWaterMark: 8 * 1024 * 1024,
      write(chunk: Buffer, _encoding, callback) {
        try {
          for (const fieldBuf of binaryParser.parse(chunk)) {
            pending[pendingRows * vpr + col] =
              fieldBuf === null ? null : decoders[col](fieldBuf);
            if (++col === decoders.length) {
              col = 0;
              if (++pendingRows >= maxBuf) {
                flush();
              }
            }
          }
          callback();
        } catch (e) {
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },
      final(callback) {
        try {
          flush();
          callback();
        } catch (e) {
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },
    }),
  );

  const totalMs = performance.now() - start;
  return {rows: totalFlushedRows, parseMs: totalMs - flushMs, flushMs, totalMs};
}

// ---- Benchmark tests ----

describe('COPY binary vs text benchmark', () => {
  async function getColumnInfo(tableName: string): Promise<ColumnInfo[]> {
    const rows = await sql`
      SELECT a.attname AS name,
             a.atttypid AS "typeOID",
             t.typname AS "dataType",
             t.typtype AS "pgTypeClass"
      FROM pg_attribute a
      JOIN pg_type t ON a.atttypid = t.oid
      WHERE a.attrelid = ${tableName}::regclass
        AND a.attnum > 0
        AND NOT a.attisdropped
      ORDER BY a.attnum
    `;
    return (rows as unknown as ColumnInfo[]).map(c =>
      c.name === 'tags' ? {...c, elemPgTypeClass: 'b' as const} : c,
    );
  }

  async function seedTable(tableName: string, rowCount: number) {
    await sql.unsafe(`
      CREATE TABLE IF NOT EXISTS ${tableName} (
        id int4,
        name text,
        email varchar(200),
        active bool,
        score float8,
        created_at timestamptz,
        birthday date,
        balance numeric(12,2),
        metadata jsonb,
        tags text[]
      )
    `);
    await sql.unsafe(`TRUNCATE ${tableName}`);

    const batchSize = 1000;
    for (let i = 0; i < rowCount; i += batchSize) {
      const end = Math.min(i + batchSize, rowCount);
      await sql.unsafe(`
        INSERT INTO ${tableName}
          (id, name, email, active, score, created_at, birthday, balance, metadata, tags)
        SELECT
          g,
          'user_' || g,
          'user_' || g || '@example.com',
          (g % 2 = 0),
          g * 1.5,
          '2024-01-01'::timestamptz + (g || ' seconds')::interval,
          '1990-01-01'::date + (g % 10000),
          (g * 100.50)::numeric(12,2),
          jsonb_build_object('key', g, 'nested', jsonb_build_object('a', g % 100)),
          ARRAY['tag_' || (g % 10), 'tag_' || (g % 20)]
        FROM generate_series(${i + 1}, ${end}) g
      `);
    }
  }

  function createLiteTable(db: Database, name: string) {
    db.exec(`
      CREATE TABLE "${name}" (
        "id" INTEGER,
        "name" TEXT,
        "email" TEXT,
        "active" INTEGER,
        "score" REAL,
        "created_at" REAL,
        "birthday" REAL,
        "balance" REAL,
        "metadata" TEXT,
        "tags" TEXT
      )
    `);
  }

  function fmt(ms: number): string {
    return ms >= 1000 ? `${(ms / 1000).toFixed(2)}s` : `${ms.toFixed(1)}ms`;
  }

  function rate(rows: number, ms: number): string {
    const rps = (rows / ms) * 1000;
    return rps >= 1_000_000
      ? `${(rps / 1_000_000).toFixed(2)}M rows/s`
      : rps >= 1000
        ? `${(rps / 1000).toFixed(1)}K rows/s`
        : `${rps.toFixed(0)} rows/s`;
  }

  for (const rowCount of ROW_COUNTS) {
    for (const latency of LATENCIES_MS) {
      const label = `${(rowCount / 1000).toFixed(0)}K rows, ${latency}ms latency`;

      test(
        label,
        {timeout: 300_000},
        async () => {
          const pgTable = `bench_${rowCount}`;
          const lc = createSilentLogContext();

          // Seed once per row count (reused across latency values via TRUNCATE guard).
          await seedTable(pgTable, rowCount);
          const cols = await getColumnInfo(pgTable);

          await addLatency(latency);
          try {
            // ---- Text ----
            const textDb = new Database(lc, ':memory:');
            textDb.exec('PRAGMA journal_mode = OFF');
            textDb.exec('PRAGMA synchronous = OFF');
            createLiteTable(textDb, pgTable);
            const textResult = await copyWithText(
              sql, pgTable, cols, textDb, pgTable,
            );
            textDb.close();

            // ---- Binary ----
            const binDb = new Database(lc, ':memory:');
            binDb.exec('PRAGMA journal_mode = OFF');
            binDb.exec('PRAGMA synchronous = OFF');
            createLiteTable(binDb, pgTable);
            const binResult = await copyWithBinary(
              sql, pgTable, cols, binDb, pgTable,
            );
            binDb.close();

            // ---- Report ----
            const speedup = textResult.totalMs / binResult.totalMs;
            const parseSpeedup =
              textResult.parseMs > 0 && binResult.parseMs > 0
                ? textResult.parseMs / binResult.parseMs
                : NaN;

            // oxlint-disable-next-line no-console
            console.log(`\n--- ${label} ---`);
            // oxlint-disable-next-line no-console
            console.log(
              `  TEXT:   total=${fmt(textResult.totalMs)}  ` +
                `parse=${fmt(textResult.parseMs)}  ` +
                `flush=${fmt(textResult.flushMs)}  ` +
                `rate=${rate(textResult.rows, textResult.totalMs)}`,
            );
            // oxlint-disable-next-line no-console
            console.log(
              `  BINARY: total=${fmt(binResult.totalMs)}  ` +
                `parse=${fmt(binResult.parseMs)}  ` +
                `flush=${fmt(binResult.flushMs)}  ` +
                `rate=${rate(binResult.rows, binResult.totalMs)}`,
            );
            // oxlint-disable-next-line no-console
            console.log(
              `  SPEEDUP: ${speedup.toFixed(2)}x total` +
                (Number.isFinite(parseSpeedup)
                  ? `, ${parseSpeedup.toFixed(2)}x parse`
                  : '') +
                '\n',
            );

            expect(textResult.rows).toBe(rowCount);
            expect(binResult.rows).toBe(rowCount);
          } finally {
            await removeLatency();
          }
        },
      );
    }
  }
});
