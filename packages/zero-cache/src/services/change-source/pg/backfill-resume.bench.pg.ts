// Measures the cost of ordering a backfill COPY by the row key (R0's gate in
// the resumable-backfills plan), and of resuming one from a mark.
//
// The COPY is drained without decoding so that the numbers isolate what
// ordering changes — Postgres's scan — rather than the (unchanged) cost of
// parsing and assembling backfill messages.
//
//   pnpm --filter zero-cache run bench:pg backfill-resume.bench

import {afterEach, describe, expect} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../../../shared/src/bench.ts';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../../shared/src/must.ts';
import {type PgTest, test} from '../../../test/db.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {
  getKeyCollations,
  orderByRowKey,
  resumeWhere,
  type ResumeColumnSpec,
} from './backfill-resume.ts';
import {makeBinarySelectExprs, makeDownloadStatements} from './initial-sync.ts';
import {getPublicationInfo} from './schema/published.ts';

const lc = createSilentLogContext();
const benchmarkRecorder = createManualBenchmarkRecorder();

const ROWS = Number(process.env['ZERO_BACKFILL_RESUME_BENCH_ROWS'] ?? 500_000);
const WARMUP_REPS = 1;
const REPS = Number(process.env['ZERO_BACKFILL_RESUME_BENCH_REPS'] ?? 3);
const PUBLICATION = 'resume_bench_pub';

let cleanup: (() => Promise<void> | void)[] = [];

afterEach(async () => {
  for (const fn of cleanup.reverse()) {
    await fn();
  }
  cleanup = [];
});

type Fixture = {
  /** The table's name, which is also the key's shape. */
  name: 'int_key' | 'uuid_key' | 'text_key';
  keyColumns: string[];
  ddl: string;
};

const FIXTURES: Fixture[] = [
  {
    name: 'int_key',
    keyColumns: ['id'],
    ddl: /*sql*/ `
      CREATE TABLE int_key(
        id INT8 NOT NULL PRIMARY KEY,
        payload TEXT NOT NULL,
        n INT4 NOT NULL
      );
      INSERT INTO int_key(id, payload, n)
        SELECT i, repeat(md5(i::text), 4), i FROM generate_series(1, ${ROWS}) i;`,
  },
  {
    // A random uuid key is the adversarial case: the heap order and the index
    // order are uncorrelated, so an index-ordered scan is random heap I/O
    // where an unordered COPY is sequential.
    name: 'uuid_key',
    keyColumns: ['id'],
    ddl: /*sql*/ `
      CREATE TABLE uuid_key(
        id UUID NOT NULL PRIMARY KEY,
        payload TEXT NOT NULL,
        n INT4 NOT NULL
      );
      INSERT INTO uuid_key(id, payload, n)
        SELECT gen_random_uuid(), repeat(md5(i::text), 4), i
          FROM generate_series(1, ${ROWS}) i;`,
  },
  {
    // The shape most Zero applications use: a random string id (nanoid/cuid).
    // Uncorrelated like uuid, and its sort additionally pays collation-aware
    // comparisons.
    name: 'text_key',
    keyColumns: ['id'],
    ddl: /*sql*/ `
      CREATE TABLE text_key(
        id TEXT NOT NULL PRIMARY KEY,
        payload TEXT NOT NULL,
        n INT4 NOT NULL
      );
      INSERT INTO text_key(id, payload, n)
        SELECT substr(md5(i::text || 'salt'), 1, 21), repeat(md5(i::text), 4), i
          FROM generate_series(1, ${ROWS}) i;`,
  },
];

function drainCopy(
  db: PostgresDB,
  select: string,
  settings: readonly string[] = [],
): Promise<number> {
  const copy = async (sql: PostgresDB): Promise<number> => {
    let bytes = 0;
    const readable = await sql
      .unsafe(`COPY (${select}) TO STDOUT WITH (FORMAT binary)`)
      .readable();
    for await (const chunk of readable) {
      bytes += (chunk as Buffer).byteLength;
    }
    return bytes;
  };
  if (settings.length === 0) {
    return copy(db);
  }
  return db.begin(async sql => {
    for (const setting of settings) {
      await sql.unsafe(`SET LOCAL ${setting}`);
    }
    return copy(sql as unknown as PostgresDB);
  }) as Promise<number>;
}

describe('zero-cache/backfill ordered COPY', () => {
  for (const fixture of FIXTURES) {
    test(
      `${fixture.name} (${ROWS} rows)`,
      {timeout: 3_600_000},
      async ({testDBs}: PgTest) => {
        const db = await testDBs.create(
          `backfill_resume_bench_${fixture.name}`,
        );
        cleanup.push(() => testDBs.drop(db));
        await db.unsafe(
          `${fixture.ddl}
           CREATE PUBLICATION ${PUBLICATION} FOR TABLE ${fixture.name};
           ANALYZE ${fixture.name};`,
        );

        const {tables} = await getPublicationInfo(db, [PUBLICATION]);
        const spec = must(tables.find(t => t.name === fixture.name));
        const cols = [...fixture.keyColumns, 'payload', 'n'];
        const collations = await getKeyCollations(
          db,
          spec.oid,
          fixture.keyColumns,
        );
        const keySpecs = fixture.keyColumns.map((col): ResumeColumnSpec => ({
          ...spec.columns[col],
          collationIsDeterministic: collations.get(col) ?? null,
        }));

        const statements = (order?: {by: string; after?: string | undefined}) =>
          makeDownloadStatements(
            spec,
            cols,
            undefined,
            undefined,
            makeBinarySelectExprs(spec, cols),
            order,
          ).select;

        // The mark at the 50% point of the key order.
        const [midRow] = await db.unsafe<Record<string, unknown>[]>(
          `SELECT ${fixture.keyColumns.map(c => `"${c}"`).join(',')} ` +
            `FROM ${fixture.name} ORDER BY ${orderByRowKey(fixture.keyColumns)} ` +
            `OFFSET ${Math.floor(ROWS / 2)} LIMIT 1`,
        );
        const mark = fixture.keyColumns.map(c => String(midRow[c]));

        const ordered = {by: orderByRowKey(fixture.keyColumns)};
        const variants: Record<
          string,
          {select: string; settings?: readonly string[]}
        > = {
          // Today's backfill: an unordered COPY.
          'unordered': {select: statements()},
          // The ordered COPY that resumability requires. For a key whose
          // order is uncorrelated with the heap (a random uuid or nanoid),
          // this is random heap I/O where today's COPY is sequential.
          'ordered': {select: statements(ordered)},
          // The same, with the planner pushed off the index scan and onto a
          // sort. The planner does not choose this on its own, but for an
          // uncorrelated key it is substantially faster than the index scan.
          'ordered-sorted': {
            select: statements(ordered),
            settings: [
              'enable_indexscan = off',
              'enable_bitmapscan = off',
              `work_mem = '256MB'`,
            ],
          },
          'resumed-50%': {
            select: statements({
              ...ordered,
              after: resumeWhere(fixture.keyColumns, keySpecs, mark),
            }),
          },
        };

        const [{count}] = await db.unsafe<{count: bigint}[]>(
          `SELECT count(*) AS count FROM ${fixture.name}`,
        );
        expect(Number(count)).toBe(ROWS);

        for (const [variant, {select, settings}] of Object.entries(variants)) {
          const samples: {elapsedMs: number; operations: number}[] = [];
          const plan = (
            await db.begin(async sql => {
              for (const setting of settings ?? []) {
                await sql.unsafe(`SET LOCAL ${setting}`);
              }
              return sql.unsafe<{'QUERY PLAN': string}[]>(`EXPLAIN ${select}`);
            })
          )
            .map(row => row['QUERY PLAN'])
            .join('\n');
          for (let rep = 0; rep < WARMUP_REPS + REPS; rep++) {
            const start = performance.now();
            const bytes = await drainCopy(db, select, settings);
            const elapsedMs = performance.now() - start;
            expect(bytes).toBeGreaterThan(0);
            if (rep >= WARMUP_REPS) {
              samples.push({elapsedMs, operations: bytes / 1_000_000});
            }
          }
          // eslint-disable-next-line no-console
          console.log(
            `\n[${fixture.name} ${variant}] ` +
              samples
                .map(
                  ({elapsedMs, operations}) =>
                    `${operations.toFixed(1)}MB in ${elapsedMs.toFixed(0)}ms ` +
                    `(${(operations / (elapsedMs / 1000)).toFixed(0)} MB/s)`,
                )
                .join(', ') +
              `\n${plan}`,
          );
          lc.info?.(`${fixture.name} ${variant}`, samples);
          benchmarkRecorder.recordThroughputSamples(
            `zero-cache/backfill COPY ${fixture.name} ${variant} MB`,
            samples,
          );
        }
      },
    );
  }
});
