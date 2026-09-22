// The resume helpers against a real Postgres: what a mark's SQL literal has to
// be for the resume predicate to seek rather than scan, which keys can carry
// one at all, and what `rowsExist` answers.

import {beforeEach, describe, expect} from 'vitest';
import {must} from '../../../../../shared/src/must.ts';
import {type PgTest, test} from '../../../test/db.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {
  getKeyCollations,
  getKeyCorrelation,
  isCheaplyOrderable,
  isResumableKey,
  orderByRowKey,
  resumeWhere,
  rowsExist,
  type ResumeColumnSpec,
} from './backfill-resume.ts';
import {makeDownloadStatements} from './initial-sync.ts';
import {getPublicationInfo} from './schema/published.ts';

describe('backfill-resume (pg)', () => {
  let db: PostgresDB;

  beforeEach<PgTest>(async ({testDBs}) => {
    db = await testDBs.create('backfill_resume_test');
    await db.unsafe(/*sql*/ `
      CREATE TABLE ints(
        a INT8 NOT NULL,
        b INT4 NOT NULL,
        v TEXT,
        PRIMARY KEY(a, b)
      );
      CREATE TABLE texts(
        k TEXT NOT NULL PRIMARY KEY,
        v INT4
      );
      CREATE TABLE uuids(
        u UUID NOT NULL PRIMARY KEY,
        v INT4
      );
      CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
      CREATE TABLE nondet(
        k TEXT COLLATE ci NOT NULL PRIMARY KEY,
        v INT4
      );
      CREATE PUBLICATION the_pub FOR TABLE ints, texts, uuids, nondet;

      INSERT INTO ints(a, b, v)
        SELECT i, i * 2, 'v' || i FROM generate_series(1, 500) i;
      INSERT INTO texts(k, v)
        SELECT 'key-' || lpad(i::text, 5, '0'), i FROM generate_series(1, 500) i;
      INSERT INTO texts(k, v) VALUES
        (E'quote''s', -1),
        (E'back\\\\slash', -2),
        (E'héllo 中文', -3);
      INSERT INTO uuids(u, v)
        SELECT ('00000000-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid, i
        FROM generate_series(1, 500) i;
      ANALYZE ints; ANALYZE texts; ANALYZE uuids;
    `);
    return () => testDBs.drop(db);
  });

  async function specs() {
    const {tables} = await getPublicationInfo(db, ['the_pub']);
    return new Map(tables.map(t => [t.name, t]));
  }

  async function resumeSpecs(table: string, columns: string[]) {
    const spec = must((await specs()).get(table));
    const collations = await getKeyCollations(db, spec.oid, columns);
    return {
      spec,
      resume: columns.map((col): ResumeColumnSpec => ({
        ...spec.columns[col],
        collationIsDeterministic: collations.get(col) ?? null,
      })),
    };
  }

  test('getKeyCollations reports determinism', async () => {
    const intSpec = must((await specs()).get('ints'));
    expect(await getKeyCollations(db, intSpec.oid, ['a', 'b'])).toEqual(
      new Map([
        ['a', null],
        ['b', null],
      ]),
    );

    const textSpec = must((await specs()).get('texts'));
    expect(await getKeyCollations(db, textSpec.oid, ['k'])).toEqual(
      new Map([['k', true]]),
    );

    const nonDetSpec = must((await specs()).get('nondet'));
    expect(await getKeyCollations(db, nonDetSpec.oid, ['k'])).toEqual(
      new Map([['k', false]]),
    );
  });

  describe('getKeyCorrelation', () => {
    test('a monotonically inserted key is cheaply orderable', async () => {
      const spec = must((await specs()).get('ints'));
      const correlation = await getKeyCorrelation(db, spec.oid, 'a');
      expect(correlation).toBe(1);
      expect(isCheaplyOrderable(correlation)).toBe(true);
    });

    test('a random key is not cheaply orderable', async () => {
      // The `uuids` fixture's keys are generated in order; a real random key
      // leaves the heap uncorrelated with the index.
      await db.unsafe(/*sql*/ `
        CREATE TABLE randoms(u UUID NOT NULL PRIMARY KEY, v INT4);
        INSERT INTO randoms(u, v)
          SELECT gen_random_uuid(), i FROM generate_series(1, 5000) i;
        ANALYZE randoms;
      `);
      const [{oid}] = await db<{oid: number}[]>`
        SELECT oid::int8::int AS oid FROM pg_class WHERE relname = 'randoms'`;
      const correlation = must(await getKeyCorrelation(db, oid, 'u'));
      expect(Math.abs(correlation)).toBeLessThan(0.9);
      expect(isCheaplyOrderable(correlation)).toBe(false);
    });

    test('a never-analyzed table has no correlation', async () => {
      await db.unsafe(/*sql*/ `
        CREATE TABLE fresh(id INT8 NOT NULL PRIMARY KEY);
        INSERT INTO fresh SELECT generate_series(1, 100);
      `);
      const [{oid}] = await db<{oid: number}[]>`
        SELECT oid::int8::int AS oid FROM pg_class WHERE relname = 'fresh'`;
      expect(await getKeyCorrelation(db, oid, 'id')).toBe(null);
      expect(isCheaplyOrderable(null)).toBe(false);
    });
  });

  test('a non-deterministic collation is not resumable', async () => {
    const {resume} = await resumeSpecs('nondet', ['k']);
    expect(isResumableKey(resume)).toBe(false);
  });

  test.each([
    {table: 'ints', key: ['a', 'b'], mark: ['250', '500']},
    {table: 'texts', key: ['k'], mark: ['key-00250']},
    {
      table: 'uuids',
      key: ['u'],
      mark: ['00000000-0000-4000-8000-000000000250'],
    },
  ])(
    'resumed download yields the suffix of $table',
    async ({table, key, mark}) => {
      const {spec, resume} = await resumeSpecs(table, key);
      expect(isResumableKey(resume)).toBe(true);

      const all = await db.unsafe(
        makeDownloadStatements(spec, key, undefined, undefined, undefined, {
          by: orderByRowKey(key),
        }).select,
      );
      // The mark is a key that exists in the table; the resumed run must yield
      // exactly what follows it.
      const markIndex = all.findIndex(row =>
        key.every((col, i) => String(row[col]) === mark[i]),
      );
      expect(markIndex).toBeGreaterThanOrEqual(0);
      const expectedRows = all.length - markIndex - 1;

      const {select, getTotalRows} = makeDownloadStatements(
        spec,
        key,
        undefined,
        undefined,
        undefined,
        {by: orderByRowKey(key), after: resumeWhere(key, resume, mark)},
      );
      const rows = await db.unsafe(select);
      expect(rows).toEqual(all.slice(markIndex + 1));
      expect(rows.length).toBe(expectedRows);

      // A resumed run's totals reflect what remains.
      const [{totalRows}] = await db.unsafe(getTotalRows);
      expect(Number(totalRows)).toBe(expectedRows);
    },
  );

  test('text marks with quotes, backslashes and unicode round trip', async () => {
    const {spec, resume} = await resumeSpecs('texts', ['k']);
    const all = await db.unsafe(
      makeDownloadStatements(spec, ['k'], undefined, undefined, undefined, {
        by: orderByRowKey(['k']),
      }).select,
    );
    const keys = all.map(({k}) => k as string);

    for (const special of [`quote's`, 'back\\slash', 'héllo 中文']) {
      const index = keys.indexOf(special);
      expect(index).toBeGreaterThanOrEqual(0);
      const {select} = makeDownloadStatements(
        spec,
        ['k'],
        undefined,
        undefined,
        undefined,
        {
          by: orderByRowKey(['k']),
          after: resumeWhere(['k'], resume, [special]),
        },
      );
      const rows = await db.unsafe(select);
      expect(rows.map(({k}) => k)).toEqual(keys.slice(index + 1));
    }
  });

  test('the resume WHERE is ANDed with the publication row filter', async () => {
    await db.unsafe(/*sql*/ `
      CREATE PUBLICATION filtered FOR TABLE ints WHERE (b > 600);
    `);
    const {tables} = await getPublicationInfo(db, ['filtered']);
    const spec = tables[0];
    const key = ['a', 'b'];
    const collations = await getKeyCollations(db, spec.oid, key);
    const resume = key.map((col): ResumeColumnSpec => ({
      ...spec.columns[col],
      collationIsDeterministic: collations.get(col) ?? null,
    }));

    const {select} = makeDownloadStatements(
      spec,
      key,
      undefined,
      undefined,
      undefined,
      {by: orderByRowKey(key), after: resumeWhere(key, resume, ['400', '800'])},
    );
    const rows = await db.unsafe(select);
    // b > 600 means a > 300; resuming after a = 400 leaves 401..500.
    expect(rows.length).toBe(100);
    expect(rows[0]).toEqual({a: 401n, b: 802});
  });

  test.each([
    {table: 'ints', key: ['a', 'b'], mark: ['250', '500'], index: 'ints_pkey'},
    {table: 'texts', key: ['k'], mark: ['key-00250'], index: 'texts_pkey'},
    {
      table: 'uuids',
      key: ['u'],
      mark: ['00000000-0000-4000-8000-000000000250'],
      index: 'uuids_pkey',
    },
  ])(
    'the ordered, resumed select seeks $index',
    async ({table, key, mark, index}) => {
      const {spec, resume} = await resumeSpecs(table, key);
      const {select} = makeDownloadStatements(
        spec,
        key,
        undefined,
        undefined,
        undefined,
        {by: orderByRowKey(key), after: resumeWhere(key, resume, mark)},
      );
      // The fixtures are too small for the planner to prefer an index scan;
      // what is being verified is that the plan *can* seek, i.e. that the
      // resume predicate is an Index Cond rather than a Filter, and that the
      // ordering does not require a Sort. (A `STABLE` expression such as
      // `convert_from(decode(...), 'UTF8')` produces a Filter here, which
      // would scan the index from the beginning on every resume.)
      const rows = await db.begin(async sql => {
        await sql.unsafe(`SET LOCAL enable_seqscan = off`);
        return sql.unsafe<{'QUERY PLAN': string}[]>(`EXPLAIN ${select}`);
      });
      const plan = rows.map(row => row['QUERY PLAN']).join('\n');
      expect(plan).toContain(index);
      expect(plan).toContain('Index Cond');
      expect(plan).not.toContain('Filter');
      expect(plan).not.toContain('Sort');
    },
  );

  describe('rowsExist', () => {
    test('bounded range', async () => {
      const {spec, resume} = await resumeSpecs('ints', ['a', 'b']);
      const key = ['a', 'b'];
      expect(
        await rowsExist(db, spec, key, resume, ['100', '200'], ['200', '400']),
      ).toBe(true);
      expect(
        await rowsExist(db, spec, key, resume, ['100', '200'], ['101', '202']),
      ).toBe(true);
      // (100,200] excludes the lower bound and there is no row between.
      expect(
        await rowsExist(db, spec, key, resume, ['100', '200'], ['100', '200']),
      ).toBe(false);
      expect(
        await rowsExist(
          db,
          spec,
          key,
          resume,
          ['500', '1000'],
          ['600', '1200'],
        ),
      ).toBe(false);
    });

    test('null lower bound means from the beginning', async () => {
      const {spec, resume} = await resumeSpecs('ints', ['a', 'b']);
      const key = ['a', 'b'];
      expect(await rowsExist(db, spec, key, resume, null, ['1', '2'])).toBe(
        true,
      );
      expect(await rowsExist(db, spec, key, resume, null, ['0', '0'])).toBe(
        false,
      );
    });

    test('honors the publication row filter', async () => {
      await db.unsafe(/*sql*/ `
        CREATE PUBLICATION filtered2 FOR TABLE ints WHERE (b > 600);
      `);
      const {tables} = await getPublicationInfo(db, ['filtered2']);
      const spec = tables[0];
      const key = ['a', 'b'];
      const collations = await getKeyCollations(db, spec.oid, key);
      const resume = key.map((col): ResumeColumnSpec => ({
        ...spec.columns[col],
        collationIsDeterministic: collations.get(col) ?? null,
      }));
      // Rows 1..300 are excluded by the filter.
      expect(await rowsExist(db, spec, key, resume, null, ['300', '600'])).toBe(
        false,
      );
      expect(await rowsExist(db, spec, key, resume, null, ['301', '602'])).toBe(
        true,
      );
    });
  });
});
