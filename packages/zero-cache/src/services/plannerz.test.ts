import {mkdtempSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import fastify, {type FastifyInstance} from 'fastify';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {
  buildPlannerzBundleFromDB,
  handlePlannerzRequest,
  parseStat1,
  summarizeStat4,
  type PlannerzBundle,
  type Stat4Sample,
} from './plannerz.ts';
import {initReplicationState} from './replicator/schema/replication-state.ts';

const lc = createSilentLogContext();

/**
 * A value that must never appear in a response. Used as the text in every
 * indexed column, so that a leak of a stat4 sample shows up as this string.
 */
const SENTINEL = 'SECRET-do-not-leak';

function newReplica(name: string): {db: Database; file: string} {
  const dir = mkdtempSync(join(tmpdir(), 'plannerz-test-'));
  const file = join(dir, `${name}.db`);
  const db = new Database(lc, file);
  db.exec(`
    CREATE TABLE "issue" (
      "id" TEXT PRIMARY KEY,
      "title" TEXT,
      "ownerID" TEXT,
      "closed" "int8" NOT NULL
    );
    CREATE INDEX "issue_owner" ON "issue" ("ownerID");
    CREATE INDEX "issue_closed_title" ON "issue" ("closed", "title" DESC);
    CREATE TABLE "no_pk" ("a" TEXT);
  `);
  initReplicationState(db, ['zero_all'], '123');
  return {db, file};
}

function seed(db: Database, rows: number) {
  const insert = db.prepare(
    `INSERT INTO "issue" ("id", "title", "ownerID", "closed") VALUES (?, ?, ?, ?)`,
  );
  db.exec('BEGIN');
  for (let i = 0; i < rows; i++) {
    // Every owner is the sentinel, so a stat4 sample of "issue_owner" that
    // leaked its value would contain it.
    insert.run(`id-${i}`, `${SENTINEL}-${i}`, SENTINEL, i % 10 ? 1 : 0);
  }
  db.exec('COMMIT');
}

function config(file: string): NormalizedZeroConfig {
  return {
    adminPassword: 'secret',
    replica: {file},
    log: {level: 'error'},
    enableQueryPlanner: true,
    serverVersion: '0.0.0-test',
  } as unknown as NormalizedZeroConfig;
}

function tableNamed(bundle: PlannerzBundle, name: string) {
  const table = bundle.tables.find(t => t.name === name);
  expect(table, `no table ${name}`).toBeDefined();
  return table!;
}

describe('parseStat1', () => {
  test.each([
    ['20000 1001', {rows: 20000, avgRowsPerPrefix: [1001], flags: []}],
    ['20000 1001 3', {rows: 20000, avgRowsPerPrefix: [1001, 3], flags: []}],
    ['100', {rows: 100, avgRowsPerPrefix: [], flags: []}],
    [
      '100 5 unordered sz=42',
      {rows: 100, avgRowsPerPrefix: [5], flags: ['unordered', 'sz=42']},
    ],
    ['', {rows: 0, avgRowsPerPrefix: [], flags: []}],
  ])('%s', (raw, expected) => {
    expect(parseStat1(raw)).toEqual({raw, ...expected});
  });
});

describe('summarizeStat4', () => {
  function sample(nEq: number[], nDLt: number[]): Stat4Sample {
    return {nEq, nLt: nEq.map(() => 0), nDLt, sample: ['text']};
  }

  test('digests counts per key prefix depth', () => {
    expect(
      summarizeStat4([
        sample([10, 1], [0, 0]),
        sample([2, 1], [40, 90]),
        sample([6, 1], [20, 50]),
      ]),
    ).toEqual({
      samples: 3,
      sampleKinds: ['text'],
      perPrefix: [
        {maxRowsPerKey: 10, medianRowsPerKey: 6, estimatedDistinctKeys: 41},
        {maxRowsPerKey: 1, medianRowsPerKey: 1, estimatedDistinctKeys: 91},
      ],
    });
  });

  test('does not depend on the order samples arrive in', () => {
    const samples = [
      sample([10, 1], [0, 0]),
      sample([2, 1], [40, 90]),
      sample([6, 1], [20, 50]),
    ];
    expect(summarizeStat4(samples)).toEqual(
      summarizeStat4(samples.toReversed()),
    );
  });
});

describe('plannerz bundle', () => {
  let db: Database;
  let file: string;

  beforeEach(() => {
    ({db, file} = newReplica('bundle'));
  });

  afterEach(() => {
    db.close();
  });

  test('schema and indexes, before any statistics exist', () => {
    const bundle = buildPlannerzBundleFromDB(lc, db, config(file));

    expect(bundle.statsQuality).toEqual({
      stat1Present: false,
      stat4Rows: 0,
      method: expect.stringContaining('analysis_limit'),
      tablesWithoutStats: ['issue', 'no_pk'],
    });
    expect(bundle.server.replicaWatermark).toBe('123');
    expect(bundle.server.planner).toEqual({
      enableQueryPlanner: true,
      enableCorrelatedPredicatePushdown: true,
      enablePlannerAwarePushdown: true,
    });

    const issue = tableNamed(bundle, 'issue');
    expect(issue.syncable).toBe(true);
    expect(issue.primaryKey).toEqual(['id']);
    expect(issue.estimatedRows).toBeUndefined();
    expect(issue.columns).toEqual([
      // `nullable` is what the catalog says. SQLite does not imply NOT NULL for
      // a TEXT PRIMARY KEY of a rowid table, so `id` reports as nullable here.
      {name: 'id', dataType: 'TEXT', nullable: true, zqlType: 'string'},
      {name: 'title', dataType: 'TEXT', nullable: true, zqlType: 'string'},
      {name: 'ownerID', dataType: 'TEXT', nullable: true, zqlType: 'string'},
      {name: 'closed', dataType: 'int8', nullable: false, zqlType: 'number'},
    ]);
    expect(issue.indexes).toEqual([
      {
        name: 'issue_closed_title',
        columns: [
          {name: 'closed', dir: 'ASC'},
          {name: 'title', dir: 'DESC'},
        ],
        unique: false,
        partial: false,
        stat1: undefined,
        stat4: undefined,
      },
      {
        name: 'issue_owner',
        columns: [{name: 'ownerID', dir: 'ASC'}],
        unique: false,
        partial: false,
        stat1: undefined,
        stat4: undefined,
      },
      // The index SQLite creates for the primary key. It is reported because
      // the planner can use it, and because it shows which key is unique.
      {
        name: 'sqlite_autoindex_issue_1',
        columns: [{name: 'id', dir: 'ASC'}],
        unique: true,
        partial: false,
        stat1: undefined,
        stat4: undefined,
      },
    ]);
  });

  test('a table with no unique key is reported as not syncable', () => {
    const bundle = buildPlannerzBundleFromDB(lc, db, config(file));
    expect(tableNamed(bundle, 'no_pk').syncable).toBe(false);
  });

  test('internal tables are left out', () => {
    const bundle = buildPlannerzBundleFromDB(lc, db, config(file));
    expect(bundle.tables.map(t => t.name)).toEqual(['issue', 'no_pk']);
  });

  test('stat1 after PRAGMA optimize, which collects no stat4', () => {
    seed(db, 2000);
    db.pragma('analysis_limit = 1000');
    db.pragma('optimize');

    const bundle = buildPlannerzBundleFromDB(lc, db, config(file));

    expect(bundle.statsQuality.stat1Present).toBe(true);
    expect(bundle.statsQuality.stat4Rows).toBe(0);
    expect(bundle.statsQuality.method).toContain('analysis_limit');
    expect(bundle.statsQuality.tablesWithoutStats).toEqual(['no_pk']);

    const issue = tableNamed(bundle, 'issue');
    expect(issue.estimatedRows).toBe(2000);
    const owner = issue.indexes.find(i => i.name === 'issue_owner');
    expect(owner?.stat1?.rows).toBe(2000);
    // Every row has the same owner, so the true average is 2000. The limit
    // stops the sampling early and reports 1001 instead, which is exactly the
    // approximation the bundle warns about in `statsQuality.method`.
    expect(owner?.stat1?.avgRowsPerPrefix).toEqual([1001]);
    expect(owner?.stat4).toBeUndefined();
  });

  test('stat4 after a full ANALYZE is digested, with values redacted', () => {
    seed(db, 2000);
    db.exec('ANALYZE');

    const bundle = buildPlannerzBundleFromDB(lc, db, config(file));

    expect(bundle.statsQuality.stat4Rows).toBeGreaterThan(0);
    expect(bundle.statsQuality.method).toContain('ANALYZE');

    const issue = tableNamed(bundle, 'issue');
    const closedTitle = issue.indexes.find(
      i => i.name === 'issue_closed_title',
    );
    const stat4 = closedTitle?.stat4;
    expect(stat4?.samples).toBeGreaterThan(0);
    // One entry per key column ("closed", "title"), plus the rowid.
    expect(stat4?.sampleKinds).toEqual(['integer', 'text', 'integer']);

    // "closed" holds two values, 10% of rows in one of them. That skew is what
    // stat1's flat average hides, and what the digest is for.
    const [byClosed] = stat4!.perPrefix;
    expect(byClosed.estimatedDistinctKeys).toBe(2);
    expect(byClosed.maxRowsPerKey).toBeGreaterThan(1000);
    expect(closedTitle?.stat1?.avgRowsPerPrefix[0]).toBe(1000);

    // The raw histogram is not included by default.
    expect(closedTitle?.stat4Samples).toBeUndefined();

    // The values themselves never appear.
    expect(JSON.stringify(bundle)).not.toContain(SENTINEL);
  });

  test('stat4=full adds the raw histogram, still with values redacted', () => {
    seed(db, 2000);
    db.exec('ANALYZE');

    const bundle = buildPlannerzBundleFromDB(lc, db, config(file), {
      fullStat4: true,
    });

    const samples = tableNamed(bundle, 'issue').indexes.find(
      i => i.name === 'issue_closed_title',
    )?.stat4Samples;
    expect(samples?.length).toBe(
      tableNamed(bundle, 'issue').indexes.find(
        i => i.name === 'issue_closed_title',
      )?.stat4?.samples,
    );
    for (const sample of samples!) {
      expect(sample.sample).toEqual(['integer', 'text', 'integer']);
      expect(sample.nEq).toHaveLength(3);
      expect(sample.nLt).toHaveLength(3);
      expect(sample.nDLt).toHaveLength(3);
    }
    expect(JSON.stringify(bundle)).not.toContain(SENTINEL);
  });

  test('no row values anywhere in the response, with statistics of every kind', () => {
    seed(db, 2000);
    db.exec('ANALYZE');
    const serialized = JSON.stringify(
      buildPlannerzBundleFromDB(lc, db, config(file)),
    );
    expect(serialized).not.toContain(SENTINEL);
    expect(serialized).not.toContain('id-1');
  });
});

describe('plannerz endpoint', () => {
  let db: Database;
  let file: string;
  let app: FastifyInstance;

  beforeEach(async () => {
    ({db, file} = newReplica('endpoint'));
    seed(db, 100);
    db.exec('ANALYZE');
    db.close();

    app = fastify();
    app.get('/plannerz', (req, res) =>
      handlePlannerzRequest(lc, config(file), req, res),
    );
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
  });

  test('401 without a password', async () => {
    const res = await app.inject({method: 'GET', url: '/plannerz'});
    expect(res.statusCode).toBe(401);
    expect(res.headers['www-authenticate']).toBe(
      'Basic realm="Plannerz Protected Area"',
    );
  });

  test('401 with the wrong password', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/plannerz',
      headers: {
        authorization: `Basic ${Buffer.from('user:wrong').toString('base64')}`,
      },
    });
    expect(res.statusCode).toBe(401);
  });

  test('returns the bundle, and never row values', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/plannerz?pretty',
      headers: {
        authorization: `Basic ${Buffer.from('user:secret').toString('base64')}`,
      },
    });

    expect(res.statusCode).toBe(200);
    expect(res.headers['content-type']).toContain('application/json');
    expect(res.body).not.toContain(SENTINEL);

    const bundle = JSON.parse(res.body) as PlannerzBundle;
    expect(bundle.about.whatThisIs).toContain('no row data');
    expect(bundle.server.zeroVersion).toBe('0.0.0-test');
    expect(tableNamed(bundle, 'issue').estimatedRows).toBe(100);
  });
});
