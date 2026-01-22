import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect} from 'vitest';
import {TestLogSink} from '../../../../../../shared/src/logging-test-utils.ts';
import {Index} from '../../../../db/postgres-replica-identity-enum.ts';
import {expectTables, initDB, type PgTest, test} from '../../../../test/db.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import {getPublicationInfo} from './published.ts';
import {
  addReplica,
  setupTablesAndReplication,
  validatePublicationName,
  validatePublications,
} from './shard.ts';

const APP_ID = 'zro';

describe('change-source/pg', () => {
  let logSink: TestLogSink;
  let lc: LogContext;
  let db: PostgresDB;

  beforeEach<PgTest>(async ({testDBs}) => {
    logSink = new TestLogSink();
    lc = new LogContext('warn', {}, logSink);
    db = await testDBs.create('zero_schema_test');

    return async () => {
      await testDBs.drop(db);
      await testDBs.sql`RESET ROLE; DROP ROLE IF EXISTS supaneon`.simple();
    };
  });

  function publications() {
    return db<{pubname: string; rowfilter: string | null}[]>`
    SELECT p.pubname, t.schemaname, t.tablename, rowfilter FROM pg_publication p
      LEFT JOIN pg_publication_tables t ON p.pubname = t.pubname 
      ORDER BY p.pubname`.values();
  }

  test('default publication, schema version setup', async () => {
    await db.begin(async tx => {
      await setupTablesAndReplication(lc, tx, {
        appID: APP_ID,
        shardNum: 0,
        publications: [],
      });
      await addReplica(
        tx,
        {appID: APP_ID, shardNum: 0},
        'zro_0_1234',
        '0wdfj02',
        {tables: [], indexes: []},
      );
    });

    expect(await publications()).toEqual([
      [`_zro_metadata_0`, 'zro', 'permissions', null],
      [`_zro_metadata_0`, `zro_0`, 'clients', null],
      [`_zro_metadata_0`, `zro_0`, 'mutations', null],
      ['_zro_public_0', null, null, null],
    ]);

    await expectTables(db, {
      ['zro.permissions']: [{lock: true, permissions: null, hash: null}],
      ['zro_0.shardConfig']: [
        {
          lock: true,
          publications: ['_zro_metadata_0', '_zro_public_0'],
          ddlDetection: true,
        },
      ],
      ['zro_0.replicas']: [
        {
          slot: 'zro_0_1234',
          version: '0wdfj02',
          initialSchema: {tables: [], indexes: []},
        },
      ],
      ['zro_0.clients']: [],
    });

    expect(
      (await db`SELECT evtname from pg_event_trigger`.values()).flat(),
    ).toEqual([
      'zro_ddl_start_0',
      'zro_create_table_0',
      'zro_alter_table_0',
      'zro_create_index_0',
      'zro_drop_table_0',
      'zro_drop_index_0',
      'zro_alter_publication_0',
      'zro_alter_schema_0',
    ]);
  });

  test('default publication, join table', async () => {
    await db.unsafe(`
    CREATE TABLE join_table(id1 TEXT NOT NULL, id2 TEXT NOT NULL);
    CREATE UNIQUE INDEX join_key ON join_table (id1, id2);
    INSERT INTO join_table (id1, id2) VALUES ('foo', 'bar');
    `);

    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {
        appID: APP_ID,
        shardNum: 0,
        publications: [],
      }),
    );

    expect(await publications()).toEqual([
      [`_zro_metadata_0`, 'zro', 'permissions', null],
      [`_zro_metadata_0`, `zro_0`, 'clients', null],
      [`_zro_metadata_0`, `zro_0`, 'mutations', null],
      ['_zro_public_0', 'public', 'join_table', null],
    ]);

    await expectTables(db, {
      ['zro.permissions']: [{lock: true, permissions: null, hash: null}],
      ['zro_0.shardConfig']: [
        {
          lock: true,
          publications: ['_zro_metadata_0', '_zro_public_0'],
          ddlDetection: true,
        },
      ],
      ['zro_0.replicas']: [],
      ['zro_0.clients']: [],
      ['join_table']: [{id1: 'foo', id2: 'bar'}],
    });

    const pubs = await getPublicationInfo(db, ['_zro_public_0']);
    const table = pubs.tables.find(t => t.name === 'join_table');
    expect(table?.replicaIdentity).toBe(Index);

    const index = pubs.indexes.find(idx => idx.name === 'join_key');
    expect(index?.isReplicaIdentity).toBe(true);
  });

  test('numeric app ID', async () => {
    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {
        appID: '1',
        shardNum: 0,
        publications: [],
      }),
    );

    expect(await publications()).toEqual([
      [`_1_metadata_0`, '1', 'permissions', null],
      [`_1_metadata_0`, `1_0`, 'clients', null],
      [`_1_metadata_0`, `1_0`, 'mutations', null],
      [`_1_public_0`, null, null, null],
    ]);

    await expectTables(db, {
      ['1.permissions']: [{lock: true, permissions: null, hash: null}],
      [`1_0.shardConfig`]: [
        {
          lock: true,
          publications: [`_1_metadata_0`, `_1_public_0`],
          ddlDetection: true,
        },
      ],
      ['1_0.replicas']: [],
      [`1_0.clients`]: [],
    });
  });

  test('multiple shards', async () => {
    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {
        appID: APP_ID,
        shardNum: 0,
        publications: [],
      }),
    );
    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {
        appID: APP_ID,
        shardNum: 1,
        publications: [],
      }),
    );

    expect(await publications()).toEqual([
      [`_zro_metadata_0`, 'zro', 'permissions', null],
      [`_zro_metadata_0`, `zro_0`, 'clients', null],
      [`_zro_metadata_0`, `zro_0`, 'mutations', null],
      [`_zro_metadata_1`, 'zro', 'permissions', null],
      [`_zro_metadata_1`, `zro_1`, 'clients', null],
      [`_zro_metadata_1`, `zro_1`, 'mutations', null],
      ['_zro_public_0', null, null, null],
      ['_zro_public_1', null, null, null],
    ]);

    await expectTables(db, {
      ['zro.permissions']: [{lock: true, permissions: null, hash: null}],
      ['zro_0.shardConfig']: [
        {
          lock: true,
          publications: ['_zro_metadata_0', '_zro_public_0'],
          ddlDetection: true,
        },
      ],
      ['zro_0.replicas']: [],
      ['zro_0.clients']: [],
      ['zro_1.shardConfig']: [
        {
          lock: true,
          publications: ['_zro_metadata_1', '_zro_public_1'],
          ddlDetection: true,
        },
      ],
      ['zro_1.clients']: [],
    });
  });

  test('unknown publications', async () => {
    let err;
    try {
      await db.begin(tx =>
        setupTablesAndReplication(lc, tx, {
          appID: APP_ID,
          shardNum: 0,
          publications: ['zero_invalid'],
        }),
      );
    } catch (e) {
      err = e;
    }
    expect(err).toMatchInlineSnapshot(
      `[Error: Unknown or invalid publications. Specified: [zero_invalid]. Found: []]`,
    );

    expect(await publications()).toEqual([]);
  });

  test('reserved publication name', async () => {
    let err;
    try {
      await db.begin(tx =>
        setupTablesAndReplication(lc, tx, {
          appID: APP_ID,
          shardNum: 0,
          publications: ['_foo_bar'],
        }),
      );
    } catch (e) {
      err = e;
    }
    expect(err).toMatchInlineSnapshot(`
      [Error: Publication names starting with "_" are reserved for internal use.
      Please use a different name for publication "_foo_bar".]
    `);

    expect(await publications()).toEqual([]);
  });

  test('supplied publications', async () => {
    await db`
    CREATE SCHEMA far;
    CREATE TABLE foo(id INT4 PRIMARY KEY);
    CREATE TABLE far.bar(id TEXT PRIMARY KEY);
    CREATE PUBLICATION zero_foo FOR TABLE foo WHERE (id > 1000);
    CREATE PUBLICATION zero_bar FOR TABLE far.bar;`.simple();

    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {
        appID: APP_ID,
        shardNum: 2,
        publications: ['zero_foo', 'zero_bar'],
      }),
    );

    expect(await publications()).toEqual([
      [`_zro_metadata_2`, 'zro', 'permissions', null],
      [`_zro_metadata_2`, `zro_2`, 'clients', null],
      [`_zro_metadata_2`, `zro_2`, 'mutations', null],
      ['zero_bar', 'far', 'bar', null],
      ['zero_foo', 'public', 'foo', '(id > 1000)'],
    ]);

    await expectTables(db, {
      ['zro.permissions']: [{lock: true, permissions: null, hash: null}],
      ['zro_2.shardConfig']: [
        {
          lock: true,
          publications: ['_zro_metadata_2', 'zero_bar', 'zero_foo'],
          ddlDetection: true,
        },
      ],
      ['zro_2.replicas']: [],
      ['zro_2.clients']: [],
    });
  });

  test('non-superuser: ddlDetection = false', async () => {
    await db`
    CREATE TABLE foo(id INT4 PRIMARY KEY);
    CREATE PUBLICATION zero_foo FOR TABLE foo;
    
    CREATE ROLE supaneon NOSUPERUSER IN ROLE current_user;
    SET ROLE supaneon;
    `.simple();

    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {
        appID: 'supaneon',
        shardNum: 0,
        publications: ['zero_foo'],
      }),
    );

    expect(await publications()).toEqual([
      [`_supaneon_metadata_0`, 'supaneon', 'permissions', null],
      [`_supaneon_metadata_0`, `supaneon_0`, 'clients', null],
      ['_supaneon_metadata_0', 'supaneon_0', 'mutations', null],
      ['zero_foo', 'public', 'foo', null],
    ]);

    await expectTables(db, {
      ['supaneon.permissions']: [{lock: true, permissions: null, hash: null}],
      ['supaneon_0.shardConfig']: [
        {
          lock: true,
          publications: ['_supaneon_metadata_0', 'zero_foo'],
          ddlDetection: false, // degraded mode
        },
      ],
      ['supaneon_0.replicas']: [],
      ['supaneon_0.clients']: [],
    });

    expect(logSink.messages[0]).toMatchInlineSnapshot(`
      [
        "warn",
        {},
        [
          "Unable to create event triggers for schema change detection:

      "Must be superuser to create an event trigger."

      Proceeding in degraded mode: schema changes will halt replication,
      requiring the replica to be reset (manually or with --auto-reset).",
        ],
      ]
    `);

    expect(await db`SELECT evtname from pg_event_trigger`.values()).toEqual([]);
  });

  test('permissions hash trigger', async () => {
    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {
        appID: APP_ID,
        shardNum: 0,
        publications: [],
      }),
    );
    await db`UPDATE zro.permissions SET permissions = ${{tables: {foo: {}}}}`;
    expect(await db`SELECT hash FROM zro.permissions`).toMatchInlineSnapshot(`
      Result [
        {
          "hash": "b2f6c5d807ae3b9536735f37302b3d82",
        },
      ]
    `);
    await db`UPDATE zro.permissions SET permissions = NULL`;
    expect(await db`SELECT hash FROM zro.permissions`).toMatchInlineSnapshot(`
      Result [
        {
          "hash": null,
        },
      ]
    `);
    await db`UPDATE zro.permissions SET permissions = ${{tables: {bar: {}}}}`;
    expect(await db`SELECT hash FROM zro.permissions`).toMatchInlineSnapshot(`
      Result [
        {
          "hash": "9042ec772bb48666c9c497b6d7f59a3a",
        },
      ]
    `);
    await db`DELETE FROM zro.permissions`;
    await db`INSERT INTO zro.permissions ${db({
      permissions: {tables: {foo: {}}},
    })}`;
    expect(await db`SELECT hash FROM zro.permissions`).toMatchInlineSnapshot(`
      Result [
        {
          "hash": "b2f6c5d807ae3b9536735f37302b3d82",
        },
      ]
    `);
  });

  type InvalidUpstreamCase = {
    error: string;
    setupUpstreamQuery: string;
  };

  const invalidUpstreamCases: InvalidUpstreamCase[] = [
    {
      error: 'uses reserved column name "_0_version"',
      setupUpstreamQuery: `
        CREATE TABLE issues(
          "issueID" INTEGER PRIMARY KEY, 
          "orgID" INTEGER, 
          _0_version INTEGER);
      `,
    },
    {
      error: 'Table "table/with/slashes" has invalid characters',
      setupUpstreamQuery: `
        CREATE TABLE "table/with/slashes" ("issueID" INTEGER PRIMARY KEY, "orgID" INTEGER);
      `,
    },
    {
      error: 'Table "table.with.dots" has invalid characters',
      setupUpstreamQuery: `
        CREATE TABLE "table.with.dots" ("issueID" INTEGER PRIMARY KEY, "orgID" INTEGER);
      `,
    },
    {
      error:
        'Column "column/with/slashes" in table "issues" has invalid characters',
      setupUpstreamQuery: `
        CREATE TABLE issues ("issueID" INTEGER PRIMARY KEY, "column/with/slashes" INTEGER);
      `,
    },
  ];

  for (const c of invalidUpstreamCases) {
    test(`Invalid publication: ${c.error}`, async () => {
      await initDB(
        db,
        (c.setupUpstreamQuery ?? '') +
          `CREATE PUBLICATION zero_data FOR TABLES IN SCHEMA public;`,
      );

      const published = await getPublicationInfo(db, ['zero_data']);
      expect(() => validatePublications(lc, published)).toThrowError(c.error);
    });
  }

  test('invalid publication name with special characters', async () => {
    let err;
    try {
      await db.begin(tx =>
        setupTablesAndReplication(lc, tx, {
          appID: APP_ID,
          shardNum: 0,
          publications: ["pub'injection"],
        }),
      );
    } catch (e) {
      err = e;
    }
    expect(err).toMatchInlineSnapshot(
      `[Error: Invalid publication name "pub'injection". Publication names must start with a letter or underscore and contain only letters, digits, and underscores.]`,
    );

    expect(await publications()).toEqual([]);
  });

  test('invalid publication name starting with number', async () => {
    let err;
    try {
      await db.begin(tx =>
        setupTablesAndReplication(lc, tx, {
          appID: APP_ID,
          shardNum: 0,
          publications: ['123pub'],
        }),
      );
    } catch (e) {
      err = e;
    }
    expect(err).toMatchInlineSnapshot(
      `[Error: Invalid publication name "123pub". Publication names must start with a letter or underscore and contain only letters, digits, and underscores.]`,
    );

    expect(await publications()).toEqual([]);
  });

  test('invalid publication name too long', async () => {
    const longName = 'a'.repeat(64);
    let err;
    try {
      await db.begin(tx =>
        setupTablesAndReplication(lc, tx, {
          appID: APP_ID,
          shardNum: 0,
          publications: [longName],
        }),
      );
    } catch (e) {
      err = e;
    }
    expect(String(err)).toContain('exceeds PostgreSQL');
    expect(String(err)).toContain('63-character identifier limit');

    expect(await publications()).toEqual([]);
  });
});

describe('validatePublicationName', () => {
  test('valid names', () => {
    expect(() => validatePublicationName('my_pub')).not.toThrow();
    expect(() => validatePublicationName('Publication1')).not.toThrow();
    expect(() => validatePublicationName('_internal')).not.toThrow();
    expect(() => validatePublicationName('zero_foo')).not.toThrow();
    expect(() => validatePublicationName('a'.repeat(63))).not.toThrow();
  });

  test('invalid names', () => {
    expect(() => validatePublicationName("pub'lic")).toThrow(/Invalid/);
    expect(() => validatePublicationName('pub,list')).toThrow(/Invalid/);
    expect(() => validatePublicationName('123pub')).toThrow(/Invalid/);
    expect(() => validatePublicationName('pub-name')).toThrow(/Invalid/);
    expect(() => validatePublicationName('pub name')).toThrow(/Invalid/);
    expect(() => validatePublicationName('')).toThrow(/Invalid/);
  });

  test('name too long', () => {
    expect(() => validatePublicationName('a'.repeat(64))).toThrow(/exceeds/);
  });
});
