import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect} from 'vitest';
import {TestLogSink} from '../../../../../../shared/src/logging-test-utils.ts';
import {Index} from '../../../../db/postgres-replica-identity-enum.ts';
import {
  expectTables,
  expectTablesToMatch,
  initDB,
  type PgTest,
  test,
} from '../../../../test/db.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import {
  createReplicaAndSlot,
  type ReplicationSlotResult,
} from '../replication-slots.ts';
import {getPublicationInfo} from './published.ts';
import * as ReplicaStage from './replica-stage-enum.ts';
import {
  createReplica,
  ensureGlobalTables,
  getReplicaState,
  getRestoreCandidates,
  initInitialSyncReplica,
  initRestoreReplica,
  metadataPublicationName,
  replicaIdentitiesForTablesWithoutPrimaryKeys,
  setupTablesAndReplication,
  setupTriggers,
  shardSetup,
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
      await createReplica(
        tx,
        {appID: APP_ID, shardNum: 0},
        '12345',
        'zro_0_1234',
        0,
        '0wdfj02',
        {backupPath: '12345', backupV5: true},
        ReplicaStage.InitialSync,
      );
      await initInitialSyncReplica(
        tx,
        {appID: APP_ID, shardNum: 0},
        '12345',
        {tables: [], indexes: []},
        {foo: 'bar'},
      );
    });

    expect(await publications()).toEqual([
      [`_zro_metadata_0`, 'zro', 'permissions', null],
      [`_zro_metadata_0`, `zro_0`, 'clients', null],
      [`_zro_metadata_0`, `zro_0`, 'mutations', null],
      [`_zro_metadata_0`, `zro_0`, 'replicas', null],
      ['_zro_public_0', null, null, null],
    ]);

    await expectTablesToMatch(db, {
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
          id: /\d{10,}/,
          slot: 'zro_0_1234',
          version: null,
          epoch: 0,
          generation: '0wdfj02',
          stage: ReplicaStage.InitialSync,
          backupPath: '12345',
          backupV5: true,
          initialSchema: {tables: [], indexes: []},
          initialSyncContext: {foo: 'bar'},
          subscriberContext: null,
        },
      ],
      ['zro_0.clients']: [],
    });

    expect(
      (await db`SELECT evtname from pg_event_trigger`.values()).flat(),
    ).toEqual(['zro_ddl_start_0', 'zro_ddl_end_0']);
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
      [`_zro_metadata_0`, `zro_0`, 'replicas', null],
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

  test('partial index is not selected as replica identity', async () => {
    await db.unsafe(`
      CREATE TABLE identity_test(id TEXT NOT NULL, active BOOLEAN NOT NULL);
      CREATE UNIQUE INDEX partial_key ON identity_test (id)
        WHERE active = true;
      CREATE UNIQUE INDEX full_key ON identity_test (id);
      CREATE PUBLICATION zero_identity_test FOR TABLE identity_test;
    `);

    const pubs = await getPublicationInfo(db, ['zero_identity_test']);
    const partialFirst = pubs.indexes.toSorted((a, b) =>
      a.predicate === undefined ? 1 : b.predicate === undefined ? -1 : 0,
    );
    await replicaIdentitiesForTablesWithoutPrimaryKeys({
      ...pubs,
      indexes: partialFirst,
    })?.apply(lc, db);

    const updated = await getPublicationInfo(db, ['zero_identity_test']);
    expect(
      updated.indexes
        .filter(index => index.isReplicaIdentity)
        .map(index => index.name),
    ).toEqual(['full_key']);
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
      [`_1_metadata_0`, `1_0`, 'replicas', null],
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
      [`_zro_metadata_0`, `zro_0`, 'replicas', null],
      [`_zro_metadata_1`, 'zro', 'permissions', null],
      [`_zro_metadata_1`, `zro_1`, 'clients', null],
      [`_zro_metadata_1`, `zro_1`, 'mutations', null],
      [`_zro_metadata_1`, `zro_1`, 'replicas', null],
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
      [`_zro_metadata_2`, `zro_2`, 'replicas', null],
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
      ['_supaneon_metadata_0', 'supaneon_0', 'replicas', null],
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

  test('trigger upgrade failure detected', async () => {
    const shardConfig = {
      appID: 'woo',
      shardNum: 0,
      publications: ['zero_foo'],
    };

    await db /*sql*/ `
      CREATE TABLE foo(id INT4 PRIMARY KEY);
      CREATE PUBLICATION zero_foo FOR TABLE foo;
    `.simple();
    await db.begin(tx => setupTablesAndReplication(lc, tx, shardConfig));
    await expectTables(db, {
      ['woo_0.shardConfig']: [
        {
          lock: true,
          publications: ['_woo_metadata_0', 'zero_foo'],
          ddlDetection: true,
        },
      ],
    });
    expect(
      await db`SELECT evtname from pg_event_trigger`.values(),
    ).toMatchObject([['woo_ddl_start_0'], ['woo_ddl_end_0']]);

    // Now try to upgrade as a different user.
    await db /*sql*/ `
      CREATE ROLE different_user IN ROLE current_user;
      SET ROLE different_user;
    `.simple();

    await expect(
      db.begin(tx => setupTriggers(lc, tx, shardConfig)),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `[PostgresError: permission denied to create event trigger "woo_ddl_start_0"]`,
    );
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

  test('publication must publish updates', () => {
    expect(() =>
      validatePublications(lc, {
        publications: [
          {
            pubname: 'zero_data',
            pubinsert: true,
            pubupdate: false,
            pubdelete: true,
            pubtruncate: true,
          },
        ],
        tables: [],
        indexes: [],
      }),
    ).toThrowError(
      'PUBLICATION zero_data must publish insert, update, delete, and truncate',
    );
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

describe('getRestoreCandidates / initRestoreReplica', () => {
  const APP_ID = 'zro';
  const SHARD_NUM = 0;
  const shard = {appID: APP_ID, shardNum: SHARD_NUM};
  const schema = `${APP_ID}_${SHARD_NUM}`;

  let lc: LogContext;
  let db: PostgresDB;

  beforeEach<PgTest>(async ({testDBs}) => {
    lc = new LogContext('warn', {}, new TestLogSink());
    db = await testDBs.create('restore_candidates_test');
    const metadataPub = metadataPublicationName(APP_ID, SHARD_NUM);
    await ensureGlobalTables(db, shard);
    await db.unsafe(
      shardSetup({...shard, publications: [metadataPub]}, metadataPub),
    );

    return async () => {
      await testDBs.drop(db);
    };
  });

  // Creates an inactive logical slot. Slots created via the SQL function
  // (as opposed to a walsender session) are `active = false`, which is all
  // that's needed to exercise the JOIN / filtering / ordering logic.
  async function createSlot(name: string) {
    await db`SELECT pg_create_logical_replication_slot(${name}, 'pgoutput')`;
  }

  async function addReplica(
    id: string,
    slot: string | null,
    {
      epoch = 0,
      generation,
      stage,
      backupV5 = true,
      backupPath = id,
    }: {
      epoch?: number;
      generation: string;
      stage:
        | ReplicaStage.InitialSync
        | ReplicaStage.Replicate
        | ReplicaStage.Restore;
      backupV5?: boolean;
      backupPath?: string | null;
    },
  ) {
    if (slot) {
      await createSlot(slot);
    }
    await createReplica(
      db,
      shard,
      id,
      slot ?? `${id}_missing_slot`,
      epoch,
      generation,
      {backupPath, backupV5},
      stage,
    );
  }

  // Sessions that hold their slots `active`; released on teardown so the
  // slots can be dropped.
  const sessions: ReplicationSlotResult<unknown>[] = [];
  // eslint-disable-next-line require-await
  afterEach(async () => {
    for (const {initialSession} of sessions.splice(0)) {
      initialSession.destroy();
    }
  });

  // Creates a replica whose slot is `active` (held by a walsender session),
  // then overrides its row to the generation / stage under test. The slot is
  // created in the Restore stage so it doesn't trip the initial-sync guard.
  async function addActiveReplica(
    id: string,
    {
      epoch = 0,
      generation,
      stage,
      backupV5 = true,
      backupPath = id,
    }: {
      epoch?: number;
      generation: string;
      stage:
        | ReplicaStage.InitialSync
        | ReplicaStage.Replicate
        | ReplicaStage.Restore;
      backupV5?: boolean;
      backupPath?: string | null;
    },
  ) {
    const result = await createReplicaAndSlot(
      lc,
      db,
      `session-${id}`,
      shard,
      epoch,
      id,
      false,
      {backupPath, backupV5},
      snapshot => Promise.resolve(snapshot),
      ReplicaStage.Restore,
    );
    sessions.push(result);
    await db`
      UPDATE ${db(schema)}.replicas
        SET generation = ${generation}, stage = ${stage}, epoch = ${epoch},
            "backupV5" = ${backupV5}, "backupPath" = ${backupPath}
        WHERE id = ${id}`;
  }

  test('coalesces on the newest generation within the epoch', async () => {
    // Older generation in the same epoch: excluded by the MAX() coalescing.
    await addReplica('old', 'zro_0_a', {
      generation: 'aaa',
      stage: ReplicaStage.Replicate,
    });
    // Two siblings sharing the winning generation.
    await addReplica('live', 'zro_0_b', {
      generation: 'ccc',
      stage: ReplicaStage.Replicate,
    });
    await addReplica('sib', 'zro_0_c', {
      generation: 'ccc',
      stage: ReplicaStage.Restore,
    });
    // A freshly-created Restore row not yet initialized (empty generation)
    // must never win MAX() and must be excluded.
    await addReplica('forking', 'zro_0_d', {
      generation: '',
      stage: ReplicaStage.Restore,
    });
    // A higher generation but backupV5 = false must NOT raise the coalesced
    // generation (the MAX() subquery filters on backupV5 = true).
    await addReplica('v3', 'zro_0_e', {
      generation: 'zzz',
      stage: ReplicaStage.Replicate,
      backupV5: false,
    });
    // A row at the winning generation but with no live slot is excluded by
    // the JOIN against pg_replication_slots.
    await addReplica('noslot', null, {
      generation: 'ccc',
      stage: ReplicaStage.Replicate,
    });
    // A different epoch must not influence the coalesced generation.
    await addReplica('otherEpoch', 'zro_0_f', {
      epoch: 1,
      generation: 'ddd',
      stage: ReplicaStage.Replicate,
    });

    const candidates = await getRestoreCandidates(lc, db, shard, 0);
    expect(candidates.map(c => c.id)).toEqual(['live', 'sib']);
    expect(candidates.map(c => c.generation)).toEqual(['ccc', 'ccc']);
    // stage ASC: Replicate (1) before Restore (2).
    expect(candidates.map(c => c.stage)).toEqual([
      ReplicaStage.Replicate,
      ReplicaStage.Restore,
    ]);
    expect(candidates.every(c => c.active === false)).toBe(true);
  });

  test('returns empty when no backupV5 replica exists in the epoch', async () => {
    await addReplica('legacy', 'zro_0_a', {
      generation: 'aaa',
      stage: ReplicaStage.Replicate,
      backupV5: false,
    });
    expect(await getRestoreCandidates(lc, db, shard, 0)).toEqual([]);
  });

  test('initRestoreReplica carries generation and context from the source', async () => {
    await addReplica('source', 'zro_0_a', {
      generation: 'srcgen',
      stage: ReplicaStage.Replicate,
      backupPath: 'source-backup',
    });
    await initInitialSyncReplica(
      db,
      shard,
      'source',
      {tables: [], indexes: []},
      {
        foo: 'bar',
      },
    );
    await addReplica('dest', 'zro_0_b', {
      generation: '', // as created by createReplicaAndSlot for a Restore replica
      stage: ReplicaStage.Restore,
      backupPath: 'dest-backup',
    });

    const returned = await initRestoreReplica(db, shard, {
      sourceID: 'source',
      destID: 'dest',
    });
    expect(returned).toMatchObject({
      id: 'dest',
      slot: 'zro_0_b',
      stage: ReplicaStage.Restore,
      generation: 'srcgen',
      backupPath: 'dest-backup', // unchanged
      active: false,
    });

    // The generation, schema, and sync context are copied; the slot and
    // backupPath are left intact.
    const [dest] = await db`
      SELECT generation, "initialSchema", "initialSyncContext", "backupPath", slot, stage
        FROM ${db(schema)}.replicas WHERE id = 'dest'`;
    expect(dest).toMatchObject({
      generation: 'srcgen',
      initialSchema: {tables: [], indexes: []},
      initialSyncContext: {foo: 'bar'},
      backupPath: 'dest-backup',
      slot: 'zro_0_b',
      stage: ReplicaStage.Restore,
    });

    // Source is untouched.
    const source = await getReplicaState(db, shard, 'source');
    expect(source).toMatchObject({id: 'source', generation: 'srcgen'});
  });

  test('skips an orphaned (inactive) initial-sync at a higher generation', async () => {
    // An initial-sync that crashed: highest generation, but inactive.
    await addReplica('deadSync', 'zro_0_a', {
      generation: 'zzz',
      stage: ReplicaStage.InitialSync,
    });
    // A completed generation below it.
    await addReplica('done', 'zro_0_b', {
      generation: 'ccc',
      stage: ReplicaStage.Replicate,
    });

    // The dead initial-sync's generation is ignored; coalesce on 'ccc'.
    const candidates = await getRestoreCandidates(lc, db, shard, 0);
    expect(candidates.map(c => c.id)).toEqual(['done']);
  });

  test('returns empty for a solo orphaned initial-sync', async () => {
    // The only replica is a crashed initial-sync (inactive). With nothing
    // restorable, the caller must fall back to a fresh initial sync.
    await addReplica('deadSync', 'zro_0_a', {
      generation: 'zzz',
      stage: ReplicaStage.InitialSync,
    });
    expect(await getRestoreCandidates(lc, db, shard, 0)).toEqual([]);
  });

  test('an active replica is not masked by a higher, inactive initial-sync', async () => {
    // Regression: the MAX() subquery must evaluate `active` against the
    // subquery's own slot (via its JOIN), not correlate to the outer row.
    // Otherwise the active 'serving' row computes MAX() = 'zzz' for itself
    // and excludes itself, wrongly returning [].
    await addReplica('deadSync', 'zro_0_x1', {
      generation: 'zzz',
      stage: ReplicaStage.InitialSync,
    });
    await addActiveReplica('serving', {
      generation: 'ccc',
      stage: ReplicaStage.Replicate,
    });

    const candidates = await getRestoreCandidates(lc, db, shard, 0);
    expect(candidates.map(c => c.id)).toEqual(['serving']);
    expect(candidates[0].active).toBe(true);
  });

  test('waits on an active initial-sync, coalescing on its generation', async () => {
    // A completed generation exists, but a newer initial-sync is actively
    // running: coalesce on the active initial-sync (wait for it), not the
    // older completed generation.
    await addReplica('done', 'zro_0_x1', {
      generation: 'ccc',
      stage: ReplicaStage.Replicate,
    });
    await addActiveReplica('syncing', {
      generation: 'zzz',
      stage: ReplicaStage.InitialSync,
    });

    const candidates = await getRestoreCandidates(lc, db, shard, 0);
    expect(candidates.map(c => c.id)).toEqual(['syncing']);
    expect(candidates[0].stage).toBe(ReplicaStage.InitialSync);
    expect(candidates[0].active).toBe(true);
  });
});
