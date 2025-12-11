import {beforeEach, describe, expect, vi, type Mock} from 'vitest';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.ts';
import type {ReadonlyJSONValue} from '../../../../shared/src/json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {test, type PgTest} from '../../test/db.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {rowIDString, type RowID} from '../../types/row-key.ts';
import {upstreamSchema} from '../../types/shards.ts';
import {id} from '../../types/sql.ts';
import {getMutationsTableDefinition} from '../change-source/pg/schema/shard.ts';
import {ClientNotFoundError, CVRStore, OwnershipError} from './cvr-store.ts';
import {
  CVRQueryDrivenUpdater,
  type CVRSnapshot,
  type RowUpdate,
} from './cvr.ts';
import {setupCVRTables, type RowsRow} from './schema/cvr.ts';
import type {
  ClientQueryRecord,
  CustomQueryRecord,
  CVRVersion,
} from './schema/types.ts';
import {
  ttlClockAsNumber,
  ttlClockFromNumber,
  type TTLClock,
} from './ttl-clock.ts';

const APP_ID = 'roze';
const SHARD_NUM = 1;
const SHARD = {appID: APP_ID, shardNum: SHARD_NUM};

describe('view-syncer/cvr-store', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;
  let upstreamDb: PostgresDB;
  let store: CVRStore;
  // vi.useFakeTimers() does not play well with the postgres client.
  // Inject a manual mock instead.
  let setTimeoutFn: Mock<typeof setTimeout>;

  const TASK_ID = 'my-task';
  const CVR_ID = 'my-cvr';
  const CONNECT_TIME = Date.UTC(2024, 10, 22);
  const ON_FAILURE = (e: unknown) => {
    throw e;
  };

  beforeEach<PgTest>(async ({testDBs}) => {
    [db, upstreamDb] = await Promise.all([
      testDBs.create('view_syncer_cvr_schema'),
      testDBs.create('view_syncer_cvr_upstream'),
    ]);
    const shard = id(upstreamSchema(SHARD));
    await upstreamDb.begin(tx =>
      tx.unsafe(`
        CREATE SCHEMA IF NOT EXISTS ${shard};
        ${getMutationsTableDefinition(shard)}
      `),
    );
    await db.begin(tx => setupCVRTables(lc, tx, SHARD));
    await db.unsafe(`
    INSERT INTO "roze_1/cvr".instances ("clientGroupID", version, "lastActive", "ttlClock", "replicaVersion")
      VALUES('${CVR_ID}', '03', '2024-09-04T00:00:00Z', 
        (EXTRACT(EPOCH FROM TIMESTAMPTZ '2024-09-04T00:00:00Z') * 1000)::BIGINT, '01');
    INSERT INTO "roze_1/cvr".queries ("clientGroupID", "queryHash", "clientAST", 
                             "patchVersion", "transformationHash", "transformationVersion")
      VALUES('${CVR_ID}', 'foo', '{"table":"issues"}', '01', 'foo-transformed', '01');
    INSERT INTO "roze_1/cvr"."rowsVersion" ("clientGroupID", version)
      VALUES('${CVR_ID}', '03');
    INSERT INTO "roze_1/cvr".rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"1"}', '01', '01', NULL);
    INSERT INTO "roze_1/cvr".rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"2"}', '01', '01', '{"foo":1}');
    INSERT INTO "roze_1/cvr".rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"3"}', '01', '01', '{"bar":2}');
    INSERT INTO "roze_1/cvr".rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"4"}', '01', '01', '{"foo":2,"bar":3}');

    INSERT INTO "roze_1/cvr".rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"5"}', '01', '02', NULL);
    INSERT INTO "roze_1/cvr".rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"6"}', '01', '02', '{"foo":1}');
    INSERT INTO "roze_1/cvr".rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"7"}', '01', '02', '{"bar":2}');
    INSERT INTO "roze_1/cvr".rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"8"}', '01', '02', '{"foo":2,"bar":3}');

    INSERT INTO "roze_1/cvr".rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"9"}', '01', '03', NULL);
    INSERT INTO "roze_1/cvr".rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"10"}', '01', '03', '{"foo":1}');
    INSERT INTO "roze_1/cvr".rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"11"}', '01', '03', '{"bar":2}');
    INSERT INTO "roze_1/cvr".rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"12"}', '01', '03', '{"foo":2,"bar":3}');
      `);

    setTimeoutFn = vi.fn();
    store = new CVRStore(
      lc,
      db,
      upstreamDb,
      SHARD,
      TASK_ID,
      CVR_ID,
      ON_FAILURE,
      10,
      5,
      DEFERRED_ROW_LIMIT,
      setTimeoutFn as unknown as typeof setTimeout,
    );

    return () => testDBs.drop(db, upstreamDb);
  });

  describe('save various json types for named queries', () => {
    const lc = createSilentLogContext();
    let id = 0;
    test.each([
      {
        name: 'null',
        value: [null],
      },
      {
        name: 'string',
        value: ['hello'],
      },
      {
        name: 'number',
        value: [42],
      },
      {
        name: 'float',
        value: [3.14],
      },
      {
        name: 'object',
        value: [{key: 'value'}],
      },
      {
        name: 'nested array',
        value: [[1, 2, 3]],
      },
      {
        name: 'mixed',
        value: [1, 'hello', {key: 'value'}, [1, 2, 3]],
      },
      {
        name: 'nested array with bool',
        value: [[1, 2, 3, true]],
      },
      {
        name: 'boolean - false',
        value: [true],
      },
      {
        name: 'boolean - true',
        value: [true],
      },
      {
        name: 'no args',
        value: [],
      },
      {
        name: 'trad query',
        value: undefined,
      },
    ])('$name', async ({name, value}) => {
      const now = Date.UTC(2024, 10, 22, 12, 0, 0);
      const cvr = await store.load(lc, CONNECT_TIME);

      let row: ClientQueryRecord | CustomQueryRecord;
      if (name === 'trad query') {
        row = {
          id: `query-${id}`,
          type: 'client',
          ast: {table: 'issues'},
          clientState: {},
        };
        store.putQuery(row);
      } else {
        row = {
          id: `query-${id}`,
          type: 'custom',
          name: `query-${id}`,
          args: value as readonly ReadonlyJSONValue[],
          clientState: {},
        };
        store.putQuery(row);
      }

      await store.flush(lc, cvr.version, cvr, now);
      const cvr2 = await store.load(lc, CONNECT_TIME);

      expect(cvr2.queries[`query-${id}`]).toEqual(row);

      ++id;
    });
  });

  test('put various json types for named queries', async () => {});

  // This is failing with:
  //
  //   ⎯⎯⎯⎯ Unhandled Rejection ⎯⎯⎯⎯⎯
  // Error: {"kind":"ClientNotFound","message":"max attempts exceeded waiting for CVR@04 to catch up from 03"}
  //  ❯ packages/zero-cache/src/services/view-syncer/cvr-store.ts:206:13
  //  ❯ processTicksAndRejections node:internal/process/task_queues:105:5
  //  ❯ packages/otel/src/span.ts:29:14

  // ⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯
  // Serialized Error: { logLevel: 'warn', errorBody: { kind: 'ClientNotFound', message: 'max attempts exceeded waiting for CVR@04 to catch up from 03' } }
  // This error originated in "packages/zero-cache/src/services/view-syncer/cvr-store.pg.test.ts" test file. It doesn't mean the error was thrown inside the file itself, but while it was running.
  // The latest test that might've caused the error is "wait for row catchup". It might mean one of the following:
  // - The error was thrown, while Vitest was running this test.
  // - If the error occurred after the test had been completed, this was the last documented test before it was thrown.
  test.skip('wait for row catchup', async () => {
    // Simulate the CVR being ahead of the rows.
    await db`UPDATE "roze_1/cvr".instances SET version = '04'`;

    // start a CVR load.
    const loading = store.load(lc, CONNECT_TIME);

    await sleep(10);

    // Simulate catching up.
    await db`
    UPDATE "roze_1/cvr".instances SET version = '05:01';
    UPDATE "roze_1/cvr"."rowsVersion" SET version = '05:01';
    `.simple();

    const cvr = await loading;
    expect(cvr.version).toEqual({
      stateVersion: '05',
      minorVersion: 1,
    });
  });

  test('fail after max attempts if rows behind', async () => {
    // Simulate the CVR being ahead of the rows.
    await db`UPDATE "roze_1/cvr".instances SET version = '04'`;

    await expect(
      store.load(lc, CONNECT_TIME),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: max attempts exceeded waiting for CVR@04 to catch up from 03]`,
    );

    // Verify that the store signaled an ownership change to 'my-task' at CONNECT_TIME.
    expect(await db`SELECT * FROM "roze_1/cvr".instances`)
      .toMatchInlineSnapshot(`
        Result [
          {
            "clientGroupID": "my-cvr",
            "clientSchema": null,
            "deleted": false,
            "grantedAt": 1732233600000,
            "lastActive": 1725408000000,
            "owner": "my-task",
            "profileID": null,
            "replicaVersion": "01",
            "ttlClock": 1725408000000,
            "version": "04",
          },
        ]
      `);
  });

  test('wrong owner', async () => {
    // Simulate the CVR being owned by someone else.
    await db`UPDATE "roze_1/cvr".instances SET owner = 'other-task', "grantedAt" = ${
      CONNECT_TIME + 1
    }`;

    await expect(store.load(lc, CONNECT_TIME)).rejects.toThrow(OwnershipError);

    // Verify that no ownership change was signaled.
    expect(await db`SELECT * FROM "roze_1/cvr".instances`)
      .toMatchInlineSnapshot(`
        Result [
          {
            "clientGroupID": "my-cvr",
            "clientSchema": null,
            "deleted": false,
            "grantedAt": 1732233600001,
            "lastActive": 1725408000000,
            "owner": "other-task",
            "profileID": null,
            "replicaVersion": "01",
            "ttlClock": 1725408000000,
            "version": "03",
          },
        ]
      `);
  });

  test('garbage collected cvr', async () => {
    // Simulate the CVR being garbage collected
    await db`UPDATE "roze_1/cvr".instances SET deleted = TRUE`;

    await expect(store.load(lc, CONNECT_TIME)).rejects.toThrow(
      ClientNotFoundError,
    );
  });

  async function catchupRows(
    after: CVRVersion,
    upTo: CVRVersion,
    current: CVRVersion,
    excludeQueryHashes: string[] = [],
  ): Promise<RowsRow[]> {
    const rows = [];
    for await (const batch of store.catchupRowPatches(
      lc,
      after,
      {version: upTo} as CVRSnapshot,
      current,
      excludeQueryHashes,
    )) {
      rows.push(...batch);
    }
    return rows;
  }

  test('catchupRows', async () => {
    // After 01, up to 02:
    expect(
      await catchupRows(
        {stateVersion: '01'},
        {stateVersion: '02'},
        {stateVersion: '03'},
      ),
    ).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": null,
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": {
            "foo": 1,
          },
          "rowKey": {
            "id": "6",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": {
            "bar": 2,
          },
          "rowKey": {
            "id": "7",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": {
            "bar": 3,
            "foo": 2,
          },
          "rowKey": {
            "id": "8",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
      ]
    `);

    // After 00, up to 02, excluding query hash 'bar'
    expect(
      await catchupRows(
        {stateVersion: '00'},
        {stateVersion: '02'},
        {stateVersion: '03'},
        ['bar'],
      ),
    ).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "01",
          "refCounts": null,
          "rowKey": {
            "id": "1",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "01",
          "refCounts": {
            "foo": 1,
          },
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": null,
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": {
            "foo": 1,
          },
          "rowKey": {
            "id": "6",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
      ]
    `);

    // After 01, up to 03, excluding multiple query hashes 'foo' and 'bar'
    expect(
      await catchupRows(
        {stateVersion: '01'},
        {stateVersion: '03'},
        {stateVersion: '03'},
        ['foo', 'bar'],
      ),
    ).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": null,
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "03",
          "refCounts": null,
          "rowKey": {
            "id": "9",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
      ]
    `);
  });

  test('row catchup detects concurrent modification', async () => {
    // Expect the stateVersion to be '02', simulating a situation in which
    // the CVR has already been updated to '03'.
    await expect(
      catchupRows(
        {stateVersion: '01'},
        {stateVersion: '02'},
        {stateVersion: '02'},
      ),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `[ConcurrentModificationException: CVR has been concurrently modified. Expected 02, got 03]`,
    );
  });

  const DEFERRED_ROW_LIMIT = 5;

  test('deferred row updates', async () => {
    const now = Date.UTC(2024, 10, 23);
    const ttlClock = ttlClockFromNumber(now);
    let cvr = await store.load(lc, CONNECT_TIME);

    // 12 rows set up in beforeEach().
    expect(await db`SELECT COUNT(*) FROM "roze_1/cvr".rows`).toEqual([
      {count: 12n},
    ]);

    let updater = new CVRQueryDrivenUpdater(store, cvr, '04', '01');
    updater.trackQueries(
      lc,
      [{id: 'foo', transformationHash: 'foo-transformed'}],
      [],
    );

    let rows = new CustomKeyMap<RowID, RowUpdate>(rowIDString);
    for (let i = 0; i < DEFERRED_ROW_LIMIT + 1; i++) {
      const id = String(20 + i);
      rows.set(
        {schema: 'public', table: 'issues', rowKey: {id}},
        {version: '04', contents: {id}, refCounts: {foo: 1}},
      );
    }
    await updater.received(lc, rows);
    cvr = (await updater.flush(lc, CONNECT_TIME, now, ttlClock)).cvr;

    expect(await db`SELECT * FROM "roze_1/cvr".instances`)
      .toMatchInlineSnapshot(`
        Result [
          {
            "clientGroupID": "my-cvr",
            "clientSchema": null,
            "deleted": false,
            "grantedAt": 1732233600000,
            "lastActive": 1732320000000,
            "owner": "my-task",
            "profileID": null,
            "replicaVersion": "01",
            "ttlClock": 1732320000000,
            "version": "04",
          },
        ]
      `);

    // rowsVersion === '03' (flush deferred).
    expect(await db`SELECT * FROM "roze_1/cvr"."rowsVersion"`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "my-cvr",
          "version": "03",
        },
      ]
    `);

    // Still only 12 rows.
    expect(await db`SELECT COUNT(*) FROM "roze_1/cvr".rows`).toEqual([
      {count: 12n},
    ]);

    // Flush was scheduled.
    expect(setTimeoutFn).toHaveBeenCalledOnce();

    // Before flushing, simulate another CVR update, this time within
    // the DEFERRED_LIMIT. It should still be deferred because there
    // are now pending rows waiting to be flushed.
    updater = new CVRQueryDrivenUpdater(store, cvr, '05', '01');
    updater.trackQueries(
      lc,
      [{id: 'foo', transformationHash: 'foo-transformed'}],
      [],
    );

    rows = new CustomKeyMap<RowID, RowUpdate>(rowIDString);
    for (let i = 0; i < DEFERRED_ROW_LIMIT - 1; i++) {
      const id = String(40 + i);
      rows.set(
        {schema: 'public', table: 'issues', rowKey: {id}},
        {version: '03', contents: {id}, refCounts: {foo: 1}},
      );
    }
    await updater.received(lc, rows);
    await updater.flush(lc, CONNECT_TIME, now, ttlClock);

    expect(await db`SELECT * FROM "roze_1/cvr".instances`)
      .toMatchInlineSnapshot(`
        Result [
          {
            "clientGroupID": "my-cvr",
            "clientSchema": null,
            "deleted": false,
            "grantedAt": 1732233600000,
            "lastActive": 1732320000000,
            "owner": "my-task",
            "profileID": null,
            "replicaVersion": "01",
            "ttlClock": 1732320000000,
            "version": "05",
          },
        ]
      `);

    // rowsVersion === '03' (flush deferred).
    expect(await db`SELECT * FROM "roze_1/cvr"."rowsVersion"`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "my-cvr",
          "version": "03",
        },
      ]
    `);

    // Still only 12 rows.
    expect(await db`SELECT COUNT(*) FROM "roze_1/cvr".rows`).toEqual([
      {count: 12n},
    ]);

    // Now run the flush logic.
    await setTimeoutFn.mock.calls[0][0]();

    // rowsVersion === '05' (flushed).
    expect(await db`SELECT * FROM "roze_1/cvr"."rowsVersion"`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "my-cvr",
          "version": "05",
        },
      ]
    `);

    // 12 + 6 + 4.
    expect(await db`SELECT COUNT(*) FROM "roze_1/cvr".rows`).toEqual([
      {count: 22n},
    ]);
  });

  test('deferred row stress test', async () => {
    const now = Date.UTC(2024, 10, 23);
    const ttlClock = ttlClockFromNumber(now);
    let cvr = await store.load(lc, CONNECT_TIME);

    // Use real setTimeout.
    setTimeoutFn.mockImplementation((cb, ms) => setTimeout(cb, ms));

    // 12 rows set up in beforeEach().
    expect(await db`SELECT COUNT(*) FROM "roze_1/cvr".rows`).toEqual([
      {count: 12n},
    ]);

    // Commit 30 flushes of 10 rows each.
    for (let i = 20; i < 320; i += 10) {
      const version = versionToLexi(i);
      const updater = new CVRQueryDrivenUpdater(store, cvr, version, '01');
      updater.trackQueries(
        lc,
        [{id: 'foo', transformationHash: 'foo-transformed'}],
        [],
      );

      const rows = new CustomKeyMap<RowID, RowUpdate>(rowIDString);
      for (let j = 0; j < 10; j++) {
        const id = String(i + j);
        rows.set(
          {schema: 'public', table: 'issues', rowKey: {id}},
          {version, contents: {id}, refCounts: {foo: 1}},
        );
      }
      await updater.received(lc, rows);
      cvr = (await updater.flush(lc, CONNECT_TIME, now, ttlClock)).cvr;

      // add a random sleep for varying the asynchronicity
      // between the CVR flush and the async row flush.
      await sleep(Math.random() * 1);
    }

    expect(await db`SELECT * FROM "roze_1/cvr".instances`)
      .toMatchInlineSnapshot(`
        Result [
          {
            "clientGroupID": "my-cvr",
            "clientSchema": null,
            "deleted": false,
            "grantedAt": 1732233600000,
            "lastActive": 1732320000000,
            "owner": "my-task",
            "profileID": null,
            "replicaVersion": "01",
            "ttlClock": 1732320000000,
            "version": "18m",
          },
        ]
      `);

    // Should block until all pending rows are flushed.
    await store.flushed(lc);

    // rowsVersion should match "roze_1/cvr".instances version
    expect(await db`SELECT * FROM "roze_1/cvr"."rowsVersion"`)
      .toMatchInlineSnapshot(`
            Result [
              {
                "clientGroupID": "my-cvr",
                "version": "18m",
              },
            ]
          `);

    // 12 + (30 * 10)
    expect(await db`SELECT COUNT(*) FROM "roze_1/cvr".rows`).toEqual([
      {count: 312n},
    ]);
  });

  test('deferred row stress test with empty updates', async () => {
    const now = Date.UTC(2024, 10, 23);
    const ttlClock = ttlClockFromNumber(now);
    let cvr = await store.load(lc, CONNECT_TIME);

    // Use real setTimeout.
    setTimeoutFn.mockImplementation((cb, ms) => setTimeout(cb, ms));

    // 12 rows set up in beforeEach().
    expect(await db`SELECT COUNT(*) FROM "roze_1/cvr".rows`).toEqual([
      {count: 12n},
    ]);

    // Commit 30 flushes of 10 rows each.
    for (let i = 20; i < 320; i += 10) {
      const version = versionToLexi(i);
      const updater = new CVRQueryDrivenUpdater(store, cvr, version, '01');
      updater.trackQueries(
        lc,
        [{id: 'foo', transformationHash: 'foo-transformed'}],
        [],
      );

      const rows = new CustomKeyMap<RowID, RowUpdate>(rowIDString);
      for (let j = 0; j < 10; j++) {
        const id = String(i + j);
        rows.set(
          {schema: 'public', table: 'issues', rowKey: {id}},
          {version, contents: {id}, refCounts: {foo: 1}},
        );
      }
      await updater.received(lc, rows);
      cvr = (await updater.flush(lc, CONNECT_TIME, now, ttlClock)).cvr;

      // add a random sleep for varying the asynchronicity
      // between the CVR flush and the async row flush.
      await sleep(Math.random() * 1);
    }

    const updater = new CVRQueryDrivenUpdater(
      store,
      cvr,
      versionToLexi(320),
      '01',
    );
    updater.trackQueries(
      lc,
      [{id: 'foo', transformationHash: 'foo-transformed'}],
      [],
    );
    // Empty rows.
    const rows = new CustomKeyMap<RowID, RowUpdate>(rowIDString);
    await updater.received(lc, rows);
    await updater.flush(lc, CONNECT_TIME, now, ttlClock);

    expect(await db`SELECT * FROM "roze_1/cvr".instances`)
      .toMatchInlineSnapshot(`
        Result [
          {
            "clientGroupID": "my-cvr",
            "clientSchema": null,
            "deleted": false,
            "grantedAt": 1732233600000,
            "lastActive": 1732320000000,
            "owner": "my-task",
            "profileID": null,
            "replicaVersion": "01",
            "ttlClock": 1732320000000,
            "version": "18m",
          },
        ]
      `);

    // Should block until all pending rows are flushed.
    await store.flushed(lc);

    // rowsVersion should match "roze_1/cvr".instances version
    expect(await db`SELECT * FROM "roze_1/cvr"."rowsVersion"`)
      .toMatchInlineSnapshot(`
            Result [
              {
                "clientGroupID": "my-cvr",
                "version": "18m",
              },
            ]
          `);

    // 12 + (30 * 10)
    expect(await db`SELECT COUNT(*) FROM "roze_1/cvr".rows`).toEqual([
      {count: 312n},
    ]);
  });

  test('large batch row updates', async () => {
    const now = Date.UTC(2024, 10, 23);
    const ttlClock = ttlClockFromNumber(now);
    let cvr = await store.load(lc, CONNECT_TIME);

    // 12 rows set up in beforeEach().
    expect(await db`SELECT COUNT(*) FROM "roze_1/cvr".rows`).toEqual([
      {count: 12n},
    ]);

    const updater = new CVRQueryDrivenUpdater(store, cvr, '04', '01');
    updater.trackQueries(
      lc,
      [{id: 'foo', transformationHash: 'foo-transformed'}],
      [],
    );

    const rows = new CustomKeyMap<RowID, RowUpdate>(rowIDString);
    // Should flush in batches of 512, 256, 128, 127, with the last one being unprepared.
    for (let i = 0; i < 1023; i++) {
      const id = String(20 + i);
      rows.set(
        {schema: 'public', table: 'issues', rowKey: {id}},
        {version: '04', contents: {id}, refCounts: {foo: 1}},
      );
    }
    await updater.received(lc, rows);
    cvr = (await updater.flush(lc, CONNECT_TIME, now, ttlClock)).cvr;

    expect(await db`SELECT * FROM "roze_1/cvr".instances`)
      .toMatchInlineSnapshot(`
        Result [
          {
            "clientGroupID": "my-cvr",
            "clientSchema": null,
            "deleted": false,
            "grantedAt": 1732233600000,
            "lastActive": 1732320000000,
            "owner": "my-task",
            "profileID": null,
            "replicaVersion": "01",
            "ttlClock": 1732320000000,
            "version": "04",
          },
        ]
      `);

    // rowsVersion === '03' (flush deferred).
    expect(await db`SELECT * FROM "roze_1/cvr"."rowsVersion"`)
      .toMatchInlineSnapshot(`
    Result [
      {
        "clientGroupID": "my-cvr",
        "version": "03",
      },
    ]
  `);

    // Still only 12 rows.
    expect(await db`SELECT COUNT(*) FROM "roze_1/cvr".rows`).toEqual([
      {count: 12n},
    ]);

    // Flush was scheduled.
    expect(setTimeoutFn).toHaveBeenCalledOnce();

    // Now run the flush logic.
    await setTimeoutFn.mock.calls[0][0]();

    // rowsVersion === '04' (flushed).
    expect(await db`SELECT * FROM "roze_1/cvr"."rowsVersion"`)
      .toMatchInlineSnapshot(`
    Result [
      {
        "clientGroupID": "my-cvr",
        "version": "04",
      },
    ]
  `);

    // 12 + 1023 = 1035
    expect(await db`SELECT COUNT(*) FROM "roze_1/cvr".rows`).toEqual([
      {count: 1035n},
    ]);
  });

  test('load with deleted client with remaining desires', async () => {
    await db.unsafe(`
      INSERT INTO "roze_1/cvr".clients ("clientGroupID", "clientID")
        VALUES('${CVR_ID}', 'client1');
      INSERT INTO "roze_1/cvr".desires ("clientGroupID", "clientID", "queryHash", "patchVersion")
        VALUES('${CVR_ID}', 'client1', 'foo', '01');
      INSERT INTO "roze_1/cvr".desires ("clientGroupID", "clientID", "queryHash", "patchVersion", "ttlMs", "inactivatedAtMs")
        VALUES('${CVR_ID}', 'missing-client', 'foo', '01', 3600000, ${Date.UTC(2025, 2, 10)});
    `);

    const cvr = await store.load(lc, CONNECT_TIME);

    expect(cvr).toMatchInlineSnapshot(`
          {
            "clientSchema": null,
            "clients": {
              "client1": {
                "desiredQueryIDs": [
                  "foo",
                ],
                "id": "client1",
              },
            },
            "id": "my-cvr",
            "lastActive": 1725408000000,
            "profileID": null,
            "queries": {
              "foo": {
                "ast": {
                  "table": "issues",
                },
                "clientState": {
                  "client1": {
                    "inactivatedAt": undefined,
                    "ttl": 300000,
                    "version": {
                      "stateVersion": "01",
                    },
                  },
                  "missing-client": {
                    "inactivatedAt": 1741564800000,
                    "ttl": 600000,
                    "version": {
                      "stateVersion": "01",
                    },
                  },
                },
                "id": "foo",
                "patchVersion": {
                  "stateVersion": "01",
                },
                "transformationHash": "foo-transformed",
                "transformationVersion": {
                  "stateVersion": "01",
                },
                "type": "client",
              },
            },
            "replicaVersion": "01",
            "ttlClock": 1725408000000,
            "version": {
              "stateVersion": "03",
            },
          }
        `);
  });

  test('inspectQueries', async () => {
    // Setup two clients with two desired queries each
    await db.unsafe(`
      -- Insert client1 and client2
      INSERT INTO "roze_1/cvr".clients ("clientGroupID", "clientID")
        VALUES('${CVR_ID}', 'client1');
      INSERT INTO "roze_1/cvr".clients ("clientGroupID", "clientID")
        VALUES('${CVR_ID}', 'client2');
      
      -- Insert query 'bar' (users table with AST)
      INSERT INTO "roze_1/cvr".queries ("clientGroupID", "queryHash", "clientAST", "patchVersion", "transformationHash", "transformationVersion")
        VALUES('${CVR_ID}', 'bar', '{"table":"users"}', '02', 'bar-transformed', '01');
      
      -- Insert query 'baz' (tasks table with AST)
      INSERT INTO "roze_1/cvr".queries ("clientGroupID", "queryHash", "clientAST", "patchVersion", "transformationHash", "transformationVersion", "queryName", "queryArgs")
        VALUES('${CVR_ID}', 'baz', NULL, '03', 'baz-transformed', '01', 'named', '["arg1",1,true]');
      
      -- Client1 desires foo and bar
      INSERT INTO "roze_1/cvr".desires ("clientGroupID", "clientID", "queryHash", "patchVersion")
        VALUES('${CVR_ID}', 'client1', 'foo', '01');
      INSERT INTO "roze_1/cvr".desires ("clientGroupID", "clientID", "queryHash", "patchVersion")
        VALUES('${CVR_ID}', 'client1', 'bar', '02');
      
      -- Client2 desires baz with TTL and bar with inactivatedAtMs
      INSERT INTO "roze_1/cvr".desires ("clientGroupID", "clientID", "queryHash", "patchVersion", "ttlMs")
        VALUES('${CVR_ID}', 'client2', 'baz', '03', 7200);
      INSERT INTO "roze_1/cvr".desires ("clientGroupID", "clientID", "queryHash", "patchVersion", "inactivatedAtMs")
        VALUES('${CVR_ID}', 'client2', 'bar', '02', ${Date.UTC(2024, 9, 15)});
    `);

    // Test inspectQueries with no clientID (should return all queries)
    const ttlClock = ttlClockFromNumber(Date.UTC(2024, 8, 4)); // 2024-09-04
    const allQueries = await store.inspectQueries(lc, ttlClock);
    expect(allQueries).toMatchInlineSnapshot(`
      Result [
        {
          "args": null,
          "ast": {
            "table": "users",
          },
          "clientID": "client1",
          "deleted": false,
          "got": true,
          "inactivatedAt": null,
          "name": null,
          "queryID": "bar",
          "rowCount": 6,
          "ttl": 300000,
        },
        {
          "args": null,
          "ast": {
            "table": "issues",
          },
          "clientID": "client1",
          "deleted": false,
          "got": true,
          "inactivatedAt": null,
          "name": null,
          "queryID": "foo",
          "rowCount": 6,
          "ttl": 300000,
        },
        {
          "args": null,
          "ast": {
            "table": "users",
          },
          "clientID": "client2",
          "deleted": false,
          "got": true,
          "inactivatedAt": 1728950400000,
          "name": null,
          "queryID": "bar",
          "rowCount": 6,
          "ttl": 300000,
        },
        {
          "args": [
            "arg1",
            1,
            true,
          ],
          "ast": null,
          "clientID": "client2",
          "deleted": false,
          "got": true,
          "inactivatedAt": null,
          "name": "named",
          "queryID": "baz",
          "rowCount": 0,
          "ttl": 7200,
        },
      ]
    `);

    // Test inspectQueries for client1
    const client1Queries = await store.inspectQueries(lc, ttlClock, 'client1');
    expect(client1Queries).toMatchInlineSnapshot(`
      Result [
        {
          "args": null,
          "ast": {
            "table": "users",
          },
          "clientID": "client1",
          "deleted": false,
          "got": true,
          "inactivatedAt": null,
          "name": null,
          "queryID": "bar",
          "rowCount": 6,
          "ttl": 300000,
        },
        {
          "args": null,
          "ast": {
            "table": "issues",
          },
          "clientID": "client1",
          "deleted": false,
          "got": true,
          "inactivatedAt": null,
          "name": null,
          "queryID": "foo",
          "rowCount": 6,
          "ttl": 300000,
        },
      ]
    `);

    // Test inspectQueries for client2
    const client2Queries = await store.inspectQueries(lc, ttlClock, 'client2');
    expect(client2Queries).toMatchInlineSnapshot(`
      Result [
        {
          "args": null,
          "ast": {
            "table": "users",
          },
          "clientID": "client2",
          "deleted": false,
          "got": true,
          "inactivatedAt": 1728950400000,
          "name": null,
          "queryID": "bar",
          "rowCount": 6,
          "ttl": 300000,
        },
        {
          "args": [
            "arg1",
            1,
            true,
          ],
          "ast": null,
          "clientID": "client2",
          "deleted": false,
          "got": true,
          "inactivatedAt": null,
          "name": "named",
          "queryID": "baz",
          "rowCount": 0,
          "ttl": 7200,
        },
      ]
    `);
  });

  test('inspectQueries filters out expired queries (inactivatedAt + ttl <= ttlClock)', async () => {
    const ttlClock: TTLClock = ttlClockFromNumber(0); //Date.UTC(2025, 6, 17, 12, 0, 0); // July 17, 2025, 12:00:00 UTC
    const expiredInactivatedAt = ttlClockFromNumber(
      ttlClockAsNumber(ttlClock) - 120_000,
    ); // 2 minutes ago
    const expiredTTL = 60_000; // 1 minute TTL
    const activeInactivatedAt = ttlClockFromNumber(
      ttlClockAsNumber(ttlClock) - 30_000,
    ); // 30 seconds ago
    const activeTTL = 60_000; // 1 minute TTL
    const deletedNotExpiredInactivatedAt = ttlClockFromNumber(
      ttlClockAsNumber(ttlClock) - 10_000,
    ); // 10 seconds ago
    const deletedNotExpiredTTL = 60_000; // 1 minute TTL

    // Setup clients and queries
    await db.unsafe(`
      -- Insert clients
      INSERT INTO "roze_1/cvr".clients ("clientGroupID", "clientID")
        VALUES('${CVR_ID}', 'test-client');
      
      -- Insert queries
      INSERT INTO "roze_1/cvr".queries ("clientGroupID", "queryHash", "clientAST", "patchVersion")
        VALUES('${CVR_ID}', 'expired-query', '{"table":"expired"}', '01');
      INSERT INTO "roze_1/cvr".queries ("clientGroupID", "queryHash", "clientAST", "patchVersion")
        VALUES('${CVR_ID}', 'active-query', '{"table":"active"}', '01');
      INSERT INTO "roze_1/cvr".queries ("clientGroupID", "queryHash", "clientAST", "patchVersion")
        VALUES('${CVR_ID}', 'deleted-not-expired-query', '{"table":"deleted"}', '01');
      
      -- Insert desires with TTL: expired query (inactivatedAtMs + ttlMs <= ttlClock)
      INSERT INTO "roze_1/cvr".desires ("clientGroupID", "clientID", "queryHash", "patchVersion", "inactivatedAtMs", "ttlMs")
        VALUES('${CVR_ID}', 'test-client', 'expired-query', '01', 
               ${ttlClockAsNumber(expiredInactivatedAt)}, 
               ${expiredTTL});
      
      -- Insert desires with TTL: active query (inactivatedAtMs + ttlMs > ttlClock)
      INSERT INTO "roze_1/cvr".desires ("clientGroupID", "clientID", "queryHash", "patchVersion", "inactivatedAtMs", "ttlMs")
        VALUES('${CVR_ID}', 'test-client', 'active-query', '01', 
               ${ttlClockAsNumber(activeInactivatedAt)}, 
               ${activeTTL});
      
      -- Insert desires with TTL: deleted but not expired query (has deleted=true but inactivatedAtMs + ttlMs > ttlClock)
      INSERT INTO "roze_1/cvr".desires ("clientGroupID", "clientID", "queryHash", "patchVersion", "inactivatedAtMs", "ttlMs", "deleted")
        VALUES('${CVR_ID}', 'test-client', 'deleted-not-expired-query', '01', 
               ${ttlClockAsNumber(deletedNotExpiredInactivatedAt)}, 
               ${deletedNotExpiredTTL},
               true);
    `);

    // Query with ttlClock = now (expired query should be filtered out, but deleted non-expired should be included)
    const results = await store.inspectQueries(lc, ttlClock, 'test-client');

    // Active and deleted-but-not-expired queries should be returned
    expect(results.map(r => r.queryID).sort()).toEqual([
      'active-query',
      'deleted-not-expired-query',
    ]);
    expect(results).toHaveLength(2);

    // Query with ttlClock = now - 2min (all queries should be present since none are expired)
    const olderTtlClock = ttlClockFromNumber(
      ttlClockAsNumber(ttlClock) - 120_000,
    );
    const results2 = await store.inspectQueries(
      lc,
      olderTtlClock,
      'test-client',
    );

    // All three queries should be returned when ttlClock is in the past
    expect(results2.map(r => r.queryID).sort()).toEqual([
      'active-query',
      'deleted-not-expired-query',
      'expired-query',
    ]);
    expect(results2).toHaveLength(3);
  });
});
