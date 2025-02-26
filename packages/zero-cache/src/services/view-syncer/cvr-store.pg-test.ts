import {
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
  vi,
  type Mock,
} from 'vitest';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {testDBs} from '../../test/db.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {rowIDString, type RowID} from '../../types/row-key.ts';
import {CVRStore, OwnershipError} from './cvr-store.ts';
import {
  CVRQueryDrivenUpdater,
  type CVRSnapshot,
  type RowUpdate,
} from './cvr.ts';
import {setupCVRTables, type RowsRow} from './schema/cvr.ts';
import type {CVRVersion} from './schema/types.ts';

const APP_ID = 'roze';
const SHARD_ID = '1';

describe('view-syncer/cvr-store', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;
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

  beforeEach(async () => {
    db = await testDBs.create('view_syncer_cvr_schema');
    await db.begin(tx => setupCVRTables(lc, tx, APP_ID, SHARD_ID));
    await db.unsafe(`
    INSERT INTO "roze_1/cvr".instances ("clientGroupID", version, "lastActive", "replicaVersion")
      VALUES('${CVR_ID}', '03', '2024-09-04', '01');
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
      APP_ID,
      SHARD_ID,
      TASK_ID,
      CVR_ID,
      ON_FAILURE,
      10,
      5,
      DEFERRED_ROW_LIMIT,
      setTimeoutFn as unknown as typeof setTimeout,
    );
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  test('wait for row catchup', async () => {
    // Simulate the CVR being ahead of the rows.
    await db`UPDATE "roze_1/cvr".instances SET version = '04'`;

    // start a CVR load.
    const loading = store.load(lc, CONNECT_TIME);

    await sleep(1);

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
      `[Error: {"kind":"ClientNotFound","message":"max attempts exceeded waiting for CVR@04 to catch up from 03"}]`,
    );

    // Verify that the store signaled an ownership change to 'my-task' at CONNECT_TIME.
    expect(await db`SELECT * FROM "roze_1/cvr".instances`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "my-cvr",
          "grantedAt": 1732233600000,
          "lastActive": 1725408000000,
          "owner": "my-task",
          "replicaVersion": "01",
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
          "grantedAt": 1732233600001,
          "lastActive": 1725408000000,
          "owner": "other-task",
          "replicaVersion": "01",
          "version": "03",
        },
      ]
    `);
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
    cvr = (await updater.flush(lc, true, CONNECT_TIME, now)).cvr;

    expect(await db`SELECT * FROM "roze_1/cvr".instances`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "my-cvr",
          "grantedAt": 1732233600000,
          "lastActive": 1732320000000,
          "owner": "my-task",
          "replicaVersion": "01",
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
    await updater.flush(lc, true, CONNECT_TIME, now);

    expect(await db`SELECT * FROM "roze_1/cvr".instances`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "my-cvr",
          "grantedAt": 1732233600000,
          "lastActive": 1732320000000,
          "owner": "my-task",
          "replicaVersion": "01",
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
      cvr = (await updater.flush(lc, true, CONNECT_TIME, now)).cvr;

      // add a random sleep for varying the asynchronicity
      // between the CVR flush and the async row flush.
      await sleep(Math.random() * 1);
    }

    expect(await db`SELECT * FROM "roze_1/cvr".instances`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "my-cvr",
          "grantedAt": 1732233600000,
          "lastActive": 1732320000000,
          "owner": "my-task",
          "replicaVersion": "01",
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
      cvr = (await updater.flush(lc, true, CONNECT_TIME, now)).cvr;

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
    await updater.flush(lc, true, CONNECT_TIME, now);

    expect(await db`SELECT * FROM "roze_1/cvr".instances`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "my-cvr",
          "grantedAt": 1732233600000,
          "lastActive": 1732320000000,
          "owner": "my-task",
          "replicaVersion": "01",
          "version": "18w",
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
                "version": "18w",
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
    cvr = (await updater.flush(lc, true, CONNECT_TIME, now)).cvr;

    expect(await db`SELECT * FROM "roze_1/cvr".instances`)
      .toMatchInlineSnapshot(`
    Result [
      {
        "clientGroupID": "my-cvr",
        "grantedAt": 1732233600000,
        "lastActive": 1732320000000,
        "owner": "my-task",
        "replicaVersion": "01",
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
});
