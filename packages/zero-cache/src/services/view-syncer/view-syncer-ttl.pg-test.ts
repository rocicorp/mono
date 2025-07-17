import {
  afterEach,
  beforeEach,
  describe,
  expect,
  type Mock,
  test,
  vi,
} from 'vitest';
import {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {type ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import {testDBs} from '../../test/db.ts';
import {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {type FakeReplicator} from '../replicator/test-utils.ts';
import {
  expectNoPokes,
  ISSUES_QUERY,
  messages,
  nextPoke,
  nextPokeParts,
  permissionsAll,
  serviceID,
  setup,
  USERS_QUERY,
} from './view-syncer-test-util.ts';
import {type SyncContext, ViewSyncerService} from './view-syncer.ts';

let replicaDbFile: DbFile;
let cvrDB: PostgresDB;
let stateChanges: Subscription<ReplicaState>;

let vs: ViewSyncerService;
let viewSyncerDone: Promise<void>;
let replicator: FakeReplicator;
let connect: (
  ctx: SyncContext,
  desiredQueriesPatch: UpQueriesPatch,
  clientSchema?: ClientSchema,
) => Queue<Downstream>;
let connectWithQueueAndSource: (
  ctx: SyncContext,
  desiredQueriesPatch: UpQueriesPatch,
  clientSchema?: ClientSchema,
  activeClients?: string[],
) => {
  queue: Queue<Downstream>;
  source: Source<Downstream>;
};
let setTimeoutFn: Mock<typeof setTimeout>;

function callNextSetTimeout(delta: number) {
  // Sanity check that the system time is the mocked time.
  expect(vi.getRealSystemTime()).not.toBe(vi.getMockedSystemTime());
  vi.setSystemTime(Date.now() + delta);
  const fn = setTimeoutFn.mock.lastCall![0];
  fn();
}

const SYNC_CONTEXT: SyncContext = {
  clientID: 'foo',
  wsID: 'ws1',
  baseCookie: null,
  protocolVersion: PROTOCOL_VERSION,
  schemaVersion: 2,
  tokenData: undefined,
  httpCookie: undefined,
};

beforeEach(async () => {
  ({
    replicaDbFile,
    cvrDB,
    stateChanges,
    vs,
    viewSyncerDone,
    replicator,
    connect,
    connectWithQueueAndSource,
    setTimeoutFn,
  } = await setup('view_syncer_ttl_test', permissionsAll));
});

afterEach(async () => {
  vi.useRealTimers();
  await vs.stop();
  await viewSyncerDone;
  await testDBs.drop(cvrDB);
  replicaDbFile.delete();
});

describe('ttl', () => {
  describe('ttlClock', () => {
    test('one client', async () => {
      const ttl = 1000;
      vi.setSystemTime(Date.UTC(2025, 5, 4, 24));
      const t0 = Date.now();

      // Connect a client with a TTL.
      const {queue: client, source} = connectWithQueueAndSource(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      ]);

      stateChanges.push({state: 'version-ready'});
      await nextPoke(client); // config
      await nextPoke(client); // hydration

      // Before closing, check initial values.
      const result = await cvrDB`
        SELECT "ttlClock", "lastActive"
        FROM "this_app_2/cvr".instances
        WHERE "clientGroupID" = ${serviceID}`;
      expect(result[0].ttlClock).toBe(t0);
      expect(result[0].lastActive).toBe(t0);

      {
        vi.setSystemTime(Date.now() + 500);
        const t1 = Date.now();

        // Close the connection.
        source.cancel();

        await sleep(10);
        vi.setSystemTime(Date.now() + 10);

        // Before closing, check initial values.
        const result = await cvrDB`
          SELECT "ttlClock", "lastActive"
          FROM "this_app_2/cvr".instances
          WHERE "clientGroupID" = ${serviceID}`;
        expect(result).toEqual([
          {
            ttlClock: t1,
            lastActive: t1,
          },
        ]);
      }
    });

    test('two clients', async () => {
      const ttl = 1000;
      vi.setSystemTime(Date.UTC(2025, 5, 4, 24));
      const t0 = Date.now();

      // Connect two clients with a TTL.
      const ctx1 = {...SYNC_CONTEXT, clientID: 'foo', wsID: 'ws1'};
      const ctx2 = {...SYNC_CONTEXT, clientID: 'bar', wsID: 'ws2'};
      const {queue: client1, source: source1} = connectWithQueueAndSource(
        ctx1,
        [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl}],
      );
      const {queue: client2, source: source2} = connectWithQueueAndSource(
        ctx2,
        [{op: 'put', hash: 'query-hash2', ast: USERS_QUERY, ttl}],
      );

      stateChanges.push({state: 'version-ready'});
      await nextPoke(client1); // config
      await nextPoke(client1); // hydration
      await nextPoke(client2); // config
      await nextPoke(client2); // hydration

      // Before closing, check initial values.
      let result = await cvrDB`
        SELECT "ttlClock", "lastActive"
        FROM "this_app_2/cvr".instances
        WHERE "clientGroupID" = ${serviceID}`;
      expect(result).toEqual([{ttlClock: t0, lastActive: t0}]);

      vi.setSystemTime(Date.now() + 500);
      // Close the first connection.
      source1.cancel();
      await sleep(10);
      vi.setSystemTime(Date.now() + 10);

      result = await cvrDB`
          SELECT "ttlClock", "lastActive"
          FROM "this_app_2/cvr".instances
          WHERE "clientGroupID" = ${serviceID}`;
      expect(result).toEqual([{ttlClock: t0, lastActive: t0}]);

      // Move time forward and close the second connection.
      vi.setSystemTime(Date.now() + 500);
      const t2 = Date.now();

      source2.cancel();
      await sleep(10);
      vi.setSystemTime(Date.now() + 10);

      result = await cvrDB`
          SELECT "ttlClock", "lastActive"
          FROM "this_app_2/cvr".instances
          WHERE "clientGroupID" = ${serviceID}`;
      expect(result).toEqual([{ttlClock: t2, lastActive: t2}]);
    });

    test('one client - disconnect and reconnect', async () => {
      const ttl = 1000;
      vi.setSystemTime(Date.UTC(2025, 5, 4, 24));
      const t0 = Date.now();

      // Connect a client with a TTL.
      const {queue: client, source} = connectWithQueueAndSource(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      ]);

      stateChanges.push({state: 'version-ready'});
      await nextPoke(client); // config
      await nextPoke(client); // hydration

      // Initial values.
      let result = await cvrDB`
        SELECT "ttlClock", "lastActive"
        FROM "this_app_2/cvr".instances
        WHERE "clientGroupID" = ${serviceID}`;
      expect(result[0].ttlClock).toBe(t0);
      expect(result[0].lastActive).toBe(t0);

      // Disconnect.
      vi.setSystemTime(Date.now() + 500);
      const t1 = Date.now();
      source.cancel();
      await sleep(10);

      result = await cvrDB`
        SELECT "ttlClock", "lastActive"
        FROM "this_app_2/cvr".instances
        WHERE "clientGroupID" = ${serviceID}`;
      expect(result[0].ttlClock).toBe(t1);
      expect(result[0].lastActive).toBe(t1);

      // Wait some time while disconnected (should not advance ttlClock).
      vi.setSystemTime(Date.now() + 1000);
      await sleep(100);

      // Reconnect.
      const client2 = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      ]);
      stateChanges.push({state: 'version-ready'});
      await nextPoke(client2);

      // After reconnect, ttlClock and lastActive have not been updated yet.
      const t2 = Date.now();
      result = await cvrDB`
          SELECT "ttlClock", "lastActive"
          FROM "this_app_2/cvr".instances
          WHERE "clientGroupID" = ${serviceID}`;
      expect(result[0].ttlClock).toBe(t1);
      expect(result[0].lastActive).toBe(t1);

      // Make a change.
      replicator.processTransaction(
        '123',
        messages.update('issues', {
          id: '1',
          title: 'new title',
          owner: 100,
          parent: null,
          big: 9007199254740991n,
        }),
      );
      stateChanges.push({state: 'version-ready'});
      await nextPoke(client2);

      result = await cvrDB`
          SELECT "ttlClock", "lastActive"
          FROM "this_app_2/cvr".instances
          WHERE "clientGroupID" = ${serviceID}`;
      expect(result[0].ttlClock).toBe(t1);
      expect(result[0].lastActive).toBe(t2);
    });
  });

  test('Query with ttl 10 minutes, disconnect after 5m, reconnect after an hour, should expire after 5 more minutes ', async () => {
    const ttl = 10 * 60 * 1000; // 10 minutes
    vi.setSystemTime(Date.UTC(2025, 5, 4, 12, 0, 0));
    const t0 = Date.now();

    // Connect a client with a TTL.
    const {queue: client, source} = connectWithQueueAndSource(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
    ]);

    stateChanges.push({state: 'version-ready'});
    await nextPoke(client); // config
    await nextPoke(client); // hydration

    // Advance 5 minutes, then remove the query.
    const t1 = t0 + 5 * 60 * 1000;
    vi.setSystemTime(t1);
    await vs.changeDesiredQueries(SYNC_CONTEXT, [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
      },
    ]);
    // Should get a desiredQueriesPatches poke, but not expired yet.
    await nextPokeParts(client);

    await expectNoPokes(client);

    // Advance another 5 minutes (total 10m), then disconnect.
    const t2 = t1 + 5 * 60 * 1000;
    vi.setSystemTime(t2);
    source.cancel();
    await sleep(10);

    expect(
      await cvrDB`
      SELECT "ttlClock", "lastActive"
      FROM "this_app_2/cvr".instances
      WHERE "clientGroupID" = ${serviceID}
    `,
    ).toEqual([
      {
        ttlClock: t2,
        lastActive: t2,
      },
    ]);

    // Wait an hour while disconnected.
    const t3 = t2 + 60 * 60 * 1000;
    vi.setSystemTime(t3);
    await sleep(10);
    setTimeoutFn.mockClear();

    // Reconnect. The query should still be there but should expire after 5 more minutes.
    const {queue: client2} = connectWithQueueAndSource(SYNC_CONTEXT, []);
    stateChanges.push({state: 'version-ready'});

    expect(
      await cvrDB`
      SELECT "ttlClock", "lastActive"
      FROM "this_app_2/cvr".instances
      WHERE "clientGroupID" = ${serviceID}
    `,
    ).toEqual([
      {
        ttlClock: t2,
        lastActive: t2,
      },
    ]);

    await nextPokeParts(client2);
    await expectNoPokes(client2);

    expect(
      await cvrDB`
      SELECT "ttlClock", "lastActive"
      FROM "this_app_2/cvr".instances
      WHERE "clientGroupID" = ${serviceID}
    `,
    ).toEqual([
      {
        ttlClock: t2,
        lastActive: t2,
      },
    ]);

    // Advance 5 minutes
    callNextSetTimeout(5 * 60 * 1000);

    expect((await nextPokeParts(client2))[0].gotQueriesPatch)
      .toMatchInlineSnapshot(`
        [
          {
            "hash": "query-hash1",
            "op": "del",
          },
        ]
      `);
    await expectNoPokes(client2);

    expect(
      await cvrDB`
      SELECT "ttlClock", "lastActive"
      FROM "this_app_2/cvr".instances
      WHERE "clientGroupID" = ${serviceID}
    `,
    ).toEqual([
      {
        ttlClock: t2 + 300_000,
        lastActive: t3 + 300_000,
      },
    ]);
  });

  test('TTL eviction based on service active time, not wallclock time', async () => {
    const ttl = 10 * 1000; // 10 seconds for faster test
    vi.setSystemTime(Date.UTC(2025, 6, 1, 12, 0, 0));
    const t0 = Date.now();

    // Connect a client with a TTL query.
    const {queue: client, source} = connectWithQueueAndSource(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
    ]);

    stateChanges.push({state: 'version-ready'});
    await nextPoke(client); // config
    await nextPoke(client); // hydration

    // Advance 2 seconds, then inactivate the query.
    const t1 = t0 + 2 * 1000;
    vi.setSystemTime(t1);
    await vs.changeDesiredQueries(SYNC_CONTEXT, [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
      },
    ]);
    await nextPokeParts(client); // Should get a desiredQueriesPatches poke

    // Verify the query is inactivated at ttlClock t1.
    expect(
      await cvrDB`
      SELECT "ttlClock", "lastActive"
      FROM "this_app_2/cvr".instances
      WHERE "clientGroupID" = ${serviceID}
    `,
    ).toEqual([
      {
        ttlClock: t1,
        lastActive: t1,
      },
    ]);

    // Simulate service downtime: advance wallclock but keep ttlClock paused.
    source.cancel();
    const t2 = t1 + 60 * 1000; // 1 minute later in wallclock
    vi.setSystemTime(t2);

    // Clear previous setTimeout calls.
    setTimeoutFn.mockClear();

    // Connect a new client to restart the service.
    const client2 = connect(SYNC_CONTEXT, []);
    stateChanges.push({state: 'version-ready'});
    await nextPokeParts(client2);

    // The query should NOT be expired yet because:
    // - inactivatedAt was t1 (when ttlClock was t1)
    // - ttlClock is still t1 (service was down, so no time passed for TTL)
    // - expiry should be at ttlClock = t1 + ttl (t1 + 10 seconds)
    // - current ttlClock is still t1, so 10 seconds remain
    const desires = await cvrDB`
      SELECT "deleted", "inactivatedAt"
      FROM "this_app_2/cvr".desires
      WHERE "clientGroupID" = ${serviceID}
    `;
    expect(desires).toEqual([
      {
        deleted: true,
        inactivatedAt: t1,
      },
    ]);

    // A timer should be scheduled for the remaining TTL (10 seconds).
    expect(setTimeoutFn).toHaveBeenCalledTimes(1);
    const [, delay] = setTimeoutFn.mock.calls[0];
    expect(delay).toBe(ttl); // Full 10 seconds since no service time passed

    // Now simulate the service running for 10 seconds to trigger eviction.
    callNextSetTimeout(ttl);

    // The eviction should trigger a poke to notify clients about the deletion.
    expect(await nextPokeParts(client2)).toMatchInlineSnapshot(`
      [
        {
          "gotQueriesPatch": [
            {
              "hash": "query-hash1",
              "op": "del",
            },
          ],
          "pokeID": "01:02",
          "rowsPatch": [
            {
              "id": {
                "id": "1",
              },
              "op": "del",
              "tableName": "issues",
            },
            {
              "id": {
                "id": "2",
              },
              "op": "del",
              "tableName": "issues",
            },
            {
              "id": {
                "id": "3",
              },
              "op": "del",
              "tableName": "issues",
            },
            {
              "id": {
                "id": "4",
              },
              "op": "del",
              "tableName": "issues",
            },
          ],
        },
      ]
    `);

    await expectNoPokes(client2);

    // The query should now be expired and cleaned up.
    const desiresAfterEviction = await cvrDB`
      SELECT "deleted"
      FROM "this_app_2/cvr".desires
      WHERE "clientGroupID" = ${serviceID}
    `;
    // Query should remain but be marked as deleted (tombstone)
    expect(desiresAfterEviction).toEqual([{deleted: true}]);
  });

  test('no error when deleting client before TTL clock is read', () => {
    vi.setSystemTime(Date.UTC(2025, 5, 4, 24));

    // Connect a client but don't wait for the TTL clock to be initialized
    const {queue: _client, source} = connectWithQueueAndSource(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl: 1000},
    ]);

    // Immediately disconnect the client before any state changes are pushed
    // This should trigger the #deleteClientDueToDisconnect path where
    // #hasTTLClock() returns false
    expect(() => source.cancel()).not.toThrow();
  });

  describe('expired queries', () => {
    test('expired query is removed', async () => {
      const ttl = 100;
      vi.setSystemTime(Date.now());
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      ]);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "put",
              },
            ],
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": -9007199254740991,
                  "id": "2",
                  "json": null,
                  "owner": "101",
                  "parent": null,
                  "title": "parent issue bar",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 123,
                  "id": "3",
                  "json": null,
                  "owner": "102",
                  "parent": "1",
                  "title": "foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
              },
            ],
          },
        ]
      `);

      // Mark query-hash1 as inactive
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
        },
      ]);

      // Make sure we do not get a delete of the gotQueriesPatch
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);

      await expectNoPokes(client);

      callNextSetTimeout(ttl);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "del",
              },
            ],
            "pokeID": "01:02",
            "rowsPatch": [
              {
                "id": {
                  "id": "1",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "3",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "4",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ]
      `);

      await expectNoPokes(client);
    });

    test('expired query is readded', async () => {
      const ttl = 100;
      vi.setSystemTime(Date.now());
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      ]);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "put",
              },
            ],
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": -9007199254740991,
                  "id": "2",
                  "json": null,
                  "owner": "101",
                  "parent": null,
                  "title": "parent issue bar",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 123,
                  "id": "3",
                  "json": null,
                  "owner": "102",
                  "parent": "1",
                  "title": "foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
              },
            ],
          },
        ]
      `);

      // Mark query-hash1 as inactive
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
        },
      ]);

      // Make sure we do not get a delete of the gotQueriesPatch
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);

      await expectNoPokes(client);

      callNextSetTimeout(ttl / 2);

      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl: ttl * 2},
          ],
        },
      ]);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "01:02",
          },
        ]
      `);

      // No got queries patch since we newer removed.
      await expectNoPokes(client);

      callNextSetTimeout(ttl);

      await expectNoPokes(client);

      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
        },
      ]);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:03",
          },
        ]
      `);

      await expectNoPokes(client);

      callNextSetTimeout(ttl * 2);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "del",
              },
            ],
            "pokeID": "01:04",
            "rowsPatch": [
              {
                "id": {
                  "id": "1",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "3",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "4",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ]
      `);
    });

    test('query is added twice with longer ttl', async () => {
      const ttl = 100;
      vi.setSystemTime(Date.now());
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      ]);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "put",
              },
            ],
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": -9007199254740991,
                  "id": "2",
                  "json": null,
                  "owner": "101",
                  "parent": null,
                  "title": "parent issue bar",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 123,
                  "id": "3",
                  "json": null,
                  "owner": "102",
                  "parent": "1",
                  "title": "foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
              },
            ],
          },
        ]
      `);

      // Set the same query again but with 2*ttl
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl: ttl * 2},
          ],
        },
      ]);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);

      await expectNoPokes(client);

      vi.setSystemTime(Date.now() + ttl * 2);

      // Now delete it and make sure it takes 2 * ttl to get the got delete.
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
        },
      ]);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:02",
          },
        ]
      `);

      await expectNoPokes(client);

      callNextSetTimeout(2 * ttl);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "del",
              },
            ],
            "pokeID": "01:03",
            "rowsPatch": [
              {
                "id": {
                  "id": "1",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "3",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "4",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ]
      `);

      await expectNoPokes(client);
    });

    test('query is added twice with shorter ttl', async () => {
      const ttl = 100;
      vi.setSystemTime(Date.now());
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl: ttl * 2},
      ]);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "put",
              },
            ],
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": -9007199254740991,
                  "id": "2",
                  "json": null,
                  "owner": "101",
                  "parent": null,
                  "title": "parent issue bar",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 123,
                  "id": "3",
                  "json": null,
                  "owner": "102",
                  "parent": "1",
                  "title": "foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
              },
            ],
          },
        ]
      `);

      // Set the same query again but with lower ttl which has no effect
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
          ],
        },
      ]);
      await expectNoPokes(client);

      vi.setSystemTime(Date.now() + 2 * ttl);

      // Now delete it and make sure it takes 2 * ttl to get the got delete.
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
        },
      ]);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);

      await expectNoPokes(client);
      callNextSetTimeout(2 * ttl);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "del",
              },
            ],
            "pokeID": "01:02",
            "rowsPatch": [
              {
                "id": {
                  "id": "1",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "3",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "4",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ]
      `);

      await expectNoPokes(client);
    });
  });
});
