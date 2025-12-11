import {beforeEach, describe, expect, type Mock, vi} from 'vitest';
import type {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {type ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Source} from '../../types/streams.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {type FakeReplicator} from '../replicator/test-utils.ts';
import {
  addQuery,
  expectDesiredDel,
  expectDesiredPut,
  expectGotDel,
  expectGotPut,
  expectNoPokes,
  inactivateQuery,
  ISSUES_QUERY,
  messages,
  nextPoke,
  nextPokeParts,
  permissionsAll,
  serviceID,
  setup,
  USERS_QUERY,
} from './view-syncer-test-util.ts';
import type {ViewSyncerService} from './view-syncer.ts';
import {
  type SyncContext,
  TTL_CLOCK_INTERVAL,
  TTL_TIMER_HYSTERESIS,
} from './view-syncer.ts';

let replicaDbFile: DbFile;
let cvrDB: PostgresDB;
let upstreamDb: PostgresDB;
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
let inspectorDelegate: InspectorDelegate;

function callNextSetTimeout(delta: number, expectedDelay?: number) {
  // Sanity check that the system time is the mocked time.
  expect(vi.getRealSystemTime()).not.toBe(vi.getMockedSystemTime());

  const {lastCall} = setTimeoutFn.mock;
  if (expectedDelay !== undefined) {
    expect(lastCall?.[1]).toBe(expectedDelay);
  }

  vi.setSystemTime(Date.now() + delta);
  const fn = lastCall![0];
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

beforeEach<PgTest>(async ({testDBs}) => {
  ({
    replicaDbFile,
    cvrDB,
    upstreamDb,
    stateChanges,
    vs,
    viewSyncerDone,
    replicator,
    connect,
    connectWithQueueAndSource,
    setTimeoutFn,
    inspectorDelegate,
  } = await setup(testDBs, 'view_syncer_ttl_test', permissionsAll));
  inspectorDelegate.setAuthenticated(serviceID);

  return async () => {
    inspectorDelegate.clearAuthenticated(serviceID);

    vi.useRealTimers();
    await vs.stop();
    await viewSyncerDone;
    await testDBs.drop(cvrDB, upstreamDb);
    replicaDbFile.delete();
  };
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
      expect(result[0].ttlClock).toBe(0);
      expect(result[0].lastActive).toBe(t0);

      {
        vi.setSystemTime(Date.now() + 500);
        const t1 = Date.now();

        // Close the connection.
        source.cancel();

        await sleep(100);
        vi.setSystemTime(Date.now() + 100);

        // Before closing, check initial values.
        const result = await cvrDB`
          SELECT "ttlClock", "lastActive"
          FROM "this_app_2/cvr".instances
          WHERE "clientGroupID" = ${serviceID}`;
        expect(result).toEqual([
          {
            ttlClock: 500,
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
      expect(result).toEqual([{ttlClock: 0, lastActive: t0}]);

      vi.setSystemTime(Date.now() + 500);
      // Close the first connection.
      source1.cancel();
      await sleep(100);
      vi.setSystemTime(Date.now() + 100);

      result = await cvrDB`
          SELECT "ttlClock", "lastActive"
          FROM "this_app_2/cvr".instances
          WHERE "clientGroupID" = ${serviceID}`;
      expect(result).toEqual([{ttlClock: 0, lastActive: t0}]);

      // Move time forward and close the second connection.
      vi.setSystemTime(Date.now() + 500);
      const t2 = Date.now();

      source2.cancel();
      await sleep(100);
      vi.setSystemTime(Date.now() + 100);

      result = await cvrDB`
          SELECT "ttlClock", "lastActive"
          FROM "this_app_2/cvr".instances
          WHERE "clientGroupID" = ${serviceID}`;
      expect(result).toEqual([{ttlClock: 1100, lastActive: t2}]);
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
      expect(result[0].ttlClock).toBe(0);
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
      expect(result[0].ttlClock).toBe(500);
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
      expect(result[0].ttlClock).toBe(500);
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
      expect(result[0].ttlClock).toBe(500);
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
    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash1');
    // Should get a desiredQueriesPatches poke, but not expired yet.
    await expectDesiredDel(client, 'foo', 'query-hash1');

    await expectNoPokes(client);

    // Advance another 5 minutes (total 10m), then disconnect.
    const t2 = t1 + 5 * 60 * 1000;
    vi.setSystemTime(t2);
    source.cancel();

    await vi.waitFor(async () => {
      expect(
        await cvrDB`
      SELECT "ttlClock", "lastActive"
      FROM "this_app_2/cvr".instances
      WHERE "clientGroupID" = ${serviceID}
    `,
      ).toEqual([
        {
          ttlClock: 10 * 60 * 1000,
          lastActive: t2,
        },
      ]);
    });

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
        ttlClock: 10 * 60 * 1000,
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
        ttlClock: 10 * 60 * 1000,
        lastActive: t2,
      },
    ]);

    // Advance 5 minutes
    callNextSetTimeout(5 * 60 * 1000);

    await expectGotDel(client2, 'query-hash1');
    await expectNoPokes(client2);

    expect(
      await cvrDB`
      SELECT "ttlClock", "lastActive"
      FROM "this_app_2/cvr".instances
      WHERE "clientGroupID" = ${serviceID}
    `,
    ).toEqual([
      {
        ttlClock: 10 * 60 * 1000 + 5 * 60 * 1000,
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
    const t1 = t0 + 2_000;
    vi.setSystemTime(t1);
    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash1');
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
        ttlClock: 2_000,
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
        // inactivatedAt is stored in DB as seconds (TIMESTAMPTZ), not milliseconds
        inactivatedAt: 2,
      },
    ]);

    // A timer should be scheduled for the remaining TTL (10 seconds).
    expect(setTimeoutFn).toHaveBeenCalledTimes(1);
    const [, delay] = setTimeoutFn.mock.calls[0];
    expect(delay).toBe(ttl + TTL_TIMER_HYSTERESIS); // Full 10 seconds since no service time passed

    // Now simulate the service running for 10 seconds to trigger eviction.
    callNextSetTimeout(ttl);

    // The eviction should trigger a poke to notify clients about the deletion.
    await expectGotDel(client2, 'query-hash1');

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

  test('expired query is removed', async () => {
    const ttl = 100;
    vi.setSystemTime(Date.now());
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
    ]);

    stateChanges.push({state: 'version-ready'});
    await expectDesiredPut(client, 'foo', 'query-hash1');

    await expectGotPut(client, 'query-hash1');

    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash1');

    // Make sure we do not get a delete of the gotQueriesPatch
    await expectDesiredDel(client, 'foo', 'query-hash1');

    await expectNoPokes(client);

    callNextSetTimeout(ttl);

    await expectGotDel(client, 'query-hash1');

    await expectNoPokes(client);
  });

  test('expired query is readded', async () => {
    const ttl = 100;
    vi.setSystemTime(Date.now());
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
    ]);

    stateChanges.push({state: 'version-ready'});
    await expectDesiredPut(client, 'foo', 'query-hash1');

    await expectGotPut(client, 'query-hash1');

    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash1');

    // Make sure we do not get a delete of the gotQueriesPatch
    await expectDesiredDel(client, 'foo', 'query-hash1');

    await expectNoPokes(client);

    callNextSetTimeout(ttl / 2);

    await addQuery(vs, SYNC_CONTEXT, 'query-hash1', ISSUES_QUERY, ttl * 2);

    await expectDesiredPut(client, 'foo', 'query-hash1');

    // No got queries patch since we newer removed.
    await expectNoPokes(client);

    callNextSetTimeout(ttl);

    await expectNoPokes(client);

    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash1');
    await expectDesiredDel(client, 'foo', 'query-hash1');

    await expectNoPokes(client);

    callNextSetTimeout(ttl * 2);

    await expectGotDel(client, 'query-hash1');
  });

  test('query is added twice with longer ttl', async () => {
    const ttl = 100;
    vi.setSystemTime(Date.now());
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
    ]);

    stateChanges.push({state: 'version-ready'});
    await expectDesiredPut(client, 'foo', 'query-hash1');

    await expectGotPut(client, 'query-hash1');

    // Set the same query again but with 2*ttl
    await addQuery(vs, SYNC_CONTEXT, 'query-hash1', ISSUES_QUERY, ttl * 2);
    await expectDesiredPut(client, 'foo', 'query-hash1');

    await expectNoPokes(client);

    vi.setSystemTime(Date.now() + ttl * 2);

    // Now delete it and make sure it takes 2 * ttl to get the got delete.
    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash1');
    await expectDesiredDel(client, 'foo', 'query-hash1');

    await expectNoPokes(client);

    callNextSetTimeout(2 * ttl);

    await expectGotDel(client, 'query-hash1');

    await expectNoPokes(client);
  });

  test('query is added twice with shorter ttl', async () => {
    const ttl = 100;
    vi.setSystemTime(Date.now());
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl: ttl * 2},
    ]);

    stateChanges.push({state: 'version-ready'});
    await expectDesiredPut(client, 'foo', 'query-hash1');

    await expectGotPut(client, 'query-hash1');

    // Set the same query again but with lower ttl which has no effect
    await addQuery(vs, SYNC_CONTEXT, 'query-hash1', ISSUES_QUERY, ttl);
    await expectNoPokes(client);

    vi.setSystemTime(Date.now() + 2 * ttl);

    // Now delete it and make sure it takes 2 * ttl to get the got delete.
    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash1');
    await expectDesiredDel(client, 'foo', 'query-hash1');

    await expectNoPokes(client);
    callNextSetTimeout(2 * ttl);

    await expectGotDel(client, 'query-hash1');

    await expectNoPokes(client);
  });

  test('two queries with different TTLs are evicted at their respective times', async () => {
    const ttl10s = 10000; // 10 seconds
    const ttl5s = 5000; // 5 seconds
    vi.setSystemTime(new Date(2025, 6, 17));

    // Start with just the 10s query to establish the timer
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl: ttl10s},
    ]);

    stateChanges.push({state: 'version-ready'});

    // Get the config and hydration pokes for the first query
    await expectDesiredPut(client, 'foo', 'query-hash1');

    await expectGotPut(client, 'query-hash1');

    // Inactivate the first query
    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash1');
    await expectDesiredDel(client, 'foo', 'query-hash1');

    // Check timeout call
    const [fn10s, delay] = setTimeoutFn.mock.lastCall!;
    expect(fn10s).toBeDefined();
    expect(delay).toBe(ttl10s + TTL_TIMER_HYSTERESIS); // Should schedule for the 10s
    setTimeoutFn.mockClear();

    // Now add the second query with 5s TTL
    await addQuery(vs, SYNC_CONTEXT, 'query-hash2', USERS_QUERY, ttl5s);

    await expectDesiredPut(client, 'foo', 'query-hash2');
    await expectGotPut(client, 'query-hash2');
    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash2');
    await expectDesiredDel(client, 'foo', 'query-hash2');

    // We should have cancelled the timeout for 10s and add a new timeout for 5s
    expect(setTimeoutFn.mock.lastCall?.[1]).toBe(ttl5s + TTL_TIMER_HYSTERESIS);
    callNextSetTimeout(ttl5s);

    await expectGotDel(client, 'query-hash2');

    // Remove the TTL_FLUSH_INTERVAL call
    expect(setTimeoutFn.mock.calls.pop()?.[1]).toEqual(TTL_CLOCK_INTERVAL);

    // Another timeout should be scheduled for the remaining 5s TTL
    const [fn10sAgain, remainingDelay] = setTimeoutFn.mock.lastCall!;
    expect(fn10sAgain).toBeDefined();
    expect(remainingDelay).toBe(ttl10s - ttl5s + TTL_TIMER_HYSTERESIS);

    callNextSetTimeout(ttl10s - ttl5s);

    await expectGotDel(client, 'query-hash1');
  });

  test('reschedule timer when query with shorter TTL is added after inactivation', async () => {
    const ttl10s = 10 * 1000; // 10 seconds
    const ttl5s = 5 * 1000; // 5 seconds
    vi.setSystemTime(new Date(2025, 6, 17));

    // Start with a query that has 10s TTL
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl: ttl10s},
    ]);

    stateChanges.push({state: 'version-ready'});
    await expectDesiredPut(client, 'foo', 'query-hash1');
    await expectGotPut(client, 'query-hash1');

    // Inactivate the query - this should schedule eviction in 10s
    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash1');
    await expectDesiredDel(client, 'foo', 'query-hash1');

    // Verify the timer was scheduled for 10s
    const [, delay] = setTimeoutFn.mock.lastCall!;
    expect(delay).toBe(ttl10s + TTL_TIMER_HYSTERESIS);
    setTimeoutFn.mockClear();

    // Re-add the same query with a shorter TTL (5s)
    // This should trigger rescheduling since the query is already inactive
    await addQuery(vs, SYNC_CONTEXT, 'query-hash1', ISSUES_QUERY, ttl5s);
    await expectDesiredPut(client, 'foo', 'query-hash1');
    // and inactivate it again. We should use the new shorter TTL here.
    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash1');
    await expectDesiredDel(client, 'foo', 'query-hash1');

    // Verify the timer was rescheduled for 5s (the shorter TTL)
    callNextSetTimeout(ttl5s, 5_000 + TTL_TIMER_HYSTERESIS);

    await expectGotDel(client, 'query-hash1');
    await expectNoPokes(client);
  });

  test('Collapse multiple eviction that are close in time into one poke', async () => {
    const ttl = 1000;
    vi.setSystemTime(Date.now());

    // Connect a client with two TTL queries
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      {op: 'put', hash: 'query-hash2', ast: USERS_QUERY, ttl},
    ]);

    stateChanges.push({state: 'version-ready'});

    // Expect both queries to be added
    await expectDesiredPut(client, 'foo', 'query-hash1', 'query-hash2');
    await expectGotPut(client, 'query-hash1', 'query-hash2');

    // Inactivate the first query
    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash1');
    await expectDesiredDel(client, 'foo', 'query-hash1');

    // Move time forward less than 50ms (the collapse delay)
    vi.setSystemTime(Date.now() + 30);

    // Inactivate the second query
    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash2');
    await expectDesiredDel(client, 'foo', 'query-hash2');

    await expectNoPokes(client);

    // Now advance the TTL time to trigger eviction
    // This should trigger a single timer that removes both queries
    callNextSetTimeout(ttl + TTL_TIMER_HYSTERESIS); // TTL + the collapse delay

    // Both queries should be removed in a single poke
    await expectGotDel(client, 'query-hash1', 'query-hash2');

    await expectNoPokes(client);
  });

  test('inspect query does not include expired queries', async () => {
    const ttl = 100;
    vi.setSystemTime(Date.now());

    // Test with one query to keep it simple, similar to existing TTL tests
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
    ]);

    stateChanges.push({state: 'version-ready'});
    await expectDesiredPut(client, 'foo', 'query-hash1');

    await expectGotPut(client, 'query-hash1');

    // Force query materialization by processing a transaction that affects the query
    // This ensures that server-side metrics are actually created in #perQueryServerMetrics
    replicator.processTransaction(
      'test-txn',
      messages.insert('issues', {
        id: 'test-issue',
        title: 'Test Issue',
        owner: '100',
        big: 1000,
        json: null,
        parent: null,
      }),
    );
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    // Verify the query has metrics via inspect queries operation
    // This indirectly tests that #perQueryServerMetrics contains an entry for this query
    const inspectId1 = 'test-metrics-before-ttl';
    await vs.inspect(SYNC_CONTEXT, [
      'inspect',
      {op: 'queries', id: inspectId1, clientID: SYNC_CONTEXT.clientID},
    ]);

    const queriesBeforeExpiry = await client.dequeue();
    expect(queriesBeforeExpiry).toMatchObject([
      'inspect',
      expect.objectContaining({
        id: inspectId1,
        op: 'queries',
        value: expect.arrayContaining([
          expect.objectContaining({
            queryID: 'query-hash1',
            metrics: expect.objectContaining({
              'query-materialization-server': expect.arrayContaining([
                expect.any(Number),
              ]),
            }),
          }),
        ]),
      }),
    ]);

    // Inactivate the query to trigger TTL timer
    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash1');
    await expectDesiredDel(client, SYNC_CONTEXT.clientID, 'query-hash1');

    await expectNoPokes(client);

    // Advance time to trigger TTL expiration for query-hash1
    callNextSetTimeout(ttl);
    await expectGotDel(client, 'query-hash1');

    // Verify that the query no longer appears in inspect results
    // This confirms that both the query AND its metrics in #perQueryServerMetrics were cleaned up
    const inspectId2 = 'test-metrics-after-ttl';
    await vs.inspect(SYNC_CONTEXT, [
      'inspect',
      {op: 'queries', id: inspectId2, clientID: SYNC_CONTEXT.clientID},
    ]);

    const queriesAfterExpiry = await client.dequeue();
    expect(queriesAfterExpiry).toMatchObject([
      'inspect',
      expect.objectContaining({
        id: inspectId2,
        op: 'queries',
        value: [], // Should be empty since the query was removed
      }),
    ]);

    await expectNoPokes(client);
  });

  test('queries with TTL larger than MAX_TTL are clamped to MAX_TTL', async () => {
    const largeTTL = 24 * 60 * 60 * 1000; // 24 hours = 86400000 ms (larger than MAX_TTL)
    const MAX_TTL_MS = 10 * 60 * 1000; // 10 minutes
    vi.setSystemTime(Date.UTC(2025, 6, 1, 12, 0, 0));

    // Connect a client with a large TTL query
    const {queue: client} = connectWithQueueAndSource(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl: largeTTL},
    ]);

    stateChanges.push({state: 'version-ready'});
    await nextPoke(client); // config
    await nextPoke(client); // hydration

    // Mark the query as inactive - this should preserve the clamped TTL
    await inactivateQuery(vs, SYNC_CONTEXT, 'query-hash1');

    // Advance time by 10 minutes (the MAX_TTL)
    // The query should expire now, not after 24 hours
    callNextSetTimeout(MAX_TTL_MS);

    // Verify the query was removed after MAX_TTL, not after 24 hours
    // If the TTL was not clamped, the query would still be alive
    await expectDesiredDel(client, 'query-hash1');
    await expectGotDel(client, 'query-hash1');

    await expectNoPokes(client);
  });
});
