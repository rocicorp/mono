import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {Queue} from 'shared/src/queue.js';
import {sleep} from 'shared/src/sleep.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {initDB, testDBs} from '../../test/db.js';
import {normalizeFilterSpec} from '../../types/invalidation.js';
import type {PostgresDB} from '../../types/pg.js';
import {Subscription} from '../../types/subscription.js';
import type {
  RegisterInvalidationFiltersResponse,
  Replicator,
  VersionChange,
} from '../replicator/replicator.js';
import {CREATE_INVALIDATION_TABLES} from '../replicator/schema/invalidation.js';
import {CREATE_REPLICATION_TABLES} from '../replicator/schema/replication.js';
import {
  InvalidationWatcherService,
  WatchRequest,
  type QueryInvalidationUpdate,
} from './invalidation-watcher.js';

describe('invalidation-watcher', () => {
  let db: PostgresDB;

  beforeEach(async () => {
    db = await testDBs.create('invalidation_watcher_test');
    await db.unsafe(
      `CREATE SCHEMA _zero;` +
        CREATE_INVALIDATION_TABLES +
        CREATE_REPLICATION_TABLES,
    );
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  type Case = {
    name: string;
    setupDB?: string;
    registerFilterResponses?: RegisterInvalidationFiltersResponse[];
    versionChanges: [stmt: string, change: VersionChange][];
    incrementalWatchRequest?: WatchRequest;
    expectedIncrementalUpdates: Omit<QueryInvalidationUpdate, 'reader'>[];
    coalescedWatchRequest?: WatchRequest;
    expectedCoalescedUpdates: Omit<QueryInvalidationUpdate, 'reader'>[];
  };

  const FOO_SPEC1 = normalizeFilterSpec({
    schema: 'public',
    table: 'foo',
    filteredColumns: {id: '='},
  });

  const FOO_SPEC2 = normalizeFilterSpec({
    schema: 'public',
    table: 'foo',
    filteredColumns: {id: '=', name: '='},
    selectedColumns: ['id', 'name'],
  });

  const FOO_SPEC3 = normalizeFilterSpec({
    schema: 'public',
    table: 'foo',
    filteredColumns: {name: '='},
    selectedColumns: ['id', 'name'],
  });

  const BAR_SPEC1 = normalizeFilterSpec({
    schema: 'public',
    table: 'bar',
    filteredColumns: {id: '='},
  });

  const BAR_SPEC2 = normalizeFilterSpec({
    schema: 'public',
    table: 'bar',
    filteredColumns: {},
    selectedColumns: ['id', 'name'],
  });

  const BAR_SPEC3 = normalizeFilterSpec({
    schema: 'public',
    table: 'bar',
    filteredColumns: {name: '='},
    selectedColumns: ['id', 'name'],
  });

  const INCREMENTAL_WATCH_REQUEST = {
    queries: {
      q1: {filters: [FOO_SPEC1, FOO_SPEC2], hashes: ['beefcafe', '01010101']},
      q2: {filters: [FOO_SPEC1], hashes: ['0abc1230']},
      q3: {filters: [FOO_SPEC3], hashes: ['12344321']},
    },
  };
  const COALESCED_WATCH_REQUEST = {
    queries: {
      q1: {filters: [BAR_SPEC1], hashes: ['01234567']},
      q2: {filters: [BAR_SPEC2], hashes: ['87654321']},
      q3: {filters: [BAR_SPEC3], hashes: ['01100110']},
    },
  };

  const cases: Case[] = [
    {
      name: 'update with hashes',
      setupDB: `
      INSERT INTO _zero."TxLog" ("stateVersion", lsn, time, xid)
        VALUES ('0a', '0/511', '2024-04-15T00:00:01Z', 103);
      `,
      versionChanges: [
        [
          `
        INSERT INTO _zero."TxLog" ("stateVersion", lsn, time, xid)
          VALUES ('101', '0/511', '2024-04-15T00:00:02Z', 123);
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\xbeefcafe', '101');
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x01234567', '0a');
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x0abc1230', '0a');
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x87654321', '101');  
        `,
          {
            newVersion: '101',
            prevVersion: '0a',
            invalidations: {['beefcafe']: '101', ['87654321']: '101'},
          },
        ],
      ],
      expectedIncrementalUpdates: [
        {
          version: '0a',
          fromVersion: null, // Initial update
          invalidatedQueries: new Set(),
        },
        {
          version: '101',
          fromVersion: '0a',
          invalidatedQueries: new Set(['q1']),
        },
      ],
      expectedCoalescedUpdates: [
        {
          version: '101',
          fromVersion: null,
          invalidatedQueries: new Set(['q2']),
        },
      ],
    },
    {
      name: 'update without hashes',
      setupDB: `
      INSERT INTO _zero."TxLog" ("stateVersion", lsn, time, xid)
        VALUES ('0a', '0/511', '2024-04-15T00:00:01Z', 103);
      `,
      versionChanges: [
        [
          `
        INSERT INTO _zero."TxLog" ("stateVersion", lsn, time, xid)
          VALUES ('101', '0/511', '2024-04-15T00:00:02Z', 123);
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\xbeefcafe', '101');
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x01234567', '0a');
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x0abc1230', '0a');
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x87654321', '101'); 
          `,
          {newVersion: '101', prevVersion: '0a'},
        ],
      ],
      expectedIncrementalUpdates: [
        {
          version: '0a',
          fromVersion: null,
          invalidatedQueries: new Set(),
        },
        {
          version: '101',
          fromVersion: '0a',
          invalidatedQueries: new Set(['q1']),
        },
      ],
      expectedCoalescedUpdates: [
        {
          version: '101',
          fromVersion: null,
          invalidatedQueries: new Set(['q2']),
        },
      ],
    },
    {
      name: 'snapshot ahead of update',
      setupDB: `
      INSERT INTO _zero."TxLog" ("stateVersion", lsn, time, xid)
        VALUES ('01', '0/511', '2024-04-15T00:00:00Z', 100);
      `,
      versionChanges: [
        [
          `
        INSERT INTO _zero."TxLog" ("stateVersion", lsn, time, xid)
          VALUES ('101', '0/511', '2024-04-15T00:00:02Z', 123);
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\xbeefcafe', '101');
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x01234567', '0a');
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x01010101', '01');
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x0abc1230', '0a');
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x87654321', '101');           
          `,
          {
            newVersion: '0a',
            prevVersion: '01',
            invalidations: {['01234567']: '0a', ['0abc1230']: '0a'},
          },
        ],
      ],
      expectedIncrementalUpdates: [
        {
          version: '01',
          fromVersion: null,
          invalidatedQueries: new Set(),
        },
        {
          version: '101',
          fromVersion: '01',
          invalidatedQueries: new Set(['q1', 'q2']),
        },
      ],
      expectedCoalescedUpdates: [
        {
          version: '101',
          fromVersion: null,
          invalidatedQueries: new Set(['q1', 'q2']),
        },
      ],
    },
    {
      name: 'incremental updates',
      versionChanges: [
        [
          `
        INSERT INTO _zero."TxLog" ("stateVersion", lsn, time, xid)
          VALUES ('01', '0/511', '2024-04-15T00:00:00Z', 101);
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x01010101', '01');
          `,
          {newVersion: '01', prevVersion: '00'},
        ],
        [
          `
        INSERT INTO _zero."TxLog" ("stateVersion", lsn, time, xid)
          VALUES ('0a', '0/511', '2024-04-15T00:00:01Z', 111);
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x01234567', '0a');
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x0abc1230', '0a');
          `,
          {newVersion: '0a', prevVersion: '01'},
        ],
        [
          `
        INSERT INTO _zero."TxLog" ("stateVersion", lsn, time, xid)
          VALUES ('101', '0/511', '2024-04-15T00:00:02Z', 123);
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\xbeefcafe', '101');
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x87654321', '101');
          `,
          {newVersion: '101', prevVersion: '0a'},
        ],
      ],
      expectedIncrementalUpdates: [
        {
          version: '00',
          fromVersion: null,
          invalidatedQueries: new Set(),
        },
        {
          version: '01',
          fromVersion: '00',
          invalidatedQueries: new Set(['q1']),
        },
        {
          version: '0a',
          fromVersion: '01',
          invalidatedQueries: new Set(['q2']),
        },
        {
          version: '101',
          fromVersion: '0a',
          invalidatedQueries: new Set(['q1']),
        },
      ],
      expectedCoalescedUpdates: [
        {
          version: '101',
          fromVersion: null,
          invalidatedQueries: new Set(['q1', 'q2']),
        },
      ],
    },
    {
      name: 'initial filter invalidation',
      setupDB: `
      INSERT INTO _zero."TxLog" ("stateVersion", lsn, time, xid)
        VALUES ('0a', '0/511', '2024-04-15T00:00:01Z', 103);
      INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x12344321', '04');
      INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x01100110', '01');
      `,
      versionChanges: [
        [
          `
        INSERT INTO _zero."TxLog" ("stateVersion", lsn, time, xid)
          VALUES ('101', '0/511', '2024-04-15T00:00:02Z', 123);
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\xbeefcafe', '101');
        INSERT INTO _zero."InvalidationIndex" (hash, "stateVersion")
          VALUES ('\\x87654321', '101'); 
          `,
          {newVersion: '101', prevVersion: '0a'},
        ],
      ],
      incrementalWatchRequest: {
        ...INCREMENTAL_WATCH_REQUEST,
        fromVersion: '01',
      },
      coalescedWatchRequest: {
        ...COALESCED_WATCH_REQUEST,
        fromVersion: '01',
      },
      registerFilterResponses: [
        {
          specs: [
            {id: FOO_SPEC1.id, fromStateVersion: '01'}, // already registered
            {id: FOO_SPEC2.id, fromStateVersion: '02'}, // newly registered
            {id: FOO_SPEC3.id, fromStateVersion: '01'}, // already registered
          ],
        },
        {specs: [{id: BAR_SPEC1.id, fromStateVersion: '09'}]},
      ],
      expectedIncrementalUpdates: [
        {
          version: '0a',
          fromVersion: '01',
          // Initial update invalidates:
          // - q1 as its FOO_SPEC2 was newly registered,
          // - q3 because the '12344321' hash at "04" is newer than fromVersion: "01".
          invalidatedQueries: new Set(['q1', 'q3']),
        },
        {
          version: '101',
          fromVersion: '0a',
          invalidatedQueries: new Set(['q1']),
        },
      ],
      expectedCoalescedUpdates: [
        {
          // The invalidation of q1 (from the newly registered BAR_SPEC1)
          // from the initial update should be coalesced
          // with the incremental update invalidating q2.
          // Note that q3 is *not* invalidated because its hash version ("01")
          // is less than or equal to fromVersion
          version: '101',
          fromVersion: '01',
          invalidatedQueries: new Set(['q1', 'q2']),
        },
      ],
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      await initDB(db, c.setupDB);

      const versionChanges = new Subscription<VersionChange>();
      const registerFilterResponses = c.registerFilterResponses ?? [];
      const replicator: Replicator = {
        versionChanges: () => Promise.resolve(versionChanges),
        registerInvalidationFilters: () =>
          Promise.resolve(registerFilterResponses.shift() ?? {specs: []}),
      };

      const lc = createSilentLogContext();
      const watcher = new InvalidationWatcherService(
        'id',
        lc,
        {getReplicator: () => Promise.resolve(replicator)},
        db,
      );
      const watcherDone = watcher.run();

      const incrementalSub = await watcher.watch(
        c.incrementalWatchRequest ?? INCREMENTAL_WATCH_REQUEST,
      );

      // Read from the incrementalSub in the background.
      const incrementalUpdates = (async () => {
        const updates: QueryInvalidationUpdate[] = [];
        if (c.expectedIncrementalUpdates.length) {
          let i = 0;
          for await (const update of incrementalSub) {
            updates.push(update);
            if (++i === c.expectedIncrementalUpdates.length) {
              break;
            }
          }
        }
        return updates;
      })();

      const coalescedSub = await watcher.watch(
        c.coalescedWatchRequest ?? COALESCED_WATCH_REQUEST,
      );

      for (const [stmt, versionChange] of c.versionChanges) {
        await db.unsafe(stmt);
        versionChanges.push(versionChange);
        // Allow time for the incremental update loop to take a snapshot and process.
        await sleep(10);
      }

      const updates = await incrementalUpdates;
      for (let i = 0; i < updates.length; i++) {
        expect(updates[i]).toMatchObject(c.expectedIncrementalUpdates[i]);
      }

      if (c.expectedCoalescedUpdates.length) {
        let i = 0;
        for await (const update of coalescedSub) {
          expect(update).toMatchObject(c.expectedCoalescedUpdates[i]);
          if (++i === c.expectedCoalescedUpdates.length) {
            break;
          }
        }
      }

      await watcher.stop();
      await watcherDone;
    });
  }

  test('unsubscribe from Replicator when no watchers', async () => {
    const subscriptionOpened = new Queue<true>();
    const subscriptionClosed = new Queue<true>();
    const replicator: Replicator = {
      versionChanges: () => {
        void subscriptionOpened.enqueue(true);
        return Promise.resolve(
          new Subscription<VersionChange>({
            cleanup: () => void subscriptionClosed.enqueue(true),
          }),
        );
      },
      registerInvalidationFilters: () => Promise.resolve({specs: []}),
    };

    const watcher = new InvalidationWatcherService(
      'id',
      createSilentLogContext(),
      {getReplicator: () => Promise.resolve(replicator)},
      db,
    );
    const watcherDone = watcher.run();

    const sub1 = await watcher.watch(INCREMENTAL_WATCH_REQUEST);
    expect(await subscriptionOpened.dequeue()).toBe(true);
    const sub2 = await watcher.watch(INCREMENTAL_WATCH_REQUEST);

    expect(subscriptionOpened.size()).toBe(0);
    expect(subscriptionClosed.size()).toBe(0);

    sub1.cancel();
    expect(subscriptionClosed.size()).toBe(0);
    sub2.cancel();
    expect(await subscriptionClosed.dequeue()).toBe(true);
    expect(subscriptionClosed.size()).toBe(0); // Only called once

    const sub3 = await watcher.watch(INCREMENTAL_WATCH_REQUEST);
    expect(await subscriptionOpened.dequeue()).toBe(true);

    expect(subscriptionOpened.size()).toBe(0);
    expect(subscriptionClosed.size()).toBe(0);

    sub3.cancel();
    expect(await subscriptionClosed.dequeue()).toBe(true);
    expect(subscriptionClosed.size()).toBe(0);

    await watcher.stop();
    await watcherDone;
  });

  test('get table schemas', async () => {
    const replicator: Replicator = {
      versionChanges: () => Promise.reject('unused'),
      registerInvalidationFilters: () => Promise.reject('unused'),
    };
    const watcher = new InvalidationWatcherService(
      'id',
      createSilentLogContext(),
      {getReplicator: () => Promise.resolve(replicator)},
      db,
    );

    await db.unsafe(`
    CREATE SCHEMA zero;
    CREATE TABLE zero.clients(
      "clientGroupID"  TEXT NOT NULL,
      "clientID"       TEXT NOT NULL,
      "lastMutationID" BIGINT,
      "userID"         TEXT,
      PRIMARY KEY ("clientGroupID", "clientID")
    );
    CREATE TABLE issues (
      issue_id INTEGER,
      description TEXT,
      org_id INTEGER,
      component_id INTEGER,
      PRIMARY KEY (org_id, component_id, issue_id)
    );
    CREATE TABLE users (
      user_id INTEGER PRIMARY KEY,
      handle text
    );
    CREATE PUBLICATION zero_data FOR TABLES IN SCHEMA public, zero;
    `);

    expect(await watcher.getTableSchemas()).toEqual([
      {
        schema: 'public',
        name: 'issues',
        columns: {
          ['issue_id']: {
            dataType: 'int4',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: true,
          },
          ['description']: {
            dataType: 'text',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: false,
          },
          ['org_id']: {
            dataType: 'int4',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: true,
          },
          ['component_id']: {
            dataType: 'int4',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: true,
          },
        },
        primaryKey: ['org_id', 'component_id', 'issue_id'],
      },
      {
        schema: 'public',
        name: 'users',
        columns: {
          ['user_id']: {
            dataType: 'int4',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: true,
          },
          handle: {
            characterMaximumLength: null,
            columnDefault: null,
            dataType: 'text',
            notNull: false,
          },
        },
        primaryKey: ['user_id'],
      },
      {
        columns: {
          clientGroupID: {
            characterMaximumLength: null,
            columnDefault: null,
            dataType: 'text',
            notNull: true,
          },
          clientID: {
            characterMaximumLength: null,
            columnDefault: null,
            dataType: 'text',
            notNull: true,
          },
          lastMutationID: {
            characterMaximumLength: null,
            columnDefault: null,
            dataType: 'int8',
            notNull: false,
          },
          userID: {
            characterMaximumLength: null,
            columnDefault: null,
            dataType: 'text',
            notNull: false,
          },
        },
        name: 'clients',
        primaryKey: ['clientGroupID', 'clientID'],
        schema: 'zero',
      },
    ]);
  });
});
