import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
  vi,
  type MockedFunction,
} from 'vitest';
import type {JSONObject} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {ZeroEvent} from '../../../../zero-events/src/index.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {initEventSinkForTesting} from '../../observability/events.ts';
import {DbFile, expectTables, initDB} from '../../test/lite.ts';
import {Subscription} from '../../types/subscription.ts';
import {orTimeoutWith} from '../../types/timeout.ts';
import {
  PROTOCOL_VERSION,
  type Downstream,
  type SubscriberContext,
} from '../change-streamer/change-streamer.ts';
import {IncrementalSyncer} from './incremental-sync.ts';
import {
  createReplicationStateTables,
  initReplicationState,
} from './schema/replication-state.ts';
import {ReplicationMessages} from './test-utils.ts';
import {ThreadWriteWorkerClient} from './write-worker-client.ts';

const TASK_ID = 'task-id';
const REPLICA_ID = 'incremental_sync_test_id';

describe('replicator/incremental-sync', () => {
  let lc: LogContext;
  let dbFile: DbFile;
  let mainDb: Database;
  let worker: ThreadWriteWorkerClient;
  let syncer: IncrementalSyncer;
  let syncing: Promise<void> | undefined;
  let downstream: Subscription<Downstream>;
  let eventSink: ZeroEvent[];
  let subscribeFn: MockedFunction<
    (ctx: SubscriberContext) => Promise<Subscription<Downstream>>
  >;

  beforeEach(async () => {
    lc = createSilentLogContext();
    dbFile = new DbFile('incremental-sync-test');
    mainDb = dbFile.connect(lc);
    mainDb.pragma('journal_mode = wal');
    createReplicationStateTables(mainDb);

    downstream = Subscription.create();
    eventSink = [];
    initEventSinkForTesting(
      eventSink,
      new Date(Date.UTC(2025, 7, 14, 1, 2, 3)),
    );
    subscribeFn = vi.fn();
    worker = new ThreadWriteWorkerClient();
    await worker.init(
      dbFile.path,
      'serving',
      {
        busyTimeout: 30000,
        analysisLimit: 1000,
      },
      {level: 'error', format: 'text'},
    );
    syncer = new IncrementalSyncer(
      lc,
      TASK_ID,
      REPLICA_ID,
      {subscribe: subscribeFn.mockResolvedValue(downstream)},
      mainDb,
      worker,
      'serving',
      true,
    );
  });

  afterEach(async () => {
    downstream?.cancel();
    syncer?.stop(lc);
    // Wait for the run loop to finish so any in-flight worker call
    // completes before we send 'stop' to the worker.
    await syncing?.catch(() => {});
    await worker?.stop();
    mainDb?.close();
    dbFile?.delete();
  });

  test('replicates transactions', async () => {
    const issues = new ReplicationMessages({issues: ['issueID', 'bool']});

    initReplicationState(mainDb, ['zero_data'], '02', {}, false);

    initDB(
      mainDb,
      `
    CREATE TABLE issues(
      issueID INTEGER,
      bool BOOL,
      big INTEGER,
      flt REAL,
      description TEXT,
      json JSON,
      json2 JSONB,
      time TIMESTAMPTZ,
      bytes bytesa,
      intArray int4[],
      _0_version TEXT,
      PRIMARY KEY(issueID, bool)
    );
      `,
    );

    syncing = syncer.run();
    const notifications = syncer.subscribe();
    const versionReady = notifications[Symbol.asyncIterator]();
    await versionReady.next(); // Get the initial nextStateVersion.
    await vi.waitFor(() => expect(subscribeFn).toHaveBeenCalled());
    expect(subscribeFn.mock.calls[0][0]).toEqual({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'incremental_sync_test_id',
      mode: 'serving',
      replicaVersion: '02',
      watermark: '02',
      initial: true,
    });

    for (const change of [
      ['status', {tag: 'status'}],
      ['begin', issues.begin(), {commitWatermark: '06'}],
      ['data', issues.insert('issues', {issueID: 123, bool: true})],
      ['data', issues.insert('issues', {issueID: 456, bool: false})],
      ['commit', issues.commit(), {watermark: '06'}],

      ['begin', issues.begin(), {commitWatermark: '0b'}],
      [
        'data',
        issues.insert('issues', {
          issueID: 789,
          bool: true,
          big: 9223372036854775807n,
          json: [{foo: 'bar', baz: 123}],
          json2: true,
          time: 1728345600123456n,
          bytes: Buffer.from('world'),
          intArray: [3, 2, 1],
        } as unknown as Record<string, JSONObject>),
      ],
      ['data', issues.insert('issues', {issueID: 987, bool: true})],
      [
        'data',
        issues.insert('issues', {issueID: 234, bool: false, flt: 123.456}),
      ],
      ['commit', issues.commit(), {watermark: '0b'}],
    ] satisfies Downstream[]) {
      downstream.push(change);
      if (change[0] === 'commit') {
        await Promise.race([versionReady.next(), syncing]);
      }
    }

    expectTables(
      mainDb,
      {
        issues: [
          {
            issueID: 123n,
            big: null,
            flt: null,
            bool: 1n,
            description: null,
            json: null,
            json2: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '06',
          },
          {
            issueID: 456n,
            big: null,
            flt: null,
            bool: 0n,
            description: null,
            json: null,
            json2: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '06',
          },
          {
            issueID: 789n,
            big: 9223372036854775807n,
            flt: null,
            bool: 1n,
            description: null,
            json: '[{"foo":"bar","baz":123}]',
            json2: 'true',
            time: 1728345600123456n,
            bytes: Buffer.from('world'),
            intArray: '[3,2,1]',
            ['_0_version']: '0b',
          },
          {
            issueID: 987n,
            big: null,
            flt: null,
            bool: 1n,
            description: null,
            json: null,
            json2: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '0b',
          },
          {
            issueID: 234n,
            big: null,
            flt: 123.456,
            bool: 0n,
            description: null,
            json: null,
            json2: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '0b',
          },
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '06',
            pos: 0n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":123}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '06',
            pos: 1n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":456}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0b',
            pos: 0n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":789}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0b',
            pos: 1n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":987}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0b',
            pos: 2n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":234}',
            backfillingColumnVersions: '{}',
          },
        ],
      },
      'bigint',
    );

    expect(eventSink).toMatchInlineSnapshot(`
      [
        {
          "component": "replication",
          "description": "Replicating from 02",
          "stage": "Replicating",
          "state": {
            "indexes": [
              {
                "columns": [
                  {
                    "column": "bool",
                    "dir": "ASC",
                  },
                  {
                    "column": "issueID",
                    "dir": "ASC",
                  },
                ],
                "table": "issues",
                "unique": true,
              },
            ],
            "replicaSize": 57344,
            "tables": [
              {
                "columns": [
                  {
                    "clientType": "string",
                    "column": "_0_version",
                    "upstreamType": "TEXT",
                  },
                  {
                    "clientType": "number",
                    "column": "big",
                    "upstreamType": "INTEGER",
                  },
                  {
                    "clientType": "boolean",
                    "column": "bool",
                    "upstreamType": "BOOL",
                  },
                  {
                    "clientType": null,
                    "column": "bytes",
                    "upstreamType": "bytesa",
                  },
                  {
                    "clientType": "string",
                    "column": "description",
                    "upstreamType": "TEXT",
                  },
                  {
                    "clientType": "number",
                    "column": "flt",
                    "upstreamType": "REAL",
                  },
                  {
                    "clientType": "json",
                    "column": "intArray",
                    "upstreamType": "int4[]",
                  },
                  {
                    "clientType": "number",
                    "column": "issueID",
                    "upstreamType": "INTEGER",
                  },
                  {
                    "clientType": "json",
                    "column": "json",
                    "upstreamType": "JSON",
                  },
                  {
                    "clientType": "json",
                    "column": "json2",
                    "upstreamType": "JSONB",
                  },
                  {
                    "clientType": "number",
                    "column": "time",
                    "upstreamType": "TIMESTAMPTZ",
                  },
                ],
                "table": "issues",
              },
            ],
          },
          "status": "OK",
          "time": "2025-08-14T01:02:03.000Z",
          "type": "zero/events/status/replication/v1",
        },
      ]
    `);
  });

  test('replicates schema changes', async () => {
    const issues = new ReplicationMessages({issues: ['issueID', 'bool']});

    initReplicationState(mainDb, ['zero_data'], '09', {}, false);

    initDB(
      mainDb,
      `
    CREATE TABLE issues(
      issueID INTEGER,
      bool BOOL,
      big INTEGER,
      _0_version TEXT,
      PRIMARY KEY(issueID, bool)
    );
      `,
    );

    syncing = syncer.run();
    const notifications = syncer.subscribe();
    const versionReady = notifications[Symbol.asyncIterator]();
    await versionReady.next(); // Get the initial nextStateVersion.
    await vi.waitFor(() => expect(subscribeFn).toHaveBeenCalled());
    expect(subscribeFn.mock.calls[0][0]).toEqual({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'incremental_sync_test_id',
      mode: 'serving',
      replicaVersion: '09',
      watermark: '09',
      initial: true,
    });

    for (const change of [
      ['status', {tag: 'status'}],
      ['begin', issues.begin(), {commitWatermark: '110'}],
      [
        'data',
        issues.addColumn('issues', 'new_column', {pos: 4, dataType: 'int8'}),
      ],
      ['commit', issues.commit(), {watermark: '110'}],
    ] satisfies Downstream[]) {
      downstream.push(change);
      if (change[0] === 'commit') {
        await Promise.race([versionReady.next(), syncing]);
      }
    }

    expect(eventSink).toMatchInlineSnapshot(`
      [
        {
          "component": "replication",
          "description": "Replicating from 09",
          "stage": "Replicating",
          "state": {
            "indexes": [
              {
                "columns": [
                  {
                    "column": "bool",
                    "dir": "ASC",
                  },
                  {
                    "column": "issueID",
                    "dir": "ASC",
                  },
                ],
                "table": "issues",
                "unique": true,
              },
            ],
            "replicaSize": 57344,
            "tables": [
              {
                "columns": [
                  {
                    "clientType": "string",
                    "column": "_0_version",
                    "upstreamType": "TEXT",
                  },
                  {
                    "clientType": "number",
                    "column": "big",
                    "upstreamType": "INTEGER",
                  },
                  {
                    "clientType": "boolean",
                    "column": "bool",
                    "upstreamType": "BOOL",
                  },
                  {
                    "clientType": "number",
                    "column": "issueID",
                    "upstreamType": "INTEGER",
                  },
                ],
                "table": "issues",
              },
            ],
          },
          "status": "OK",
          "time": "2025-08-14T01:02:03.000Z",
          "type": "zero/events/status/replication/v1",
        },
        {
          "component": "replication",
          "description": "Schema updated",
          "stage": "Replicating",
          "state": {
            "indexes": [
              {
                "columns": [
                  {
                    "column": "bool",
                    "dir": "ASC",
                  },
                  {
                    "column": "issueID",
                    "dir": "ASC",
                  },
                ],
                "table": "issues",
                "unique": true,
              },
            ],
            "replicaSize": 65536,
            "tables": [
              {
                "columns": [
                  {
                    "clientType": "string",
                    "column": "_0_version",
                    "upstreamType": "TEXT",
                  },
                  {
                    "clientType": "number",
                    "column": "big",
                    "upstreamType": "INTEGER",
                  },
                  {
                    "clientType": "boolean",
                    "column": "bool",
                    "upstreamType": "BOOL",
                  },
                  {
                    "clientType": "number",
                    "column": "issueID",
                    "upstreamType": "INTEGER",
                  },
                  {
                    "clientType": "number",
                    "column": "new_column",
                    "upstreamType": "int8",
                  },
                ],
                "table": "issues",
              },
            ],
          },
          "status": "OK",
          "time": "2025-08-14T01:02:03.000Z",
          "type": "zero/events/status/replication/v1",
        },
      ]
    `);
  });

  async function noNotification(
    notification: Promise<IteratorResult<unknown>>,
  ) {
    expect(await orTimeoutWith(notification, 50, 'timed-out')).toBe(
      'timed-out',
    );
  }

  test('does not notify on incomplete backfills', async () => {
    const issues = new ReplicationMessages({issues: ['issueID']});

    initReplicationState(mainDb, ['zero_data'], '09', {}, false);

    initDB(
      mainDb,
      /*sql*/ `
    CREATE TABLE issues(
      issueID INTEGER PRIMARY KEY,
      big INTEGER,
      _0_version TEXT
    );
    CREATE UNIQUE INDEX issues_pkey ON issues ("issueID");

    INSERT INTO issues ("issueID", big, _0_version) VALUES (1, 2, '100');
    INSERT INTO issues ("issueID", big, _0_version) VALUES (2, 3, '100');
      `,
    );

    syncing = syncer.run();
    const notifications = syncer.subscribe();
    const versionReady = notifications[Symbol.asyncIterator]();
    await versionReady.next(); // Get the initial nextStateVersion.
    await vi.waitFor(() => expect(subscribeFn).toHaveBeenCalled());
    expect(subscribeFn.mock.calls[0][0]).toEqual({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'incremental_sync_test_id',
      mode: 'serving',
      replicaVersion: '09',
      watermark: '09',
      initial: true,
    });

    const next = versionReady.next();

    for (const change of [
      ['status', {tag: 'status'}],
      ['begin', issues.begin(), {commitWatermark: '110'}],
      [
        'data',
        issues.addColumn(
          'issues',
          'new_column',
          {pos: 4, dataType: 'text'},
          {backfill: {id: 123}},
        ),
      ],
      ['commit', issues.commit(), {watermark: '110'}],
      ['begin', issues.begin(), {commitWatermark: '110.01'}],
      [
        'data',
        {
          tag: 'backfill',
          relation: {
            schema: 'public',
            name: 'issues',
            rowKey: {columns: ['issueID']},
          },
          watermark: '110',
          columns: ['new_column'],
          rowValues: [[1, 'hello']],
        },
      ],
      ['commit', issues.commit(), {watermark: '110.01'}],
    ] satisfies Downstream[]) {
      downstream.push(change);
    }

    // Ensure no notifications have been published.
    await noNotification(next);

    // And that row versions have not changed, even for backfilled rows.
    const issuesDump = mainDb.prepare(/*sql*/ `SELECT * FROM issues`);
    expect(issuesDump.all()).toEqual([
      {
        _0_version: '100',
        big: 2,
        issueID: 1,
        new_column: 'hello',
      },
      {
        _0_version: '100',
        big: 3,
        issueID: 2,
        new_column: null,
      },
    ]);

    // Complete the backfill.
    for (const change of [
      ['begin', issues.begin(), {commitWatermark: '110.02'}],
      [
        'data',
        {
          tag: 'backfill',
          relation: {
            schema: 'public',
            name: 'issues',
            rowKey: {columns: ['issueID']},
          },
          watermark: '110',
          columns: ['new_column'],
          rowValues: [[2, 'world']],
        },
      ],
      [
        'data',
        {
          tag: 'backfill-completed',
          relation: {
            schema: 'public',
            name: 'issues',
            rowKey: {columns: ['issueID']},
          },
          columns: ['new_column'],
          watermark: '110',
        },
      ],
      ['commit', issues.commit(), {watermark: '110.02'}],
    ] satisfies Downstream[]) {
      downstream.push(change);
    }

    // Now there should be a notification.
    expect(
      await orTimeoutWith(next, 5000, new Error('timed-out')),
    ).not.toBeInstanceOf(Error);

    // The row version in the table metadata should be bumped.
    expect(
      mainDb.prepare(/*sql*/ `SELECT * FROM "_zero.tableMetadata"`).get(),
    ).toMatchObject({
      minRowVersion: '110.02',
      schema: 'public',
      table: 'issues',
    });
    // (The row columns themselves are not updated ... too costly)
    expect(issuesDump.all()).toEqual([
      {
        _0_version: '100',
        big: 2,
        issueID: 1,
        new_column: 'hello',
      },
      {
        _0_version: '100',
        big: 3,
        issueID: 2,
        new_column: 'world',
      },
    ]);
  });

  test('retry on initial change-streamer connection failure', async () => {
    initReplicationState(mainDb, ['zero_data'], '02', {}, false);

    const {promise: hasRetried, resolve: retried} = resolver<true>();
    const syncer = new IncrementalSyncer(
      lc,
      TASK_ID,
      REPLICA_ID,
      {
        subscribe: vi
          .fn()
          .mockRejectedValueOnce('error')
          .mockImplementation(() => {
            retried(true);
            return resolver().promise;
          }),
      },
      mainDb,
      worker,
      'serving',
      true,
    );

    const localSyncing = syncer.run();

    expect(await hasRetried).toBe(true);

    syncer.stop(lc);
    void localSyncing.catch(() => {});
  });

  test('retry on error in change-stream', async () => {
    initReplicationState(mainDb, ['zero_data'], '02', {}, false);

    const {promise: hasRetried, resolve: retried} = resolver<true>();
    const syncer = new IncrementalSyncer(
      lc,
      TASK_ID,
      REPLICA_ID,
      {
        subscribe: vi
          .fn()
          .mockImplementationOnce(() => Promise.resolve(downstream))
          .mockImplementation(() => {
            retried(true);
            return resolver().promise;
          }),
      },
      mainDb,
      worker,
      'serving',
      true,
    );

    const localSyncing = syncer.run();

    downstream.fail(new Error('doh'));

    expect(await hasRetried).toBe(true);

    syncer.stop(lc);
    void localSyncing.catch(() => {});
  });
});
