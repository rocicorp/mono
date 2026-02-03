import {
  afterEach,
  beforeEach,
  describe,
  expect,
  type Mock,
  type MockedFunction,
  vi,
} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {type ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {
  TransformResponseBody,
  TransformResponseMessage,
} from '../../../../zero-protocol/src/custom-queries.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../../zero-protocol/src/error-reason.ts';
import type {ErrorBody} from '../../../../zero-protocol/src/error.ts';
import {ProtocolError} from '../../../../zero-protocol/src/error.ts';
import type {
  PokeEndBody,
  PokePartBody,
  PokeStartBody,
} from '../../../../zero-protocol/src/poke.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import {DEFAULT_TTL_MS} from '../../../../zql/src/query/ttl.ts';
import {type ClientGroupStorage} from '../../../../zqlite/src/database-storage.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import type {CustomQueryTransformer} from '../../custom-queries/transform-query.ts';
import {StatementRunner} from '../../db/statements.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {cvrSchema} from '../../types/shards.ts';
import type {Source} from '../../types/streams.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {updateReplicationWatermark} from '../replicator/schema/replication-state.ts';
import {type FakeReplicator} from '../replicator/test-utils.ts';
import {CVRStore} from './cvr-store.ts';
import {CVRQueryDrivenUpdater} from './cvr.ts';
import type {DrainCoordinator} from './drain-coordinator.ts';
import {ttlClockFromNumber} from './ttl-clock.ts';
import {
  app2Messages,
  COMMENTS_QUERY,
  EXPECTED_LMIDS_AST,
  expectNoPokes,
  inactivateQuery,
  ISSUES_QUERY,
  ISSUES_QUERY2,
  ISSUES_QUERY_WITH_EXISTS,
  ISSUES_QUERY_WITH_EXISTS_AND_RELATED,
  ISSUES_QUERY_WITH_NOT_EXISTS_AND_RELATED,
  ISSUES_QUERY_WITH_RELATED,
  messages,
  nextPoke,
  nextPokeParts,
  ON_FAILURE,
  permissionsAll,
  REPLICA_VERSION,
  serviceID,
  setup,
  SHARD,
  TASK_ID,
  USERS_QUERY,
} from './view-syncer-test-util.ts';
import type {ViewSyncerService} from './view-syncer.ts';
import {type SyncContext} from './view-syncer.ts';

describe('view-syncer/service', () => {
  let storageDB: Database;
  let replicaDbFile: DbFile;
  let replica: Database;
  let cvrDB: PostgresDB;
  let upstreamDb: PostgresDB;
  const lc = createSilentLogContext();
  let stateChanges: Subscription<ReplicaState>;
  let drainCoordinator: DrainCoordinator;

  let operatorStorage: ClientGroupStorage;
  let vs: ViewSyncerService;
  let viewSyncerDone: Promise<void>;
  let replicator: FakeReplicator;
  let connect: (
    ctx: SyncContext,
    desiredQueriesPatch: UpQueriesPatch,
    clientSchema?: ClientSchema | null,
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
  let customQueryTransformer: CustomQueryTransformer | undefined;

  function callNextSetTimeout(delta: number) {
    // Sanity check that the system time is the mocked time.
    expect(vi.getRealSystemTime()).not.toBe(vi.getMockedSystemTime());
    vi.setSystemTime(Date.now() + delta);
    // oxlint-disable-next-line no-non-null-assertion
    const fn = setTimeoutFn.mock.lastCall![0];
    fn();
  }

  const SYNC_CONTEXT: SyncContext = {
    clientID: 'foo',
    profileID: 'p0000g00000003203',
    wsID: 'ws1',
    baseCookie: null,
    protocolVersion: PROTOCOL_VERSION,
    tokenData: undefined,
    httpCookie: undefined,
    origin: undefined,
  };

  beforeEach<PgTest>(async ({testDBs}) => {
    ({
      storageDB,
      replicaDbFile,
      replica,
      cvrDB,
      upstreamDb,
      stateChanges,
      drainCoordinator,
      operatorStorage,
      vs,
      viewSyncerDone,
      replicator,
      connect,
      connectWithQueueAndSource,
      setTimeoutFn,
      customQueryTransformer,
    } = await setup(testDBs, 'view_syncer_service_test', permissionsAll));

    return async () => {
      vi.useRealTimers();
      await vs.stop();
      await viewSyncerDone;
      await testDBs.drop(cvrDB, upstreamDb);
      replicaDbFile.delete();
    };
  });

  async function getCVROwner() {
    const [{owner}] = await cvrDB<{owner: string}[]>`
    SELECT owner FROM ${cvrDB(cvrSchema(SHARD))}.instances
       WHERE "clientGroupID" = ${serviceID};
  `;
    return owner;
  }

  test('adds desired queries from initConnectionMessage', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    await nextPoke(client);

    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, Date.now());
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash1'],
          id: 'foo',
        },
      },
      id: '9876',
      queries: {
        'query-hash1': {
          ast: ISSUES_QUERY,
          type: 'client',
          clientState: {foo: {version: {stateVersion: '00', configVersion: 1}}},
          id: 'query-hash1',
        },
      },
      version: {stateVersion: '00', configVersion: 1},
    });
  });

  test('initConnectionMessage for new client group missing clientSchema', async () => {
    const client = connect(
      SYNC_CONTEXT,
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
      null /** no clientSchema */,
    );
    await expect(client.dequeue()).rejects.toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: The initConnection message for a new client group must include client schema.]`,
    );
  });

  test('initConnectionMessage sets profileID', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    await client.dequeue();

    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, Date.now());
    expect(cvr).toMatchObject({
      profileID: SYNC_CONTEXT.profileID,
    });
  });

  test('initConnectionMessage with no profileID sets a default profileID based on the client group ID', async () => {
    const oldSyncContext = {
      ...SYNC_CONTEXT,
      profileID: null,
    };
    const client = connect(oldSyncContext, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    await client.dequeue();

    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, Date.now());
    expect(cvr).toMatchObject({
      profileID: `cg${serviceID}`,
    });
  });

  test('responds to changeDesiredQueries patch', async () => {
    const now = Date.UTC(2025, 1, 20);
    const ttlClock = 0;
    vi.setSystemTime(now);
    connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    // Ignore messages from an old websockets.
    await vs.changeDesiredQueries({...SYNC_CONTEXT, wsID: 'old-wsid'}, [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {op: 'put', hash: 'query-hash-1234567890', ast: USERS_QUERY},
        ],
      },
    ]);

    const inactivatedAt = ttlClock;
    // Change the set of queries.
    await vs.changeDesiredQueries(SYNC_CONTEXT, [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {op: 'put', hash: 'query-hash2', ast: USERS_QUERY},
          {op: 'del', hash: 'query-hash1'},
        ],
      },
    ]);

    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, Date.now());
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash2'],
          id: 'foo',
        },
      },
      id: '9876',
      queries: {
        'lmids': {
          ast: EXPECTED_LMIDS_AST,
          type: 'internal',
          id: 'lmids',
        },
        'query-hash1': {
          ast: ISSUES_QUERY,
          type: 'client',
          clientState: {
            foo: {
              inactivatedAt,
              ttl: DEFAULT_TTL_MS,
              version: {configVersion: 2, stateVersion: '00'},
            },
          },
          id: 'query-hash1',
        },
        'query-hash2': {
          ast: USERS_QUERY,
          type: 'client',
          clientState: {
            foo: {
              inactivatedAt: undefined,
              ttl: DEFAULT_TTL_MS,
              version: {stateVersion: '00', configVersion: 2},
            },
          },
          id: 'query-hash2',
        },
      },
      version: {stateVersion: '00', configVersion: 2},
    });
  });

  test('initial hydration', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
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
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
            "pokeID": "01",
          },
        ],
        [
          "pokePart",
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
        ],
        [
          "pokeEnd",
          {
            "cookie": "01",
            "pokeID": "01",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "lmids": 1,
          },
          "rowKey": {
            "clientGroupID": "9876",
            "clientID": "foo",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "this_app_2.clients",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
          },
          "rowKey": {
            "id": "1",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
          },
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
          },
          "rowKey": {
            "id": "3",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
          },
          "rowKey": {
            "id": "4",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
      ]
    `);
  });

  describe('custom queries', () => {
    const mockFetch = vi.fn() as MockedFunction<typeof fetch>;
    beforeEach(() => {
      vi.stubGlobal('fetch', mockFetch);
    });

    afterEach(() => {
      mockFetch.mockClear();
      vi.unstubAllGlobals();
    });

    function mockFetchImpl(
      queryResponses: TransformResponseBody | (() => Promise<Response>),
    ) {
      mockFetch.mockImplementation(url => {
        if (
          url ===
          'http://my-pull-endpoint.dev/api/zero/pull?schema=this_app_2&appID=this_app'
        ) {
          if (typeof queryResponses === 'function') {
            return queryResponses();
          }
          return Promise.resolve(
            new Response(
              JSON.stringify([
                'transformed',
                queryResponses,
              ] satisfies TransformResponseMessage),
            ),
          );
        }
        return Promise.reject(new Error('Unexpected fetch call ' + url));
      });
    }

    test('initial hydration of a custom query', async () => {
      mockFetchImpl([
        {
          ast: ISSUES_QUERY,
          id: 'custom-1',
          name: 'named-query',
        },
      ]);
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'custom-1', name: 'named-query', args: ['thing']},
      ]);
      expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "custom-1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "00:01",
              "pokeID": "01",
            },
          ],
          [
            "pokePart",
            {
              "gotQueriesPatch": [
                {
                  "hash": "custom-1",
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
          ],
          [
            "pokeEnd",
            {
              "cookie": "01",
              "pokeID": "01",
            },
          ],
        ]
      `);

      expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
        .toMatchInlineSnapshot(`
          Result [
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "lmids": 1,
              },
              "rowKey": {
                "clientGroupID": "9876",
                "clientID": "foo",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "this_app_2.clients",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "1",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "2",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "3",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "4",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
          ]
        `);
    });

    test('removing one of two queries with shared transformation hash does not remove pipeline', async () => {
      mockFetchImpl([
        {
          ast: ISSUES_QUERY,
          id: 'custom-1',
          name: 'named-query-1',
        },
        {
          ast: ISSUES_QUERY,
          id: 'custom-2',
          name: 'named-query-2',
        },
      ]);
      const ttl = 100;
      vi.setSystemTime(Date.now());

      const client = connect(SYNC_CONTEXT, [
        {
          op: 'put',
          hash: 'custom-1',
          name: 'named-query-1',
          args: ['thing'],
          ttl,
        },
        {op: 'put', hash: 'custom-2', name: 'named-query-2', args: ['thing']},
      ]);

      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": null,
              "pokeID": "00:01",
            },
          ],
          [
            "pokePart",
            {
              "desiredQueriesPatches": {
                "foo": [
                  {
                    "hash": "custom-1",
                    "op": "put",
                  },
                  {
                    "hash": "custom-2",
                    "op": "put",
                  },
                ],
              },
              "pokeID": "00:01",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "00:01",
              "pokeID": "00:01",
            },
          ],
        ]
      `);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "00:01",
              "pokeID": "01",
            },
          ],
          [
            "pokePart",
            {
              "gotQueriesPatch": [
                {
                  "hash": "custom-1",
                  "op": "put",
                },
                {
                  "hash": "custom-2",
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
          ],
          [
            "pokeEnd",
            {
              "cookie": "01",
              "pokeID": "01",
            },
          ],
        ]
      `);

      await inactivateQuery(vs, SYNC_CONTEXT, 'custom-1');

      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "01",
              "pokeID": "01:01",
            },
          ],
          [
            "pokePart",
            {
              "desiredQueriesPatches": {
                "foo": [
                  {
                    "hash": "custom-1",
                    "op": "del",
                  },
                ],
              },
              "pokeID": "01:01",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "01:01",
              "pokeID": "01:01",
            },
          ],
        ]
      `);

      // Expire custom-1
      callNextSetTimeout(ttl);

      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "01:01",
              "pokeID": "01:02",
            },
          ],
          [
            "pokePart",
            {
              "gotQueriesPatch": [
                {
                  "hash": "custom-1",
                  "op": "del",
                },
              ],
              "pokeID": "01:02",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "01:02",
              "pokeID": "01:02",
            },
          ],
        ]
      `);

      // Verify custom-2 is still alive by making a data change
      replicator.processTransaction(
        '101',
        messages.delete('issues', {id: '2'}),
      );
      stateChanges.push({state: 'version-ready'});

      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "01:02",
              "pokeID": "101",
            },
          ],
          [
            "pokePart",
            {
              "pokeID": "101",
              "rowsPatch": [
                {
                  "id": {
                    "id": "2",
                  },
                  "op": "del",
                  "tableName": "issues",
                },
              ],
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "101",
              "pokeID": "101",
            },
          ],
        ]
      `);
    });

    test('patching desired only trasnforms added custom queries', async () => {
      // Spy on transformer's transform method instead of mocking fetch
      using transformSpy = vi.spyOn(customQueryTransformer!, 'transform');

      transformSpy.mockResolvedValue([
        {
          id: 'custom-1',
          transformedAst: ISSUES_QUERY,
          transformationHash: 'hash-1',
        },
      ]);

      const client = connect(SYNC_CONTEXT, [
        {
          op: 'put',
          hash: 'custom-1',
          name: 'named-query-1',
          args: ['thing'],
        },
      ]);

      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": null,
              "pokeID": "00:01",
            },
          ],
          [
            "pokePart",
            {
              "desiredQueriesPatches": {
                "foo": [
                  {
                    "hash": "custom-1",
                    "op": "put",
                  },
                ],
              },
              "pokeID": "00:01",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "00:01",
              "pokeID": "00:01",
            },
          ],
        ]
      `);
      stateChanges.push({state: 'version-ready'});
      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "00:01",
              "pokeID": "01",
            },
          ],
          [
            "pokePart",
            {
              "gotQueriesPatch": [
                {
                  "hash": "custom-1",
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
          ],
          [
            "pokeEnd",
            {
              "cookie": "01",
              "pokeID": "01",
            },
          ],
        ]
      `);
      expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
        .toMatchInlineSnapshot(`
          Result [
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "lmids": 1,
              },
              "rowKey": {
                "clientGroupID": "9876",
                "clientID": "foo",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "this_app_2.clients",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "1",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "2",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "3",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "4",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
          ]
        `);

      // First client should have called transform once
      expect(transformSpy).toHaveBeenCalledTimes(1);
      expect(transformSpy.mock.calls[0]).toMatchInlineSnapshot(`
        [
          {
            "allowedClientHeaders": undefined,
            "apiKey": undefined,
            "cookie": undefined,
            "customHeaders": undefined,
            "origin": undefined,
            "token": undefined,
          },
          [
            {
              "args": [
                "thing",
              ],
              "clientState": {
                "foo": {
                  "inactivatedAt": undefined,
                  "ttl": 300000,
                  "version": {
                    "configVersion": 1,
                    "stateVersion": "00",
                  },
                },
              },
              "id": "custom-1",
              "name": "named-query-1",
              "type": "custom",
            },
          ],
          undefined,
        ]
      `);

      transformSpy.mockResolvedValue([
        {
          id: 'custom-2',
          transformedAst: USERS_QUERY,
          transformationHash: 'hash-2',
        },
      ]);

      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {
              op: 'put',
              hash: 'custom-2',
              name: 'named-query-2',
              args: ['thing'],
            },
          ],
        },
      ]);

      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "01",
              "pokeID": "01:01",
            },
          ],
          [
            "pokePart",
            {
              "desiredQueriesPatches": {
                "foo": [
                  {
                    "hash": "custom-2",
                    "op": "put",
                  },
                ],
              },
              "pokeID": "01:01",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "01:01",
              "pokeID": "01:01",
            },
          ],
        ]
      `);
      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "01:01",
              "pokeID": "01:02",
            },
          ],
          [
            "pokePart",
            {
              "gotQueriesPatch": [
                {
                  "hash": "custom-2",
                  "op": "put",
                },
              ],
              "pokeID": "01:02",
              "rowsPatch": [
                {
                  "op": "put",
                  "tableName": "users",
                  "value": {
                    "id": "100",
                    "name": "Alice",
                  },
                },
                {
                  "op": "put",
                  "tableName": "users",
                  "value": {
                    "id": "101",
                    "name": "Bob",
                  },
                },
                {
                  "op": "put",
                  "tableName": "users",
                  "value": {
                    "id": "102",
                    "name": "Candice",
                  },
                },
              ],
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "01:02",
              "pokeID": "01:02",
            },
          ],
        ]
      `);

      expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
        .toMatchInlineSnapshot(`
          Result [
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "lmids": 1,
              },
              "rowKey": {
                "clientGroupID": "9876",
                "clientID": "foo",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "this_app_2.clients",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "1",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "2",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "3",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "4",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01:02",
              "refCounts": {
                "custom-2": 1,
              },
              "rowKey": {
                "id": "100",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "users",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01:02",
              "refCounts": {
                "custom-2": 1,
              },
              "rowKey": {
                "id": "101",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "users",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01:02",
              "refCounts": {
                "custom-2": 1,
              },
              "rowKey": {
                "id": "102",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "users",
            },
          ]
        `);

      expect(transformSpy).toHaveBeenCalledTimes(2);
      // custom-1 is not transformed again
      expect(transformSpy.mock.calls[1]).toMatchInlineSnapshot(`
        [
          {
            "allowedClientHeaders": undefined,
            "apiKey": undefined,
            "cookie": undefined,
            "customHeaders": undefined,
            "origin": undefined,
            "token": undefined,
          },
          [
            {
              "args": [
                "thing",
              ],
              "clientState": {
                "foo": {
                  "inactivatedAt": undefined,
                  "ttl": 300000,
                  "version": {
                    "configVersion": 1,
                    "stateVersion": "01",
                  },
                },
              },
              "id": "custom-2",
              "name": "named-query-2",
              "type": "custom",
            },
          ],
          undefined,
        ]
      `);
    });

    test('different custom queries end up with the same query after transformation', async () => {
      mockFetchImpl([
        {
          ast: ISSUES_QUERY,
          id: 'custom-1',
          name: 'named-query-1',
        },
        {
          ast: ISSUES_QUERY,
          id: 'custom-2',
          name: 'named-query-2',
        },
      ]);
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'custom-1', name: 'named-query-1', args: ['thing']},
        {op: 'put', hash: 'custom-2', name: 'named-query-2', args: ['thing']},
      ]);

      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": null,
              "pokeID": "00:01",
            },
          ],
          [
            "pokePart",
            {
              "desiredQueriesPatches": {
                "foo": [
                  {
                    "hash": "custom-1",
                    "op": "put",
                  },
                  {
                    "hash": "custom-2",
                    "op": "put",
                  },
                ],
              },
              "pokeID": "00:01",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "00:01",
              "pokeID": "00:01",
            },
          ],
        ]
      `);
      stateChanges.push({state: 'version-ready'});
      expect(await nextPoke(client)).toMatchInlineSnapshot(`
          [
            [
              "pokeStart",
              {
                "baseCookie": "00:01",
                "pokeID": "01",
              },
            ],
            [
              "pokePart",
              {
                "gotQueriesPatch": [
                  {
                    "hash": "custom-1",
                    "op": "put",
                  },
                  {
                    "hash": "custom-2",
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
            ],
            [
              "pokeEnd",
              {
                "cookie": "01",
                "pokeID": "01",
              },
            ],
          ]
        `);
      expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
        .toMatchInlineSnapshot(`
          Result [
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "lmids": 1,
              },
              "rowKey": {
                "clientGroupID": "9876",
                "clientID": "foo",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "this_app_2.clients",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
                "custom-2": 1,
              },
              "rowKey": {
                "id": "1",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
                "custom-2": 1,
              },
              "rowKey": {
                "id": "2",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
                "custom-2": 1,
              },
              "rowKey": {
                "id": "3",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
                "custom-2": 1,
              },
              "rowKey": {
                "id": "4",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
          ]
        `);
    });

    test('different custom queries in different put desired queries patches end up with the same query after transformation', async () => {
      mockFetchImpl([
        {
          ast: ISSUES_QUERY,
          id: 'custom-1',
          name: 'named-query-1',
        },
        {
          ast: ISSUES_QUERY,
          id: 'custom-2',
          name: 'named-query-2',
        },
      ]);
      const client = connect(SYNC_CONTEXT, [
        {
          op: 'put',
          hash: 'custom-1',
          name: 'named-query-1',
          args: ['thing'],
        },
      ]);

      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": null,
              "pokeID": "00:01",
            },
          ],
          [
            "pokePart",
            {
              "desiredQueriesPatches": {
                "foo": [
                  {
                    "hash": "custom-1",
                    "op": "put",
                  },
                ],
              },
              "pokeID": "00:01",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "00:01",
              "pokeID": "00:01",
            },
          ],
        ]
      `);
      stateChanges.push({state: 'version-ready'});
      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "00:01",
              "pokeID": "01",
            },
          ],
          [
            "pokePart",
            {
              "gotQueriesPatch": [
                {
                  "hash": "custom-1",
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
          ],
          [
            "pokeEnd",
            {
              "cookie": "01",
              "pokeID": "01",
            },
          ],
        ]
      `);
      expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
        .toMatchInlineSnapshot(`
          Result [
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "lmids": 1,
              },
              "rowKey": {
                "clientGroupID": "9876",
                "clientID": "foo",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "this_app_2.clients",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "1",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "2",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "3",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "4",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
          ]
        `);

      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {
              op: 'put',
              hash: 'custom-2',
              name: 'named-query-2',
              args: ['thing'],
            },
          ],
        },
      ]);

      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "01",
              "pokeID": "01:01",
            },
          ],
          [
            "pokePart",
            {
              "desiredQueriesPatches": {
                "foo": [
                  {
                    "hash": "custom-2",
                    "op": "put",
                  },
                ],
              },
              "pokeID": "01:01",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "01:01",
              "pokeID": "01:01",
            },
          ],
        ]
      `);
      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "01:01",
              "pokeID": "01:02",
            },
          ],
          [
            "pokePart",
            {
              "gotQueriesPatch": [
                {
                  "hash": "custom-2",
                  "op": "put",
                },
              ],
              "pokeID": "01:02",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "01:02",
              "pokeID": "01:02",
            },
          ],
        ]
      `);

      expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
        .toMatchInlineSnapshot(`
          Result [
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "lmids": 1,
              },
              "rowKey": {
                "clientGroupID": "9876",
                "clientID": "foo",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "this_app_2.clients",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
                "custom-2": 1,
              },
              "rowKey": {
                "id": "1",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
                "custom-2": 1,
              },
              "rowKey": {
                "id": "2",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
                "custom-2": 1,
              },
              "rowKey": {
                "id": "3",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
                "custom-2": 1,
              },
              "rowKey": {
                "id": "4",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
          ]
        `);
    });

    test('transforms all custom queries on new connection to validate authorization', async () => {
      // Spy on transformer's transform method instead of mocking fetch
      using transformSpy = vi
        .spyOn(customQueryTransformer!, 'transform')
        .mockResolvedValue([
          {
            id: 'custom-1',
            transformedAst: ISSUES_QUERY,
            transformationHash: 'hash-1',
          },
          {
            id: 'custom-2',
            transformedAst: ISSUES_QUERY,
            transformationHash: 'hash-2',
          },
        ]);

      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'custom-1', name: 'named-query-1', args: ['thing']},
        {op: 'put', hash: 'custom-2', name: 'named-query-2', args: ['thing']},
      ]);

      await nextPoke(client);
      stateChanges.push({state: 'version-ready'});
      await nextPoke(client);

      // First client should have called transform once
      expect(transformSpy).toHaveBeenCalledTimes(1);
      expect(transformSpy.mock.calls[0]).toMatchInlineSnapshot(`
        [
          {
            "allowedClientHeaders": undefined,
            "apiKey": undefined,
            "cookie": undefined,
            "customHeaders": undefined,
            "origin": undefined,
            "token": undefined,
          },
          [
            {
              "args": [
                "thing",
              ],
              "clientState": {
                "foo": {
                  "inactivatedAt": undefined,
                  "ttl": 300000,
                  "version": {
                    "configVersion": 1,
                    "stateVersion": "00",
                  },
                },
              },
              "id": "custom-1",
              "name": "named-query-1",
              "type": "custom",
            },
            {
              "args": [
                "thing",
              ],
              "clientState": {
                "foo": {
                  "inactivatedAt": undefined,
                  "ttl": 300000,
                  "version": {
                    "configVersion": 1,
                    "stateVersion": "00",
                  },
                },
              },
              "id": "custom-2",
              "name": "named-query-2",
              "type": "custom",
            },
          ],
          undefined,
        ]
      `);

      // Create second client with same queries
      const client2 = connect(
        {
          ...SYNC_CONTEXT,
          clientID: 'cq-c2-client',
          wsID: 'cq-c2-wsid',
        },
        [
          {op: 'put', hash: 'custom-1', name: 'named-query-1', args: ['thing']},
          {op: 'put', hash: 'custom-2', name: 'named-query-2', args: ['thing']},
        ],
      );

      // query should still transition to `got`
      expect(await nextPoke(client2)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": null,
              "pokeID": "01:01",
            },
          ],
          [
            "pokePart",
            {
              "desiredQueriesPatches": {
                "cq-c2-client": [
                  {
                    "hash": "custom-1",
                    "op": "put",
                  },
                  {
                    "hash": "custom-2",
                    "op": "put",
                  },
                ],
                "foo": [
                  {
                    "hash": "custom-1",
                    "op": "put",
                  },
                  {
                    "hash": "custom-2",
                    "op": "put",
                  },
                ],
              },
              "gotQueriesPatch": [
                {
                  "hash": "custom-1",
                  "op": "put",
                },
                {
                  "hash": "custom-2",
                  "op": "put",
                },
              ],
              "lastMutationIDChanges": {
                "foo": 42,
              },
              "pokeID": "01:01",
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
          ],
          [
            "pokeEnd",
            {
              "cookie": "01:01",
              "pokeID": "01:01",
            },
          ],
        ]
      `);
      // Transform is called twice:
      // 1. First client connection triggers initial transform
      // 2. Second client connection triggers transform again of all queries (separate validation)
      expect(transformSpy).toHaveBeenCalledTimes(2);
      expect(transformSpy.mock.calls[1]).toMatchInlineSnapshot(`
        [
          {
            "allowedClientHeaders": undefined,
            "apiKey": undefined,
            "cookie": undefined,
            "customHeaders": undefined,
            "origin": undefined,
            "token": undefined,
          },
          [
            {
              "args": [
                "thing",
              ],
              "clientState": {
                "cq-c2-client": {
                  "inactivatedAt": undefined,
                  "ttl": 300000,
                  "version": {
                    "configVersion": 1,
                    "stateVersion": "01",
                  },
                },
                "foo": {
                  "inactivatedAt": undefined,
                  "ttl": 300000,
                  "version": {
                    "configVersion": 1,
                    "stateVersion": "00",
                  },
                },
              },
              "id": "custom-1",
              "name": "named-query-1",
              "patchVersion": {
                "stateVersion": "01",
              },
              "transformationHash": "hash-1",
              "transformationVersion": {
                "stateVersion": "01",
              },
              "type": "custom",
            },
            {
              "args": [
                "thing",
              ],
              "clientState": {
                "cq-c2-client": {
                  "inactivatedAt": undefined,
                  "ttl": 300000,
                  "version": {
                    "configVersion": 1,
                    "stateVersion": "01",
                  },
                },
                "foo": {
                  "inactivatedAt": undefined,
                  "ttl": 300000,
                  "version": {
                    "configVersion": 1,
                    "stateVersion": "00",
                  },
                },
              },
              "id": "custom-2",
              "name": "named-query-2",
              "patchVersion": {
                "stateVersion": "01",
              },
              "transformationHash": "hash-2",
              "transformationVersion": {
                "stateVersion": "01",
              },
              "type": "custom",
            },
          ],
          undefined,
        ]
      `);
    });

    // test cases where custom query transforms fail
    test('http transform call fails', async () => {
      mockFetchImpl(() =>
        Promise.reject(
          new ProtocolError({
            kind: ErrorKind.TransformFailed,
            message: 'Fetch from API server returned non-OK status 500',
            origin: ErrorOrigin.ZeroCache,
            queryIDs: ['custom-1', 'custom-2'],
            reason: ErrorReason.HTTP,
            status: 500,
            bodyPreview: '{ "error": "Internal Server Error" }',
          }),
        ),
      );
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'custom-1', name: 'named-query-1', args: ['thing']},
        {op: 'put', hash: 'custom-2', name: 'named-query-2', args: ['thing']},
      ]);

      await nextPoke(client);
      stateChanges.push({state: 'version-ready'});
      await expect(nextPoke(client)).rejects.toMatchInlineSnapshot(
        `[ProtocolError: Fetch from API server returned non-OK status 500]`,
      );
    });

    test('bad http response', async () => {
      const r = new Response(JSON.stringify({}), {
        status: 500,
        statusText: 'Internal Server Error',
      });
      mockFetchImpl(() => Promise.resolve(r));
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'custom-1', name: 'named-query-1', args: ['thing']},
        {op: 'put', hash: 'custom-2', name: 'named-query-2', args: ['thing']},
      ]);

      await nextPoke(client);
      stateChanges.push({state: 'version-ready'});
      await expect(nextPoke(client)).rejects.toMatchInlineSnapshot(
        `[ProtocolError: Fetch from API server returned non-OK status 500]`,
      );
    });

    test('all individual queries fail', async () => {
      mockFetchImpl([
        {
          error: 'app',
          id: 'custom-1',
          name: 'named-query-1',
          message: 'errrrrr',
        },
        {
          error: 'app',
          id: 'custom-2',
          name: 'named-query-2',
          message: 'brrrr',
          details: {reason: 'somereason'},
        },
        {
          error: 'parse',
          id: 'custom-3',
          name: 'named-query-3',
          message: 'Could not parse parameters',
          details: {reason: 'Invalid syntax'},
        },
      ] satisfies TransformResponseBody);
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'custom-1', name: 'named-query-1', args: ['thing']},
        {op: 'put', hash: 'custom-2', name: 'named-query-2', args: ['thing']},
        {op: 'put', hash: 'custom-3', name: 'named-query-3', args: ['thing']},
      ]);

      await nextPoke(client);
      stateChanges.push({state: 'version-ready'});
      expect(await nextPoke(client)).toMatchInlineSnapshot(
        `
        [
          [
            "transformError",
            [
              {
                "error": "app",
                "id": "custom-1",
                "message": "errrrrr",
                "name": "named-query-1",
              },
              {
                "details": {
                  "reason": "somereason",
                },
                "error": "app",
                "id": "custom-2",
                "message": "brrrr",
                "name": "named-query-2",
              },
              {
                "details": {
                  "reason": "Invalid syntax",
                },
                "error": "parse",
                "id": "custom-3",
                "message": "Could not parse parameters",
                "name": "named-query-3",
              },
            ],
          ],
          [
            "pokeStart",
            {
              "baseCookie": "00:01",
              "pokeID": "01",
            },
          ],
          [
            "pokePart",
            {
              "gotQueriesPatch": [
                {
                  "hash": "custom-1",
                  "op": "del",
                },
                {
                  "hash": "custom-2",
                  "op": "del",
                },
                {
                  "hash": "custom-3",
                  "op": "del",
                },
              ],
              "lastMutationIDChanges": {
                "foo": 42,
              },
              "pokeID": "01",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "01",
              "pokeID": "01",
            },
          ],
        ]
      `,
      );
    });

    test('some individual queries fail', async () => {
      mockFetchImpl([
        {
          error: 'app',
          id: 'custom-1',
          name: 'named-query-1',
          message: 'errrrrr',
        },
        {
          id: 'custom-2',
          name: 'named-query-2',
          ast: USERS_QUERY,
        },
      ] satisfies TransformResponseBody);
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'custom-1', name: 'named-query-1', args: ['thing']},
        {op: 'put', hash: 'custom-2', name: 'named-query-2', args: ['thing']},
      ]);

      await nextPoke(client);
      stateChanges.push({state: 'version-ready'});
      expect(await nextPoke(client)).toMatchInlineSnapshot(
        `
        [
          [
            "transformError",
            [
              {
                "error": "app",
                "id": "custom-1",
                "message": "errrrrr",
                "name": "named-query-1",
              },
            ],
          ],
          [
            "pokeStart",
            {
              "baseCookie": "00:01",
              "pokeID": "01",
            },
          ],
          [
            "pokePart",
            {
              "gotQueriesPatch": [
                {
                  "hash": "custom-2",
                  "op": "put",
                },
                {
                  "hash": "custom-1",
                  "op": "del",
                },
              ],
              "lastMutationIDChanges": {
                "foo": 42,
              },
              "pokeID": "01",
              "rowsPatch": [
                {
                  "op": "put",
                  "tableName": "users",
                  "value": {
                    "id": "100",
                    "name": "Alice",
                  },
                },
                {
                  "op": "put",
                  "tableName": "users",
                  "value": {
                    "id": "101",
                    "name": "Bob",
                  },
                },
                {
                  "op": "put",
                  "tableName": "users",
                  "value": {
                    "id": "102",
                    "name": "Candice",
                  },
                },
              ],
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "01",
              "pokeID": "01",
            },
          ],
        ]
      `,
      );
    });

    describe.each([
      {
        name: 'different hashes - removes old pipeline',
        hash1: 'hash-1',
        hash2: 'hash-2',
        ast1: ISSUES_QUERY,
        ast2: USERS_QUERY,
        expectSameHash: false,
        expectRowsPresent: false,
      },
      {
        name: 'same hash - keeps pipeline for successful query',
        hash1: 'hash-same',
        hash2: 'hash-same',
        ast1: ISSUES_QUERY,
        ast2: ISSUES_QUERY,
        expectSameHash: true,
        expectRowsPresent: true,
      },
    ])(
      'failed query re-transformation: $name',
      ({hash1, hash2, ast1, ast2, expectSameHash, expectRowsPresent}) => {
        test('removes failed query using last known transformation hash', async () => {
          // Use spy pattern to control mock behavior between connections
          using transformSpy = vi
            .spyOn(customQueryTransformer!, 'transform')
            .mockResolvedValueOnce([
              {
                id: 'custom-1',
                transformedAst: ast1,
                transformationHash: hash1,
              },
              {
                id: 'custom-2',
                transformedAst: ast2,
                transformationHash: hash2,
              },
            ]);

          const client1 = connect(SYNC_CONTEXT, [
            {
              op: 'put',
              hash: 'custom-1',
              name: 'named-query-1',
              args: ['thing'],
            },
            {
              op: 'put',
              hash: 'custom-2',
              name: 'named-query-2',
              args: ['thing'],
            },
          ]);

          // Initial config poke
          await nextPoke(client1);

          // Trigger hydration
          stateChanges.push({state: 'version-ready'});
          const hydrateResponse = await nextPoke(client1);

          // Verify first transformation was called
          expect(transformSpy).toHaveBeenCalledTimes(1);

          if (expectRowsPresent) {
            // Verify rows were hydrated (pipeline was created)
            const pokePart = hydrateResponse.find(
              ([cmd]) => cmd === 'pokePart',
            ) as [string, PokePartBody];
            expect(pokePart).toBeTruthy();
            const rowsPatch = pokePart[1].rowsPatch;
            expect(rowsPatch).toBeTruthy();
            expect(rowsPatch!.length).toBeGreaterThan(0);
          }

          // Verify both queries are in CVR with transformation hashes
          const queriesAfterFirstConnect =
            await cvrDB`SELECT "queryHash", "transformationHash" FROM "this_app_2/cvr".queries WHERE "transformationHash" IS NOT NULL ORDER BY "queryHash"`;
          // Should have custom-1, custom-2, and possibly internal queries
          expect(queriesAfterFirstConnect.length).toBeGreaterThanOrEqual(2);

          const custom1Query = queriesAfterFirstConnect.find(
            q => q.queryHash === 'custom-1',
          );
          const custom2Query = queriesAfterFirstConnect.find(
            q => q.queryHash === 'custom-2',
          );

          expect(custom1Query).toBeTruthy();
          expect(custom1Query!.transformationHash).toBe(hash1);
          expect(custom2Query).toBeTruthy();
          expect(custom2Query!.transformationHash).toBe(hash2);

          if (expectSameHash) {
            // Verify both queries share the same transformation hash
            expect(custom1Query!.transformationHash).toBe(
              custom2Query!.transformationHash,
            );
          }

          // Second connection - custom-1 fails transformation, custom-2 succeeds
          transformSpy.mockResolvedValueOnce([
            {
              error: 'app',
              id: 'custom-1',
              name: 'named-query-1',
              message: 'Authorization failed',
            },
            {
              id: 'custom-2',
              transformedAst: ast2,
              transformationHash: hash2,
            },
          ]);

          const client2 = connect(
            {...SYNC_CONTEXT, clientID: 'bar', wsID: 'ws2'},
            [
              {
                op: 'put',
                hash: 'custom-1',
                name: 'named-query-1',
                args: ['thing'],
              },
              {
                op: 'put',
                hash: 'custom-2',
                name: 'named-query-2',
                args: ['thing'],
              },
            ],
          );

          // Get the response - on reconnection, the query removal happens via poke
          const response = await nextPoke(client2);

          // Verify second transformation was called
          expect(transformSpy).toHaveBeenCalledTimes(2);

          // Verify custom-1 was removed from gotQueries
          const pokePart = response.find(([cmd]) => cmd === 'pokePart') as [
            string,
            PokePartBody,
          ];
          expect(pokePart).toBeTruthy();
          const gotQueriesPatch = pokePart[1].gotQueriesPatch;
          expect(gotQueriesPatch).toContainEqual({hash: 'custom-1', op: 'del'});

          if (expectRowsPresent) {
            // Verify rows are still present (pipeline not removed because custom-2 uses same hash)
            const rowsPatchAfterFailure = pokePart[1].rowsPatch;
            expect(rowsPatchAfterFailure).toBeTruthy();
            expect(rowsPatchAfterFailure!.length).toBeGreaterThan(0);

            // Verify NO deletions for issues table (pipeline shared, still active)
            const issueDelOps = rowsPatchAfterFailure!.filter(
              op => op.op === 'del' && op.tableName === 'issues',
            );
            expect(issueDelOps.length).toBe(0);

            // Verify issues rows are still present (put operations)
            const issuePutOps = rowsPatchAfterFailure!.filter(
              op => op.op === 'put' && op.tableName === 'issues',
            );
            expect(issuePutOps.length).toBeGreaterThan(0);
          } else {
            // Verify custom-1 was removed from CVR (by checking it's marked as deleted or removed)
            const queriesAfterFailure =
              await cvrDB`SELECT "queryHash", "transformationHash" FROM "this_app_2/cvr".queries WHERE "queryHash" = 'custom-1'`;
            // Query should either be deleted or removed from the active set
            expect(
              queriesAfterFailure.length === 0 ||
                queriesAfterFailure[0].transformationHash === null,
            ).toBe(true);

            // Verify rowsPatch contains deletions for issues table (hash-1 pipeline removed)
            const rowsPatchAfterFailure = pokePart[1].rowsPatch;
            expect(rowsPatchAfterFailure).toBeTruthy();

            // Should have deletion operations for all 4 issues rows (ids: '1', '2', '3', '4')
            const issueDelOps = rowsPatchAfterFailure!.filter(
              op => op.op === 'del' && op.tableName === 'issues',
            );
            expect(issueDelOps.length).toBe(4);

            // Verify all issue IDs are deleted
            const deletedIssueIds = issueDelOps
              .map(
                op =>
                  (op as {op: 'del'; tableName: string; id: {id: string}}).id
                    .id,
              )
              .sort();
            expect(deletedIssueIds).toEqual(['1', '2', '3', '4']);

            // Verify no deletions for users table (hash-2 pipeline still active)
            const userDelOps = rowsPatchAfterFailure!.filter(
              op => op.op === 'del' && op.tableName === 'users',
            );
            expect(userDelOps.length).toBe(0);
          }

          // Verify custom-2 is still present/active
          if (expectRowsPresent) {
            expect(gotQueriesPatch).toContainEqual({
              hash: 'custom-2',
              op: 'put',
            });
          } else {
            const custom2QueryAfterFailure =
              await cvrDB`SELECT "queryHash", "transformationHash" FROM "this_app_2/cvr".queries WHERE "queryHash" = 'custom-2' AND "transformationHash" IS NOT NULL`;
            expect(custom2QueryAfterFailure).toHaveLength(1);
          }
        });
      },
    );

    // not yet supported: test('a single custom query that returns many queries' () => {});
  });

  test('delete client', async () => {
    const ttl = 5000; // 5s
    vi.setSystemTime(Date.UTC(2025, 2, 4));

    const {queue: client1} = connectWithQueueAndSource(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
    ]);

    const {queue: client2, source: connectSource2} = connectWithQueueAndSource(
      {...SYNC_CONTEXT, clientID: 'bar', wsID: 'ws2'},
      [{op: 'put', hash: 'query-hash2', ast: USERS_QUERY, ttl}],
    );

    await nextPoke(client1);
    await nextPoke(client2);

    stateChanges.push({state: 'version-ready'});

    await nextPoke(client1);
    await nextPoke(client1);

    await nextPoke(client2);
    await nextPoke(client2);

    expect(
      await cvrDB`SELECT "clientID" from "this_app_2/cvr".clients`,
    ).toMatchInlineSnapshot(
      `
      Result [
        {
          "clientID": "foo",
        },
        {
          "clientID": "bar",
        },
      ]
    `,
    );

    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "ttl", "inactivatedAt" from "this_app_2/cvr".desires`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "foo",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hash1",
          "ttl": "00:00:05",
        },
        {
          "clientID": "bar",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hash2",
          "ttl": "00:00:05",
        },
      ]
    `);

    connectSource2.cancel();

    await vs.deleteClients(SYNC_CONTEXT, [
      'deleteClients',
      {clientIDs: ['bar', 'no-such-client']},
    ]);

    expect(await nextPokeParts(client1)).toMatchInlineSnapshot(`
      [
        {
          "desiredQueriesPatches": {
            "bar": [
              {
                "hash": "query-hash2",
                "op": "del",
              },
            ],
          },
          "pokeID": "01:01",
        },
      ]
    `);

    expect(await client1.dequeue()).toMatchInlineSnapshot(`
      [
        "deleteClients",
        {
          "clientIDs": [
            "bar",
            "no-such-client",
          ],
        },
      ]
    `);

    await expectNoPokes(client1);

    expect(
      await cvrDB`SELECT "clientID" from "this_app_2/cvr".clients`,
    ).toMatchInlineSnapshot(
      `
      Result [
        {
          "clientID": "foo",
        },
      ]
    `,
    );

    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "ttl", "inactivatedAt" from "this_app_2/cvr".desires`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "foo",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hash1",
          "ttl": "00:00:05",
        },
        {
          "clientID": "bar",
          "deleted": true,
          "inactivatedAt": 0,
          "queryHash": "query-hash2",
          "ttl": "00:00:05",
        },
      ]
    `);

    callNextSetTimeout(ttl);

    expect(await nextPokeParts(client1)).toMatchInlineSnapshot(`
      [
        {
          "gotQueriesPatch": [
            {
              "hash": "query-hash2",
              "op": "del",
            },
          ],
          "pokeID": "01:02",
          "rowsPatch": [
            {
              "id": {
                "id": "100",
              },
              "op": "del",
              "tableName": "users",
            },
            {
              "id": {
                "id": "101",
              },
              "op": "del",
              "tableName": "users",
            },
            {
              "id": {
                "id": "102",
              },
              "op": "del",
              "tableName": "users",
            },
          ],
        },
      ]
    `);

    await expectNoPokes(client1);

    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "ttl", "inactivatedAt" from "this_app_2/cvr".desires`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "foo",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hash1",
          "ttl": "00:00:05",
        },
        {
          "clientID": "bar",
          "deleted": true,
          "inactivatedAt": 0,
          "queryHash": "query-hash2",
          "ttl": "00:00:05",
        },
      ]
    `);
  });

  test('activeClients inactivates queries from inactive clients', async () => {
    const ttl = 5000; // 5s
    vi.setSystemTime(Date.UTC(2025, 5, 30));

    // First, connect client A with queries
    const ctxA = {...SYNC_CONTEXT, clientID: 'clientA', wsID: 'wsA'};
    const {source: streamA, queue: clientA} = connectWithQueueAndSource(ctxA, [
      {op: 'put', hash: 'query-hashA', ast: ISSUES_QUERY, ttl},
    ]);

    stateChanges.push({state: 'version-ready'});

    await nextPoke(clientA); // desire query-hashA
    await nextPoke(clientA); // Got query-hashA and rows

    // Now connect client B and C using initConnection
    const ctxB = {...SYNC_CONTEXT, clientID: 'clientB', wsID: 'wsB'};
    const ctxC = {...SYNC_CONTEXT, clientID: 'clientC', wsID: 'wsC'};

    // Connect client B
    const {source: streamB, queue: clientB} = connectWithQueueAndSource(ctxB, [
      {op: 'put', hash: 'query-hashB', ast: USERS_QUERY, ttl},
    ]);

    // Connect client C
    const {source: streamC, queue: clientC} = connectWithQueueAndSource(ctxC, [
      {op: 'put', hash: 'query-hashC', ast: COMMENTS_QUERY, ttl},
    ]);

    await nextPoke(clientA); // desire query-hashB
    await nextPoke(clientA); // Got query-hashB and rows

    await nextPoke(clientA); // desire query-hashC
    await nextPoke(clientA); // Got query-hashC
    await expectNoPokes(clientA);

    await nextPoke(clientB); // Desire and got A & B and rows
    await nextPoke(clientB); // desire query-hashC
    await nextPoke(clientB); // Got query-hashC
    await expectNoPokes(clientB);

    await nextPoke(clientC); // Desire and got A & B and rows
    await nextPoke(clientC); // desire query-hashC
    await nextPoke(clientC); // Got query-hashC
    await expectNoPokes(clientC);

    // Verify all three clients are active and have their queries
    expect(
      await cvrDB`SELECT "clientID" from "this_app_2/cvr".clients ORDER BY "clientID"`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "clientA",
        },
        {
          "clientID": "clientB",
        },
        {
          "clientID": "clientC",
        },
      ]
    `);

    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "inactivatedAt" from "this_app_2/cvr".desires ORDER BY "clientID"`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "clientA",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashA",
        },
        {
          "clientID": "clientB",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashB",
        },
        {
          "clientID": "clientC",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashC",
        },
      ]
    `);

    // Close client A & C
    streamA.cancel();
    streamC.cancel();

    await expectNoPokes(clientA);
    await expectNoPokes(clientB);
    await expectNoPokes(clientC);

    // Verify that the clients' queries are NOT inactivated
    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "inactivatedAt"
        FROM "this_app_2/cvr".desires
        ORDER BY "clientID"`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "clientA",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashA",
        },
        {
          "clientID": "clientB",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashB",
        },
        {
          "clientID": "clientC",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashC",
        },
      ]
    `);

    // Simulate the passage of time.
    const ONE_HOUR = 60 * 60 * 1000;
    vi.setSystemTime(Date.now() + ONE_HOUR);

    // Now reconnect client A with activeClients [clientA, clientB]
    // This should inactivate clientC's queries
    const newCtxA = {...ctxA, baseCookie: '01:04', wsID: 'wsA2'};
    const {source: newStreamA, queue: newClientA} = connectWithQueueAndSource(
      newCtxA,
      [{op: 'put', hash: 'query-hashA', ast: ISSUES_QUERY, ttl}],
      undefined,
      ['clientA', 'clientB'],
    );

    expect(await nextPokeParts(newClientA)).toMatchInlineSnapshot(`
      [
        {
          "desiredQueriesPatches": {
            "clientC": [
              {
                "hash": "query-hashC",
                "op": "del",
              },
            ],
          },
          "pokeID": "01:05",
        },
      ]
    `);

    await expectNoPokes(newClientA);

    await nextPoke(clientB); // desire delete query-hashC
    await expectNoPokes(clientB);

    // Verify that clientC's query remains present but is inactivated.
    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "inactivatedAt" FROM "this_app_2/cvr".desires`,
    ).toEqual([
      {
        clientID: 'clientA',
        deleted: false,
        inactivatedAt: null,
        queryHash: 'query-hashA',
      },
      {
        clientID: 'clientB',
        deleted: false,
        inactivatedAt: null,
        queryHash: 'query-hashB',
      },
      {
        clientID: 'clientC',
        deleted: true,
        // inactivatedAt is stored as TIMESTAMPTZ in seconds (raw SELECT returns seconds)
        inactivatedAt: 60 * 60,
        queryHash: 'query-hashC',
      },
    ]);

    // If we move time forward 5s the inactivated query should be deleted
    callNextSetTimeout(ttl);

    expect(await nextPokeParts(newClientA)).toMatchInlineSnapshot(`
      [
        {
          "gotQueriesPatch": [
            {
              "hash": "query-hashC",
              "op": "del",
            },
          ],
          "pokeID": "01:06",
          "rowsPatch": [
            {
              "id": {
                "id": "1",
              },
              "op": "del",
              "tableName": "comments",
            },
            {
              "id": {
                "id": "2",
              },
              "op": "del",
              "tableName": "comments",
            },
          ],
        },
      ]
    `);
    expect(await nextPokeParts(clientB)).toMatchInlineSnapshot(`
      [
        {
          "gotQueriesPatch": [
            {
              "hash": "query-hashC",
              "op": "del",
            },
          ],
          "pokeID": "01:06",
          "rowsPatch": [
            {
              "id": {
                "id": "1",
              },
              "op": "del",
              "tableName": "comments",
            },
            {
              "id": {
                "id": "2",
              },
              "op": "del",
              "tableName": "comments",
            },
          ],
        },
      ]
    `);

    await expectNoPokes(newClientA);
    await expectNoPokes(clientB);

    // Clean up the streams
    newStreamA.cancel();
    streamB.cancel();

    await expectNoPokes(newClientA);
    await expectNoPokes(clientB);
  });

  test('initial hydration, rows in multiple queries', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      // Test multiple queries that normalize to the same hash.
      {op: 'put', hash: 'query-hash1.1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY2},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
                {
                  "hash": "query-hash1.1",
                  "op": "put",
                },
                {
                  "hash": "query-hash2",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
            "pokeID": "01",
          },
        ],
        [
          "pokePart",
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "put",
              },
              {
                "hash": "query-hash1.1",
                "op": "put",
              },
              {
                "hash": "query-hash2",
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
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "5",
                  "json": [
                    123,
                    {
                      "bar": 789,
                      "foo": 456,
                    },
                    "baz",
                  ],
                  "owner": "101",
                  "parent": "2",
                  "title": "not matched",
                },
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01",
            "pokeID": "01",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
      .toMatchInlineSnapshot(`
        Result [
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "lmids": 1,
            },
            "rowKey": {
              "clientGroupID": "9876",
              "clientID": "foo",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "this_app_2.clients",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "query-hash1": 1,
              "query-hash1.1": 1,
              "query-hash2": 1,
            },
            "rowKey": {
              "id": "1",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "query-hash1": 1,
              "query-hash1.1": 1,
              "query-hash2": 1,
            },
            "rowKey": {
              "id": "2",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "query-hash1": 1,
              "query-hash1.1": 1,
              "query-hash2": 1,
            },
            "rowKey": {
              "id": "3",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "query-hash1": 1,
              "query-hash1.1": 1,
              "query-hash2": 1,
            },
            "rowKey": {
              "id": "4",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "query-hash2": 1,
            },
            "rowKey": {
              "id": "5",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
        ]
      `);
  });

  test('process advancements', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY2},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
                {
                  "hash": "query-hash2",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect((await nextPoke(client))[0]).toMatchInlineSnapshot(`
      [
        "pokeStart",
        {
          "baseCookie": "00:01",
          "pokeID": "01",
        },
      ]
    `);

    // Perform an unrelated transaction that does not affect any queries.
    // This should not result in a poke.
    replicator.processTransaction(
      '101',
      messages.insert('users', {
        id: '103',
        name: 'Dude',
      }),
    );
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    // Then, a relevant change should bump the client from '01' directly to '123'.
    replicator.processTransaction(
      '123',
      messages.update('issues', {
        id: '1',
        title: 'new title',
        owner: 100,
        parent: null,
        big: 9007199254740991n,
      }),
      messages.delete('issues', {id: '2'}),
    );

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "123",
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "123",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123",
            "pokeID": "123",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
      .toMatchInlineSnapshot(`
        Result [
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "lmids": 1,
            },
            "rowKey": {
              "clientGroupID": "9876",
              "clientID": "foo",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "this_app_2.clients",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "query-hash1": 1,
              "query-hash2": 1,
            },
            "rowKey": {
              "id": "3",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "query-hash1": 1,
              "query-hash2": 1,
            },
            "rowKey": {
              "id": "4",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "query-hash2": 1,
            },
            "rowKey": {
              "id": "5",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "123",
            "refCounts": {
              "query-hash1": 1,
              "query-hash2": 1,
            },
            "rowKey": {
              "id": "1",
            },
            "rowVersion": "123",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "123",
            "refCounts": null,
            "rowKey": {
              "id": "2",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
        ]
      `);

    replicator.processTransaction('124', messages.truncate('issues'));

    stateChanges.push({state: 'version-ready'});

    // Then a poke that deletes issues rows in the CVR.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "123",
            "pokeID": "124",
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "124",
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
              {
                "id": {
                  "id": "5",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "124",
            "pokeID": "124",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
      .toMatchInlineSnapshot(`
        Result [
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "lmids": 1,
            },
            "rowKey": {
              "clientGroupID": "9876",
              "clientID": "foo",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "this_app_2.clients",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "123",
            "refCounts": null,
            "rowKey": {
              "id": "2",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "124",
            "refCounts": null,
            "rowKey": {
              "id": "1",
            },
            "rowVersion": "123",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "124",
            "refCounts": null,
            "rowKey": {
              "id": "3",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "124",
            "refCounts": null,
            "rowKey": {
              "id": "4",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "124",
            "refCounts": null,
            "rowKey": {
              "id": "5",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
        ]
      `);
  });

  test('process advancement with lmid change, client has no queries.  See https://bugs.rocicorp.dev/issue/3628', async () => {
    const client = connect(SYNC_CONTEXT, []);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
            "pokeID": "01",
          },
        ],
        [
          "pokePart",
          {
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01",
            "pokeID": "01",
          },
        ],
      ]
    `);

    replicator.processTransaction(
      '02',
      app2Messages.update('clients', {
        clientGroupID: serviceID,
        clientID: SYNC_CONTEXT.clientID,
        userID: null,
        lastMutationID: 43,
      }),
    );
    stateChanges.push({state: 'version-ready'});

    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "02",
          },
        ],
        [
          "pokePart",
          {
            "lastMutationIDChanges": {
              "foo": 43,
            },
            "pokeID": "02",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "02",
            "pokeID": "02",
          },
        ],
      ]
    `);
  });

  test('catchup client', async () => {
    const client1 = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
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
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    const preAdvancement = (await nextPoke(client1))[2][1] as PokeEndBody;
    expect(preAdvancement).toEqual({
      cookie: '01',
      pokeID: '01',
    });

    replicator.processTransaction(
      '123',
      messages.update('issues', {
        id: '1',
        title: 'new title',
        owner: 100,
        parent: null,
        big: 9007199254740991n,
      }),
      messages.delete('issues', {id: '2'}),
    );

    stateChanges.push({state: 'version-ready'});
    const advancement = (await nextPoke(client1))[1][1] as PokePartBody;
    expect(advancement).toEqual({
      rowsPatch: [
        {
          tableName: 'issues',
          op: 'put',
          value: {
            big: 9007199254740991,
            id: '1',
            owner: '100.0',
            parent: null,
            title: 'new title',
            json: null,
          },
        },
        {
          id: {id: '2'},
          tableName: 'issues',
          op: 'del',
        },
      ],
      pokeID: '123',
    });

    // Connect with another client (i.e. tab) at older version '00:02'
    // (i.e. pre-advancement).
    const client2 = connect(
      {
        clientID: 'bar',
        profileID: 'p0000g00000003203',
        wsID: '9382',
        baseCookie: preAdvancement.cookie,
        protocolVersion: PROTOCOL_VERSION,
        tokenData: undefined,
        httpCookie: undefined,
        origin: undefined,
      },
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
    );

    // Response should catch client2 up with the rowsPatch from
    // the advancement.
    const response2 = await nextPoke(client2);
    expect(response2[1][1]).toMatchObject({
      ...advancement,
      pokeID: '123:01',
    });
    expect(response2).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "123:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "bar": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "123:01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123:01",
            "pokeID": "123:01",
          },
        ],
      ]
    `);

    // client1 should be poked to get the new client2 config,
    // but no new entities.
    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "123",
            "pokeID": "123:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "bar": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "123:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123:01",
            "pokeID": "123:01",
          },
        ],
      ]
    `);
  });

  test('catchup new client before advancement', async () => {
    const client1 = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    await nextPoke(client1);

    stateChanges.push({state: 'version-ready'});
    const preAdvancement = (await nextPoke(client1))[0][1] as PokeStartBody;
    expect(preAdvancement).toEqual({
      baseCookie: '00:01',
      pokeID: '01',
    });

    replicator.processTransaction(
      '123',
      messages.update('issues', {
        id: '1',
        title: 'new title',
        owner: 100,
        parent: null,
        big: 9007199254740991n,
      }),
      messages.delete('issues', {id: '2'}),
    );

    stateChanges.push({state: 'version-ready'});

    // Connect a second client right as the advancement is about to be processed.
    await sleep(0.5);
    const client2 = connect({...SYNC_CONTEXT, clientID: 'bar'}, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    // Response should catch client2 from scratch.
    expect(await nextPoke(client2)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "123:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "bar": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "put",
              },
            ],
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "123:01",
            "rowsPatch": [
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
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123:01",
            "pokeID": "123:01",
          },
        ],
      ]
    `);
  });

  test('waits for replica to catchup', async () => {
    // Before connecting, artificially set the CVR version to '07',
    // which is ahead of the current replica version '01'.
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, now),
      '07',
      REPLICA_VERSION,
    ).flush(lc, now, now, ttlClock);

    // Connect the client.
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    // Signal that the replica is ready.
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    // Manually simulate advancements in the replica.
    const db = new StatementRunner(replica);
    replica.prepare(`DELETE from issues where id = '1'`).run();
    updateReplicationWatermark(db, '03');
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    replica.prepare(`DELETE from issues where id = '2'`).run();
    updateReplicationWatermark(db, '05');
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    replica.prepare(`DELETE from issues where id = '3'`).run();
    updateReplicationWatermark(db, '06');
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    replica
      .prepare(`UPDATE issues SET title = 'caught up' where id = '4'`)
      .run();
    updateReplicationWatermark(db, '07'); // Caught up with stateVersion=07, watermark=09.
    stateChanges.push({state: 'version-ready'});

    // The single poke should only contain issues {id='4', title='caught up'}
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "07:02",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "put",
              },
            ],
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "07:02",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "caught up",
                },
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "07:02",
            "pokeID": "07:02",
          },
        ],
      ]
    `);
  });

  test('sends reset for CVR from older replica version up', async () => {
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, now),
      '07',
      '1' + REPLICA_VERSION, // CVR is at a newer replica version.
    ).flush(lc, now, now, ttlClock);

    // Connect the client.
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    // Signal that the replica is ready.
    stateChanges.push({state: 'version-ready'});

    let result;
    try {
      result = await client.dequeue();
    } catch (e) {
      result = e;
    }
    expect(result).toBeInstanceOf(ProtocolError);
    expect((result as ProtocolError).errorBody).toEqual({
      kind: ErrorKind.ClientNotFound,
      message: 'Cannot sync from older replica: CVR=101, DB=01',
      origin: ErrorOrigin.ZeroCache,
    } satisfies ErrorBody);
  });

  test('sends client not found if CVR is not found', async () => {
    // Connect the client at a non-empty base cookie.
    const client = connect({...SYNC_CONTEXT, baseCookie: '00:02'}, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    let result;
    try {
      result = await client.dequeue();
    } catch (e) {
      result = e;
    }
    expect(result).toBeInstanceOf(ProtocolError);
    expect((result as ProtocolError).errorBody).toEqual({
      kind: ErrorKind.ClientNotFound,
      message: 'Client not found',
      origin: ErrorOrigin.ZeroCache,
    } satisfies ErrorBody);
  });

  test('initial CVR ownership takeover', async () => {
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      'some-other-task-id',
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    const otherTaskOwnershipTime = now - 600_000;
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, otherTaskOwnershipTime),
      '07',
      REPLICA_VERSION, // CVR is at a newer replica version.
    ).flush(lc, otherTaskOwnershipTime, now, ttlClock);

    expect(await getCVROwner()).toBe('some-other-task-id');

    // Mark as initialized since this test doesn't call initConnection().
    vs.markInitialized();

    // Signal that the replica is ready before any connection
    // message is received.
    stateChanges.push({state: 'version-ready'});

    // Wait for the fire-and-forget takeover to happen.
    await sleep(1000);
    expect(await getCVROwner()).toBe(TASK_ID);
  });

  test('deleteClients before init connection initiates takeover', async () => {
    // First simulate a takeover that has happened since the view-syncer
    // was started.
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      'some-other-task-id',
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    const otherTaskOwnershipTime = now;
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, otherTaskOwnershipTime),
      '07',
      REPLICA_VERSION, // CVR is at a newer replica version.
    ).flush(lc, otherTaskOwnershipTime, now, ttlClock);

    expect(await getCVROwner()).toBe('some-other-task-id');

    // Mark as initialized since this test doesn't call initConnection().
    vs.markInitialized();

    // deleteClients should be considered a new connection and
    // take over the CVR.
    await vs.deleteClients(SYNC_CONTEXT, [
      'deleteClients',
      {clientIDs: ['bar', 'no-such-client']},
    ]);

    // Wait for the fire-and-forget takeover to happen.
    await sleep(1000);
    expect(await getCVROwner()).toBe(TASK_ID);
  });

  test('sends invalid base cookie if client is ahead of CVR', async () => {
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, now),
      '07',
      REPLICA_VERSION,
    ).flush(lc, now, now, ttlClock);

    // Connect the client with a base cookie from the future.
    const client = connect({...SYNC_CONTEXT, baseCookie: '08'}, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    let result;
    try {
      result = await client.dequeue();
    } catch (e) {
      result = e;
    }
    expect(result).toBeInstanceOf(ProtocolError);
    expect((result as ProtocolError).errorBody).toEqual({
      kind: ErrorKind.InvalidConnectionRequestBaseCookie,
      message: 'CVR is at version 07',
      origin: ErrorOrigin.ZeroCache,
    } satisfies ErrorBody);
  });

  test('clean up operator storage on close', async () => {
    const storage = operatorStorage.createStorage();
    storage.set('foo', 'bar');
    expect(storageDB.prepare('SELECT * from storage').all()).toHaveLength(1);

    await vs.stop();
    await viewSyncerDone;

    expect(storageDB.prepare('SELECT * from storage').all()).toHaveLength(0);
  });

  // Does not test the actual timeout logic, but better than nothing.
  test('keepalive return value', () => {
    expect(vs.keepalive()).toBe(true);
    void vs.stop();
    expect(vs.keepalive()).toBe(false);
  });

  test('elective drain', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY2},
      {op: 'put', hash: 'query-hash3', ast: USERS_QUERY},
    ]);

    stateChanges.push({state: 'version-ready'});
    // This should result in computing a non-zero hydration time.
    await nextPoke(client);

    drainCoordinator.drainNextIn(0);
    expect(drainCoordinator.shouldDrain()).toBe(true);
    const now = Date.now();
    // Bump time forward to verify that the timeout is reset later.
    vi.setSystemTime(now + 3);

    // Enqueue a dummy task so that the view-syncer can elect to drain.
    stateChanges.push({state: 'version-ready'});

    // Upon completion, the view-syncer should have called drainNextIn()
    // with its hydration time so that the next drain is not triggered
    // until that interval elapses.
    await viewSyncerDone;
    expect(drainCoordinator.nextDrainTime).toBeGreaterThan(now);
  });

  test('retracting an exists relationship', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY_WITH_RELATED},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY_WITH_EXISTS},
    ]);
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);
    await nextPoke(client);

    replicator.processTransaction(
      '123',
      messages.delete('issueLabels', {
        issueID: '1',
        labelID: '1',
      }),
      messages.delete('issues', {id: '2'}),
    );

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "123",
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "123",
            "rowsPatch": [
              {
                "id": {
                  "issueID": "1",
                  "labelID": "1",
                },
                "op": "del",
                "tableName": "issueLabels",
              },
              {
                "id": {
                  "id": "1",
                },
                "op": "del",
                "tableName": "labels",
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123",
            "pokeID": "123",
          },
        ],
      ]
    `);
  });

  test('query with exists and related', async () => {
    const client = connect(SYNC_CONTEXT, [
      {
        op: 'put',
        hash: 'query-hash',
        ast: ISSUES_QUERY_WITH_EXISTS_AND_RELATED,
      },
    ]);
    await nextPoke(client); // config update
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client); // hydration

    // Satisfy the exists condition
    replicator.processTransaction(
      '123',
      messages.update('comments', {
        id: '1',
        text: 'foo',
      }),
    );

    stateChanges.push({state: 'version-ready'});

    expect(await nextPoke(client)).toMatchInlineSnapshot(`
            [
              [
                "pokeStart",
                {
                  "baseCookie": "01",
                  "pokeID": "123",
                },
              ],
              [
                "pokePart",
                {
                  "pokeID": "123",
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
                      "tableName": "comments",
                      "value": {
                        "id": "1",
                        "issueID": "1",
                        "text": "foo",
                      },
                    },
                    {
                      "op": "put",
                      "tableName": "comments",
                      "value": {
                        "id": "2",
                        "issueID": "1",
                        "text": "bar",
                      },
                    },
                  ],
                },
              ],
              [
                "pokeEnd",
                {
                  "cookie": "123",
                  "pokeID": "123",
                },
              ],
            ]
          `);
  });

  test('query with not exists and related', async () => {
    const client = connect(SYNC_CONTEXT, [
      {
        op: 'put',
        hash: 'query-hash',
        ast: ISSUES_QUERY_WITH_NOT_EXISTS_AND_RELATED,
      },
    ]);
    await nextPoke(client); // config update
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client); // hydration

    // Satisfy the not-exists condition by deleting the comment
    // that matches text='bar'.
    replicator.processTransaction(
      '123',
      messages.delete('comments', {id: '2'}),
    );

    stateChanges.push({state: 'version-ready'});

    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "123",
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "123",
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
                "tableName": "comments",
                "value": {
                  "id": "1",
                  "issueID": "1",
                  "text": "comment 1",
                },
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123",
            "pokeID": "123",
          },
        ],
      ]
    `);
  });

  test('errors on new connection if already shut down', async () => {
    // Simulates the view-syncer being stopped, e.g. due to an error,
    // but a client connecting to it before it was removed from the
    // service map.

    await vs.stop();
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    await expect(nextPoke(client)).rejects.toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: Reconnect required]`,
    );
  });
});
