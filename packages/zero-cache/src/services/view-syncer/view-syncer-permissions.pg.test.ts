import {beforeEach, describe, expect, vi} from 'vitest';
import {h128} from '../../../../shared/src/hash.ts';
import type {Queue} from '../../../../shared/src/queue.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import type {PermissionsConfig} from '../../../../zero-schema/src/compiled-permissions.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {type FakeReplicator} from '../replicator/test-utils.ts';
import {
  appMessages,
  COMMENTS_QUERY,
  ISSUES_QUERY,
  nextPoke,
  permissions,
  setup,
} from './view-syncer-test-util.ts';
import type {ViewSyncerService} from './view-syncer.ts';
import {type SyncContext} from './view-syncer.ts';

describe('permissions', () => {
  let stateChanges: Subscription<ReplicaState>;
  let connect: (
    ctx: SyncContext,
    desiredQueriesPatch: UpQueriesPatch,
  ) => Queue<Downstream>;
  let replicaDbFile: DbFile;
  let cvrDB: PostgresDB;
  let upstreamDb: PostgresDB;
  let vs: ViewSyncerService;
  let viewSyncerDone: Promise<void>;
  let replicator: FakeReplicator;

  const SYNC_CONTEXT: SyncContext = {
    clientID: 'foo',
    wsID: 'ws1',
    baseCookie: null,
    protocolVersion: PROTOCOL_VERSION,
    schemaVersion: 2,
    tokenData: {
      raw: '',
      decoded: {sub: 'foo', role: 'user', iat: 0},
    },
    httpCookie: undefined,
  };

  beforeEach<PgTest>(async ({testDBs}) => {
    ({
      stateChanges,
      connect,
      vs,
      viewSyncerDone,
      cvrDB,
      upstreamDb,
      replicaDbFile,
      replicator,
    } = await setup(testDBs, 'view_syncer_permissions_test', permissions));

    return async () => {
      // Restores fake date if used.
      vi.useRealTimers();
      await vs.stop();
      await viewSyncerDone;
      await testDBs.drop(cvrDB, upstreamDb);
      replicaDbFile.delete();
    };
  });

  test('client with user role followed by client with admin role', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);
    // the user is not logged in as admin and so cannot see any issues.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
            "pokeID": "01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
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

    // New client connects with same everything (client group, user id) but brings a new role.
    // This should transform their existing queries to return the data they can now see.
    const client2 = connect(
      {
        ...SYNC_CONTEXT,
        clientID: 'bar',
        tokenData: {
          raw: '',
          decoded: {sub: 'foo', role: 'admin', iat: 1},
        },
      },
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
    );

    expect(await nextPoke(client2)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "01:02",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
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
            "pokeID": "01:02",
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
            "cookie": "01:02",
            "pokeID": "01:02",
          },
        ],
      ]
    `);
  });

  test('upstream permissions change', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);
    // the user is not logged in as admin and so cannot see any issues.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
            "pokeID": "01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
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

    // Open permissions
    const relaxed: PermissionsConfig = {
      tables: {
        issues: {
          row: {
            select: [
              [
                'allow',
                {
                  type: 'simple',
                  left: {type: 'literal', value: true},
                  op: '=',
                  right: {type: 'literal', value: true},
                },
              ],
            ],
          },
        },
        comments: {},
      },
    };
    replicator.processTransaction(
      '05',
      appMessages.update('permissions', {
        lock: 1,
        permissions: relaxed,
        hash: h128(JSON.stringify(relaxed)).toString(16),
      }),
    );
    stateChanges.push({state: 'version-ready'});

    // Newly visible rows are poked.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "05",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "05",
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
            "cookie": "05",
            "pokeID": "05",
          },
        ],
      ]
    `);
  });

  test('permissions via subquery', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: COMMENTS_QUERY},
    ]);
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);
    // Should not receive any comments b/c they cannot see any issues
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
            "pokeID": "01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
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
  });

  test('query for comments does not return issue rows as those are gotten by the permission system', async () => {
    const client = connect(
      {
        ...SYNC_CONTEXT,
        tokenData: {
          raw: '',
          decoded: {sub: 'foo', role: 'admin', iat: 1},
        },
      },
      [{op: 'put', hash: 'query-hash2', ast: COMMENTS_QUERY}],
    );
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);
    // Should receive comments since they can see issues as the admin
    // but should not receive those issues since the query for them was added by
    // the auth system.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
            "pokeID": "01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "gotQueriesPatch": [
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
                "tableName": "comments",
                "value": {
                  "id": "1",
                  "issueID": "1",
                  "text": "comment 1",
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
            "cookie": "01",
            "pokeID": "01",
          },
        ],
      ]
    `);
  });
});
