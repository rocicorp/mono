import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {unreachable} from '../../../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {testDBs} from '../../test/db.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {cvrSchema} from '../../types/shards.ts';
import type {PatchToVersion} from './client-handler.ts';
import {
  ConcurrentModificationException,
  CVRStore,
  OwnershipError,
} from './cvr-store.ts';
import {
  CVRConfigDrivenUpdater,
  CVRQueryDrivenUpdater,
  type CVRSnapshot,
  CVRUpdater,
} from './cvr.ts';
import {
  type ClientsRow,
  compareClientsRows,
  compareDesiresRows,
  compareInstancesRows,
  compareQueriesRows,
  compareRowsRows,
  type DesiresRow,
  type InstancesRow,
  type QueriesRow,
  type RowsRow,
  type RowsVersionRow,
  setupCVRTables,
} from './schema/cvr.ts';
import type {ClientQueryRecord, CVRVersion, RowID} from './schema/types.ts';

const APP_ID = 'dapp';
const SHARD_NUM = 3;
const SHARD = {appID: APP_ID, shardNum: SHARD_NUM};

const LAST_CONNECT = Date.UTC(2024, 2, 1);

describe('view-syncer/cvr', () => {
  type DBState = {
    instances: (Partial<InstancesRow> &
      Pick<InstancesRow, 'clientGroupID' | 'version' | 'clientSchema'>)[];
    clients: ClientsRow[];
    queries: QueriesRow[];
    desires: DesiresRow[];
    rows: RowsRow[];
    rowsVersion?: RowsVersionRow[];
  };

  function setInitialState(
    db: PostgresDB,
    state: Partial<DBState>,
  ): Promise<void> {
    return db.begin(async tx => {
      const {instances, rowsVersion} = state;
      if (instances && !rowsVersion) {
        state = {
          ...state,
          rowsVersion: instances.map(({clientGroupID, version}) => ({
            clientGroupID,
            version,
          })),
        };
      }
      for (const [table, rows] of Object.entries(state)) {
        for (const row of rows) {
          await tx`INSERT INTO ${tx(`${cvrSchema(SHARD)}.` + table)} ${tx(
            row,
          )}`;
        }
      }
    });
  }

  async function expectState(db: PostgresDB, state: Partial<DBState>) {
    for (const table of Object.keys(state)) {
      const res = [
        ...(await db`SELECT * FROM ${db(`${cvrSchema(SHARD)}.` + table)}`),
      ];
      const tableState = [...(state[table as keyof DBState] || [])];
      switch (table) {
        case 'instances': {
          (res as InstancesRow[]).sort(compareInstancesRows);
          (tableState as InstancesRow[]).sort(compareInstancesRows);
          break;
        }
        case 'clients': {
          (res as ClientsRow[]).sort(compareClientsRows);
          (tableState as ClientsRow[]).sort(compareClientsRows);
          break;
        }
        case 'queries': {
          (res as QueriesRow[]).sort(compareQueriesRows);
          (tableState as QueriesRow[]).sort(compareQueriesRows);
          break;
        }
        case 'desires': {
          res.forEach(row => {
            // expiresAt is deprecated. It is still in the db but we do not
            // want it in the js objects.
            delete row.expiresAt;
          });
          (res as DesiresRow[]).sort(compareDesiresRows);
          (tableState as DesiresRow[]).sort(compareDesiresRows);
          break;
        }
        case 'rows': {
          (res as RowsRow[]).sort(compareRowsRows);
          (tableState as RowsRow[]).sort(compareRowsRows);
          break;
        }
        default: {
          unreachable();
        }
      }
      expect(res).toEqual(tableState);
    }
  }

  async function getAllState(db: PostgresDB): Promise<DBState> {
    const [instances, clients, queries, desires, rows] = await Promise.all([
      db`SELECT * FROM ${db('dapp_3/cvr.instances')}`,
      db`SELECT * FROM ${db('dapp_3/cvr.clients')}`,
      db`SELECT * FROM ${db('dapp_3/cvr.queries')}`,
      db`SELECT * FROM ${db('dapp_3/cvr.desires')}`,
      db`SELECT * FROM ${db('dapp_3/cvr.rows')}`,
    ]);

    desires.forEach(row => {
      // expiresAt is deprecated. It is still in the db but we do not
      // want it in the js objects.
      delete row.expiresAt;
    });
    return {
      instances,
      clients,
      queries,
      desires,
      rows,
    } as unknown as DBState;
  }

  const lc = createSilentLogContext();
  let db: PostgresDB;

  const ON_FAILURE = (e: unknown) => {
    throw e;
  };

  beforeEach(async () => {
    db = await testDBs.create('cvr_test_db');
    await db.begin(tx => setupCVRTables(lc, tx, SHARD));
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  async function catchupRows(
    cvrStore: CVRStore,
    afterVersion: CVRVersion,
    upToCVR: CVRSnapshot,
    current: CVRVersion,
    excludeQueries: string[] = [],
  ) {
    const rows: RowsRow[] = [];
    for await (const batch of cvrStore.catchupRowPatches(
      lc,
      afterVersion,
      upToCVR,
      current,
      excludeQueries,
    )) {
      rows.push(...batch);
    }
    return rows;
  }

  test('load first time cvr', async () => {
    const pgStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );

    const cvr = await pgStore.load(lc, LAST_CONNECT);
    expect(cvr).toEqual({
      id: 'abc123',
      version: {stateVersion: '00'},
      lastActive: 0,
      replicaVersion: null,
      clients: {},
      queries: {},
      clientSchema: null,
    } satisfies CVRSnapshot);
    const flushed = (
      await new CVRUpdater(pgStore, cvr, cvr.replicaVersion).flush(
        lc,
        LAST_CONNECT,
        Date.UTC(2024, 3, 20),
      )
    ).cvr;

    expect(flushed).toEqual({
      ...cvr,
      lastActive: 1713571200000,
    } satisfies CVRSnapshot);

    // Verify round tripping.
    const pgStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await pgStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(flushed);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '00',
          lastActive: 1713571200000,
          replicaVersion: null,
          owner: 'my-task',
          grantedAt: 1709251200000,
          clientSchema: null,
        },
      ],
      clients: [],
      queries: [],
      desires: [],
    });
  });

  test('set client schema', async () => {
    const pgStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );

    const cvr = await pgStore.load(lc, LAST_CONNECT);
    expect(cvr).toEqual({
      id: 'abc123',
      version: {stateVersion: '00'},
      lastActive: 0,
      replicaVersion: null,
      clients: {},
      queries: {},
      clientSchema: null,
    } satisfies CVRSnapshot);

    const updater = new CVRConfigDrivenUpdater(pgStore, cvr, SHARD);
    updater.setClientSchema(lc, {
      tables: {
        foo: {
          columns: {
            bar: {type: 'string'},
            baz: {type: 'number'},
          },
        },
      },
    });

    const {cvr: updated} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 20),
    );
    expect(updated).toMatchInlineSnapshot(`
      {
        "clientSchema": {
          "tables": {
            "foo": {
              "columns": {
                "bar": {
                  "type": "string",
                },
                "baz": {
                  "type": "number",
                },
              },
            },
          },
        },
        "clients": {},
        "id": "abc123",
        "lastActive": 1713571200000,
        "queries": {},
        "replicaVersion": null,
        "version": {
          "stateVersion": "00",
        },
      }
    `);

    // Verify round tripping.
    const pgStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await pgStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(updated);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '00',
          lastActive: 1713571200000,
          replicaVersion: null,
          owner: 'my-task',
          grantedAt: 1709251200000,
          clientSchema: {
            tables: {
              foo: {columns: {bar: {type: 'string'}, baz: {type: 'number'}}},
            },
          },
        },
      ],
      clients: [],
      queries: [],
      desires: [],
    });

    const updater2 = new CVRConfigDrivenUpdater(pgStore, updated, SHARD);

    // Setting the same client schema should be fine.
    updater2.setClientSchema(lc, {
      tables: {
        foo: {
          columns: {
            // baz is first this time.
            baz: {type: 'number'},
            bar: {type: 'string'},
          },
        },
      },
    });

    // Setting a different client schema should result in an error.
    expect(() =>
      updater2.setClientSchema(lc, {
        tables: {
          foo: {
            columns: {
              // data types are flipped
              baz: {type: 'string'},
              bar: {type: 'number'},
            },
          },
        },
      }),
    ).toThrowErrorMatchingInlineSnapshot(
      `[Error: {"kind":"InvalidConnectionRequest","message":"Provided schema does not match previous schema"}]`,
    );
  });

  // Relies on an async homing signal (with no explicit flush, so allow retries)
  test('load existing cvr', {retry: 3}, async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1a9:02',
          replicaVersion: '123',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: false,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          transformationHash: 'twoHash',
          queryArgs: null,
          queryName: null,
          transformationVersion: null,
          patchVersion: '1a9:02',
          internal: null,
          deleted: false,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: false,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [],
    };
    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );

    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    expect(cvr).toEqual({
      id: 'abc123',
      version: {stateVersion: '1a9', minorVersion: 2},
      replicaVersion: '123',
      lastActive: 1713830400000,
      clients: {
        fooClient: {
          id: 'fooClient',
          desiredQueryIDs: ['oneHash'],
        },
      },
      queries: {
        ['oneHash']: {
          id: 'oneHash',
          type: 'client',
          ast: {table: 'issues'},
          transformationHash: 'twoHash',
          clientState: {
            fooClient: {
              version: {stateVersion: '1a9', minorVersion: 1},
              inactivatedAt: undefined,
              ttl: -1,
            },
          },
          patchVersion: {stateVersion: '1a9', minorVersion: 2},
        },
      },
      clientSchema: null,
    } satisfies CVRSnapshot);

    await expectState(db, {
      ...initialState,
      instances: [
        {
          ...initialState.instances[0],
          owner: 'my-task',
          grantedAt: 1709251200000,
        },
      ],
    });
  });

  test('no update', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1a9:02',
          replicaVersion: '112',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: false,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: 'twoHash',
          transformationVersion: null,
          patchVersion: '1a9:02',
          internal: null,
          deleted: false,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: false,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [],
    };
    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRUpdater(cvrStore, cvr, cvr.replicaVersion);

    const {cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 24),
    );
    expect(flushed).toBe(false);

    expect(cvr).toEqual({
      id: 'abc123',
      version: {stateVersion: '1a9', minorVersion: 2},
      replicaVersion: '112',
      lastActive: 1713830400000,
      clients: {
        fooClient: {
          id: 'fooClient',
          desiredQueryIDs: ['oneHash'],
        },
      },
      queries: {
        oneHash: {
          id: 'oneHash',
          type: 'client',
          ast: {table: 'issues'},
          transformationHash: 'twoHash',
          clientState: {
            fooClient: {
              version: {stateVersion: '1a9', minorVersion: 1},
              inactivatedAt: undefined,
              ttl: -1,
            },
          },
          patchVersion: {stateVersion: '1a9', minorVersion: 2},
        },
      },
      clientSchema: null,
    } satisfies CVRSnapshot);

    expect(updated).toEqual(cvr);

    // Verify round tripping.
    const cvrStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await cvrStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(updated);

    // Let the takeover write that's fired during load to reach PG.
    await sleep(100);

    await expectState(db, {
      ...initialState,
      instances: [
        {
          ...initialState.instances[0],
          lastActive: Date.UTC(2024, 3, 23),
          owner: 'my-task',
          grantedAt: 1709251200000,
        },
      ],
    });
  });

  test('detects concurrent modification', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1a9:02',
          replicaVersion: '100',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [],
      queries: [],
      desires: [],
      rows: [],
    };
    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRConfigDrivenUpdater(cvrStore, cvr, SHARD);

    // Simulate an external modification, incrementing the patch version.
    await db`UPDATE "dapp_3/cvr".instances SET version = '1a9:03' WHERE "clientGroupID" = 'abc123'`;

    // force a flush to trigger detection
    updater.ensureClient('client-foo');

    await expect(
      updater.flush(lc, LAST_CONNECT, Date.UTC(2024, 4, 19)),
    ).rejects.toThrow(ConcurrentModificationException);

    // The last active time should not have been modified.
    expect(
      await db`SELECT "lastActive" FROM "dapp_3/cvr".instances WHERE "clientGroupID" = 'abc123'`,
    ).toEqual([{lastActive: Date.UTC(2024, 3, 23)}]);
  });

  test('detects ownership change', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1a9:02',
          replicaVersion: '100',
          lastActive: Date.UTC(2024, 3, 23),
          owner: 'my-task',
          grantedAt: LAST_CONNECT,
          clientSchema: null,
        },
      ],
      clients: [],
      queries: [],
      desires: [],
      rows: [],
    };
    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRConfigDrivenUpdater(cvrStore, cvr, SHARD);

    // Simulate an ownership change.
    await db`
    UPDATE "dapp_3/cvr".instances SET "owner"     = 'other-task', 
                             "grantedAt" = ${LAST_CONNECT + 1}
    WHERE "clientGroupID" = 'abc123'`;

    // force flush to trigger detection
    updater.ensureClient('client-bar');

    await expect(
      updater.flush(lc, LAST_CONNECT, Date.UTC(2024, 4, 19)),
    ).rejects.toThrow(OwnershipError);

    // The last active time should not have been modified.
    expect(
      await db`SELECT "lastActive" FROM "dapp_3/cvr".instances WHERE "clientGroupID" = 'abc123'`,
    ).toEqual([{lastActive: Date.UTC(2024, 3, 23)}]);
  });

  test('update desired query set', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1aa',
          replicaVersion: '101',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'dooClient',
          patchVersion: '1a8',
          deleted: false,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: false,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: 'twoHash',
          transformationVersion: null,
          patchVersion: '1a9:02',
          internal: null,
          deleted: false,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'dooClient',
          queryHash: 'oneHash',
          patchVersion: '1a8',
          deleted: false,
          inactivatedAt: null,
          ttl: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: false,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [],
    };
    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    expect(cvr).toEqual({
      id: 'abc123',
      version: {stateVersion: '1aa'},
      replicaVersion: '101',
      lastActive: 1713830400000,
      clients: {
        dooClient: {
          id: 'dooClient',
          desiredQueryIDs: ['oneHash'],
        },
        fooClient: {
          id: 'fooClient',
          desiredQueryIDs: ['oneHash'],
        },
      },
      queries: {
        oneHash: {
          id: 'oneHash',
          type: 'client',
          ast: {table: 'issues'},
          transformationHash: 'twoHash',
          transformationVersion: undefined,
          clientState: {
            dooClient: {
              version: {stateVersion: '1a8'},
              inactivatedAt: undefined,
              ttl: -1,
            },
            fooClient: {
              version: {stateVersion: '1a9', minorVersion: 1},
              inactivatedAt: undefined,
              ttl: -1,
            },
          },
          patchVersion: {stateVersion: '1a9', minorVersion: 2},
        },
      },
      clientSchema: null,
    } satisfies CVRSnapshot);

    const updater = new CVRConfigDrivenUpdater(cvrStore, cvr, SHARD);

    // This removes and adds desired queries to the existing fooClient.
    expect(updater.deleteDesiredQueries('fooClient', ['oneHash', 'twoHash']))
      .toMatchInlineSnapshot(`
      [
        {
          "patch": {
            "clientID": "fooClient",
            "id": "oneHash",
            "op": "del",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        },
      ]
    `);

    expect(
      updater.putDesiredQueries('fooClient', [
        {hash: 'fourHash', ast: {table: 'users'}, ttl: undefined},
        {hash: 'threeHash', ast: {table: 'comments'}, ttl: undefined},
      ]),
    ).toMatchInlineSnapshot(`
      [
        {
          "patch": {
            "clientID": "fooClient",
            "id": "fourHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        },
        {
          "patch": {
            "clientID": "fooClient",
            "id": "threeHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        },
      ]
    `);

    // This adds a new barClient with desired queries.
    expect(
      updater.putDesiredQueries('barClient', [
        {hash: 'oneHash', ast: {table: 'issues'}, ttl: undefined},
        {hash: 'threeHash', ast: {table: 'comments'}, ttl: undefined},
      ]),
    ).toMatchInlineSnapshot(`
      [
        {
          "patch": {
            "clientID": "barClient",
            "id": "oneHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        },
        {
          "patch": {
            "clientID": "barClient",
            "id": "threeHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        },
      ]
    `);

    // Adds a new client with no desired queries.
    expect(updater.putDesiredQueries('bonkClient', [])).toMatchInlineSnapshot(
      `[]`,
    );
    expect(updater.clearDesiredQueries('dooClient')).toMatchInlineSnapshot(`
                  [
                    {
                      "patch": {
                        "clientID": "dooClient",
                        "id": "oneHash",
                        "op": "del",
                        "type": "query",
                      },
                      "toVersion": {
                        "minorVersion": 1,
                        "stateVersion": "1aa",
                      },
                    },
                  ]
                `);

    const {cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 24),
    );

    expect(flushed).toMatchInlineSnapshot(`
      {
        "clients": 2,
        "desires": 6,
        "instances": 1,
        "queries": 7,
        "rows": 0,
        "rowsDeferred": 0,
        "statements": 17,
      }
    `);
    expect(updated).toEqual({
      id: 'abc123',
      version: {stateVersion: '1aa', minorVersion: 1}, // minorVersion bump
      replicaVersion: '101',
      lastActive: 1713916800000,
      clients: {
        barClient: {
          id: 'barClient',
          desiredQueryIDs: ['oneHash', 'threeHash'],
        },
        bonkClient: {
          id: 'bonkClient',
          desiredQueryIDs: [],
        },
        dooClient: {
          desiredQueryIDs: [],
          id: 'dooClient',
        },
        fooClient: {
          id: 'fooClient',
          desiredQueryIDs: ['fourHash', 'threeHash'],
        },
      },
      queries: {
        lmids: {
          id: 'lmids',
          type: 'internal',
          ast: {
            table: `${APP_ID}_${SHARD_NUM}.clients`,
            schema: '',
            where: {
              type: 'simple',
              op: '=',
              left: {
                type: 'column',
                name: 'clientGroupID',
              },
              right: {
                type: 'literal',
                value: 'abc123',
              },
            },
            orderBy: [
              ['clientGroupID', 'asc'],
              ['clientID', 'asc'],
            ],
          },
        },
        oneHash: {
          id: 'oneHash',
          type: 'client',
          ast: {table: 'issues'},
          transformationHash: 'twoHash',
          transformationVersion: undefined,
          clientState: {
            barClient: {
              version: {stateVersion: '1aa', minorVersion: 1},
              inactivatedAt: undefined,
              ttl: -1,
            },
          },
          patchVersion: {stateVersion: '1a9', minorVersion: 2},
        },
        threeHash: {
          id: 'threeHash',
          type: 'client',
          ast: {table: 'comments'},
          clientState: {
            barClient: {
              version: {stateVersion: '1aa', minorVersion: 1},
              inactivatedAt: undefined,
              ttl: -1,
            },
            fooClient: {
              version: {stateVersion: '1aa', minorVersion: 1},
              inactivatedAt: undefined,
              ttl: -1,
            },
          },
        },
        fourHash: {
          id: 'fourHash',
          type: 'client',
          ast: {table: 'users'},
          clientState: {
            fooClient: {
              version: {stateVersion: '1aa', minorVersion: 1},
              inactivatedAt: undefined,
              ttl: -1,
            },
          },
        },
      },
      clientSchema: null,
    } satisfies CVRSnapshot);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          lastActive: new Date('2024-04-24T00:00:00.000Z').getTime(),
          version: '1aa:01',
          replicaVersion: '101',
          owner: 'my-task',
          grantedAt: 1709251200000,
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          deleted: false,
          patchVersion: '1a9:01',
        },
        {
          clientGroupID: 'abc123',
          clientID: 'barClient',
          deleted: false,
          patchVersion: '1aa:01',
        },
        {
          clientGroupID: 'abc123',
          clientID: 'bonkClient',
          deleted: false,
          patchVersion: '1aa:01',
        },
        {
          clientGroupID: 'abc123',
          clientID: 'dooClient',
          deleted: false,
          patchVersion: '1a8',
        },
      ],
      queries: [
        {
          clientAST: {
            table: 'users',
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: false,
          internal: null,
          patchVersion: null,
          queryHash: 'fourHash',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            schema: '',
            table: `${APP_ID}_${SHARD_NUM}.clients`,
            where: {
              left: {
                type: 'column',
                name: 'clientGroupID',
              },
              op: '=',
              type: 'simple',
              right: {
                type: 'literal',
                value: 'abc123',
              },
            },
            orderBy: [
              ['clientGroupID', 'asc'],
              ['clientID', 'asc'],
            ],
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: false,
          internal: true,
          patchVersion: null,
          queryHash: 'lmids',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'comments',
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: false,
          internal: null,
          patchVersion: null,
          queryHash: 'threeHash',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: false,
          internal: null,
          patchVersion: '1a9:02',
          queryHash: 'oneHash',
          transformationHash: 'twoHash',
          transformationVersion: null,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          deleted: true,
          patchVersion: '1aa:01',
          queryHash: 'oneHash',
          inactivatedAt: null,
          ttl: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          deleted: false,
          patchVersion: '1aa:01',
          queryHash: 'fourHash',
          inactivatedAt: null,
          ttl: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          deleted: false,
          patchVersion: '1aa:01',
          queryHash: 'threeHash',
          inactivatedAt: null,
          ttl: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'barClient',
          deleted: false,
          patchVersion: '1aa:01',
          queryHash: 'oneHash',
          inactivatedAt: null,
          ttl: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'barClient',
          deleted: false,
          patchVersion: '1aa:01',
          queryHash: 'threeHash',
          inactivatedAt: null,
          ttl: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'dooClient',
          deleted: true,
          patchVersion: '1aa:01',
          queryHash: 'oneHash',
          inactivatedAt: null,
          ttl: null,
        },
      ],

      //  rows: [],
    });

    // Verify round tripping.
    const cvrStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await cvrStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(updated);

    // Add the deleted desired query back. This ensures that the
    // desired query update statement is an UPSERT.
    const updater2 = new CVRConfigDrivenUpdater(cvrStore2, reloaded, SHARD);
    expect(
      updater2.putDesiredQueries('fooClient', [
        {hash: 'oneHash', ast: {table: 'issues'}, ttl: undefined},
      ]),
    ).toMatchInlineSnapshot(`
      [
        {
          "patch": {
            "clientID": "fooClient",
            "id": "oneHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 2,
            "stateVersion": "1aa",
          },
        },
      ]
    `);

    const {cvr: updated2} = await updater2.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 24, 1),
    );
    expect(updated2.clients.fooClient.desiredQueryIDs).toContain('oneHash');
  });

  test('no-op change to desired query set', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1aa',
          replicaVersion: '03',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: false,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          transformationHash: 'twoHash',
          transformationVersion: null,
          patchVersion: '1a9:02',
          deleted: false,
          internal: null,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: false,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [],
    };
    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRConfigDrivenUpdater(cvrStore, cvr, SHARD);

    // Same desired query set. Nothing should change except last active time.
    expect(
      updater.putDesiredQueries('fooClient', [
        {hash: 'oneHash', ast: {table: 'issues'}, ttl: undefined},
      ]),
    ).toMatchInlineSnapshot(`[]`);

    // Same last active day (no index change), but different hour.
    const {cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 1),
    );
    expect(flushed).toBe(false);

    expect(updated).toEqual(cvr);

    // Verify round tripping.
    const doCVRStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await doCVRStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(updated);

    // Let the takeover write that's fired during load to reach PG.
    await sleep(100);

    await expectState(db, {
      ...initialState,
      instances: [
        {
          ...initialState.instances[0],
          lastActive: Date.UTC(2024, 3, 23),
          owner: 'my-task',
          grantedAt: 1709251200000,
        },
      ],
    });
  });

  const ROW_TABLE = {
    schema: 'public',
    table: 'issues',
  };

  const ROW_KEY1 = {id: '123'};
  const ROW_ID1: RowID = {...ROW_TABLE, rowKey: ROW_KEY1};

  const ROW_KEY2 = {id: '321'};
  const ROW_ID2: RowID = {...ROW_TABLE, rowKey: ROW_KEY2};

  const ROW_KEY3 = {id: '888'};
  const ROW_ID3: RowID = {...ROW_TABLE, rowKey: ROW_KEY3};

  const DELETE_ROW_KEY = {id: '456'};

  const IN_OLD_PATCH_ROW_KEY = {id: '777'};

  test('desired to got', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1aa',
          replicaVersion: null,
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          transformationHash: null,
          transformationVersion: null,
          patchVersion: null,
          internal: null,
          deleted: null,
        },
        {
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          queryHash: 'already-deleted',
          clientAST: {table: 'issues'}, // TODO(arv): Maybe nullable
          patchVersion: '189',
          transformationHash: null,
          transformationVersion: null,
          internal: null,
          deleted: true, // Already in CVRs from "189"
        },
        {
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          queryHash: 'catchup-delete',
          clientAST: {table: 'issues'}, // TODO(arv): Maybe nullable
          patchVersion: '19z',
          transformationHash: null,
          transformationVersion: null,
          internal: null,
          deleted: true,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: {twoHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {twoHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY3,
          rowVersion: '03',
          refCounts: null,
          patchVersion: '19z',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: IN_OLD_PATCH_ROW_KEY,
          rowVersion: '03',
          refCounts: null,
          patchVersion: '189',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: DELETE_ROW_KEY,
          rowVersion: '03',
          refCounts: null,
          patchVersion: '1aa',
          schema: 'public',
          table: 'issues',
        },
      ],
    };

    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1aa', '123');

    const {newVersion, queryPatches} = updater.trackQueries(
      lc,
      [{id: 'oneHash', transformationHash: 'serverOneHash'}],
      [],
    );
    expect(newVersion).toEqual({stateVersion: '1aa', minorVersion: 1});
    expect(queryPatches).toMatchInlineSnapshot(`
      [
        {
          "patch": {
            "id": "oneHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        },
      ]
    `);

    // Simulate receiving different views rows at different time times.
    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '03',
              refCounts: {oneHash: 1},
              contents: {id: 'should-show-up-in-patch'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1a0'},
        patch: {
          type: 'row',
          op: 'put',
          id: ROW_ID1,
          contents: {id: 'should-show-up-in-patch'},
        },
      },
    ] satisfies PatchToVersion[]);
    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID2,
            {
              version: '03',
              refCounts: {oneHash: 1},
              contents: {id: 'same column selection as twoHash'},
            },
          ],
          [
            ROW_ID3,
            {
              version: '09',
              refCounts: {oneHash: 1},
              contents: {id: 'new version patch'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1a0'},
        patch: {
          type: 'row',
          op: 'put',
          id: ROW_ID2,
          contents: {id: 'same column selection as twoHash'},
        },
      },
      {
        toVersion: {stateVersion: '1aa', minorVersion: 1},
        patch: {
          type: 'row',
          op: 'put',
          id: ROW_ID3,
          contents: {id: 'new version patch'},
        },
      },
    ] satisfies PatchToVersion[]);
    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '03',
              refCounts: {oneHash: 1},
              contents: {id: 'should-show-up-in-patch'},
            },
          ],
        ]),
      ),
    ).toEqual([
      // deduped
    ]);

    expect(await updater.deleteUnreferencedRows()).toEqual([]);

    // Same last active day (no index change), but different hour.
    const {cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 1),
    );
    expect(flushed).toMatchInlineSnapshot(`
      {
        "clients": 0,
        "desires": 0,
        "instances": 1,
        "queries": 1,
        "rows": 3,
        "rowsDeferred": 0,
        "statements": 4,
      }
    `);

    expect(
      await cvrStore.catchupConfigPatches(
        lc,
        {stateVersion: '189'},
        cvr,
        updated.version,
      ),
    ).toMatchInlineSnapshot(`
      [
        {
          "patch": {
            "id": "catchup-delete",
            "op": "del",
            "type": "query",
          },
          "toVersion": {
            "stateVersion": "19z",
          },
        },
        {
          "patch": {
            "clientID": "fooClient",
            "id": "oneHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1a9",
          },
        },
      ]
    `);

    expect(
      await catchupRows(cvrStore, {stateVersion: '189'}, cvr, updated.version, [
        'oneHash',
      ]),
    ).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "abc123",
          "patchVersion": "1aa",
          "refCounts": null,
          "rowKey": {
            "id": "456",
          },
          "rowVersion": "03",
          "schema": "public",
          "table": "issues",
        },
      ]
    `);

    expect(updated).toEqual({
      ...cvr,
      replicaVersion: '123',
      version: newVersion,
      queries: {
        oneHash: {
          id: 'oneHash',
          type: 'client',
          ast: {table: 'issues'},
          clientState: {
            fooClient: {
              version: {stateVersion: '1a9', minorVersion: 1},
              inactivatedAt: undefined,
              ttl: -1,
            },
          },
          transformationHash: 'serverOneHash',
          transformationVersion: {stateVersion: '1aa', minorVersion: 1},
          patchVersion: {stateVersion: '1aa', minorVersion: 1},
        },
      },
      lastActive: 1713834000000,
    } satisfies CVRSnapshot);

    // Verify round tripping.
    const cvrStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await cvrStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(updated);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          lastActive: new Date('2024-04-23T01:00:00Z').getTime(),
          version: '1aa:01',
          replicaVersion: '123',
          owner: 'my-task',
          grantedAt: 1709251200000,
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          deleted: null,
          patchVersion: '1a9:01',
        },
      ],
      queries: [
        {
          clientAST: {
            table: 'issues',
          },
          queryArgs: null,
          queryName: null,
          clientGroupID: 'abc123',
          deleted: true,
          internal: null,
          patchVersion: '189',
          queryHash: 'already-deleted',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'issues',
          },
          queryArgs: null,
          queryName: null,
          clientGroupID: 'abc123',
          deleted: true,
          internal: null,
          patchVersion: '19z',
          queryHash: 'catchup-delete',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'issues',
          },
          queryArgs: null,
          queryName: null,
          clientGroupID: 'abc123',
          deleted: false,
          internal: null,
          patchVersion: '1aa:01',
          queryHash: 'oneHash',
          transformationHash: 'serverOneHash',
          transformationVersion: '1aa:01',
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          deleted: null,
          patchVersion: '1a9:01',
          queryHash: 'oneHash',
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          patchVersion: '189',
          refCounts: null,
          rowKey: IN_OLD_PATCH_ROW_KEY,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1aa',
          refCounts: null,
          rowKey: DELETE_ROW_KEY,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1a0',
          refCounts: {
            oneHash: 1,
            twoHash: 1,
          },
          rowKey: ROW_KEY2,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1aa:01',
          refCounts: {
            oneHash: 1,
          },
          rowKey: ROW_KEY3,
          rowVersion: '09',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1a0',
          refCounts: {
            oneHash: 2,
            twoHash: 1,
          },
          rowKey: ROW_KEY1,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
      ],
    });
  });

  // ^^: just run this test twice? Once for executed once for transformed
  test('new transformation hash', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1ba',
          replicaVersion: '123',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          transformationHash: 'serverOneHash',
          transformationVersion: '1aa',
          patchVersion: '1aa:01',
          internal: null,
          deleted: null,
        },
        {
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          queryHash: 'already-deleted',
          clientAST: {table: 'issues'}, // TODO(arv): Maybe nullable
          patchVersion: '189',
          transformationHash: null,
          transformationVersion: null,
          internal: null,
          deleted: true, // Already in CVRs from "189"
        },
        {
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          queryHash: 'catchup-delete',
          clientAST: {table: 'issues'}, // TODO(arv): Maybe nullable
          patchVersion: '19z',
          transformationHash: null,
          transformationVersion: null,
          internal: null,
          deleted: true,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: {
            oneHash: 1,
            twoHash: 1,
          },
          patchVersion: '1aa:01',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {twoHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY3,
          rowVersion: '09',
          refCounts: {oneHash: 1},
          patchVersion: '1aa:01',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: IN_OLD_PATCH_ROW_KEY,
          rowVersion: '03',
          refCounts: null,
          patchVersion: '189',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: DELETE_ROW_KEY,
          rowVersion: '03',
          refCounts: null,
          patchVersion: '1ba',
          schema: 'public',
          table: 'issues',
        },
      ],
    };
    await setInitialState(db, initialState);

    let cvrStore = new CVRStore(lc, db, SHARD, 'my-task', 'abc123', ON_FAILURE);
    let cvr = await cvrStore.load(lc, LAST_CONNECT);
    let updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba', '123');

    let {newVersion, queryPatches} = updater.trackQueries(
      lc,
      [{id: 'oneHash', transformationHash: 'serverTwoHash'}],
      [],
    );
    expect(newVersion).toEqual({stateVersion: '1ba', minorVersion: 1});
    expect(queryPatches).toHaveLength(0);

    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '03',
              refCounts: {oneHash: 1},
              contents: {id: 'existing patch'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1aa', minorVersion: 1},
        patch: {
          type: 'row',
          op: 'put',
          id: ROW_ID1,
          contents: {id: 'existing patch'},
        },
      },
    ] satisfies PatchToVersion[]);

    expect(updater.updatedVersion()).toEqual({
      stateVersion: '1ba',
      minorVersion: 1,
    });

    expect(
      await updater.received(
        lc,
        new Map([
          [
            // Now referencing ROW_ID2 instead of ROW_ID3
            ROW_ID2,
            {
              version: '09',
              refCounts: {oneHash: 1},
              contents: {id: 'new-row-version-should-bump-cvr-version'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1ba', minorVersion: 1},
        patch: {
          type: 'row',
          op: 'put',
          id: ROW_ID2,
          contents: {id: 'new-row-version-should-bump-cvr-version'},
        },
      },
    ]);

    expect(await updater.deleteUnreferencedRows()).toEqual([
      {
        patch: {type: 'row', op: 'del', id: ROW_ID3},
        toVersion: newVersion,
      },
    ] satisfies PatchToVersion[]);

    // Same last active day (no index change), but different hour.
    let {cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 1),
    );
    expect(flushed).toMatchInlineSnapshot(`
      {
        "clients": 0,
        "desires": 0,
        "instances": 1,
        "queries": 1,
        "rows": 2,
        "rowsDeferred": 0,
        "statements": 4,
      }
    `);

    expect(
      await cvrStore.catchupConfigPatches(
        lc,
        {stateVersion: '189'},
        cvr,
        updated.version,
      ),
    ).toMatchInlineSnapshot(`
      [
        {
          "patch": {
            "id": "catchup-delete",
            "op": "del",
            "type": "query",
          },
          "toVersion": {
            "stateVersion": "19z",
          },
        },
        {
          "patch": {
            "id": "oneHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        },
        {
          "patch": {
            "clientID": "fooClient",
            "id": "oneHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1a9",
          },
        },
      ]
    `);

    expect(
      await catchupRows(cvrStore, {stateVersion: '189'}, cvr, updated.version, [
        'oneHash',
      ]),
    ).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "abc123",
          "patchVersion": "1ba",
          "refCounts": null,
          "rowKey": {
            "id": "456",
          },
          "rowVersion": "03",
          "schema": "public",
          "table": "issues",
        },
      ]
    `);

    expect(updated).toEqual({
      ...cvr,
      version: newVersion,
      queries: {
        oneHash: {
          id: 'oneHash',
          type: 'client',
          ast: {table: 'issues'},
          clientState: {
            fooClient: {
              version: {stateVersion: '1a9', minorVersion: 1},
              inactivatedAt: undefined,
              ttl: -1,
            },
          },
          transformationHash: 'serverTwoHash',
          transformationVersion: {stateVersion: '1ba', minorVersion: 1},
          patchVersion: {stateVersion: '1aa', minorVersion: 1},
        },
      },
      lastActive: 1713834000000,
    } satisfies CVRSnapshot);

    // Verify round tripping.
    cvrStore = new CVRStore(lc, db, SHARD, 'my-task', 'abc123', ON_FAILURE);
    cvr = await cvrStore.load(lc, LAST_CONNECT);
    expect(cvr).toEqual(updated);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          lastActive: new Date('2024-04-23T01:00:00Z').getTime(),
          version: '1ba:01',
          replicaVersion: '123',
          owner: 'my-task',
          grantedAt: 1709251200000,
          clientSchema: null,
        },
      ],
      clients: initialState.clients,
      queries: [
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: true,
          internal: null,
          patchVersion: '189',
          queryHash: 'already-deleted',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: true,
          internal: null,
          patchVersion: '19z',
          queryHash: 'catchup-delete',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: false,
          internal: null,
          patchVersion: '1aa:01',
          queryHash: 'oneHash',
          transformationHash: 'serverTwoHash',
          transformationVersion: '1ba:01',
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          deleted: null,
          patchVersion: '1a9:01',
          queryHash: 'oneHash',
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          patchVersion: '1aa:01',
          refCounts: {
            oneHash: 1,
            twoHash: 1,
          },
          rowKey: ROW_KEY1,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '189',
          refCounts: null,
          rowKey: IN_OLD_PATCH_ROW_KEY,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1ba',
          refCounts: null,
          rowKey: DELETE_ROW_KEY,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1ba:01',
          refCounts: {
            oneHash: 1,
            twoHash: 1,
          },
          rowKey: ROW_KEY2,
          rowVersion: '09',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1ba:01',
          refCounts: null,
          rowKey: ROW_KEY3,
          rowVersion: '09',
          schema: 'public',
          table: 'issues',
        },
      ],
    });

    updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba', '123');
    ({newVersion, queryPatches} = updater.trackQueries(
      lc,
      [{id: 'oneHash', transformationHash: 'newXFormHash'}],
      [],
    ));
    expect(newVersion).toEqual({stateVersion: '1ba', minorVersion: 2});
    expect(queryPatches).toHaveLength(0);

    ({cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 2),
    ));
    expect(flushed).toMatchInlineSnapshot(`
      {
        "clients": 0,
        "desires": 0,
        "instances": 1,
        "queries": 1,
        "rows": 0,
        "rowsDeferred": 0,
        "statements": 3,
      }
    `);

    const newState = await getAllState(db);
    expect({
      instances: newState.instances,
      clients: newState.clients,
      queries: newState.queries,
    }).toEqual({
      instances: [
        {
          clientGroupID: 'abc123',
          lastActive: new Date('2024-04-23T02:00:00Z').getTime(),
          version: '1ba:02',
          replicaVersion: '123',
          owner: 'my-task',
          grantedAt: 1709251200000,
          clientSchema: null,
        },
      ],
      clients: initialState.clients,
      queries: [
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryName: null,
          queryArgs: null,
          deleted: true,
          internal: null,
          patchVersion: '189',
          queryHash: 'already-deleted',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryName: null,
          queryArgs: null,
          deleted: true,
          internal: null,
          patchVersion: '19z',
          queryHash: 'catchup-delete',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryName: null,
          queryArgs: null,
          deleted: false,
          internal: null,
          patchVersion: '1aa:01',
          queryHash: 'oneHash',
          transformationHash: 'newXFormHash',
          transformationVersion: '1ba:02',
        },
      ],
    });
  });

  test('multiple executed queries', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1ba',
          replicaVersion: '123',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: 'serverOneHash',
          transformationVersion: '1aa',
          patchVersion: '1aa:01',
          internal: null,
          deleted: null,
        },
        {
          clientGroupID: 'abc123',
          queryHash: 'twoHash',
          queryArgs: null,
          queryName: null,
          clientAST: {table: 'issues'},
          transformationHash: 'serverTwoHash',
          transformationVersion: '1aa',
          patchVersion: '1aa:01',
          internal: null,
          deleted: null,
        },
        {
          clientGroupID: 'abc123',
          queryHash: 'already-deleted',
          queryArgs: null,
          queryName: null,
          clientAST: {table: 'issues'},
          patchVersion: '189',
          transformationHash: null,
          transformationVersion: null,
          internal: null,
          deleted: true,
        },
        {
          clientGroupID: 'abc123',
          queryHash: 'catchup-delete',
          queryArgs: null,
          queryName: null,
          clientAST: {table: 'issues'},
          patchVersion: '19z',
          transformationHash: null,
          transformationVersion: null,
          internal: null,
          deleted: true,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'twoHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: IN_OLD_PATCH_ROW_KEY,
          rowVersion: '03',
          refCounts: null,
          patchVersion: '189',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: DELETE_ROW_KEY,
          rowVersion: '03',
          refCounts: null,
          patchVersion: '1ba',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: {
            oneHash: 1,
            twoHash: 1,
          },
          patchVersion: '1aa:01',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {twoHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY3,
          rowVersion: '09',
          refCounts: {oneHash: 1},
          patchVersion: '1aa:01',
          schema: 'public',
          table: 'issues',
        },
      ],
    };

    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba', '123');

    const {newVersion, queryPatches} = updater.trackQueries(
      lc,
      [
        {id: 'oneHash', transformationHash: 'updatedServerOneHash'},
        {id: 'twoHash', transformationHash: 'updatedServerTwoHash'},
      ],
      [],
    );
    expect(newVersion).toEqual({stateVersion: '1ba', minorVersion: 1});
    expect(queryPatches).toHaveLength(0);

    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '03',
              refCounts: {oneHash: 1},
              contents: {id: 'existing-patch'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1aa', minorVersion: 1},
        patch: {
          type: 'row',
          op: 'put',
          id: ROW_ID1,
          contents: {id: 'existing-patch'},
        },
      },
    ] satisfies PatchToVersion[]);
    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '03',
              refCounts: {twoHash: 1},
              contents: {id: 'existing-patch'},
            },
          ],
        ]),
      ),
    ).toEqual([
      // deduped
    ]);
    await updater.received(
      lc,
      new Map([
        [
          // Now referencing ROW_ID2 instead of ROW_ID3
          ROW_ID2,
          {
            version: '09',
            refCounts: {oneHash: 1},
            contents: {
              /* ignored */
            },
          },
        ],
      ]),
    );
    await updater.received(
      lc,
      new Map([
        [
          ROW_ID2,
          {
            version: '09',
            refCounts: {twoHash: 1},
            contents: {
              /* ignored */
            },
          },
        ],
      ]),
    );

    expect(await updater.deleteUnreferencedRows()).toEqual([
      {
        patch: {type: 'row', op: 'del', id: ROW_ID3},
        toVersion: newVersion,
      },
    ] satisfies PatchToVersion[]);

    // Same last active day (no index change), but different hour.
    const {cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 1),
    );
    expect(flushed).toMatchInlineSnapshot(`
      {
        "clients": 0,
        "desires": 0,
        "instances": 1,
        "queries": 2,
        "rows": 2,
        "rowsDeferred": 0,
        "statements": 5,
      }
    `);

    expect(
      await cvrStore.catchupConfigPatches(
        lc,
        {stateVersion: '189'},
        cvr,
        updated.version,
      ),
    ).toMatchInlineSnapshot(`
      [
        {
          "patch": {
            "id": "catchup-delete",
            "op": "del",
            "type": "query",
          },
          "toVersion": {
            "stateVersion": "19z",
          },
        },
        {
          "patch": {
            "id": "oneHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        },
        {
          "patch": {
            "id": "twoHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        },
        {
          "patch": {
            "clientID": "fooClient",
            "id": "oneHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1a9",
          },
        },
        {
          "patch": {
            "clientID": "fooClient",
            "id": "twoHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1a9",
          },
        },
      ]
    `);

    expect(
      await catchupRows(cvrStore, {stateVersion: '189'}, cvr, updated.version, [
        'oneHash',
        'twoHash',
      ]),
    ).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "abc123",
          "patchVersion": "1ba",
          "refCounts": null,
          "rowKey": {
            "id": "456",
          },
          "rowVersion": "03",
          "schema": "public",
          "table": "issues",
        },
      ]
    `);

    expect(updated).toEqual({
      ...cvr,
      version: newVersion,
      lastActive: 1713834000000,
      queries: {
        oneHash: {
          id: 'oneHash',
          type: 'client',
          ast: {table: 'issues'},
          clientState: {
            fooClient: {
              version: {stateVersion: '1a9', minorVersion: 1},
              inactivatedAt: undefined,
              ttl: -1,
            },
          },
          transformationHash: 'updatedServerOneHash',
          transformationVersion: newVersion,
          patchVersion: {stateVersion: '1aa', minorVersion: 1},
        },
        twoHash: {
          id: 'twoHash',
          type: 'client',
          ast: {table: 'issues'},
          clientState: {
            fooClient: {
              version: {stateVersion: '1a9', minorVersion: 1},
              inactivatedAt: undefined,
              ttl: -1,
            },
          },
          transformationHash: 'updatedServerTwoHash',
          transformationVersion: newVersion,
          patchVersion: {stateVersion: '1aa', minorVersion: 1},
        },
      },
    } satisfies CVRSnapshot);

    // Verify round tripping.
    const doCVRStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await doCVRStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(updated);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          lastActive: new Date('2024-04-23T01:00:00Z').getTime(),
          version: '1ba:01',
          replicaVersion: '123',
          owner: 'my-task',
          grantedAt: 1709251200000,
          clientSchema: null,
        },
      ],
      clients: initialState.clients,
      queries: [
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: true,
          internal: null,
          patchVersion: '189',
          queryHash: 'already-deleted',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: true,
          internal: null,
          patchVersion: '19z',
          queryHash: 'catchup-delete',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: false,
          internal: null,
          patchVersion: '1aa:01',
          queryHash: 'oneHash',
          transformationHash: 'updatedServerOneHash',
          transformationVersion: '1ba:01',
        },
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: false,
          internal: null,
          patchVersion: '1aa:01',
          queryHash: 'twoHash',
          transformationHash: 'updatedServerTwoHash',
          transformationVersion: '1ba:01',
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          deleted: null,
          patchVersion: '1a9:01',
          queryHash: 'oneHash',
          inactivatedAt: null,
          ttl: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          deleted: null,
          patchVersion: '1a9:01',
          queryHash: 'twoHash',
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          patchVersion: '189',
          refCounts: null,
          rowKey: IN_OLD_PATCH_ROW_KEY,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1ba',
          refCounts: null,
          rowKey: DELETE_ROW_KEY,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1aa:01',
          refCounts: {
            oneHash: 1,
            twoHash: 1,
          },
          rowKey: ROW_KEY1,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1ba:01',
          refCounts: {
            oneHash: 1,
            twoHash: 1,
          },
          rowKey: ROW_KEY2,
          rowVersion: '09',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1ba:01',
          refCounts: null,
          rowKey: ROW_KEY3,
          rowVersion: '09',
          schema: 'public',
          table: 'issues',
        },
      ],
    });
  });

  test('removed query', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1ba',
          replicaVersion: '123',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          queryArgs: null,
          queryName: null,
          clientAST: {table: 'issues'},
          transformationHash: 'serverOneHash',
          transformationVersion: '1aa',
          patchVersion: '1aa:01',
          internal: null,
          deleted: false,
        },
        {
          clientGroupID: 'abc123',
          queryHash: 'already-deleted',
          queryArgs: null,
          queryName: null,
          clientAST: {table: 'issues'},
          patchVersion: '189',
          transformationHash: null,
          transformationVersion: null,
          internal: null,
          deleted: true,
        },
        {
          clientGroupID: 'abc123',
          queryHash: 'catchup-delete',
          queryArgs: null,
          queryName: null,
          clientAST: {table: 'issues'},
          patchVersion: '19z',
          transformationHash: null,
          transformationVersion: null,
          internal: null,
          deleted: true,
        },
      ],
      desires: [],
      rows: [
        {
          clientGroupID: 'abc123',
          patchVersion: '189',
          rowKey: IN_OLD_PATCH_ROW_KEY,
          rowVersion: '03',
          refCounts: null,
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '19z',
          rowKey: DELETE_ROW_KEY,
          rowVersion: '03',
          refCounts: null,
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1aa:01',
          rowKey: ROW_KEY1,
          refCounts: {
            oneHash: 1,
            twoHash: 1,
          },
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1ba',
          rowKey: ROW_KEY2,
          refCounts: {twoHash: 1},
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1aa:01',
          rowKey: ROW_KEY3,
          refCounts: {oneHash: 1},
          rowVersion: '09',
          schema: 'public',
          table: 'issues',
        },
      ],
    };

    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba', '123');

    const {newVersion, queryPatches} = updater.trackQueries(
      lc,
      [],
      [{id: 'oneHash', transformationHash: 'oneHash'}],
    );
    expect(newVersion).toEqual({stateVersion: '1ba', minorVersion: 1});
    expect(queryPatches).toMatchInlineSnapshot(`
      [
        {
          "patch": {
            "id": "oneHash",
            "op": "del",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1ba",
          },
        },
      ]
    `);

    expect(await updater.deleteUnreferencedRows()).toEqual([
      {
        patch: {type: 'row', op: 'del', id: ROW_ID3},
        toVersion: newVersion,
      },
    ] satisfies PatchToVersion[]);

    // Same last active day (no index change), but different hour.
    // Note: Must flush before generating config patches.
    const {cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 1),
    );
    expect(flushed).toMatchInlineSnapshot(`
      {
        "clients": 0,
        "desires": 0,
        "instances": 1,
        "queries": 1,
        "rows": 2,
        "rowsDeferred": 0,
        "statements": 4,
      }
    `);

    expect(
      await cvrStore.catchupConfigPatches(
        lc,
        {stateVersion: '189'},
        cvr,
        updated.version,
      ),
    ).toMatchInlineSnapshot(`
      [
        {
          "patch": {
            "id": "catchup-delete",
            "op": "del",
            "type": "query",
          },
          "toVersion": {
            "stateVersion": "19z",
          },
        },
      ]
    `);

    expect(
      await catchupRows(
        cvrStore,
        {stateVersion: '189'},
        cvr,
        updated.version,
        [],
      ),
    ).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "abc123",
          "patchVersion": "19z",
          "refCounts": null,
          "rowKey": {
            "id": "456",
          },
          "rowVersion": "03",
          "schema": "public",
          "table": "issues",
        },
        {
          "clientGroupID": "abc123",
          "patchVersion": "1ba",
          "refCounts": {
            "twoHash": 1,
          },
          "rowKey": {
            "id": "321",
          },
          "rowVersion": "03",
          "schema": "public",
          "table": "issues",
        },
        {
          "clientGroupID": "abc123",
          "patchVersion": "1aa:01",
          "refCounts": {
            "twoHash": 1,
          },
          "rowKey": {
            "id": "123",
          },
          "rowVersion": "03",
          "schema": "public",
          "table": "issues",
        },
      ]
    `);

    expect(updated).toEqual({
      ...cvr,
      version: newVersion,
      queries: {},
      lastActive: 1713834000000,
    } satisfies CVRSnapshot);

    // Verify round tripping.
    const doCVRStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await doCVRStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(updated);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          lastActive: new Date('2024-04-23T01:00:00Z').getTime(),
          version: '1ba:01',
          replicaVersion: '123',
          owner: 'my-task',
          grantedAt: 1709251200000,
          clientSchema: null,
        },
      ],
      clients: [],
      queries: [
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: true,
          internal: null,
          patchVersion: '189',
          queryHash: 'already-deleted',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: true,
          internal: null,
          patchVersion: '19z',
          queryHash: 'catchup-delete',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          deleted: true,
          queryArgs: null,
          queryName: null,
          internal: null,
          patchVersion: '1ba:01',
          queryHash: 'oneHash',
          transformationHash: null,
          transformationVersion: null,
        },
      ],
      desires: [],
      rows: [
        {
          clientGroupID: 'abc123',
          patchVersion: '189',
          refCounts: null,
          rowKey: IN_OLD_PATCH_ROW_KEY,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '19z',
          refCounts: null,
          rowKey: DELETE_ROW_KEY,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1ba',
          refCounts: {
            twoHash: 1,
          },
          rowKey: ROW_KEY2,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1aa:01',
          refCounts: {
            twoHash: 1,
          },
          rowKey: ROW_KEY1,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1ba:01',
          refCounts: null,
          rowKey: ROW_KEY3,
          rowVersion: '09',
          schema: 'public',
          table: 'issues',
        },
      ],
    });
  });

  test('unchanged queries', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1ba',
          replicaVersion: '120',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: false,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          transformationHash: 'serverOneHash',
          queryArgs: null,
          queryName: null,
          transformationVersion: '1aa',
          patchVersion: '1aa:01',
          internal: null,
          deleted: null,
        },
        {
          clientGroupID: 'abc123',
          queryHash: 'twoHash',
          clientAST: {table: 'issues'},
          transformationHash: 'serverTwoHash',
          queryArgs: null,
          queryName: null,
          transformationVersion: '1aa',
          patchVersion: '1aa:01',
          internal: null,
          deleted: null,
        },
        {
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          queryHash: 'already-deleted',
          clientAST: {table: 'issues'},
          patchVersion: '189',
          transformationHash: null,
          transformationVersion: null,
          internal: null,
          deleted: true,
        },
        {
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          queryHash: 'catchup-delete',
          clientAST: {table: 'issues'},
          patchVersion: '19z',
          transformationHash: null,
          transformationVersion: null,
          internal: null,
          deleted: true,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'twoHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          patchVersion: '189',
          rowKey: IN_OLD_PATCH_ROW_KEY,
          rowVersion: '03',
          refCounts: null,
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1ba',
          rowKey: DELETE_ROW_KEY,
          rowVersion: '03',
          refCounts: null,
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1aa:01',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: {
            oneHash: 1,
            twoHash: 1,
          },
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1a0',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {twoHash: 1},
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1aa:01',
          rowKey: ROW_KEY3,
          rowVersion: '09',
          refCounts: {oneHash: 1},
          schema: 'public',
          table: 'issues',
        },
      ],
    };

    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    expect(cvr).toMatchInlineSnapshot(`
      {
        "clientSchema": null,
        "clients": {
          "fooClient": {
            "desiredQueryIDs": [
              "oneHash",
              "twoHash",
            ],
            "id": "fooClient",
          },
        },
        "id": "abc123",
        "lastActive": 1713830400000,
        "queries": {
          "oneHash": {
            "ast": {
              "table": "issues",
            },
            "clientState": {
              "fooClient": {
                "inactivatedAt": undefined,
                "ttl": -1,
                "version": {
                  "minorVersion": 1,
                  "stateVersion": "1a9",
                },
              },
            },
            "id": "oneHash",
            "patchVersion": {
              "minorVersion": 1,
              "stateVersion": "1aa",
            },
            "transformationHash": "serverOneHash",
            "transformationVersion": {
              "stateVersion": "1aa",
            },
            "type": "client",
          },
          "twoHash": {
            "ast": {
              "table": "issues",
            },
            "clientState": {
              "fooClient": {
                "inactivatedAt": undefined,
                "ttl": -1,
                "version": {
                  "minorVersion": 1,
                  "stateVersion": "1a9",
                },
              },
            },
            "id": "twoHash",
            "patchVersion": {
              "minorVersion": 1,
              "stateVersion": "1aa",
            },
            "transformationHash": "serverTwoHash",
            "transformationVersion": {
              "stateVersion": "1aa",
            },
            "type": "client",
          },
        },
        "replicaVersion": "120",
        "version": {
          "stateVersion": "1ba",
        },
      }
    `);
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba', '120');

    const {newVersion, queryPatches} = updater.trackQueries(
      lc,
      [
        {id: 'oneHash', transformationHash: 'serverOneHash'},
        {id: 'twoHash', transformationHash: 'serverTwoHash'},
      ],
      [],
    );
    expect(newVersion).toEqual({stateVersion: '1ba'});
    expect(queryPatches).toHaveLength(0);

    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '03',
              refCounts: {oneHash: 1},
              contents: {id: 'existing-patch'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1aa', minorVersion: 1},
        patch: {
          type: 'row',
          op: 'put',
          id: ROW_ID1,
          contents: {id: 'existing-patch'},
        },
      },
    ] satisfies PatchToVersion[]);
    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '03',
              refCounts: {twoHash: 1},
              contents: {id: 'existing-patch'},
            },
          ],
        ]),
      ),
    ).toEqual([
      // deduped
    ]);
    await updater.received(
      lc,
      new Map([
        [
          ROW_ID3,
          {
            version: '09',
            refCounts: {oneHash: 1},
            contents: {
              /* ignored */
            },
          },
        ],
      ]),
    );
    await updater.received(
      lc,
      new Map([
        [
          ROW_ID2,
          {
            version: '03',
            refCounts: {twoHash: 1},
            contents: {
              /* ignored */
            },
          },
        ],
      ]),
    );

    expect(await updater.deleteUnreferencedRows()).toEqual([]);

    const {cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 1),
    );
    expect(flushed).toBe(false);

    expect(
      await cvrStore.catchupConfigPatches(
        lc,
        {stateVersion: '189'},
        cvr,
        updated.version,
      ),
    ).toMatchInlineSnapshot(`
      [
        {
          "patch": {
            "id": "catchup-delete",
            "op": "del",
            "type": "query",
          },
          "toVersion": {
            "stateVersion": "19z",
          },
        },
        {
          "patch": {
            "id": "oneHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        },
        {
          "patch": {
            "id": "twoHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        },
        {
          "patch": {
            "clientID": "fooClient",
            "id": "oneHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1a9",
          },
        },
        {
          "patch": {
            "clientID": "fooClient",
            "id": "twoHash",
            "op": "put",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1a9",
          },
        },
      ]
    `);

    expect(
      await catchupRows(cvrStore, {stateVersion: '189'}, cvr, updated.version, [
        'oneHash',
        'twoHash',
      ]),
    ).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "abc123",
          "patchVersion": "1ba",
          "refCounts": null,
          "rowKey": {
            "id": "456",
          },
          "rowVersion": "03",
          "schema": "public",
          "table": "issues",
        },
      ]
    `);

    expect(updated).toEqual(cvr);

    // Verify round tripping.
    const doCVRStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await doCVRStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(updated);

    // await expectStorage(storage, {
    //   ...initialState,
    //   ['/vs/cvr/abc123/m/lastActive']: {
    //     epochMillis: Date.UTC(2024, 3, 23, 1),
    //   } satisfies LastActive,
    // });
  });

  test('row key changed', async () => {
    const ROW_KEY4 = {id: 999};
    const NEW_ROW_KEY1 = {newID: '1foo'};
    const NEW_ROW_KEY3 = {newID: '3baz'};
    const NEW_ROW_KEY4 = {newID: 'voo'};

    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1ba',
          replicaVersion: '123',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: 'serverOneHash',
          transformationVersion: '1aa',
          patchVersion: '1aa:01',
          internal: null,
          deleted: null,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1aa:01',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY4,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
      ],
    };

    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1bb', '123');

    const {newVersion, queryPatches} = updater.trackQueries(
      lc,
      [{id: 'oneHash', transformationHash: 'serverOneHash'}],
      [],
    );
    expect(newVersion).toEqual({stateVersion: '1bb'});
    expect(queryPatches).toHaveLength(0);

    // NEW_ROW_KEY1 should replace ROW_KEY1
    expect(
      await updater.received(
        lc,
        new Map([
          [
            {...ROW_TABLE, rowKey: NEW_ROW_KEY1},
            {
              version: '03',
              refCounts: {oneHash: 1},
              contents: {...ROW_KEY1, ...NEW_ROW_KEY1, value: 'foobar'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1aa', minorVersion: 1},
        patch: {
          type: 'row',
          op: 'put',
          id: {...ROW_TABLE, rowKey: NEW_ROW_KEY1},
          contents: {...ROW_KEY1, ...NEW_ROW_KEY1, value: 'foobar'},
        },
      },
    ] satisfies PatchToVersion[]);

    // NEW_ROW_KEY3 is new to this CVR.
    expect(
      await updater.received(
        lc,
        new Map([
          [
            {...ROW_TABLE, rowKey: NEW_ROW_KEY3},
            {
              version: '09',
              refCounts: {oneHash: 1},
              contents: {...ROW_KEY3, ...NEW_ROW_KEY3, value: 'barfoo'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: newVersion,
        patch: {
          type: 'row',
          op: 'put',
          id: {...ROW_TABLE, rowKey: NEW_ROW_KEY3},
          contents: {...ROW_KEY3, ...NEW_ROW_KEY3, value: 'barfoo'},
        },
      },
    ] satisfies PatchToVersion[]);

    // NEW_ROW_KEY4 gets added and removed, and should replace ROW_KEY4
    expect(
      await updater.received(
        lc,
        new Map([
          [
            {...ROW_TABLE, rowKey: NEW_ROW_KEY4},
            {
              version: '03',
              refCounts: {oneHash: 1},
              contents: {...ROW_KEY4, ...NEW_ROW_KEY4, value: 'voodoo'},
            },
          ],
          [
            {...ROW_TABLE, rowKey: NEW_ROW_KEY4},
            {
              version: '03',
              refCounts: {oneHash: -1},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1a0'},
        patch: {
          type: 'row',
          op: 'put',
          id: {...ROW_TABLE, rowKey: NEW_ROW_KEY4},
          contents: {...ROW_KEY4, ...NEW_ROW_KEY4, value: 'voodoo'},
        },
      },
      {
        toVersion: {stateVersion: '1bb'},
        patch: {
          type: 'row',
          op: 'del',
          id: {...ROW_TABLE, rowKey: NEW_ROW_KEY4},
        },
      },
    ] satisfies PatchToVersion[]);

    // Note: ROW_ID2 was not received so it is deleted.
    // ROW_ID1, on the other hand was recognized with the NEW_ROW_KEY1
    // and so it is not deleted.
    expect(await updater.deleteUnreferencedRows()).toEqual([
      {
        patch: {type: 'row', op: 'del', id: ROW_ID2},
        toVersion: newVersion,
      },
    ] satisfies PatchToVersion[]);

    const {cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 1),
    );
    expect(flushed).toMatchInlineSnapshot(`
      {
        "clients": 0,
        "desires": 0,
        "instances": 1,
        "queries": 0,
        "rows": 6,
        "rowsDeferred": 0,
        "statements": 5,
      }
    `);

    // Verify round tripping.
    const doCVRStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await doCVRStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(updated);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          lastActive: new Date('2024-04-23T01:00:00Z').getTime(),
          version: '1bb',
          replicaVersion: '123',
          owner: 'my-task',
          grantedAt: 1709251200000,
          clientSchema: null,
        },
      ],
      clients: initialState.clients,
      queries: [
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
          queryArgs: null,
          queryName: null,
          deleted: null,
          internal: null,
          patchVersion: '1aa:01',
          queryHash: 'oneHash',
          transformationHash: 'serverOneHash',
          transformationVersion: '1aa',
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          deleted: null,
          patchVersion: '1a9:01',
          queryHash: 'oneHash',
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        // Note: All the state from the previous ROW_KEY1 remains.
        {
          clientGroupID: 'abc123',
          patchVersion: '1aa:01',
          refCounts: {oneHash: 1},
          rowKey: NEW_ROW_KEY1,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1bb',
          refCounts: null, // Deleted
          rowKey: ROW_KEY2,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          patchVersion: '1bb',
          refCounts: {oneHash: 1},
          rowKey: NEW_ROW_KEY3,
          rowVersion: '09',
          schema: 'public',
          table: 'issues',
        },
        // NEW_ROW_KEY4 should added as deleted row, ensuring that
        // the delete is computed when catching up old clients.
        {
          clientGroupID: 'abc123',
          patchVersion: '1bb',
          refCounts: null,
          rowKey: NEW_ROW_KEY4,
          rowVersion: '03',
          schema: 'public',
          table: 'issues',
        },
      ],
    });
  });

  test('advance with delete that cancels out add', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1aa',
          replicaVersion: '120',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: null,
          transformationVersion: null,
          patchVersion: null,
          internal: null,
          deleted: null,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
      ],
    };

    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba', '120');

    const newVersion = updater.updatedVersion();
    expect(newVersion).toEqual({
      stateVersion: '1ba',
    });

    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '04',
              refCounts: {oneHash: 0},
              contents: {id: 'should-show-up-in-patch'},
            },
          ],
          [
            ROW_ID3,
            {
              version: '01',
              refCounts: {oneHash: 0},
              contents: {id: 'should-not-show-up-in-patch'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1ba'},
        patch: {
          type: 'row',
          op: 'put',
          id: ROW_ID1,
          contents: {id: 'should-show-up-in-patch'},
        },
      },
    ] satisfies PatchToVersion[]);

    // Same last active day (no index change), but different hour.
    const {cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 1),
    );
    expect(flushed).toMatchInlineSnapshot(`
      {
        "clients": 0,
        "desires": 0,
        "instances": 1,
        "queries": 0,
        "rows": 1,
        "rowsDeferred": 0,
        "statements": 3,
      }
    `);

    // Verify round tripping.
    const cvrStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await cvrStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(updated);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1ba',
          replicaVersion: '120',
          lastActive: Date.UTC(2024, 3, 23, 1),
          owner: 'my-task',
          grantedAt: 1709251200000,
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: null,
          transformationVersion: null,
          patchVersion: null,
          internal: null,
          deleted: null,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '04',
          refCounts: {oneHash: 1},
          patchVersion: '1ba',
          schema: 'public',
          table: 'issues',
        },
      ],
    });
  });

  test('advance with add and delete+delete of existing row', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1aa',
          replicaVersion: '120',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: null,
          transformationVersion: null,
          patchVersion: null,
          internal: null,
          deleted: null,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
      ],
    };

    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba', '120');

    const newVersion = updater.updatedVersion();
    expect(newVersion).toEqual({
      stateVersion: '1ba',
    });

    expect(
      await updater.received(
        lc,
        new Map([
          // An update with a negative refCount AND version+contents.
          // This happens if a row-add is batched with multiple row-deletes.
          [
            ROW_ID1,
            {
              version: '03',
              refCounts: {oneHash: -1},
              contents: {id: 'should-be-deleted-with-new-patch-version'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1ba'},
        patch: {
          type: 'row',
          op: 'del',
          id: ROW_ID1,
        },
      },
    ] satisfies PatchToVersion[]);

    // Same last active day (no index change), but different hour.
    const {cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 1),
    );
    expect(flushed).toMatchInlineSnapshot(`
      {
        "clients": 0,
        "desires": 0,
        "instances": 1,
        "queries": 0,
        "rows": 1,
        "rowsDeferred": 0,
        "statements": 3,
      }
    `);

    // Verify round tripping.
    const cvrStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await cvrStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(updated);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1ba',
          replicaVersion: '120',
          lastActive: Date.UTC(2024, 3, 23, 1),
          owner: 'my-task',
          grantedAt: 1709251200000,
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: null,
          transformationVersion: null,
          patchVersion: null,
          internal: null,
          deleted: null,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: null,
          patchVersion: '1ba',
          schema: 'public',
          table: 'issues',
        },
      ],
    });
  });

  test('advance with adds and different versions of a row', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1aa',
          replicaVersion: '120',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: null,
          transformationVersion: null,
          patchVersion: null,
          internal: null,
          deleted: null,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
      ],
    };

    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba', '120');

    const newVersion = updater.updatedVersion();
    expect(newVersion).toEqual({
      stateVersion: '1ba',
    });

    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '03',
              refCounts: {oneHash: 0},
              contents: {id: 'old-view-of-row'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1a0'},
        patch: {
          type: 'row',
          op: 'put',
          id: ROW_ID1,
          contents: {id: 'old-view-of-row'},
        },
      },
    ] satisfies PatchToVersion[]);

    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '04',
              refCounts: {oneHash: 0},
              contents: {id: 'new-view-of-row'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1ba'},
        patch: {
          type: 'row',
          op: 'put',
          id: ROW_ID1,
          contents: {id: 'new-view-of-row'},
        },
      },
    ] satisfies PatchToVersion[]);

    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '03',
              refCounts: {oneHash: 0},
              contents: {id: 'old-view-of-row'},
            },
          ],
        ]),
      ),
    ).toEqual([
      // suppressed - doesn't go backwards
    ]);

    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '04',
              refCounts: {oneHash: 0},
              contents: {id: 'new-view-of-row'},
            },
          ],
        ]),
      ),
    ).toEqual([
      // deduped
    ]);

    // Same last active day (no index change), but different hour.
    const {cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 1),
    );
    expect(flushed).toMatchInlineSnapshot(`
      {
        "clients": 0,
        "desires": 0,
        "instances": 1,
        "queries": 0,
        "rows": 1,
        "rowsDeferred": 0,
        "statements": 3,
      }
    `);

    // Verify round tripping.
    const cvrStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await cvrStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(updated);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1ba',
          replicaVersion: '120',
          lastActive: Date.UTC(2024, 3, 23, 1),
          owner: 'my-task',
          grantedAt: 1709251200000,
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: null,
          transformationVersion: null,
          patchVersion: null,
          internal: null,
          deleted: null,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '04',
          refCounts: {oneHash: 1},
          patchVersion: '1ba',
          schema: 'public',
          table: 'issues',
        },
      ],
    });
  });

  test('advance with delete and re-add existing row', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1aa',
          replicaVersion: '120',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: null,
          transformationVersion: null,
          patchVersion: null,
          internal: null,
          deleted: null,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
      ],
    };

    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba', '120');

    const newVersion = updater.updatedVersion();
    expect(newVersion).toEqual({
      stateVersion: '1ba',
    });

    expect(
      await updater.received(
        lc,
        new Map([[ROW_ID1, {refCounts: {oneHash: -1}}]]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1ba'},
        patch: {
          type: 'row',
          op: 'del',
          id: ROW_ID1,
        },
      },
    ] satisfies PatchToVersion[]);

    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '03',
              refCounts: {oneHash: 1},
              contents: {id: 'im-back'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        // Note: The toVersion needs to be bumped, even though the row
        //       technically has not changed, in order to override the
        //       row-del that was previously sent.
        toVersion: {stateVersion: '1ba'},
        patch: {
          type: 'row',
          op: 'put',
          id: ROW_ID1,
          contents: {id: 'im-back'},
        },
      },
    ] satisfies PatchToVersion[]);

    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID1,
            {
              version: '03',
              refCounts: {oneHash: 0},
              contents: {id: 'im-back'},
            },
          ],
        ]),
      ),
    ).toEqual([
      // deduped
    ]);

    // No net change to the CVR expected.
    const {flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 1),
    );
    expect(flushed).toBe(false);
  });

  test('advance with add and delete of new row', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1aa',
          replicaVersion: '120',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          patchVersion: '1a9:01',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: null,
          transformationVersion: null,
          patchVersion: null,
          internal: null,
          deleted: null,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
      ],
    };

    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba', '120');

    const newVersion = updater.updatedVersion();
    expect(newVersion).toEqual({stateVersion: '1ba'});

    expect(
      await updater.received(
        lc,
        new Map([
          [
            ROW_ID3,
            {
              version: '01',
              refCounts: {oneHash: 1},
              contents: {id: '123'},
            },
          ],
        ]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1ba'},
        patch: {
          type: 'row',
          op: 'put',
          id: ROW_ID3,
          contents: {id: '123'},
        },
      },
    ] satisfies PatchToVersion[]);

    expect(
      await updater.received(
        lc,
        new Map([[ROW_ID3, {refCounts: {oneHash: -1}}]]),
      ),
    ).toEqual([
      {
        toVersion: {stateVersion: '1ba'},
        patch: {
          type: 'row',
          op: 'del',
          id: ROW_ID3,
        },
      },
    ] satisfies PatchToVersion[]);

    // Same last active day (no index change), but different hour.
    const {flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 1),
    );
    expect(flushed).toBe(false);

    // Verify round tripping.
    const cvrStore2 = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const reloaded = await cvrStore2.load(lc, LAST_CONNECT);
    expect(reloaded).toEqual(cvr);

    await expectState(db, {
      ...initialState,
      instances: [
        {
          ...initialState.instances[0],
          owner: 'my-task',
          grantedAt: 1709251200000,
        },
      ],
    });
  });

  describe('markDesiredQueryAsInactive', () => {
    test('no ttl', async () => {
      const now = Date.UTC(2025, 2, 18);

      const initialState: DBState = {
        instances: [
          {
            clientGroupID: 'abc123',
            version: '1aa',
            replicaVersion: '120',
            lastActive: now,
            clientSchema: null,
          },
        ],
        clients: [
          {
            clientGroupID: 'abc123',
            clientID: 'fooClient',
            patchVersion: '1a9:01',
            deleted: null,
          },
        ],
        queries: [
          {
            clientGroupID: 'abc123',
            queryHash: 'oneHash',
            clientAST: {table: 'issues'},
            queryArgs: null,
            queryName: null,
            transformationHash: null,
            transformationVersion: null,
            patchVersion: null,
            internal: null,
            deleted: null,
          },
        ],
        desires: [
          {
            clientGroupID: 'abc123',
            clientID: 'fooClient',
            queryHash: 'oneHash',
            patchVersion: '1a9:01',
            deleted: null,
            inactivatedAt: null,
            ttl: null,
          },
        ],
        rows: [
          {
            clientGroupID: 'abc123',
            rowKey: ROW_KEY1,
            rowVersion: '03',
            refCounts: {oneHash: 1},
            patchVersion: '1a0',
            schema: 'public',
            table: 'issues',
          },
          {
            clientGroupID: 'abc123',
            rowKey: ROW_KEY2,
            rowVersion: '03',
            refCounts: {oneHash: 1},
            patchVersion: '1a0',
            schema: 'public',
            table: 'issues',
          },
        ],
      };

      await setInitialState(db, initialState);

      const cvrStore = new CVRStore(
        lc,
        db,
        SHARD,
        'my-task',
        'abc123',
        ON_FAILURE,
      );
      const cvr = await cvrStore.load(lc, LAST_CONNECT);
      expect(cvr).toMatchInlineSnapshot(`
        {
          "clientSchema": null,
          "clients": {
            "fooClient": {
              "desiredQueryIDs": [
                "oneHash",
              ],
              "id": "fooClient",
            },
          },
          "id": "abc123",
          "lastActive": 1742256000000,
          "queries": {
            "oneHash": {
              "ast": {
                "table": "issues",
              },
              "clientState": {
                "fooClient": {
                  "inactivatedAt": undefined,
                  "ttl": -1,
                  "version": {
                    "minorVersion": 1,
                    "stateVersion": "1a9",
                  },
                },
              },
              "id": "oneHash",
              "patchVersion": undefined,
              "transformationHash": undefined,
              "transformationVersion": undefined,
              "type": "client",
            },
          },
          "replicaVersion": "120",
          "version": {
            "stateVersion": "1aa",
          },
        }
      `);

      const updater = new CVRConfigDrivenUpdater(cvrStore, cvr, SHARD);
      updater.markDesiredQueriesAsInactive('fooClient', ['oneHash'], now);

      const {cvr: updated} = await updater.flush(lc, LAST_CONNECT, now);
      expect(updated).toEqual({
        clients: {
          fooClient: {
            desiredQueryIDs: [],
            id: 'fooClient',
          },
        },
        id: 'abc123',
        lastActive: now,
        queries: {
          oneHash: {
            type: 'client',
            ast: {
              table: 'issues',
            },
            clientState: {
              fooClient: {
                inactivatedAt: now,
                ttl: -1,
                version: {
                  minorVersion: 1,
                  stateVersion: '1aa',
                },
              },
            },
            id: 'oneHash',
            patchVersion: undefined,
            transformationHash: undefined,
            transformationVersion: undefined,
          },
        },
        replicaVersion: '120',
        version: {
          minorVersion: 1,
          stateVersion: '1aa',
        },
        clientSchema: null,
      } satisfies CVRSnapshot);
    });

    test('with ttl', async () => {
      const now = Date.UTC(2025, 2, 18);
      const ttl = 10_000;

      const initialState: DBState = {
        instances: [
          {
            clientGroupID: 'abc123',
            version: '1aa',
            replicaVersion: '120',
            lastActive: now,
            clientSchema: null,
          },
        ],
        clients: [
          {
            clientGroupID: 'abc123',
            clientID: 'fooClient',
            patchVersion: '1a9:01',
            deleted: null,
          },
        ],
        queries: [
          {
            clientGroupID: 'abc123',
            queryHash: 'oneHash',
            clientAST: {table: 'issues'},
            queryArgs: null,
            queryName: null,
            transformationHash: null,
            transformationVersion: null,
            patchVersion: null,
            internal: null,
            deleted: null,
          },
        ],
        desires: [
          {
            clientGroupID: 'abc123',
            clientID: 'fooClient',
            queryHash: 'oneHash',
            patchVersion: '1a9:01',
            deleted: null,
            inactivatedAt: null,
            ttl: ttl / 1000,
          },
        ],
        rows: [
          {
            clientGroupID: 'abc123',
            rowKey: ROW_KEY1,
            rowVersion: '03',
            refCounts: {oneHash: 1},
            patchVersion: '1a0',
            schema: 'public',
            table: 'issues',
          },
          {
            clientGroupID: 'abc123',
            rowKey: ROW_KEY2,
            rowVersion: '03',
            refCounts: {oneHash: 1},
            patchVersion: '1a0',
            schema: 'public',
            table: 'issues',
          },
        ],
      };

      await setInitialState(db, initialState);

      const cvrStore = new CVRStore(
        lc,
        db,
        SHARD,
        'my-task',
        'abc123',
        ON_FAILURE,
      );
      const cvr = await cvrStore.load(lc, LAST_CONNECT);
      expect(cvr.queries).toEqual({
        oneHash: {
          ast: {
            table: 'issues',
          },
          clientState: {
            fooClient: {
              inactivatedAt: undefined,
              ttl,
              version: {
                minorVersion: 1,
                stateVersion: '1a9',
              },
            },
          },
          type: 'client',
          id: 'oneHash',
          patchVersion: undefined,
          transformationHash: undefined,
          transformationVersion: undefined,
        },
      });

      const updater = new CVRConfigDrivenUpdater(cvrStore, cvr, SHARD);
      updater.markDesiredQueriesAsInactive('fooClient', ['oneHash'], now);

      const {cvr: updated} = await updater.flush(lc, LAST_CONNECT, now);
      expect(updated.queries).toEqual({
        oneHash: {
          ast: {
            table: 'issues',
          },
          type: 'client',
          clientState: {
            fooClient: {
              inactivatedAt: now,
              ttl,
              version: {
                minorVersion: 1,
                stateVersion: '1aa',
              },
            },
          },
          id: 'oneHash',
          patchVersion: undefined,
          transformationHash: undefined,
          transformationVersion: undefined,
        },
      });
    });

    test('no ttl, got', async () => {
      const now = Date.UTC(2025, 2, 18);

      const initialState: DBState = {
        instances: [
          {
            clientGroupID: 'abc123',
            version: '1aa',
            replicaVersion: '120',
            lastActive: now,
            clientSchema: null,
          },
        ],
        clients: [
          {
            clientGroupID: 'abc123',
            clientID: 'fooClient',
            patchVersion: '1a9:01',
            deleted: null,
          },
        ],
        queries: [
          {
            clientGroupID: 'abc123',
            queryHash: 'oneHash',
            clientAST: {table: 'issues'},
            queryArgs: null,
            queryName: null,
            transformationHash: 'oneHashTransformed',
            transformationVersion: '1a9:01',
            patchVersion: null,
            internal: null,
            deleted: null,
          },
        ],
        desires: [
          {
            clientGroupID: 'abc123',
            clientID: 'fooClient',
            queryHash: 'oneHash',
            patchVersion: '1a9:01',
            deleted: null,
            inactivatedAt: null,
            ttl: null,
          },
        ],
        rows: [
          {
            clientGroupID: 'abc123',
            rowKey: ROW_KEY1,
            rowVersion: '03',
            refCounts: {oneHash: 1},
            patchVersion: '1a0',
            schema: 'public',
            table: 'issues',
          },
          {
            clientGroupID: 'abc123',
            rowKey: ROW_KEY2,
            rowVersion: '03',
            refCounts: {oneHash: 1},
            patchVersion: '1a0',
            schema: 'public',
            table: 'issues',
          },
        ],
      };

      await setInitialState(db, initialState);

      const cvrStore = new CVRStore(
        lc,
        db,
        SHARD,
        'my-task',
        'abc123',
        ON_FAILURE,
      );
      const cvr = await cvrStore.load(lc, LAST_CONNECT);
      expect(cvr).toMatchInlineSnapshot(`
          {
            "clientSchema": null,
            "clients": {
              "fooClient": {
                "desiredQueryIDs": [
                  "oneHash",
                ],
                "id": "fooClient",
              },
            },
            "id": "abc123",
            "lastActive": 1742256000000,
            "queries": {
              "oneHash": {
                "ast": {
                  "table": "issues",
                },
                "clientState": {
                  "fooClient": {
                    "inactivatedAt": undefined,
                    "ttl": -1,
                    "version": {
                      "minorVersion": 1,
                      "stateVersion": "1a9",
                    },
                  },
                },
                "id": "oneHash",
                "patchVersion": undefined,
                "transformationHash": "oneHashTransformed",
                "transformationVersion": {
                  "minorVersion": 1,
                  "stateVersion": "1a9",
                },
                "type": "client",
              },
            },
            "replicaVersion": "120",
            "version": {
              "stateVersion": "1aa",
            },
          }
        `);

      const updater = new CVRConfigDrivenUpdater(cvrStore, cvr, SHARD);
      expect(
        updater.markDesiredQueriesAsInactive('fooClient', ['oneHash'], now),
      ).toMatchInlineSnapshot(`
        [
          {
            "patch": {
              "clientID": "fooClient",
              "id": "oneHash",
              "op": "del",
              "type": "query",
            },
            "toVersion": {
              "minorVersion": 1,
              "stateVersion": "1aa",
            },
          },
        ]
      `);

      const {cvr: updated} = await updater.flush(lc, LAST_CONNECT, now);
      expect(updated).toEqual({
        clients: {
          fooClient: {
            desiredQueryIDs: [],
            id: 'fooClient',
          },
        },
        id: 'abc123',
        lastActive: now,
        queries: {
          oneHash: {
            type: 'client',
            ast: {
              table: 'issues',
            },
            clientState: {
              fooClient: {
                inactivatedAt: now,
                ttl: -1,
                version: {
                  minorVersion: 1,
                  stateVersion: '1aa',
                },
              },
            },
            id: 'oneHash',
            patchVersion: undefined,
            transformationHash: 'oneHashTransformed',
            transformationVersion: {
              minorVersion: 1,
              stateVersion: '1a9',
            },
          },
        },
        replicaVersion: '120',
        version: {
          minorVersion: 1,
          stateVersion: '1aa',
        },
        clientSchema: null,
      } satisfies CVRSnapshot);
    });

    test('using negative numbers for ttl', async () => {
      // Negative number are treated as no ttl/forever.
      const now = Date.UTC(2025, 2, 18);

      const initialState: DBState = {
        instances: [
          {
            clientGroupID: 'abc123',
            version: '1aa',
            replicaVersion: '120',
            lastActive: now,
            clientSchema: null,
          },
        ],
        clients: [
          {
            clientGroupID: 'abc123',
            clientID: 'fooClient',
            patchVersion: '1a9:01',
            deleted: null,
          },
        ],
        queries: [
          {
            clientGroupID: 'abc123',
            queryHash: 'oneHash',
            clientAST: {table: 'issues'},
            queryArgs: null,
            queryName: null,
            transformationHash: null,
            transformationVersion: null,
            patchVersion: null,
            internal: null,
            deleted: null,
          },
        ],
        desires: [
          {
            clientGroupID: 'abc123',
            clientID: 'fooClient',
            queryHash: 'oneHash',
            patchVersion: '1a9:01',
            deleted: null,
            inactivatedAt: null,
            ttl: 10 / 1000,
          },
        ],
        rows: [],
      };

      await setInitialState(db, initialState);

      const cvrStore = new CVRStore(
        lc,
        db,
        SHARD,
        'my-task',
        'abc123',
        ON_FAILURE,
      );
      const cvr = await cvrStore.load(lc, LAST_CONNECT);
      expect(cvr.clients.fooClient).toMatchInlineSnapshot(`
        {
          "desiredQueryIDs": [
            "oneHash",
          ],
          "id": "fooClient",
        }
      `);
      expect((cvr.queries.oneHash as ClientQueryRecord).clientState.fooClient)
        .toMatchInlineSnapshot(`
          {
            "inactivatedAt": undefined,
            "ttl": 10,
            "version": {
              "minorVersion": 1,
              "stateVersion": "1a9",
            },
          }
        `);

      const updater = new CVRConfigDrivenUpdater(cvrStore, cvr, SHARD);
      expect(
        updater.putDesiredQueries('fooClient', [
          {hash: 'oneHash', ast: {table: 'issues'}, ttl: -1},
        ]),
      ).toMatchInlineSnapshot(`
        [
          {
            "patch": {
              "clientID": "fooClient",
              "id": "oneHash",
              "op": "put",
              "type": "query",
            },
            "toVersion": {
              "minorVersion": 1,
              "stateVersion": "1aa",
            },
          },
        ]
      `);
      const {cvr: updated} = await updater.flush(lc, LAST_CONNECT, now);
      expect(
        (updated.queries.oneHash as ClientQueryRecord).clientState.fooClient,
      ).toMatchInlineSnapshot(`
        {
          "inactivatedAt": undefined,
          "ttl": -1,
          "version": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        }
      `);
    });
  });

  test('deleteClient', async () => {
    vi.setSystemTime(Date.UTC(2024, 2, 6));
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1aa',
          replicaVersion: '120',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'client-a',
          patchVersion: '1aa',
          deleted: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'client-b',
          patchVersion: '1aa',
          deleted: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'client-c',
          patchVersion: '1aa',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: null,
          transformationVersion: null,
          patchVersion: null,
          internal: null,
          deleted: null,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'client-a',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'client-b',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'client-c',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: {oneHash: 3},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {oneHash: 3},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
      ],
    };

    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);
    const updater = new CVRConfigDrivenUpdater(cvrStore, cvr, SHARD);

    expect(updater.deleteClient('client-b')).toMatchInlineSnapshot(`
      [
        {
          "patch": {
            "clientID": "client-b",
            "id": "oneHash",
            "op": "del",
            "type": "query",
          },
          "toVersion": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        },
      ]
    `);

    const {cvr: updated, flushed} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.now(),
    );
    expect(updated).toMatchInlineSnapshot(`
        {
          "clientSchema": null,
          "clients": {
            "client-a": {
              "desiredQueryIDs": [
                "oneHash",
              ],
              "id": "client-a",
            },
            "client-c": {
              "desiredQueryIDs": [
                "oneHash",
              ],
              "id": "client-c",
            },
          },
          "id": "abc123",
          "lastActive": 1709683200000,
          "queries": {
            "oneHash": {
              "ast": {
                "table": "issues",
              },
              "clientState": {
                "client-a": {
                  "inactivatedAt": undefined,
                  "ttl": -1,
                  "version": {
                    "minorVersion": 1,
                    "stateVersion": "1a9",
                  },
                },
                "client-b": {
                  "inactivatedAt": 1709683200000,
                  "ttl": -1,
                  "version": {
                    "minorVersion": 1,
                    "stateVersion": "1aa",
                  },
                },
                "client-c": {
                  "inactivatedAt": undefined,
                  "ttl": -1,
                  "version": {
                    "minorVersion": 1,
                    "stateVersion": "1a9",
                  },
                },
              },
              "id": "oneHash",
              "patchVersion": undefined,
              "transformationHash": undefined,
              "transformationVersion": undefined,
              "type": "client",
            },
          },
          "replicaVersion": "120",
          "version": {
            "minorVersion": 1,
            "stateVersion": "1aa",
          },
        }
      `);
    expect(flushed).toMatchInlineSnapshot(`
      {
        "clients": 1,
        "desires": 1,
        "instances": 1,
        "queries": 1,
        "rows": 0,
        "rowsDeferred": 0,
        "statements": 5,
      }
    `);

    expect(await getAllState(db)).toMatchInlineSnapshot(`
      {
        "clients": Result [
          {
            "clientGroupID": "abc123",
            "clientID": "client-a",
            "deleted": null,
            "patchVersion": "1aa",
          },
          {
            "clientGroupID": "abc123",
            "clientID": "client-c",
            "deleted": null,
            "patchVersion": "1aa",
          },
        ],
        "desires": Result [
          {
            "clientGroupID": "abc123",
            "clientID": "client-a",
            "deleted": null,
            "inactivatedAt": null,
            "patchVersion": "1a9:01",
            "queryHash": "oneHash",
            "ttl": null,
          },
          {
            "clientGroupID": "abc123",
            "clientID": "client-c",
            "deleted": null,
            "inactivatedAt": null,
            "patchVersion": "1a9:01",
            "queryHash": "oneHash",
            "ttl": null,
          },
          {
            "clientGroupID": "abc123",
            "clientID": "client-b",
            "deleted": true,
            "inactivatedAt": 1709683200000,
            "patchVersion": "1aa:01",
            "queryHash": "oneHash",
            "ttl": null,
          },
        ],
        "instances": Result [
          {
            "clientGroupID": "abc123",
            "clientSchema": null,
            "grantedAt": 1709251200000,
            "lastActive": 1709683200000,
            "owner": "my-task",
            "replicaVersion": "120",
            "version": "1aa:01",
          },
        ],
        "queries": Result [
          {
            "clientAST": {
              "table": "issues",
            },
            "clientGroupID": "abc123",
            "deleted": false,
            "internal": null,
            "patchVersion": null,
            "queryArgs": null,
            "queryHash": "oneHash",
            "queryName": null,
            "transformationHash": null,
            "transformationVersion": null,
          },
        ],
        "rows": Result [
          {
            "clientGroupID": "abc123",
            "patchVersion": "1a0",
            "refCounts": {
              "oneHash": 3,
            },
            "rowKey": {
              "id": "123",
            },
            "rowVersion": "03",
            "schema": "public",
            "table": "issues",
          },
          {
            "clientGroupID": "abc123",
            "patchVersion": "1a0",
            "refCounts": {
              "oneHash": 3,
            },
            "rowKey": {
              "id": "321",
            },
            "rowVersion": "03",
            "schema": "public",
            "table": "issues",
          },
        ],
      }
    `);
  });

  test('deleteClient from other group', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1aa',
          replicaVersion: '120',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },

        {
          clientGroupID: 'def456',
          version: '1aa',
          replicaVersion: '120',
          lastActive: Date.UTC(2024, 3, 23),
          clientSchema: null,
        },
      ],
      clients: [
        {
          clientGroupID: 'abc123',
          clientID: 'client-a',
          patchVersion: '1aa',
          deleted: null,
        },
        {
          clientGroupID: 'def456',
          clientID: 'client-b',
          patchVersion: '1aa',
          deleted: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'client-c',
          patchVersion: '1aa',
          deleted: null,
        },
      ],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: null,
          transformationVersion: null,
          patchVersion: null,
          internal: null,
          deleted: null,
        },
        {
          clientGroupID: 'def456',
          queryHash: 'oneHash',
          clientAST: {table: 'issues'},
          queryArgs: null,
          queryName: null,
          transformationHash: null,
          transformationVersion: null,
          patchVersion: null,
          internal: null,
          deleted: null,
        },
      ],
      desires: [
        {
          clientGroupID: 'abc123',
          clientID: 'client-a',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
        {
          clientGroupID: 'def456',
          clientID: 'client-b',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
        {
          clientGroupID: 'abc123',
          clientID: 'client-c',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: null,
          inactivatedAt: null,
          ttl: null,
        },
      ],
      rows: [
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: {oneHash: 2},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'abc123',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {oneHash: 2},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'def456',
          rowKey: ROW_KEY1,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
        {
          clientGroupID: 'def456',
          rowKey: ROW_KEY2,
          rowVersion: '03',
          refCounts: {oneHash: 1},
          patchVersion: '1a0',
          schema: 'public',
          table: 'issues',
        },
      ],
    };

    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(
      lc,
      db,
      SHARD,
      'my-task',
      'abc123',
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, LAST_CONNECT);

    expect(cvr).toMatchInlineSnapshot(`
        {
          "clientSchema": null,
          "clients": {
            "client-a": {
              "desiredQueryIDs": [
                "oneHash",
              ],
              "id": "client-a",
            },
            "client-c": {
              "desiredQueryIDs": [
                "oneHash",
              ],
              "id": "client-c",
            },
          },
          "id": "abc123",
          "lastActive": 1713830400000,
          "queries": {
            "oneHash": {
              "ast": {
                "table": "issues",
              },
              "clientState": {
                "client-a": {
                  "inactivatedAt": undefined,
                  "ttl": -1,
                  "version": {
                    "minorVersion": 1,
                    "stateVersion": "1a9",
                  },
                },
                "client-c": {
                  "inactivatedAt": undefined,
                  "ttl": -1,
                  "version": {
                    "minorVersion": 1,
                    "stateVersion": "1a9",
                  },
                },
              },
              "id": "oneHash",
              "patchVersion": undefined,
              "transformationHash": undefined,
              "transformationVersion": undefined,
              "type": "client",
            },
          },
          "replicaVersion": "120",
          "version": {
            "stateVersion": "1aa",
          },
        }
      `);

    const updater = new CVRConfigDrivenUpdater(cvrStore, cvr, SHARD);

    // No patches because client-b is from a different group.
    expect(updater.deleteClient('client-b')).toEqual([]);

    const {cvr: updated} = await updater.flush(
      lc,
      LAST_CONNECT,
      Date.UTC(2024, 3, 23, 1),
    );

    expect(await getAllState(db)).toMatchInlineSnapshot(`
      {
        "clients": Result [
          {
            "clientGroupID": "abc123",
            "clientID": "client-a",
            "deleted": null,
            "patchVersion": "1aa",
          },
          {
            "clientGroupID": "abc123",
            "clientID": "client-c",
            "deleted": null,
            "patchVersion": "1aa",
          },
        ],
        "desires": Result [
          {
            "clientGroupID": "abc123",
            "clientID": "client-a",
            "deleted": null,
            "inactivatedAt": null,
            "patchVersion": "1a9:01",
            "queryHash": "oneHash",
            "ttl": null,
          },
          {
            "clientGroupID": "def456",
            "clientID": "client-b",
            "deleted": null,
            "inactivatedAt": null,
            "patchVersion": "1a9:01",
            "queryHash": "oneHash",
            "ttl": null,
          },
          {
            "clientGroupID": "abc123",
            "clientID": "client-c",
            "deleted": null,
            "inactivatedAt": null,
            "patchVersion": "1a9:01",
            "queryHash": "oneHash",
            "ttl": null,
          },
        ],
        "instances": Result [
          {
            "clientGroupID": "def456",
            "clientSchema": null,
            "grantedAt": null,
            "lastActive": 1713830400000,
            "owner": null,
            "replicaVersion": "120",
            "version": "1aa",
          },
          {
            "clientGroupID": "abc123",
            "clientSchema": null,
            "grantedAt": 1709251200000,
            "lastActive": 1713834000000,
            "owner": "my-task",
            "replicaVersion": "120",
            "version": "1aa",
          },
        ],
        "queries": Result [
          {
            "clientAST": {
              "table": "issues",
            },
            "clientGroupID": "abc123",
            "deleted": null,
            "internal": null,
            "patchVersion": null,
            "queryArgs": null,
            "queryHash": "oneHash",
            "queryName": null,
            "transformationHash": null,
            "transformationVersion": null,
          },
          {
            "clientAST": {
              "table": "issues",
            },
            "clientGroupID": "def456",
            "deleted": null,
            "internal": null,
            "patchVersion": null,
            "queryArgs": null,
            "queryHash": "oneHash",
            "queryName": null,
            "transformationHash": null,
            "transformationVersion": null,
          },
        ],
        "rows": Result [
          {
            "clientGroupID": "abc123",
            "patchVersion": "1a0",
            "refCounts": {
              "oneHash": 2,
            },
            "rowKey": {
              "id": "123",
            },
            "rowVersion": "03",
            "schema": "public",
            "table": "issues",
          },
          {
            "clientGroupID": "abc123",
            "patchVersion": "1a0",
            "refCounts": {
              "oneHash": 2,
            },
            "rowKey": {
              "id": "321",
            },
            "rowVersion": "03",
            "schema": "public",
            "table": "issues",
          },
          {
            "clientGroupID": "def456",
            "patchVersion": "1a0",
            "refCounts": {
              "oneHash": 1,
            },
            "rowKey": {
              "id": "123",
            },
            "rowVersion": "03",
            "schema": "public",
            "table": "issues",
          },
          {
            "clientGroupID": "def456",
            "patchVersion": "1a0",
            "refCounts": {
              "oneHash": 1,
            },
            "rowKey": {
              "id": "321",
            },
            "rowVersion": "03",
            "schema": "public",
            "table": "issues",
          },
        ],
      }
    `);

    expect(updated).toMatchInlineSnapshot(`
              {
                "clientSchema": null,
                "clients": {
                  "client-a": {
                    "desiredQueryIDs": [
                      "oneHash",
                    ],
                    "id": "client-a",
                  },
                  "client-c": {
                    "desiredQueryIDs": [
                      "oneHash",
                    ],
                    "id": "client-c",
                  },
                },
                "id": "abc123",
                "lastActive": 1713834000000,
                "queries": {
                  "oneHash": {
                    "ast": {
                      "table": "issues",
                    },
                    "clientState": {
                      "client-a": {
                        "inactivatedAt": undefined,
                        "ttl": -1,
                        "version": {
                          "minorVersion": 1,
                          "stateVersion": "1a9",
                        },
                      },
                      "client-c": {
                        "inactivatedAt": undefined,
                        "ttl": -1,
                        "version": {
                          "minorVersion": 1,
                          "stateVersion": "1a9",
                        },
                      },
                    },
                    "id": "oneHash",
                    "patchVersion": undefined,
                    "transformationHash": undefined,
                    "transformationVersion": undefined,
                    "type": "client",
                  },
                },
                "replicaVersion": "120",
                "version": {
                  "stateVersion": "1aa",
                },
              }
            `);

    {
      const cvr = await cvrStore.load(lc, LAST_CONNECT);
      const updater = new CVRConfigDrivenUpdater(cvrStore, cvr, SHARD);

      updater.deleteClientGroup('def456');
      const {cvr: updated2} = await updater.flush(
        lc,
        LAST_CONNECT,
        Date.UTC(2024, 3, 23, 1),
      );

      expect(updated2).toEqual(updated);

      expect(await getAllState(db)).toMatchInlineSnapshot(`
        {
          "clients": Result [
            {
              "clientGroupID": "abc123",
              "clientID": "client-a",
              "deleted": null,
              "patchVersion": "1aa",
            },
            {
              "clientGroupID": "abc123",
              "clientID": "client-c",
              "deleted": null,
              "patchVersion": "1aa",
            },
          ],
          "desires": Result [
            {
              "clientGroupID": "abc123",
              "clientID": "client-a",
              "deleted": null,
              "inactivatedAt": null,
              "patchVersion": "1a9:01",
              "queryHash": "oneHash",
              "ttl": null,
            },
            {
              "clientGroupID": "abc123",
              "clientID": "client-c",
              "deleted": null,
              "inactivatedAt": null,
              "patchVersion": "1a9:01",
              "queryHash": "oneHash",
              "ttl": null,
            },
          ],
          "instances": Result [
            {
              "clientGroupID": "abc123",
              "clientSchema": null,
              "grantedAt": 1709251200000,
              "lastActive": 1713834000000,
              "owner": "my-task",
              "replicaVersion": "120",
              "version": "1aa",
            },
          ],
          "queries": Result [
            {
              "clientAST": {
                "table": "issues",
              },
              "clientGroupID": "abc123",
              "deleted": null,
              "internal": null,
              "patchVersion": null,
              "queryArgs": null,
              "queryHash": "oneHash",
              "queryName": null,
              "transformationHash": null,
              "transformationVersion": null,
            },
          ],
          "rows": Result [
            {
              "clientGroupID": "abc123",
              "patchVersion": "1a0",
              "refCounts": {
                "oneHash": 2,
              },
              "rowKey": {
                "id": "123",
              },
              "rowVersion": "03",
              "schema": "public",
              "table": "issues",
            },
            {
              "clientGroupID": "abc123",
              "patchVersion": "1a0",
              "refCounts": {
                "oneHash": 2,
              },
              "rowKey": {
                "id": "321",
              },
              "rowVersion": "03",
              "schema": "public",
              "table": "issues",
            },
          ],
        }
      `);
    }
  });
});
