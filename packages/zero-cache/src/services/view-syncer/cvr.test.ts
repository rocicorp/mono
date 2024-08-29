import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterEach, beforeEach, describe, test} from 'vitest';
import {testDBs} from '../../test/db.js';
import type {PostgresDB} from '../../types/pg.js';
import {CVRStore} from './cvr-store.js';

import {expect} from 'vitest';
import type {PatchToVersion} from './client-handler.js';
import {
  CVRConfigDrivenUpdater,
  CVRQueryDrivenUpdater,
  CVRSnapshot,
  CVRUpdater,
} from './cvr.js';
import {
  ClientsRow,
  DesiresRow,
  InstancesRow,
  QueriesRow,
  RowsRow,
  setupCVRTables,
} from './schema/cvr.js';
import type {RowID} from './schema/types.js';

describe('view-syncer/cvr', () => {
  type DBState = {
    instances: InstancesRow[];
    clients: ClientsRow[];
    queries: QueriesRow[];
    desires: DesiresRow[];
    rows: RowsRow[];
  };

  function setInitialState(
    db: PostgresDB,
    state: Partial<DBState>,
  ): Promise<void> {
    return db.begin(async tx => {
      for (const [table, rows] of Object.entries(state)) {
        for (const row of rows) {
          await tx`INSERT INTO ${tx('cvr.' + table)} ${tx(row)}`;
        }
      }
    });
  }

  async function expectState(db: PostgresDB, state: Partial<DBState>) {
    for (const table of Object.keys(state)) {
      const res = await db`SELECT * FROM ${db('cvr.' + table)}`;
      expect(res).toEqual(state[table as keyof DBState]);
    }
  }

  async function getAllState(db: PostgresDB): Promise<DBState> {
    const [instances, clients, queries, desires, rows] = await Promise.all([
      db`SELECT * FROM ${db('cvr.instances')}`,
      db`SELECT * FROM ${db('cvr.clients')}`,
      db`SELECT * FROM ${db('cvr.queries')}`,
      db`SELECT * FROM ${db('cvr.desires')}`,
      db`SELECT * FROM ${db('cvr.rows')}`,
    ]);
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

  beforeEach(async () => {
    db = await testDBs.create('cvr_test_db');
    await db.begin(tx => setupCVRTables(lc, tx));
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  test('load first time cvr', async () => {
    const pgStore = new CVRStore(lc, db, 'abc123');

    const cvr = await pgStore.load();
    expect(cvr).toEqual({
      id: 'abc123',
      version: {stateVersion: '00'},
      lastActive: {epochMillis: 0},
      clients: {},
      queries: {},
    } satisfies CVRSnapshot);
    const flushed = await new CVRUpdater(pgStore, cvr).flush(
      lc,
      new Date(Date.UTC(2024, 3, 20)),
    );

    expect(flushed).toEqual({
      ...cvr,
      lastActive: {epochMillis: 1713571200000},
    } satisfies CVRSnapshot);

    // Verify round tripping.
    const pgStore2 = new CVRStore(lc, db, 'abc123');
    const reloaded = await pgStore2.load();
    expect(reloaded).toEqual(flushed);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '00',
          lastActive: new Date(Date.UTC(2024, 3, 20)),
        },
      ],
      clients: [],
      queries: [],
      desires: [],
    });
  });

  test('load existing cvr', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1a9:02',
          lastActive: new Date(Date.UTC(2024, 3, 23)),
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
        },
      ],
      rows: [],
    };
    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(lc, db, 'abc123');

    const cvr = await cvrStore.load();
    expect(cvr).toEqual({
      id: 'abc123',
      version: {stateVersion: '1a9', minorVersion: 2},
      lastActive: {epochMillis: 1713830400000},
      clients: {
        fooClient: {
          id: 'fooClient',
          desiredQueryIDs: ['oneHash'],
          patchVersion: {stateVersion: '1a9', minorVersion: 1},
        },
      },
      queries: {
        ['oneHash']: {
          id: 'oneHash',
          ast: {table: 'issues'},
          transformationHash: 'twoHash',
          desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
          patchVersion: {stateVersion: '1a9', minorVersion: 2},
        },
      },
    } satisfies CVRSnapshot);

    await expectState(db, initialState);
  });

  test('update active time', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1a9:02',
          lastActive: new Date(Date.UTC(2024, 3, 23)),
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
        },
      ],
      rows: [],
    };
    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(lc, db, 'abc123');
    const cvr = await cvrStore.load();
    const updater = new CVRUpdater(cvrStore, cvr);

    const updated = await updater.flush(lc, new Date(Date.UTC(2024, 3, 24)));

    expect(cvr).toEqual({
      id: 'abc123',
      version: {stateVersion: '1a9', minorVersion: 2},
      lastActive: {epochMillis: 1713830400000},
      clients: {
        fooClient: {
          id: 'fooClient',
          desiredQueryIDs: ['oneHash'],
          patchVersion: {stateVersion: '1a9', minorVersion: 1},
        },
      },
      queries: {
        oneHash: {
          id: 'oneHash',
          ast: {table: 'issues'},
          transformationHash: 'twoHash',
          desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
          patchVersion: {stateVersion: '1a9', minorVersion: 2},
        },
      },
    } satisfies CVRSnapshot);

    expect(updated).toEqual({
      ...cvr,
      lastActive: {epochMillis: 1713916800000},
    } satisfies CVRSnapshot);

    // Verify round tripping.
    const cvrStore2 = new CVRStore(lc, db, 'abc123');
    const reloaded = await cvrStore2.load();
    expect(reloaded).toEqual(updated);

    const updatedState = structuredClone(initialState);
    updatedState.instances[0].lastActive = new Date(Date.UTC(2024, 3, 24));
    await expectState(db, updatedState);
  });

  test('update desired query set', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1aa',
          lastActive: new Date(Date.UTC(2024, 3, 23)),
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
        },
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'oneHash',
          patchVersion: '1a9:01',
          deleted: false,
        },
      ],
      rows: [],
    };
    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(lc, db, 'abc123');
    const cvr = await cvrStore.load();
    expect(cvr).toEqual({
      id: 'abc123',
      version: {stateVersion: '1aa'},
      lastActive: {epochMillis: 1713830400000},
      clients: {
        dooClient: {
          id: 'dooClient',
          desiredQueryIDs: ['oneHash'],
          patchVersion: {stateVersion: '1a8'},
        },
        fooClient: {
          id: 'fooClient',
          desiredQueryIDs: ['oneHash'],
          patchVersion: {stateVersion: '1a9', minorVersion: 1},
        },
      },
      queries: {
        oneHash: {
          id: 'oneHash',
          ast: {table: 'issues'},
          transformationHash: 'twoHash',
          transformationVersion: undefined,
          desiredBy: {
            dooClient: {stateVersion: '1a8'},
            fooClient: {stateVersion: '1a9', minorVersion: 1},
          },
          patchVersion: {stateVersion: '1a9', minorVersion: 2},
        },
      },
    } satisfies CVRSnapshot);

    const updater = new CVRConfigDrivenUpdater(cvrStore, cvr);

    // This removes and adds desired queries to the existing fooClient.
    updater.deleteDesiredQueries('fooClient', ['oneHash', 'twoHash']);
    expect(
      updater.putDesiredQueries('fooClient', {
        fourHash: {table: 'users'},
        threeHash: {table: 'comments'},
      }),
    ).toEqual([
      {id: 'fourHash', ast: {table: 'users'}},
      {id: 'threeHash', ast: {table: 'comments'}},
    ]);
    // This adds a new barClient with desired queries.
    expect(
      updater.putDesiredQueries('barClient', {
        oneHash: {table: 'issues'},
        threeHash: {table: 'comments'},
      }),
    ).toEqual([
      {id: 'oneHash', ast: {table: 'issues'}},
      {id: 'threeHash', ast: {table: 'comments'}},
    ]);
    // Adds a new client with no desired queries.
    expect(updater.putDesiredQueries('bonkClient', {})).toEqual([]);
    updater.clearDesiredQueries('dooClient');

    const updated = await updater.flush(lc, new Date(Date.UTC(2024, 3, 24)));

    expect(updated).toEqual({
      id: 'abc123',
      version: {stateVersion: '1aa', minorVersion: 1}, // minorVersion bump
      lastActive: {epochMillis: 1713916800000},
      clients: {
        barClient: {
          id: 'barClient',
          desiredQueryIDs: ['oneHash', 'threeHash'],
          patchVersion: {stateVersion: '1aa', minorVersion: 1},
        },
        bonkClient: {
          id: 'bonkClient',
          desiredQueryIDs: [],
          patchVersion: {stateVersion: '1aa', minorVersion: 1},
        },
        dooClient: {
          desiredQueryIDs: [],
          id: 'dooClient',
          patchVersion: {stateVersion: '1a8'},
        },
        fooClient: {
          id: 'fooClient',
          desiredQueryIDs: ['fourHash', 'threeHash'],
          patchVersion: {stateVersion: '1a9', minorVersion: 1},
        },
      },
      queries: {
        lmids: {
          id: 'lmids',
          internal: true,
          ast: {
            table: 'zero.clients',
            schema: '',
            where: [
              {
                type: 'simple',
                op: '=',
                field: 'clientGroupID',
                value: 'abc123',
              },
            ],
            orderBy: [['clientID', 'asc']],
          },
        },
        oneHash: {
          id: 'oneHash',
          ast: {table: 'issues'},
          transformationHash: 'twoHash',
          transformationVersion: undefined,
          desiredBy: {barClient: {stateVersion: '1aa', minorVersion: 1}},
          patchVersion: {stateVersion: '1a9', minorVersion: 2},
        },
        threeHash: {
          id: 'threeHash',
          ast: {table: 'comments'},
          desiredBy: {
            barClient: {stateVersion: '1aa', minorVersion: 1},
            fooClient: {stateVersion: '1aa', minorVersion: 1},
          },
        },
        fourHash: {
          id: 'fourHash',
          ast: {table: 'users'},
          desiredBy: {fooClient: {stateVersion: '1aa', minorVersion: 1}},
        },
      },
    } satisfies CVRSnapshot);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          lastActive: new Date('2024-04-24T00:00:00.000Z'),
          version: '1aa:01',
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
          deleted: false,
          internal: null,
          patchVersion: null,
          queryHash: 'fourHash',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'comments',
          },
          clientGroupID: 'abc123',
          deleted: false,
          internal: null,
          patchVersion: null,
          queryHash: 'threeHash',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            schema: '',
            table: 'zero.clients',
            where: [
              {
                field: 'clientGroupID',
                op: '=',
                type: 'simple',
                value: 'abc123',
              },
            ],
            orderBy: [['clientID', 'asc']],
          },
          clientGroupID: 'abc123',
          deleted: false,
          internal: true,
          patchVersion: null,
          queryHash: 'lmids',
          transformationHash: null,
          transformationVersion: null,
        },
        {
          clientAST: {
            table: 'issues',
          },
          clientGroupID: 'abc123',
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
        },
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          deleted: false,
          patchVersion: '1aa:01',
          queryHash: 'fourHash',
        },
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          deleted: false,
          patchVersion: '1aa:01',
          queryHash: 'threeHash',
        },
        {
          clientGroupID: 'abc123',
          clientID: 'barClient',
          deleted: false,
          patchVersion: '1aa:01',
          queryHash: 'oneHash',
        },
        {
          clientGroupID: 'abc123',
          clientID: 'barClient',
          deleted: false,
          patchVersion: '1aa:01',
          queryHash: 'threeHash',
        },
        {
          clientGroupID: 'abc123',
          clientID: 'dooClient',
          deleted: true,
          patchVersion: '1aa:01',
          queryHash: 'oneHash',
        },
      ],

      //  rows: [],
    });

    // Verify round tripping.
    const cvrStore2 = new CVRStore(lc, db, 'abc123');
    const reloaded = await cvrStore2.load();
    expect(reloaded).toEqual(updated);
  });

  test('no-op change to desired query set', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1aa',
          lastActive: new Date(Date.UTC(2024, 3, 23)),
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
        },
      ],
      rows: [],
    };
    await setInitialState(db, initialState);

    const cvrStore = new CVRStore(lc, db, 'abc123');
    const cvr = await cvrStore.load();
    const updater = new CVRConfigDrivenUpdater(cvrStore, cvr);

    // Same desired query set. Nothing should change except last active time.
    expect(
      updater.putDesiredQueries('fooClient', {oneHash: {table: 'issues'}}),
    ).toEqual([]);

    // Same last active day (no index change), but different hour.
    const updated = await updater.flush(lc, new Date(Date.UTC(2024, 3, 23, 1)));
    expect(updated).toEqual({
      ...cvr,
      lastActive: {epochMillis: 1713834000000},
    } satisfies CVRSnapshot);

    // Verify round tripping.
    const doCVRStore2 = new CVRStore(lc, db, 'abc123');
    const reloaded = await doCVRStore2.load();
    expect(reloaded).toEqual(updated);

    const updatedState = structuredClone(initialState);
    updatedState.instances[0].lastActive = new Date(Date.UTC(2024, 3, 23, 1));
    await expectState(db, updatedState);
  });

  const ROW_KEY1 = {id: '123'};
  const ROW_ID1: RowID = {
    schema: 'public',
    table: 'issues',
    rowKey: ROW_KEY1,
  };

  const ROW_KEY2 = {id: '321'};
  const ROW_ID2: RowID = {
    schema: 'public',
    table: 'issues',
    rowKey: ROW_KEY2,
  };

  const ROW_KEY3 = {id: '888'};
  const ROW_ID3: RowID = {
    schema: 'public',
    table: 'issues',
    rowKey: ROW_KEY3,
  };

  const DELETE_ROW_KEY = {id: '456'};
  const DELETED_ROW_ID: RowID = {
    schema: 'public',
    table: 'issues',
    rowKey: DELETE_ROW_KEY,
  };

  const IN_OLD_PATCH_ROW_KEY = {id: '777'};

  test('desired to got', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1aa',
          lastActive: new Date(Date.UTC(2024, 3, 23)),
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
          transformationHash: null,
          transformationVersion: null,
          patchVersion: null,
          internal: null,
          deleted: null,
        },
        {
          clientGroupID: 'abc123',
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

    const cvrStore = new CVRStore(lc, db, 'abc123');
    const cvr = await cvrStore.load();
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1aa');

    updater.trackQueries(
      lc,
      [{id: 'oneHash', transformationHash: 'serverOneHash'}],
      [],
      {stateVersion: '189'},
    );
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

    expect(await updater.deleteUnreferencedRows(lc)).toEqual([
      {
        patch: {type: 'row', op: 'del', id: DELETED_ROW_ID},
        toVersion: {stateVersion: '1aa'},
      },
    ] satisfies PatchToVersion[]);
    //  {
    // Catchup from v: "189" needs constrain / delete patches in ("189", "1aa"].
    // patchRows: [[{stateVersion: '1a0'}, ROW_ID2, ['id']]],
    // deleteRows: [[{stateVersion: '1aa'}, DELETED_ROW_ID]],
    // });

    const newVersion = {stateVersion: '1aa', minorVersion: 1};
    expect(await updater.generateConfigPatches(lc)).toEqual([
      {
        patch: {type: 'query', op: 'del', id: 'catchup-delete'},
        toVersion: {stateVersion: '19z'},
      },
      {
        patch: {
          type: 'query',
          op: 'put',
          id: 'oneHash',
          ast: {table: 'issues'},
        },
        toVersion: newVersion,
      },
    ]);

    // expect(updater.numPendingWrites()).toBe(11);

    // Same last active day (no index change), but different hour.
    const updated = await updater.flush(lc, new Date(Date.UTC(2024, 3, 23, 1)));
    expect(updated).toEqual({
      ...cvr,
      version: newVersion,
      queries: {
        oneHash: {
          id: 'oneHash',
          ast: {table: 'issues'},
          desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
          transformationHash: 'serverOneHash',
          transformationVersion: {stateVersion: '1aa', minorVersion: 1},
          patchVersion: {stateVersion: '1aa', minorVersion: 1},
        },
      },
      lastActive: {epochMillis: 1713834000000},
    } satisfies CVRSnapshot);

    // Verify round tripping.
    const cvrStore2 = new CVRStore(lc, db, 'abc123');
    const reloaded = await cvrStore2.load();
    expect(reloaded).toEqual(updated);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          lastActive: new Date('2024-04-23T01:00:00Z'),
          version: '1aa:01',
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

  test('new transformation hash', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1ba',
          lastActive: new Date(Date.UTC(2024, 3, 23)),
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
          transformationHash: 'serverOneHash',
          transformationVersion: '1aa',
          patchVersion: '1aa:01',
          internal: null,
          deleted: null,
        },
        {
          clientGroupID: 'abc123',
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

    const cvrStore = new CVRStore(lc, db, 'abc123');
    const cvr = await cvrStore.load();
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba');

    updater.trackQueries(
      lc,
      [{id: 'oneHash', transformationHash: 'serverTwoHash'}],
      [],
      {stateVersion: '189'},
    );
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

    const newVersion = {stateVersion: '1ba', minorVersion: 1};

    expect(await updater.deleteUnreferencedRows(lc)).toEqual([
      {
        patch: {type: 'row', op: 'del', id: ROW_ID3},
        toVersion: newVersion,
      },
      {
        patch: {type: 'row', op: 'del', id: DELETED_ROW_ID},
        toVersion: {stateVersion: '1ba'},
      },
    ] satisfies PatchToVersion[]);

    expect(await updater.generateConfigPatches(lc)).toEqual([
      {
        patch: {type: 'query', op: 'del', id: 'catchup-delete'},
        toVersion: {stateVersion: '19z'},
      },
    ] satisfies PatchToVersion[]);

    // expect(updater.numPendingWrites()).toBe(11);

    // Same last active day (no index change), but different hour.
    const updated = await updater.flush(lc, new Date(Date.UTC(2024, 3, 23, 1)));
    expect(updated).toEqual({
      ...cvr,
      version: newVersion,
      queries: {
        oneHash: {
          id: 'oneHash',
          ast: {table: 'issues'},
          desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
          transformationHash: 'serverTwoHash',
          transformationVersion: {stateVersion: '1ba', minorVersion: 1},
          patchVersion: {stateVersion: '1aa', minorVersion: 1},
        },
      },
      lastActive: {epochMillis: 1713834000000},
    } satisfies CVRSnapshot);

    // Verify round tripping.
    const doCVRStore2 = new CVRStore(lc, db, 'abc123');
    const reloaded = await doCVRStore2.load();
    expect(reloaded).toEqual(updated);

    expect(await getAllState(db)).toEqual({
      instances: [
        {
          clientGroupID: 'abc123',
          lastActive: new Date('2024-04-23T01:00:00Z'),
          version: '1ba:01',
        },
      ],
      clients: initialState.clients,
      queries: [
        {
          clientAST: {
            table: 'issues',
          },
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
          clientGroupID: 'abc123',
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
  });

  test('multiple executed queries', async () => {
    const initialState: DBState = {
      instances: [
        {
          clientGroupID: 'abc123',
          version: '1ba',
          lastActive: new Date(Date.UTC(2024, 3, 23)),
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
          transformationHash: 'serverOneHash',
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
          transformationVersion: '1aa',
          patchVersion: '1aa:01',
          internal: null,
          deleted: null,
        },
        {
          clientGroupID: 'abc123',
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
        },
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'twoHash',
          patchVersion: '1a9:01',
          deleted: null,
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

    const cvrStore = new CVRStore(lc, db, 'abc123');
    const cvr = await cvrStore.load();
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba');

    updater.trackQueries(
      lc,
      [
        {id: 'oneHash', transformationHash: 'updatedServerOneHash'},
        {id: 'twoHash', transformationHash: 'updatedServerTwoHash'},
      ],
      [],
      {stateVersion: '189'},
    );
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

    const newVersion = {stateVersion: '1ba', minorVersion: 1};

    expect(await updater.deleteUnreferencedRows(lc)).toEqual([
      {
        patch: {type: 'row', op: 'del', id: ROW_ID3},
        toVersion: newVersion,
      },
      {
        patch: {type: 'row', op: 'del', id: DELETED_ROW_ID},
        toVersion: {stateVersion: '1ba'},
      },
    ] satisfies PatchToVersion[]);
    expect(await updater.generateConfigPatches(lc)).toEqual([
      {
        patch: {type: 'query', op: 'del', id: 'catchup-delete'},
        toVersion: {stateVersion: '19z'},
      },
    ] satisfies PatchToVersion[]);

    // Same last active day (no index change), but different hour.
    const updated = await updater.flush(lc, new Date(Date.UTC(2024, 3, 23, 1)));
    expect(updated).toEqual({
      ...cvr,
      version: newVersion,
      lastActive: {epochMillis: 1713834000000},
      queries: {
        oneHash: {
          id: 'oneHash',
          ast: {table: 'issues'},
          desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
          transformationHash: 'updatedServerOneHash',
          transformationVersion: newVersion,
          patchVersion: {stateVersion: '1aa', minorVersion: 1},
        },
        twoHash: {
          id: 'twoHash',
          ast: {table: 'issues'},
          desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
          transformationHash: 'updatedServerTwoHash',
          transformationVersion: newVersion,
          patchVersion: {stateVersion: '1aa', minorVersion: 1},
        },
      },
    } satisfies CVRSnapshot);

    // Verify round tripping.
    const doCVRStore2 = new CVRStore(lc, db, 'abc123');
    const reloaded = await doCVRStore2.load();
    expect(reloaded).toEqual(updated);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          lastActive: new Date('2024-04-23T01:00:00Z'),
          version: '1ba:01',
        },
      ],
      clients: initialState.clients,
      queries: [
        {
          clientAST: {
            table: 'issues',
          },
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
          clientGroupID: 'abc123',
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
        },
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          deleted: null,
          patchVersion: '1a9:01',
          queryHash: 'twoHash',
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
          lastActive: new Date(Date.UTC(2024, 3, 23)),
        },
      ],
      clients: [],
      queries: [
        {
          clientGroupID: 'abc123',
          queryHash: 'oneHash',
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

    const cvrStore = new CVRStore(lc, db, 'abc123');
    const cvr = await cvrStore.load();
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba');

    updater.trackQueries(lc, [], ['oneHash'], {stateVersion: '189'});

    const newVersion = {stateVersion: '1ba', minorVersion: 1};
    expect(await updater.deleteUnreferencedRows(lc)).toEqual([
      {
        patch: {type: 'row', op: 'del', id: ROW_ID3},
        toVersion: newVersion,
      },
      {
        patch: {type: 'row', op: 'del', id: DELETED_ROW_ID},
        toVersion: {stateVersion: '19z'},
      },
    ] satisfies PatchToVersion[]);

    expect(await updater.generateConfigPatches(lc)).toEqual([
      {
        patch: {type: 'query', op: 'del', id: 'catchup-delete'},
        toVersion: {stateVersion: '19z'},
      },
      {
        patch: {type: 'query', op: 'del', id: 'oneHash'},
        toVersion: newVersion,
      },
    ] satisfies PatchToVersion[]);

    // expect(updater.numPendingWrites()).toBe(10);

    // Same last active day (no index change), but different hour.
    const updated = await updater.flush(lc, new Date(Date.UTC(2024, 3, 23, 1)));
    expect(updated).toEqual({
      ...cvr,
      version: newVersion,
      queries: {},
      lastActive: {epochMillis: 1713834000000},
    } satisfies CVRSnapshot);

    // Verify round tripping.
    const doCVRStore2 = new CVRStore(lc, db, 'abc123');
    const reloaded = await doCVRStore2.load();
    expect(reloaded).toEqual(updated);

    await expectState(db, {
      instances: [
        {
          clientGroupID: 'abc123',
          lastActive: new Date('2024-04-23T01:00:00Z'),
          version: '1ba:01',
        },
      ],
      clients: [],
      queries: [
        {
          clientAST: {
            table: 'issues',
          },
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
          clientGroupID: 'abc123',
          deleted: true,
          internal: null,
          patchVersion: '19z',
          queryHash: 'catchup-delete',
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
          patchVersion: '1ba:01',
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
          lastActive: new Date(Date.UTC(2024, 3, 23)),
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
          transformationVersion: '1aa',
          patchVersion: '1aa:01',
          internal: null,
          deleted: null,
        },
        {
          clientGroupID: 'abc123',
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
        },
        {
          clientGroupID: 'abc123',
          clientID: 'fooClient',
          queryHash: 'twoHash',
          patchVersion: '1a9:01',
          deleted: null,
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

    const cvrStore = new CVRStore(lc, db, 'abc123');
    const cvr = await cvrStore.load();
    expect(cvr).toMatchInlineSnapshot(`
      {
        "clients": {
          "fooClient": {
            "desiredQueryIDs": [
              "oneHash",
              "twoHash",
            ],
            "id": "fooClient",
            "patchVersion": {
              "minorVersion": 1,
              "stateVersion": "1a9",
            },
          },
        },
        "id": "abc123",
        "lastActive": {
          "epochMillis": 1713830400000,
        },
        "queries": {
          "oneHash": {
            "ast": {
              "table": "issues",
            },
            "desiredBy": {
              "fooClient": {
                "minorVersion": 1,
                "stateVersion": "1a9",
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
          },
          "twoHash": {
            "ast": {
              "table": "issues",
            },
            "desiredBy": {
              "fooClient": {
                "minorVersion": 1,
                "stateVersion": "1a9",
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
          },
        },
        "version": {
          "stateVersion": "1ba",
        },
      }
    `);
    const updater = new CVRQueryDrivenUpdater(cvrStore, cvr, '1ba');

    updater.trackQueries(
      lc,
      [
        {id: 'oneHash', transformationHash: 'serverOneHash'},
        {id: 'twoHash', transformationHash: 'serverTwoHash'},
      ],
      [],
      {stateVersion: '189'},
    );
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

    expect(new Set(await updater.deleteUnreferencedRows(lc))).toEqual(
      new Set([
        {
          patch: {type: 'row', op: 'del', id: DELETED_ROW_ID},
          toVersion: {stateVersion: '1ba'},
        },
      ] satisfies PatchToVersion[]),
    );
    expect(await updater.generateConfigPatches(lc)).toEqual([
      // {
      //   patch: {
      //     ast: {
      //       table: 'issues',
      //     },
      //     id: 'oneHash',
      //     op: 'put',
      //     type: 'query',
      //   },
      //   toVersion: {
      //     minorVersion: 1,
      //     stateVersion: '1aa',
      //   },
      // },
      // {
      //   patch: {
      //     ast: {
      //       table: 'issues',
      //     },
      //     id: 'twoHash',
      //     op: 'put',
      //     type: 'query',
      //   },
      //   toVersion: {
      //     minorVersion: 1,
      //     stateVersion: '1aa',
      //   },
      // },
      {
        patch: {type: 'query', op: 'del', id: 'catchup-delete'},
        toVersion: {stateVersion: '19z'},
      },
      {
        patch: {type: 'client', op: 'put', id: 'fooClient'},
        toVersion: {stateVersion: '1a9', minorVersion: 1},
      },
      // {
      //   patch: {
      //     ast: {
      //       table: 'issues',
      //     },
      //     clientID: 'fooClient',
      //     id: 'oneHash',
      //     op: 'put',
      //     type: 'query',
      //   },
      //   toVersion: {
      //     minorVersion: 1,
      //     stateVersion: '1a9',
      //   },
      // },
    ] satisfies PatchToVersion[]);

    // No writes!
    expect(updater.numPendingWrites()).toBe(0);

    // Only the last active time should change.
    const updated = await updater.flush(lc, new Date(Date.UTC(2024, 3, 23, 1)));
    expect(updated).toEqual({
      ...cvr,
      lastActive: {epochMillis: 1713834000000},
    } satisfies CVRSnapshot);

    // Verify round tripping.
    const doCVRStore2 = new CVRStore(lc, db, 'abc123');
    const reloaded = await doCVRStore2.load();
    expect(reloaded).toEqual(updated);

    // await expectStorage(storage, {
    //   ...initialState,
    //   ['/vs/cvr/abc123/m/lastActive']: {
    //     epochMillis: Date.UTC(2024, 3, 23, 1),
    //   } satisfies LastActive,
    // });
  });
});
