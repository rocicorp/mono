import {describe, expect, test} from 'vitest';
import {DurableStorage} from '../../storage/durable-storage.js';
import {
  expectStorage,
  initStorage,
  runWithDurableObjectStorage,
} from '../../test/do.js';
import {rowIDHash} from '../../types/row-key.js';
import {
  CVRConfigDrivenUpdater,
  CVRQueryDrivenUpdater,
  CVRSnapshot,
  CVRUpdater,
  loadCVR,
} from './cvr.js';
import type {
  CVRVersion,
  ClientPatch,
  ClientRecord,
  CvrID,
  LastActive,
  QueryPatch,
  QueryRecord,
  RowID,
  RowPatch,
  RowRecord,
} from './schema/types.js';

describe('view-syncer/cvr', () => {
  test('load first time cvr', async () => {
    await runWithDurableObjectStorage(async doStorage => {
      const storage = new DurableStorage(doStorage);
      const cvr = await loadCVR(storage, 'abc123');
      expect(cvr).toEqual({
        id: 'abc123',
        version: {stateVersion: '00'},
        lastActive: {epochMillis: 0},
        clients: {},
        queries: {},
      } satisfies CVRSnapshot);

      const flushed = await new CVRUpdater(storage, cvr).flush(
        new Date(Date.UTC(2024, 3, 20)),
      );

      expect(flushed).toEqual({
        ...cvr,
        lastActive: {epochMillis: 1713571200000},
      } satisfies CVRSnapshot);

      // Verify round tripping.
      const reloaded = await loadCVR(storage, 'abc123');
      expect(reloaded).toEqual(flushed);

      await expectStorage(doStorage, {
        ['/vs/cvr/abc123/m/lastActive']: flushed.lastActive,
        ['/vs/lastActive/2024-04-20/abc123']: {id: 'abc123'} satisfies CvrID,
      });
    });
  });

  test('load existing cvr', async () => {
    await runWithDurableObjectStorage(async storage => {
      await initStorage(storage, {
        ['/vs/cvr/abc123/m/version']: {
          stateVersion: '1a9',
          minorVersion: 2,
        } satisfies CVRVersion,
        ['/vs/cvr/abc123/m/lastActive']: {
          epochMillis: Date.UTC(2024, 3, 23),
        } satisfies LastActive,
        ['/vs/cvr/abc123/m/c/fooClient']: {
          id: 'fooClient',
          desiredQueryIDs: ['oneHash'],
          putPatch: {stateVersion: '1a9', minorVersion: 1},
        } satisfies ClientRecord,
        ['/vs/cvr/abc123/m/q/oneHash']: {
          id: 'oneHash',
          ast: {table: 'issues'},
          transformationHash: 'twoHash',
          desiredBy: {
            fooClient: {stateVersion: '1a9', minorVersion: 1},
          },
          putPatch: {stateVersion: '1a9', minorVersion: 2},
        } satisfies QueryRecord,
      });

      const cvr = await loadCVR(new DurableStorage(storage), 'abc123');
      expect(cvr).toEqual({
        id: 'abc123',
        version: {stateVersion: '1a9', minorVersion: 2},
        lastActive: {epochMillis: 1713830400000},
        clients: {
          fooClient: {
            id: 'fooClient',
            desiredQueryIDs: ['oneHash'],
            putPatch: {stateVersion: '1a9', minorVersion: 1},
          },
        },
        queries: {
          ['oneHash']: {
            id: 'oneHash',
            ast: {table: 'issues'},
            transformationHash: 'twoHash',
            desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
            putPatch: {stateVersion: '1a9', minorVersion: 2},
          },
        },
      } satisfies CVRSnapshot);
    });
  });

  test('update active time', async () => {
    await runWithDurableObjectStorage(async storage => {
      await initStorage(storage, {
        ['/vs/cvr/abc123/m/version']: {
          stateVersion: '1a9',
          minorVersion: 2,
        } satisfies CVRVersion,
        ['/vs/cvr/abc123/m/lastActive']: {
          epochMillis: Date.UTC(2024, 3, 23),
        } satisfies LastActive,
        ['/vs/cvr/abc123/m/c/fooClient']: {
          id: 'fooClient',
          desiredQueryIDs: ['oneHash'],
          putPatch: {stateVersion: '1a9', minorVersion: 1},
        } satisfies ClientRecord,
        ['/vs/cvr/abc123/m/q/oneHash']: {
          id: 'oneHash',
          ast: {table: 'issues'},
          transformationHash: 'twoHash',
          desiredBy: {
            fooClient: {stateVersion: '1a9', minorVersion: 1},
          },
          putPatch: {stateVersion: '1a9', minorVersion: 2},
        } satisfies QueryRecord,
        ['/vs/lastActive/2024-04-23/abc123']: {id: 'abc123'} satisfies CvrID,
      });

      const cvr = await loadCVR(new DurableStorage(storage), 'abc123');
      const updater = new CVRUpdater(new DurableStorage(storage), cvr);

      const updated = await updater.flush(new Date(Date.UTC(2024, 3, 24)));

      expect(cvr).toEqual({
        id: 'abc123',
        version: {stateVersion: '1a9', minorVersion: 2},
        lastActive: {epochMillis: 1713830400000},
        clients: {
          fooClient: {
            id: 'fooClient',
            desiredQueryIDs: ['oneHash'],
            putPatch: {stateVersion: '1a9', minorVersion: 1},
          },
        },
        queries: {
          oneHash: {
            id: 'oneHash',
            ast: {table: 'issues'},
            transformationHash: 'twoHash',
            desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
            putPatch: {stateVersion: '1a9', minorVersion: 2},
          },
        },
      } satisfies CVRSnapshot);

      expect(updated).toEqual({
        ...cvr,
        lastActive: {epochMillis: 1713916800000},
      } satisfies CVRSnapshot);

      // Verify round tripping.
      const reloaded = await loadCVR(new DurableStorage(storage), 'abc123');
      expect(reloaded).toEqual(updated);

      await expectStorage(storage, {
        ['/vs/cvr/abc123/m/version']: updated.version,
        ['/vs/cvr/abc123/m/lastActive']: updated.lastActive,
        ['/vs/cvr/abc123/m/c/fooClient']: updated.clients.fooClient,
        ['/vs/cvr/abc123/m/q/oneHash']: updated.queries.oneHash,
        // LastActive index
        ['/vs/lastActive/2024-04-24/abc123']: {id: 'abc123'} satisfies CvrID,
      });
    });
  });

  test('update desired query set', async () => {
    await runWithDurableObjectStorage(async storage => {
      await initStorage(storage, {
        ['/vs/cvr/abc123/m/version']: {
          stateVersion: '1aa',
        } satisfies CVRVersion,
        ['/vs/cvr/abc123/m/lastActive']: {
          epochMillis: Date.UTC(2024, 3, 23),
        } satisfies LastActive,
        ['/vs/cvr/abc123/m/c/dooClient']: {
          id: 'dooClient',
          desiredQueryIDs: ['oneHash', 'nonExistentQuery'],
          putPatch: {stateVersion: '1a8'},
        } satisfies ClientRecord,
        ['/vs/cvr/abc123/m/c/fooClient']: {
          id: 'fooClient',
          desiredQueryIDs: ['oneHash'],
          putPatch: {stateVersion: '1a9', minorVersion: 1},
        } satisfies ClientRecord,
        ['/vs/cvr/abc123/m/q/oneHash']: {
          id: 'oneHash',
          ast: {table: 'issues'},
          transformationHash: 'twoHash',
          desiredBy: {
            dooClient: {stateVersion: '1a8'},
            fooClient: {stateVersion: '1a9', minorVersion: 1},
          },
          putPatch: {stateVersion: '1a9', minorVersion: 2},
        } satisfies QueryRecord,
        ['/vs/cvr/abc123/p/m/1a8/q/oneHash/c/dooClient']: {
          type: 'query',
          op: 'put',
          id: 'oneHash',
          clientID: 'dooClient,',
        } satisfies QueryPatch,
        ['/vs/cvr/abc123/p/m/1a9.01/q/oneHash/c/fooClient']: {
          type: 'query',
          op: 'put',
          id: 'oneHash',
          clientID: 'fooClient,',
        } satisfies QueryPatch,
        ['/vs/lastActive/2024-04-23/abc123']: {id: 'abc123'} satisfies CvrID,
      });

      const cvr = await loadCVR(new DurableStorage(storage), 'abc123');
      const updater = new CVRConfigDrivenUpdater(
        new DurableStorage(storage),
        cvr,
      );

      // This removes and adds desired queries to the existing fooClient.
      updater.deleteDesiredQueries('fooClient', ['oneHash', 'twoHash']);
      expect(
        updater.putDesiredQueries('fooClient', {
          fourHash: {table: 'users'},
          threeHash: {table: 'comments'},
        }),
      ).toEqual([{table: 'users'}, {table: 'comments'}]);
      // This adds a new barClient with desired queries.
      expect(
        updater.putDesiredQueries('barClient', {
          oneHash: {table: 'issues'}, // oneHash is already "got", formerly desired by foo.
          threeHash: {table: 'comments'},
        }),
      ).toEqual([{table: 'issues'}, {table: 'comments'}]);
      // Adds a new client with no desired queries.
      expect(updater.putDesiredQueries('bonkClient', {})).toEqual([]);
      updater.clearDesiredQueries('dooClient');

      const updated = await updater.flush(new Date(Date.UTC(2024, 3, 24)));

      expect(cvr).toEqual({
        id: 'abc123',
        version: {stateVersion: '1aa'},
        lastActive: {epochMillis: 1713830400000},
        clients: {
          dooClient: {
            id: 'dooClient',
            desiredQueryIDs: ['oneHash', 'nonExistentQuery'],
            putPatch: {stateVersion: '1a8'},
          },
          fooClient: {
            id: 'fooClient',
            desiredQueryIDs: ['oneHash'],
            putPatch: {stateVersion: '1a9', minorVersion: 1},
          },
        },
        queries: {
          oneHash: {
            id: 'oneHash',
            ast: {table: 'issues'},
            transformationHash: 'twoHash',
            desiredBy: {
              dooClient: {stateVersion: '1a8'},
              fooClient: {stateVersion: '1a9', minorVersion: 1},
            },
            putPatch: {stateVersion: '1a9', minorVersion: 2},
          },
        },
      } satisfies CVRSnapshot);

      expect(updated).toEqual({
        id: 'abc123',
        version: {stateVersion: '1aa', minorVersion: 1}, // minorVersion bump
        lastActive: {epochMillis: 1713916800000},
        clients: {
          barClient: {
            id: 'barClient',
            desiredQueryIDs: ['oneHash', 'threeHash'],
            putPatch: {stateVersion: '1aa', minorVersion: 1},
          },
          bonkClient: {
            id: 'bonkClient',
            desiredQueryIDs: [],
            putPatch: {stateVersion: '1aa', minorVersion: 1},
          },
          dooClient: {
            id: 'dooClient',
            desiredQueryIDs: [],
            putPatch: {stateVersion: '1a8'},
          },
          fooClient: {
            id: 'fooClient',
            desiredQueryIDs: ['fourHash', 'threeHash'],
            putPatch: {stateVersion: '1a9', minorVersion: 1},
          },
        },
        queries: {
          oneHash: {
            id: 'oneHash',
            ast: {table: 'issues'},
            transformationHash: 'twoHash',
            desiredBy: {barClient: {stateVersion: '1aa', minorVersion: 1}},
            putPatch: {stateVersion: '1a9', minorVersion: 2},
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

      // Verify round tripping.
      const reloaded = await loadCVR(new DurableStorage(storage), 'abc123');
      expect(reloaded).toEqual(updated);

      await expectStorage(storage, {
        ['/vs/cvr/abc123/m/version']: updated.version,
        ['/vs/cvr/abc123/m/lastActive']: updated.lastActive,
        ['/vs/cvr/abc123/m/c/barClient']: updated.clients.barClient,
        ['/vs/cvr/abc123/m/c/bonkClient']: updated.clients.bonkClient,
        ['/vs/cvr/abc123/m/c/dooClient']: updated.clients.dooClient,
        ['/vs/cvr/abc123/m/c/fooClient']: updated.clients.fooClient,
        ['/vs/cvr/abc123/m/q/oneHash']: updated.queries.oneHash,
        ['/vs/cvr/abc123/m/q/threeHash']: updated.queries.threeHash,
        ['/vs/cvr/abc123/m/q/fourHash']: updated.queries.fourHash,
        // Patches!
        ['/vs/cvr/abc123/p/m/1aa.01/c/barClient']: {
          type: 'client',
          op: 'put',
          id: 'barClient',
        } satisfies ClientPatch,
        ['/vs/cvr/abc123/p/m/1aa.01/c/bonkClient']: {
          type: 'client',
          op: 'put',
          id: 'bonkClient',
        } satisfies ClientPatch,
        ['/vs/cvr/abc123/p/m/1aa.01/q/threeHash/c/barClient']: {
          type: 'query',
          op: 'put',
          id: 'threeHash',
          clientID: 'barClient',
        } satisfies QueryPatch,
        ['/vs/cvr/abc123/p/m/1aa.01/q/threeHash/c/fooClient']: {
          type: 'query',
          op: 'put',
          id: 'threeHash',
          clientID: 'fooClient',
        } satisfies QueryPatch,
        ['/vs/cvr/abc123/p/m/1aa.01/q/oneHash/c/barClient']: {
          type: 'query',
          op: 'put',
          id: 'oneHash',
          clientID: 'barClient',
        } satisfies QueryPatch,
        ['/vs/cvr/abc123/p/m/1aa.01/q/oneHash/c/dooClient']: {
          type: 'query',
          op: 'del', // The obsoleted 'put' patch at 1a9.01 is deleted too.
          id: 'oneHash',
          clientID: 'dooClient',
        } satisfies QueryPatch,
        ['/vs/cvr/abc123/p/m/1aa.01/q/oneHash/c/fooClient']: {
          type: 'query',
          op: 'del', // The obsoleted 'put' patch at 1a9.01 is deleted too.
          id: 'oneHash',
          clientID: 'fooClient',
        } satisfies QueryPatch,
        ['/vs/cvr/abc123/p/m/1aa.01/q/fourHash/c/fooClient']: {
          type: 'query',
          op: 'put',
          id: 'fourHash',
          clientID: 'fooClient',
        } satisfies QueryPatch,
        ['/vs/lastActive/2024-04-24/abc123']: {id: 'abc123'} satisfies CvrID,
      });
    });
  });

  test('no-op change to desired query set', async () => {
    const initialState = {
      ['/vs/cvr/abc123/m/version']: {
        stateVersion: '1aa',
      } satisfies CVRVersion,
      ['/vs/cvr/abc123/m/lastActive']: {
        epochMillis: Date.UTC(2024, 3, 23),
      } satisfies LastActive,
      ['/vs/cvr/abc123/m/c/fooClient']: {
        id: 'fooClient',
        desiredQueryIDs: ['oneHash'],
        putPatch: {stateVersion: '1a9', minorVersion: 1},
      } satisfies ClientRecord,
      ['/vs/cvr/abc123/m/q/oneHash']: {
        id: 'oneHash',
        ast: {table: 'issues'},
        transformationHash: 'twoHash',
        desiredBy: {
          fooClient: {stateVersion: '1a9', minorVersion: 1},
        },
        putPatch: {stateVersion: '1a9', minorVersion: 2},
      } satisfies QueryRecord,
      ['/vs/cvr/abc123/p/m/1a9.01/q/oneHash/c/fooClient']: {
        type: 'query',
        op: 'put',
        id: 'oneHash',
        clientID: 'fooClient,',
      } satisfies QueryPatch,
      ['/vs/lastActive/2024-04-23/abc123']: {id: 'abc123'} satisfies CvrID,
    };

    await runWithDurableObjectStorage(async storage => {
      await initStorage(storage, initialState);

      const cvr = await loadCVR(new DurableStorage(storage), 'abc123');
      const updater = new CVRConfigDrivenUpdater(
        new DurableStorage(storage),
        cvr,
      );

      // Same desired query set. Nothing should change except last active time.
      expect(
        updater.putDesiredQueries('fooClient', {oneHash: {table: 'issues'}}),
      ).toEqual([]);

      // Same last active day (no index change), but different hour.
      const updated = await updater.flush(new Date(Date.UTC(2024, 3, 23, 1)));
      expect(updated).toEqual({
        ...cvr,
        lastActive: {epochMillis: 1713834000000},
      } satisfies CVRSnapshot);

      // Verify round tripping.
      const reloaded = await loadCVR(new DurableStorage(storage), 'abc123');
      expect(reloaded).toEqual(updated);

      await expectStorage(storage, {
        ...initialState,
        ['/vs/cvr/abc123/m/lastActive']: {
          epochMillis: Date.UTC(2024, 3, 23, 1),
        } satisfies LastActive,
      });
    });
  });

  const ROW_ID1: RowID = {
    schema: 'public',
    table: 'issues',
    rowKey: {id: '123'},
  };
  const ROW_HASH1 = rowIDHash(ROW_ID1);
  const ROW_ID2: RowID = {
    schema: 'public',
    table: 'issues',
    rowKey: {id: '321'},
  };
  const ROW_HASH2 = rowIDHash(ROW_ID2);
  const ROW_ID3: RowID = {
    schema: 'public',
    table: 'issues',
    rowKey: {id: '888'},
  };
  const ROW_HASH3 = rowIDHash(ROW_ID3);

  test('desired to got', async () => {
    const initialState = {
      ['/vs/cvr/abc123/m/version']: {
        stateVersion: '1aa',
      } satisfies CVRVersion,
      ['/vs/cvr/abc123/m/lastActive']: {
        epochMillis: Date.UTC(2024, 3, 23),
      } satisfies LastActive,
      ['/vs/cvr/abc123/m/q/oneHash']: {
        id: 'oneHash',
        ast: {table: 'issues'},
        desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
      } satisfies QueryRecord,
      [`/vs/cvr/abc123/d/r/${ROW_HASH1}`]: {
        putPatch: {stateVersion: '1a0'},
        id: ROW_ID1,
        rowVersion: '03',
        queriedColumns: {id: ['twoHash']},
      } satisfies RowRecord,
      [`/vs/cvr/abc123/d/r/${ROW_HASH2}`]: {
        putPatch: {stateVersion: '1a0'},
        id: ROW_ID2,
        rowVersion: '03',
        queriedColumns: {id: ['twoHash']},
      } satisfies RowRecord,
      [`/vs/cvr/abc123/p/d/1a0/r/${ROW_HASH1}`]: {
        type: 'row',
        op: 'put',
        id: ROW_ID1,
        rowVersion: '03',
        columns: ['id'],
      } satisfies RowPatch,
      [`/vs/cvr/abc123/p/d/1a0/r/${ROW_HASH2}`]: {
        type: 'row',
        op: 'put',
        id: ROW_ID2,
        rowVersion: '03',
        columns: ['id'],
      } satisfies RowPatch,
      ['/vs/lastActive/2024-04-23/abc123']: {id: 'abc123'} satisfies CvrID,
    };

    await runWithDurableObjectStorage(async storage => {
      await initStorage(storage, initialState);

      const cvr = await loadCVR(new DurableStorage(storage), 'abc123');
      const updater = new CVRQueryDrivenUpdater(
        new DurableStorage(storage),
        cvr,
        '1aa',
      );

      updater.executed('oneHash', 'serverOneHash');
      // Simulate receiving different views rows at different time times.
      expect(
        await updater.received(
          new Map([
            [
              `/vs/cvr/abc123/d/r/${ROW_HASH1}`,
              {
                record: {
                  id: ROW_ID1,
                  rowVersion: '03',
                  queriedColumns: {id: ['oneHash']},
                },
                contents: {id: 'should-show-up-in-patch'},
              },
            ],
          ]),
        ),
      ).toEqual([
        [{stateVersion: '1a0'}, ROW_ID1, {id: 'should-show-up-in-patch'}],
      ]);
      expect(
        await updater.received(
          new Map([
            [
              `/vs/cvr/abc123/d/r/${ROW_HASH1}`,
              {
                record: {
                  id: ROW_ID1,
                  rowVersion: '03',
                  queriedColumns: {id: ['oneHash'], name: ['oneHash']},
                },
                contents: {id: 'new version patch with new field'},
              },
            ],
            [
              `/vs/cvr/abc123/d/r/${ROW_HASH3}`,
              {
                record: {
                  id: ROW_ID3,
                  rowVersion: '09',
                  queriedColumns: {id: ['oneHash']},
                },
                contents: {id: 'new version patch'},
              },
            ],
          ]),
        ),
      ).toEqual([
        [
          {stateVersion: '1aa', minorVersion: 1},
          ROW_ID1,
          {id: 'new version patch with new field'},
        ],
        [
          {stateVersion: '1aa', minorVersion: 1},
          ROW_ID3,
          {id: 'new version patch'},
        ],
      ]);
      expect(
        await updater.received(
          new Map([
            [
              `/vs/cvr/abc123/d/r/${ROW_HASH1}`,
              {
                record: {
                  id: ROW_ID1,
                  rowVersion: '03',
                  queriedColumns: {id: ['oneHash']},
                },
                contents: {id: 'patch stays at new version'},
              },
            ],
          ]),
        ),
      ).toEqual([
        [
          {stateVersion: '1aa', minorVersion: 1},
          ROW_ID1,
          {id: 'patch stays at new version'},
        ],
      ]);

      expect(await updater.deleteUnreferencedColumnsAndRows()).toEqual({
        patchRows: [],
        deleteRows: [],
      });

      // Same last active day (no index change), but different hour.
      const updated = await updater.flush(new Date(Date.UTC(2024, 3, 23, 1)));
      expect(updated).toEqual({
        ...cvr,
        version: {stateVersion: '1aa', minorVersion: 1},
        queries: {
          oneHash: {
            id: 'oneHash',
            ast: {table: 'issues'},
            desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
            transformationHash: 'serverOneHash',
            transformationVersion: {stateVersion: '1aa', minorVersion: 1},
            putPatch: {stateVersion: '1aa', minorVersion: 1},
          },
        },
        lastActive: {epochMillis: 1713834000000},
      } satisfies CVRSnapshot);

      // Verify round tripping.
      const reloaded = await loadCVR(new DurableStorage(storage), 'abc123');
      expect(reloaded).toEqual(updated);

      const {
        [`/vs/cvr/abc123/p/d/1a0/r/${ROW_HASH1}`]: _removed,
        ...remainingState
      } = initialState;

      await expectStorage(storage, {
        ...remainingState,
        ['/vs/cvr/abc123/m/version']: updated.version,
        ['/vs/cvr/abc123/m/q/oneHash']: updated.queries.oneHash,
        ['/vs/cvr/abc123/p/m/1aa.01/q/oneHash']: {
          type: 'query',
          op: 'put',
          id: 'oneHash',
        } satisfies QueryPatch,
        ['/vs/cvr/abc123/m/lastActive']: {
          epochMillis: Date.UTC(2024, 3, 23, 1),
        } satisfies LastActive,
        [`/vs/cvr/abc123/d/r/${ROW_HASH1}`]: {
          id: ROW_ID1,
          putPatch: updated.version,
          queriedColumns: {id: ['oneHash', 'twoHash'], name: ['oneHash']},
          rowVersion: '03',
        } satisfies RowRecord,
        [`/vs/cvr/abc123/d/r/${ROW_HASH3}`]: {
          id: ROW_ID3,
          putPatch: updated.version,
          queriedColumns: {id: ['oneHash']},
          rowVersion: '09',
        } satisfies RowRecord,
        [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH1}`]: {
          type: 'row',
          op: 'put',
          id: ROW_ID1,
          rowVersion: '03',
          columns: ['id', 'name'],
        } satisfies RowPatch,
        [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH3}`]: {
          type: 'row',
          op: 'put',
          id: ROW_ID3,
          rowVersion: '09',
          columns: ['id'],
        } satisfies RowPatch,
      });
    });
  });

  test('new transformation hash', async () => {
    const initialState = {
      ['/vs/cvr/abc123/m/version']: {
        stateVersion: '1ba',
      } satisfies CVRVersion,
      ['/vs/cvr/abc123/m/lastActive']: {
        epochMillis: Date.UTC(2024, 3, 23),
      } satisfies LastActive,
      ['/vs/cvr/abc123/m/q/oneHash']: {
        id: 'oneHash',
        ast: {table: 'issues'},
        desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
        transformationHash: 'oneServerHash',
        transformationVersion: {stateVersion: '1aa'},
        putPatch: {stateVersion: '1aa', minorVersion: 1},
      } satisfies QueryRecord,
      [`/vs/cvr/abc123/d/r/${ROW_HASH1}`]: {
        id: ROW_ID1,
        putPatch: {stateVersion: '1aa', minorVersion: 1},
        queriedColumns: {id: ['twoHash', 'oneHash'], name: ['oneHash']},
        rowVersion: '03',
      } satisfies RowRecord,
      [`/vs/cvr/abc123/d/r/${ROW_HASH2}`]: {
        putPatch: {stateVersion: '1a0'},
        id: ROW_ID2,
        rowVersion: '03',
        queriedColumns: {id: ['twoHash']},
      } satisfies RowRecord,
      [`/vs/cvr/abc123/d/r/${ROW_HASH3}`]: {
        id: ROW_ID3,
        putPatch: {stateVersion: '1aa', minorVersion: 1},
        queriedColumns: {id: ['oneHash']},
        rowVersion: '09',
      } satisfies RowRecord,
      [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH1}`]: {
        type: 'row',
        op: 'put',
        id: ROW_ID1,
        rowVersion: '03',
        columns: ['id', 'name'],
      } satisfies RowPatch,
      [`/vs/cvr/abc123/p/d/1a0/r/${ROW_HASH2}`]: {
        type: 'row',
        op: 'put',
        id: ROW_ID2,
        rowVersion: '03',
        columns: ['id'],
      } satisfies RowPatch,
      [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH3}`]: {
        type: 'row',
        op: 'put',
        id: ROW_ID3,
        rowVersion: '09',
        columns: ['id'],
      } satisfies RowPatch,
      ['/vs/lastActive/2024-04-23/abc123']: {id: 'abc123'} satisfies CvrID,
    };

    await runWithDurableObjectStorage(async storage => {
      await initStorage(storage, initialState);

      const cvr = await loadCVR(new DurableStorage(storage), 'abc123');
      const updater = new CVRQueryDrivenUpdater(
        new DurableStorage(storage),
        cvr,
        '1ba',
      );

      updater.executed('oneHash', 'serverTwoHash');
      expect(
        await updater.received(
          new Map([
            [
              `/vs/cvr/abc123/d/r/${ROW_HASH1}`,
              {
                record: {
                  id: ROW_ID1,
                  rowVersion: '03',
                  queriedColumns: {id: ['oneHash']}, // No longer referencing "name"
                },
                contents: {id: 'existing patch'},
              },
            ],
          ]),
        ),
      ).toEqual([
        [
          {stateVersion: '1aa', minorVersion: 1},
          ROW_ID1,
          {id: 'existing patch'},
        ],
      ]);
      expect(
        await updater.received(
          new Map([
            [
              // Now referencing ROW_ID2 instead of ROW_ID3
              `/vs/cvr/abc123/d/r/${ROW_HASH2}`,
              {
                record: {
                  id: ROW_ID2,
                  rowVersion: '09',
                  queriedColumns: {id: ['oneHash']},
                },
                contents: {id: 'new-row-version-should-bump-cvr-version'},
              },
            ],
          ]),
        ),
      ).toEqual([
        [
          {stateVersion: '1ba', minorVersion: 1},
          ROW_ID2,
          {id: 'new-row-version-should-bump-cvr-version'},
        ],
      ]);

      expect(await updater.deleteUnreferencedColumnsAndRows()).toEqual({
        patchRows: [[ROW_ID1, ['id']]],
        deleteRows: [ROW_ID3],
      });

      // Same last active day (no index change), but different hour.
      const updated = await updater.flush(new Date(Date.UTC(2024, 3, 23, 1)));
      expect(updated).toEqual({
        ...cvr,
        version: {stateVersion: '1ba', minorVersion: 1},
        queries: {
          oneHash: {
            id: 'oneHash',
            ast: {table: 'issues'},
            desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
            transformationHash: 'serverTwoHash',
            transformationVersion: {stateVersion: '1ba', minorVersion: 1},
            putPatch: {stateVersion: '1aa', minorVersion: 1},
          },
        },
        lastActive: {epochMillis: 1713834000000},
      } satisfies CVRSnapshot);

      // Verify round tripping.
      const reloaded = await loadCVR(new DurableStorage(storage), 'abc123');
      expect(reloaded).toEqual(updated);

      const {
        // Deleted keys
        [`/vs/cvr/abc123/d/r/${ROW_HASH3}`]: _row3,
        [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH1}`]: _row1Put,
        [`/vs/cvr/abc123/p/d/1a0/r/${ROW_HASH2}`]: _row2Put,
        [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH3}`]: _row3Put,
        ...remainingState
      } = initialState;

      await expectStorage(storage, {
        ...remainingState,
        ['/vs/cvr/abc123/m/version']: updated.version,
        ['/vs/cvr/abc123/m/q/oneHash']: updated.queries.oneHash,
        ['/vs/cvr/abc123/m/lastActive']: {
          epochMillis: Date.UTC(2024, 3, 23, 1),
        } satisfies LastActive,
        [`/vs/cvr/abc123/d/r/${ROW_HASH1}`]: {
          id: ROW_ID1,
          putPatch: updated.version,
          queriedColumns: {id: ['oneHash', 'twoHash']},
          rowVersion: '03',
        } satisfies RowRecord,
        [`/vs/cvr/abc123/d/r/${ROW_HASH2}`]: {
          putPatch: updated.version,
          id: ROW_ID2,
          rowVersion: '09',
          queriedColumns: {id: ['oneHash', 'twoHash']},
        } satisfies RowRecord,
        [`/vs/cvr/abc123/p/d/1ba.01/r/${ROW_HASH1}`]: {
          type: 'row',
          op: 'put',
          id: ROW_ID1,
          rowVersion: '03',
          columns: ['id'],
        } satisfies RowPatch,
        [`/vs/cvr/abc123/p/d/1ba.01/r/${ROW_HASH2}`]: {
          type: 'row',
          op: 'put',
          id: ROW_ID2,
          rowVersion: '09',
          columns: ['id'],
        } satisfies RowPatch,
        [`/vs/cvr/abc123/p/d/1ba.01/r/${ROW_HASH3}`]: {
          type: 'row',
          op: 'del',
          id: ROW_ID3,
        } satisfies RowPatch,
      });
    });
  });

  test('multiple executed queries', async () => {
    const initialState = {
      ['/vs/cvr/abc123/m/version']: {
        stateVersion: '1ba',
      } satisfies CVRVersion,
      ['/vs/cvr/abc123/m/lastActive']: {
        epochMillis: Date.UTC(2024, 3, 23),
      } satisfies LastActive,
      ['/vs/cvr/abc123/m/q/oneHash']: {
        id: 'oneHash',
        ast: {table: 'issues'},
        desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
        transformationHash: 'oneServerHash',
        transformationVersion: {stateVersion: '1aa'},
        putPatch: {stateVersion: '1aa', minorVersion: 1},
      } satisfies QueryRecord,
      ['/vs/cvr/abc123/m/q/twoHash']: {
        id: 'twoHash',
        ast: {table: 'issues'},
        desiredBy: {fooClient: {stateVersion: '1a9', minorVersion: 1}},
        transformationHash: 'twoServerHash',
        transformationVersion: {stateVersion: '1aa'},
        putPatch: {stateVersion: '1aa', minorVersion: 1},
      } satisfies QueryRecord,
      [`/vs/cvr/abc123/d/r/${ROW_HASH1}`]: {
        id: ROW_ID1,
        putPatch: {stateVersion: '1aa', minorVersion: 1},
        queriedColumns: {id: ['oneHash', 'twoHash'], name: ['oneHash']},
        rowVersion: '03',
      } satisfies RowRecord,
      [`/vs/cvr/abc123/d/r/${ROW_HASH2}`]: {
        putPatch: {stateVersion: '1a0'},
        id: ROW_ID2,
        rowVersion: '03',
        queriedColumns: {id: ['twoHash']},
      } satisfies RowRecord,
      [`/vs/cvr/abc123/d/r/${ROW_HASH3}`]: {
        id: ROW_ID3,
        putPatch: {stateVersion: '1aa', minorVersion: 1},
        queriedColumns: {id: ['oneHash']},
        rowVersion: '09',
      } satisfies RowRecord,
      [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH1}`]: {
        type: 'row',
        op: 'put',
        id: ROW_ID1,
        rowVersion: '03',
        columns: ['id', 'name'],
      } satisfies RowPatch,
      [`/vs/cvr/abc123/p/d/1a0/r/${ROW_HASH2}`]: {
        type: 'row',
        op: 'put',
        id: ROW_ID2,
        rowVersion: '03',
        columns: ['id'],
      } satisfies RowPatch,
      [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH3}`]: {
        type: 'row',
        op: 'put',
        id: ROW_ID3,
        rowVersion: '09',
        columns: ['id'],
      } satisfies RowPatch,
      ['/vs/lastActive/2024-04-23/abc123']: {id: 'abc123'} satisfies CvrID,
    };

    await runWithDurableObjectStorage(async storage => {
      await initStorage(storage, initialState);

      const cvr = await loadCVR(new DurableStorage(storage), 'abc123');
      const updater = new CVRQueryDrivenUpdater(
        new DurableStorage(storage),
        cvr,
        '1ba',
      );

      updater.executed('oneHash', 'oneServerHash');
      updater.executed('twoHash', 'twoServerHash');
      expect(
        await updater.received(
          new Map([
            [
              `/vs/cvr/abc123/d/r/${ROW_HASH1}`,
              {
                record: {
                  id: ROW_ID1,
                  rowVersion: '03',
                  queriedColumns: {id: ['oneHash']}, // No longer referencing "name"
                },
                contents: {id: 'existing-patch'},
              },
            ],
          ]),
        ),
      ).toEqual([
        [
          {stateVersion: '1aa', minorVersion: 1},
          ROW_ID1,
          {id: 'existing-patch'},
        ],
      ]);
      expect(
        await updater.received(
          new Map([
            [
              `/vs/cvr/abc123/d/r/${ROW_HASH1}`,
              {
                record: {
                  id: ROW_ID1,
                  rowVersion: '03',
                  queriedColumns: {id: ['twoHash'], desc: ['twoHash']}, // Now referencing "desc"
                },
                contents: {id: 'new-column-bumps-cvr-version'},
              },
            ],
          ]),
        ),
      ).toEqual([
        [
          {stateVersion: '1ba', minorVersion: 1},
          ROW_ID1,
          {id: 'new-column-bumps-cvr-version'},
        ],
      ]);
      await updater.received(
        new Map([
          [
            // Now referencing ROW_ID2 instead of ROW_ID3
            `/vs/cvr/abc123/d/r/${ROW_HASH2}`,
            {
              record: {
                id: ROW_ID2,
                rowVersion: '09',
                queriedColumns: {id: ['oneHash']},
              },
              contents: {
                /* ignored */
              },
            },
          ],
        ]),
      );
      await updater.received(
        new Map([
          [
            `/vs/cvr/abc123/d/r/${ROW_HASH2}`,
            {
              record: {
                id: ROW_ID2,
                rowVersion: '09',
                queriedColumns: {id: ['twoHash']},
              },
              contents: {
                /* ignored */
              },
            },
          ],
        ]),
      );

      expect(await updater.deleteUnreferencedColumnsAndRows()).toEqual({
        patchRows: [[ROW_ID1, ['id', 'desc']]],
        deleteRows: [ROW_ID3],
      });

      // Same last active day (no index change), but different hour.
      const updated = await updater.flush(new Date(Date.UTC(2024, 3, 23, 1)));
      expect(updated).toEqual({
        ...cvr,
        version: {stateVersion: '1ba', minorVersion: 1},
        lastActive: {epochMillis: 1713834000000},
      } satisfies CVRSnapshot);

      // Verify round tripping.
      const reloaded = await loadCVR(new DurableStorage(storage), 'abc123');
      expect(reloaded).toEqual(updated);

      const {
        // Deleted keys
        [`/vs/cvr/abc123/d/r/${ROW_HASH3}`]: _row3,
        [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH1}`]: _row1Put,
        [`/vs/cvr/abc123/p/d/1a0/r/${ROW_HASH2}`]: _row2Put,
        [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH3}`]: _row3Put,
        ...remainingState
      } = initialState;

      await expectStorage(storage, {
        ...remainingState,
        ['/vs/cvr/abc123/m/version']: updated.version,
        ['/vs/cvr/abc123/m/q/oneHash']: updated.queries.oneHash,
        ['/vs/cvr/abc123/m/lastActive']: {
          epochMillis: Date.UTC(2024, 3, 23, 1),
        } satisfies LastActive,
        [`/vs/cvr/abc123/d/r/${ROW_HASH1}`]: {
          id: ROW_ID1,
          putPatch: updated.version,
          queriedColumns: {id: ['oneHash', 'twoHash'], desc: ['twoHash']},
          rowVersion: '03',
        } satisfies RowRecord,
        [`/vs/cvr/abc123/d/r/${ROW_HASH2}`]: {
          putPatch: updated.version,
          id: ROW_ID2,
          rowVersion: '09',
          queriedColumns: {id: ['oneHash', 'twoHash']},
        } satisfies RowRecord,
        [`/vs/cvr/abc123/p/d/1ba.01/r/${ROW_HASH1}`]: {
          type: 'row',
          op: 'put',
          id: ROW_ID1,
          rowVersion: '03',
          columns: ['id', 'desc'],
        } satisfies RowPatch,
        [`/vs/cvr/abc123/p/d/1ba.01/r/${ROW_HASH2}`]: {
          type: 'row',
          op: 'put',
          id: ROW_ID2,
          rowVersion: '09',
          columns: ['id'],
        } satisfies RowPatch,
        [`/vs/cvr/abc123/p/d/1ba.01/r/${ROW_HASH3}`]: {
          type: 'row',
          op: 'del',
          id: ROW_ID3,
        } satisfies RowPatch,
      });
    });
  });

  test('removed query', async () => {
    const initialState = {
      ['/vs/cvr/abc123/m/version']: {
        stateVersion: '1ba',
      } satisfies CVRVersion,
      ['/vs/cvr/abc123/m/lastActive']: {
        epochMillis: Date.UTC(2024, 3, 23),
      } satisfies LastActive,
      ['/vs/cvr/abc123/m/q/oneHash']: {
        id: 'oneHash',
        ast: {table: 'issues'},
        desiredBy: {},
        transformationHash: 'oneServerHash',
        transformationVersion: {stateVersion: '1aa'},
        putPatch: {stateVersion: '1aa', minorVersion: 1},
      } satisfies QueryRecord,
      ['/vs/lastActive/2024-04-23/abc123']: {id: 'abc123'} satisfies CvrID,
      ['/vs/cvr/abc123/p/m/1aa.01/q/oneHash']: {
        type: 'query',
        op: 'put',
        id: 'oneHash',
      } satisfies QueryPatch,
      [`/vs/cvr/abc123/d/r/${ROW_HASH1}`]: {
        id: ROW_ID1,
        putPatch: {stateVersion: '1aa', minorVersion: 1},
        queriedColumns: {id: ['twoHash', 'oneHash'], name: ['oneHash']},
        rowVersion: '03',
      } satisfies RowRecord,
      [`/vs/cvr/abc123/d/r/${ROW_HASH2}`]: {
        putPatch: {stateVersion: '1a0'},
        id: ROW_ID2,
        rowVersion: '03',
        queriedColumns: {id: ['twoHash']},
      } satisfies RowRecord,
      [`/vs/cvr/abc123/d/r/${ROW_HASH3}`]: {
        id: ROW_ID3,
        putPatch: {stateVersion: '1aa', minorVersion: 1},
        queriedColumns: {id: ['oneHash']},
        rowVersion: '09',
      } satisfies RowRecord,
      [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH1}`]: {
        type: 'row',
        op: 'put',
        id: ROW_ID1,
        rowVersion: '03',
        columns: ['id', 'name'],
      } satisfies RowPatch,
      [`/vs/cvr/abc123/p/d/1a0/r/${ROW_HASH2}`]: {
        type: 'row',
        op: 'put',
        id: ROW_ID2,
        rowVersion: '03',
        columns: ['id'],
      } satisfies RowPatch,
      [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH3}`]: {
        type: 'row',
        op: 'put',
        id: ROW_ID3,
        rowVersion: '09',
        columns: ['id'],
      } satisfies RowPatch,
    };

    await runWithDurableObjectStorage(async storage => {
      await initStorage(storage, initialState);

      const cvr = await loadCVR(new DurableStorage(storage), 'abc123');
      const updater = new CVRQueryDrivenUpdater(
        new DurableStorage(storage),
        cvr,
        '1ba',
      );

      updater.removed('oneHash');
      expect(await updater.deleteUnreferencedColumnsAndRows()).toEqual({
        patchRows: [[ROW_ID1, ['id']]],
        deleteRows: [ROW_ID3],
      });

      // Same last active day (no index change), but different hour.
      const updated = await updater.flush(new Date(Date.UTC(2024, 3, 23, 1)));
      expect(updated).toEqual({
        ...cvr,
        version: {stateVersion: '1ba', minorVersion: 1},
        queries: {},
        lastActive: {epochMillis: 1713834000000},
      } satisfies CVRSnapshot);

      // Verify round tripping.
      const reloaded = await loadCVR(new DurableStorage(storage), 'abc123');
      expect(reloaded).toEqual(updated);

      const {
        // Deleted keys
        ['/vs/cvr/abc123/m/q/oneHash']: _removed,
        [`/vs/cvr/abc123/d/r/${ROW_HASH3}`]: _row3,
        [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH1}`]: _row1Put,
        [`/vs/cvr/abc123/p/d/1aa.01/r/${ROW_HASH3}`]: _row3Put,
        ['/vs/cvr/abc123/p/m/1aa.01/q/oneHash']: _removedToo,
        ...remainingState
      } = initialState;

      await expectStorage(storage, {
        ...remainingState,
        ['/vs/cvr/abc123/m/version']: updated.version,
        ['/vs/cvr/abc123/m/lastActive']: {
          epochMillis: Date.UTC(2024, 3, 23, 1),
        } satisfies LastActive,
        [`/vs/cvr/abc123/d/r/${ROW_HASH1}`]: {
          id: ROW_ID1,
          putPatch: updated.version,
          queriedColumns: {id: ['twoHash']},
          rowVersion: '03',
        } satisfies RowRecord,
        [`/vs/cvr/abc123/p/d/1ba.01/r/${ROW_HASH1}`]: {
          type: 'row',
          op: 'put',
          id: ROW_ID1,
          rowVersion: '03',
          columns: ['id'],
        } satisfies RowPatch,
        [`/vs/cvr/abc123/p/d/1ba.01/r/${ROW_HASH3}`]: {
          type: 'row',
          op: 'del',
          id: ROW_ID3,
        } satisfies RowPatch,
        ['/vs/cvr/abc123/p/m/1ba.01/q/oneHash']: {
          type: 'query',
          op: 'del',
          id: 'oneHash',
        } satisfies QueryPatch,
      });
    });
  });
});
