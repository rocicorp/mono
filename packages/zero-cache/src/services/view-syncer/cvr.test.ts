import {describe, expect, test} from 'vitest';
import {DurableStorage} from '../../storage/durable-storage.js';
import {
  expectStorage,
  initStorage,
  runWithDurableObjectStorage,
} from '../../test/do.js';
import {
  CVRConfigDrivenUpdater,
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
        ['/vs/cvr/abc123/meta/lastActive']: flushed.lastActive,
        ['/vs/lastActive/2024-04-20/abc123']: {id: 'abc123'} satisfies CvrID,
      });
    });
  });

  test('load existing cvr', async () => {
    await runWithDurableObjectStorage(async storage => {
      await initStorage(storage, {
        ['/vs/cvr/abc123/meta/version']: {
          stateVersion: '1a9',
          minorVersion: 2,
        } satisfies CVRVersion,
        ['/vs/cvr/abc123/meta/lastActive']: {
          epochMillis: Date.UTC(2024, 3, 23),
        } satisfies LastActive,
        ['/vs/cvr/abc123/meta/clients/fooClient']: {
          id: 'fooClient',
          desiredQueryIDs: ['oneHash'],
          putPatch: {stateVersion: '1a9', minorVersion: 1},
        } satisfies ClientRecord,
        ['/vs/cvr/abc123/meta/queries/oneHash']: {
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
        ['/vs/cvr/abc123/meta/version']: {
          stateVersion: '1a9',
          minorVersion: 2,
        } satisfies CVRVersion,
        ['/vs/cvr/abc123/meta/lastActive']: {
          epochMillis: Date.UTC(2024, 3, 23),
        } satisfies LastActive,
        ['/vs/cvr/abc123/meta/clients/fooClient']: {
          id: 'fooClient',
          desiredQueryIDs: ['oneHash'],
          putPatch: {stateVersion: '1a9', minorVersion: 1},
        } satisfies ClientRecord,
        ['/vs/cvr/abc123/meta/queries/oneHash']: {
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
        ['/vs/cvr/abc123/meta/version']: updated.version,
        ['/vs/cvr/abc123/meta/lastActive']: updated.lastActive,
        ['/vs/cvr/abc123/meta/clients/fooClient']: updated.clients.fooClient,
        ['/vs/cvr/abc123/meta/queries/oneHash']: updated.queries.oneHash,
        // LastActive index
        ['/vs/lastActive/2024-04-24/abc123']: {id: 'abc123'} satisfies CvrID,
      });
    });
  });

  test('update desired query set', async () => {
    await runWithDurableObjectStorage(async storage => {
      await initStorage(storage, {
        ['/vs/cvr/abc123/meta/version']: {
          stateVersion: '1aa',
        } satisfies CVRVersion,
        ['/vs/cvr/abc123/meta/lastActive']: {
          epochMillis: Date.UTC(2024, 3, 23),
        } satisfies LastActive,
        ['/vs/cvr/abc123/meta/clients/dooClient']: {
          id: 'dooClient',
          desiredQueryIDs: ['oneHash', 'nonExistentQuery'],
          putPatch: {stateVersion: '1a8'},
        } satisfies ClientRecord,
        ['/vs/cvr/abc123/meta/clients/fooClient']: {
          id: 'fooClient',
          desiredQueryIDs: ['oneHash'],
          putPatch: {stateVersion: '1a9', minorVersion: 1},
        } satisfies ClientRecord,
        ['/vs/cvr/abc123/meta/queries/oneHash']: {
          id: 'oneHash',
          ast: {table: 'issues'},
          transformationHash: 'twoHash',
          desiredBy: {
            dooClient: {stateVersion: '1a8'},
            fooClient: {stateVersion: '1a9', minorVersion: 1},
          },
          putPatch: {stateVersion: '1a9', minorVersion: 2},
        } satisfies QueryRecord,
        ['/vs/cvr/abc123/patches/meta/1a8/queries/oneHash/clients/dooClient']: {
          type: 'query',
          op: 'put',
          id: 'oneHash',
          clientID: 'dooClient,',
        } satisfies QueryPatch,
        ['/vs/cvr/abc123/patches/meta/1a9.01/queries/oneHash/clients/fooClient']:
          {
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
        ['/vs/cvr/abc123/meta/version']: updated.version,
        ['/vs/cvr/abc123/meta/lastActive']: updated.lastActive,
        ['/vs/cvr/abc123/meta/clients/barClient']: updated.clients.barClient,
        ['/vs/cvr/abc123/meta/clients/bonkClient']: updated.clients.bonkClient,
        ['/vs/cvr/abc123/meta/clients/dooClient']: updated.clients.dooClient,
        ['/vs/cvr/abc123/meta/clients/fooClient']: updated.clients.fooClient,
        ['/vs/cvr/abc123/meta/queries/oneHash']: updated.queries.oneHash,
        ['/vs/cvr/abc123/meta/queries/threeHash']: updated.queries.threeHash,
        ['/vs/cvr/abc123/meta/queries/fourHash']: updated.queries.fourHash,
        // Patches!
        ['/vs/cvr/abc123/patches/meta/1aa.01/clients/barClient']: {
          type: 'client',
          op: 'put',
          id: 'barClient',
        } satisfies ClientPatch,
        ['/vs/cvr/abc123/patches/meta/1aa.01/clients/bonkClient']: {
          type: 'client',
          op: 'put',
          id: 'bonkClient',
        } satisfies ClientPatch,
        ['/vs/cvr/abc123/patches/meta/1aa.01/queries/threeHash/clients/barClient']:
          {
            type: 'query',
            op: 'put',
            id: 'threeHash',
            clientID: 'barClient',
          } satisfies QueryPatch,
        ['/vs/cvr/abc123/patches/meta/1aa.01/queries/threeHash/clients/fooClient']:
          {
            type: 'query',
            op: 'put',
            id: 'threeHash',
            clientID: 'fooClient',
          } satisfies QueryPatch,
        ['/vs/cvr/abc123/patches/meta/1aa.01/queries/oneHash/clients/barClient']:
          {
            type: 'query',
            op: 'put',
            id: 'oneHash',
            clientID: 'barClient',
          } satisfies QueryPatch,
        ['/vs/cvr/abc123/patches/meta/1aa.01/queries/oneHash/clients/dooClient']:
          {
            type: 'query',
            op: 'del', // The obsoleted 'put' patch at 1a9.01 is deleted too.
            id: 'oneHash',
            clientID: 'dooClient',
          } satisfies QueryPatch,
        ['/vs/cvr/abc123/patches/meta/1aa.01/queries/oneHash/clients/fooClient']:
          {
            type: 'query',
            op: 'del', // The obsoleted 'put' patch at 1a9.01 is deleted too.
            id: 'oneHash',
            clientID: 'fooClient',
          } satisfies QueryPatch,
        ['/vs/cvr/abc123/patches/meta/1aa.01/queries/fourHash/clients/fooClient']:
          {
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
      ['/vs/cvr/abc123/meta/version']: {
        stateVersion: '1aa',
      } satisfies CVRVersion,
      ['/vs/cvr/abc123/meta/lastActive']: {
        epochMillis: Date.UTC(2024, 3, 23),
      } satisfies LastActive,
      ['/vs/cvr/abc123/meta/clients/fooClient']: {
        id: 'fooClient',
        desiredQueryIDs: ['oneHash'],
        putPatch: {stateVersion: '1a9', minorVersion: 1},
      } satisfies ClientRecord,
      ['/vs/cvr/abc123/meta/queries/oneHash']: {
        id: 'oneHash',
        ast: {table: 'issues'},
        transformationHash: 'twoHash',
        desiredBy: {
          fooClient: {stateVersion: '1a9', minorVersion: 1},
        },
        putPatch: {stateVersion: '1a9', minorVersion: 2},
      } satisfies QueryRecord,
      ['/vs/cvr/abc123/patches/meta/1a9.01/queries/oneHash/clients/fooClient']:
        {
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
        ['/vs/cvr/abc123/meta/lastActive']: {
          epochMillis: Date.UTC(2024, 3, 23, 1),
        } satisfies LastActive,
      });
    });
  });
});
