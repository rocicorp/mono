import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {assert, assertNotUndefined} from '../../../shared/src/asserts.ts';
import {BTreeRead} from '../btree/read.ts';
import type {Read, Write} from '../dag/store.ts';
import {TestStore} from '../dag/test-store.ts';
import type {Commit} from '../db/commit.ts';
import {
  type SnapshotMetaDD31,
  commitFromHash,
  commitIsSnapshot,
  fromChunk,
} from '../db/commit.ts';
import {ChainBuilder} from '../db/test-helpers.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {deepFreeze} from '../frozen-json.ts';
import {assertHash, fakeHash, newRandomHash} from '../hash.ts';
import type {IndexDefinitions} from '../index-defs.ts';
import type {ClientGroupID, ClientID} from '../sync/ids.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {
  type ClientGroup,
  getClientGroup,
  setClientGroup,
} from './client-groups.ts';
import {makeClientV6, setClientsForTesting} from './clients-test-helpers.ts';
import {
  CLIENTS_HEAD_NAME,
  type ClientV5,
  FIND_MATCHING_CLIENT_TYPE_FORK,
  FIND_MATCHING_CLIENT_TYPE_HEAD,
  FIND_MATCHING_CLIENT_TYPE_NEW,
  type FindMatchingClientResult,
  assertClientV6,
  findMatchingClient,
  getClient,
  getClientGroupForClient,
  getClientGroupIDForClient,
  getClients,
  initClientV6,
  setClient,
} from './clients.ts';
import {makeClientID} from './make-client-id.ts';

beforeEach(() => {
  vi.setSystemTime(0);
});

afterEach(() => {
  vi.useRealTimers();
});

const headClient1Hash = fakeHash('f1');
const headClient2Hash = fakeHash('f2');
const headClient3Hash = fakeHash('f3');
const randomStuffHash = fakeHash('c3');
const refresh1Hash = fakeHash('e1');

test('getClients with no existing ClientMap in dag store', async () => {
  const dagStore = new TestStore();
  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap.size).toBe(0);
  });
});

test('updateClients and getClients', async () => {
  const dagStore = new TestStore();
  const clientMap = new Map(
    Object.entries({
      client1: makeClientV6({
        heartbeatTimestampMs: 1000,
        refreshHashes: [headClient1Hash],
      }),
      client2: makeClientV6({
        heartbeatTimestampMs: 3000,
        refreshHashes: [headClient2Hash],
      }),
    }),
  );
  await setClientsForTesting(clientMap, dagStore);

  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).toEqual(clientMap);
  });
});

test('updateClients and getClients for DD31', async () => {
  const dagStore = new TestStore();
  const clientMap = new Map(
    Object.entries({
      client1: makeClientV6({
        heartbeatTimestampMs: 1000,
        refreshHashes: [headClient1Hash, refresh1Hash],
        clientGroupID: 'client-group-id-1',
      }),
      client2: makeClientV6({
        heartbeatTimestampMs: 3000,
        refreshHashes: [headClient2Hash],
        clientGroupID: 'client-group-id-2',
      }),
    }),
  );
  await setClientsForTesting(clientMap, dagStore);

  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).toEqual(clientMap);
  });

  // Make sure we write the refresh1Hash as well.
  await withRead(dagStore, async read => {
    const h = await read.getHead(CLIENTS_HEAD_NAME);
    assert(h);
    const chunk = await read.getChunk(h);
    assert(chunk);
    expect(chunk.meta).toEqual([
      refresh1Hash,
      headClient1Hash,
      headClient2Hash,
    ]);
  });
});

test('updateClients and getClients sequence', async () => {
  const dagStore = new TestStore();
  const clientMap1 = new Map(
    Object.entries({
      client1: makeClientV6({
        heartbeatTimestampMs: 1000,
        refreshHashes: [headClient1Hash],
      }),
      client2: makeClientV6({
        heartbeatTimestampMs: 3000,
        refreshHashes: [headClient2Hash],
      }),
    }),
  );

  const clientMap2 = new Map(
    Object.entries({
      client3: makeClientV6({
        heartbeatTimestampMs: 4000,
        refreshHashes: [headClient3Hash],
      }),
    }),
  );
  await setClientsForTesting(clientMap1, dagStore);

  await withRead(dagStore, async (read: Read) => {
    const readClientMap1 = await getClients(read);
    expect(readClientMap1).toEqual(clientMap1);
  });

  await setClientsForTesting(clientMap2, dagStore);

  await withRead(dagStore, async (read: Read) => {
    const readClientMap2 = await getClients(read);
    expect(readClientMap2).toEqual(clientMap2);
  });
});

test('updateClients properly manages refs to client heads when clients are removed and added', async () => {
  const dagStore = new TestStore();
  const client1HeadHash = headClient1Hash;
  const client2HeadHash = headClient2Hash;

  const clientMap1 = new Map(
    Object.entries({
      client1: makeClientV6({
        heartbeatTimestampMs: 1000,
        refreshHashes: [client1HeadHash],
      }),
      client2: makeClientV6({
        heartbeatTimestampMs: 3000,
        refreshHashes: [client2HeadHash],
      }),
    }),
  );

  const client3HeadHash = headClient3Hash;
  const clientMap2 = new Map(
    Object.entries({
      client3: makeClientV6({
        heartbeatTimestampMs: 4000,
        refreshHashes: [client3HeadHash],
      }),
    }),
  );
  await setClientsForTesting(clientMap1, dagStore);

  await withRead(dagStore, async (read: Read) => {
    const clientsHash = await read.getHead('clients');
    assertHash(clientsHash);
    const clientsChunk = await read.getChunk(clientsHash);
    expect(clientsChunk?.meta).toEqual([client1HeadHash, client2HeadHash]);
  });
  await setClientsForTesting(clientMap2, dagStore);

  await withRead(dagStore, async (read: Read) => {
    const clientsHash = await read.getHead('clients');
    assertHash(clientsHash);
    const clientsChunk = await read.getChunk(clientsHash);
    expect(clientsChunk?.meta).toEqual([client3HeadHash]);
  });
});

test("updateClients properly manages refs to client heads when a client's head changes", async () => {
  const dagStore = new TestStore();
  const client1V1HeadHash = fakeHash('c11');
  const client1V2HeadHash = fakeHash('c12');
  const client2HeadHash = fakeHash('c2');

  const client1V1 = makeClientV6({
    heartbeatTimestampMs: 1000,
    refreshHashes: [client1V1HeadHash],
  });
  const client1V2 = makeClientV6({
    heartbeatTimestampMs: 2000,
    refreshHashes: [client1V2HeadHash],
  });
  const client2 = makeClientV6({
    heartbeatTimestampMs: 3000,
    refreshHashes: [client2HeadHash],
  });

  const clientMap1 = new Map(
    Object.entries({
      client1: client1V1,
      client2,
    }),
  );

  await setClientsForTesting(clientMap1, dagStore);

  await withRead(dagStore, async (read: Read) => {
    const clientsHash = await read.getHead('clients');
    assertHash(clientsHash);
    const clientsChunk = await read.getChunk(clientsHash);
    expect(clientsChunk?.meta).toEqual([client2HeadHash, client1V1HeadHash]);
  });

  await setClientsForTesting(
    new Map(
      Object.entries({
        client1: client1V2,
        client2,
      }),
    ),
    dagStore,
  );

  await withRead(dagStore, async (read: Read) => {
    const clientsHash = await read.getHead('clients');
    assertHash(clientsHash);
    const clientsChunk = await read.getChunk(clientsHash);
    expect(clientsChunk?.meta).toEqual([client2HeadHash, client1V2HeadHash]);
  });
});

test('getClient', async () => {
  const dagStore = new TestStore();
  const client1 = makeClientV6({
    heartbeatTimestampMs: 1000,
    refreshHashes: [headClient1Hash],
  });
  const clientMap = new Map(
    Object.entries({
      client1,
      client2: makeClientV6({
        heartbeatTimestampMs: 3000,
        refreshHashes: [headClient2Hash],
      }),
    }),
  );
  await setClientsForTesting(clientMap, dagStore);

  await withRead(dagStore, async (read: Read) => {
    const readClient1 = await getClient('client1', read);
    expect(readClient1).toEqual(client1);
  });
});

test('updateClients throws errors if clients head exist but the chunk it references does not', async () => {
  const dagStore = new TestStore();
  await withWrite(dagStore, async (write: Write) => {
    await write.setHead('clients', randomStuffHash);
  });
  await withRead(dagStore, async (read: Read) => {
    let e;
    try {
      await getClients(read);
    } catch (ex) {
      e = ex;
    }
    expect(e).toBeInstanceOf(Error);
  });
});

test('updateClients throws errors if chunk pointed to by clients head does not contain a valid ClientMap', async () => {
  const dagStore = new TestStore();
  await withWrite(dagStore, async (write: Write) => {
    const headHash = headClient1Hash;
    const chunk = write.createChunk(
      deepFreeze({
        heartbeatTimestampMs: 'this should be a number',
        headHash,
      }),
      [headHash],
    );

    await Promise.all([
      write.putChunk(chunk),
      write.setHead('clients', chunk.hash),
    ]);
  });
  await withRead(dagStore, async (read: Read) => {
    let e;
    try {
      await getClients(read);
    } catch (ex) {
      e = ex;
    }
    expect(e).toBeInstanceOf(Error);
  });
});

test('initClient creates new empty snapshot when no existing snapshot to bootstrap from', async () => {
  const formatVersion = FormatVersion.Latest;
  const dagStore = new TestStore();
  vi.setSystemTime(Date.now() + 4000);
  const clientID = makeClientID();
  const [client, headHash, clients] = await initClientV6(
    clientID,
    new LogContext(),
    dagStore,
    [],
    {},
    formatVersion,
    true /* enableClientGroupForking */,
  );

  expect(clients).toEqual(
    new Map(
      Object.entries({
        [clientID]: client,
      }),
    ),
  );

  await withRead(dagStore, async (dagRead: Read) => {
    // New client was added to the client map.
    expect(await getClient(clientID, dagRead)).toEqual(client);
    expect(client.heartbeatTimestampMs).toBe(Date.now());

    const {clientGroupID} = client;
    const clientGroup = await getClientGroup(clientGroupID, dagRead);
    assert(clientGroup);
    expect(clientGroup.mutationIDs).toEqual({});
    expect(clientGroup.lastServerAckdMutationIDs).toEqual({});

    // New client's head hash points to an empty snapshot with an empty btree.
    const headChunk = await dagRead.getChunk(headHash);
    assertNotUndefined(headChunk);
    const commit = fromChunk(headChunk);
    expect(commitIsSnapshot(commit)).toBe(true);
    const snapshotMeta = commit.meta as SnapshotMetaDD31;
    expect(snapshotMeta.basisHash).toBeNull();
    expect(snapshotMeta.cookieJSON).toBeNull();
    expect(await commit.getMutationID(clientID, dagRead)).toBe(0);
    expect(commit.indexes).toHaveLength(0);
    expect(
      await new BTreeRead(dagRead, formatVersion, commit.valueHash).isEmpty(),
    ).toBe(true);
  });
});

test('setClient', async () => {
  const dagStore = new TestStore();

  const t = async (clientID: ClientID, client: ClientV5) => {
    await withWrite(dagStore, async (write: Write) => {
      await setClient(clientID, client, write);
    });

    await withRead(dagStore, async (read: Read) => {
      const actualClient = await getClient(clientID, read);
      expect(actualClient).toEqual(client);
    });
  };

  const clientID = 'client-id';
  await t(clientID, {
    clientGroupID: 'client-group-id-1',
    headHash: newRandomHash(),
    heartbeatTimestampMs: 1,
    tempRefreshHash: null,
  });

  await t(clientID, {
    clientGroupID: 'client-group-id-1',
    headHash: newRandomHash(),
    heartbeatTimestampMs: 2,
    tempRefreshHash: newRandomHash(),
  });

  const clientID2 = 'client-id-2';
  await t(clientID2, {
    clientGroupID: 'client-group-id-1',
    headHash: newRandomHash(),
    heartbeatTimestampMs: 3,
    tempRefreshHash: newRandomHash(),
  });
});

test('getClientGroupID', async () => {
  const dagStore = new TestStore();

  const t = async (
    clientID: ClientID,
    client: ClientV5,
    clientGroupID: ClientGroupID,
    clientGroup: ClientGroup,
    expectedClientGroupID: ClientGroupID | undefined,
    expectedClientGroup: ClientGroup | undefined,
  ) => {
    await withWrite(dagStore, async write => {
      await setClient(clientID, client, write);
      await setClientGroup(clientGroupID, clientGroup, write);
    });

    const actualClientGroupID = await withRead(dagStore, read =>
      getClientGroupIDForClient(clientID, read),
    );
    expect(actualClientGroupID).toBe(expectedClientGroupID);

    const actualClientGroup = await withRead(dagStore, read =>
      getClientGroupForClient(clientID, read),
    );
    expect(actualClientGroup).toEqual(expectedClientGroup);
  };

  const clientID = 'client-id-1';
  const clientGroupID = 'client-group-id-1';

  const clientGroup = {
    headHash: newRandomHash(),
    lastServerAckdMutationIDs: {[clientID]: 0},
    mutationIDs: {[clientID]: 0},
    indexes: {},
    mutatorNames: [],
    disabled: false,
  };
  {
    const client = {
      clientGroupID,
      headHash: newRandomHash(),
      heartbeatTimestampMs: 1,
      tempRefreshHash: null,
    };
    await t(
      clientID,
      client,
      clientGroupID,
      clientGroup,
      clientGroupID,
      clientGroup,
    );
  }

  {
    const client = {
      clientGroupID: 'client-group-id-wrong',
      headHash: newRandomHash(),
      heartbeatTimestampMs: 1,
      tempRefreshHash: null,
    };
    let err;
    try {
      await t(
        clientID,
        client,
        clientGroupID,
        clientGroup,
        undefined,
        undefined,
      );
    } catch (e) {
      err = e;
    }
    // Invalid client group ID.
    expect(err).toBeInstanceOf(Error);
  }

  const actualClientGroupID2 = await withRead(dagStore, read =>
    getClientGroupIDForClient(clientID, read),
  );
  expect(actualClientGroupID2).toBe('client-group-id-wrong');

  const actualClientGroup2 = await withRead(dagStore, read =>
    getClientGroupForClient(clientID, read),
  );
  expect(actualClientGroup2).toBeUndefined();
});

describe('findMatchingClient', () => {
  test('new (empty perdag)', async () => {
    const perdag = new TestStore();
    await withRead(perdag, async read => {
      const mutatorNames: string[] = [];
      const indexes = {};
      expect(await findMatchingClient(read, mutatorNames, indexes)).toEqual({
        type: FIND_MATCHING_CLIENT_TYPE_NEW,
      });
    });
  });

  async function testFindMatchingClientFork(
    initialMutatorNames: string[],
    initialIndexes: IndexDefinitions,
    newMutatorNames: string[],
    newIndexes: IndexDefinitions,
    initialDisabled = false,
  ) {
    const perdag = new TestStore();
    const clientID = 'client-id';
    const clientGroupID = 'client-group-id';
    const b = new ChainBuilder(perdag);
    await b.addGenesis(clientID);
    await b.addLocal(clientID, []);

    await withWrite(perdag, async write => {
      const client: ClientV5 = {
        clientGroupID,
        headHash: b.chain[1].chunk.hash,
        heartbeatTimestampMs: 1,
        tempRefreshHash: null,
      };
      await setClient(clientID, client, write);

      const clientGroup: ClientGroup = {
        headHash: b.chain[1].chunk.hash,
        lastServerAckdMutationIDs: {[clientID]: 0},
        mutationIDs: {[clientID]: 1},
        indexes: initialIndexes,
        mutatorNames: initialMutatorNames,
        disabled: initialDisabled,
      };
      await setClientGroup(clientGroupID, clientGroup, write);
    });

    await withRead(perdag, async read => {
      const res = await findMatchingClient(read, newMutatorNames, newIndexes);
      const expected: FindMatchingClientResult = {
        type: FIND_MATCHING_CLIENT_TYPE_FORK,
        snapshot: b.chain[0] as Commit<SnapshotMetaDD31>,
      };
      expect(res).toEqual(expected);
    });
  }

  test('fork because different mutator names', async () => {
    await testFindMatchingClientFork([], {}, ['fork'], {});
    await testFindMatchingClientFork(['x'], {}, ['y'], {});
    await testFindMatchingClientFork(['z'], {}, [], {});
  });

  test('fork because different indexes', async () => {
    await testFindMatchingClientFork([], {}, [], {
      idx: {jsonPointer: '/foo'},
    });

    await testFindMatchingClientFork(
      [],
      {
        idx: {jsonPointer: '/foo'},
      },
      [],
      {
        idx: {jsonPointer: '/bar'},
      },
    );

    await testFindMatchingClientFork(
      [],
      {
        idx: {jsonPointer: '/foo'},
      },
      [],
      {},
    );
  });

  test('fork because client group disabled', async () => {
    const t = (mutatorNames: string[], indexes: IndexDefinitions) =>
      testFindMatchingClientFork(
        mutatorNames,
        indexes,
        mutatorNames,
        indexes,
        true,
      );
    await t([], {});
    await t(['x'], {});
    await t(['z'], {i: {jsonPointer: '/foo'}});
  });

  async function testFindMatchingClientHead(
    initialMutatorNames: string[],
    initialIndexes: IndexDefinitions,
    newMutatorNames: string[] = initialMutatorNames,
    newIndexes: IndexDefinitions = initialIndexes,
  ) {
    const perdag = new TestStore();
    const clientID = 'client-id';
    const clientGroupID = 'client-group-id';

    const chainBuilder = new ChainBuilder(perdag, 'temp-head');
    await chainBuilder.addGenesis(clientID);
    await chainBuilder.addLocal(clientID, []);
    const {headHash} = chainBuilder;

    const clientGroup: ClientGroup = {
      headHash,
      lastServerAckdMutationIDs: {[clientID]: 0},
      mutationIDs: {[clientID]: 1},
      indexes: initialIndexes,
      mutatorNames: initialMutatorNames,
      disabled: false,
    };
    await withWrite(perdag, async write => {
      await setClientGroup(clientGroupID, clientGroup, write);
    });

    await chainBuilder.removeHead();

    await withRead(perdag, async read => {
      const res = await findMatchingClient(read, newMutatorNames, newIndexes);
      const expected: FindMatchingClientResult = {
        type: FIND_MATCHING_CLIENT_TYPE_HEAD,
        clientGroupID,
        headHash,
      };
      expect(res).toEqual(expected);
    });
  }

  test('reuse head', async () => {
    await testFindMatchingClientHead([], {});
    await testFindMatchingClientHead(['x'], {});
    await testFindMatchingClientHead([], {idx: {jsonPointer: '/foo'}});
    await testFindMatchingClientHead(['x', 'y'], {}, ['y', 'x']);
  });
});

describe('initClientV6', () => {
  function makeSuite(enableClientGroupForking: boolean) {
    describe(`enableClientGroupForking: ${enableClientGroupForking}`, () => {
      test('new client for empty db', async () => {
        const formatVersion = FormatVersion.Latest;
        const lc = new LogContext();
        const perdag = new TestStore();
        const mutatorNames: string[] = [];
        const indexes: IndexDefinitions = {};

        const clientID = makeClientID();
        const [client, , clientMap, newClientGroup] = await initClientV6(
          clientID,
          lc,
          perdag,
          mutatorNames,
          indexes,
          formatVersion,
          enableClientGroupForking,
        );
        expect(clientID).toBeTypeOf('string');
        assertClientV6(client);
        expect(newClientGroup).toBe(true);
        expect(clientMap.size).toBe(1);
        expect(clientMap.get(clientID)).toBe(client);
        expect(client.persistHash).toBeNull();

        await withRead(perdag, async read => {
          const clientGroup = await getClientGroup(client.clientGroupID, read);
          return clientGroup;
        });
      });

      test('reuse head', async () => {
        const formatVersion = FormatVersion.Latest;
        const lc = new LogContext();

        const perdag = new TestStore();
        const clientID1 = 'client-id-1';
        const clientGroupID = 'client-group-id';
        const b = new ChainBuilder(perdag);
        await b.addGenesis(clientID1);
        await b.addLocal(clientID1, []);
        const headHash = b.chain[1].chunk.hash;
        const mutatorNames: string[] = ['x'];
        const indexes: IndexDefinitions = {};

        vi.setSystemTime(10);

        const client1: ClientV5 = {
          clientGroupID,
          headHash,
          heartbeatTimestampMs: 1,
          tempRefreshHash: null,
        };
        const clientGroup1: ClientGroup = {
          headHash: b.chain[1].chunk.hash,
          lastServerAckdMutationIDs: {[clientID1]: 0},
          mutationIDs: {[clientID1]: 1},
          indexes,
          mutatorNames,
          disabled: false,
        };

        await withWrite(perdag, async write => {
          await setClient(clientID1, client1, write);
          await setClientGroup(clientGroupID, clientGroup1, write);
        });

        const clientID2 = makeClientID();
        const [client2, client2HeadHash, clientMap, newClientGroup] =
          await initClientV6(
            clientID2,
            lc,
            perdag,
            mutatorNames,
            indexes,
            formatVersion,
            enableClientGroupForking,
          );
        expect(clientID2).not.toBe(clientID1);
        expect(newClientGroup).toBe(false);
        expect(clientMap.size).toBe(2);
        expect(client2).toEqual({
          clientGroupID,
          refreshHashes: [client2HeadHash],
          heartbeatTimestampMs: 10,
          persistHash: null,
        });
        expect(client2HeadHash).toBe(clientGroup1.headHash);

        const clientGroup2 = await withRead(perdag, read =>
          getClientGroup(clientGroupID, read),
        );
        expect(clientGroup2).toEqual({
          ...clientGroup1,
          lastServerAckdMutationIDs: {
            [clientID1]: 0,
          },
          mutationIDs: {
            [clientID1]: 1,
          },
        });
      });

      test('fork snapshot due to incompatible defs', async () => {
        const formatVersion = FormatVersion.Latest;
        const lc = new LogContext();

        const perdag = new TestStore();
        const clientID1 = 'client-id-1';
        const clientGroupID1 = 'client-group-id-1';
        const b = new ChainBuilder(perdag);
        await b.addGenesis(clientID1);
        const clientGroup1Snapshot = await b.addSnapshot(
          [
            ['a', 'b'],
            ['c', 'd'],
          ],
          clientID1,
          1,
          {[clientID1]: 10},
        );
        await b.addLocal(clientID1, []);
        const headHash = b.chain[1].chunk.hash;
        const initialMutatorNames: string[] = ['x'];
        const initialIndexes: IndexDefinitions = {};
        const newMutatorNames = ['y'];
        const newIndexes: IndexDefinitions = {};

        vi.setSystemTime(10);

        const client1: ClientV5 = {
          clientGroupID: clientGroupID1,
          headHash,
          heartbeatTimestampMs: 1,
          tempRefreshHash: null,
        };
        const clientGroup1: ClientGroup = {
          headHash,
          lastServerAckdMutationIDs: {[clientID1]: 0},
          mutationIDs: {[clientID1]: 1},
          indexes: initialIndexes,
          mutatorNames: initialMutatorNames,
          disabled: false,
        };

        await withWrite(perdag, async write => {
          await setClient(clientID1, client1, write);
          await setClientGroup(clientGroupID1, clientGroup1, write);
        });

        const clientID2 = makeClientID();
        const [client2, client2HeadHash, clientMap] = await initClientV6(
          clientID2,
          lc,
          perdag,
          newMutatorNames,
          newIndexes,
          formatVersion,
          enableClientGroupForking,
        );
        expect(clientID2).not.toBe(clientID1);
        assertClientV6(client2);
        const clientGroupID2 = client2.clientGroupID;
        expect(clientGroupID2).not.toBe(clientGroupID1);
        expect(clientMap.size).toBe(2);

        expect(client2HeadHash, 'Forked so we need a new head').not.toBe(
          clientGroup1.headHash,
        );
        expect(client2.refreshHashes).toEqual([client2HeadHash]);
        expect(client2.heartbeatTimestampMs).toBe(10);
        expect(client2.persistHash).toBeNull();

        await withRead(perdag, async read => {
          const clientGroup2 = await getClientGroup(clientGroupID2, read);
          expect(clientGroup2).toEqual({
            headHash: client2HeadHash,
            indexes: newIndexes,
            mutatorNames: newMutatorNames,
            lastServerAckdMutationIDs: {},
            mutationIDs: {},
            disabled: false,
          });
          const clientGroup2HeadCommit = await commitFromHash(
            client2HeadHash,
            read,
          );
          if (enableClientGroupForking) {
            expect(clientGroup2HeadCommit.valueHash).toBe(
              clientGroup1Snapshot.valueHash,
            );
            expect(clientGroup2HeadCommit.meta.basisHash).toBe(
              clientGroup1Snapshot.meta.basisHash,
            );
          } else {
            expect(clientGroup2HeadCommit.valueHash).not.toBe(
              clientGroup1Snapshot.valueHash,
            );
            expect(clientGroup2HeadCommit.meta.basisHash).not.toBe(
              clientGroup1Snapshot.meta.basisHash,
            );
            expect(
              await new BTreeRead(
                read,
                formatVersion,
                clientGroup2HeadCommit.valueHash,
              ).isEmpty(),
            ).toBe(true);
          }
        });
      });

      test('fork snapshot due to incompatible index names - reuse index maps', async () => {
        const formatVersion = FormatVersion.Latest;
        const lc = new LogContext();

        const perdag = new TestStore();
        const clientID1 = 'client-id-1';
        const clientGroupID1 = 'client-group-id-1';
        const b = new ChainBuilder(perdag);

        const initialIndexes: IndexDefinitions = {
          a1: {jsonPointer: '', prefix: 'a'},
        };
        await b.addGenesis(clientID1, initialIndexes);
        const newMutatorNames = ['x'];
        const newIndexes: IndexDefinitions = {
          a2: {jsonPointer: '', prefix: 'a'},
          b: {jsonPointer: ''},
        };

        const clientGroup1Snapshot = await b.addSnapshot(
          [
            ['a', 'b'],
            ['c', 'd'],
          ],
          clientID1,
          1,
          {[clientID1]: 10},
        );
        await b.addLocal(clientID1, []);
        const headHash = b.chain[2].chunk.hash;
        const initialMutatorNames = ['x'];

        vi.setSystemTime(10);

        const client1: ClientV5 = {
          clientGroupID: clientGroupID1,
          headHash,
          heartbeatTimestampMs: 1,
          tempRefreshHash: null,
        };
        const clientGroup1: ClientGroup = {
          headHash,
          lastServerAckdMutationIDs: {[clientID1]: 0},
          mutationIDs: {[clientID1]: 1},
          indexes: initialIndexes,
          mutatorNames: initialMutatorNames,
          disabled: false,
        };

        await withWrite(perdag, async write => {
          await setClient(clientID1, client1, write);
          await setClientGroup(clientGroupID1, clientGroup1, write);
        });

        const clientID2 = makeClientID();
        const [client2, client2HeadHash, clientMap] = await initClientV6(
          clientID2,
          lc,
          perdag,
          newMutatorNames,
          newIndexes,
          formatVersion,
          enableClientGroupForking,
        );
        expect(clientID2).not.toBe(clientID1);
        assertClientV6(client2);
        const clientGroupID2 = client2.clientGroupID;
        expect(clientGroupID2).not.toBe(clientGroupID1);
        expect(clientMap.size).toBe(2);

        expect(client2HeadHash, 'Forked so we need a new head').not.toBe(
          client1.headHash,
        );
        expect(client2.refreshHashes).toEqual([client2HeadHash]);
        expect(client2.heartbeatTimestampMs).toBe(10);
        expect(client2.persistHash).toBeNull();

        const clientGroup2 = await withRead(perdag, read =>
          getClientGroup(clientGroupID2, read),
        );
        expect(clientGroup2).toEqual({
          headHash: client2HeadHash,
          indexes: newIndexes,
          mutatorNames: newMutatorNames,
          lastServerAckdMutationIDs: {},
          mutationIDs: {},
          disabled: false,
        });

        await withRead(perdag, async read => {
          const c1 = await commitFromHash(client1.headHash, read);
          expect(c1.chunk.data.indexes).length(1);

          const c2 = await commitFromHash(client2HeadHash, read);
          expect(c2.chunk.data.indexes).length(2);

          if (enableClientGroupForking) {
            expect(c2.valueHash).toBe(clientGroup1Snapshot.valueHash);
            expect(c2.meta.basisHash).toBe(clientGroup1Snapshot.meta.basisHash);
            expect(c1.chunk.data.indexes[0].valueHash).toBe(
              c2.chunk.data.indexes[0].valueHash,
            );
            expect(c1.chunk.data.indexes[0].valueHash).not.toBe(
              c2.chunk.data.indexes[1].valueHash,
            );
          } else {
            expect(c2.valueHash).not.toBe(clientGroup1Snapshot.valueHash);
            expect(c2.meta.basisHash).not.toBe(
              clientGroup1Snapshot.meta.basisHash,
            );
            expect(
              await new BTreeRead(read, formatVersion, c2.valueHash).isEmpty(),
            ).toBe(true);
          }
        });
      });
    });
  }
  makeSuite(true);
  makeSuite(false);
});
