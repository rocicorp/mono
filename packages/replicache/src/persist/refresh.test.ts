import {LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import {assert, assertNotUndefined} from '../../../shared/src/asserts.ts';
import type {JSONValue, ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {Entry} from '../btree/node.ts';
import {BTreeWrite} from '../btree/write.ts';
import type {Cookie} from '../cookies.ts';
import {LazyStore} from '../dag/lazy-store.ts';
import type {Store} from '../dag/store.ts';
import {TestStore} from '../dag/test-store.ts';
import type {Commit} from '../db/commit.ts';
import {
  DEFAULT_HEAD_NAME,
  type IndexRecord,
  type LocalMetaDD31,
  type SnapshotMetaDD31,
  baseSnapshotHashFromHash,
  newLocalDD31,
  newSnapshotDD31,
} from '../db/commit.ts';
import {ChainBuilder} from '../db/test-helpers.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {deepFreeze} from '../frozen-json.ts';
import {
  type Hash,
  assertHash,
  fakeHash,
  makeNewFakeHashFunction,
} from '../hash.ts';
import {
  type ClientGroupMap,
  setClientGroup,
  setClientGroups,
} from '../persist/client-groups.ts';
import {
  type ClientV6,
  assertClientV6,
  getClient,
  setClient,
} from '../persist/clients.ts';
import type {ClientID} from '../sync/ids.ts';
import {addData, testSubscriptionsManagerOptions} from '../test-util.ts';
import type {WriteTransaction} from '../transactions.ts';
import type {MutatorDefs} from '../types.ts';
import {withRead, withWriteNoImplicitCommit} from '../with-transactions.ts';
import {refresh} from './refresh.ts';

async function makeChain(
  store: Store,
  clientID: ClientID,
  cookie: number,
  headName: string,
  withLocal = true,
): Promise<{headHash: Hash; chainBuilder: ChainBuilder}> {
  const chainBuilder: ChainBuilder = new ChainBuilder(store, headName);
  await chainBuilder.addGenesis(clientID);
  await chainBuilder.addSnapshot([], clientID, cookie);
  if (withLocal) {
    await chainBuilder.addLocal(clientID, []);
  }
  const headHash = chainBuilder.chain.at(-1)?.chunk.hash;
  assertNotUndefined(headHash);
  return {headHash, chainBuilder};
}

function makeMemdagChain(
  memdag: Store,
  clientID: ClientID,
  cookie: number,
): Promise<{headHash: Hash; chainBuilder: ChainBuilder}> {
  return makeChain(memdag, clientID, cookie, DEFAULT_HEAD_NAME);
}

const PERDAG_TEST_SETUP_HEAD_NAME = 'test-setup-head';
async function makePerdagChainAndSetClientsAndClientGroup(
  perdag: Store,
  clientID: ClientID,
  cookie: number,
  withLocal = true,
): Promise<{headHash: Hash; chainBuilder: ChainBuilder; client: ClientV6}> {
  const {headHash, chainBuilder} = await makeChain(
    perdag,
    clientID,
    cookie,
    PERDAG_TEST_SETUP_HEAD_NAME,
    withLocal,
  );
  return {
    headHash,
    chainBuilder,
    client: await setClientsAndClientGroups(headHash, clientID, perdag),
  };
}

async function setClientsAndClientGroups(
  headHash: Hash,
  clientID: ClientID,
  perdag: Store,
) {
  const clientGroupID = 'client-group-1';
  const clientGroups: ClientGroupMap = new Map([
    [
      clientGroupID,
      {
        headHash,
        indexes: {},
        // Not used
        mutationIDs: {[clientID]: -1},
        // Not used
        lastServerAckdMutationIDs: {[clientID]: -1},
        mutatorNames: [],
        disabled: false,
      },
    ],
  ]);

  const client: ClientV6 = {
    clientGroupID,
    refreshHashes: [fakeHash('abc')],
    // Not used
    heartbeatTimestampMs: -1,
    persistHash: null,
  };

  await withWriteNoImplicitCommit(perdag, async perdagWrite => {
    await setClientGroups(clientGroups, perdagWrite);
    await setClient(clientID, client, perdagWrite);
    await perdagWrite.removeHead(PERDAG_TEST_SETUP_HEAD_NAME);
    await perdagWrite.commit();
  });
  return client;
}

function makeStores() {
  const LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT = 10 * 2 ** 20; // 10 MB
  const chunkHasher = makeNewFakeHashFunction();
  const perdag = new TestStore(undefined, chunkHasher);
  const memdag = new LazyStore(
    perdag,
    LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT,
    chunkHasher,
    assertHash,
  );
  return {perdag, memdag};
}

function mutatorsProxy(): MutatorDefs {
  return new Proxy(
    {},
    {
      get(_target, prop) {
        return async (tx: WriteTransaction, args: JSONValue) => {
          await tx.set(`from ${String(prop)}`, args);
        };
      },
    },
  );
}

function assertRefreshHashes(
  perdag: TestStore,
  clientID: string,
  hashes: readonly (Hash | undefined)[],
) {
  return withRead(perdag, async read => {
    const client = await getClient(clientID, read);
    assert(client, `No client ${clientID}`);
    assertClientV6(client);
    expect(client.refreshHashes).toEqual(hashes);
  });
}

describe('refresh', () => {
  const formatVersion = FormatVersion.Latest;
  test('identical dags', async () => {
    // If the dags are the same then refresh is a no op.
    const {perdag, memdag} = makeStores();
    const clientID = 'client-id-1';
    const mutators = mutatorsProxy();

    await makePerdagChainAndSetClientsAndClientGroup(perdag, clientID, 1);
    await makeMemdagChain(memdag, clientID, 1);

    const refreshResult = await refresh(
      new LogContext(),
      memdag,
      perdag,
      clientID,
      mutators,
      testSubscriptionsManagerOptions,
      () => false,
      formatVersion,
      undefined,
    );
    assert(refreshResult);
    expect(Object.fromEntries(refreshResult.diffs)).toEqual({});
    const hashes = [
      await withRead(memdag, read => read.getHead(DEFAULT_HEAD_NAME)),
    ];
    await assertRefreshHashes(perdag, clientID, hashes);
  });

  test('identical dags, multiple refreshHashes at start', async () => {
    // If the dags are the same then refresh is a no op.
    const {perdag, memdag} = makeStores();
    const clientID = 'client-id-1';
    const mutators = mutatorsProxy();

    await makePerdagChainAndSetClientsAndClientGroup(
      perdag,
      clientID,
      1,
      undefined,
    );
    await withWriteNoImplicitCommit(perdag, async perdagWrite => {
      const client = await getClient(clientID, perdagWrite);
      assertClientV6(client);
      await setClient(
        clientID,
        {
          ...client,
          refreshHashes: [
            ...client.refreshHashes,
            fakeHash('a'),
            fakeHash('b'),
          ],
        },
        perdagWrite,
      );
      await perdagWrite.commit();
    });
    await makeMemdagChain(memdag, clientID, 1);

    const refreshResult = await refresh(
      new LogContext(),
      memdag,
      perdag,
      clientID,
      mutators,
      testSubscriptionsManagerOptions,
      () => false,
      formatVersion,
      undefined,
    );
    assert(refreshResult);
    const hashes = [
      await withRead(memdag, read => read.getHead(DEFAULT_HEAD_NAME)),
    ];

    expect(Object.fromEntries(refreshResult.diffs)).toEqual({});

    await assertRefreshHashes(perdag, clientID, hashes);
  });

  test('memdag has one more LM', async () => {
    const {perdag, memdag} = makeStores();
    const clientID = 'client-id-1';
    const mutators: MutatorDefs = mutatorsProxy();

    const {chainBuilder: perdagChainBuilder} =
      await makePerdagChainAndSetClientsAndClientGroup(perdag, clientID, 1);

    // Memdag has one more LM than perdag.
    const {chainBuilder: memdagChainBuilder} = await makeMemdagChain(
      memdag,
      clientID,
      1,
    );
    await memdagChainBuilder.addLocal(clientID, []);

    const refreshResult = await refresh(
      new LogContext(),
      memdag,
      perdag,
      clientID,
      mutators,
      testSubscriptionsManagerOptions,
      () => false,
      formatVersion,
      undefined,
    );
    assert(refreshResult);

    expect(Object.fromEntries(refreshResult.diffs)).toEqual({
      '': [
        {
          key: 'from mutator_name_3',
          newValue: [3],
          op: 'add',
        },
      ],
    });

    await assertRefreshHashes(perdag, clientID, [
      perdagChainBuilder.chain.at(-1)?.chunk.hash,
    ]);
  });

  test('memdag has a newer cookie', async () => {
    const {perdag, memdag} = makeStores();
    const clientID = 'client-id-1';
    const mutators: MutatorDefs = mutatorsProxy();

    const {client} = await makePerdagChainAndSetClientsAndClientGroup(
      perdag,
      clientID,
      1,
    );

    // Memdag has a newer cookie than perdag so we abort the refresh
    const {chainBuilder: memdagChainBuilder} = await makeMemdagChain(
      memdag,
      clientID,
      2,
    );
    // Memdag has one more LM than perdag.
    await memdagChainBuilder.addLocal(clientID, []);

    const result = await refresh(
      new LogContext(),
      memdag,
      perdag,
      clientID,
      mutators,
      testSubscriptionsManagerOptions,
      () => false,
      formatVersion,
      undefined,
    );
    expect(result).toBeUndefined();
    await assertRefreshHashes(perdag, clientID, client.refreshHashes);
  });

  test('cookies are equal and perdag has no LM', async () => {
    const {perdag, memdag} = makeStores();
    const clientID = 'client-id-1';
    const mutators: MutatorDefs = mutatorsProxy();

    const {client} = await makePerdagChainAndSetClientsAndClientGroup(
      perdag,
      clientID,
      1,
      false,
    );

    // Memdag has same cookie as perdag, and perdag has no
    // LM so we abort the refresh
    await makeMemdagChain(memdag, clientID, 1);

    const result = await refresh(
      new LogContext(),
      memdag,
      perdag,
      clientID,
      mutators,
      testSubscriptionsManagerOptions,
      () => false,
      formatVersion,
      undefined,
    );
    expect(result).toBeUndefined();
    await assertRefreshHashes(perdag, clientID, client.refreshHashes);
  });

  test('memdag has two more LMs', async () => {
    const {perdag, memdag} = makeStores();
    const clientID = 'client-id-1';
    const mutators: MutatorDefs = mutatorsProxy();

    const {chainBuilder: perdagChainBuilder} =
      await makePerdagChainAndSetClientsAndClientGroup(perdag, clientID, 1);

    // Memdag has two more LM than perdag.
    const {chainBuilder: memdagChainBuilder} = await makeMemdagChain(
      memdag,
      clientID,
      1,
    );
    await memdagChainBuilder.addLocal(clientID, []);
    await memdagChainBuilder.addLocal(clientID, []);

    const refreshResult = await refresh(
      new LogContext(),
      memdag,
      perdag,
      clientID,
      mutators,
      testSubscriptionsManagerOptions,
      () => false,
      formatVersion,
      undefined,
    );
    assert(refreshResult);
    expect(Object.fromEntries(refreshResult.diffs)).toEqual({
      '': [
        {
          key: 'from mutator_name_3',
          newValue: [3],
          op: 'add',
        },
        {
          key: 'from mutator_name_4',
          newValue: [4],
          op: 'add',
        },
      ],
    });
    await assertRefreshHashes(perdag, clientID, [
      perdagChainBuilder.chain.at(-1)?.chunk.hash,
    ]);
  });

  test('perdag has LM from different clients', async () => {
    const {perdag, memdag} = makeStores();
    const clientID1 = 'client-id-1';
    const clientID2 = 'client-id-2';

    const mutators: MutatorDefs = mutatorsProxy();

    const perdagChainBuilder: ChainBuilder = new ChainBuilder(
      perdag,
      PERDAG_TEST_SETUP_HEAD_NAME,
    );
    await perdagChainBuilder.addGenesis(clientID1);
    await perdagChainBuilder.addSnapshot([], clientID1, 1, {
      [clientID1]: 0,
      [clientID2]: 0,
    });
    await perdagChainBuilder.addLocal(clientID1, []);
    const perdagHeadCommit = await perdagChainBuilder.addLocal(clientID2, []);
    const perdagHeadHash = perdagHeadCommit.chunk.hash;
    await setClientsAndClientGroups(perdagHeadHash, clientID1, perdag);

    const memdagChainBuilder: ChainBuilder = new ChainBuilder(
      memdag,
      DEFAULT_HEAD_NAME,
    );
    await memdagChainBuilder.addGenesis(clientID1);
    await memdagChainBuilder.addSnapshot([], clientID1, 1, {
      [clientID1]: 0,
      [clientID2]: 0,
    });
    await memdagChainBuilder.addLocal(clientID1, []);
    await memdagChainBuilder.addLocal(clientID1, []);

    const refreshResult = await refresh(
      new LogContext(),
      memdag,
      perdag,
      clientID1,
      mutators,
      testSubscriptionsManagerOptions,
      () => false,
      formatVersion,
      undefined,
    );
    assert(refreshResult);
    expect(Object.fromEntries(refreshResult.diffs)).toEqual({
      '': [
        {
          key: 'from mutator_name_3',
          newValue: [3],
          op: 'add',
        },
      ],
    });
    await assertRefreshHashes(perdag, clientID1, [
      perdagChainBuilder.chain.at(-1)?.chunk.hash,
    ]);
  });

  test('new snapshot during refresh', async () => {
    const {perdag, memdag} = makeStores();
    const clientID = 'client-id-1';
    const mutators: MutatorDefs = mutatorsProxy();

    let {client} = await makePerdagChainAndSetClientsAndClientGroup(
      perdag,
      clientID,
      2,
    );
    client = await withWriteNoImplicitCommit(perdag, async perdagWrite => {
      const newClient = {
        ...client,
        refreshHashes: [fakeHash('a'), fakeHash('b')],
      };
      await setClient(clientID, newClient, perdagWrite);
      await perdagWrite.commit();
      return newClient;
    });

    // Memdag has one more LM than perdag.
    const {chainBuilder: memdagChainBuilder} = await makeMemdagChain(
      memdag,
      clientID,
      2,
    );
    await memdagChainBuilder.addLocal(clientID, []);

    // Here we use a brittle way to inject a snapshot in the middle of the refresh
    // algorithm.
    let writeCalls = 0;
    const {write} = perdag;
    perdag.write = async () => {
      if (writeCalls++ === 0) {
        await memdagChainBuilder.addSnapshot([], clientID, 3);
        await memdagChainBuilder.addLocal(clientID, []);
      }
      return write.call(perdag);
    };

    const result = await refresh(
      new LogContext(),
      memdag,
      perdag,
      clientID,
      mutators,
      testSubscriptionsManagerOptions,
      () => false,
      formatVersion,
      undefined,
    );
    expect(result).toBeUndefined();
    await assertRefreshHashes(perdag, clientID, client.refreshHashes);
  });

  test('second perdag write fails', async () => {
    const {perdag, memdag} = makeStores();
    const clientID = 'client-id-1';
    const mutators: MutatorDefs = mutatorsProxy();

    const {chainBuilder: perdagChainBuilder, client: c} =
      await makePerdagChainAndSetClientsAndClientGroup(perdag, clientID, 2);
    const client = await withWriteNoImplicitCommit(
      perdag,
      async perdagWrite => {
        const newClient = {
          ...c,
          refreshHashes: [fakeHash('a'), fakeHash('b')],
        };
        await setClient(clientID, newClient, perdagWrite);
        await perdagWrite.commit();
        return newClient;
      },
    );

    // Memdag has one more LM than perdag.
    const {chainBuilder: memdagChainBuilder} = await makeMemdagChain(
      memdag,
      clientID,
      2,
    );
    await memdagChainBuilder.addLocal(clientID, []);

    let writeCalls = 0;
    const {write} = perdag;
    perdag.write = () => {
      if (writeCalls++ === 1) {
        throw new Error('Test error in second perdag write');
      }
      return write.call(perdag);
    };

    let expectedE;
    try {
      await refresh(
        new LogContext(),
        memdag,
        perdag,
        clientID,
        mutators,
        testSubscriptionsManagerOptions,
        () => false,
        formatVersion,
        undefined,
      );
    } catch (e) {
      expectedE = e;
    }
    expect(expectedE).toBeInstanceOf(Error);
    expect((expectedE as Error).message).toBe(
      'Test error in second perdag write',
    );
    await assertRefreshHashes(perdag, clientID, [
      ...client.refreshHashes,
      perdagChainBuilder.chain.at(-1)?.chunk.hash,
    ]);
  });

  test('greg example', async () => {
    // This sample case was used by Greg to explain DD31 refresh to arv. Here is
    // an extract of that explanation (from Slack)
    //
    // We update the memdags head to the head of the perdag client group The
    // perdag client group may look like
    //
    // perdag:mainHead -> LM {clientID: 1 id: 4 } -> LM {clientID: 2 id: 3 } ->
    // Snapshot { lmids: { 1: 3, 2: 2} }
    //
    // so then lets say client 1 is refreshing and his memdag looks like
    //
    // memdag:main -> LM {clientID: 1 id: 5 }  -> LM {clientID: 1 id: 4 }  ->
    // Snapshot { lmids: { 1: 3, 2: 2} }
    //
    // we create a refresh head
    //
    // medag:refresh = perdag:mainHead, so
    //
    // memdag:refresh -> LM {clientID: 1 id: 4 } -> LM {clientID: 2 id: 3 } ->
    // Snapshot { lmids: { 1: 3, 2: 2} }
    //
    // then we rebase from memdag main any mutations not on refreshHead
    //
    // medag:refresh -> LM {clientID: 1 id: 5 } -> LM {clientID: 1 id: 4 } -> LM
    // {clientID: 2 id: 3 } -> Snapshot { lmids: { 1: 3, 2: 2} }
    //
    // then we set
    //
    // medag:main = medag:refresh
    //
    // medag:main -> LM {clientID: 1 id: 5 } -> LM {clientID: 1 id: 4 } -> LM
    // {clientID: 2 id: 3 } -> Snapshot { lmids: { 1: 3, 2: 2} }

    const {perdag, memdag} = makeStores();

    function makeSnapshot({
      store,
      basisHash = null,
      lastMutationIDs,
      cookieJSON,
      valueHash,
      indexes = [],
    }: {
      store: Store;
      basisHash?: Hash | null;
      lastMutationIDs: Record<ClientID, number>;
      cookieJSON: Cookie;
      valueHash?: Hash;
      indexes?: IndexRecord[];
    }): Promise<Commit<SnapshotMetaDD31>> {
      return withWriteNoImplicitCommit(store, async dagWrite => {
        if (!valueHash) {
          const map = new BTreeWrite(dagWrite, formatVersion);
          valueHash = await map.flush();
        }
        const c = newSnapshotDD31(
          dagWrite.createChunk,
          basisHash,
          lastMutationIDs,
          deepFreeze(cookieJSON),
          valueHash,
          indexes,
        );

        await dagWrite.putChunk(c.chunk);
        await dagWrite.setHead('test', c.chunk.hash);
        await dagWrite.commit();

        return c;
      });
    }

    // oxlint-disable-next-line no-unused-vars -- Used in function parameter default
    let timestampCounter = 0;

    function makeLocalMutation({
      store,
      clientID,
      mutationID,
      basisHash,
      mutatorName,
      mutatorArgsJSON,
      originalHash = null,
      indexes = [],
      valueHash,
      timestamp = timestampCounter++,
      entries = [],
    }: {
      store: Store;
      clientID: ClientID;
      mutationID: number;
      basisHash: Hash;
      mutatorName: string;
      mutatorArgsJSON: JSONValue;
      originalHash?: Hash | null;
      indexes?: readonly IndexRecord[];
      valueHash: Hash;
      timestamp?: number;
      entries?: readonly Entry<ReadonlyJSONValue>[];
    }): Promise<Commit<LocalMetaDD31>> {
      return withWriteNoImplicitCommit(store, async dagWrite => {
        const m = new BTreeWrite(dagWrite, formatVersion, valueHash);
        for (const [k, v] of entries) {
          await m.put(k, deepFreeze(v));
        }
        const newValueHash = await m.flush();

        const c = newLocalDD31(
          dagWrite.createChunk,
          basisHash,
          await baseSnapshotHashFromHash(basisHash, dagWrite),
          mutationID,
          mutatorName,
          deepFreeze(mutatorArgsJSON),
          originalHash,
          newValueHash,
          indexes,
          timestamp,
          clientID,
        );

        await dagWrite.putChunk(c.chunk);
        await dagWrite.setHead('test', c.chunk.hash);
        await dagWrite.commit();

        return c;
      });
    }

    const clientID1 = 'client-id-1';
    const clientID2 = 'client-id-2';
    const clientGroupID = 'client-group-1';

    const s1 = await makeSnapshot({
      store: perdag,
      lastMutationIDs: {[clientID1]: 3, [clientID2]: 2},
      cookieJSON: 1,
    });
    const l1 = await makeLocalMutation({
      store: perdag,
      clientID: clientID2,
      mutationID: 3,
      basisHash: s1.chunk.hash,
      mutatorName: 'addData',
      mutatorArgsJSON: {a: 1},
      valueHash: s1.chunk.data.valueHash,
      // entries: [['a', 1]],
    });
    const l2 = await makeLocalMutation({
      store: perdag,
      clientID: clientID1,
      mutationID: 4,
      basisHash: l1.chunk.hash,
      mutatorName: 'addData',
      mutatorArgsJSON: {b: 2},
      valueHash: l1.chunk.data.valueHash,
      // entries: [['b', 2]],
    });

    await withWriteNoImplicitCommit(perdag, async dagWrite => {
      await setClient(
        clientID1,
        {
          clientGroupID,
          refreshHashes: [l2.chunk.hash],
          heartbeatTimestampMs: 1,
          persistHash: null,
        },
        dagWrite,
      );
      await setClient(
        clientID2,
        {
          clientGroupID,
          refreshHashes: [l2.chunk.hash],
          heartbeatTimestampMs: 2,
          persistHash: null,
        },
        dagWrite,
      );
      await setClientGroup(
        clientGroupID,
        {
          headHash: l2.chunk.hash,
          indexes: {},
          mutationIDs: {[clientID1]: 4, [clientID2]: 3},
          lastServerAckdMutationIDs: {[clientID1]: 0, [clientID2]: 0},
          mutatorNames: ['addData'],
          disabled: false,
        },
        dagWrite,
      );

      await dagWrite.removeHead('test');
      await dagWrite.commit();
    });

    // Memdag
    {
      const s1 = await makeSnapshot({
        store: memdag,
        lastMutationIDs: {[clientID1]: 3, [clientID2]: 2},
        cookieJSON: 1,
      });
      const l1 = await makeLocalMutation({
        store: memdag,
        clientID: clientID1,
        mutationID: 4,
        basisHash: s1.chunk.hash,
        mutatorName: 'addData',
        mutatorArgsJSON: {b: 2},
        valueHash: s1.chunk.data.valueHash,
      });
      const l2 = await makeLocalMutation({
        store: memdag,
        clientID: clientID1,
        mutationID: 5,
        basisHash: l1.chunk.hash,
        mutatorName: 'addData',
        mutatorArgsJSON: {c: 3},
        valueHash: l1.chunk.data.valueHash,
      });

      await withWriteNoImplicitCommit(memdag, async dagWrite => {
        await dagWrite.setHead(DEFAULT_HEAD_NAME, l2.chunk.hash);
        await dagWrite.removeHead('test');
        await dagWrite.commit();
      });
    }

    const refreshResult = await refresh(
      new LogContext(),
      memdag,
      perdag,
      clientID1,
      {
        addData,
      },
      testSubscriptionsManagerOptions,
      () => false,
      formatVersion,
      undefined,
    );
    assert(refreshResult);
    expect(Object.fromEntries(refreshResult.diffs)).toEqual({
      '': [{key: 'c', newValue: 3, op: 'add'}],
    });

    await assertRefreshHashes(perdag, clientID1, [l2.chunk.hash]);
  });
});
