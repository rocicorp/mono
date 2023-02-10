import {expect} from '@esm-bundle/chai';
import {SinonFakeTimers, useFakeTimers} from 'sinon';
import {assert} from '../asserts.js';
import * as sync from '../sync/mod.js';
import * as dag from '../dag/mod.js';
import * as db from '../db/mod.js';
import {ChainBuilder, getChunkSnapshot} from '../db/test-helpers.js';
import {assertHash, Hash, makeNewFakeHashFunction} from '../hash.js';
import {
  getClient,
  ClientStateNotFoundError,
  assertClientSDD,
  CLIENTS_HEAD_NAME,
} from './clients.js';
import {persist} from './persist.js';
import {gcClients} from './client-gc.js';
import {initClientWithClientID} from './clients-test-helpers.js';
import {assertSnapshotMetaSDD} from '../db/commit.js';
import {LogContext} from '@rocicorp/logger';
import sinon from 'sinon';

let clock: SinonFakeTimers;
setup(() => {
  clock = useFakeTimers(123456789);
});

teardown(() => {
  clock.restore();
  sinon.restore();
});

async function assertSameDagData(
  clientID: sync.ClientID,
  memdag: dag.LazyStore,
  perdag: dag.Store,
): Promise<void> {
  const memdagHeadHash = await memdag.withRead(async dagRead => {
    const headHash = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
    assert(headHash);
    expect(dagRead.isMemOnlyChunkHash(headHash)).to.be.false;
    return headHash;
  });
  const perdagClientHash = await perdag.withRead(async dagRead => {
    const client = await getClient(clientID, dagRead);
    assert(client);
    return client.headHash;
  });
  expect(memdagHeadHash).to.equal(perdagClientHash);
  assertHash(memdagHeadHash);

  const memSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);
  const perSnapshot = await getChunkSnapshot(perdag, perdagClientHash);

  expect(memSnapshot).to.deep.equal(perSnapshot);
}
async function assertClientMutationIDsCorrect(
  clientID: sync.ClientID,
  perdag: dag.Store,
): Promise<void> {
  await perdag.withRead(async dagRead => {
    const client = await getClient(clientID, dagRead);
    assertClientSDD(client);
    const headCommit = await db.commitFromHash(client.headHash, dagRead);
    const baseSnapshotCommit = await db.baseSnapshotFromHash(
      client.headHash,
      dagRead,
    );
    expect(client.mutationID).to.equal(
      await headCommit.getMutationID(clientID, dagRead),
    );
    const {meta} = baseSnapshotCommit;
    assertSnapshotMetaSDD(meta);

    expect(client.lastServerAckdMutationID).to.equal(meta.lastMutationID);
  });
}

suite('persist on top of different kinds of commits', () => {
  if (DD31) {
    // persistDD31 is tested in persist-dd31.test.ts
    return;
  }
  let memdag: dag.LazyStore,
    perdag: dag.TestStore,
    b: ChainBuilder,
    testPersist: () => Promise<void>,
    clientID: sync.ClientID;

  setup(async () => {
    ({memdag, perdag, b, testPersist, clientID} = setupPersistTest());
    await initClientWithClientID(clientID, perdag, [], {}, false);
    await b.addGenesis(clientID);
  });

  test('Genesis only', async () => {
    await testPersist();
  });

  test('local', async () => {
    await b.addLocal(clientID);
    await testPersist();
  });

  test('snapshot', async () => {
    await b.addSnapshot(
      [
        ['a', 0],
        ['b', 1],
        ['c', 2],
      ],
      clientID,
    );
    await testPersist();
  });

  test('local + syncSnapshot', async () => {
    await b.addLocal(clientID);
    await b.addSyncSnapshot(1, clientID);
    await testPersist();
  });

  test('local + local', async () => {
    await b.addLocal(clientID);
    await b.addLocal(clientID);
    await testPersist();
  });

  test('local on top of a persisted local', async () => {
    await b.addLocal(clientID);
    await testPersist();
    await b.addLocal(clientID);
    await testPersist();
  });

  test('local * 3', async () => {
    await b.addLocal(clientID);
    await b.addLocal(clientID);
    await b.addLocal(clientID);
    await testPersist();
  });

  test('local + snapshot', async () => {
    await b.addLocal(clientID);
    await b.addSnapshot([['changed', 3]], clientID);
    await testPersist();
  });

  test('local + snapshot + local', async () => {
    await b.addLocal(clientID);
    await b.addSnapshot([['changed', 4]], clientID);
    await b.addLocal(clientID);
    await testPersist();
  });

  test('local + snapshot + local + syncSnapshot', async () => {
    await b.addLocal(clientID);
    await b.addSnapshot([['changed', 5]], clientID);
    await b.addLocal(clientID);
    await b.addSyncSnapshot(3, clientID);

    const syncHeadCommitBefore = await memdag.withRead(async dagRead => {
      const h = await dagRead.getHead(sync.SYNC_HEAD_NAME);
      assert(h);
      return db.commitFromHash(h, dagRead);
    });

    await testPersist();

    const syncHeadCommitAfter = await memdag.withRead(async dagRead => {
      const h = await dagRead.getHead(sync.SYNC_HEAD_NAME);
      assert(h);
      return db.commitFromHash(h, dagRead);
    });

    expect(syncHeadCommitBefore.chunk.hash).to.equal(
      syncHeadCommitAfter.chunk.hash,
    );
  });

  test('local + indexChange', async () => {
    await b.addLocal(clientID);
    await b.addIndexChange(clientID);
    await testPersist();
  });
});

test('We get a MissingClientException during persist if client is missing', async () => {
  if (DD31) {
    // persistDD31 is tested in persist-dd31.test.ts
    return;
  }
  const {memdag, perdag, b, testPersist, clientID} = setupPersistTest();
  await initClientWithClientID(clientID, perdag, [], {}, false);

  await b.addGenesis(clientID);
  await b.addLocal(clientID);
  await testPersist();

  await b.addLocal(clientID);

  await clock.tickAsync(14 * 24 * 60 * 60 * 1000);

  // Remove the client from the clients map.
  await gcClients('dummy', perdag);

  let err;
  try {
    await persist(new LogContext(), clientID, memdag, perdag, {}, () => false);
  } catch (e) {
    err = e;
  }
  expect(err)
    .to.be.an.instanceof(ClientStateNotFoundError)
    .property('id', clientID);
});

function setupPersistTest() {
  const hashFunction = makeNewFakeHashFunction('eda2');
  const perdag = new dag.TestStore(undefined, hashFunction, assertHash);
  const memdag = new dag.LazyStore(
    perdag,
    100 * 2 ** 20, // 100 MB
    hashFunction,
    assertHash,
  );
  const chunksPersistedCalls: Hash[][] = [];
  sinon
    .stub(memdag, 'chunksPersisted')
    .callsFake((chunkHashes: Iterable<Hash>) => {
      const chunkHashesArray = [...chunkHashes];
      chunksPersistedCalls.push(chunkHashesArray);
      return dag.LazyStore.prototype.chunksPersisted.apply(memdag, [
        chunkHashesArray,
      ]);
    });

  const clientID = 'client-id';
  const b = new ChainBuilder(memdag);

  const testPersist = async () => {
    chunksPersistedCalls.length = 0;
    const perdagChunkHashesPrePersist = perdag.chunkHashes();
    await persist(new LogContext(), clientID, memdag, perdag, {}, () => false);

    await assertSameDagData(clientID, memdag, perdag);
    await assertClientMutationIDsCorrect(clientID, perdag);
    const persistedChunkHashes = new Set<Hash>();
    const clientsHeadHash = await perdag.withRead(read => {
      return read.getHead(CLIENTS_HEAD_NAME);
    });
    for (const hash of perdag.chunkHashes()) {
      if (!perdagChunkHashesPrePersist.has(hash) && hash !== clientsHeadHash) {
        persistedChunkHashes.add(hash);
      }
    }
    expect(chunksPersistedCalls.length).to.equal(1);
    expect(new Set(chunksPersistedCalls[0])).to.deep.equal(
      persistedChunkHashes,
    );
  };

  return {memdag, perdag, b, testPersist, clientID};
}
