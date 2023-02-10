import {expect} from '@esm-bundle/chai';
import {SinonFakeTimers, useFakeTimers} from 'sinon';
import {assert} from '../asserts';
import type {Node} from '../btree/node';
import * as dag from '../dag/mod';
import * as sync from '../sync/mod';
import * as db from '../db/mod';
import {
  addGenesis,
  addIndexChange,
  addLocal,
  addSnapshot,
  Chain,
} from '../db/test-helpers';
import {
  assertHash,
  assertNotTempHash,
  Hash,
  isTempHash,
  makeNewFakeHashFunction,
} from '../hash';
import type {Value} from '../kv/store';
import type {ClientID} from '../sync/client-id';
import {getClient, ClientStateNotFoundError} from './clients';
import {addSyncSnapshot} from '../sync/test-helpers';
import {persist} from './persist';
import {gcClients} from './client-gc.js';
import {initClientWithClientID} from './clients-test-helpers.js';

let clock: SinonFakeTimers;
setup(() => {
  clock = useFakeTimers(123456789);
});

teardown(() => {
  clock.restore();
});

async function assertSameDagData(
  clientID: ClientID,
  memdag: dag.TestStore,
  perdag: dag.TestStore,
): Promise<void> {
  const memdagHeadHash = await memdag.withRead(async dagRead => {
    const headHash = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
    expect(isTempHash(headHash)).to.be.false;
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
  clientID: ClientID,
  perdag: dag.TestStore,
): Promise<void> {
  await perdag.withRead(async dagRead => {
    const client = await getClient(clientID, dagRead);
    assert(client);
    const headCommit = await db.commitFromHash(client.headHash, dagRead);
    const baseSnapshotCommit = await db.baseSnapshot(client.headHash, dagRead);
    expect(client.mutationID).to.equal(headCommit.mutationID);
    expect(client.lastServerAckdMutationID).to.equal(
      baseSnapshotCommit.meta.lastMutationID,
    );
  });
}

class ChunkSnapshotVisitor extends db.Visitor {
  snapshot: Record<string, Value> = {};

  override visitCommitChunk(
    chunk: dag.Chunk<db.CommitData<db.Meta>>,
  ): Promise<void> {
    this.snapshot[chunk.hash.toString()] = chunk.data;
    return super.visitCommitChunk(chunk);
  }

  override visitBTreeNodeChunk(chunk: dag.Chunk<Node>): Promise<void> {
    this.snapshot[chunk.hash.toString()] = chunk.data;
    return super.visitBTreeNodeChunk(chunk);
  }
}

async function getChunkSnapshot(
  dagStore: dag.Store,
  hash: Hash,
): Promise<Record<string, Value>> {
  return dagStore.withRead(async dagRead => {
    const v = new ChunkSnapshotVisitor(dagRead);
    await v.visitCommit(hash);
    return v.snapshot;
  });
}

suite('persist on top of different kinds of commits', () => {
  const {memdag, perdag, chain, testPersist, clientID} = setupPersistTest();

  setup(async () => {
    memdag.clear();
    perdag.clear();
    chain.length = 0;
    await initClientWithClientID(clientID, perdag);
    await addGenesis(chain, memdag);
  });

  teardown(async () => {
    await testPersist();
  });

  test('Genesis only', async () => {
    // all the required work is done in setup/teardown.
  });

  test('local', async () => {
    await addLocal(chain, memdag);
  });

  test('snapshot', async () => {
    await addSnapshot(chain, memdag, [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ]);
  });

  test('local + syncSnapshot', async () => {
    await addLocal(chain, memdag);
    await addSyncSnapshot(chain, memdag, 1);
  });

  test('local + local', async () => {
    await addLocal(chain, memdag);
    await addLocal(chain, memdag);
  });

  test('local on to of a persisted local', async () => {
    await addLocal(chain, memdag);
    await testPersist();
    await addLocal(chain, memdag);
  });

  test('local * 3', async () => {
    await addLocal(chain, memdag);
    await addLocal(chain, memdag);
    await addLocal(chain, memdag);
  });

  test('local + snapshot', async () => {
    await addLocal(chain, memdag);
    await addSnapshot(chain, memdag, [['changed', 3]]);
  });

  test('local + snapshot + local', async () => {
    await addLocal(chain, memdag);
    await addSnapshot(chain, memdag, [['changed', 4]]);
    await addLocal(chain, memdag);
  });

  test('local + snapshot + local + syncSnapshot', async () => {
    await addLocal(chain, memdag);
    await addSnapshot(chain, memdag, [['changed', 5]]);
    await addLocal(chain, memdag);
    await addSyncSnapshot(chain, memdag, 3);

    const syncHeadCommitBefore = await memdag.withRead(async dagRead => {
      const h = await dagRead.getHead(sync.SYNC_HEAD_NAME);
      assert(h);
      return db.commitFromHash(h, dagRead);
    });

    expect(
      isTempHash(
        (syncHeadCommitBefore.chunk.data as db.CommitData<db.SnapshotMeta>)
          .valueHash,
      ),
    ).to.be.true;
    await testPersist();

    const syncHeadCommitAfter = await memdag.withRead(async dagRead => {
      const h = await dagRead.getHead(sync.SYNC_HEAD_NAME);
      assert(h);
      return db.commitFromHash(h, dagRead);
    });

    expect(syncHeadCommitBefore.chunk.hash).to.not.equal(
      syncHeadCommitAfter.chunk.hash,
    );

    expect(
      isTempHash(
        (syncHeadCommitAfter.chunk.data as db.CommitData<db.SnapshotMeta>)
          .valueHash,
      ),
    ).to.be.false;
  });

  test('local + indexChange', async () => {
    await addLocal(chain, memdag);
    await addIndexChange(chain, memdag);
  });
});

test('We get a MissingClientException during persist if client is missing', async () => {
  const {memdag, perdag, chain, testPersist, clientID} = setupPersistTest();
  await initClientWithClientID(clientID, perdag);

  await addGenesis(chain, memdag);
  await addLocal(chain, memdag);
  await testPersist();

  await addLocal(chain, memdag);

  await clock.tickAsync(14 * 24 * 60 * 60 * 1000);

  // Remove the client from the clients map.
  await gcClients('dummy', perdag);

  let err;
  try {
    await persist(clientID, memdag, perdag, () => false);
  } catch (e) {
    err = e;
  }
  expect(err)
    .to.be.an.instanceof(ClientStateNotFoundError)
    .property('id', clientID);
});

function setupPersistTest() {
  const memdag = new dag.TestStore(
    undefined,
    makeNewFakeHashFunction('t/memdag'),
    assertHash,
  );
  const perdag = new dag.TestStore(
    undefined,
    makeNewFakeHashFunction('perdag'),
    assertNotTempHash,
  );

  const clientID = 'client-id';
  const chain: Chain = [];

  const testPersist = async () => {
    await persist(clientID, memdag, perdag, () => false);
    await assertSameDagData(clientID, memdag, perdag);
    await assertClientMutationIDsCorrect(clientID, perdag);
  };
  return {memdag, perdag, chain, testPersist, clientID};
}
