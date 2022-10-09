import {expect} from '@esm-bundle/chai';
import {SinonFakeTimers, useFakeTimers} from 'sinon';
import {assert} from '../asserts';
import * as dag from '../dag/mod';
import * as db from '../db/mod';
import {
  addGenesis,
  addIndexChange,
  addLocal,
  addSnapshot,
  Chain,
  getChunkSnapshot,
} from '../db/test-helpers';
import {assertHash, makeNewFakeHashFunction} from '../hash';
import {getClient, ClientStateNotFoundError, assertClientSDD} from './clients';
import {addSyncSnapshot} from '../sync/test-helpers';
import {persist} from './persist';
import {gcClients} from './client-gc.js';
import {initClientWithClientID} from './clients-test-helpers.js';
import {assertSnapshotMeta} from '../db/commit.js';
import {LogContext} from '@rocicorp/logger';

let clock: SinonFakeTimers;
setup(() => {
  clock = useFakeTimers(123456789);
});

teardown(() => {
  clock.restore();
});

async function assertSameDagData(
  clientID: sync.ClientID,
  memdag: dag.LazyStore,
  perdag: dag.Store,
): Promise<void> {
  const memdagHeadHash = await memdag.withRead(async dagRead => {
    const headHash = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
    assert(headHash);
    expect(await memdag.isMemOnlyChunkHash(headHash)).to.be.false;
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
    assertSnapshotMeta(meta);

    expect(client.lastServerAckdMutationID).to.equal(meta.lastMutationID);
  });
}

suite('persist on top of different kinds of commits', () => {
  if (DD31) {
    // persitDD31 is tested in persist-dd31.test.ts
    return;
  }
  let memdag: dag.LazyStore,
    perdag: dag.TestStore,
    chain: Chain,
    testPersist: () => Promise<void>,
    clientID: sync.ClientID;

  setup(async () => {
    ({memdag, perdag, chain, testPersist, clientID} = setupPersistTest());
    await initClientWithClientID(clientID, perdag);
    await addGenesis(chain, memdag, clientID);
  });

  teardown(async () => {
    await testPersist();
  });

  test('Genesis only', async () => {
    // all the required work is done in setup/teardown.
  });

  test('local', async () => {
    await addLocal(chain, memdag, clientID);
  });

  test('snapshot', async () => {
    await addSnapshot(
      chain,
      memdag,
      [
        ['a', 0],
        ['b', 1],
        ['c', 2],
      ],
      clientID,
    );
  });

  test('local + syncSnapshot', async () => {
    await addLocal(chain, memdag, clientID);
    await addSyncSnapshot(chain, memdag, 1, clientID);
  });

  test('local + local', async () => {
    await addLocal(chain, memdag, clientID);
    await addLocal(chain, memdag, clientID);
  });

  test('local on to of a persisted local', async () => {
    await addLocal(chain, memdag, clientID);
    await testPersist();
    await addLocal(chain, memdag, clientID);
  });

  test('local * 3', async () => {
    await addLocal(chain, memdag, clientID);
    await addLocal(chain, memdag, clientID);
    await addLocal(chain, memdag, clientID);
  });

  test('local + snapshot', async () => {
    await addLocal(chain, memdag, clientID);
    await addSnapshot(chain, memdag, [['changed', 3]], clientID);
  });

  test('local + snapshot + local', async () => {
    await addLocal(chain, memdag, clientID);
    await addSnapshot(chain, memdag, [['changed', 4]], clientID);
    await addLocal(chain, memdag, clientID);
  });

  test('local + indexChange', async () => {
    await addLocal(chain, memdag, clientID);
    await addIndexChange(chain, memdag, clientID);
  });
});

test('We get a MissingClientException during persist if client is missing', async () => {
  if (DD31) {
    // persitDD31 is tested in persist-dd31.test.ts
    return;
  }
  const {memdag, perdag, chain, testPersist, clientID} = setupPersistTest();
  await initClientWithClientID(clientID, perdag);

  await addGenesis(chain, memdag, clientID);
  await addLocal(chain, memdag, clientID);
  await testPersist();

  await addLocal(chain, memdag, clientID);

  await clock.tickAsync(14 * 24 * 60 * 60 * 1000);

  // Remove the client from the clients map.
  await gcClients('dummy', perdag);

  let err;
  try {
    await persist(
      new LogContext(),
      clientID,
      memdag,
      memdag,
      perdag,
      {},
      () => false,
    );
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

  const clientID = 'client-id';
  const chain: Chain = [];

  const testPersist = async () => {
    await persist(
      new LogContext(),
      clientID,
      memdag,
      memdag,
      perdag,
      {},
      () => false,
    );
    await assertSameDagData(clientID, memdag, perdag);
    await assertClientMutationIDsCorrect(clientID, perdag);
  };
  return {memdag, perdag, chain, testPersist, clientID};
}
