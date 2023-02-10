import {expect} from '@esm-bundle/chai';
import {assertNotUndefined} from '../asserts';
import {BTreeRead} from '../btree/read';
import * as dag from '../dag/mod';
import {fromChunk, SnapshotMeta} from '../db/commit';
import {assertHash, fakeHash, newTempHash} from '../hash';
import {
  ClientMap,
  getClient,
  getClients,
  initClient,
  noUpdates,
  updateClients,
} from './clients';
import {SinonFakeTimers, useFakeTimers} from 'sinon';
import {
  addGenesis,
  addIndexChange,
  addLocal,
  addSnapshot,
  Chain,
} from '../db/test-helpers';
import {makeClient, setClients} from './clients-test-helpers';

let clock: SinonFakeTimers;
setup(() => {
  clock = useFakeTimers(0);
});

teardown(() => {
  clock.restore();
});

test('getClients with no existing ClientMap in dag store', async () => {
  const dagStore = new dag.TestStore();
  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap.size).to.equal(0);
  });
});

test('updateClients and getClients', async () => {
  const dagStore = new dag.TestStore();
  const clientMap = new Map(
    Object.entries({
      client1: makeClient({
        heartbeatTimestampMs: 1000,
        headHash: fakeHash('headclient1'),
      }),
      client2: makeClient({
        heartbeatTimestampMs: 3000,
        headHash: fakeHash('headclient2'),
      }),
    }),
  );
  await setClients(clientMap, dagStore);

  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });
});

test('updateClients and getClients sequence', async () => {
  const dagStore = new dag.TestStore();
  const clientMap1 = new Map(
    Object.entries({
      client1: makeClient({
        heartbeatTimestampMs: 1000,
        headHash: fakeHash('headclient1'),
      }),
      client2: makeClient({
        heartbeatTimestampMs: 3000,
        headHash: fakeHash('headclient2'),
      }),
    }),
  );

  const clientMap2 = new Map(
    Object.entries({
      client3: makeClient({
        heartbeatTimestampMs: 4000,
        headHash: fakeHash('headclient3'),
      }),
    }),
  );
  await setClients(clientMap1, dagStore);

  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap1 = await getClients(read);
    expect(readClientMap1).to.deep.equal(clientMap1);
  });

  await setClients(clientMap2, dagStore);

  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap2 = await getClients(read);
    expect(readClientMap2).to.deep.equal(clientMap2);
  });
});

test('updateClients properly manages refs to client heads when clients are removed and added', async () => {
  const dagStore = new dag.TestStore();
  const client1HeadHash = fakeHash('headclient1');
  const client2HeadHash = fakeHash('headclient2');

  const clientMap1 = new Map(
    Object.entries({
      client1: makeClient({
        heartbeatTimestampMs: 1000,
        headHash: client1HeadHash,
      }),
      client2: makeClient({
        heartbeatTimestampMs: 3000,
        headHash: client2HeadHash,
      }),
    }),
  );

  const client3HeadHash = fakeHash('headclient3');
  const clientMap2 = new Map(
    Object.entries({
      client3: makeClient({
        heartbeatTimestampMs: 4000,
        headHash: client3HeadHash,
      }),
    }),
  );
  await setClients(clientMap1, dagStore);

  await dagStore.withRead(async (read: dag.Read) => {
    const clientsHash = await read.getHead('clients');
    assertHash(clientsHash);
    const clientsChunk = await read.getChunk(clientsHash);
    expect(clientsChunk?.meta).to.deep.equal([
      client1HeadHash,
      client2HeadHash,
    ]);
  });
  await setClients(clientMap2, dagStore);

  await dagStore.withRead(async (read: dag.Read) => {
    const clientsHash = await read.getHead('clients');
    assertHash(clientsHash);
    const clientsChunk = await read.getChunk(clientsHash);
    expect(clientsChunk?.meta).to.deep.equal([client3HeadHash]);
  });
});

test("updateClients properly manages refs to client heads when a client's head changes", async () => {
  const dagStore = new dag.TestStore();
  const client1V1HeadHash = fakeHash('headclient1');
  const client1V2HeadHash = fakeHash('headclient1v2');
  const client2HeadHash = fakeHash('headclient2');

  const client1V1 = makeClient({
    heartbeatTimestampMs: 1000,
    headHash: client1V1HeadHash,
  });
  const client1V2 = makeClient({
    heartbeatTimestampMs: 2000,
    headHash: client1V2HeadHash,
  });
  const client2 = makeClient({
    heartbeatTimestampMs: 3000,
    headHash: client2HeadHash,
  });

  const clientMap1 = new Map(
    Object.entries({
      client1: client1V1,
      client2,
    }),
  );

  await setClients(clientMap1, dagStore);

  await dagStore.withRead(async (read: dag.Read) => {
    const clientsHash = await read.getHead('clients');
    assertHash(clientsHash);
    const clientsChunk = await read.getChunk(clientsHash);
    expect(clientsChunk?.meta).to.deep.equal([
      client1V1HeadHash,
      client2HeadHash,
    ]);
  });

  await setClients(
    new Map(
      Object.entries({
        client1: client1V2,
        client2,
      }),
    ),
    dagStore,
  );

  await dagStore.withRead(async (read: dag.Read) => {
    const clientsHash = await read.getHead('clients');
    assertHash(clientsHash);
    const clientsChunk = await read.getChunk(clientsHash);
    expect(clientsChunk?.meta).to.deep.equal([
      client1V2HeadHash,
      client2HeadHash,
    ]);
  });
});

test('getClient', async () => {
  const dagStore = new dag.TestStore();
  const client1 = makeClient({
    heartbeatTimestampMs: 1000,
    headHash: fakeHash('headclient1'),
  });
  const clientMap = new Map(
    Object.entries({
      client1,
      client2: makeClient({
        heartbeatTimestampMs: 3000,
        headHash: fakeHash('headclient2'),
      }),
    }),
  );
  await setClients(clientMap, dagStore);

  await dagStore.withRead(async (read: dag.Read) => {
    const readClient1 = await getClient('client1', read);
    expect(readClient1).to.deep.equal(client1);
  });
});

test('updateClients throws error if any client headHash is a temp hash', async () => {
  const dagStore = new dag.TestStore();
  const client1 = makeClient({
    heartbeatTimestampMs: 1000,
    headHash: fakeHash('headclient1'),
  });
  const client2 = makeClient({
    heartbeatTimestampMs: 3000,
    headHash: fakeHash('headclient2'),
  });
  const clientMap = new Map(
    Object.entries({
      client1,
      client2,
    }),
  );

  await setClients(clientMap, dagStore);

  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });

  const clientMapWTempHash = new Map(
    Object.entries({
      client1,
      client2: makeClient({
        heartbeatTimestampMs: 3000,
        headHash: newTempHash(),
      }),
    }),
  );

  let e;
  try {
    await setClients(clientMapWTempHash, dagStore);
  } catch (ex) {
    e = ex;
  }
  expect(e).to.be.instanceOf(Error);

  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });
});

test('updateClients throws errors if clients head exist but the chunk it refrences does not', async () => {
  const dagStore = new dag.TestStore();
  await dagStore.withWrite(async (write: dag.Write) => {
    await write.setHead('clients', fakeHash('randomstuff'));
    await write.commit();
  });
  await dagStore.withRead(async (read: dag.Read) => {
    let e;
    try {
      await getClients(read);
    } catch (ex) {
      e = ex;
    }
    expect(e).to.be.instanceOf(Error);
  });
});

test('updateClients is a noop if noUpdates is returned from update', async () => {
  const dagStore = new dag.TestStore();
  const clientMap = new Map(
    Object.entries({
      client1: makeClient({
        heartbeatTimestampMs: 1000,
        headHash: fakeHash('headclient1'),
      }),
      client2: makeClient({
        heartbeatTimestampMs: 3000,
        headHash: fakeHash('headclient2'),
      }),
    }),
  );
  await setClients(clientMap, dagStore);
  await updateClients(_ => noUpdates, dagStore);
  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });
});

test('updateClients puts chunksToPut returned by update', async () => {
  const dagStore = new dag.TestStore();
  const chunksToPut = [
    dag.createChunkWithHash(fakeHash('chunktoput1'), 'chunktoPut1', []),
    dag.createChunkWithHash(fakeHash('chunktoput2'), 'chunkToPut2', [
      fakeHash('chunktoput1'),
    ]),
  ];
  const clientMap = new Map(
    Object.entries({
      client1: makeClient({
        heartbeatTimestampMs: 1000,
        headHash: chunksToPut[1].hash,
      }),
    }),
  );
  const update = async (_: ClientMap) => {
    return {clients: clientMap, chunksToPut};
  };

  await updateClients(update, dagStore);
  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });
  await dagStore.withRead(async (read: dag.Read) => {
    expect(await read.getChunk(chunksToPut[0].hash)).to.deep.equal(
      chunksToPut[0],
    );
    expect(await read.getChunk(chunksToPut[1].hash)).to.deep.equal(
      chunksToPut[1],
    );
  });
});

test('updateClients with conflict during update (i.e. testing race case with retry)', async () => {
  const dagStore = new dag.TestStore();
  const client1 = makeClient({
    heartbeatTimestampMs: 1000,
    headHash: fakeHash('headclient1'),
  });
  const client2 = makeClient({
    heartbeatTimestampMs: 3000,
    headHash: fakeHash('headclient2'),
  });
  const clientMap = new Map(
    Object.entries({
      client1,
      client2,
    }),
  );

  const client3 = makeClient({
    heartbeatTimestampMs: 5000,
    headHash: fakeHash('headclient3'),
  });
  const clientMap2 = new Map(clientMap).set('client3', client3);

  await setClients(clientMap, dagStore);

  const chunksToPut = [
    dag.createChunkWithHash(fakeHash('chunktoput1'), 'chunkToPut1', []),
    dag.createChunkWithHash(fakeHash('chunktoput2'), 'chunkToPut2', [
      fakeHash('chunktoput1'),
    ]),
  ];
  const client4 = makeClient({
    heartbeatTimestampMs: 7000,
    headHash: chunksToPut[1].hash,
  });

  let updateCallCount = 0;
  const update = async (clients: ClientMap) => {
    updateCallCount++;
    expect(updateCallCount).to.be.lessThan(
      3,
      'Expect update to only be called twice',
    );
    if (updateCallCount === 1) {
      // create conflict
      await setClients(clientMap2, dagStore);
    }
    if (updateCallCount === 2) {
      expect(clients).to.deep.equal(clientMap2);
    }
    // chunksToPut are not written until there is not conflict.
    await dagStore.withRead(async (read: dag.Read) => {
      expect(await read.getChunk(chunksToPut[0].hash)).to.be.undefined;
      expect(await read.getChunk(chunksToPut[1].hash)).to.be.undefined;
    });
    return {clients: new Map(clients).set('client4', client4), chunksToPut};
  };

  const expectedClientMap = new Map(
    Object.entries({
      client1,
      client2,
      client3,
      client4,
    }),
  );
  const updatedClients = await updateClients(update, dagStore);
  expect(updatedClients).to.deep.equal(expectedClientMap);
  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(expectedClientMap);
    expect(await read.getChunk(chunksToPut[0].hash)).to.deep.equal(
      chunksToPut[0],
    );
    expect(await read.getChunk(chunksToPut[1].hash)).to.deep.equal(
      chunksToPut[1],
    );
  });
});

test('updateClients where update return noUpdates after conflict during update', async () => {
  const dagStore = new dag.TestStore();
  const client1 = makeClient({
    heartbeatTimestampMs: 1000,
    headHash: fakeHash('headclient1'),
  });
  const client2 = makeClient({
    heartbeatTimestampMs: 3000,
    headHash: fakeHash('headclient2'),
  });
  const clientMap = new Map(
    Object.entries({
      client1,
      client2,
    }),
  );

  const client3 = makeClient({
    heartbeatTimestampMs: 5000,
    headHash: fakeHash('headclient3'),
  });
  const clientMap2 = new Map(clientMap).set('client3', client3);

  await setClients(clientMap, dagStore);

  const chunksToPut = [
    dag.createChunkWithHash(fakeHash('chunktoput1'), 'chunkToPut1', []),
    dag.createChunkWithHash(fakeHash('chunktoput2'), 'chunkToPut2', [
      fakeHash('chunktoput1'),
    ]),
  ];
  const client4 = makeClient({
    heartbeatTimestampMs: 7000,
    headHash: chunksToPut[1].hash,
  });

  let updateCallCount = 0;
  const update = async (clients: ClientMap) => {
    updateCallCount++;
    expect(updateCallCount).to.be.lessThan(
      3,
      'Expect update to only be called twice',
    );
    if (updateCallCount === 1) {
      // create conflict
      await setClients(clientMap2, dagStore);
      return {clients: new Map(clients).set('client4', client4), chunksToPut};
    }
    if (updateCallCount === 2) {
      expect(clients).to.deep.equal(clientMap2);
    }
    return noUpdates;
  };

  // Client 4 never added.
  const expectedClientMap = new Map(
    Object.entries({
      client1,
      client2,
      client3,
    }),
  );
  const updatedClients = await updateClients(update, dagStore);
  expect(updatedClients).to.deep.equal(expectedClientMap);
  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(expectedClientMap);
    // chunks never put
    expect(await read.getChunk(chunksToPut[0].hash)).to.be.undefined;
    expect(await read.getChunk(chunksToPut[1].hash)).to.be.undefined;
  });
});

test('updateClients throws errors if chunk pointed to by clients head does not contain a valid ClientMap', async () => {
  const dagStore = new dag.TestStore();
  await dagStore.withWrite(async (write: dag.Write) => {
    const headHash = fakeHash('headclient1');
    const chunk = write.createChunk(
      {
        heartbeatTimestampMs: 'this should be a number',
        headHash,
      },
      [headHash],
    );

    await Promise.all([
      write.putChunk(chunk),
      write.setHead('clients', chunk.hash),
    ]);
    await write.commit();
  });
  await dagStore.withRead(async (read: dag.Read) => {
    let e;
    try {
      await getClients(read);
    } catch (ex) {
      e = ex;
    }
    expect(e).to.be.instanceOf(Error);
  });
});

test('initClient creates new empty snapshot when no existing snapshot to bootstrap from', async () => {
  const dagStore = new dag.TestStore();
  clock.tick(4000);
  const [clientID, client, clients] = await initClient(dagStore);

  expect(clients).to.deep.equal(
    new Map(
      Object.entries({
        [clientID]: client,
      }),
    ),
  );

  await dagStore.withRead(async (dagRead: dag.Read) => {
    // New client was added to the client map.
    expect(await getClient(clientID, dagRead)).to.deep.equal(client);
    expect(client.heartbeatTimestampMs).to.equal(clock.now);
    expect(client.mutationID).to.equal(0);
    expect(client.lastServerAckdMutationID).to.equal(0);

    // New client's head hash points to an empty snapshot with an empty btree.
    const headChunk = await dagRead.getChunk(client.headHash);
    assertNotUndefined(headChunk);
    const commit = fromChunk(headChunk);
    expect(commit.isSnapshot()).to.be.true;
    const snapshotMeta = commit.meta as SnapshotMeta;
    expect(snapshotMeta.basisHash).to.be.null;
    expect(snapshotMeta.cookieJSON).to.be.null;
    expect(await commit.getMutationID(clientID, dagRead)).to.equal(0);
    expect(commit.indexes).to.be.empty;
    expect(await new BTreeRead(dagRead, commit.valueHash).isEmpty()).to.be.true;
  });
});

test('initClient bootstraps from base snapshot of client with highest heartbeat', async () => {
  const clientID = 'client-id';
  const dagStore = new dag.TestStore();

  const chain: Chain = [];
  await addGenesis(chain, dagStore, clientID);
  await addSnapshot(chain, dagStore, [['foo', 'bar']], clientID);
  await addLocal(chain, dagStore, clientID);
  const client1HeadCommit = chain[chain.length - 1];
  await addIndexChange(chain, dagStore, clientID);
  await addSnapshot(chain, dagStore, [['fuz', 'bang']], clientID);
  const client2BaseSnapshotCommit = chain[chain.length - 1];
  await addLocal(chain, dagStore, clientID);
  await addLocal(chain, dagStore, clientID);
  const client2HeadCommit = chain[chain.length - 1];

  const clientMap = new Map(
    Object.entries({
      client1: makeClient({
        heartbeatTimestampMs: 1000,
        headHash: client1HeadCommit.chunk.hash,
      }),
      client2: makeClient({
        heartbeatTimestampMs: 3000,
        headHash: client2HeadCommit.chunk.hash,
      }),
    }),
  );
  await setClients(clientMap, dagStore);

  clock.tick(4000);
  const [clientID2, client, clients] = await initClient(dagStore);

  expect(clients).to.deep.equal(new Map(clientMap).set(clientID2, client));

  await dagStore.withRead(async (dagRead: dag.Read) => {
    // New client was added to the client map.
    expect(await getClient(clientID2, dagRead)).to.deep.equal(client);
    expect(client.heartbeatTimestampMs).to.equal(clock.now);
    expect(client.mutationID).to.equal(0);
    expect(client.lastServerAckdMutationID).to.equal(0);

    // New client's head hash points to a commit that matches
    // client2BaseSnapshotCommit but with a local mutation id of 0.
    const headChunk = await dagRead.getChunk(client.headHash);
    assertNotUndefined(headChunk);
    const commit = fromChunk(headChunk);
    expect(commit.isSnapshot()).to.be.true;
    const snapshotMeta = commit.meta as SnapshotMeta;
    expect(client2BaseSnapshotCommit.isSnapshot()).to.be.true;
    const client2BaseSnapshotMeta =
      client2BaseSnapshotCommit.meta as SnapshotMeta;

    expect(snapshotMeta.basisHash).to.equal(client2BaseSnapshotMeta.basisHash);
    expect(snapshotMeta.cookieJSON).to.equal(
      client2BaseSnapshotMeta.cookieJSON,
    );
    expect(await commit.getMutationID(clientID2, dagRead)).to.equal(0);
    expect(commit.indexes).to.not.be.empty;
    expect(commit.indexes).to.deep.equal(client2BaseSnapshotCommit.indexes);
    expect(commit.valueHash).to.equal(client2BaseSnapshotCommit.valueHash);
  });
});
