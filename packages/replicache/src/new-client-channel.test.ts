import {resolver} from '@rocicorp/resolver';
import {expect} from 'chai';
import {sleep} from 'shared/src/sleep.js';
import {
  initNewClientChannel,
  makeChannelNameForTesting,
} from './new-client-channel.js';
import * as dag from './dag/mod.js';
import {withWrite} from './with-transactions.js';
import {setClientGroup} from './persist/client-groups.js';
import {fakeHash} from './hash.js';

function getChannelMessagePromise(replicacheName: string) {
  const channel = new BroadcastChannel(
    makeChannelNameForTesting(replicacheName),
  );
  const messageResolver = resolver();
  channel.onmessage = e => {
    messageResolver.resolve(e.data);
  };
  return messageResolver.promise;
}

suite('initNewClientChannel', () => {
  test('sends client group ID and idb name to channel', async () => {
    const replicacheName = 'test-name';
    const idbName = 'test-idb-name';
    const channelMessagePromise = getChannelMessagePromise(replicacheName);
    const controller = new AbortController();
    const clientGroupID = 'test-client-group-id-1';
    initNewClientChannel(
      replicacheName,
      idbName,
      controller.signal,
      clientGroupID,
      true,
      () => undefined,
      new dag.TestStore(),
    );
    expect(await channelMessagePromise).to.deep.equal([clientGroupID, idbName]);
  });

  test("Doesn't send message if client group is not new", async () => {
    const replicacheName = 'test-name';
    const idbName = 'test-idb-name';
    const channelMessagePromise = getChannelMessagePromise(replicacheName);
    const controller = new AbortController();
    const clientGroupID = 'test-client-group-id-1';
    initNewClientChannel(
      replicacheName,
      idbName,
      controller.signal,
      clientGroupID,
      false,
      () => undefined,
      new dag.TestStore(),
    );

    const sentinel = Symbol();
    const res = await Promise.race([
      channelMessagePromise,
      sleep(10).then(() => sentinel),
    ]);
    expect(res).equal(sentinel);

    {
      // And test that we get the message if another client group is created
      const channel = new BroadcastChannel(
        makeChannelNameForTesting(replicacheName),
      );
      const anotherClientGroupID = 'test-client-group-id-2';
      channel.postMessage([anotherClientGroupID]);
      expect(await channelMessagePromise).to.deep.equal([anotherClientGroupID]);
    }
  });

  // test('calls onUpdateNeeded when a different format version is received and does not assert Client schema', async () => {
  //   const replicacheName = 'test-name';
  //   const controller = new AbortController();
  //   const clientID1 = 'client1';
  //   const client1: ClientV5 = {
  //     heartbeatTimestampMs: 111,
  //     headHash: fakeHash('c1'),
  //     tempRefreshHash: null,
  //     clientGroupID: 'branch-1',
  //   };
  //   const clientID2 = 'client2';
  //   // intentionally missing required fields.
  //   const client2 = {} as ClientV5;
  //   const clientID3 = 'client3';
  //   // intentionally missing required fields.
  //   const client3 = {} as ClientV5;
  //   let client1OnUpdateNeededCallCount = 0;
  //   initNewClientChannel(
  //     replicacheName,
  //     TEST_FORMAT_VERSION,
  //     controller.signal,
  //     clientID1,
  //     client1,
  //     () => {
  //       client1OnUpdateNeededCallCount++;
  //     },
  //   );
  //   expect(client1OnUpdateNeededCallCount).to.equal(0);
  //   let client2OnUpdateNeededCallCount = 0;
  //   const channelMessagePromise = getChannelMessagePromise(replicacheName);
  //   initNewClientChannel(
  //     replicacheName,
  //     TEST_FORMAT_VERSION + 1,
  //     controller.signal,
  //     clientID2,
  //     client2,
  //     () => {
  //       client2OnUpdateNeededCallCount++;
  //     },
  //   );
  //   await channelMessagePromise;
  //   expect(client1OnUpdateNeededCallCount).to.equal(1);
  //   expect(client2OnUpdateNeededCallCount).to.equal(0);

  //   let client3OnUpdateNeededCallCount = 0;
  //   const channelMessagePromise2 = getChannelMessagePromise(replicacheName);
  //   initNewClientChannel(
  //     replicacheName,
  //     TEST_FORMAT_VERSION - 1,
  //     controller.signal,
  //     clientID3,
  //     client3,
  //     () => {
  //       client3OnUpdateNeededCallCount++;
  //     },
  //   );
  //   await channelMessagePromise2;
  //   expect(client1OnUpdateNeededCallCount).to.equal(2);
  //   expect(client2OnUpdateNeededCallCount).to.equal(1);
  //   expect(client3OnUpdateNeededCallCount).to.equal(0);
  // });

  test('calls onUpdateNeeded when a client with a different idbName is received', async () => {
    const replicacheName = 'test-name';
    const idbName1 = 'test-idb-name-1';
    const idbName2 = 'test-idb-name-2';
    const controller = new AbortController();
    const clientGroupID1 = 'client-group-1';
    const clientGroupID2 = 'client-group-2';
    const perdag = new dag.TestStore();

    await putClientGroup(perdag, clientGroupID1);
    let client1OnUpdateNeededCallCount = 0;
    initNewClientChannel(
      replicacheName,
      idbName1,
      controller.signal,
      clientGroupID1,
      true,
      () => {
        client1OnUpdateNeededCallCount++;
      },
      new dag.TestStore(),
    );
    expect(client1OnUpdateNeededCallCount).to.equal(0);

    let client2OnUpdateNeededCallCount = 0;
    const channelMessagePromise = getChannelMessagePromise(replicacheName);
    initNewClientChannel(
      replicacheName,
      idbName2,
      controller.signal,
      clientGroupID2,
      true, // This should not happen because the client group is already created
      () => {
        client2OnUpdateNeededCallCount++;
      },
      new dag.TestStore(),
    );
    await channelMessagePromise;
    expect(client1OnUpdateNeededCallCount).to.equal(1);
    expect(client2OnUpdateNeededCallCount).to.equal(0);
  });

  test('calls onUpdateNeeded when a client with a different clientGroupID and same idbName is received and that newClientGroupID is present in perdag', async () => {
    const replicacheName = 'test-name';
    const idbName = 'test-idb-name';
    const controller = new AbortController();
    const clientGroupID1 = 'client-group-1';
    const clientGroupID2 = 'client-group-2';
    const perdag = new dag.TestStore();

    await putClientGroup(perdag, clientGroupID1);
    let client1OnUpdateNeededCallCount = 0;
    initNewClientChannel(
      replicacheName,
      idbName,
      controller.signal,
      clientGroupID1,
      true,
      () => {
        client1OnUpdateNeededCallCount++;
      },
      perdag,
    );
    expect(client1OnUpdateNeededCallCount).to.equal(0);

    await putClientGroup(perdag, clientGroupID2);
    let client2OnUpdateNeededCallCount = 0;
    const channelMessagePromise = getChannelMessagePromise(replicacheName);
    initNewClientChannel(
      replicacheName,
      idbName,
      controller.signal,
      clientGroupID2,
      true,
      () => {
        client2OnUpdateNeededCallCount++;
      },
      perdag,
    );
    await channelMessagePromise;
    expect(client1OnUpdateNeededCallCount).to.equal(1);
    expect(client2OnUpdateNeededCallCount).to.equal(0);
  });

  test('does not call onUpdateNeeded when a client with a different clientGroupID and same idbName is received and that newClientGroupID is *not* present in perdag', async () => {
    const replicacheName = 'test-name';
    const idbName = 'test-idb-name';
    const controller = new AbortController();
    const clientGroupID1 = 'client-group-1';
    const clientGroupID2 = 'client-group-2';
    const perdag = new dag.TestStore();

    await putClientGroup(perdag, clientGroupID1);
    let client1OnUpdateNeededCallCount = 0;
    initNewClientChannel(
      replicacheName,
      idbName,
      controller.signal,
      clientGroupID1,
      true,
      () => {
        client1OnUpdateNeededCallCount++;
      },
      perdag,
    );
    expect(client1OnUpdateNeededCallCount).to.equal(0);

    // don't put clientGroupID2 in perdag
    let client2OnUpdateNeededCallCount = 0;
    const channelMessagePromise = getChannelMessagePromise(replicacheName);
    initNewClientChannel(
      replicacheName,
      idbName,
      controller.signal,
      clientGroupID2,
      true,
      () => {
        client2OnUpdateNeededCallCount++;
      },
      perdag,
    );
    await channelMessagePromise;
    // 0 because clientGroupID2 is not in perdag
    expect(client1OnUpdateNeededCallCount).to.equal(0);
    expect(client2OnUpdateNeededCallCount).to.equal(0);
  });

  test('does not call onUpdateNeeded when a client with the same clientGroupID and idbName is received', async () => {
    const replicacheName = 'test-name';
    const idbName = 'test-idb-name';
    const controller = new AbortController();
    const clientGroupID1 = 'client-group-1';
    const perdag = new dag.TestStore();

    await putClientGroup(perdag, clientGroupID1);
    let client1OnUpdateNeededCallCount = 0;
    initNewClientChannel(
      replicacheName,
      idbName,
      controller.signal,
      clientGroupID1,
      true,
      () => {
        client1OnUpdateNeededCallCount++;
      },
      new dag.TestStore(),
    );
    expect(client1OnUpdateNeededCallCount).to.equal(0);

    let client2OnUpdateNeededCallCount = 0;
    const channelMessagePromise = getChannelMessagePromise(replicacheName);
    initNewClientChannel(
      replicacheName,
      idbName,
      controller.signal,
      clientGroupID1,
      true, // This should not happen because the client group is already created
      () => {
        client2OnUpdateNeededCallCount++;
      },
      new dag.TestStore(),
    );
    await channelMessagePromise;
    expect(client1OnUpdateNeededCallCount).to.equal(0);
    expect(client2OnUpdateNeededCallCount).to.equal(0);
  });

  test('closes channel when abort is signaled', async () => {
    const replicacheName = 'test-name';
    const idbName = 'test-idb-name';
    const controller1 = new AbortController();
    const controller2 = new AbortController();
    const clientGroupID1 = 'client-group-1';
    const clientGroupID2 = 'client-group-2';
    const perdag = new dag.TestStore();

    await putClientGroup(perdag, clientGroupID1);
    let client1OnUpdateNeededCallCount = 0;
    initNewClientChannel(
      replicacheName,
      idbName,
      controller1.signal,
      clientGroupID1,
      true,
      () => {
        client1OnUpdateNeededCallCount++;
      },
      new dag.TestStore(),
    );
    expect(client1OnUpdateNeededCallCount).to.equal(0);
    let client2OnUpdateNeededCallCount = 0;
    controller1.abort();

    await putClientGroup(perdag, clientGroupID2);
    const channelMessagePromise = getChannelMessagePromise(replicacheName);
    initNewClientChannel(
      replicacheName,
      idbName,
      controller2.signal,
      clientGroupID2,
      true,
      () => {
        client2OnUpdateNeededCallCount++;
      },
      new dag.TestStore(),
    );
    await channelMessagePromise;
    // 0 because controller1.abort was called, causing
    // client1's channel to be closed before receiving client2
    expect(client1OnUpdateNeededCallCount).to.equal(0);
    expect(client2OnUpdateNeededCallCount).to.equal(0);
  });
});

async function putClientGroup(perdag: dag.TestStore, clientGroupID1: string) {
  await withWrite(perdag, async perdagWrite => {
    await setClientGroup(
      clientGroupID1,
      {
        headHash: fakeHash('abc'),
        indexes: {},
        mutationIDs: {},
        lastServerAckdMutationIDs: {},
        mutatorNames: ['addData'],
        disabled: false,
      },
      perdagWrite,
    );
    await perdagWrite.commit();
  });
}

test('v0 message, calls onUpdateNeeded when a client with a different clientGroupID is received and that newClientGroupID even if it is not present in perdag', async () => {
  const replicacheName = 'test-name';
  const idbName = 'test-idb-name';
  const controller = new AbortController();
  const clientGroupID1 = 'client-group-1';
  const perdag = new dag.TestStore();

  let client1OnUpdateNeededCallCount = 0;
  initNewClientChannel(
    replicacheName,
    idbName,
    controller.signal,
    clientGroupID1,
    true,
    () => {
      client1OnUpdateNeededCallCount++;
    },
    perdag,
  );
  expect(client1OnUpdateNeededCallCount).to.equal(0);

  const channelMessagePromise = getChannelMessagePromise(replicacheName);
  const channel = new BroadcastChannel(
    `replicache-new-client-group:${replicacheName}`,
  );
  channel.postMessage(['client-group-2']);
  await channelMessagePromise;

  expect(client1OnUpdateNeededCallCount).to.equal(1);
});

test('v0 message, does not call onUpdateNeeded when a client with the same clientGroupID is received', async () => {
  const replicacheName = 'test-name';
  const idbName = 'test-idb-name';
  const controller = new AbortController();
  const clientGroupID1 = 'client-group-1';
  const perdag = new dag.TestStore();

  let client1OnUpdateNeededCallCount = 0;
  initNewClientChannel(
    replicacheName,
    idbName,
    controller.signal,
    clientGroupID1,
    true,
    () => {
      client1OnUpdateNeededCallCount++;
    },
    perdag,
  );
  expect(client1OnUpdateNeededCallCount).to.equal(0);

  const channelMessagePromise = getChannelMessagePromise(replicacheName);
  const channel = new BroadcastChannel(
    `replicache-new-client-group:${replicacheName}`,
  );
  channel.postMessage(['client-group-1']);
  await channelMessagePromise;

  expect(client1OnUpdateNeededCallCount).to.equal(0);
});
