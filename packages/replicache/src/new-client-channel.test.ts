import {resolver} from '@rocicorp/resolver';
import {describe, expect, test} from 'vitest';
import {BroadcastChannel} from '../../shared/src/broadcast-channel.ts';
import {sleep} from '../../shared/src/sleep.ts';
import {TestStore} from './dag/test-store.ts';
import {fakeHash} from './hash.ts';
import {
  initNewClientChannel,
  makeChannelNameV0ForTesting,
  makeChannelNameV1ForTesting,
} from './new-client-channel.ts';
import {setClientGroup} from './persist/client-groups.ts';
import {withWriteNoImplicitCommit} from './with-transactions.ts';

function getChannelMessagePromise(
  replicacheName: string,
  makeChannelName = makeChannelNameV1ForTesting,
) {
  const channel = new BroadcastChannel(makeChannelName(replicacheName));
  const messageResolver = resolver();
  channel.onmessage = e => {
    messageResolver.resolve(e.data);
  };
  return messageResolver.promise;
}

describe('initNewClientChannel', () => {
  test('sends client group ID to channel v0 and client group ID and idb name to channel v1', async () => {
    const replicacheName = 'test-name';
    const idbName = 'test-idb-name';
    const channelMessageV0Promise = getChannelMessagePromise(
      replicacheName,
      makeChannelNameV0ForTesting,
    );
    const channelMessageV1Promise = getChannelMessagePromise(
      replicacheName,
      makeChannelNameV1ForTesting,
    );
    const controller = new AbortController();
    const clientGroupID = 'test-client-group-id-1';
    initNewClientChannel(
      replicacheName,
      idbName,
      controller.signal,
      clientGroupID,
      true,
      () => undefined,
      new TestStore(),
    );
    expect(await channelMessageV0Promise).to.deep.equal([clientGroupID]);
    expect(await channelMessageV1Promise).to.deep.equal({
      clientGroupID,
      idbName,
    });
  });

  test("Doesn't send messages to channels if client group is not new", async () => {
    const replicacheName = 'test-name';
    const idbName = 'test-idb-name';
    const channelMessageV0Promise = getChannelMessagePromise(
      replicacheName,
      makeChannelNameV0ForTesting,
    );
    const channelMessageV1Promise = getChannelMessagePromise(
      replicacheName,
      makeChannelNameV1ForTesting,
    );
    const controller = new AbortController();
    const clientGroupID = 'test-client-group-id-1';
    initNewClientChannel(
      replicacheName,
      idbName,
      controller.signal,
      clientGroupID,
      false,
      () => undefined,
      new TestStore(),
    );

    const sentinel = Symbol();
    const res = await Promise.race([
      channelMessageV0Promise,
      channelMessageV1Promise,
      sleep(10).then(() => sentinel),
    ]);
    expect(res).equal(sentinel);

    {
      // And test that we get the message if another client group is created

      const anotherClientGroupID = 'test-client-group-id-2';
      initNewClientChannel(
        replicacheName,
        idbName,
        controller.signal,
        anotherClientGroupID,
        true,
        () => undefined,
        new TestStore(),
      );
      expect(await channelMessageV0Promise).to.deep.equal([
        anotherClientGroupID,
      ]);
      expect(await channelMessageV1Promise).to.deep.equal({
        clientGroupID: anotherClientGroupID,
        idbName,
      });
    }
  });

  test('calls onUpdateNeeded when a client with a different idbName is received', async () => {
    const replicacheName = 'test-name';
    const idbName1 = 'test-idb-name-1';
    const idbName2 = 'test-idb-name-2';
    const controller = new AbortController();
    const clientGroupID1 = 'client-group-1';
    const clientGroupID2 = 'client-group-2';
    const perdag = new TestStore();

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
      new TestStore(),
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
      new TestStore(),
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
    const perdag = new TestStore();

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
    const perdag = new TestStore();

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
    const perdag = new TestStore();

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
      new TestStore(),
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
      new TestStore(),
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
    const perdag = new TestStore();

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
      new TestStore(),
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
      new TestStore(),
    );
    await channelMessagePromise;
    // 0 because controller1.abort was called, causing
    // client1's channel to be closed before receiving client2
    expect(client1OnUpdateNeededCallCount).to.equal(0);
    expect(client2OnUpdateNeededCallCount).to.equal(0);
  });
});

async function putClientGroup(perdag: TestStore, clientGroupID1: string) {
  await withWriteNoImplicitCommit(perdag, async perdagWrite => {
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

test('v0 message is not handled', async () => {
  const replicacheName = 'test-name';
  const idbName = 'test-idb-name';
  const controller = new AbortController();
  const clientGroupID1 = 'client-group-1';
  const perdag = new TestStore();

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

  const channelMessagePromise = getChannelMessagePromise(
    replicacheName,
    makeChannelNameV0ForTesting,
  );
  const channel = new BroadcastChannel(
    makeChannelNameV0ForTesting(replicacheName),
  );
  channel.postMessage(['client-group-2']);
  await channelMessagePromise;

  expect(client1OnUpdateNeededCallCount).to.equal(0);
});
