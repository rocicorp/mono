import {expect} from '@esm-bundle/chai';
import {resolver} from '@rocicorp/resolver';
import {
  initNewClientChannel,
  makeChannelNameForTesting,
} from './new-client-channel.js';
import {sleep} from './sleep.js';

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
  test('sends client group ID to channel', async () => {
    const replicacheName = 'test-name';
    const channelMessagePromise = getChannelMessagePromise(replicacheName);
    const controller = new AbortController();
    const clientGroupID = 'test-client-group-id-1';
    initNewClientChannel(
      replicacheName,
      controller.signal,
      clientGroupID,
      true,
      () => undefined,
    );
    expect(await channelMessagePromise).to.deep.equal([clientGroupID]);
  });

  test("Doesn't send message if client group is not new", async () => {
    const replicacheName = 'test-name';
    const channelMessagePromise = getChannelMessagePromise(replicacheName);
    const controller = new AbortController();
    const clientGroupID = 'test-client-group-id-1';
    initNewClientChannel(
      replicacheName,
      controller.signal,
      clientGroupID,
      false,
      () => undefined,
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
  //   const client1: ClientDD31 = {
  //     heartbeatTimestampMs: 111,
  //     headHash: fakeHash('c1'),
  //     tempRefreshHash: null,
  //     clientGroupID: 'branch-1',
  //   };
  //   const clientID2 = 'client2';
  //   // intentionally missing required fields.
  //   const client2 = {} as ClientDD31;
  //   const clientID3 = 'client3';
  //   // intentionally missing required fields.
  //   const client3 = {} as ClientDD31;
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

  test('calls onUpdateNeeded when a client with a different clientGroupID is received', async () => {
    const replicacheName = 'test-name';
    const controller = new AbortController();
    const clientGroupID1 = 'client-group-1';
    const clientGroupID2 = 'client-group-2';

    let client1OnUpdateNeededCallCount = 0;
    initNewClientChannel(
      replicacheName,
      controller.signal,
      clientGroupID1,
      true,
      () => {
        client1OnUpdateNeededCallCount++;
      },
    );
    expect(client1OnUpdateNeededCallCount).to.equal(0);

    let client2OnUpdateNeededCallCount = 0;
    const channelMessagePromise = getChannelMessagePromise(replicacheName);
    initNewClientChannel(
      replicacheName,
      controller.signal,
      clientGroupID2,
      true,
      () => {
        client2OnUpdateNeededCallCount++;
      },
    );
    await channelMessagePromise;
    expect(client1OnUpdateNeededCallCount).to.equal(1);
    expect(client2OnUpdateNeededCallCount).to.equal(0);
  });

  test('does not call onUpdateNeeded when a client with the same clientGroupID is received', async () => {
    const replicacheName = 'test-name';
    const controller = new AbortController();
    const clientGroupID1 = 'client-group-1';

    let client1OnUpdateNeededCallCount = 0;
    initNewClientChannel(
      replicacheName,
      controller.signal,
      clientGroupID1,
      true,
      () => {
        client1OnUpdateNeededCallCount++;
      },
    );
    expect(client1OnUpdateNeededCallCount).to.equal(0);

    let client2OnUpdateNeededCallCount = 0;
    const channelMessagePromise = getChannelMessagePromise(replicacheName);
    initNewClientChannel(
      replicacheName,
      controller.signal,
      clientGroupID1,
      true, // This should not happen because the client group is already created
      () => {
        client2OnUpdateNeededCallCount++;
      },
    );
    await channelMessagePromise;
    expect(client1OnUpdateNeededCallCount).to.equal(0);
    expect(client2OnUpdateNeededCallCount).to.equal(0);
  });

  test('closes channel when abort is signaled', async () => {
    const replicacheName = 'test-name';
    const controller1 = new AbortController();
    const controller2 = new AbortController();
    const clientGroupID1 = 'client-group-1';
    const clientGroupID2 = 'client-group-2';
    let client1OnUpdateNeededCallCount = 0;
    initNewClientChannel(
      replicacheName,
      controller1.signal,
      clientGroupID1,
      true,
      () => {
        client1OnUpdateNeededCallCount++;
      },
    );
    expect(client1OnUpdateNeededCallCount).to.equal(0);
    let client2OnUpdateNeededCallCount = 0;
    controller1.abort();

    const channelMessagePromise = getChannelMessagePromise(replicacheName);
    initNewClientChannel(
      replicacheName,
      controller2.signal,
      clientGroupID2,
      true,
      () => {
        client2OnUpdateNeededCallCount++;
      },
    );
    await channelMessagePromise;
    // 0 because controller1.abort was called, causing
    // client1's channel to be closed before receiving client2
    expect(client1OnUpdateNeededCallCount).to.equal(0);
    expect(client2OnUpdateNeededCallCount).to.equal(0);
  });
});
