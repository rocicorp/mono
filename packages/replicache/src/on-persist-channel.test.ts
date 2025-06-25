import {resolver} from '@rocicorp/resolver';
import {afterEach, describe, expect, test} from 'vitest';
import {BroadcastChannel} from '../../shared/src/broadcast-channel.ts';
import {initOnPersistChannel, type PersistInfo} from './on-persist-channel.ts';

describe('initOnPersistChannel', () => {
  let channel: BroadcastChannel | undefined;

  afterEach(() => {
    if (channel) {
      channel.close();
    }
  });

  test('returned OnPersist fn sends persist info to channel and HandlePersist callback', async () => {
    const replicacheName = 'test-name';
    const controller = new AbortController();
    const handlePersistCalls1: PersistInfo[] = [];
    const onPersist1 = initOnPersistChannel(
      replicacheName,
      controller.signal,
      persistInfo => {
        handlePersistCalls1.push(persistInfo);
      },
    );

    const handlePersistCalls2: PersistInfo[] = [];
    const onPersist2 = initOnPersistChannel(
      replicacheName,
      controller.signal,
      persistInfo => {
        handlePersistCalls2.push(persistInfo);
      },
    );

    const channelMessageCalls: MessageEvent[] = [];
    const channelMessageCallResolvers = [resolver(), resolver()];
    channel = new BroadcastChannel(`replicache-on-persist:${replicacheName}`);

    channel.onmessage = e => {
      channelMessageCalls.push(e);
      channelMessageCallResolvers[channelMessageCalls.length - 1].resolve();
    };

    async function expectPersistInfo(
      i: number,
      persistInfo: PersistInfo,
    ): Promise<void> {
      await channelMessageCallResolvers[i].promise;
      expect(channelMessageCalls.length).to.equal(i + 1);
      expect(channelMessageCalls[i].data).to.deep.equal(persistInfo);
      expect(handlePersistCalls1.length).to.equal(i + 1);
      expect(handlePersistCalls1[i]).to.deep.equal(persistInfo);
      expect(handlePersistCalls2.length).to.equal(i + 1);
      expect(handlePersistCalls2[i]).to.deep.equal(persistInfo);
    }

    onPersist1({
      clientID: 'client1',
      clientGroupID: 'client-group-1',
    });
    await expectPersistInfo(0, {
      clientID: 'client1',
      clientGroupID: 'client-group-1',
    });

    onPersist2({
      clientID: 'client2',
      clientGroupID: 'client-group-2',
    });
    await expectPersistInfo(1, {
      clientID: 'client2',
      clientGroupID: 'client-group-2',
    });
  });

  test('closes channel when abort is signaled', async () => {
    const replicacheName = 'test-name';
    const controller1 = new AbortController();
    const handlePersistCalls1: PersistInfo[] = [];
    const onPersist1 = initOnPersistChannel(
      replicacheName,
      controller1.signal,
      persistInfo => {
        handlePersistCalls1.push(persistInfo);
      },
    );

    const controller2 = new AbortController();
    const handlePersistCalls2: PersistInfo[] = [];
    const onPersist2 = initOnPersistChannel(
      replicacheName,
      controller2.signal,
      persistInfo => {
        handlePersistCalls2.push(persistInfo);
      },
    );

    const channelMessageCalls: MessageEvent[] = [];
    const channelMessageCallResolvers = [resolver(), resolver()];
    const channel = new BroadcastChannel(
      `replicache-on-persist:${replicacheName}`,
    );

    channel.onmessage = e => {
      channelMessageCalls.push(e);
      channelMessageCallResolvers[channelMessageCalls.length - 1].resolve();
    };

    const persistInfo1 = {
      clientID: 'client1',
      clientGroupID: 'clientGroup1',
    };
    onPersist1(persistInfo1);
    await channelMessageCallResolvers[0].promise;
    expect(channelMessageCalls[0].data).to.deep.equal(persistInfo1);
    expect(handlePersistCalls1[0]).to.deep.equal(persistInfo1);
    expect(handlePersistCalls2[0]).to.deep.equal(persistInfo1);

    controller1.abort();

    const persistInfo2 = {
      clientID: 'client2',
      clientGroupID: 'clientGroup2',
    };
    onPersist2(persistInfo2);
    await channelMessageCallResolvers[1].promise;
    expect(channelMessageCalls[1].data).to.deep.equal(persistInfo2);
    // not called because closed
    expect(handlePersistCalls1.length).to.equal(1);
    expect(handlePersistCalls2[1]).to.deep.equal(persistInfo2);
  });
});
