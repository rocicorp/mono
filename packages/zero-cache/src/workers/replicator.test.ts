import {Queue} from 'shared/src/queue.js';
import {describe, expect, test, vi} from 'vitest';
import {
  Replicator,
  ReplicaVersionReady,
} from 'zero-cache/src/services/replicator/replicator.js';
import {Service} from 'zero-cache/src/services/service.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {getStatusFromWorker, runAsWorker} from './replicator.js';

describe('workers/replicator', () => {
  test('status', async () => {
    const replicator = {
      status: vi.fn().mockResolvedValue({status: 'yo'}),
      subscribe: vi.fn(),
      run: vi.fn(),
    };

    const {port1: parentPort, port2: childPort} = new MessageChannel();

    void runAsWorker(
      replicator as unknown as Replicator & Service,
      parentPort,
      {subscriberPorts: []},
    );
    expect(replicator.run).toHaveBeenCalledOnce;

    // Simulate a status request from the parent.
    const status = await getStatusFromWorker(childPort);
    expect(status).toEqual({status: 'yo'});
  });
});

test('subscription', async () => {
  const subscription = Subscription.create<ReplicaVersionReady>();

  const replicator = {
    status: vi.fn(),
    subscribe: () => subscription,
    run: vi.fn(),
  };

  const statusChannel = new MessageChannel();
  const {port1: notificationPort, port2: syncerPort} = new MessageChannel();

  void runAsWorker(
    replicator as unknown as Replicator & Service,
    statusChannel.port1,
    {subscriberPorts: [syncerPort]},
  );
  expect(replicator.run).toHaveBeenCalledOnce;

  const notifications = new Queue<unknown>();
  notificationPort.on('message', msg => notifications.enqueue(msg));
  notificationPort.postMessage({});

  subscription.push({foo: 'bar'});
  subscription.push({foo: 'baz'});
  expect(await notifications.dequeue()).toEqual({foo: 'bar'});
  expect(await notifications.dequeue()).toEqual({foo: 'baz'});
});
