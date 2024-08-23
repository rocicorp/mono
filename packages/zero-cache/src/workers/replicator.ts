import {resolver} from '@rocicorp/resolver';
import {assert} from 'shared/src/asserts.js';
import {MessagePort, Worker} from 'worker_threads';
import {Replicator} from 'zero-cache/src/services/replicator/replicator.js';
import {Service} from 'zero-cache/src/services/service.js';
import {Notifier} from '../services/replicator/notifier.js';

export type ReplicatorWorkerData = {
  subscriberPorts: MessagePort[];
};

function validate(workerData: unknown): ReplicatorWorkerData {
  // Sanity check that the WorkerThread is initialized with the expected data.
  const data = workerData as ReplicatorWorkerData;
  const {subscriberPorts} = data;
  assert(Array.isArray(subscriberPorts));
  subscriberPorts.forEach(port => assert(port instanceof MessagePort));
  return data;
}

export function runAsWorker(
  replicator: Replicator & Service,
  parentPort: MessagePort | null,
  workerData: ReplicatorWorkerData,
): Promise<void> {
  const {subscriberPorts} = validate(workerData);

  // Respond to status requests from the parent (main) thread.
  const statusPort = parentPort;
  assert(statusPort);
  statusPort.on('message', async () => {
    const status = await replicator.status();
    statusPort.postMessage(status);
  });

  // Start a subscription for the MessageChannel of every Syncer thread.
  // A Syncer thread relays this subscription to its ViewSyncers with a
  // Notifier created by the createNotifier() function below.
  for (const subscriber of subscriberPorts) {
    const subscription = replicator.subscribe();
    subscriber.once('close', () => subscription.cancel());

    void (async () => {
      for await (const msg of subscription) {
        subscriber.postMessage(msg);
      }
      subscriber.close();
    })();
  }

  return replicator.run();
}

export function getStatusFromWorker(
  replicator: Worker | MessagePort, // MessagePort used in tests.
): Promise<unknown> {
  const {promise, resolve} = resolver<unknown>();
  replicator.postMessage({});
  replicator.once('message', resolve);
  return promise;
}

/**
 * Creates a Notifier to listen to the Subscription from the other
 * side of the `subscriberPorts` passed into the Replicator thread
 * (i.e. from the Syncer thread).
 */
export function createNotifier(replicatorPort: MessagePort): Notifier {
  const notifier = new Notifier();
  replicatorPort.on('message', () => notifier.notifySubscribers());
  return notifier;
}
