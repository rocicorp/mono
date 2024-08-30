import {resolver} from '@rocicorp/resolver';
import {
  Replicator,
  ReplicaVersionNotifier,
} from 'zero-cache/src/services/replicator/replicator.js';
import {Service} from 'zero-cache/src/services/service.js';
import {Notifier} from '../services/replicator/notifier.js';
import {getMessage, Worker} from '../types/processes.js';

export function runAsWorker(
  replicator: Replicator & Service,
  parent: Worker,
): Promise<void> {
  // Respond to status requests from the parent process.
  parent.on('message', async data => {
    const msg = getMessage('status', data);
    if (msg) {
      const status = await replicator.status();
      parent.send(['status', status]);
    }
  });

  handleSubscriptionsFrom(parent, replicator);

  return replicator.run();
}

export function handleSubscriptionsFrom(
  subscriber: Worker,
  notifier: ReplicaVersionNotifier,
) {
  subscriber.on('message', async data => {
    const msg = getMessage('subscribe', data);
    if (msg) {
      const subscription = notifier.subscribe();
      for await (const msg of subscription) {
        subscriber.send(['notify', msg]);
      }
    }
  });
}

export function getStatusFromWorker(replicator: Worker): Promise<unknown> {
  const {promise, resolve} = resolver<unknown>();
  const received = (data: unknown) => {
    const msg = getMessage('status', data);
    if (msg) {
      // Simulates 'once', but keeps listening until we get a ['status', ...] message.
      replicator.off('message', received);
      resolve(msg);
    }
  };
  replicator.on('message', received);

  replicator.send(['status', {}]);
  return promise;
}

/**
 * Creates a Notifier to relay notifications the notifier of another Worker.
 * This does not send the initial subscription message. Use {@link subscribeTo}
 * to initiate the subscription.
 */
export function createNotifierFrom(source: Worker): Notifier {
  const notifier = new Notifier();
  source.on('message', data => {
    const msg = getMessage('notify', data);
    if (msg) {
      notifier.notifySubscribers(msg);
    }
  });
  return notifier;
}

export function subscribeTo(source: Worker) {
  source.send(['subscribe', {}]);
}
