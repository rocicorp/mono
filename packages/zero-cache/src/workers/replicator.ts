import {resolver} from '@rocicorp/resolver';
import {
  ReplicaState,
  ReplicaStateNotifier,
  Replicator,
} from 'zero-cache/src/services/replicator/replicator.js';
import {Notifier} from '../services/replicator/notifier.js';
import {Worker} from '../types/processes.js';

export function setUpMessageHandlers(replicator: Replicator, parent: Worker) {
  // Respond to status requests from the parent process.
  parent.onMessageType('status', async () => {
    const status = await replicator.status();
    parent.send(['status', status]);
  });

  handleSubscriptionsFrom(parent, replicator);
}

export function getStatusFromWorker(replicator: Worker): Promise<unknown> {
  const {promise, resolve} = resolver<unknown>();
  replicator.onceMessageType('status', resolve);
  replicator.send(['status', {}]);
  return promise;
}

type Notification = ['notify', ReplicaState];

export function handleSubscriptionsFrom(
  subscriber: Worker,
  notifier: ReplicaStateNotifier,
) {
  subscriber.onMessageType('subscribe', async () => {
    const subscription = notifier.subscribe();
    for await (const msg of subscription) {
      subscriber.send<Notification>(['notify', msg]);
    }
  });
}

/**
 * Creates a Notifier to relay notifications the notifier of another Worker.
 * This does not send the initial subscription message. Use {@link subscribeTo}
 * to initiate the subscription.
 */
export function createNotifierFrom(source: Worker): Notifier {
  const notifier = new Notifier();
  source.onMessageType<Notification>('notify', msg => {
    notifier.notifySubscribers(msg);
  });
  return notifier;
}

export function subscribeTo(source: Worker) {
  source.send(['subscribe', {}]);
}
