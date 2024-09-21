import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {assert} from 'shared/src/asserts.js';
import {promiseVoid} from 'shared/src/resolved-promises.js';
import {
  ReplicaState,
  ReplicaStateNotifier,
  Replicator,
} from 'zero-cache/src/services/replicator/replicator.js';
import {Notifier} from '../services/replicator/notifier.js';
import {Worker} from '../types/processes.js';

export function setUpMessageHandlers(
  lc: LogContext,
  replicator: Replicator,
  parent: Worker,
) {
  handleSubscriptionsFrom(lc, parent, replicator);
}

type Notification = ['notify', ReplicaState];

type NotificationACK = ['ackNotify', ReplicaState];

export function handleSubscriptionsFrom(
  lc: LogContext,
  subscriber: Worker,
  notifier: ReplicaStateNotifier,
) {
  const pendingACKs = new Map<number, () => void>();

  subscriber.onMessageType<NotificationACK>('ackNotify', msg => {
    assert(msg.ack);
    const resolve = pendingACKs.get(msg.ack);
    if (resolve) {
      resolve();
      pendingACKs.delete(msg.ack);
    } else {
      lc.error?.('received ack with no resolver', msg);
    }
  });

  subscriber.onMessageType('subscribe', async () => {
    const subscription = notifier.subscribe();
    for await (const msg of subscription) {
      let ack = promiseVoid; // By default, nothing to await.

      if (msg.ack !== undefined) {
        const {promise, resolve} = resolver();
        ack = promise;
        pendingACKs.set(msg.ack, resolve);
      }

      subscriber.send<Notification>(['notify', msg]);
      await ack;
    }
  });
}

/**
 * Creates a Notifier to relay notifications the notifier of another Worker.
 * This does not send the initial subscription message. Use {@link subscribeTo}
 * to initiate the subscription.
 */
export function createNotifierFrom(_lc: LogContext, source: Worker): Notifier {
  const notifier = new Notifier();
  source.onMessageType<Notification>('notify', async msg => {
    const results = notifier.notifySubscribers(msg);

    if (msg.ack !== undefined) {
      await Promise.allSettled(results);
      source.send<NotificationACK>(['ackNotify', msg]);
    }
  });
  return notifier;
}

export function subscribeTo(source: Worker) {
  source.send(['subscribe', {}]);
}
