import {resolver} from '@rocicorp/resolver';
import {Replicator} from 'zero-cache/src/services/replicator/replicator.js';
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

  // Respond to subscribe requests from the parent process.
  parent.on('message', async data => {
    const msg = getMessage('subscribe', data);
    if (msg) {
      const subscription = replicator.subscribe();
      for await (const msg of subscription) {
        parent.send(['notify', msg]);
      }
    }
  });

  return replicator.run();
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
 * Creates a Notifier to listen to the Subscription from the Replicator
 * running in a different process. This is only meant to be done once
 * by the parent process.
 */
export function createNotifier(replicator: Worker): Notifier {
  const notifier = new Notifier();
  replicator.on('message', data => {
    const msg = getMessage('notify', data);
    if (msg) {
      notifier.notifySubscribers(msg);
    }
  });
  replicator.send(['subscribe', {}]);
  return notifier;
}
