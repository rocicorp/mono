import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {rmSync} from 'node:fs';
import {assert} from 'shared/src/asserts.js';
import {promiseVoid} from 'shared/src/resolved-promises.js';
import type {
  ReplicaState,
  ReplicaStateNotifier,
  Replicator,
} from 'zero-cache/src/services/replicator/replicator.js';
import {Database} from 'zqlite/src/db.js';
import {Notifier} from '../services/replicator/notifier.js';
import type {Worker} from '../types/processes.js';

export type ReplicaFileMode = 'serving' | 'serving-copy' | 'backup';

function connect(
  lc: LogContext,
  replicaDbFile: string,
  walMode: 'wal' | 'wal2',
): Database {
  const replica = new Database(lc, replicaDbFile);

  const [{journal_mode: mode}] = replica.pragma('journal_mode') as {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    journal_mode: string;
  }[];

  if (mode !== walMode) {
    lc.info?.(`switching ${replicaDbFile} from ${mode} to ${walMode} mode`);
    replica.pragma('journal_mode = delete');
    replica.pragma(`journal_mode = ${walMode}`);
  }

  replica.pragma('synchronous = NORMAL');
  replica.pragma('optimize = 0x10002');
  return replica;
}

export function setupReplica(
  lc: LogContext,
  mode: ReplicaFileMode,
  replicaDbFile: string,
): Database {
  lc.info?.(`setting up ${mode} replica`);

  switch (mode) {
    case 'backup': {
      const replica = connect(lc, replicaDbFile, 'wal');
      // In 'backup' mode, litestream is replicating the file, and
      // locks it to perform its backups and checkpoints.
      // The official docs recommend a 5 second timeout
      // (https://litestream.io/tips/#busy-timeout), but since this is
      // an isolated backup replica, we can wait longer to achieve
      // higher write throughput.
      replica.pragma('busy_timeout = 60000');
      replica.pragma('wal_autocheckpoint = 0');
      return replica;
    }

    case 'serving-copy': {
      // In 'serving-copy' mode, the original file is being used for 'backup'
      // mode, so we make a copy for servicing sync requests.
      const copyLocation = `${replicaDbFile}-serving-copy`;
      for (const suffix of ['', '-wal', '-shm']) {
        rmSync(`${copyLocation}${suffix}`, {force: true});
      }

      const start = Date.now();
      lc.info?.(`copying ${replicaDbFile} to ${copyLocation}`);
      const replica = connect(lc, replicaDbFile, 'wal');
      replica.prepare(`VACUUM INTO ?`).run(copyLocation);
      replica.close();
      lc.info?.(`finished copy (${Date.now() - start} ms)`);

      return connect(lc, copyLocation, 'wal2');
    }

    case 'serving':
      return connect(lc, replicaDbFile, 'wal2');

    default:
      throw new Error(`Invalid ReplicaMode ${mode}`);
  }
}

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

export function subscribeTo(_lc: LogContext, source: Worker) {
  source.send(['subscribe', {}]);
}
