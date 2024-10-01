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
import {
  type Checkpointer as Checkpoint,
  NULL_CHECKPOINTER,
  WALCheckpointer,
} from '../services/replicator/checkpointer.js';
import {Notifier} from '../services/replicator/notifier.js';
import type {Worker} from '../types/processes.js';

export type ReplicatorMode = 'serving' | 'serving-copy' | 'backup';

function connect(lc: LogContext, replicaDbFile: string): Database {
  const replica = new Database(lc, replicaDbFile);
  replica.pragma('journal_mode = WAL');
  replica.pragma('synchronous = NORMAL');
  replica.pragma('optimize = 0x10002');

  // checkpoints are handled by us or by litestream (in 'backup' mode).
  replica.pragma('wal_autocheckpoint = 0');
  return replica;
}

export function setupReplicaAndCheckpointer(
  lc: LogContext,
  mode: ReplicatorMode,
  replicaDbFile: string,
): {replica: Database; checkpointer: Checkpoint} {
  lc.info?.(`setting up replicator in ${mode} mode`);

  let replica = connect(lc, replicaDbFile);

  // In 'backup' mode, litestream is replicating the file, and
  // locks it to perform its backups and checkpoints.
  if (mode === 'backup') {
    // The official docs recommend a 5 second timeout
    // (https://litestream.io/tips/#busy-timeout), but since this is
    // an isolated backup replica, we can wait longer to achieve
    // higher write throughput.
    replica.pragma('busy_timeout = 10000');
    return {replica, checkpointer: NULL_CHECKPOINTER};
  }

  // In 'serving-copy' mode, the original file is being used for 'backup'
  // mode, so we make a copy for servicing sync requests.
  if (mode === 'serving-copy') {
    const copyLocation = `${replicaDbFile}-serving-copy`;
    for (const suffix of ['', '-wal', '-shm']) {
      rmSync(`${copyLocation}${suffix}`, {force: true});
    }

    const start = Date.now();
    lc.info?.(`copying ${replicaDbFile} to ${copyLocation}`);
    replica.prepare(`VACUUM INTO ?`).run(copyLocation);
    lc.info?.(`finished copy (${Date.now() - start} ms)`);

    replica.close();
    replica = connect(lc, copyLocation);
  } else if (mode !== 'serving') {
    throw new Error(`Invalid ReplicaMode ${mode}`);
  }

  // In 'serving' and 'serving-copy' modes, the WALCheckpointer
  // manages checkpointing.
  return {
    replica,
    checkpointer: new WALCheckpointer(lc, replica.name),
  };
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
