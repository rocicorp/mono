import {LogContext} from '@rocicorp/logger';
import * as v from '../../../shared/src/valita.ts';
import {Database} from '../../../zqlite/src/db.ts';
import type {ReplicaOptions} from '../config/zero-config.ts';
import {deleteLiteDB} from '../db/delete-lite-db.ts';
import {upgradeReplica} from '../services/change-source/replica-schema.ts';
import {Notifier} from '../services/replicator/notifier.ts';
import type {
  ReplicaState,
  ReplicaStateNotifier,
  Replicator,
} from '../services/replicator/replicator.ts';
import {
  getAscendingEvents,
  recordEvent,
} from '../services/replicator/schema/replication-state.ts';
import type {Worker} from '../types/processes.ts';

export const replicaFileModeSchema = v.union(
  v.literal('serving'),
  v.literal('serving-copy'),
  v.literal('backup'),
);

export type ReplicaFileMode = v.Infer<typeof replicaFileModeSchema>;

export function replicaFileName(replicaFile: string, mode: ReplicaFileMode) {
  return mode === 'serving-copy' ? `${replicaFile}-serving-copy` : replicaFile;
}

const MILLIS_PER_HOUR = 1000 * 60 * 60;
const MB = 1024 * 1024;

async function connect(
  lc: LogContext,
  {file, vacuumIntervalHours}: ReplicaOptions,
  walMode: 'wal' | 'wal2',
  mode: ReplicaFileMode,
): Promise<Database> {
  const replica = new Database(lc, file);
  const start = Date.now();

  // Perform any upgrades to the replica in case the backup is an
  // earlier version.
  await upgradeReplica(lc, `${mode}-replica`, file);

  // Start by folding any (e.g. restored) WAL(2) files into the main db.
  replica.pragma('journal_mode = delete');

  //eslint-disable-next-line @typescript-eslint/naming-convention
  const [{page_size: pageSize}] = replica.pragma<{page_size: number}>(
    'page_size',
  );
  //eslint-disable-next-line @typescript-eslint/naming-convention
  const [{page_count: pageCount}] = replica.pragma<{page_count: number}>(
    'page_count',
  );
  const [{freelist_count: freelistCount}] = replica.pragma<{
    //eslint-disable-next-line @typescript-eslint/naming-convention
    freelist_count: number;
  }>('freelist_count');

  const dbSize = ((pageCount * pageSize) / MB).toFixed(2);
  const freelistSize = ((freelistCount * pageSize) / MB).toFixed(2);

  // TODO: Consider adding a freelist size or ratio based vacuum trigger.
  lc.info?.(`Size of db ${file}: ${dbSize} MB (${freelistSize} MB freeable)`);

  // Check for the VACUUM threshold.
  const events = getAscendingEvents(replica);
  lc.debug?.(`Runtime events for db ${file}`, {events});
  if (vacuumIntervalHours !== undefined) {
    const millisSinceLastEvent =
      Date.now() - (events.at(-1)?.timestamp.getTime() ?? 0);
    if (millisSinceLastEvent / MILLIS_PER_HOUR > vacuumIntervalHours) {
      lc.info?.(`Performing maintenance VACUUM on ${file}`);
      replica.unsafeMode(true);
      replica.pragma('journal_mode = OFF');
      replica.exec('VACUUM');
      recordEvent(replica, 'vacuum');
      replica.unsafeMode(false);
      lc.info?.(`VACUUM completed (${Date.now() - start} ms)`);
    }
  }

  lc.info?.(`setting ${file} to ${walMode} mode`);
  replica.pragma(`journal_mode = ${walMode}`);

  // Set a busy timeout at litestream's recommended 5 seconds:
  // (https://litestream.io/tips/#busy-timeout).
  //
  // In the view-syncer (for which there is no litestream replicate
  // process), this is still useful for handling the `PRAGMA optimize`
  // call the sync workers, which results in occasional `ANALYZE` calls
  // that may contend with each other and with the replicator for the lock.
  replica.pragma('busy_timeout = 5000');

  replica.pragma('optimize = 0x10002');
  lc.info?.(`optimized ${file}`);
  return replica;
}

export async function setupReplica(
  lc: LogContext,
  mode: ReplicaFileMode,
  replicaOptions: ReplicaOptions,
): Promise<Database> {
  lc.info?.(`setting up ${mode} replica`);

  switch (mode) {
    case 'backup': {
      const replica = await connect(lc, replicaOptions, 'wal', mode);
      // https://litestream.io/tips/#disable-autocheckpoints-for-high-write-load-servers
      replica.pragma('wal_autocheckpoint = 0');
      return replica;
    }

    case 'serving-copy': {
      // In 'serving-copy' mode, the original file is being used for 'backup'
      // mode, so we make a copy for servicing sync requests.
      const {file} = replicaOptions;
      const copyLocation = replicaFileName(file, mode);
      deleteLiteDB(copyLocation);

      const start = Date.now();
      lc.info?.(`copying ${file} to ${copyLocation}`);
      const replica = new Database(lc, file);
      replica.prepare(`VACUUM INTO ?`).run(copyLocation);
      replica.close();
      lc.info?.(`finished copy (${Date.now() - start} ms)`);

      return connect(lc, {...replicaOptions, file: copyLocation}, 'wal2', mode);
    }

    case 'serving':
      return connect(lc, replicaOptions, 'wal2', mode);

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

export function handleSubscriptionsFrom(
  lc: LogContext,
  subscriber: Worker,
  notifier: ReplicaStateNotifier,
) {
  subscriber.onMessageType('subscribe', async () => {
    const subscription = notifier.subscribe();

    subscriber.on('close', () => {
      lc.debug?.(`closing replication subscription from ${subscriber.pid}`);
      subscription.cancel();
    });

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
export function createNotifierFrom(_lc: LogContext, source: Worker): Notifier {
  const notifier = new Notifier();
  source.onMessageType<Notification>('notify', msg =>
    notifier.notifySubscribers(msg),
  );
  return notifier;
}

export function subscribeTo(_lc: LogContext, source: Worker) {
  source.send(['subscribe', {}]);
}
