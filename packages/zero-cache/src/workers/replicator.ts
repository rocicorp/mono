import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import * as v from '../../../shared/src/valita.ts';
import {Database} from '../../../zqlite/src/db.ts';
import type {ReplicaOptions} from '../config/zero-config.ts';
import {deleteLiteDB} from '../db/delete-lite-db.ts';
import {upgradeReplica} from '../services/change-source/common/replica-schema.ts';
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
import {
  validateSQLiteChangeLogMaintenanceRequest,
  type SQLiteChangeLogMaintenance,
  type SQLiteChangeLogMaintenanceRequest,
  type SQLiteChangeLogMaintenanceResponse,
} from '../services/replicator/sqlite-change-log-maintenance.ts';
import type {SQLiteChangeLogPurgeResult} from '../services/replicator/sqlite-change-log-purger.ts';
import {
  applyPragmas,
  serializeError,
  type PragmaConfig,
} from '../services/replicator/write-worker-client.ts';
import type {Worker} from '../types/processes.ts';

export const replicaFileModeSchema = v.literalUnion(
  'serving',
  'serving-copy',
  'backup',
);

export type ReplicaFileMode = v.Infer<typeof replicaFileModeSchema>;

export function createsCanonicalReplicator(
  runsLocalChangeStreamer: boolean,
  backupURL: string | undefined,
  numSyncWorkers: number,
): boolean {
  return runsLocalChangeStreamer && (Boolean(backupURL) || numSyncWorkers > 0);
}

export function replicaLogsChangeStream(
  fileMode: ReplicaFileMode,
  sqliteChangeLogEnabled: boolean,
  runsLocalChangeStreamer: boolean,
  backupURL: string | undefined,
): boolean {
  if (!sqliteChangeLogEnabled || !runsLocalChangeStreamer) {
    return false;
  }
  return (
    fileMode === 'backup' || (fileMode === 'serving' && !Boolean(backupURL))
  );
}

export type WalMode = 'wal' | 'wal2';

export function replicaFileName(replicaFile: string, mode: ReplicaFileMode) {
  return mode === 'serving-copy' ? `${replicaFile}-serving-copy` : replicaFile;
}

const MILLIS_PER_HOUR = 1000 * 60 * 60;
const MB = 1024 * 1024;

async function prepare(
  lc: LogContext,
  {file, vacuumIntervalHours}: ReplicaOptions,
  walMode: WalMode,
  mode: ReplicaFileMode,
): Promise<{file: string; walMode: WalMode}> {
  // Perform any upgrades to the replica in case the backup is an
  // earlier version.
  await upgradeReplica(lc, `${mode}-replica`, file);

  const replica = new Database(lc, file);
  try {
    // Start by folding any (e.g. restored) WAL(2) files into the main db.
    await setJournalMode(lc, replica, 'delete');

    const [{page_size: pageSize}] = replica.pragma<{page_size: number}>(
      'page_size',
    );
    const [{page_count: pageCount}] = replica.pragma<{page_count: number}>(
      'page_count',
    );
    const [{freelist_count: freelistCount}] = replica.pragma<{
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
        lc.info?.(`Performing maintenance cleanup on ${file}`);
        const t0 = performance.now();
        replica.unsafeMode(true);
        replica.pragma('journal_mode = OFF');
        replica.exec('VACUUM');
        recordEvent(replica, 'vacuum');
        replica.unsafeMode(false);
        const t1 = performance.now();
        lc.info?.(`VACUUM completed (${t1 - t0} ms)`);
      }
    }

    await setJournalMode(lc, replica, walMode);

    const pragmas = getPragmaConfig(mode);
    applyPragmas(replica, pragmas);

    replica.pragma('optimize = 0x10002');
    lc.info?.(`optimized ${file}`);
  } finally {
    replica.close();
  }
  return {file, walMode};
}

// Setting the journal_mode requires an exclusive lock on the replica.
// Add resilience against random replica reads (for stats, etc.) by
// retrying if the database is locked. Note that the busy_timeout doesn't
// work here.
async function setJournalMode(
  lc: LogContext,
  replica: Database,
  mode: 'delete' | 'wal' | 'wal2',
) {
  lc.info?.(`setting ${replica.name} to ${mode} mode`);
  let err: unknown;
  for (let i = 0; i < 5; i++) {
    try {
      replica.pragma(`journal_mode = ${mode}`);
      return;
    } catch (e) {
      lc.warn?.(`error setting journal_mode to ${mode} (attempt ${i + 1})`, e);
      err = e;
    }
    await sleep(500);
  }
  throw err;
}

/**
 * Returns the PragmaConfig for a given replica file mode.
 * This is used by both the main thread (setupReplica) and
 * the write worker thread to apply the same pragma settings.
 */
export function getPragmaConfig(mode: ReplicaFileMode): PragmaConfig {
  return {
    busyTimeout: 30000,
    analysisLimit: 1000,
    walAutocheckpoint: mode === 'backup' ? 0 : undefined,
  };
}

export function setupReplica(
  lc: LogContext,
  mode: ReplicaFileMode,
  replicaOptions: ReplicaOptions,
) {
  lc.info?.(`setting up ${mode} replica`);

  switch (mode) {
    case 'backup':
      return prepare(lc, replicaOptions, 'wal', mode);

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

      return prepare(lc, {...replicaOptions, file: copyLocation}, 'wal2', mode);
    }

    case 'serving':
      return prepare(lc, replicaOptions, 'wal2', mode);

    default:
      throw new Error(`Invalid ReplicaMode ${mode}`);
  }
}

export function setUpMessageHandlers(
  lc: LogContext,
  replicator: Replicator & Partial<SQLiteChangeLogMaintainer>,
  parent: Worker,
  sqliteChangeLogMaintenanceEnabled = false,
) {
  handleSubscriptionsFrom(lc, parent, replicator);
  parent.onMessageType<SQLiteChangeLogMaintenanceRequest>(
    'sqliteChangeLogMaintenanceRequest',
    msg => {
      void handleSQLiteChangeLogMaintenanceRequest(
        lc,
        replicator,
        parent,
        msg,
        sqliteChangeLogMaintenanceEnabled,
      );
    },
  );
}

export interface SQLiteChangeLogMaintainer {
  purgeChangeLog(
    maintenance: SQLiteChangeLogMaintenance,
  ): Promise<SQLiteChangeLogPurgeResult>;
}

async function handleSQLiteChangeLogMaintenanceRequest(
  lc: LogContext,
  replicator: Replicator & Partial<SQLiteChangeLogMaintainer>,
  parent: Worker,
  value: unknown,
  enabled: boolean,
): Promise<void> {
  let requestID =
    value &&
    typeof value === 'object' &&
    'requestID' in value &&
    typeof value.requestID === 'string' &&
    value.requestID.length > 0
      ? value.requestID
      : 'invalid-request';
  try {
    const request = validateSQLiteChangeLogMaintenanceRequest(value);
    requestID = request.requestID;
    const purgeChangeLog = replicator.purgeChangeLog;
    assert(
      enabled && purgeChangeLog !== undefined,
      'SQLite change-log maintenance is disabled for this replicator',
    );
    const {requestID: _, ...maintenance} = request;
    const result = await purgeChangeLog.call(replicator, maintenance);
    parent.send<SQLiteChangeLogMaintenanceResponse>([
      'sqliteChangeLogMaintenanceResponse',
      {requestID, result},
    ]);
  } catch (error) {
    lc.warn?.('SQLite change-log maintenance request failed', error);
    parent.send<SQLiteChangeLogMaintenanceResponse>([
      'sqliteChangeLogMaintenanceResponse',
      {requestID, error: serializeError(error)},
    ]);
  }
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
      try {
        subscriber.send<Notification>(['notify', msg]);
      } catch (e) {
        const log =
          e instanceof Error &&
          'code' in e &&
          // This can happen in a race condition if the subscribing process
          // is closed before the 'close' message is processed.
          e.code === 'ERR_IPC_CHANNEL_CLOSED'
            ? 'warn'
            : 'error';

        lc[log]?.(
          `error sending replicator notification to ${subscriber.pid}: ${String(e)}`,
          e,
        );
      }
    }
  });
}

/**
 * Creates a Notifier to relay notifications the notifier of another Worker.
 * This does not send the initial subscription message. Use {@link subscribeTo}
 * to initiate the subscription.
 */
export function createNotifierFrom(
  _lc: LogContext,
  source: Worker,
  onNotify?: (state: ReplicaState) => void,
): Notifier {
  const notifier = new Notifier();
  source.onMessageType<Notification>('notify', msg => {
    onNotify?.(msg);
    void notifier.notifySubscribers(msg);
  });
  return notifier;
}

export function subscribeTo(_lc: LogContext, source: Worker) {
  source.send(['subscribe', {}]);
}
