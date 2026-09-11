import {copyFileSync} from 'node:fs';
import {join} from 'node:path';
import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {
  LitestreamSyncClient,
  SyncResponse,
} from '../../services/litestream/litestream-controller.ts';
import {BACKFILLING_TABLE} from '../../services/replicator/schema/backfilling.ts';
import type {Trace} from './trace.ts';

export type SimBackupRecord = {
  readonly id: number;
  readonly file: string;
  /** The backed-up replica's state version, read from the copy. */
  readonly watermark: string;
  /** How many columns the copy is still backfilling. */
  readonly backfilling: number;
};

/**
 * Stands in for litestream and its bucket.
 *
 * A backup is a `VACUUM INTO` copy of the backup replicator's replica, on its
 * own autocommit connection (`VACUUM INTO` cannot run in a transaction), as
 * `serving-copy` does. Its watermark is read from the copy, never from the
 * replicator.
 *
 * It also answers the backup replicator's write-path `/sync` calls, checkpointing
 * the WAL as litestream's would. While stalled, those calls hang. That brings
 * the real backpressure: the checkpointer's soft wait times out, and past
 * `maxWalPages` it pauses the write path.
 */
export class SimBackup {
  readonly #dir: string;
  readonly #trace: Trace;
  readonly #records: SimBackupRecord[] = [];
  readonly #stalledSyncs = new Set<() => void>();
  #stalled = false;
  #syncs = 0;

  constructor(dir: string, trace: Trace) {
    this.#dir = dir;
    this.#trace = trace;
  }

  get latest(): SimBackupRecord | undefined {
    return this.#records.at(-1);
  }

  get records(): readonly SimBackupRecord[] {
    return this.#records;
  }

  get stalled(): boolean {
    return this.#stalled;
  }

  /** Backs up `replicaFile` as of its last commit. */
  take(lc: LogContext, replicaFile: string): SimBackupRecord {
    const id = this.#records.length + 1;
    const file = join(this.#dir, `backup-${id}.db`);
    const source = new Database(lc, replicaFile, {readonly: true});
    try {
      source.prepare(/*sql*/ `VACUUM INTO ?`).run(file);
    } finally {
      source.close();
    }
    const copy = new Database(lc, file, {readonly: true});
    let watermark: string;
    let backfilling = 0;
    try {
      ({stateVersion: watermark} = copy
        .prepare(/*sql*/ `SELECT "stateVersion" FROM "_zero.replicationState"`)
        .get<{stateVersion: string}>());
      if (
        copy
          .prepare(/*sql*/ `SELECT 1 FROM sqlite_master WHERE name = ?`)
          .get(BACKFILLING_TABLE) !== undefined
      ) {
        ({backfilling} = copy
          .prepare(
            /*sql*/ `SELECT count(*) AS "backfilling" FROM "${BACKFILLING_TABLE}"`,
          )
          .get<{backfilling: number}>());
      }
    } finally {
      copy.close();
    }
    const record = {id, file, watermark, backfilling};
    this.#records.push(record);
    this.#trace.emit('backup', 0, 'backup.taken', {id, watermark, backfilling});
    return record;
  }

  /** Materializes `record` at `replicaFile`, as a restore does. */
  restore(record: SimBackupRecord, replicaFile: string): void {
    copyFileSync(record.file, replicaFile);
    this.#trace.emit('backup', 0, 'backup.restored', {id: record.id});
  }

  /** The litestream control client of the backup replicator at `replicaFile`. */
  client(lc: LogContext, replicaFile: string): LitestreamSyncClient {
    return {
      sync: (_opts, signal) => this.#sync(lc, replicaFile, signal),
      close: () => {},
    };
  }

  stall(): void {
    this.#stalled = true;
    this.#trace.emit('backup', 0, 'backup.stall');
  }

  resume(): void {
    this.#stalled = false;
    this.#trace.emit('backup', 0, 'backup.resume', {
      released: this.#stalledSyncs.size,
    });
    for (const release of [...this.#stalledSyncs]) {
      release();
    }
  }

  async #sync(
    lc: LogContext,
    replicaFile: string,
    signal: AbortSignal | undefined,
  ): Promise<SyncResponse> {
    const txid = ++this.#syncs;
    if (this.#stalled) {
      this.#trace.emit('backup', 0, 'backup.sync-stalled', {txid});
      await new Promise<void>((resolve, reject) => {
        const onAbort = () => {
          this.#stalledSyncs.delete(release);
          reject(new AbortError('sync aborted'));
        };
        const release = () => {
          this.#stalledSyncs.delete(release);
          signal?.removeEventListener('abort', onAbort);
          resolve();
        };
        this.#stalledSyncs.add(release);
        signal?.addEventListener('abort', onAbort, {once: true});
      });
    }
    const db = new Database(lc, replicaFile);
    try {
      db.pragma('wal_checkpoint(PASSIVE)');
    } finally {
      db.close();
    }
    this.#trace.emit('backup', 0, 'backup.sync', {txid});
    return {
      status: 'synced_local',
      path: replicaFile,
      txid,
      ['replicated_txid']: txid,
    };
  }
}
