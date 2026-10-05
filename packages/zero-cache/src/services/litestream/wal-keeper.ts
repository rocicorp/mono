import type {LogContext} from '@rocicorp/logger';
import type {Database} from '../../../../zqlite/src/db.ts';

/**
 * Keeps the WAL of a replica intact until `litestream replicate` has attached
 * to it, when the backup continues from the local litestream state (see
 * `ResumableBackup`), e.g. after a forking `litestream restore`.
 *
 * Litestream needs its latest local L0 file to correspond to the state of
 * the WAL to continue replication from it rather than creating a new snapshot.
 * Normally, the last connection to close a database checkpoints and deletes
 * its WAL, which loses the salt and makes litestream fall back to the
 * snapshot. The keeper prevents that because every connection holds a SHARED
 * lock on the database file while it is open in WAL mode, so while the keeper
 * is open, no other connection's close can be the last one.
 *
 * While the keeper is held, nothing can change the journal mode or take an
 * exclusive lock on the replica, as those fail with SQLITE_BUSY. The backup
 * replicator keeps a replica that is already in 'wal' mode as is, and skips
 * a due VACUUM (see `setupReplica`). Incremental schema migrations run in WAL
 * mode (see `runSchemaMigrations`).
 */
export class WalKeeper {
  readonly #lc: LogContext;
  #db: Database | null;

  /**
   * Takes ownership of `db`, an open connection to the replica, which it
   * closes when released.
   */
  constructor(lc: LogContext, db: Database) {
    this.#lc = lc.withContext('component', 'wal-keeper');
    // The SHARED lock is taken by the first read (if no read has taken it).
    db.prepare('SELECT count(*) FROM sqlite_master').get();
    this.#db = db;
    this.#lc.info?.(`holding the restored WAL of ${db.name}`);
  }

  get held(): boolean {
    return this.#db !== null;
  }

  /** Closes the keeper connection. Idempotent. */
  release(reason: string) {
    if (this.#db) {
      this.#lc.info?.(`releasing the restored WAL: ${reason}`);
      this.#db.close();
      this.#db = null;
    }
  }
}
