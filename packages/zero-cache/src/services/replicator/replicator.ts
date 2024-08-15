import type {LogContext} from '@rocicorp/logger';
import Database from 'better-sqlite3';
import type {ReadonlyJSONObject} from 'shared/src/json.js';
import type {PostgresDB} from '../../types/pg.js';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import type {Service} from '../service.js';
import {IncrementalSyncer} from './incremental-sync.js';
import {initSyncSchema} from './schema/sync-schema.js';

// The version ready payload is simply a signal. All of the information
// that the consumer needs is retrieved by opening a new snapshot transaction
// on the replica.
export type ReplicaVersionReady = NonNullable<unknown>;

export interface Replicator {
  /**
   * Returns an opaque message for human-readable consumption. This is
   * purely for ensuring that the Replicator has started at least once to
   * bootstrap a new replica.
   */
  status(): Promise<ReadonlyJSONObject>;

  /**
   * Creates a cancelable subscription of notifications when the replica is ready to be
   * read for new data. The first message is sent when the replica is ready (e.g. initialized),
   * and henceforth when an incremental change has been committed to the replica.
   *
   * Messages are coalesced if multiple notifications occur before the subscriber consumes
   * the next message. The messages themselves contain no information; the subscriber queries
   * the SQLite replica for the latest replicated changes.
   */
  subscribe(): Promise<CancelableAsyncIterable<ReplicaVersionReady>>;
}

export class ReplicatorService implements Replicator, Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #upstreamUri: string;
  readonly #upstream: PostgresDB;
  readonly #syncReplicaDbFile: string;
  readonly #incrementalSyncer: IncrementalSyncer;

  constructor(
    lc: LogContext,
    replicaID: string,
    upstreamUri: string,
    upstream: PostgresDB,
    syncReplicaDbFile: string,
  ) {
    this.id = replicaID;
    this.#lc = lc
      .withContext('component', 'Replicator')
      .withContext('serviceID', this.id);
    this.#upstreamUri = upstreamUri;
    this.#upstream = upstream;
    this.#syncReplicaDbFile = syncReplicaDbFile;

    const replica = new Database(syncReplicaDbFile);
    replica.pragma('journal_mode = WAL');
    // TODO: Any other replica setup required here?

    this.#incrementalSyncer = new IncrementalSyncer(
      upstreamUri,
      replicaID,
      replica,
    );
  }

  status() {
    return Promise.resolve({status: 'ok'});
  }

  async run() {
    await initSyncSchema(
      this.#lc,
      'replicator',
      this.id,
      this.#syncReplicaDbFile,
      this.#upstream,
      this.#upstreamUri,
    );

    await this.#incrementalSyncer.run(this.#lc);
  }

  subscribe(): Promise<CancelableAsyncIterable<ReplicaVersionReady>> {
    return this.#incrementalSyncer.subscribe();
  }

  async stop() {
    await this.#incrementalSyncer.stop(this.#lc);
  }
}
