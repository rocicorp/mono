import type {LogContext} from '@rocicorp/logger';
import {Database} from 'zqlite/src/db.js';
import type {ReadonlyJSONObject} from 'shared/src/json.js';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import type {Service} from '../service.js';
import {IncrementalSyncer} from './incremental-sync.js';
import {initSyncSchema} from './schema/sync-schema.js';

// The version ready payload is simply a signal. All of the information
// that the consumer needs is retrieved by opening a new snapshot transaction
// on the replica.
export type ReplicaVersionReady = NonNullable<unknown>;

export interface ReplicaVersionNotifier {
  /**
   * Creates a cancelable subscription of notifications when the replica is ready to be
   * read for new data. The first message is sent when the replica is ready (e.g. initialized),
   * and henceforth when an incremental change has been committed to the replica.
   *
   * Messages are coalesced if multiple notifications occur before the subscriber consumes
   * the next message. The messages themselves contain no information; the subscriber queries
   * the SQLite replica for the latest replicated changes.
   */
  subscribe(): CancelableAsyncIterable<ReplicaVersionReady>;
}

export interface Replicator extends ReplicaVersionNotifier {
  /**
   * Returns an opaque message for human-readable consumption. This is
   * purely for ensuring that the Replicator has started at least once to
   * bootstrap a new replica.
   */
  status(): Promise<ReadonlyJSONObject>;
}

export class ReplicatorService implements Replicator, Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #upstreamUri: string;
  readonly #syncReplicaDbFile: string;
  readonly #incrementalSyncer: IncrementalSyncer;

  constructor(
    lc: LogContext,
    replicaID: string,
    upstreamUri: string,
    syncReplicaDbFile: string,
  ) {
    this.id = replicaID;
    this.#lc = lc
      .withContext('component', 'Replicator')
      .withContext('serviceID', this.id);
    this.#upstreamUri = upstreamUri;
    this.#syncReplicaDbFile = syncReplicaDbFile;

    const replica = new Database(this.#lc, syncReplicaDbFile);
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
      this.#upstreamUri,
    );

    await this.#incrementalSyncer.run(this.#lc);
  }

  subscribe(): CancelableAsyncIterable<ReplicaVersionReady> {
    return this.#incrementalSyncer.subscribe();
  }

  async stop() {
    await this.#incrementalSyncer.stop(this.#lc);
  }
}
