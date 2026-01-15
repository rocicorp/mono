import type {LogContext} from '@rocicorp/logger';
import type {ReadonlyJSONObject} from '../../../../shared/src/json.ts';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import type {Source} from '../../types/streams.ts';
import type {ChangeStreamer} from '../change-streamer/change-streamer.ts';
import type {Service} from '../service.ts';
import {IncrementalSyncer} from './incremental-sync.ts';

/** See {@link ReplicaStateNotifier.subscribe()}. */
export type ReplicaState = {
  readonly state: 'version-ready';

  // Used in tests to verify behavior when additional information
  // is ferried in the future. Not set in production.
  readonly testSeqNum?: number;
};

export interface ReplicaStateNotifier {
  /**
   * Creates a cancelable subscription of changes in the replica state.
   *
   * A `version-ready` message indicates that the replica is ready to be
   * read, and henceforth that a _new_ version is ready, i.e. whenever a
   * change is committed to the replica. The `version-ready` message itself
   * otherwise contains no other information; the subscriber queries the
   * replica for the current data.
   *
   * A `maintenance` state indicates that the replica should not be read from.
   * If a subscriber is holding any transaction locks, it should release them
   * until the next `version-ready` signal.
   *
   * Upon subscription, the current state of the replica is sent immediately
   * if known. If multiple notifications occur before the subscriber
   * can consume them, all but the last notification are discarded by the
   * Subscription object (i.e. not buffered). Thus, a subscriber only
   * ever consumes the current (i.e. known) state of the replica. This avoids
   * a buildup of "work" if a subscriber is too busy to consume all
   * notifications.
   */
  subscribe(): Source<ReplicaState>;
}

export interface Replicator extends ReplicaStateNotifier {
  /**
   * Returns an opaque message for human-readable consumption. This is
   * purely for ensuring that the Replicator has started at least once to
   * bootstrap a new replica.
   */
  status(): Promise<ReadonlyJSONObject>;
}

export type ReplicatorMode = 'backup' | 'serving';

export class ReplicatorService implements Replicator, Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #incrementalSyncer: IncrementalSyncer;

  constructor(
    lc: LogContext,
    taskID: string,
    id: string,
    mode: ReplicatorMode,
    changeStreamer: ChangeStreamer,
    replica: Database,
    publishReplicationStatus: boolean,
  ) {
    this.id = id;
    this.#lc = lc
      .withContext('component', 'replicator')
      .withContext('serviceID', this.id);

    this.#incrementalSyncer = new IncrementalSyncer(
      taskID,
      `${taskID}/${id}`,
      changeStreamer,
      replica,
      mode,
      publishReplicationStatus,
    );
  }

  status() {
    return Promise.resolve({status: 'ok'});
  }

  run() {
    return this.#incrementalSyncer.run(this.#lc);
  }

  subscribe(): Source<ReplicaState> {
    return this.#incrementalSyncer.subscribe();
  }

  stop() {
    this.#incrementalSyncer.stop(this.#lc);
    return promiseVoid;
  }
}
