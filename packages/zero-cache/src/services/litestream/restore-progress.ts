import {statSync} from 'node:fs';
import type {LogContext} from '@rocicorp/logger';
import type {
  RestoreStatus,
  ViewSyncerStage,
  ViewSyncerStatusEvent,
} from '../../../../zero-events/src/status.ts';
import {publishEvent} from '../../observability/events.ts';

const PUBLISH_INTERVAL_MS = 5000;

type PublishFn = (lc: LogContext, event: ViewSyncerStatusEvent) => void;

/**
 * Publishes the progress of a view-syncer's litestream restore.
 *
 * `litestream restore` writes the database page by page into
 * `<replica>.tmp` and renames it to the replica when it is done, so the
 * size of the temporary file is the number of bytes restored so far.
 */
export class RestoreProgressReporter {
  readonly #lc: LogContext;
  readonly #replicaFile: string;
  readonly #publishFn: PublishFn;
  readonly #start = performance.now();
  #totalBytes: number | undefined;
  #lastBytes: number | undefined;
  #timer: NodeJS.Timeout | undefined;

  constructor(
    lc: LogContext,
    replicaFile: string,
    publishFn: PublishFn = publishEvent,
  ) {
    this.#lc = lc;
    this.#replicaFile = replicaFile;
    this.#publishFn = publishFn;
  }

  /**
   * Publishes a `Restoring` event now and then every `intervalMs` while the
   * number of restored bytes changes, until {@link stop} or {@link done}.
   */
  start(totalBytes: number | undefined, intervalMs = PUBLISH_INTERVAL_MS) {
    this.stop();
    this.#totalBytes = totalBytes;
    // Start from 0 rather than checking now: a `.tmp` left behind by an
    // interrupted restore is only deleted when the restore starts.
    this.#update(0);
    this.#timer = setInterval(() => this.#check(), intervalMs);
  }

  stop() {
    clearInterval(this.#timer);
    this.#timer = undefined;
  }

  /** Publishes a `Restored` event with the size of the restored replica. */
  done() {
    this.stop();
    this.#publish('Restored', fileSize(this.#replicaFile) ?? 0);
  }

  #check() {
    // litestream renames `.tmp` to the replica before its post-restore
    // integrity check, so once `.tmp` is gone the replica holds the bytes.
    this.#update(
      fileSize(`${this.#replicaFile}.tmp`) ?? fileSize(this.#replicaFile) ?? 0,
    );
  }

  #update(bytes: number) {
    if (bytes !== this.#lastBytes) {
      this.#lastBytes = bytes;
      this.#publish('Restoring', bytes);
    }
  }

  #publish(stage: ViewSyncerStage, bytes: number) {
    const restoreStatus: RestoreStatus = {
      bytes,
      totalBytes: this.#totalBytes,
      elapsedMs: Math.round(performance.now() - this.#start),
    };
    const event: ViewSyncerStatusEvent = {
      type: 'zero/events/status/view-syncer/v1',
      component: 'view-syncer',
      status: 'OK',
      stage,
      description:
        stage === 'Restoring'
          ? 'Restoring replica from backup'
          : 'Restored replica from backup',
      time: new Date().toISOString(),
      state: {restoreStatus},
    };
    this.#publishFn(this.#lc, event);
  }
}

function fileSize(file: string): number | undefined {
  try {
    return statSync(file).size;
  } catch {
    return undefined;
  }
}
