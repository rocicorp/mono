import type {ZeroEvent} from './index.ts';
import type {JSONObject} from './json.ts';

export type Status = 'OK' | 'ERROR';

export const ZERO_STATUS_EVENT_PREFIX = 'zero/events/status/';

/**
 * A StatusEvent conveys the most current status of a given component,
 * with each event replacing any preceding status events (based on the
 * `time` field) for the same component.
 *
 * All StatusEvents have a `type` value that starts with `zero/events/status/`,
 * with common fields (e.g. `stage`, `description`, `errorDetails`) that can
 * be used to describe the state of any component even if the specific subtype
 * is not known. In this respect, an event consumer can subscribe to
 * "zero/events/status/*" and display general status information without
 * needing to understand subtype-specific fields.
 */
export interface StatusEvent extends ZeroEvent {
  type: `${typeof ZERO_STATUS_EVENT_PREFIX}${string}`;

  /**
   * The component of the zero-cache to which the event pertains,
   * e.g. "replication".
   */
  component: string;

  /** Whether the component is healthy. */
  status: Status;

  /**
   * The stage describing the component's current state. This is meant to be
   * both machine and human readable (e.g. a single work serving as a well-known
   * constant).
   */
  stage: string;

  /**
   * An optional, human readable description.
   */
  description?: string | undefined;

  /** Structured data describing the state of the component. */
  state?: JSONObject | undefined;

  /** Error details should be supplied for an 'ERROR' status message. */
  errorDetails?: JSONObject | undefined;
}

export type ReplicatedColumn = {
  column: string;
  upstreamType: string;
  clientType: string | null;
};

export type ReplicatedTable = {
  table: string;
  columns: ReplicatedColumn[];
};

export type IndexedColumn = {
  column: string;
  dir: 'ASC' | 'DESC';
};

export type ReplicatedIndex = {
  table: string;
  columns: IndexedColumn[];
  unique: boolean;
};

export type DownloadStatus = {
  table: string;
  columns: string[];
  totalRows: number;
  totalBytes?: number | undefined;

  rows: number;
};

/**
 * Progress of index creation, reported while an index is being built
 * (e.g. during initial sync, or when an index is created on an existing
 * table during replication).
 */
export type IndexingStatus = {
  /** The name of the index currently being created. */
  name: string;
  table: string;
  columns: string[];
  unique: boolean;

  /** The 1-based position of the current index. */
  index: number;

  /**
   * The total number of indexes to create. This is `undefined` when the
   * total is not known in advance (e.g. during replication).
   */
  totalIndexes?: number | undefined;

  /** Milliseconds spent so far creating the current index. */
  elapsedMs: number;

  /** Milliseconds spent creating the preceding (completed) indexes. */
  completedMs: number;

  /** Whether creation of the current index has completed. */
  done: boolean;
};

export type ReplicationState = {
  tables: ReplicatedTable[];
  indexes: ReplicatedIndex[];
  replicaSize?: number | undefined;
  downloadStatus?: DownloadStatus[] | undefined;
  indexingStatus?: IndexingStatus | undefined;
};

export type ReplicationStage = 'Initializing' | 'Indexing' | 'Replicating';

export const REPLICATION_STATUS_EVENT_V1_TYPE =
  'zero/events/status/replication/v1';

export interface ReplicationStatusEvent extends StatusEvent {
  type: typeof REPLICATION_STATUS_EVENT_V1_TYPE;
  component: 'replication';
  stage: ReplicationStage;
  state?: ReplicationState;
}

/**
 * Progress of a view-syncer restoring its replica from the
 * replication-manager's backup.
 */
export type RestoreStatus = {
  /** Bytes of the replica written so far. */
  bytes: number;

  /**
   * The estimated size of the restored replica, taken from the size of the
   * replication-manager's replica when it handed out the backup. This is
   * `undefined` when the replication-manager does not report it.
   */
  totalBytes?: number | undefined;

  /** Milliseconds spent so far restoring the replica. */
  elapsedMs: number;
};

export type ViewSyncerState = {
  restoreStatus?: RestoreStatus | undefined;
};

/**
 * - `Restoring`: the replica is being restored from the backup.
 * - `Restored`: the replica has been restored and the view-syncer is
 *   starting up.
 */
export type ViewSyncerStage = 'Restoring' | 'Restored';

export const VIEW_SYNCER_STATUS_EVENT_V1_TYPE =
  'zero/events/status/view-syncer/v1';

export interface ViewSyncerStatusEvent extends StatusEvent {
  type: typeof VIEW_SYNCER_STATUS_EVENT_V1_TYPE;
  component: 'view-syncer';
  stage: ViewSyncerStage;
  state?: ViewSyncerState;
}
