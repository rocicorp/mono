import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {ReadonlyJSONObject} from 'shared/src/json.js';
import * as v from 'shared/src/valita.js';
import {jsonObjectSchema} from '../../types/bigint-json.js';
import {normalizedFilterSpecSchema} from '../../types/invalidation.js';
import type {PostgresDB} from '../../types/pg.js';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import type {Service} from '../service.js';
import {IncrementalSyncer} from './incremental-sync.js';
import {InvalidationFilters, Invalidator} from './invalidation.js';
import {initSyncSchema} from './schema/sync-schema.js';
import {TransactionTrainService} from './transaction-train.js';

export const registerInvalidationFiltersRequest = v.object({
  specs: v.array(normalizedFilterSpecSchema),
});

export type RegisterInvalidationFiltersRequest = v.Infer<
  typeof registerInvalidationFiltersRequest
>;

export const registerInvalidationFiltersResponse = v.object({
  specs: v.array(
    v.object({
      id: v.string(),
      fromStateVersion: v.string(),
    }),
  ),
});

export type RegisterInvalidationFiltersResponse = v.Infer<
  typeof registerInvalidationFiltersResponse
>;

export const truncateChangeSchema = v.object({
  schema: v.string(),
  table: v.string(),
  rowKey: v.undefined().optional(),
  rowData: v.undefined().optional(),
});

export const deleteRowChangeSchema = v.object({
  schema: v.string(),
  table: v.string(),
  rowKey: jsonObjectSchema,
  rowData: v.undefined().optional(),
});

export const putRowChangeSchema = v.object({
  schema: v.string(),
  table: v.string(),
  rowKey: jsonObjectSchema,
  rowData: jsonObjectSchema,
});

export const rowChangeSchema = v.union(
  truncateChangeSchema,
  deleteRowChangeSchema,
  putRowChangeSchema,
);

export type RowChange = v.Infer<typeof rowChangeSchema>;

export const versionChangeSchema = v.object({
  /** The new version. */
  newVersion: v.string(),

  /**
   * The previous version. Note that multiple VersionChanges may be coalesced
   * such that multiple transactions occurred between the `prevVersion` and
   * `newVersion`.
   */
  prevVersion: v.string(),

  /**
   * The Postgres snapshot id of the database state at `prevVersion`, which can be
   * accessed via the `SET TRANSACTION SNAPSHOT <snapshot-id>` statement. The
   * Replicator will keep the snapshot ID valid (by holding a transaction open)
   * until all subscribers have consumed the VersionChange, as communicated by the
   * Subscription / CancelableAsyncIterable chain from the Replicator to the
   * consumer(s), or until a timeout has elapsed. Subscribers should create
   * snapshot transactions (as necessary) and ACK as soon as possible to free up
   * Replicator resources.
   */
  prevSnapshotID: v.string(),

  /**
   * A mapping from hex invalidation hash to the latest version in which
   * the invalidation occurred, if greater than `prevVersion`.
   *
   * The inclusion of the invalidations is optional and may be absent if the
   * number of invalidations exceeds a certain limit. Consumers must consider
   * this field an optional optimization and handle its absence accordingly.
   */
  invalidations: v.readonlyRecord(v.string()).optional(),

  /**
   * An ordered list RowChanges comprising the difference between the `prevVersion`
   * and `newVersion`. Redundant changes may or may not be removed (e.g. an insert
   * followed by a delete of the same row). Combined with the fact that row changes
   * are not associated specific version, it is important that the client process the
   * changes either in their entirety or not at all, as a partial scan is not necessarily
   * be consistent with any snapshot of the database.
   *
   * The inclusion of changes is optional and may be absent if the number of changes
   * exceeds a certain limit. Consumers must consider this field an optional optimization
   * and handle its absence accordingly.
   */
  changes: v.readonly(v.array(rowChangeSchema)).optional(),
});

export type VersionChange = v.Infer<typeof versionChangeSchema>;

export interface Replicator {
  /**
   * Returns an opaque message for human-readable consumption. This is
   * purely for ensuring that the Replicator has started at least once to
   * bootstrap a new replica.
   */
  status(): Promise<ReadonlyJSONObject>;

  /**
   * Registers a set of InvalidationFilterSpecs.
   *
   * This is called by the View Syncer (via the View Notifier) when initiating a
   * session for a client, either from scratch or from an existing CVR, and when
   * its set of queries (and thus filters) changes.
   *
   * The Replicator ensures all filters are registered, returning the state versions
   * from which each filter has been active (i.e. used for populating the Invalidation
   * Index during replication).
   *
   * In the common case, the filters will have been registered in the past and all
   * versions will be older than the CVR version. However, it is possible for a spec's
   * `fromStateVersion` to be later than that of the CVR. This can happen because:
   *
   * 1. Old entries of the `InvalidationIndex` and `ChangeLog` are periodically pruned up to an
   *    "earliest version threshold" in order to manage storage usage. When this happens, the
   *   `fromStateVersion` entries of all older invalidation specs are bumped accordingly.
   *
   * 2. The `InvalidationRegistry` itself also goes through periodic cleanup whereby specs
   *    with a `lastRequested` time very far in the past are deleted to avoid unnecessary work.
   *    When the happens, requested specs may no longer be in the registry, and the Replicator
   *    will re-register them and return a (current) state version.
   *
   * Note that in both cases the CVR is very old and the queries are likely to be invalid
   * anyway. If the returned state version of a spec is later than that of the CVR, the
   * the associated queries must be considered invalidated and re-executed.
   *
   * @param req The {@link NormalizedInvalidationFilterSpec}s needed by the caller.
   * @returns The versions from which each filter has been active.
   */
  registerInvalidationFilters(
    req: RegisterInvalidationFiltersRequest,
  ): Promise<RegisterInvalidationFiltersResponse>;

  /**
   * Creates a cancelable subscription to {@link VersionChange} messages for the
   * stream of replicated transactions.
   */
  versionChanges(): Promise<CancelableAsyncIterable<VersionChange>>;
}

export class ReplicatorService implements Replicator, Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #upstreamUri: string;
  readonly #upstream: PostgresDB;
  readonly #syncReplica: PostgresDB;
  readonly #txTrain: TransactionTrainService;
  readonly #incrementalSyncer: IncrementalSyncer;
  readonly #invalidator: Invalidator;
  readonly #ready = resolver();

  constructor(
    lc: LogContext,
    replicaID: string,
    upstreamUri: string,
    upstream: PostgresDB,
    syncReplica: PostgresDB,
  ) {
    this.id = replicaID;
    this.#lc = lc
      .withContext('component', 'Replicator')
      .withContext('serviceID', this.id);
    this.#upstreamUri = upstreamUri;
    this.#upstream = upstream;
    this.#syncReplica = syncReplica;

    this.#txTrain = new TransactionTrainService(this.#lc, syncReplica);
    const invalidationFilters = new InvalidationFilters();

    this.#incrementalSyncer = new IncrementalSyncer(
      upstreamUri,
      replicaID,
      this.#syncReplica,
      this.#txTrain,
      invalidationFilters,
    );
    this.#invalidator = new Invalidator(
      this.#syncReplica,
      this.#txTrain,
      invalidationFilters,
    );
  }

  status() {
    return Promise.resolve({status: 'ok'});
  }

  async run() {
    await initSyncSchema(
      this.#lc,
      this.id,
      this.#syncReplica,
      this.#upstream,
      this.#upstreamUri,
    );

    this.#ready.resolve();

    void this.#txTrain.run();

    await this.#incrementalSyncer.run(this.#lc);
  }

  async registerInvalidationFilters(
    req: RegisterInvalidationFiltersRequest,
  ): Promise<RegisterInvalidationFiltersResponse> {
    // Registration requires the sync schema to be initialized.
    await this.#ready.promise;
    return this.#invalidator.registerInvalidationFilters(this.#lc, req);
  }

  versionChanges(): Promise<CancelableAsyncIterable<VersionChange>> {
    return this.#incrementalSyncer.versionChanges();
  }

  async stop() {
    await this.#incrementalSyncer.stop(this.#lc);
    await this.#txTrain.stop();
  }
}
