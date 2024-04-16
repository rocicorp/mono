import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import * as v from 'shared/src/valita.js';
import {normalizedFilterSpecSchema} from '../../types/invalidation.js';
import {postgresTypeConfig} from '../../types/pg.js';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import type {Service} from '../service.js';
import {IncrementalSyncer} from './incremental-sync.js';
import {InvalidationFilters, Invalidator} from './invalidation.js';
import {initSyncSchema} from './schema/sync-schema.js';

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
   * A mapping from hex invalidation hash to the latest version in which
   * the invalidation occurred, if greater than `prevVersion`. The inclusion
   * of the invalidations is optional and may be absent if the number of
   * invalidations exceeds a certain limit. Consumers must consider this field
   * an optional optimization and handle its absence accordingly.
   */
  invalidations: v.readonlyRecord(v.string()).optional(),
});

export type VersionChange = v.Infer<typeof versionChangeSchema>;

export interface Replicator {
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
  versionChanges(): CancelableAsyncIterable<VersionChange>;
}

export class ReplicatorService implements Replicator, Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #upstreamUri: string;
  readonly #syncReplica: postgres.Sql;
  readonly #incrementalSyncer: IncrementalSyncer;
  readonly #invalidator: Invalidator;

  constructor(
    lc: LogContext,
    replicaID: string,
    upstreamUri: string,
    syncReplicaUri: string,
  ) {
    this.id = replicaID;
    this.#lc = lc
      .withContext('component', 'Replicator')
      .withContext('serviceID', this.id);
    this.#upstreamUri = upstreamUri;
    this.#syncReplica = postgres(syncReplicaUri, {
      ...postgresTypeConfig(),
    });

    // This lock ensures that transactions are processed serially, even
    // across re-connects to the upstream db.
    const txSerializer = new Lock();
    const invalidationFilters = new InvalidationFilters();

    this.#incrementalSyncer = new IncrementalSyncer(
      upstreamUri,
      replicaID,
      this.#syncReplica,
      txSerializer,
      invalidationFilters,
    );
    this.#invalidator = new Invalidator(
      this.#syncReplica,
      txSerializer,
      invalidationFilters,
    );
  }

  async run() {
    await initSyncSchema(
      this.#lc,
      this.id,
      this.#syncReplica,
      this.#upstreamUri,
    );
    await this.#incrementalSyncer.run(this.#lc);
  }

  registerInvalidationFilters(
    req: RegisterInvalidationFiltersRequest,
  ): Promise<RegisterInvalidationFiltersResponse> {
    return this.#invalidator.registerInvalidationFilters(this.#lc, req);
  }

  versionChanges(): CancelableAsyncIterable<VersionChange> {
    return this.#incrementalSyncer.versionChanges();
  }

  async stop() {}
}
