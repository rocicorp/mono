import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import * as v from 'shared/src/valita.js';
import {normalizedFilterSpecSchema} from '../../types/invalidation.js';
import {postgresTypeConfig} from '../../types/pg.js';
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
  invalidatingFromVersion: v.string(),
});

export type RegisterInvalidationFiltersResponse = v.Infer<
  typeof registerInvalidationFiltersResponse
>;

export interface Replicator {
  /**
   * Registers a set of InvalidationFilterSpecs.
   *
   * This is called by the ViewSyncer when initiating a session for a client.
   *
   * * For new clients / specs, the Replicator registers the specs with the InvalidationRegistry
   *   and returns the (current) state version at which it is safe to query the database and
   *   construct a CVR.
   *
   * * For catching up existing CVRs, the Replicator similarly checks the specs with the
   *   InvalidationRegistry and returns the latest `fromStateVersion` of all specs. If this
   *   version is equal to or earlier than the CVR version, the caller can proceed with catchup.
   *   However, it is possible for the Replicator to return a state version later than that of the
   *   CVR. This can happen, for example:
   *
   *   1. Old entries of the `InvalidationIndex` and `ChangeLog` are periodically pruned up to an
   *      "earliest version threshold" in order to manage storage usage. When this happens, the
   *     `fromStateVersion` entries of all older invalidation specs are bumped accordingly.
   *
   *   2. The `InvalidationRegistry` itself also goes through periodic cleanup whereby specs
   *      with a `lastRequested` time too far in the past are deleted to avoid unnecessary work.
   *      When the happens, requested specs may no longer be in the registry, and the Replicator
   *      will re-register them and return a (current) state version from which a CVR can safely
   *      be constructed.
   *
   *   If the returned state version is later than that of the CVR, the client "reset", by dropping
   *   all state and rerunning all queries from scratch.
   *
   * @param req The {@link InvalidationFilterSpec}s needed by the caller.
   * @returns The version from which all filters in the request are (or have been) active.
   */
  registerInvalidationFilters(
    req: RegisterInvalidationFiltersRequest,
  ): Promise<RegisterInvalidationFiltersResponse>;
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

  async start() {
    await initSyncSchema(
      this.#lc,
      this.id,
      this.#syncReplica,
      this.#upstreamUri,
    );
    await this.#incrementalSyncer.start(this.#lc);
  }

  registerInvalidationFilters(
    req: RegisterInvalidationFiltersRequest,
  ): Promise<RegisterInvalidationFiltersResponse> {
    return this.#invalidator.registerInvalidationFilters(this.#lc, req);
  }

  async stop() {}
}
