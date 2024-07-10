import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {assert} from 'shared/src/asserts.js';
import {union} from 'shared/src/set-utils.js';
import {sleep} from 'shared/src/sleep.js';
import {
  Mode,
  TransactionPool,
  importSnapshot,
  sharedSnapshot,
} from '../../db/transaction-pool.js';
import {stringify} from '../../types/bigint-json.js';
import {
  max,
  min,
  type LexiVersion,
} from 'zqlite-zero-cache-shared/src/lexi-version.js';
import type {PostgresDB} from '../../types/pg.js';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import type {InvalidationInfo} from '../../zql/invalidation.js';
import type {ReplicatorRegistry} from '../replicator/registry.js';
import type {RowChange, VersionChange} from '../replicator/replicator.js';
import {queryStateVersion} from '../replicator/schema/replication.js';
import {
  PublicationInfo,
  getPublicationInfo,
} from '../replicator/tables/published.js';
import type {TableSpec} from '../replicator/tables/specs.js';
import type {Service} from '../service.js';
import {HashSubscriptions} from './hash-subscriptions.js';

export type WatchRequest = {
  /**
   * Maps caller-defined query IDs to corresponding invalidation filters and hashes.
   */
  queries: Record<string, InvalidationInfo>;

  /**
   * The starting version from which to watch for query invalidation (i.e. the CVR version),
   * absent if the caller is starting from scratch and has no queries (rows) to invalidate.
   */
  fromVersion?: string | undefined;
};

export type QueryInvalidationUpdate = {
  /** The version of the database at the time the invalidations were processed. */
  version: LexiVersion;

  /**
   * The starting point (exclusive) from which invalidations were computed. This will be
   * `null` in the initial update for a {@link WatchRequest} with no `fromVersion`, in which
   * case invalidations are not computed (because all queries must be executed anyway).
   */
  fromVersion: LexiVersion | null;

  /** Set of caller-defined query IDs which were within the version range. */
  invalidatedQueries: Set<string>;

  /**
   * A `READ ONLY` TransactionPool snapshotted at the new `version` for re-executing
   * invalidated queries.
   */
  reader: TransactionPool;

  /**
   * A `READ ONLY` TransactionPool snapshotted at the `fromVersion` used for computing
   * IVM updates. This is available for updates triggered from the Replicator's
   * `VersionChange` message.
   *
   * In exceptional circumstances the snapshot may not be imported successfully
   * (e.g. if the Replicator process holding the exporting transaction was shut down).
   * In such cases, the `prevReader` will be undefined and the sync logic would need
   * to fall back to rerunning invalidated queries.
   */
  prevReader?: TransactionPool | undefined;

  /**
   * An ordered list RowChanges comprising the difference between the `fromVersion`
   * and `newVersion`. Redundant changes may or may not be removed (e.g. an insert
   * followed by a delete of the same row). Combined with the fact that row changes
   * are not associated specific version, it is important that the client process the
   * changes either in their entirety or not at all, as a partial scan will not
   * necessarily be consistent with any snapshot of the database.
   *
   * Currently, changes are only included when received from the Replicator's
   * `VersionChange` message for the exact version range.
   */
  changes: readonly RowChange[] | undefined;

  /** New table schemas if there was a schema change. */
  tableSchemas?: TableSpec[];
};

/**
 * An Invalidation Watcher is a per-Service Runner (i.e. Durable Object) service that
 * serves as the liaison between the View Syncers in the Service Runner and the global
 * Replicator.
 *
 * ```
 * ┌-------------------------------------------------------┐
 * |                                      <--> View Syncer |
 * | Replicator <--> Invalidation Watcher <--> View Syncer |
 * |     ^                                <--> View Syncer |
 * └-----|-------------------------------------------------┘
 *       |
 * ┌-----|-------------------------------------------------┐
 * |     |                                <--> View Syncer |
 * |     └---------> Invalidation Watcher <--> View Syncer |
 * |                                      <--> View Syncer |
 * └-------------------------------------------------------┘
 * ```
 *
 * The Invalidation Watcher serves two architectural purposes:
 *
 * * **Reduces notification fan-out from the Replicator**: Replicators only need to manage
 *   `O(num-service-runners)` notification streams, which is orders of magnitudes less than
 *   `O(num-view-syncers)`.
 *
 * * **Reduces query fan-in when computing view invalidation**: View Syncers register their
 *   invalidation hashes with the Invalidation Watcher. On each replication change, the
 *   Invalidation Watcher makes a single, composite query on the Invalidation Index, as
 *   opposed to all View Syncers querying individually. This is a critical scalability
 *   component; the connection, cpu, and I/O usage incurred by having all View Syncers
 *   query the index for every transaction would be otherwise untenable.
 *
 * As a logistical corollary to the latter, the Invalidation Watcher also plays a role in
 * connection / transaction management. On each replication change, the Invalidation Watcher
 * creates a read-only TransactionPool, initially sized for a single connection, to query
 * the Invalidation Index. If queries have been invalidated, it passes the TransactionPool
 * to the corresponding View Syncers to execute their queries at the same snapshot of the
 * database, growing the pool to a configurable maximum number of connections to increase
 * concurrency and reduce latency. When the View Syncers finish, the `Subscription` cleanup
 * logic facilitates reference counting to close TransactionPools when they are no longer
 * needed.
 *
 * In other words, the InvalidationWatcher is where all TransactionPools for view
 * queries originate. All View Syncers access the database via TransactionPools
 * (and corresponding invalidation info) managed by the InvalidationWatcher.
 */
export interface InvalidationWatcher {
  /**
   * Returns the current table schemas (needed for primary key information).
   */
  getTableSchemas(): Promise<readonly TableSpec[]>;

  /**
   * Creates a Subscription of {@link QueryInvalidationUpdate}s for the set of queries
   * specified in the {@link WatchRequest}.
   *
   * * `watch()` ensures that all Invalidation Filter Specs are
   *   registered with the Replicator, noting the starting version from which each
   *   filter has been active.
   *
   * * At the same time, it queries the Invalidation Index for all specified
   *   invalidation hashes to see if any have been invalidated since the caller's
   *   `fromVersion` field (i.e. the version of the CVR.)
   *
   * The first update returned in the subscription spans the `fromVersion` specified
   * in the `request`, up to the current `newVersion` of the database, indicating the
   * `invalidatedQueries` that have hashes or filter registrations that are newer than
   * `fromVersion`.
   *
   * Subsequent updates are sent for incremental invalidations as new transactions are
   * replicated. If the subscriber (i.e. View Syncer) takes a long time to process an
   * update (i.e. re-executing queries) during which multiple new updates are produced,
   * those updates will be coalesced into a single update representing the cumulative
   * invalidations since the last one processed.
   *
   * For new views with no existing data, the caller should omit the `fromVersion` field
   * from the request. In this case, the query to the Invalidation Index will be skipped,
   * and the first message will span `{fromVersion: newVersion, newVersion: newVersion}`
   * with no invalidated queries.
   */
  watch(
    request: WatchRequest,
  ): Promise<CancelableAsyncIterable<QueryInvalidationUpdate>>;
}

const INITIAL_RETRY_DELAY_MS = 100;
const MAX_RETRY_DELAY_MS = 10000;

const READER_INITIAL_WORKERS = 1;
export const READER_MAX_WORKERS = 4;

export class InvalidationWatcherService
  implements InvalidationWatcher, Service
{
  readonly id: string;
  readonly #lc: LogContext;
  readonly #registry: ReplicatorRegistry;
  readonly #replica: PostgresDB;

  #latestReader: ReaderAtVersion | undefined;
  readonly #hashSubscriptions = new HashSubscriptions();

  #started = false;
  readonly #shouldRun = resolver<false>();
  #hasWatchRequests = resolver<true>();

  #cachedTableSchemas: readonly TableSpec[] | undefined;

  #versionChangeSubscription:
    | CancelableAsyncIterable<VersionChange>
    | undefined;

  constructor(
    serviceID: string,
    lc: LogContext,
    registry: ReplicatorRegistry,
    replica: PostgresDB,
  ) {
    this.id = serviceID;
    this.#lc = lc
      .withContext('component', 'invalidation-watcher')
      .withContext('id', this.id);
    this.#registry = registry;
    this.#replica = replica;
  }

  /**
   * The `run` loop waits for {@link watch} requests to arrive, and subscribes to
   * VersionChanges from the Replicator when they do. Where there are no watch
   * requests running (i.e. all canceled), the VersionChange subscription is
   * canceled and the loop again waits for {@link watch} requests to arrive.
   */
  async run(): Promise<void> {
    assert(!this.#started, `InvalidationWatcher has already been started`);
    this.#started = true;

    this.#lc.info?.('started');
    while (
      await Promise.race([
        this.#shouldRun.promise, // resolves to false on stop()
        this.#hasWatchRequests.promise, // resolves to true on a watch request
      ])
    ) {
      const replicator = await this.#registry.getReplicator();
      this.#lc.info?.('subscribing to VersionChanges');

      // The Subscription is canceled when there are no longer any watchers.
      this.#versionChangeSubscription = await replicator.versionChanges();
      for await (const versionChange of this.#versionChangeSubscription) {
        await this.#processVersionChange(versionChange);
      }

      this.#versionChangeSubscription = undefined;
      this.#lc.info?.(`waiting for watchers`);
    }

    this.#lc.info?.('stopped');
  }

  // eslint-disable-next-line require-await
  async stop(): Promise<void> {
    this.#shouldRun.resolve(false);
    this.#versionChangeSubscription?.cancel();
    this.#versionChangeSubscription = undefined;

    if (this.#latestReader) {
      const {reader} = this.#latestReader;
      reader.unref();
      await reader.done();
    }
  }

  async getTableSchemas(): Promise<readonly TableSpec[]> {
    if (!this.#cachedTableSchemas) {
      let published: PublicationInfo;
      for (
        let delay = INITIAL_RETRY_DELAY_MS;
        ;
        delay = Math.min(delay * 2, MAX_RETRY_DELAY_MS)
      ) {
        published = await getPublicationInfo(this.#replica);
        if (
          published.tables.find(
            t => t.schema === 'zero' && t.name === 'clients',
          )
        ) {
          break;
        }
        // This is a bootstrapping condition that happens for sync requests
        // received when initial replication is still happening.
        this.#lc.info?.(`No zero.clients table. Retrying in ${delay} ms`);
        await sleep(delay);
      }
      this.#cachedTableSchemas = published.tables;
    }
    return this.#cachedTableSchemas;
  }

  async watch(
    request: WatchRequest,
  ): Promise<CancelableAsyncIterable<QueryInvalidationUpdate>> {
    const subscription: Subscription<QueryInvalidationUpdate> =
      Subscription.create<QueryInvalidationUpdate>({
        // Coalescing {@link QueryInvalidationUpdate} messages is important in two contexts:
        //
        // * Capping the amount of outstanding work a subscriber has to process when
        //   the rate of incremental updates outpaces the rate at which queries are
        //   re-executing and views updated.
        //
        // * Combining the initial QueryInvalidationUpdate, computed from the parameters of the
        //   view's {@link WatchRequest}, with that of updates concurrently produced from
        //   incremental {@link VersionChange} updates from the Replicator. This ensures that
        //   the invalidations in the first Subscription message correctly catch the caller
        //   up to the main {@link VersionChange} stream.
        coalesce: (curr, prev) => {
          // Since the update for the initial filter registration step may be out of order
          // with respect to incremental VersionChange updates, ensure that `curr` (and
          // importantly, its `reader`) is the one with the latest `newVersion`.
          if (prev.version > curr.version) {
            const tmp = curr;
            curr = prev;
            prev = tmp;
          }
          // The Subscription will not call `consumed()` on coalesced `prev` messages.
          // cleanup must be done explicitly when coalescing. Unref the intermediate
          // readers that are coalesced away.
          prev.reader.unref();
          curr.prevReader?.unref();

          return {
            version: max(prev.version, curr.version),
            fromVersion:
              prev.fromVersion === null || curr.fromVersion === null
                ? null
                : min(prev.fromVersion, curr.fromVersion),
            invalidatedQueries: union(
              prev.invalidatedQueries,
              curr.invalidatedQueries,
            ),
            reader: curr.reader,
            changes:
              !prev.changes ||
              !curr.changes ||
              prev.version !== curr.fromVersion
                ? undefined
                : [...prev.changes, ...curr.changes],
          };
        },

        consumed: prev => {
          prev.reader.unref();
          prev.prevReader?.unref();
        },

        cleanup: unconsumed => {
          for (const update of unconsumed) {
            update.reader.unref();
            update.prevReader?.unref();
          }
          this.#hashSubscriptions.remove(subscription, request);
          if (this.#hashSubscriptions.empty()) {
            this.#hasWatchRequests = resolver<true>(); // reset to wait for next watch()
            this.#versionChangeSubscription?.cancel();
          }
        },
      });

    // Add the subscription to #hashSubscriptions so that it starts receiving incremental
    // updates. However, don't return the subscription to the caller until filters
    // are registered and the resulting initial invalidation update is pushed,
    // which will be coalesced with any concurrently received the incremental updates.
    this.#hashSubscriptions.add(subscription, request);

    // Start a subscription to Replicator.versionChanges() in run() if there isn't one already.
    this.#hasWatchRequests.resolve(true);

    // Compute and push/coalesce the initial update.
    await this.#registerFilters(request, subscription);

    // Now return the subscription, which is guaranteed to have the initial update,
    // possibly coalesced with incremental VersionChanges from the Replicator.
    return subscription;
  }

  /**
   * Registers the invalidation filters specified in the {@link WatchRequest} and
   * pushes an initial {@link QueryInvalidationUpdate} to the `subscription` based
   * on the registration version of the filters and any `hashes` from the
   * WatchRequest that were updated since its `fromVersion`.
   */
  async #registerFilters(
    request: WatchRequest,
    subscription: Subscription<QueryInvalidationUpdate>,
  ) {
    const lc = this.#lc.withContext('registerFilters', request.fromVersion);

    const replicator = await this.#registry.getReplicator();
    const registered = await replicator.registerInvalidationFilters({
      specs: Object.values(request.queries)
        .map(({filters}) => filters)
        .flat(),
    });

    const {fromVersion} = request;

    // The minVersion is the latest (i.e. max) `fromStateVersion` of the registered specs.
    const minVersion = registered.specs.reduce(
      (acc, {fromStateVersion: version}) => (acc > version ? acc : version),
      fromVersion ?? '00',
    );
    const {update, hashes} = await this.#createBaseUpdate(
      lc,
      fromVersion,
      minVersion,
    );

    const invalidatedQueries =
      this.#hashSubscriptions.computeInvalidationUpdate(hashes, subscription);

    if (fromVersion) {
      // Invalidate any queries associated with newly registered filters.
      const newlyRegisteredFilterIDs = new Set(
        registered.specs
          .filter(({fromStateVersion}) => fromStateVersion > fromVersion)
          .map(({id}) => id),
      );
      Object.entries(request.queries).forEach(([queryID, {filters}]) => {
        if (filters.some(({id}) => newlyRegisteredFilterIDs.has(id))) {
          invalidatedQueries.add(queryID);
        }
      });
      this.#lc.info?.(
        `initial update has ${invalidatedQueries.size} invalidated queries ` +
          `from ${hashes.size} hashes and ${newlyRegisteredFilterIDs.size} ` +
          `new filters`,
      );
    }

    update.reader.ref();
    subscription.push({...update, invalidatedQueries});
  }

  /**
   * Processes a {@link VersionChange} update from the Replicator and pushes
   * {@link QueryInvalidationUpdate} messages to all affected subscribers.
   */
  async #processVersionChange(versionChange: VersionChange) {
    const lc = this.#lc.withContext('versionChange', versionChange.newVersion);
    lc.debug?.(`processing VersionChange ${stringify(versionChange)}`);

    // TODO: Plumb schema changes from the logical replication stream through the
    //       VersionChange updates, and handle them by reloading the table schemas
    //       when constructing the base update.
    const {update, hashes} = await this.#createBaseUpdate(
      lc,
      versionChange.prevVersion,
      versionChange.newVersion,
      versionChange.prevSnapshotID,
      versionChange.invalidations,
      versionChange.changes,
    );

    const updates = this.#hashSubscriptions.computeInvalidationUpdates(hashes);
    const numUpdates = updates.size;

    if (numUpdates === 0) {
      lc.debug?.(`no views to update from ${hashes.size} hashes`);
    } else {
      lc.info?.(`${numUpdates} view updates for ${hashes.size} hashes`);

      update.reader.ref(numUpdates);
      update.prevReader?.ref(numUpdates);
      for (const [subscription, queryIDs] of updates) {
        subscription.push({...update, invalidatedQueries: queryIDs});
      }
    }
    // Release the `prevReader` reference from #getOrCreatePrevReader().
    // Leave the `reader` alone at it is referenced as the #latestReader.
    update.prevReader?.unref();
  }

  async #getOrCreatePrevReader(
    lc: LogContext,
    prevVersion: LexiVersion,
    prevSnapshotID: string,
  ): Promise<TransactionPool | undefined> {
    if (this.#latestReader && this.#latestReader.version === prevVersion) {
      lc.debug?.(
        `Reusing cached reader @${this.#latestReader.version} as prevReader`,
      );
      const {reader} = this.#latestReader;
      // The #latestReader reference is considered one ref. Increment the ref count when
      // reusing it as the prevReader since this means that the #latestReader will be
      // replaced (and unref-ed in the process).
      reader.ref();
      return reader;
    }
    const {init, imported} = importSnapshot(prevSnapshotID);
    const prevReader = new TransactionPool(
      this.#lc,
      Mode.READONLY,
      init,
      undefined,
      READER_INITIAL_WORKERS,
      READER_MAX_WORKERS,
    ); // TODO: Choose maxWorkers more intelligently / dynamically.
    prevReader.run(this.#replica).catch(e => lc.error?.(e));
    try {
      await imported;
    } catch (e) {
      lc.error?.('Failed to create prevReader', e);
      return undefined;
    }

    lc.debug?.(
      `Created prevReader @${prevVersion} from snapshot ${prevSnapshotID}`,
    );
    return prevReader;
  }

  /**
   * Checks if the `#latestReader` satisfies the specified `minVersion`,
   * and creates a new one if not, releasing the cache reference to the
   * existing one.
   */
  async #getOrCreateReader(
    lc: LogContext,
    minVersion: LexiVersion,
  ): Promise<ReaderAtVersion> {
    if (this.#latestReader && this.#latestReader.version >= minVersion) {
      lc.debug?.(`Reusing cached reader @${this.#latestReader.version}`);
      return this.#latestReader;
    }
    const {init, cleanup} = sharedSnapshot();
    const reader = new TransactionPool(
      this.#lc,
      Mode.READONLY,
      init,
      cleanup,
      READER_INITIAL_WORKERS,
      READER_MAX_WORKERS,
    ); // TODO: Choose maxWorkers more intelligently / dynamically.
    reader.run(this.#replica).catch(e => lc.error?.(e));

    const snapshotQuery = await reader.processReadTask(queryStateVersion);
    const version = snapshotQuery[0].max ?? '00';
    reader.addLoggingContext('version', version);
    lc.debug?.(`Created reader @${version}`);

    if (this.#latestReader) {
      if (this.#latestReader.version === version) {
        // This can happen if readers are created concurrently.
        // Reuse the previous reader since its workers had a head start in setting up.
        lc.info?.(`Cached reader @${version} is current. Reusing.`);
        reader.setDone();
        return this.#latestReader;
      }
      // Allow it to be cleaned up if no one else is referencing it.
      this.#latestReader.reader.unref();
    }
    this.#latestReader = {reader, version};

    // Remove the reader if it finishes unexpectedly.
    reader
      .done()
      .catch(e => lc.error?.(`Error from reader @${version}.`, e))
      .finally(() => {
        if (this.#latestReader?.reader === reader) {
          lc.info?.(`Removing reader @${version} from cache`);
          this.#latestReader = undefined;
        }
      });

    return this.#latestReader;
  }

  /**
   * Creates a "base" {@link QueryInvalidationUpdate} with a TransactionPool at the current
   * snapshot of the database, querying invalidated hashes since `fromVersion` if specified.
   * The update will not contain the `invalidatedQueries` field, as those are Subscription
   * specific and are left to the caller to supply based on the returned invalidated `hashes`.
   *
   * It is the responsibility of the caller to properly track the references to
   * the {@link TransactionPool} `reader` field by calling {@link TransactionPool.ref ref()}
   * and {@link TransactionPool.unref unref()} so that it is properly cleaned up.
   *
   * @param fromVersion The version after which to query invalidated `hashes`, or unspecified
   *                    to skip hash querying (e.g. for new CVRs).
   * @param minVersion The minimum version expected for the TransactionPool, used for determining if
   *                   the #latestReader is reusable. If `invalidations` are specified, this is the
   *                   version at which they were computed (e.g. from a {@link VersionChange} update).
   *                   If the TransactionPool snapshot is at this exact version, the `invalidation`
   *                   can be used directly instead of querying them from the invalidation index.
   * @param prevSnapshotID The snapshot ID associated with the `fromVersion`, when creating an update
   *                   from a {@link VersionChange} update.
   * @param invalidations Existing invalidation hashes to use if the version of the resulting
   *                    snapshot is equal to `minVersion`.
   * @param changes Existing RowChanges to use if the version of the resulting snapshot is
   *                equal to `minVersion`.
   */
  async #createBaseUpdate(
    lc: LogContext,
    fromVersion: LexiVersion | undefined,
    minVersion: LexiVersion,
    prevSnapshotID?: string,
    invalidations?: Record<string, string>,
    changes?: readonly RowChange[],
  ): Promise<{
    update: Omit<QueryInvalidationUpdate, 'invalidatedQueries'>;
    hashes: Set<string>;
  }> {
    // Note: #getOrCreatePrevReader() first so that it can reuse the cached
    // #latestReader before it is replaced / unref-ed by #getOrCreateReader().
    const prevReader =
      fromVersion && prevSnapshotID
        ? await this.#getOrCreatePrevReader(lc, fromVersion, prevSnapshotID)
        : undefined;
    const {reader, version} = await this.#getOrCreateReader(lc, minVersion);
    const update = {
      version,
      fromVersion: fromVersion ?? null,
      reader,
      prevReader,
      changes,
    };

    if (!fromVersion) {
      // Brand new CVR. Invalidations are irrelevant and need not be looked up.
      return {update, hashes: new Set()};
    }

    if (update.changes && minVersion !== version) {
      lc.debug?.(
        `dropping row changes because tx pool version ${version} !== ${minVersion}`,
      );
      update.changes = undefined;
    }

    if (invalidations && minVersion === version) {
      // Invalidations sent from the Replicator's VersionChange message can be used
      // directly if `minVersion` matches that of the `reader` snapshot.
      lc.debug?.(`using hashes from VersionChange`);
      const hashes = new Set(Object.keys(invalidations));
      return {update, hashes};
    }

    let hashes: Set<string>;
    if (version === fromVersion) {
      hashes = new Set(); // Subscriber is up to date. No hashes to look up.
    } else {
      lc.debug?.(`looking up hashes at ${version} from ${fromVersion}`);
      const rows = await reader.processReadTask(
        tx => tx<{hash: Buffer}[]>`
        SELECT "hash" FROM _zero."InvalidationIndex" WHERE "stateVersion" > ${fromVersion};
      `,
      );
      hashes = new Set(rows.map(row => row.hash.toString('hex')));
    }
    return {update, hashes};
  }
}

type ReaderAtVersion = {
  readonly reader: TransactionPool;
  readonly version: LexiVersion;
};
