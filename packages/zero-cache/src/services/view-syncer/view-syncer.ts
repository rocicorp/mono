import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {assert} from 'shared/src/asserts.js';
import type {
  ChangeDesiredQueriesBody,
  ChangeDesiredQueriesMessage,
  Downstream,
  InitConnectionMessage,
} from 'zero-protocol';
import type {AST} from 'zql/src/zql/ast/ast.js';
import type {DurableStorage} from '../../storage/durable-storage.js';
import {initStorageSchema} from '../../storage/schema.js';
import type {JSONObject} from '../../types/bigint-json.js';
import type {PostgresDB} from '../../types/pg.js';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import type {QueryInvalidationUpdate} from '../invalidation-watcher/invalidation-watcher.js';
import type {InvalidationWatcherRegistry} from '../invalidation-watcher/registry.js';
import type {Service} from '../service.js';
import {ClientHandler} from './client-handler.js';
import {
  CVRConfigDrivenUpdater,
  CVRQueryDrivenUpdater,
  loadCVR,
  type CVRSnapshot,
} from './cvr.js';
import {QueryHandler, TransformedQuery} from './queries.js';
import {SCHEMA_MIGRATIONS} from './schema/migrations.js';
import {schemaRoot} from './schema/paths.js';
import {cmpVersions} from './schema/types.js';

export type SyncContext = {
  readonly clientID: string;
  readonly wsID: string;
  readonly baseCookie: string | null;
};

export interface ViewSyncer {
  initConnection(
    ctx: SyncContext,
    msg: InitConnectionMessage,
  ): Promise<CancelableAsyncIterable<Downstream>>;

  changeDesiredQueries(
    ctx: SyncContext,
    msg: ChangeDesiredQueriesMessage,
  ): Promise<void>;
}

export class ViewSyncerService implements ViewSyncer, Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #storage: DurableStorage;
  readonly #registry: InvalidationWatcherRegistry;
  readonly #queryChecker: PostgresDB;

  // Serialize on this lock for:
  // (1) storage or database-dependent operations
  // (2) updating member variables.
  readonly #lock = new Lock();
  readonly #clients = new Map<string, ClientHandler>();
  #cvr: CVRSnapshot | undefined;

  #started = false;
  #stopped = false;
  readonly #shouldRun = resolver<false>();
  #hasSyncRequests = resolver<true>();

  #invalidationSubscription:
    | CancelableAsyncIterable<QueryInvalidationUpdate>
    | undefined;

  constructor(
    lc: LogContext,
    clientGroupID: string,
    storage: DurableStorage,
    registry: InvalidationWatcherRegistry,
    queryChecker: PostgresDB,
  ) {
    this.id = clientGroupID;
    this.#lc = lc
      .withContext('component', 'view-syncer')
      .withContext('serviceID', this.id);
    this.#storage = storage;
    this.#registry = registry;
    this.#queryChecker = queryChecker;
  }

  async run(): Promise<void> {
    assert(!this.#started, `ViewSyncer ${this.id} has already been started`);
    this.#started = true;

    this.#lc.info?.('starting view-syncer');
    try {
      await this.#lock.withLock(async () => {
        await initStorageSchema(
          this.#lc,
          'view-syncer',
          this.#storage,
          schemaRoot,
          SCHEMA_MIGRATIONS,
        );

        this.#cvr = await loadCVR(this.#lc, this.#storage, this.id);
      });

      this.#lc.info?.('started view-syncer');

      while (
        await Promise.race([
          this.#shouldRun.promise, // resolves to false on stop()
          this.#hasSyncRequests.promise, // resolves to true on a sync request
          // TODO: Figure out idle shutdown + incoming sync() race condition.
          //       Maybe it's the ServiceRunner that needs to track this.
          // sleep(IDLE_TIMEOUT_MS),
        ])
      ) {
        const watching = await this.#lock.withLock(() =>
          this.#watchInvalidations(),
        );
        if (!watching || this.#stopped) {
          // Cancel the subscription that started concurrently with stop().
          await this.stop();
          break;
        }
        const {subscription, handleInvalidations} = watching;
        for await (const update of subscription) {
          await this.#lock.withLock(async () => {
            if (!this.#invalidationSubscription) {
              return; // Subscription was canceled. Update must be dropped.
            }
            await handleInvalidations(update);
          });
        }

        this.#invalidationSubscription = undefined;
        this.#lc.info?.(`waiting for syncers`);
      }
      await this.stop();
    } catch (e) {
      this.#lc.error?.(e);
      await this.stop(e);
    } finally {
      this.#lc.info?.('stopped');
    }
  }

  #deleteClient(
    lc: LogContext,
    clientID: string,
    client: ClientHandler,
  ): Promise<void> {
    return this.#lock.withLock(() => {
      const c = this.#clients.get(clientID);
      if (c === client) {
        this.#clients.delete(clientID);

        if (this.#clients.size === 0) {
          lc.info?.('no more clients. closing invalidation subscription');
          this.#invalidationSubscription?.cancel();
          this.#invalidationSubscription = undefined;
          this.#hasSyncRequests = resolver<true>();
        }
      }
    });
  }

  async initConnection(
    ctx: SyncContext,
    initConnectionMessage: InitConnectionMessage,
  ): Promise<CancelableAsyncIterable<Downstream>> {
    const {clientID, wsID, baseCookie} = ctx;
    const lc = this.#lc
      .withContext('clientID', clientID)
      .withContext('wsID', wsID);
    lc.debug?.('initConnection', initConnectionMessage);

    // Setup the downstream connection.
    const downstream = new Subscription<Downstream>({
      cleanup: (_, err) => {
        err
          ? lc.error?.(`client closed with error`, err)
          : lc.info?.('client closed');
        void this.#deleteClient(lc, clientID, client);
      },
    });

    const client = new ClientHandler(
      lc,
      this.id,
      clientID,
      wsID,
      baseCookie,
      downstream,
    );

    // Note: It is tempting to try to re-use #runInLockForClient(), but for
    // the initConnection case the client is not yet in the #clients map, and
    // it must be added from within the lock, and only after the desired
    // queries have been processed.
    await this.#lock.withLock(async () => {
      await this.#patchQueries(lc, client, initConnectionMessage[1]);

      // Update #clients, close any previous connection.
      this.#clients.get(clientID)?.close();
      this.#clients.set(clientID, client);

      this.#hasSyncRequests.resolve(true);
    });
    return downstream;
  }

  async changeDesiredQueries(
    ctx: SyncContext,
    msg: ChangeDesiredQueriesMessage,
  ): Promise<void> {
    await this.#runInLockForClient<ChangeDesiredQueriesBody>(
      ctx,
      msg,
      (lc, client, body) => this.#patchQueries(lc, client, body),
    );
  }

  async #runInLockForClient<B, M extends [cmd: string, B] = [string, B]>(
    ctx: SyncContext,
    msg: M,
    fn: (lc: LogContext, client: ClientHandler, body: B) => Promise<void>,
  ): Promise<void> {
    const {clientID, wsID} = ctx;
    const lc = this.#lc
      .withContext('clientID', clientID)
      .withContext('wsID', wsID);

    const [cmd, body] = msg;
    lc.debug?.(cmd, body);

    const client = this.#clients.get(clientID);
    if (client?.wsID !== wsID) {
      // Only respond to messages of the currently connected client.
      // Past connections may have been dropped due to an error, so consider them invalid.
      lc.info?.(`client no longer connected. dropping ${cmd} message`);
      return;
    }

    try {
      await this.#lock.withLock(() => fn(lc, client, body));
    } catch (e) {
      lc.error?.(`closing connection with error`, e);
      client.fail(e);
      throw e;
    }
  }

  // Must be called from within #lock.
  async #patchQueries(
    lc: LogContext,
    client: ClientHandler,
    {desiredQueriesPatch}: ChangeDesiredQueriesBody,
  ) {
    assert(this.#started);
    assert(this.#cvr);

    // Apply patches requested in the initConnectionMessage.
    const {clientID} = client;
    const updater = new CVRConfigDrivenUpdater(this.#storage, this.#cvr);
    const added: {id: string; ast: AST}[] = [];
    for (const patch of desiredQueriesPatch) {
      switch (patch.op) {
        case 'put':
          added.push(
            ...updater.putDesiredQueries(clientID, {[patch.hash]: patch.ast}),
          );
          break;
        case 'del':
          updater.deleteDesiredQueries(clientID, [patch.hash]);
          break;
        case 'clear':
          updater.clearDesiredQueries(clientID);
          break;
      }
    }
    await this.#checkQueries(added);

    this.#cvr = await updater.flush(lc);

    // The client will not be up to date if it is old, or if the set of queries
    // changed. Cancel any current subscription to start a new one that takes the
    // changed queries and/or client's baseCookie into account.
    if (cmpVersions(client.version(), this.#cvr.version) < 0) {
      this.#invalidationSubscription?.cancel();
      this.#invalidationSubscription = undefined;
    }
  }

  // Must be called in lock.
  async #watchInvalidations() {
    assert(this.#started);
    assert(this.#cvr);

    if (this.#clients.size === 0) {
      // A client may have immediately disconnected before the watch could be started.
      return null;
    }
    const minVersion = [...this.#clients.values()]
      .map(c => c.version())
      .reduce((a, b) => (cmpVersions(a, b) < 0 ? a : b));

    this.#lc.info?.(
      `subscribing to invalidations ${minVersion ? 'since ' + minVersion : ''}`,
    );

    const watcher = await this.#registry.getInvalidationWatcher();
    const tableSchemas = await watcher.getTableSchemas();
    const queryHandler = new QueryHandler(tableSchemas);

    // Exclude queries that are no longer desired.
    // These will be removed on the first invalidation.
    const queries = Object.values(this.#cvr.queries).filter(
      q => q.internal || Object.keys(q.desiredBy).length > 0,
    );
    const transformed = queryHandler.transform(queries);
    this.#invalidationSubscription = await watcher.watch({
      fromVersion: minVersion?.stateVersion,
      queries: Object.fromEntries(
        [...transformed].map(([id, t]) => [id, t.invalidationInfo]),
      ),
    });
    return {
      subscription: this.#invalidationSubscription,
      handleInvalidations: (update: QueryInvalidationUpdate) =>
        this.#handleInvalidations(update, queryHandler, transformed),
    };
  }

  // Must be called in lock.
  async #handleInvalidations(
    invalidation: QueryInvalidationUpdate,
    queryHandler: QueryHandler,
    transformedQueries: Map<string, TransformedQuery>,
  ) {
    assert(this.#started);
    assert(this.#cvr);
    const cvr = this.#cvr;

    const {fromVersion, version, invalidatedQueries, reader} = invalidation;
    const lc = this.#lc
      .withContext('fromVersion', fromVersion)
      .withContext('newVersion', version);

    const minCVRVersion = [...this.#clients.values()]
      .map(c => c.version())
      .reduce((a, b) => (cmpVersions(a, b) < 0 ? a : b));

    const queriesToExecute = [...transformedQueries.values()].filter(q => {
      if (invalidatedQueries.has(q.transformationHash)) {
        return true; // Invalidated.
      }
      for (const id of q.queryIDs) {
        const queryRecord = cvr.queries[id];
        if (queryRecord.transformationHash !== q.transformationHash) {
          return true; // Transformation hash changed or doesn't exist (for first time queries).
        }
        if (
          cmpVersions(
            minCVRVersion,
            queryRecord.transformationVersion ?? null,
          ) < 0
        ) {
          return true; // Transformed since fromVersion.
        }
      }
      return false;
    });

    lc.info?.(`Executing ${queriesToExecute.length} queries`);

    const updater = new CVRQueryDrivenUpdater(this.#storage, cvr, version);
    // Track which queries are being executed and removed.
    const cvrVersion = updater.trackQueries(
      lc,
      queriesToExecute.flatMap(q =>
        q.queryIDs.map(id => ({id, transformationHash: q.transformationHash})),
      ),
      Object.values(cvr.queries)
        .filter(q => !q.internal && Object.keys(q.desiredBy).length === 0)
        .map(q => q.id),
      minCVRVersion,
    );

    const pokers = [...this.#clients.values()].map(c =>
      c.startPoke(cvrVersion),
    );

    // Kick off queries in parallel. The reader pool will limit concurrent queries,
    // and the cursor page size limits the amount of row content in memory.
    const cursorPageSize = 1000; // TODO: something less arbitrary.
    const queriesDone = queriesToExecute.map(q => {
      const {queryIDs, columnAliases} = q;
      const resultParser = queryHandler.resultParser(
        cvr.id,
        queryIDs,
        columnAliases,
      );

      return reader.processReadTask(async tx => {
        const {query, values} = q.transformedAST.query();
        const start = Date.now();
        let totalResults = 0;
        let totalRows = 0;
        lc.debug?.(`executing [${queryIDs}]: ${query}`);

        async function processBatch(results: readonly JSONObject[]) {
          const elapsed = Date.now() - start;
          const rows = resultParser.parseResults(results);
          totalResults += results.length;
          totalRows += rows.size;
          lc.debug?.(
            `processing ${rows.size}/${results.length} row/result(s) (of ${totalRows}/${totalResults}) for [${queryIDs}] (${elapsed} ms)`,
          );
          const patches = await updater.received(lc, rows);
          patches.forEach(patch =>
            pokers.forEach(poker => poker.addPatch(patch)),
          );
        }

        // Pipeline the processing of each batch with Postgres' computation of the next batch.
        let lastBatch = Promise.resolve();
        for await (const results of tx
          .unsafe(query, values)
          .cursor(cursorPageSize)) {
          await lastBatch;
          lastBatch = processBatch(results);
        }
        await lastBatch;
      });
    });

    await Promise.all(queriesDone);

    lc.debug?.(`generating delete / constrain patches`);
    for (const patch of await updater.deleteUnreferencedColumnsAndRows(lc)) {
      pokers.forEach(poker => poker.addPatch(patch));
    }
    lc.debug?.(`generating config patches`);
    for (const patch of await updater.generateConfigPatches(lc)) {
      pokers.forEach(poker => poker.addPatch(patch));
    }

    // Commit the changes and update the CVR snapshot.
    this.#cvr = await updater.flush(lc);

    // Signal clients to commit.
    pokers.forEach(poker => poker.end());

    lc.info?.(`finished processing update`);
  }

  // eslint-disable-next-line require-await
  async stop(err?: unknown): Promise<void> {
    this.#stopped = true;
    this.#shouldRun.resolve(false);
    this.#invalidationSubscription?.cancel();
    this.#invalidationSubscription = undefined;

    for (const client of this.#clients.values()) {
      if (err) {
        client.fail(err);
      } else {
        client.close();
      }
    }
  }

  async #checkQueries(queries: {id: string; ast: AST}[]): Promise<void> {
    if (queries.length === 0) {
      return;
    }
    const watcher = await this.#registry.getInvalidationWatcher();
    const tableSchemas = await watcher.getTableSchemas();

    const queryHandler = new QueryHandler(tableSchemas);
    const transformed = queryHandler.transform(queries);

    await Promise.all(
      [...transformed].map(
        async ([
          id,
          {
            transformedAST: ast,
            invalidationInfo: {filters},
          },
        ]) => {
          const parameterized = ast.query();
          if (isAlreadyChecked(parameterized.query)) {
            this.#lc.debug?.(`query [${id}] has been checked`);
            return;
          }
          const start = Date.now();
          this.#lc.debug?.(`checking query [${id}]`, parameterized.query);

          const checked = this.#queryChecker
            .unsafe(parameterized.query, parameterized.values)
            .describe();

          for (const filter of filters) {
            const t = queryHandler.tableSpec(filter.schema, filter.table);
            if (!t) {
              throw new Error(`Invalid table ${filter.schema}.${filter.table}`);
            }
            for (const col of Object.keys(filter.filteredColumns)) {
              if (!(col in t.columns)) {
                throw new Error(
                  `Column ${col} does not exist in ${filter.table}`,
                );
              }
            }
            for (const col of filter.selectedColumns ?? []) {
              if (!(col in t.columns)) {
                throw new Error(
                  `Column ${col} does not exist in ${filter.table}`,
                );
              }
            }
          }

          await checked;
          this.#lc.debug?.(`checked [${id}] (${Date.now() - start} ms)`);
          cacheCheckedQuery(parameterized.query);
        },
      ),
    );
  }
}

const MAX_CHECKED_QUERY_CACHE_SIZE = 100;
const checkedQueryCache = new Set<string>();

function isAlreadyChecked(query: string) {
  return checkedQueryCache.has(query);
}

function cacheCheckedQuery(query: string) {
  if (!checkedQueryCache.has(query)) {
    if (checkedQueryCache.size >= MAX_CHECKED_QUERY_CACHE_SIZE) {
      checkedQueryCache.delete(checkedQueryCache.values().next().value);
    }
    checkedQueryCache.add(query);
  }
}
