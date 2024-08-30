import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import {CustomKeyMap} from 'shared/src/custom-key-map.js';
import {difference} from 'shared/src/set-utils.js';
import {stringify} from 'zero-cache/src/types/bigint-json.js';
import {rowIDHash, RowKey} from 'zero-cache/src/types/row-key.js';
import type {
  ChangeDesiredQueriesBody,
  ChangeDesiredQueriesMessage,
  Downstream,
  InitConnectionMessage,
} from 'zero-protocol';
import type {AST} from 'zql/src/zql/ast/ast.js';
import type {PostgresDB} from '../../types/pg.js';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import {ReplicaVersionReady} from '../replicator/replicator.js';
import {ZERO_VERSION_COLUMN_NAME} from '../replicator/schema/replication-state.js';
import type {ActivityBasedService} from '../service.js';
import {ClientHandler, PokeHandler} from './client-handler.js';
import {CVRStore} from './cvr-store.js';
import {
  CVRConfigDrivenUpdater,
  CVRQueryDrivenUpdater,
  RowUpdate,
  type CVRSnapshot,
} from './cvr.js';
import {PipelineDriver, RowChange} from './pipeline-driver.js';
import {cmpVersions, RowID} from './schema/types.js';

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

type IdleToken = {
  timeoutID?: ReturnType<typeof setTimeout>;
};

const DEFAULT_KEEPALIVE_MS = 30_000;

export class ViewSyncerService implements ViewSyncer, ActivityBasedService {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #db: PostgresDB;
  readonly #pipelines: PipelineDriver;
  readonly #versionChanges: Subscription<ReplicaVersionReady>;
  readonly #keepaliveMs: number;

  // Serialize on this lock for:
  // (1) storage or database-dependent operations
  // (2) updating member variables.
  readonly #lock = new Lock();
  readonly #clients = new Map<string, ClientHandler>();
  #cvr: CVRSnapshot | undefined;

  constructor(
    lc: LogContext,
    clientGroupID: string,
    db: PostgresDB,
    pipelineDriver: PipelineDriver,
    versionChanges: Subscription<ReplicaVersionReady>,
    keepaliveMs = DEFAULT_KEEPALIVE_MS,
  ) {
    this.id = clientGroupID;
    this.#lc = lc
      .withContext('component', 'view-syncer')
      .withContext('serviceID', this.id);
    this.#db = db;
    this.#pipelines = pipelineDriver;
    this.#versionChanges = versionChanges;
    this.#keepaliveMs = keepaliveMs;
  }

  async run(): Promise<void> {
    try {
      await this.#lock.withLock(async () => {
        const cvrStore = new CVRStore(this.#lc, this.#db, this.id);
        this.#cvr = await cvrStore.load();
      });

      this.#lc.info?.('view-syncer started', this.#cvr?.version);

      for await (const _ of this.#versionChanges) {
        await this.#lock.withLock(async () => {
          if (!this.#pipelines.initialized()) {
            this.#pipelines.init();
            this.#hydrateUnchangedQueries();
            await this.#syncQueryPipelineSet();
          } else {
            await this.#advancePipelines();
          }
        });
      }

      this.#cleanup();
    } catch (e) {
      this.#lc.error?.(e);
      this.#cleanup(e);
    } finally {
      this.#lc.info?.('view-syncer stopped');
    }
  }

  // The idleToken is an object associated with an idle timeout function,
  // the latter of which checks the token with identity equality before
  // executing. Setting the #idleToken to a new object or to `null`
  // effectively cancels the previous timeout.
  #idleToken: IdleToken | null = null;

  #startIdleTimer(reason: string) {
    if (this.#idleToken) {
      // Previous timeout is canceled for efficiency
      // (but not necessary for correctness).
      clearTimeout(this.#idleToken?.timeoutID);
      this.#lc.debug?.(`${reason}. resetting idle timer`);
    } else {
      this.#lc.debug?.(`${reason}. starting idle timer`);
    }

    const idleToken: IdleToken = {};
    this.#idleToken = idleToken;

    idleToken.timeoutID = setTimeout(() => {
      // If #idleToken has changed, this timeout is effectively canceled.
      if (this.#idleToken === idleToken) {
        this.#lc.info?.('shutting down after idle timeout');
        this.#versionChanges.cancel(); // Note: #versionChanges.active becomes false.
      }
    }, this.#keepaliveMs);
  }

  /**
   * Guarantees that the ViewSyncer will remain running for at least
   * its configured `keepaliveMs`. This is called when establishing a
   * new connection to ensure that its associated ViewSyncer isn't
   * shutdown before it receives the connection.
   *
   * @return `true` if the ViewSyncer will stay alive, `false` if the
   *         ViewSyncer is shutting down.
   */
  keepalive(): boolean {
    if (!this.#versionChanges.active) {
      return false;
    }
    if (this.#idleToken) {
      // Resets the idle timer for another `keepaliveMs`.
      this.#startIdleTimer('received keepalive');
    }
    return true;
  }

  #deleteClient(clientID: string, client: ClientHandler): Promise<void> {
    return this.#lock.withLock(() => {
      const c = this.#clients.get(clientID);
      if (c === client) {
        this.#clients.delete(clientID);

        if (this.#clients.size === 0) {
          this.#startIdleTimer('no more clients');
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
    const downstream = Subscription.create<Downstream>({
      cleanup: (_, err) => {
        err
          ? lc.error?.(`client closed with error`, err)
          : lc.info?.('client closed');
        void this.#deleteClient(clientID, client);
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

      // Clear and cancel any idle timeout.
      if (this.#idleToken) {
        clearTimeout(this.#idleToken.timeoutID);
        this.#idleToken = null;
      }
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
    assert(this.#cvr, 'CVR must be loaded before patching queries');

    // Apply patches requested in the initConnectionMessage.
    const {clientID} = client;
    const cvrStore = new CVRStore(this.#lc, this.#db, this.#cvr.id);
    const updater = new CVRConfigDrivenUpdater(cvrStore, this.#cvr);

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

    this.#cvr = await updater.flush(lc);

    if (this.#pipelines.initialized()) {
      await this.#syncQueryPipelineSet();
    }
  }

  /**
   * Adds and hydrates pipelines for queries whose results are already
   * recorded in the CVR. Namely:
   *
   * 1. The CVR state version and database version are the same.
   * 2. The transformation hash of the queries equal those in the CVR.
   *
   * Note that by definition, only "got" queries can satisfy condition (2),
   * as desired queries do not have a transformation hash.
   *
   * This is an initialization step that sets up pipeline state without
   * the expensive of loading and diffing CVR row state.
   *
   * This must be called from within the #lock.
   */
  #hydrateUnchangedQueries() {
    assert(this.#cvr, 'service must be running');
    assert(this.#pipelines.initialized());

    const dbVersion = this.#pipelines.currentVersion();
    const cvrVersion = this.#cvr.version;

    if (cvrVersion.stateVersion !== dbVersion) {
      this.#lc.info?.(`CVR (${cvrVersion}) is behind db ${dbVersion}`);
      return; // hydration needs to be run with the CVR updater.
    }

    const gotQueries = Object.entries(this.#cvr.queries).filter(
      ([_, state]) => state.transformationHash !== undefined,
    );

    for (const [hash, query] of gotQueries) {
      const {ast, transformationHash} = query;
      if (!query.internal && Object.keys(query.desiredBy).length === 0) {
        continue; // No longer desired.
      }
      const newTransformationHash = hash; // Currently, no transformations are done.
      if (newTransformationHash !== transformationHash) {
        continue; // Query results have changed.
      }
      const start = Date.now();
      let count = 0;
      for (const _ of this.#pipelines.addQuery(hash, ast)) {
        count++;
      }
      const elapsed = Date.now() - start;
      this.#lc.debug?.(`hydrated ${count} rows for ${hash} (${elapsed} ms)`);
    }
  }

  /**
   * Adds and/or removes queries to/from the PipelineDriver to bring it
   * in sync with the set of queries in the CVR (both got and desired).
   * If queries are added, removed, or queried due to a new state version,
   * a new CVR version is created and pokes sent to connected clients.
   */
  async #syncQueryPipelineSet() {
    assert(this.#cvr, 'service must be running');
    assert(this.#pipelines.initialized());
    const cvr = this.#cvr;

    const hydratedQueries = this.#pipelines.addedQueries();
    const allClientQueries = new Set(Object.keys(cvr.queries));
    const desiredClientQueries = new Set(
      Object.keys(cvr.queries).filter(id => {
        const q = cvr.queries[id];
        return q.internal || Object.keys(q.desiredBy).length > 0;
      }),
    );

    const addQueries = [...difference(desiredClientQueries, hydratedQueries)];
    const removeQueries = [
      ...difference(allClientQueries, desiredClientQueries),
    ];
    if (addQueries.length > 0 || removeQueries.length > 0) {
      await this.#addAndRemoveQueries(cvr, addQueries, removeQueries);
    }
  }

  async #addAndRemoveQueries(
    cvr: CVRSnapshot,
    addQueries: string[],
    removeQueries: string[],
  ) {
    assert(addQueries.length > 0 || removeQueries.length > 0);
    const start = Date.now();

    const stateVersion = this.#pipelines.currentVersion();
    const lc = this.#lc.withContext('newVersion', stateVersion);
    lc.info?.(`hydrating ${addQueries.length} queries`);

    const updater = new CVRQueryDrivenUpdater(
      new CVRStore(this.#lc, this.#db, cvr.id),
      cvr,
      stateVersion,
    );
    const minClientVersion = [...this.#clients.values()]
      .map(c => c.version())
      .reduce((a, b) => (cmpVersions(a, b) < 0 ? a : b), {stateVersion});

    // Note: This kicks of background PG queries for CVR data associated with the
    // executed and removed queries.
    const cvrVersion = updater.trackQueries(
      lc,
      addQueries.map(hash => ({id: hash, transformationHash: hash})),
      removeQueries,
      minClientVersion,
    );
    const pokers = [...this.#clients.values()].map(c =>
      c.startPoke(cvrVersion),
    );

    // Removing queries is easy. The pipelines are dropped, and the CVR
    // updater handles the updates and pokes.
    for (const hash of removeQueries) {
      this.#pipelines.removeQuery(hash);
    }

    for (const hash of addQueries) {
      const {ast} = cvr.queries[hash];
      lc.debug?.(`adding pipeline for query ${hash}`, ast);
      await this.#processChanges(
        lc,
        this.#pipelines.addQuery(hash, ast),
        updater,
        pokers,
      );
    }

    lc.debug?.(`generating delete patches`);
    for (const patch of await updater.deleteUnreferencedRows(lc)) {
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

    lc.info?.(`finished processing queries (${Date.now() - start} ms)`);
  }

  async #processChanges(
    lc: LogContext,
    changes: Iterable<RowChange>,
    updater: CVRQueryDrivenUpdater,
    pokers: PokeHandler[],
  ) {
    const start = Date.now();
    const rows = new CustomKeyMap<RowID, RowUpdate>(rowIDHash);
    let total = 0;

    // eslint-disable-next-line require-await
    const processBatch = async () => {
      const elapsed = Date.now() - start;
      total += rows.size;
      lc.debug?.(`processing ${rows.size} (of ${total}) rows (${elapsed} ms)`);
      const patches = await updater.received(this.#lc, rows);
      patches.forEach(patch => pokers.forEach(poker => poker.addPatch(patch)));
      rows.clear();
    };

    for (const change of changes) {
      const {queryHash, table, rowKey, row} = change;
      const rowID: RowID = {schema: '', table, rowKey: rowKey as RowKey};

      let parsedRow = rows.get(rowID);
      if (!parsedRow) {
        parsedRow = {refCounts: {}};
        rows.set(rowID, parsedRow);
      }
      parsedRow.refCounts[queryHash] ??= 0;
      parsedRow.refCounts[queryHash] += row ? 1 : -1;

      if (row && !parsedRow.version) {
        const {[ZERO_VERSION_COLUMN_NAME]: version, ...contents} = row;
        if (typeof version !== 'string' || version.length === 0) {
          throw new Error(`Invalid _0_version in ${stringify(row)}`);
        }
        parsedRow.version = version;
        parsedRow.contents = contents;
      }

      if (rows.size % CURSOR_PAGE_SIZE === 0) {
        await processBatch();
      }
    }
    if (rows.size) {
      await processBatch();
    }
  }

  /**
   * Advance to the current snapshot of the replica and apply / send
   * changes.
   *
   * Must be called from within the #lock.
   */
  async #advancePipelines() {
    assert(this.#cvr, 'service must be running');
    assert(this.#pipelines.initialized());
    const start = Date.now();

    const {version, numChanges, changes} = this.#pipelines.advance();
    const lc = this.#lc.withContext('newVersion', version);
    const cvr = this.#cvr;

    // Probably need a new updater type. CVRAdvancementUpdater?
    const updater = new CVRQueryDrivenUpdater(
      new CVRStore(this.#lc, this.#db, cvr.id),
      cvr,
      version,
    );
    const pokers = [...this.#clients.values()].map(c =>
      c.startPoke(updater.updatedVersion()),
    );

    lc.debug?.(`applying ${numChanges} to advance to ${version}`);
    await this.#processChanges(lc, changes, updater, pokers);

    // Commit the changes and update the CVR snapshot.
    this.#cvr = await updater.flush(lc);

    // Signal clients to commit.
    pokers.forEach(poker => poker.end());

    lc.info?.(`finished processing advancement (${Date.now() - start} ms)`);
  }

  // eslint-disable-next-line require-await
  async stop(): Promise<void> {
    this.#lc.info?.('stopping view syncer');
    this.#versionChanges.cancel();
  }

  #cleanup(err?: unknown) {
    this.#pipelines.destroy();
    for (const client of this.#clients.values()) {
      if (err) {
        client.fail(err);
      } else {
        client.close();
      }
    }
  }
}

// TODO: Increase this once performance issues are resolved.
const CURSOR_PAGE_SIZE = 10;
