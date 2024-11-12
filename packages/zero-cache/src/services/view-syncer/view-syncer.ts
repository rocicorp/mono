import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {Row} from 'postgres';
import {assert, unreachable} from '../../../../shared/src/asserts.js';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.js';
import {must} from '../../../../shared/src/must.js';
import {difference} from '../../../../shared/src/set-utils.js';
import type {AST} from '../../../../zero-protocol/src/ast.js';
import {
  ErrorKind,
  type ChangeDesiredQueriesBody,
  type ChangeDesiredQueriesMessage,
  type Downstream,
  type InitConnectionMessage,
} from '../../../../zero-protocol/src/mod.js';
import {stringify} from '../../types/bigint-json.js';
import {ErrorForClient} from '../../types/error-for-client.js';
import type {PostgresDB} from '../../types/pg.js';
import {rowIDHash, type RowKey} from '../../types/row-key.js';
import type {Source} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import type {ReplicaState} from '../replicator/replicator.js';
import {ZERO_VERSION_COLUMN_NAME} from '../replicator/schema/replication-state.js';
import type {ActivityBasedService} from '../service.js';
import {
  ClientHandler,
  type PokeHandler,
  type RowPatch,
} from './client-handler.js';
import {CVRStore} from './cvr-store.js';
import {
  CVRConfigDrivenUpdater,
  CVRQueryDrivenUpdater,
  type CVRSnapshot,
  type RowUpdate,
} from './cvr.js';
import type {DrainCoordinator} from './drain-coordinator.js';
import {PipelineDriver, type RowChange} from './pipeline-driver.js';
import {
  cmpVersions,
  versionFromString,
  versionString,
  versionToCookie,
  type RowID,
} from './schema/types.js';
import {SchemaChangeError} from './snapshotter.js';

export type SyncContext = {
  readonly clientID: string;
  readonly wsID: string;
  readonly baseCookie: string | null;
  readonly schemaVersion: number;
};

export interface ViewSyncer {
  initConnection(
    ctx: SyncContext,
    msg: InitConnectionMessage,
  ): Promise<Source<Downstream>>;

  changeDesiredQueries(
    ctx: SyncContext,
    msg: ChangeDesiredQueriesMessage,
  ): Promise<void>;
}

type ShutdownToken = {
  timeoutID?: ReturnType<typeof setTimeout>;
};

type ShutdownTimerReason = 'keepalive' | 'no more clients';

const DEFAULT_KEEPALIVE_MS = 5_000;
// TODO: make idle timeout more intelligent when browser-level client
//       management can provide signals of whether a client is likely to
//       reconnect.
const DEFAULT_IDLE_TIMEOUT_MS = 0;

export class ViewSyncerService implements ViewSyncer, ActivityBasedService {
  readonly id: string;
  readonly #shardID: string;
  readonly #lc: LogContext;
  readonly #pipelines: PipelineDriver;
  readonly #stateChanges: Subscription<ReplicaState>;
  readonly #drainCoordinator: DrainCoordinator;
  readonly #keepaliveMs: number;
  readonly #idleTimeoutMs: number;

  // Serialize on this lock for:
  // (1) storage or database-dependent operations
  // (2) updating member variables.
  // (3) initializing a new client, to ensure that it only gets pokes after
  //     we have processed its initConnectionMessage.
  //
  // Note that it is okay to remove/delete clients without acquiring the lock.
  readonly #lock = new Lock();
  readonly #clients = new Map<string, ClientHandler>();
  readonly #cvrStore: CVRStore;
  readonly #stopped = resolver();

  #cvr: CVRSnapshot | undefined;
  #pipelinesSynced = false;

  constructor(
    lc: LogContext,
    clientGroupID: string,
    shardID: string,
    db: PostgresDB,
    pipelineDriver: PipelineDriver,
    versionChanges: Subscription<ReplicaState>,
    drainCoordinator: DrainCoordinator,
    keepaliveMs = DEFAULT_KEEPALIVE_MS,
    idleTimeoutMs = DEFAULT_IDLE_TIMEOUT_MS,
  ) {
    this.id = clientGroupID;
    this.#shardID = shardID;
    this.#lc = lc
      .withContext('component', 'view-syncer')
      .withContext('serviceID', this.id);
    this.#pipelines = pipelineDriver;
    this.#stateChanges = versionChanges;
    this.#drainCoordinator = drainCoordinator;
    this.#keepaliveMs = keepaliveMs;
    this.#idleTimeoutMs = idleTimeoutMs;
    this.#cvrStore = new CVRStore(lc, db, clientGroupID);
  }

  #runInLockWithCVR<T>(fn: (cvr: CVRSnapshot) => Promise<T> | T): Promise<T> {
    return this.#lock.withLock(async () => {
      if (!this.#cvr) {
        this.#cvr = await this.#cvrStore.load();
      }
      try {
        return await fn(this.#cvr);
      } catch (e) {
        // Clear cached state if an error is encountered.
        this.#cvr = undefined;
        throw e;
      }
    });
  }

  async run(): Promise<void> {
    try {
      for await (const {state} of this.#stateChanges) {
        if (this.#drainCoordinator.shouldDrain()) {
          this.#lc.debug?.(`draining view-syncer ${this.id} (elective)`);
          break;
        }
        assert(state === 'version-ready'); // This is the only state change used.
        if (!this.#pipelines.initialized()) {
          // On the first version-ready signal, connect to the replica.
          this.#pipelines.init();
        }
        await this.#runInLockWithCVR(async cvr => {
          if (
            (cvr.replicaVersion !== null ||
              cvr.version.stateVersion !== '00') &&
            cvr.replicaVersion !== this.#pipelines.replicaVersion
          ) {
            const msg = `Replica Version mismatch: CVR=${
              cvr.replicaVersion
            }, DB=${this.#pipelines.replicaVersion}`;
            this.#lc.info?.(`resetting CVR: ${msg}`);
            throw new ErrorForClient(['error', ErrorKind.ClientNotFound, msg]);
          }

          if (this.#pipelinesSynced) {
            const result = await this.#advancePipelines(cvr);
            if (result === 'success') {
              return;
            }
            this.#lc.info?.(`resetting for schema change: ${result.message}`);
            this.#pipelines.reset();
          }

          // Advance the snapshot to the current version.
          const version = this.#pipelines.advanceWithoutDiff();
          const cvrVer = versionString(cvr.version);

          if (version < cvr.version.stateVersion) {
            this.#lc.debug?.(`replica@${version} is behind cvr@${cvrVer}`);
            return; // Wait for the next advancement.
          }

          // stateVersion is at or beyond CVR version for the first time.
          this.#lc.info?.(`init pipelines@${version} (cvr@${cvrVer})`);
          this.#hydrateUnchangedQueries(cvr);
          await this.#syncQueryPipelineSet(cvr);
          this.#pipelinesSynced = true;
        });
      }

      // If this view-syncer exited due to an elective or forced drain,
      // set the next drain timeout.
      if (this.#drainCoordinator.shouldDrain()) {
        this.#drainCoordinator.drainNextIn(this.#totalHydrationTimeMs());
      }
      this.#cleanup();
    } catch (e) {
      this.#lc.error?.(e);
      this.#cleanup(e);
    } finally {
      this.#lc.info?.('view-syncer stopped');
      this.#stopped.resolve();
    }
  }

  #totalHydrationTimeMs(): number {
    return this.#pipelines.totalHydrationTimeMs();
  }

  // The shutdownToken is an object associated with an shutdown timeout function,
  // the latter of which checks the token with identity equality before
  // executing. Setting the #shutdownToken to a new object or to `null`
  // effectively cancels the previous timeout.
  #shutdownToken: ShutdownToken | null = null;

  #startShutdownTimer(reason: ShutdownTimerReason) {
    if (this.#shutdownToken) {
      // Previous timeout is canceled for efficiency
      // (but not necessary for correctness).
      clearTimeout(this.#shutdownToken?.timeoutID);
      this.#lc.debug?.(`${reason}. resetting idle timer`);
    } else {
      this.#lc.debug?.(`${reason}. starting idle timer`);
    }

    const shutdownToken: ShutdownToken = {};
    this.#shutdownToken = shutdownToken;

    shutdownToken.timeoutID = setTimeout(
      () => {
        // If #idleToken has changed, this timeout is effectively canceled.
        if (this.#shutdownToken === shutdownToken) {
          this.#lc.info?.('shutting down after idle timeout');
          this.#stateChanges.cancel(); // Note: #versionChanges.active becomes false.
        }
      },
      reason === 'keepalive' ? this.#keepaliveMs : this.#idleTimeoutMs,
    );
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
    if (!this.#stateChanges.active) {
      return false;
    }
    if (this.#shutdownToken) {
      // Resets the idle timer for another `#keepaliveMs`.
      this.#startShutdownTimer('keepalive');
    }
    return true;
  }

  #deleteClient(clientID: string, client: ClientHandler) {
    // Note: It is okay to delete / cleanup clients without acquiring the lock.
    // In fact, it is important to do so in order to guarantee that idle cleanup
    // is performed in a timely manner, regardless of the amount of work
    // queued on the lock.
    const c = this.#clients.get(clientID);
    if (c === client) {
      this.#clients.delete(clientID);

      if (this.#clients.size === 0) {
        this.#startShutdownTimer('no more clients');
      }
    }
  }

  async initConnection(
    ctx: SyncContext,
    initConnectionMessage: InitConnectionMessage,
  ): Promise<Source<Downstream>> {
    const {clientID, wsID, baseCookie, schemaVersion} = ctx;
    const lc = this.#lc
      .withContext('clientID', clientID)
      .withContext('wsID', wsID);

    // Setup the downstream connection.
    const downstream = Subscription.create<Downstream>({
      cleanup: (_, err) => {
        err
          ? lc.error?.(`client closed with error`, err)
          : lc.info?.('client closed');
        this.#deleteClient(clientID, newClient);
      },
    });

    const newClient = new ClientHandler(
      lc,
      this.id,
      clientID,
      wsID,
      this.#shardID,
      baseCookie,
      schemaVersion,
      downstream,
    );

    await this.#runInLockForClient(
      ctx,
      initConnectionMessage,
      this.#patchQueries,
      newClient,
    );

    return downstream;
  }

  async changeDesiredQueries(
    ctx: SyncContext,
    msg: ChangeDesiredQueriesMessage,
  ): Promise<void> {
    await this.#runInLockForClient(ctx, msg, this.#patchQueries);
  }

  /**
   * Runs the given `fn` to process the `msg` from within the `#lock`,
   * optionally adding the `newClient` if supplied.
   */
  async #runInLockForClient<B, M extends [cmd: string, B] = [string, B]>(
    ctx: SyncContext,
    msg: M,
    fn: (
      lc: LogContext,
      clientID: string,
      body: B,
      cvr: CVRSnapshot,
    ) => Promise<void>,
    newClient?: ClientHandler,
  ): Promise<void> {
    const {clientID, wsID} = ctx;
    const [cmd, body] = msg;
    const lc = this.#lc
      .withContext('clientID', clientID)
      .withContext('wsID', wsID)
      .withContext('cmd', cmd);

    // Clear and cancel any shutdown timeout.
    if (this.#shutdownToken) {
      clearTimeout(this.#shutdownToken.timeoutID);
      this.#shutdownToken = null;
    }

    let client: ClientHandler | undefined;
    try {
      await this.#runInLockWithCVR(cvr => {
        lc.debug?.(cmd, body);

        if (newClient) {
          assert(newClient.wsID === wsID);
          this.#clients.get(clientID)?.close();
          this.#clients.set(clientID, newClient);
          client = newClient;
        } else {
          client = this.#clients.get(clientID);
          if (client?.wsID !== wsID) {
            // Only respond to messages of the currently connected client.
            // Connections may have been drained or dropped due to an error.
            lc.debug?.(`client no longer connected. dropping ${cmd} message`);
            return;
          }
        }

        return fn(lc, clientID, body, cvr);
      });
    } catch (e) {
      lc.error?.(`closing connection with error`, e);
      if (client) {
        // Ideally, propagate the exception to the client's downstream subscription ...
        client.fail(e);
      } else {
        // unless the exception happened before the client could be looked up.
        throw e;
      }
    }
  }

  // Must be called from within #lock.
  readonly #patchQueries = async (
    lc: LogContext,
    clientID: string,
    {desiredQueriesPatch}: ChangeDesiredQueriesBody,
    cvr: CVRSnapshot,
  ) => {
    // Apply requested patches.
    if (desiredQueriesPatch.length) {
      lc.debug?.(`applying ${desiredQueriesPatch.length} query patches`);
      const updater = new CVRConfigDrivenUpdater(
        this.#cvrStore,
        cvr,
        this.#shardID,
      );

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

      this.#cvr = (await updater.flush(lc)).cvr;
      cvr = this.#cvr; // For #syncQueryPipelineSet().
    }

    if (this.#pipelinesSynced) {
      await this.#syncQueryPipelineSet(cvr);
    }
  };

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
  #hydrateUnchangedQueries(cvr: CVRSnapshot) {
    assert(this.#pipelines.initialized());

    const dbVersion = this.#pipelines.currentVersion();
    const cvrVersion = cvr.version;

    if (cvrVersion.stateVersion !== dbVersion) {
      this.#lc.info?.(
        `CVR (${versionToCookie(cvrVersion)}) is behind db ${dbVersion}`,
      );
      return; // hydration needs to be run with the CVR updater.
    }

    const gotQueries = Object.entries(cvr.queries).filter(
      ([_, state]) => state.transformationHash !== undefined,
    );

    for (const [hash, query] of gotQueries) {
      const {ast, transformationHash} = query;
      if (!query.internal && Object.keys(query.desiredBy).length === 0) {
        continue; // No longer desired.
      }
      const newTransformationHash = hash; // Currently, no transformations are done.
      if (newTransformationHash !== transformationHash) {
        continue; // Query results may have changed.
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
   *
   * This must be called from within the #lock.
   */
  async #syncQueryPipelineSet(cvr: CVRSnapshot) {
    assert(this.#pipelines.initialized());
    const lc = this.#lc.withContext('cvrVersion', versionString(cvr.version));

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
      // Note: clients are caught up as part of #addAndRemoveQueries().
      await this.#addAndRemoveQueries(lc, cvr, addQueries, removeQueries);
    } else {
      await this.#catchupClients(lc, cvr);
    }

    // If CVR was non-empty, then the CVR, database, and all clients
    // should now be at the same version.
    if (allClientQueries.size) {
      const cvrVersion = must(this.#cvr).version;
      const dbVersion = this.#pipelines.currentVersion();
      assert(
        cvrVersion.stateVersion === dbVersion,
        `CVR@${versionString(cvrVersion)}" does not match DB@${dbVersion}`,
      );
    }
  }

  // This must be called from within the #lock.
  async #addAndRemoveQueries(
    lc: LogContext,
    cvr: CVRSnapshot,
    addQueries: string[],
    removeQueries: string[],
  ) {
    assert(addQueries.length > 0 || removeQueries.length > 0);
    const start = Date.now();

    const stateVersion = this.#pipelines.currentVersion();
    lc = lc.withContext('stateVersion', stateVersion);
    lc.info?.(`hydrating ${addQueries.length} queries`);

    const updater = new CVRQueryDrivenUpdater(
      this.#cvrStore,
      cvr,
      stateVersion,
      this.#pipelines.replicaVersion,
    );

    // Note: This kicks of background PG queries for CVR data associated with the
    // executed and removed queries.
    const {newVersion, queryPatches} = updater.trackQueries(
      lc,
      addQueries.map(hash => ({id: hash, transformationHash: hash})),
      removeQueries,
    );
    const pokers = [...this.#clients.values()].map(c =>
      c.startPoke(newVersion, this.#pipelines.currentSchemaVersions()),
    );
    for (const patch of queryPatches) {
      pokers.forEach(poker => poker.addPatch(patch));
    }

    // Removing queries is easy. The pipelines are dropped, and the CVR
    // updater handles the updates and pokes.
    for (const hash of removeQueries) {
      this.#pipelines.removeQuery(hash);
    }

    const pipelines = this.#pipelines;
    function* generateRowChanges() {
      for (const hash of addQueries) {
        const {ast} = cvr.queries[hash];
        lc.debug?.(`adding pipeline for query ${hash}`, ast);
        yield* pipelines.addQuery(hash, ast);
      }
    }
    // #processChanges does batched de-duping of rows. Wrap all pipelines in
    // a single generator in order to maximize de-duping.
    await this.#processChanges(lc, generateRowChanges(), updater, pokers);

    lc.debug?.(`generating delete patches`);
    for (const patch of await updater.deleteUnreferencedRows()) {
      pokers.forEach(poker => poker.addPatch(patch));
    }

    // Commit the changes and update the CVR snapshot.
    this.#cvr = (await updater.flush(lc)).cvr;

    // Before ending the poke, catch up clients that were behind the old CVR.
    await this.#catchupClients(lc, cvr, addQueries, pokers);

    // Signal clients to commit.
    pokers.forEach(poker => poker.end());

    lc.info?.(`finished processing queries (${Date.now() - start} ms)`);
  }

  /**
   * @param cvr The CVR to which clients should be caught up to. This does
   *     not necessarily need to be the current CVR.
   * @param excludeQueryHashes Exclude patches from rows associated with
   *     the specified queries.
   * @param usePokers If specified, sends pokes on existing PokeHandlers,
   *     in which case the caller is responsible for sending the `pokeEnd`
   *     messages. If unspecified, the pokes will be started and ended
   *     using the version from the supplied `cvr`.
   */
  // Must be called within #lock
  async #catchupClients(
    lc: LogContext,
    cvr: CVRSnapshot,
    excludeQueryHashes: string[] = [],
    usePokers?: PokeHandler[],
  ) {
    const pokers =
      usePokers ??
      [...this.#clients.values()].map(c =>
        c.startPoke(cvr.version, this.#pipelines.currentSchemaVersions()),
      );

    const catchupFrom = [...this.#clients.values()]
      .map(c => c.version())
      .reduce((a, b) => (cmpVersions(a, b) < 0 ? a : b), cvr.version);

    const rowPatches = this.#cvrStore.catchupRowPatches(
      lc,
      catchupFrom,
      cvr,
      excludeQueryHashes,
    );
    const configPatches = this.#cvrStore.catchupConfigPatches(
      lc,
      catchupFrom,
      cvr,
    );

    for (const patch of await configPatches) {
      pokers.forEach(poker => poker.addPatch(patch));
    }

    let rowPatchCount = 0;
    for await (const rows of rowPatches) {
      for (const row of rows) {
        const {schema, table} = row;
        const rowKey = row.rowKey as RowKey;
        const toVersion = versionFromString(row.patchVersion);

        const id: RowID = {schema, table, rowKey};
        let patch: RowPatch;
        if (!row.refCounts) {
          patch = {type: 'row', op: 'del', id};
        } else {
          const row = must(
            this.#pipelines.getRow(table, rowKey),
            `Missing row ${table}:${stringify(rowKey)}`,
          );
          const {contents} = contentsAndVersion(row);
          patch = {type: 'row', op: 'put', id, contents};
        }
        const patchToVersion = {patch, toVersion};
        pokers.forEach(poker => poker.addPatch(patchToVersion));
        rowPatchCount++;
      }
    }
    lc.debug?.(`sent ${rowPatchCount} row patches`);

    if (!usePokers) {
      pokers.forEach(poker => poker.end());
    }
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

    const processBatch = async () => {
      const elapsed = Date.now() - start;
      total += rows.size;
      lc.debug?.(`processing ${rows.size} (of ${total}) rows (${elapsed} ms)`);
      const patches = await updater.received(this.#lc, rows);
      patches.forEach(patch => pokers.forEach(poker => poker.addPatch(patch)));
      rows.clear();
    };

    for (const change of changes) {
      const {type, queryHash, table, rowKey, row} = change;
      const rowID: RowID = {schema: '', table, rowKey: rowKey as RowKey};

      let parsedRow = rows.get(rowID);
      let rc: number;
      if (!parsedRow) {
        parsedRow = {refCounts: {}};
        rows.set(rowID, parsedRow);
        rc = 0;
      } else {
        rc = parsedRow.refCounts[queryHash] ?? 0;
      }

      const updateVersion = (row: Row) => {
        if (!parsedRow.version) {
          const {version, contents} = contentsAndVersion(row);
          parsedRow.version = version;
          parsedRow.contents = contents;
        }
      };
      switch (type) {
        case 'add':
          updateVersion(row);
          rc++;
          break;
        case 'edit':
          updateVersion(row);
          // No update to rc.
          break;
        case 'remove':
          rc--;
          break;
        default:
          unreachable(type);
      }

      parsedRow.refCounts[queryHash] = rc;

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
   *
   * Returns false if the advancement failed due to a schema change.
   */
  async #advancePipelines(
    cvr: CVRSnapshot,
  ): Promise<'success' | SchemaChangeError> {
    assert(this.#pipelines.initialized());
    const start = Date.now();

    const {version, numChanges, changes} = this.#pipelines.advance();
    const lc = this.#lc.withContext('newVersion', version);

    // Probably need a new updater type. CVRAdvancementUpdater?
    const updater = new CVRQueryDrivenUpdater(
      this.#cvrStore,
      cvr,
      version,
      this.#pipelines.replicaVersion,
    );
    const pokers = [...this.#clients.values()].map(c =>
      c.startPoke(
        updater.updatedVersion(),
        this.#pipelines.currentSchemaVersions(),
      ),
    );

    lc.debug?.(`applying ${numChanges} to advance to ${version}`);
    try {
      await this.#processChanges(lc, changes, updater, pokers);
    } catch (e) {
      if (e instanceof SchemaChangeError) {
        pokers.forEach(poker => poker.cancel());
        return e;
      }
      throw e;
    }

    // Commit the changes and update the CVR snapshot.
    this.#cvr = (await updater.flush(lc)).cvr;

    // Signal clients to commit.
    pokers.forEach(poker => poker.end());

    lc.info?.(`finished processing advancement (${Date.now() - start} ms)`);
    return 'success';
  }

  stop(): Promise<void> {
    this.#lc.info?.('stopping view syncer');
    this.#stateChanges.cancel();
    return this.#stopped.promise;
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

const CURSOR_PAGE_SIZE = 10000;

function contentsAndVersion(row: Row) {
  const {[ZERO_VERSION_COLUMN_NAME]: version, ...contents} = row;
  if (typeof version !== 'string' || version.length === 0) {
    throw new Error(`Invalid _0_version in ${stringify(row)}`);
  }
  return {contents, version};
}
