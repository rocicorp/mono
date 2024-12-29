import {trace} from '@opentelemetry/api';
import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {JWTPayload} from 'jose';
import type {Row} from 'postgres';
import {
  manualSpan,
  startAsyncSpan,
  startSpan,
} from '../../../../otel/src/span.js';
import {version} from '../../../../otel/src/version.js';
import {assert, unreachable} from '../../../../shared/src/asserts.js';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.js';
import {must} from '../../../../shared/src/must.js';
import {randInt} from '../../../../shared/src/rand.js';
import type {AST} from '../../../../zero-protocol/src/ast.js';
import {
  ErrorKind,
  type ChangeDesiredQueriesBody,
  type ChangeDesiredQueriesMessage,
  type Downstream,
  type InitConnectionMessage,
} from '../../../../zero-protocol/src/mod.js';
import type {PermissionsConfig} from '../../../../zero-schema/src/compiled-permissions.js';
import {transformAndHashQuery} from '../../auth/read-authorizer.js';
import {stringify} from '../../types/bigint-json.js';
import {ErrorForClient, getLogLevel} from '../../types/error-for-client.js';
import type {PostgresDB} from '../../types/pg.js';
import {rowIDString, type RowKey} from '../../types/row-key.js';
import type {Source} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import type {ReplicaState} from '../replicator/replicator.js';
import {ZERO_VERSION_COLUMN_NAME} from '../replicator/schema/replication-state.js';
import type {ActivityBasedService} from '../service.js';
import {
  ClientHandler,
  type PatchToVersion,
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
  EMPTY_CVR_VERSION,
  versionFromString,
  versionString,
  versionToCookie,
  type CVRVersion,
  type NullableCVRVersion,
  type RowID,
} from './schema/types.js';
import {ResetPipelineSignal} from './snapshotter.js';

export type TokenData = {
  readonly raw: string;
  readonly decoded: JWTPayload;
};

export type SyncContext = {
  readonly clientID: string;
  readonly wsID: string;
  readonly baseCookie: string | null;
  readonly schemaVersion: number;
  readonly tokenData: TokenData | undefined;
};

const tracer = trace.getTracer('view-syncer', version);

export interface ViewSyncer {
  initConnection(
    ctx: SyncContext,
    msg: InitConnectionMessage,
  ): Source<Downstream>;

  changeDesiredQueries(
    ctx: SyncContext,
    msg: ChangeDesiredQueriesMessage,
  ): Promise<void>;
}

const DEFAULT_KEEPALIVE_MS = 5_000;

function randomID() {
  return randInt(1, Number.MAX_SAFE_INTEGER).toString(36);
}

export class ViewSyncerService implements ViewSyncer, ActivityBasedService {
  readonly id: string;
  readonly #shardID: string;
  readonly #lc: LogContext;
  readonly #pipelines: PipelineDriver;
  readonly #stateChanges: Subscription<ReplicaState>;
  readonly #drainCoordinator: DrainCoordinator;
  readonly #keepaliveMs: number;

  // Note: It is fine to update this variable outside of the lock.
  #lastConnectTime = 0;
  // Note: It is okay to add/remove clients without acquiring the lock.
  readonly #clients = new Map<string, ClientHandler>();

  // Serialize on this lock for:
  // (1) storage or database-dependent operations
  // (2) updating member variables.
  readonly #lock = new Lock();
  readonly #cvrStore: CVRStore;
  readonly #stopped = resolver();
  readonly #permissions: PermissionsConfig;

  #cvr: CVRSnapshot | undefined;
  #pipelinesSynced = false;
  #authData: JWTPayload | undefined;

  constructor(
    lc: LogContext,
    taskID: string,
    clientGroupID: string,
    shardID: string,
    db: PostgresDB,
    pipelineDriver: PipelineDriver,
    versionChanges: Subscription<ReplicaState>,
    drainCoordinator: DrainCoordinator,
    permissions: PermissionsConfig,
    keepaliveMs = DEFAULT_KEEPALIVE_MS,
  ) {
    this.id = clientGroupID;
    this.#shardID = shardID;
    this.#lc = lc;
    this.#pipelines = pipelineDriver;
    this.#stateChanges = versionChanges;
    this.#drainCoordinator = drainCoordinator;
    this.#keepaliveMs = keepaliveMs;
    this.#cvrStore = new CVRStore(
      lc,
      db,
      taskID,
      clientGroupID,
      // On failure, cancel the #stateChanges subscription. The run()
      // loop will then await #cvrStore.flushed() which rejects if necessary.
      () => this.#stateChanges.cancel(),
    );
    this.#permissions = permissions;

    // Wait for the first connection to init.
    this.keepalive();
  }

  #runInLockWithCVR(
    fn: (lc: LogContext, cvr: CVRSnapshot) => Promise<void> | void,
  ): Promise<void> {
    return this.#lock.withLock(async () => {
      const lc = this.#lc.withContext('lock', randomID());
      if (!this.#stateChanges.active) {
        return; // view-syncer has been shutdown
      }
      // If all clients have disconnected, cancel all pending work.
      if (await this.#checkForShutdownConditionsInLock()) {
        this.#lc.info?.('shutting down');
        this.#stateChanges.cancel(); // Note: #stateChanges.active becomes false.
        return;
      }
      if (!this.#cvr) {
        this.#cvr = await this.#cvrStore.load(lc, this.#lastConnectTime);
      }
      try {
        await fn(lc, this.#cvr);
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
        await this.#runInLockWithCVR(async (lc, cvr) => {
          if (
            (cvr.replicaVersion !== null ||
              cvr.version.stateVersion !== '00') &&
            cvr.replicaVersion !== this.#pipelines.replicaVersion
          ) {
            const message = `Replica Version mismatch: CVR=${
              cvr.replicaVersion
            }, DB=${this.#pipelines.replicaVersion}`;
            lc.info?.(`resetting CVR: ${message}`);
            throw new ErrorForClient({kind: ErrorKind.ClientNotFound, message});
          }

          if (this.#pipelinesSynced) {
            const result = await this.#advancePipelines(lc, cvr);
            if (result === 'success') {
              return;
            }
            lc.info?.(`resetting pipelines: ${result.message}`);
            this.#pipelines.reset();
          }

          // Advance the snapshot to the current version.
          const version = this.#pipelines.advanceWithoutDiff();
          const cvrVer = versionString(cvr.version);

          if (version < cvr.version.stateVersion) {
            lc.debug?.(`replica@${version} is behind cvr@${cvrVer}`);
            return; // Wait for the next advancement.
          }

          // stateVersion is at or beyond CVR version for the first time.
          lc.info?.(`init pipelines@${version} (cvr@${cvrVer})`);
          this.#hydrateUnchangedQueries(lc, cvr);
          await this.#syncQueryPipelineSet(lc, cvr);
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
      this.#lc[getLogLevel(e)]?.(`stopping view-syncer: ${String(e)}`, e);
      this.#cleanup(e);
    } finally {
      // Always wait for the cvrStore to flush, regardless of how the service
      // was stopped.
      await this.#cvrStore
        .flushed(this.#lc)
        .catch(e => this.#lc[getLogLevel(e)]?.(e));
      this.#lc.info?.('view-syncer stopped');
      this.#stopped.resolve();
    }
  }

  #totalHydrationTimeMs(): number {
    return this.#pipelines.totalHydrationTimeMs();
  }

  #keepAliveUntil: number = 0;

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
    this.#keepAliveUntil = Date.now() + this.#keepaliveMs;
    return true;
  }

  #shutdownTimer: NodeJS.Timeout | null = null;

  #scheduleShutdown(delayMs = 0) {
    this.#shutdownTimer ??= setTimeout(() => {
      this.#shutdownTimer = null;

      // All lock tasks check for shutdown so that queued work is immediately
      // canceled when clients disconnect. Queue an empty task to ensure that
      // this check happens.
      void this.#runInLockWithCVR(() => {}).catch(e =>
        // If an error occurs (e.g. ownership change), propagate the error
        // to the main run() loop via the #stateChanges Subscription.
        this.#stateChanges.fail(e),
      );
    }, delayMs);
  }

  async #checkForShutdownConditionsInLock(): Promise<boolean> {
    if (this.#clients.size > 0) {
      return false; // common case.
    }

    // Keep the view-syncer alive if there are pending rows being flushed.
    // It's better to do this before shutting down since it may take a
    // while, during which new connections may come in.
    await this.#cvrStore.flushed(this.#lc);

    if (Date.now() <= this.#keepAliveUntil) {
      this.#scheduleShutdown(this.#keepaliveMs); // check again later
      return false;
    }

    // If no clients have connected while waiting for the row flush, shutdown.
    return this.#clients.size === 0;
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
        this.#scheduleShutdown();
      }
    }
  }

  initConnection(
    ctx: SyncContext,
    initConnectionMessage: InitConnectionMessage,
  ): Source<Downstream> {
    return startSpan(tracer, 'vs.initConnection', () => {
      this.#lastConnectTime = Date.now();
      const {clientID, wsID, baseCookie, schemaVersion, tokenData} = ctx;
      this.#authData = pickToken(this.#authData, tokenData?.decoded);

      const lc = this.#lc
        .withContext('clientID', clientID)
        .withContext('wsID', wsID);

      // Setup the downstream connection.
      const downstream = Subscription.create<Downstream>({
        cleanup: (_, err) => {
          err
            ? lc[getLogLevel(err)]?.(`client closed with error`, err)
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
      this.#clients.get(clientID)?.close(`replaced by wsID: ${wsID}`);
      this.#clients.set(clientID, newClient);

      // Note: initConnection() must be synchronous so that `downstream` is
      // immediately returned to the caller (connection.ts). This ensures
      // that if the connection is subsequently closed, the `downstream`
      // subscription can be properly canceled even if #runInLockForClient()
      // has not had a chance to run.
      void this.#runInLockForClient(
        ctx,
        initConnectionMessage,
        this.#patchQueries,
        newClient,
      ).catch(e => newClient.fail(e));

      return downstream;
    });
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
  #runInLockForClient<B, M extends [cmd: string, B] = [string, B]>(
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

    return startAsyncSpan(
      tracer,
      `vs.#runInLockForClient(${cmd})`,
      async () => {
        let client: ClientHandler | undefined;
        try {
          await this.#runInLockWithCVR((lc, cvr) => {
            lc = lc
              .withContext('clientID', clientID)
              .withContext('wsID', wsID)
              .withContext('cmd', cmd);

            client = this.#clients.get(clientID);
            if (client?.wsID !== wsID) {
              // Only respond to messages of the currently connected client.
              // Connections may have been drained or dropped due to an error.
              return;
            }

            if (newClient) {
              assert(newClient === client);
              checkClientAndCVRVersions(client.version(), cvr.version);
            }

            lc.debug?.(cmd, body);
            return fn(lc, clientID, body, cvr);
          });
        } catch (e) {
          const lc = this.#lc
            .withContext('clientID', clientID)
            .withContext('wsID', wsID)
            .withContext('cmd', cmd);
          lc[getLogLevel(e)]?.(`closing connection with error`, e);
          if (client) {
            // Ideally, propagate the exception to the client's downstream subscription ...
            client.fail(e);
          } else {
            // unless the exception happened before the client could be looked up.
            throw e;
          }
        }
      },
    );
  }

  #getClients(atVersion?: CVRVersion): ClientHandler[] {
    const clients = [...this.#clients.values()];
    return atVersion
      ? clients.filter(
          c => cmpVersions(c.version() ?? EMPTY_CVR_VERSION, atVersion) === 0,
        )
      : clients;
  }

  // Must be called from within #lock.
  readonly #patchQueries = (
    lc: LogContext,
    clientID: string,
    {desiredQueriesPatch}: ChangeDesiredQueriesBody,
    cvr: CVRSnapshot,
  ) =>
    startAsyncSpan(tracer, 'vs.#patchQueries', async () => {
      // Apply requested patches.
      if (desiredQueriesPatch.length) {
        lc.debug?.(`applying ${desiredQueriesPatch.length} query patches`);
        const updater = new CVRConfigDrivenUpdater(
          this.#cvrStore,
          cvr,
          this.#shardID,
        );

        const patches: PatchToVersion[] = [];
        for (const patch of desiredQueriesPatch) {
          switch (patch.op) {
            case 'put':
              patches.push(
                ...updater.putDesiredQueries(clientID, {
                  [patch.hash]: patch.ast,
                }),
              );
              break;
            case 'del':
              patches.push(
                ...updater.deleteDesiredQueries(clientID, [patch.hash]),
              );
              break;
            case 'clear':
              patches.push(...updater.clearDesiredQueries(clientID));
              break;
          }
        }

        this.#cvr = (await updater.flush(lc, this.#lastConnectTime)).cvr;

        if (cmpVersions(cvr.version, this.#cvr.version) < 0) {
          // Send pokes to catch up clients that are up to date.
          // (Clients that are behind the cvr.version need to be caught up in
          //  #syncQueryPipelineSet(), as row data may be needed for catchup)
          const newCVR = this.#cvr;
          const pokers = this.#getClients(cvr.version).map(c =>
            c.startPoke(newCVR.version),
          );
          for (const patch of patches) {
            pokers.forEach(poker => poker.addPatch(patch));
          }
          pokers.forEach(poker => poker.end());
        }

        cvr = this.#cvr; // For #syncQueryPipelineSet().
      }

      if (this.#pipelinesSynced) {
        await this.#syncQueryPipelineSet(lc, cvr);
      }
    });

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
  #hydrateUnchangedQueries(lc: LogContext, cvr: CVRSnapshot) {
    assert(this.#pipelines.initialized());

    const dbVersion = this.#pipelines.currentVersion();
    const cvrVersion = cvr.version;

    if (cvrVersion.stateVersion !== dbVersion) {
      lc.info?.(
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
      const {query: transformedAst, hash: newTransformationHash} =
        transformAndHashQuery(ast, this.#permissions, this.#authData);
      assert(
        transformedAst !== undefined && newTransformationHash !== undefined,
        'This query may not be run because the table it reads is not readable. Table: ' +
          ast.table,
      );
      if (newTransformationHash !== transformationHash) {
        continue; // Query results may have changed.
      }
      const start = Date.now();
      let count = 0;
      startSpan(tracer, 'vs.#hydrateUnchangedQueries.addQuery', span => {
        span.setAttribute('queryHash', hash);
        span.setAttribute('transformationHash', transformationHash);
        span.setAttribute('table', ast.table);
        for (const _ of this.#pipelines.addQuery(
          transformationHash,
          transformedAst,
        )) {
          count++;
        }
      });
      const elapsed = Date.now() - start;
      lc.debug?.(`hydrated ${count} rows for ${hash} (${elapsed} ms)`);
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
  #syncQueryPipelineSet(lc: LogContext, cvr: CVRSnapshot) {
    return startAsyncSpan(tracer, 'vs.#syncQueryPipelineSet', async () => {
      assert(this.#pipelines.initialized());

      const hydratedQueries = this.#pipelines.addedQueries();

      // Convert queries to their transformed ast's and hashes
      const transformationHashToHash = new Map<string, string>();
      const serverQueries = Object.entries(cvr.queries).map(([id, q]) => {
        const {query, hash: transformationHash} = transformAndHashQuery(
          q.ast,
          this.#permissions,
          this.#authData,
        );
        assert(
          query !== undefined && transformationHash !== undefined,
          'This query may not be run because the table it reads is not readable. Table: ' +
            q.ast.table,
        );
        transformationHashToHash.set(transformationHash, id);
        return {
          id,
          // TODO(mlaw): follow up to handle the case where we statically determine
          // the query cannot be run and is `undefined`.
          ast: query,
          transformationHash,
          desired: q.internal || Object.keys(q.desiredBy).length > 0,
        };
      });

      const addQueries = serverQueries.filter(
        q => q.desired && !hydratedQueries.has(q.transformationHash),
      );
      const removeQueries = serverQueries.filter(q => !q.desired);
      const desiredQueries = new Set(
        serverQueries.filter(q => q.desired).map(q => q.transformationHash),
      );
      const unhydrateQueries = [...hydratedQueries].filter(
        transformationHash => !desiredQueries.has(transformationHash),
      );

      if (
        addQueries.length > 0 ||
        removeQueries.length > 0 ||
        unhydrateQueries.length > 0
      ) {
        await this.#addAndRemoveQueries(
          lc,
          cvr,
          addQueries,
          removeQueries,
          unhydrateQueries,
          transformationHashToHash,
        );
      } else {
        await this.#catchupClients(lc, cvr);
      }

      // If CVR was non-empty, then the CVR, database, and all clients
      // should now be at the same version.
      if (serverQueries.length > 0) {
        const cvrVersion = must(this.#cvr).version;
        const dbVersion = this.#pipelines.currentVersion();
        assert(
          cvrVersion.stateVersion === dbVersion,
          `CVR@${versionString(cvrVersion)}" does not match DB@${dbVersion}`,
        );
      }
    });
  }

  // This must be called from within the #lock.
  #addAndRemoveQueries(
    lc: LogContext,
    cvr: CVRSnapshot,
    addQueries: {id: string; ast: AST; transformationHash: string}[],
    removeQueries: {id: string; ast: AST; transformationHash: string}[],
    unhydrateQueries: string[],
    transformationHashToHash: Map<string, string>,
  ) {
    return startAsyncSpan(tracer, 'vs.#addAndRemoveQueries', async () => {
      assert(
        addQueries.length > 0 ||
          removeQueries.length > 0 ||
          unhydrateQueries.length > 0,
      );
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

      // Note: This kicks off background PG queries for CVR data associated with the
      // executed and removed queries.
      const {newVersion, queryPatches} = updater.trackQueries(
        lc,
        addQueries,
        removeQueries,
      );
      const pokers = this.#getClients().map(c =>
        c.startPoke(newVersion, this.#pipelines.currentSchemaVersions()),
      );
      for (const patch of queryPatches) {
        pokers.forEach(poker => poker.addPatch(patch));
      }

      // Removing queries is easy. The pipelines are dropped, and the CVR
      // updater handles the updates and pokes.
      for (const q of removeQueries) {
        this.#pipelines.removeQuery(q.transformationHash);
      }
      for (const hash of unhydrateQueries) {
        this.#pipelines.removeQuery(hash);
      }

      const pipelines = this.#pipelines;
      function* generateRowChanges() {
        for (const q of addQueries) {
          lc.debug?.(`adding pipeline for query ${q.id}`, q.ast);
          const start = performance.now();
          yield* pipelines.addQuery(q.transformationHash, q.ast);
          const end = performance.now();
          manualSpan(tracer, 'vs.addAndConsumeQuery', end - start, {
            hash: q.id,
            transformationHash: q.transformationHash,
          });
        }
      }
      // #processChanges does batched de-duping of rows. Wrap all pipelines in
      // a single generator in order to maximize de-duping.
      await this.#processChanges(
        lc,
        generateRowChanges(),
        updater,
        pokers,
        transformationHashToHash,
      );

      for (const patch of await updater.deleteUnreferencedRows(lc)) {
        pokers.forEach(poker => poker.addPatch(patch));
      }

      // Commit the changes and update the CVR snapshot.
      this.#cvr = (await updater.flush(lc, this.#lastConnectTime)).cvr;

      // Before ending the poke, catch up clients that were behind the old CVR.
      await this.#catchupClients(
        lc,
        cvr,
        this.#cvr.version,
        addQueries.map(q => q.id),
        pokers,
      );

      // Signal clients to commit.
      pokers.forEach(poker => poker.end());

      lc.info?.(`finished processing queries (${Date.now() - start} ms)`);
    });
  }

  /**
   * @param cvr The CVR to which clients should be caught up to. This does
   *     not necessarily need to be the current CVR.
   * @param current The expected current CVR version. Before performing
   *     catchup, the snapshot read will verify that the CVR has not been
   *     concurrently modified. Note that this only needs to be done for
   *     catchup because it is the only time data from the CVR DB is
   *     "exported" without being gated by a CVR flush (which provides
   *     concurrency protection in all other cases).
   *
   *     If unspecified, the version of the `cvr` is used.
   * @param excludeQueryHashes Exclude patches from rows associated with
   *     the specified queries.
   * @param usePokers If specified, sends pokes on existing PokeHandlers,
   *     in which case the caller is responsible for sending the `pokeEnd`
   *     messages. If unspecified, the pokes will be started and ended
   *     using the version from the supplied `cvr`.
   */
  // Must be called within #lock
  #catchupClients(
    lc: LogContext,
    cvr: CVRSnapshot,
    current?: CVRVersion,
    excludeQueryHashes: string[] = [],
    usePokers?: PokeHandler[],
  ) {
    return startAsyncSpan(tracer, 'vs.#catchupClients', async span => {
      current ??= cvr.version;
      const clients = this.#getClients();
      const pokers =
        usePokers ??
        clients.map(c =>
          c.startPoke(cvr.version, this.#pipelines.currentSchemaVersions()),
        );
      span.setAttribute('numPokers', pokers.length);
      span.setAttribute('numClients', clients.length);

      const catchupFrom = clients
        .map(c => c.version())
        .reduce((a, b) => (cmpVersions(a, b) < 0 ? a : b), cvr.version);

      // This is an AsyncGenerator which won't execute until awaited.
      const rowPatches = this.#cvrStore.catchupRowPatches(
        lc,
        catchupFrom,
        cvr,
        current,
        excludeQueryHashes,
      );

      // This is a plain async function that kicks off immediately.
      const configPatches = this.#cvrStore.catchupConfigPatches(
        lc,
        catchupFrom,
        cvr,
        current,
      );

      // await the rowPatches first so that the AsyncGenerator kicks off.
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
      span.setAttribute('rowPatchCount', rowPatchCount);
      if (rowPatchCount) {
        lc.debug?.(`sent ${rowPatchCount} row patches`);
      }

      // Then await the config patches which were fetched in parallel.
      for (const patch of await configPatches) {
        pokers.forEach(poker => poker.addPatch(patch));
      }

      if (!usePokers) {
        pokers.forEach(poker => poker.end());
      }
    });
  }

  #processChanges(
    lc: LogContext,
    changes: Iterable<RowChange>,
    updater: CVRQueryDrivenUpdater,
    pokers: PokeHandler[],
    transformationHashToHash: Map<string, string>,
  ) {
    return startAsyncSpan(tracer, 'vs.#processChanges', async () => {
      const start = Date.now();
      const rows = new CustomKeyMap<RowID, RowUpdate>(rowIDString);
      let total = 0;

      const processBatch = () =>
        startAsyncSpan(tracer, 'processBatch', async () => {
          const elapsed = Date.now() - start;
          total += rows.size;
          lc.debug?.(
            `processing ${rows.size} (of ${total}) rows (${elapsed} ms)`,
          );
          const patches = await updater.received(lc, rows);
          patches.forEach(patch =>
            pokers.forEach(poker => poker.addPatch(patch)),
          );
          rows.clear();
        });

      await startAsyncSpan(tracer, 'loopingChanges', async span => {
        for (const change of changes) {
          const {
            type,
            queryHash: transformationHash,
            table,
            rowKey,
            row,
          } = change;
          const queryHash = must(
            transformationHashToHash.get(transformationHash),
            'could not find the original hash for the transformation hash',
          );
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
        span.setAttribute('totalRows', total);
      });
    });
  }

  /**
   * Advance to the current snapshot of the replica and apply / send
   * changes.
   *
   * Must be called from within the #lock.
   *
   * Returns false if the advancement failed due to a schema change.
   */
  #advancePipelines(
    lc: LogContext,
    cvr: CVRSnapshot,
  ): Promise<'success' | ResetPipelineSignal> {
    return startAsyncSpan(tracer, 'vs.#advancePipelines', async () => {
      assert(this.#pipelines.initialized());
      const start = Date.now();

      const {version, numChanges, changes} = this.#pipelines.advance();
      lc = lc.withContext('newVersion', version);

      // Probably need a new updater type. CVRAdvancementUpdater?
      const updater = new CVRQueryDrivenUpdater(
        this.#cvrStore,
        cvr,
        version,
        this.#pipelines.replicaVersion,
      );
      // Only poke clients that are at the cvr.version. New clients that
      // are behind need to first be caught up when their initConnection
      // message is processed (and #syncQueryPipelines is called).
      const pokers = this.#getClients(cvr.version).map(c =>
        c.startPoke(
          updater.updatedVersion(),
          this.#pipelines.currentSchemaVersions(),
        ),
      );

      lc.debug?.(`applying ${numChanges} to advance to ${version}`);
      const transformationHashToHash = new Map<string, string>();
      for (const query of Object.values(cvr.queries)) {
        if (!query.transformationHash) {
          continue;
        }
        transformationHashToHash.set(query.transformationHash, query.id);
      }

      try {
        await this.#processChanges(
          lc,
          changes,
          updater,
          pokers,
          transformationHashToHash,
        );
      } catch (e) {
        if (e instanceof ResetPipelineSignal) {
          pokers.forEach(poker => poker.cancel());
          return e;
        }
        throw e;
      }

      // Commit the changes and update the CVR snapshot.
      this.#cvr = (await updater.flush(lc, this.#lastConnectTime)).cvr;

      // Signal clients to commit.
      pokers.forEach(poker => poker.end());

      lc.info?.(`finished processing advancement (${Date.now() - start} ms)`);
      return 'success';
    });
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
        client.close('shutting down');
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

const NEW_CVR_VERSION = {stateVersion: '00'};

function checkClientAndCVRVersions(
  client: NullableCVRVersion,
  cvr: CVRVersion,
) {
  if (
    cmpVersions(cvr, NEW_CVR_VERSION) === 0 &&
    cmpVersions(client, NEW_CVR_VERSION) > 0
  ) {
    // CVR is empty but client is not.
    throw new ErrorForClient({
      kind: ErrorKind.ClientNotFound,
      message: 'Client not found',
    });
  }

  if (cmpVersions(client, cvr) > 0) {
    // Client is ahead of a non-empty CVR.
    throw new ErrorForClient({
      kind: ErrorKind.InvalidConnectionRequestBaseCookie,
      message: `CVR is at version ${versionString(cvr)}`,
    });
  }
}

export function pickToken(
  previousToken: JWTPayload | undefined,
  newToken: JWTPayload | undefined,
) {
  if (previousToken === undefined) {
    return newToken;
  }

  if (newToken) {
    if (previousToken.sub !== newToken.sub) {
      throw new ErrorForClient({
        kind: ErrorKind.Unauthorized,
        message:
          'The user id in the new token does not match the previous token. Client groups are pinned to a single user.',
      });
    }

    if (previousToken.iat === undefined) {
      // No issued at time for the existing token? We take the most recently received token.
      return newToken;
    }

    if (newToken.iat === undefined) {
      throw new ErrorForClient({
        kind: ErrorKind.Unauthorized,
        message:
          'The new token does not have an issued at time but the prior token does. Tokens for a client group must either all have issued at times or all not have issued at times',
      });
    }

    // The new token is newer, so we take it.
    if (previousToken.iat < newToken.iat) {
      return newToken;
    }

    // if the new token is older or the same, we keep the existing token.
    return previousToken;
  }

  // previousToken !== undefined but newToken is undefined
  throw new ErrorForClient({
    kind: ErrorKind.Unauthorized,
    message:
      'No token provided. An unauthenticated client cannot connect to an authenticated client group.',
  });
}
