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
} from '../../../../otel/src/span.ts';
import {version} from '../../../../otel/src/version.ts';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {stringify} from '../../../../shared/src/bigint-json.ts';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.ts';
import {must} from '../../../../shared/src/must.ts';
import {randInt} from '../../../../shared/src/rand.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {ChangeDesiredQueriesMessage} from '../../../../zero-protocol/src/change-desired-queries.ts';
import type {
  InitConnectionBody,
  InitConnectionMessage,
} from '../../../../zero-protocol/src/connect.ts';
import type {ErroredQuery} from '../../../../zero-protocol/src/custom-queries.ts';
import type {DeleteClientsMessage} from '../../../../zero-protocol/src/delete-clients.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../../zero-protocol/src/error-origin.ts';
import {
  ProtocolError,
  type TransformFailedBody,
} from '../../../../zero-protocol/src/error.ts';
import type {
  InspectUpBody,
  InspectUpMessage,
} from '../../../../zero-protocol/src/inspect-up.ts';
import {clampTTL, MAX_TTL_MS} from '../../../../zql/src/query/ttl.ts';
import {
  transformAndHashQuery,
  type TransformedAndHashed,
} from '../../auth/read-authorizer.ts';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import type {ZeroConfig} from '../../config/zero-config.ts';
import type {CustomQueryTransformer} from '../../custom-queries/transform-query.ts';
import type {HeaderOptions} from '../../custom/fetch.ts';
import {
  getOrCreateCounter,
  getOrCreateHistogram,
  getOrCreateUpDownCounter,
} from '../../observability/metrics.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {
  getLogLevel,
  ProtocolErrorWithLevel,
} from '../../types/error-with-level.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {rowIDString, type RowKey} from '../../types/row-key.ts';
import type {ShardID} from '../../types/shards.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../replicator/schema/replication-state.ts';
import type {ActivityBasedService} from '../service.ts';
import {
  ClientHandler,
  startPoke,
  type PatchToVersion,
  type PokeHandler,
  type RowPatch,
} from './client-handler.ts';
import {ClientNotFoundError, CVRStore} from './cvr-store.ts';
import type {CVRUpdater} from './cvr.ts';
import {
  CVRConfigDrivenUpdater,
  CVRQueryDrivenUpdater,
  nextEvictionTime,
  type CVRSnapshot,
  type RowUpdate,
} from './cvr.ts';
import type {DrainCoordinator} from './drain-coordinator.ts';
import {handleInspect} from './inspect-handler.ts';
import type {PipelineDriver} from './pipeline-driver.ts';
import {type RowChange} from './pipeline-driver.ts';
import {
  cmpVersions,
  EMPTY_CVR_VERSION,
  versionFromString,
  versionString,
  versionToCookie,
  type ClientQueryRecord,
  type CustomQueryRecord,
  type CVRVersion,
  type InternalQueryRecord,
  type NullableCVRVersion,
  type QueryRecord,
  type RowID,
} from './schema/types.ts';
import {ResetPipelinesSignal} from './snapshotter.ts';
import {
  ttlClockAsNumber,
  ttlClockFromNumber,
  type TTLClock,
} from './ttl-clock.ts';
import {
  isPriorityOpRunning,
  noPriorityOpRunningPromise,
} from '../../server/priority-op.ts';

export type TokenData = {
  readonly raw: string;
  /** @deprecated */
  readonly decoded: JWTPayload;
};

export type SyncContext = {
  readonly clientID: string;
  readonly wsID: string;
  readonly profileID: string | null;
  readonly baseCookie: string | null;
  readonly protocolVersion: number;
  readonly tokenData: TokenData | undefined;
  readonly httpCookie: string | undefined;
  readonly origin: string | undefined;
};

const tracer = trace.getTracer('view-syncer', version);

const PROTOCOL_VERSION_ATTR = 'protocol.version';

export interface ViewSyncer {
  initConnection(
    ctx: SyncContext,
    msg: InitConnectionMessage,
  ): Source<Downstream>;

  changeDesiredQueries(
    ctx: SyncContext,
    msg: ChangeDesiredQueriesMessage,
  ): Promise<void>;

  deleteClients(ctx: SyncContext, msg: DeleteClientsMessage): Promise<void>;
  inspect(context: SyncContext, msg: InspectUpMessage): Promise<void>;
}

const DEFAULT_KEEPALIVE_MS = 5_000;

function randomID() {
  return randInt(1, Number.MAX_SAFE_INTEGER).toString(36);
}

type SetTimeout = (
  fn: (...args: unknown[]) => void,
  delay?: number,
) => ReturnType<typeof setTimeout>;

/**
 * We update the ttlClock in flush that writes to the CVR but
 * some flushes do not write to the CVR and in those cases we
 * use a timer to update the ttlClock every minute.
 */
export const TTL_CLOCK_INTERVAL = 60_000;

/**
 * This is some extra time we delay the TTL timer to allow for some
 * slack in the timing of the timer. This is to allow multiple evictions
 * to happen in a short period of time without having to wait for the
 * next tick of the timer.
 */
export const TTL_TIMER_HYSTERESIS = 50; // ms

type CustomQueryTransformMode = 'all' | 'missing';

export class ViewSyncerService implements ViewSyncer, ActivityBasedService {
  readonly id: string;
  readonly #shard: ShardID;
  readonly #lc: LogContext;
  readonly #pipelines: PipelineDriver;
  readonly #stateChanges: Subscription<ReplicaState>;
  readonly #drainCoordinator: DrainCoordinator;
  readonly #keepaliveMs: number;
  readonly #slowHydrateThreshold: number;
  readonly #queryConfig: ZeroConfig['query'];

  userQueryURL?: string | undefined;
  userQueryHeaders?: Record<string, string> | undefined;

  // The ViewSyncerService is only started in response to a connection,
  // so #lastConnectTime is always initialized to now(). This is necessary
  // to handle race conditions in which, e.g. the replica is ready and the
  // CVR is accessed before the first connection sends a request.
  //
  // Note: It is fine to update this variable outside of the lock.
  #lastConnectTime = Date.now();

  /**
   * The TTL clock is used to determine the time at which queries are considered
   * expired.
   */
  #ttlClock: TTLClock | undefined;

  /**
   * The base time for the TTL clock. This is used to compute the current TTL
   * clock value. The first time a connection is made, this is set to the
   * current time. On subsequent connections, the TTL clock is computed as the
   * difference between the current time and this base time.
   *
   * Every time we write the ttlClock this is update to the current time. That
   * way we can compute how much time has passed since the last time we set the
   * ttlClock. When we set the ttlClock we just increment it by the amount of
   * time that has passed since the last time we set it.
   */
  #ttlClockBase = Date.now();

  /**
   * We update the ttlClock every minute to ensure that it is not too much
   * out of sync with the current time.
   */
  #ttlClockInterval: ReturnType<SetTimeout> | 0 = 0;

  // Note: It is okay to add/remove clients without acquiring the lock.
  readonly #clients = new Map<string, ClientHandler>();

  // Serialize on this lock for:
  // (1) storage or database-dependent operations
  // (2) updating member variables.
  readonly #lock = new Lock();
  readonly #cvrStore: CVRStore;
  readonly #stopped = resolver();
  readonly #initialized = resolver<'initialized'>();

  #cvr: CVRSnapshot | undefined;
  #pipelinesSynced = false;
  // DEPRECATED: remove `authData` in favor of forwarding
  // auth and cookie headers directly
  #authData: TokenData | undefined;

  #httpCookie: string | undefined;
  #origin: string | undefined;

  #expiredQueriesTimer: ReturnType<SetTimeout> | 0 = 0;
  readonly #setTimeout: SetTimeout;
  readonly #customQueryTransformer: CustomQueryTransformer | undefined;

  // Track query replacements for thrashing detection
  readonly #queryReplacements = new Map<
    string,
    {count: number; windowStart: number}
  >();

  readonly #activeClients = getOrCreateUpDownCounter(
    'sync',
    'active-clients',
    'Number of active sync clients',
  );
  readonly #hydrations = getOrCreateCounter(
    'sync',
    'hydration',
    'Number of query hydrations',
  );
  readonly #hydrationTime = getOrCreateHistogram('sync', 'hydration-time', {
    description: 'Time to hydrate a query.',
    unit: 's',
  });
  readonly #transactionAdvanceTime = getOrCreateHistogram(
    'sync',
    'advance-time',
    {
      description:
        'Time to advance all queries for a given client group after applying a new transaction to the replica.',
      unit: 's',
    },
  );
  readonly #queryTransformations = getOrCreateCounter(
    'sync',
    'query.transformations',
    'Number of query transformations performed',
  );
  readonly #queryTransformationTime = getOrCreateHistogram(
    'sync',
    'query.transformation-time',
    {
      description: 'Time to transform custom queries via API server',
      unit: 's',
    },
  );
  readonly #queryTransformationHashChanges = getOrCreateCounter(
    'sync',
    'query.transformation-hash-changes',
    'Number of times query transformation hash changed',
  );
  readonly #queryTransformationNoOps = getOrCreateCounter(
    'sync',
    'query.transformation-no-ops',
    'Number of times query transformation resulted in no-op (hash unchanged)',
  );

  readonly #inspectorDelegate: InspectorDelegate;

  readonly #config: NormalizedZeroConfig;
  #runPriorityOp: <T>(
    lc: LogContext,
    description: string,
    op: () => Promise<T>,
  ) => Promise<T>;

  constructor(
    config: NormalizedZeroConfig,
    lc: LogContext,
    shard: ShardID,
    taskID: string,
    clientGroupID: string,
    cvrDb: PostgresDB,
    upstreamDb: PostgresDB | undefined,
    pipelineDriver: PipelineDriver,
    versionChanges: Subscription<ReplicaState>,
    drainCoordinator: DrainCoordinator,
    slowHydrateThreshold: number,
    inspectorDelegate: InspectorDelegate,
    customQueryTransformer: CustomQueryTransformer | undefined,
    runPriorityOp: <T>(op: () => Promise<T>) => Promise<T>,
    keepaliveMs = DEFAULT_KEEPALIVE_MS,
    setTimeoutFn: SetTimeout = setTimeout.bind(globalThis),
  ) {
    const queryConfig = config.query?.url ? config.query : config.getQueries;
    this.#config = config;
    this.id = clientGroupID;
    this.#shard = shard;
    this.#queryConfig = queryConfig;
    this.#lc = lc;
    this.#pipelines = pipelineDriver;
    this.#stateChanges = versionChanges;
    this.#drainCoordinator = drainCoordinator;
    this.#keepaliveMs = keepaliveMs;
    this.#slowHydrateThreshold = slowHydrateThreshold;
    this.#inspectorDelegate = inspectorDelegate;
    this.#customQueryTransformer = customQueryTransformer;
    this.#cvrStore = new CVRStore(
      lc,
      cvrDb,
      upstreamDb,
      shard,
      taskID,
      clientGroupID,
      // On failure, cancel the #stateChanges subscription. The run()
      // loop will then await #cvrStore.flushed() which rejects if necessary.
      () => this.#stateChanges.cancel(),
    );
    this.#setTimeout = setTimeoutFn;
    this.#runPriorityOp = (lc, description, op) => {
      const start = Date.now();
      lc.debug?.(`running priority op ${description}`);
      return runPriorityOp(op);
      lc.debug?.(
        `finished priority op ${description} after ${Date.now() - start} ms`,
      );
    };
    // Wait for the first connection to init.
    this.keepalive();
  }

  #getHeaderOptions(forwardCookie: boolean): HeaderOptions {
    return {
      apiKey: this.#queryConfig.apiKey,
      customHeaders: this.userQueryHeaders,
      allowedClientHeaders: this.#queryConfig.allowedClientHeaders,
      token: this.#authData?.raw,
      cookie: forwardCookie ? this.#httpCookie : undefined,
      origin: this.#origin,
    };
  }

  #runInLockWithCVR(
    fn: (lc: LogContext, cvr: CVRSnapshot) => Promise<void> | void,
  ): Promise<void> {
    const rid = randomID();
    this.#lc.debug?.('about to acquire lock for cvr ', rid);
    return this.#lock.withLock(async () => {
      this.#lc.debug?.('acquired lock in #runInLockWithCVR ', rid);
      const lc = this.#lc.withContext('lock', rid);
      if (!this.#stateChanges.active) {
        // view-syncer has been shutdown. this can be a backlog of tasks
        // queued on the lock, or it can be a race condition in which a
        // client connects before the ViewSyncer has been deleted from the
        // ServiceRunner.
        this.#lc.debug?.('state changes are inactive');
        clearTimeout(this.#expiredQueriesTimer);
        throw new ProtocolErrorWithLevel({
          kind: ErrorKind.Rehome,
          message: 'Reconnect required',
          origin: ErrorOrigin.ZeroCache,
        });
      }
      // If all clients have disconnected, cancel all pending work.
      if (await this.#checkForShutdownConditionsInLock()) {
        this.#lc.info?.(`closing clientGroupID=${this.id}`);
        this.#stateChanges.cancel(); // Note: #stateChanges.active becomes false.
        return;
      }
      if (!this.#cvr) {
        this.#lc.debug?.('loading cvr');
        this.#cvr = await this.#runPriorityOp(lc, 'loading cvr', () =>
          this.#cvrStore.load(lc, this.#lastConnectTime),
        );
        this.#ttlClock = this.#cvr.ttlClock;
        this.#ttlClockBase = Date.now();
      } else {
        // Make sure the CVR ttlClock is up to date.
        const now = Date.now();
        this.#cvr = {
          ...this.#cvr,
          ttlClock: this.#getTTLClock(now),
        };
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

  readyState(): Promise<'initialized' | 'draining'> {
    return Promise.race([
      this.#initialized.promise,
      this.#drainCoordinator.draining,
    ]);
  }

  async run(): Promise<void> {
    try {
      // Wait for initialization if we need to process queries.
      // This ensures authData and cvr.clientSchema are available before
      // transforming custom queries (dependency on authData) and building
      // pipelines (dependency on cvr.clientSchema).
      if ((await this.readyState()) === 'draining') {
        this.#lc.debug?.(`draining view-syncer ${this.id} before running`);
        void this.stop();
      }
      for await (const {state} of this.#stateChanges) {
        if (this.#drainCoordinator.shouldDrain()) {
          this.#lc.debug?.(`draining view-syncer ${this.id} (elective)`);
          break;
        }
        assert(state === 'version-ready', 'state should be version-ready'); // This is the only state change used.

        await this.#runInLockWithCVR(async (lc, cvr) => {
          const clientSchema = must(
            cvr.clientSchema,
            'cvr.clientSchema missing after initialization',
          );
          if (!this.#pipelines.initialized()) {
            // On the first version-ready signal, connect to the replica.
            this.#pipelines.init(clientSchema);
          }
          if (
            cvr.replicaVersion !== null &&
            cvr.version.stateVersion !== '00' &&
            this.#pipelines.replicaVersion < cvr.replicaVersion
          ) {
            const message = `Cannot sync from older replica: CVR=${
              cvr.replicaVersion
            }, DB=${this.#pipelines.replicaVersion}`;
            lc.info?.(`resetting CVR: ${message}`);
            throw new ClientNotFoundError(message);
          }

          if (this.#pipelinesSynced) {
            const result = await this.#advancePipelines(lc, cvr);
            if (result === 'success') {
              return;
            }
            lc.info?.(`resetting pipelines: ${result.message}`);
            this.#pipelines.reset(clientSchema);
            this.#pipelinesSynced = false;
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

          await this.#hydrateUnchangedQueries(lc, cvr);
          // hydrateUnchangedQueries just transformed
          // all the custom queries, this #syncQueryPipelineSet call
          // should retransform those that are missing from #pipelines, which
          // are those which errored or changed transform hash
          await this.#syncQueryPipelineSet(lc, cvr, 'missing');
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
      this.#lc[getLogLevel(e)]?.(
        `stopping view-syncer ${this.id}: ${String(e)}`,
        e,
      );
      this.#cleanup(e);
    } finally {
      // Always wait for the cvrStore to flush, regardless of how the service
      // was stopped.
      await this.#cvrStore
        .flushed(this.#lc)
        .catch(e => this.#lc[getLogLevel(e)]?.(e));
      this.#lc.info?.(`view-syncer ${this.id} finished`);
      this.#stopped.resolve();
    }
  }

  // must be called from within #lock
  #removeExpiredQueries = async (
    lc: LogContext,
    cvr: CVRSnapshot,
  ): Promise<void> => {
    if (hasExpiredQueries(cvr)) {
      lc = lc.withContext('method', '#removeExpiredQueries');
      lc.debug?.('Queries have expired');
      // #syncQueryPipelineSet() will remove the expired queries.
      if (this.#pipelinesSynced) {
        await this.#syncQueryPipelineSet(lc, cvr, 'missing');
      }
    }

    // Even if we have expired queries, we still need to schedule next eviction
    // since there might be inactivated queries that need to be expired queries
    // in the future.
    this.#scheduleExpireEviction(lc, cvr);
  };

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

  // oxlint-disable-next-line no-unused-private-class-members -- False positive, used in #scheduleShutdown
  #shutdownTimer: NodeJS.Timeout | null = null;

  #scheduleShutdown(delayMs = 0) {
    this.#shutdownTimer ??= this.#setTimeout(() => {
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

  #deleteClientDueToDisconnect(clientID: string, client: ClientHandler) {
    // Note: It is okay to delete / cleanup clients without acquiring the lock.
    // In fact, it is important to do so in order to guarantee that idle cleanup
    // is performed in a timely manner, regardless of the amount of work
    // queued on the lock.
    const c = this.#clients.get(clientID);
    if (c === client) {
      this.#clients.delete(clientID);

      if (this.#clients.size === 0) {
        // It is possible to delete a client before we read the ttl clock from
        // the CVR.
        if (this.#ttlClock !== undefined) {
          this.#updateTTLClockInCVRWithoutLock(this.#lc);
        }
        this.#stopExpireTimer();
        this.#scheduleShutdown();
      }
    }
  }

  #stopExpireTimer() {
    this.#lc.debug?.('Stopping expired queries timer');
    clearTimeout(this.#expiredQueriesTimer);
    this.#expiredQueriesTimer = 0;
  }

  initConnection(
    ctx: SyncContext,
    initConnectionMessage: InitConnectionMessage,
  ): Source<Downstream> {
    this.#lc.debug?.('viewSyncer.initConnection');
    return startSpan(tracer, 'vs.initConnection', () => {
      const {
        clientID,
        profileID,
        wsID,
        baseCookie,
        tokenData,
        httpCookie,
        origin,
        protocolVersion,
      } = ctx;
      this.#authData = pickToken(this.#lc, this.#authData, tokenData);
      this.#lc.debug?.(`Picked auth token for clientGroupID`);
      this.#httpCookie = httpCookie;
      this.#origin = origin;

      // Handle custom query URL and headers
      const [, {userQueryURL, userQueryHeaders}] = initConnectionMessage;
      if (this.userQueryURL === undefined) {
        // First client in the group - store its parameters
        this.userQueryURL = userQueryURL;
        this.userQueryHeaders = userQueryHeaders;
      } else {
        // Validate that subsequent clients have compatible parameters
        if (this.userQueryURL !== userQueryURL) {
          this.#lc.warn?.(
            'Client provided different query parameters than client group',
            {
              clientID,
              clientURL: userQueryURL,
              clientGroupURL: this.userQueryURL,
            },
          );
        }
      }

      const lc = this.#lc
        .withContext('clientID', clientID)
        .withContext('wsID', wsID);

      // Setup the downstream connection.
      const downstream = Subscription.create<Downstream>({
        cleanup: (_, err) => {
          err
            ? lc[getLogLevel(err)]?.(`client closed with error`, err)
            : lc.info?.('client closed');
          this.#deleteClientDueToDisconnect(clientID, newClient);
          this.#activeClients.add(-1, {
            [PROTOCOL_VERSION_ATTR]: protocolVersion,
          });
        },
      });
      this.#activeClients.add(1, {
        [PROTOCOL_VERSION_ATTR]: protocolVersion,
      });

      if (this.#clients.size === 0) {
        // First connection to this ViewSyncerService.

        // initConnection must be synchronous so that the downstream
        // subscription is returned immediately.
        const now = Date.now();
        this.#ttlClockBase = now;
      }

      const newClient = new ClientHandler(
        lc,
        this.id,
        clientID,
        wsID,
        this.#shard,
        baseCookie,
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
        async (lc, clientID, msg: InitConnectionBody, cvr) => {
          if (cvr.clientSchema === null && !msg.clientSchema) {
            throw new ProtocolErrorWithLevel({
              kind: ErrorKind.InvalidConnectionRequest,
              message:
                'The initConnection message for a new client group must include client schema.',
              origin: ErrorOrigin.ZeroCache,
            });
          }
          await this.#handleConfigUpdate(
            lc,
            clientID,
            msg,
            cvr,
            'all', // re transform all on new connections
            // Until the profileID is required in the URL, default it to
            // `cg${clientGroupID}`, as is done in the schema migration.
            // As clients update to the zero version with the profileID logic,
            // the value will be correspondingly in the CVR db.
            profileID ?? `cg${this.id}`,
          );
          // this.#authData  and cvr (in particular cvr.clientSchema) have been
          // initialized, signal the run loop to run.
          this.#initialized.resolve('initialized');
        },
        newClient,
      ).catch(e => newClient.fail(e));

      return downstream;
    });
  }

  async changeDesiredQueries(
    ctx: SyncContext,
    msg: ChangeDesiredQueriesMessage,
  ): Promise<void> {
    await this.#runInLockForClient(
      ctx,
      msg,
      (lc, clientID, msg: Partial<InitConnectionBody>, cvr) =>
        this.#handleConfigUpdate(lc, clientID, msg, cvr, 'missing'),
    );
  }

  async deleteClients(
    ctx: SyncContext,
    msg: DeleteClientsMessage,
  ): Promise<void> {
    await this.#runInLockForClient(
      ctx,
      [msg[0], {deleted: msg[1]}],
      (lc, clientID, msg: Partial<InitConnectionBody>, cvr) =>
        this.#handleConfigUpdate(lc, clientID, msg, cvr, 'missing'),
    );
  }

  #getTTLClock(now: number): TTLClock {
    // We will update ttlClock with delta from the ttlClockBase to the current time.
    const delta = now - this.#ttlClockBase;
    assert(this.#ttlClock !== undefined, 'ttlClock should be defined');
    const ttlClock = ttlClockFromNumber(
      ttlClockAsNumber(this.#ttlClock) + delta,
    );
    assert(
      ttlClockAsNumber(ttlClock) <= now,
      'ttlClock should be less than or equal to now',
    );
    this.#ttlClock = ttlClock;
    this.#ttlClockBase = now;
    return ttlClock as TTLClock;
  }

  #flushUpdater(lc: LogContext, updater: CVRUpdater): Promise<CVRSnapshot> {
    return this.#runPriorityOp(lc, 'flushing cvr', async () => {
      const now = Date.now();
      const ttlClock = this.#getTTLClock(now);
      const {cvr, flushed} = await updater.flush(
        lc,
        this.#lastConnectTime,
        now,
        ttlClock,
      );

      if (flushed) {
        // If the CVR was flushed, we restart the ttlClock interval.
        this.#startTTLClockInterval(lc);
      }

      return cvr;
    });
  }

  #startTTLClockInterval(lc: LogContext): void {
    this.#stopTTLClockInterval();
    this.#ttlClockInterval = this.#setTimeout(() => {
      this.#updateTTLClockInCVRWithoutLock(lc);
      this.#startTTLClockInterval(lc);
    }, TTL_CLOCK_INTERVAL);
  }

  #stopTTLClockInterval(): void {
    clearTimeout(this.#ttlClockInterval);
    this.#ttlClockInterval = 0;
  }

  #updateTTLClockInCVRWithoutLock(lc: LogContext): void {
    const rid = randomID();
    lc.debug?.('Syncing ttlClock', rid);
    const start = Date.now();
    const ttlClock = this.#getTTLClock(start);
    this.#cvrStore
      .updateTTLClock(ttlClock, start)
      .then(() => {
        lc.debug?.('Synced ttlClock', rid, `in ${Date.now() - start} ms`);
      })
      .catch(e => {
        lc.error?.(
          'failed to update TTL clock',
          rid,
          `after ${Date.now() - start} ms`,
          e,
        );
      });
  }

  async #updateCVRConfig(
    lc: LogContext,
    cvr: CVRSnapshot,
    clientID: string,
    customQueryTransformMode: CustomQueryTransformMode,
    fn: (updater: CVRConfigDrivenUpdater) => PatchToVersion[],
  ): Promise<CVRSnapshot> {
    const updater = new CVRConfigDrivenUpdater(
      this.#cvrStore,
      cvr,
      this.#shard,
    );
    updater.ensureClient(clientID);
    const patches = fn(updater);

    this.#cvr = await this.#flushUpdater(lc, updater);

    if (cmpVersions(cvr.version, this.#cvr.version) < 0) {
      // Send pokes to catch up clients that are up to date.
      // (Clients that are behind the cvr.version need to be caught up in
      //  #syncQueryPipelineSet(), as row data may be needed for catchup)
      const newCVR = this.#cvr;
      const pokers = startPoke(this.#getClients(cvr.version), newCVR.version);
      for (const patch of patches) {
        await pokers.addPatch(patch);
      }
      await pokers.end(newCVR.version);
    }

    if (this.#pipelinesSynced) {
      await this.#syncQueryPipelineSet(lc, this.#cvr, customQueryTransformMode);
    }

    return this.#cvr;
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
    this.#lc.debug?.('viewSyncer.#runInLockForClient');
    const {clientID, wsID} = ctx;
    const [cmd, body] = msg;

    if (newClient || !this.#clients.has(clientID)) {
      this.#lastConnectTime = Date.now();
    }

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
            lc.debug?.('acquired lock for cvr');

            client = this.#clients.get(clientID);
            if (client?.wsID !== wsID) {
              lc.debug?.('mismatched wsID', client?.wsID, wsID);
              // Only respond to messages of the currently connected client.
              // Connections may have been drained or dropped due to an error.
              return;
            }

            if (newClient) {
              assert(
                newClient === client,
                'newClient must match existing client',
              );
              checkClientAndCVRVersions(client.version(), cvr.version);
            } else if (!this.#clients.has(clientID)) {
              lc.warn?.(`Processing ${cmd} before initConnection was received`);
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
  readonly #handleConfigUpdate = (
    lc: LogContext,
    clientID: string,

    {
      clientSchema,
      deleted,
      desiredQueriesPatch,
      activeClients,
    }: Partial<InitConnectionBody>,
    cvr: CVRSnapshot,
    customQueryTransformMode: CustomQueryTransformMode,
    profileID?: string,
  ) =>
    startAsyncSpan(tracer, 'vs.#patchQueries', async () => {
      const deletedClientIDs: string[] = [];
      const deletedClientGroupIDs: string[] = [];

      cvr = await this.#updateCVRConfig(
        lc,
        cvr,
        clientID,
        customQueryTransformMode,
        updater => {
          const {ttlClock} = cvr;
          const patches: PatchToVersion[] = [];

          if (clientSchema) {
            updater.setClientSchema(lc, clientSchema);
          }
          if (profileID) {
            updater.setProfileID(lc, profileID);
          }

          // Apply requested patches.
          lc.debug?.(`applying ${desiredQueriesPatch?.length} query patches`);
          if (desiredQueriesPatch?.length) {
            for (const patch of desiredQueriesPatch) {
              switch (patch.op) {
                case 'put':
                  patches.push(...updater.putDesiredQueries(clientID, [patch]));
                  break;
                case 'del':
                  patches.push(
                    ...updater.markDesiredQueriesAsInactive(
                      clientID,
                      [patch.hash],
                      ttlClock,
                    ),
                  );
                  break;
                case 'clear':
                  patches.push(...updater.clearDesiredQueries(clientID));
                  break;
              }
            }
          }

          const clientIDsToDelete: Set<string> = new Set();

          if (activeClients) {
            // We find all the clients in this client group that are not active.
            const allClientIDs = Object.keys(cvr.clients);
            const activeClientsSet = new Set(activeClients);
            for (const id of allClientIDs) {
              if (!activeClientsSet.has(id)) {
                clientIDsToDelete.add(id);
              }
            }
          }

          if (deleted?.clientIDs?.length) {
            for (const cid of deleted.clientIDs) {
              assert(cid !== clientID, 'cannot delete self');
              clientIDsToDelete.add(cid);
            }
          }

          for (const cid of clientIDsToDelete) {
            const patchesDueToClient = updater.deleteClient(cid, ttlClock);
            patches.push(...patchesDueToClient);
            deletedClientIDs.push(cid);
          }

          if (deleted?.clientGroupIDs?.length) {
            lc.debug?.(
              `ignoring ${deleted.clientGroupIDs.length} deprecated client group deletes`,
            );
          }

          return patches;
        },
      );

      // Send 'deleteClients' ack to the clients.
      if (
        (deletedClientIDs.length && deleted?.clientIDs?.length) ||
        deletedClientGroupIDs.length
      ) {
        const clients = this.#getClients();
        await Promise.allSettled(
          clients.map(client =>
            client.sendDeleteClients(
              lc,
              deletedClientIDs,
              deletedClientGroupIDs,
            ),
          ),
        );
      }

      this.#scheduleExpireEviction(lc, cvr);
    });

  #scheduleExpireEviction(lc: LogContext, cvr: CVRSnapshot): void {
    const {ttlClock} = cvr;
    this.#stopExpireTimer();

    // first see if there is any inactive query with a ttl.
    const next = nextEvictionTime(cvr);

    if (next === undefined) {
      lc.debug?.('no inactive queries with ttl');
      // no inactive queries with a ttl. Cancel existing timeout if any.
      return;
    }

    // It is common for many queries to be evicted close to the same time, so
    // we add a small delay so we can collapse multiple evictions into a
    // single timer. However, don't add the delay if we're already at the
    // maximum timer limit, as that's not about collapsing.
    const delay = Math.max(
      TTL_TIMER_HYSTERESIS,
      Math.min(
        ttlClockAsNumber(next) -
          ttlClockAsNumber(ttlClock) +
          TTL_TIMER_HYSTERESIS,
        MAX_TTL_MS,
      ),
    );

    lc.debug?.('Scheduling eviction timer to run in ', delay, 'ms');
    this.#expiredQueriesTimer = this.#setTimeout(() => {
      this.#expiredQueriesTimer = 0;
      this.#runInLockWithCVR((lc, cvr) =>
        this.#removeExpiredQueries(lc, cvr),
      ).catch(e =>
        // If an error occurs (e.g. ownership change), propagate the error
        // to the main run() loop via the #stateChanges Subscription.
        this.#stateChanges.fail(e),
      );
    }, delay);
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
  async #hydrateUnchangedQueries(lc: LogContext, cvr: CVRSnapshot) {
    assert(this.#pipelines.initialized(), 'pipelines must be initialized');

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

    const customQueries: Map<string, CustomQueryRecord> = new Map();
    const otherQueries: (ClientQueryRecord | InternalQueryRecord)[] = [];

    for (const [, query] of gotQueries) {
      if (
        query.type !== 'internal' &&
        Object.values(query.clientState).every(
          ({inactivatedAt}) => inactivatedAt !== undefined,
        )
      ) {
        continue; // No longer desired.
      }

      if (query.type === 'custom') {
        customQueries.set(query.id, query);
      } else {
        otherQueries.push(query);
      }
    }

    const transformedQueries: TransformedAndHashed[] = [];
    if (customQueries.size > 0 && !this.#customQueryTransformer) {
      lc.warn?.(
        'Custom/named queries were requested but no `ZERO_QUERY_URL` is configured for Zero Cache.',
      );
    }
    await this.#runPriorityOp(lc, 'hydrating unchanged queries', async () => {
      const customQueryTransformer = this.#customQueryTransformer;
      if (customQueryTransformer && customQueries.size > 0) {
        // Always transform custom queries, even during initialization,
        // to ensure authorization validation with current auth context.
        const transformedCustomQueries = await this.#runPriorityOp(
          lc,
          '#hydrateUnchangedQueries transforming custom queries',
          () =>
            customQueryTransformer.transform(
              this.#getHeaderOptions(this.#queryConfig.forwardCookies),
              customQueries.values(),
              this.userQueryURL,
            ),
        );

        // Only process queries that successfully transformed and transformed to
        // the same transformationHash as in the CVR here.
        // Queries that failed to transform will be retransformed by
        // #syncQueryPipelineSet, if they fail again errors will be sent to
        // the client.
        if (Array.isArray(transformedCustomQueries)) {
          for (const q of transformedCustomQueries) {
            if (
              !('error' in q) &&
              q.transformationHash ===
                customQueries.get(q.id)?.transformationHash
            ) {
              transformedQueries.push(q);
            }
          }
        }
      }
    });

    for (const q of otherQueries) {
      const transformed = transformAndHashQuery(
        lc,
        q.id,
        q.ast,
        must(this.#pipelines.currentPermissions()).permissions ?? {
          tables: {},
        },
        this.#authData?.decoded,
        q.type === 'internal',
      );
      if (transformed.transformationHash === q.transformationHash) {
        // only process queries that transformed to the same
        // transformationHash as in the CVR here
        transformedQueries.push(transformed);
      }
    }

    for (const {
      id: queryID,
      transformationHash,
      transformedAst,
    } of transformedQueries) {
      const timer = new TimeSliceTimer(lc);
      let count = 0;
      await startAsyncSpan(
        tracer,
        'vs.#hydrateUnchangedQueries.addQuery',
        async span => {
          span.setAttribute('queryHash', queryID);
          span.setAttribute('transformationHash', transformationHash);
          span.setAttribute('table', transformedAst.table);
          for (const change of this.#pipelines.addQuery(
            transformationHash,
            queryID,
            transformedAst,
            await timer.start(),
          )) {
            if (change === 'yield') {
              await timer.yieldProcess('yield in hydrateUnchangedQueries');
            } else {
              count++;
            }
          }
        },
      );

      const elapsed = timer.totalElapsed();
      this.#hydrations.add(1);
      this.#hydrationTime.record(elapsed / 1000);
      this.#addQueryMaterializationServerMetric(transformationHash, elapsed);
      lc.debug?.(`hydrated ${count} rows for ${queryID} (${elapsed} ms)`);
    }
  }

  #processTransformedCustomQueries(
    lc: LogContext,
    transformedCustomQueries:
      | (TransformedAndHashed | ErroredQuery)[]
      | TransformFailedBody,
    cb: (q: TransformedAndHashed) => void,
    customQueryMap: Map<string, CustomQueryRecord>,
  ): string[] {
    if ('kind' in transformedCustomQueries) {
      this.#sendQueryTransformErrorToClients(
        customQueryMap,
        transformedCustomQueries,
      );
      return transformedCustomQueries.queryIDs;
    }

    const appQueryErrors: ErroredQuery[] = [];

    for (const q of transformedCustomQueries) {
      if ('error' in q) {
        const errorMessage = `Error transforming custom query ${q.name}: ${q.error}${q.details ? ` ${JSON.stringify(q.details)}` : ''}`;
        lc.warn?.(errorMessage, q);
        appQueryErrors.push(q);
        continue;
      }
      cb(q);
    }

    this.#sendQueryTransformErrorToClients(customQueryMap, appQueryErrors);
    return appQueryErrors.map(q => q.id);
  }

  #sendQueryTransformErrorToClients(
    customQueryMap: Map<string, CustomQueryRecord>,
    errorOrErrors: ErroredQuery[] | TransformFailedBody,
  ) {
    const getAffectedClientIDs = (queryIDs: string[]): Set<string> => {
      const clientIds = new Set<string>();
      for (const queryID of queryIDs) {
        const q = customQueryMap.get(queryID);
        assert(
          q,
          `got an error for query ${queryID} that does not map back to a custom query`,
        );
        Object.keys(q.clientState).forEach(id => clientIds.add(id));
      }
      return clientIds;
    };

    // send the transform failed error to each affected client
    if ('queryIDs' in errorOrErrors) {
      for (const clientId of getAffectedClientIDs(errorOrErrors.queryIDs)) {
        this.#clients
          .get(clientId)
          ?.sendQueryTransformFailedError(errorOrErrors);
      }

      return;
    }

    // Group and send application errors to each affected client
    const appErrorGroups = new Map<string, ErroredQuery[]>();

    for (const err of errorOrErrors) {
      // Application errors need to be grouped by client
      for (const clientId of getAffectedClientIDs([err.id])) {
        const group = appErrorGroups.get(clientId) ?? [];
        group.push(err);
        appErrorGroups.set(clientId, group);
      }
    }

    for (const [clientId, errors] of appErrorGroups) {
      this.#clients.get(clientId)?.sendQueryTransformApplicationErrors(errors);
    }
  }

  #addQueryMaterializationServerMetric(queryID: string, elapsed: number) {
    this.#inspectorDelegate.addMetric(
      'query-materialization-server',
      elapsed,
      queryID,
    );
  }

  /**
   * Adds and/or removes queries to/from the PipelineDriver to bring it
   * in sync with the set of queries in the CVR (both got and desired).
   * If queries are added, removed, or queried due to a new state version,
   * a new CVR version is created and pokes sent to connected clients.
   *
   * This must be called from within the #lock.
   */
  #syncQueryPipelineSet(
    lc: LogContext,
    cvr: CVRSnapshot,
    customQueryTransformMode: CustomQueryTransformMode,
  ) {
    return startAsyncSpan(tracer, 'vs.#syncQueryPipelineSet', async () => {
      assert(
        this.#pipelines.initialized(),
        'pipelines must be initialized (syncQueryPipelineSet)',
      );

      if (this.#ttlClock === undefined) {
        // Get it from the CVR or initialize it to now.
        this.#ttlClock = cvr.ttlClock;
      }
      const now = Date.now();
      const ttlClock = this.#getTTLClock(now);

      // group cvr queries into:
      // 1. custom queries
      // 2. everything else
      // Handle transformation appropriately
      // Then hydrate as `serverQueries`
      const cvrQueryEntires = Object.entries(cvr.queries);
      const customQueries: Map<string, CustomQueryRecord> = new Map();
      const otherQueries: {
        id: string;
        query: ClientQueryRecord | InternalQueryRecord;
      }[] = [];
      const transformedQueries: {
        id: string;
        origQuery: QueryRecord;
        transformed: TransformedAndHashed;
      }[] = [];
      for (const [id, query] of cvrQueryEntires) {
        if (query.type === 'custom') {
          // This should always match, no?
          assert(id === query.id, 'custom query id mismatch');
          customQueries.set(id, query);
        } else {
          otherQueries.push({id, query});
        }
      }

      for (const {id, query: origQuery} of otherQueries) {
        // This should always match, no?
        assert(id === origQuery.id, 'query id mismatch');
        const transformed = transformAndHashQuery(
          lc,
          origQuery.id,
          origQuery.ast,
          must(this.#pipelines.currentPermissions()).permissions ?? {
            tables: {},
          },
          this.#authData?.decoded,
          origQuery.type === 'internal',
        );
        transformedQueries.push({
          id,
          origQuery,
          transformed,
        });
      }

      if (customQueries.size > 0 && !this.#customQueryTransformer) {
        lc.warn?.(
          'Custom/named queries were requested but no `ZERO_QUERY_URL` is configured for Zero Cache.',
        );
      }

      let erroredQueryIDs: string[] | undefined;
      const customQueriesToTransform =
        customQueryTransformMode === 'all'
          ? [...customQueries.values()]
          : (customQueryTransformMode satisfies 'missing') &&
            [...customQueries.values()].filter(
              q => !this.#pipelines.queries().has(q.id),
            );
      const customQueryTransformer = this.#customQueryTransformer;
      if (customQueryTransformer && customQueriesToTransform.length > 0) {
        // Always re-transform custom queries on client connection for security.
        // This ensures the user's API server validates authorization with the
        // current auth context.
        const transformStart = performance.now();
        let transformedCustomQueries;
        try {
          transformedCustomQueries = await this.#runPriorityOp(
            lc,
            '#syncQueryPipelineSet transforming custom queries',
            () =>
              customQueryTransformer.transform(
                this.#getHeaderOptions(true),
                customQueriesToTransform,
                this.userQueryURL,
              ),
          );
          this.#queryTransformations.add(1, {result: 'success'});
        } catch (e) {
          this.#queryTransformations.add(1, {result: 'error'});
          throw e;
        } finally {
          const transformDuration = (performance.now() - transformStart) / 1000;
          this.#queryTransformationTime.record(transformDuration);
        }

        // Check if transform failed entirely (HTTP error or server-side failure).
        // This should disconnect the client and keep existing pipelines intact.
        if (
          !Array.isArray(transformedCustomQueries) &&
          transformedCustomQueries.kind === ErrorKind.TransformFailed
        ) {
          // TransformFailedBody indicates an HTTP or infrastructure error.
          // Throw to disconnect the client without modifying pipelines.
          throw new ProtocolErrorWithLevel(
            transformedCustomQueries,
            getLogLevel(transformedCustomQueries.kind),
          );
        }

        // Process the transformed queries and track which ones succeeded.
        const successfullyTransformedCustomQueries = new Map<
          string,
          TransformedAndHashed
        >();
        erroredQueryIDs = this.#processTransformedCustomQueries(
          lc,
          transformedCustomQueries,
          (q: TransformedAndHashed) => {
            const origQuery = customQueries.get(q.id);
            if (origQuery) {
              successfullyTransformedCustomQueries.set(q.id, q);
              transformedQueries.push({
                id: q.id,
                origQuery,
                transformed: q,
              });
            }
          },
          customQueries,
        );

        // Check for queries whose transformation hash changed and log for debugging.
        // The old pipelines will be removed and destroyed when
        // PipelineManager.addQuery is called with an existing query id and
        // different transformation hash.
        for (const [
          queryID,
          newTransform,
        ] of successfullyTransformedCustomQueries) {
          const existingTransformHash =
            cvr.queries[queryID]?.transformationHash;
          if (existingTransformHash) {
            const oldHash = existingTransformHash;
            const newHash = newTransform.transformationHash;

            if (oldHash !== newHash) {
              // Transformation changed - log and check for thrashing.
              // The unhydrateQueries mechanism below will remove the old pipeline,
              // and addQueries will add the new one.
              lc.info?.(
                `Query ${queryID} transformation changed: ${oldHash} -> ${newHash}`,
              );
              this.#checkForThrashing(queryID);
              this.#queryTransformationHashChanges.add(1);
            } else {
              // hash is the same (no re-hydration needed)
              this.#queryTransformationNoOps.add(1);
            }
          }
          // else: new query, will be added normally
        }
      }

      const removeQueriesQueryIds: Set<string> = new Set(
        Object.values(cvr.queries)
          .filter(q => expired(ttlClock, q))
          .map(q => q.id)
          .concat(erroredQueryIDs || []),
      );
      const addQueries = transformedQueries
        .map(({id, transformed}) => ({
          id,
          ast: transformed.transformedAst,
          transformationHash: transformed.transformationHash,
        }))
        .filter(
          q =>
            !removeQueriesQueryIds.has(q.id) &&
            this.#pipelines.queries().get(q.id)?.transformationHash !==
              q.transformationHash,
        );

      for (const q of addQueries) {
        const orig = cvr.queries[q.id];
        lc.debug?.(
          'ViewSyncer adding query',
          q.ast,
          'transformed from',
          orig.type === 'custom' ? orig.name : orig.ast,
        );
      }

      if (addQueries.length > 0 || removeQueriesQueryIds.size > 0) {
        await this.#addAndRemoveQueries(
          lc,
          cvr,
          addQueries,
          [...removeQueriesQueryIds].map(id => ({id})),
        );
      } else {
        await this.#catchupClients(lc, cvr);
      }
    });
  }

  /**
   * Check if a query is being replaced too frequently (thrashing).
   * Logs a warning if the query has been replaced more than 3 times in 60 seconds.
   */
  #checkForThrashing(queryID: string) {
    const THRASH_WINDOW_MS = 60_000; // 60 seconds
    const THRASH_THRESHOLD = 3;
    const now = Date.now();

    let record = this.#queryReplacements.get(queryID);
    if (!record) {
      record = {count: 1, windowStart: now};
      this.#queryReplacements.set(queryID, record);
      return;
    }

    // If outside the time window, delete the old entry and create a new one
    if (now - record.windowStart > THRASH_WINDOW_MS) {
      this.#queryReplacements.delete(queryID);
      this.#queryReplacements.set(queryID, {count: 1, windowStart: now});
      return;
    }

    // Increment count within the window
    record.count++;

    if (record.count >= THRASH_THRESHOLD) {
      this.#lc.warn?.(
        `Query thrashing detected for query ${queryID}. ${record.count} replacements in 60s. This may indicate clients with different auth contexts connecting to the same client group.`,
      );
    }
  }

  // This must be called from within the #lock.
  #addAndRemoveQueries(
    lc: LogContext,
    cvr: CVRSnapshot,
    addQueries: {id: string; ast: AST; transformationHash: string}[],
    removeQueries: {id: string}[],
  ): Promise<void> {
    return startAsyncSpan(tracer, 'vs.#addAndRemoveQueries', async () => {
      assert(
        addQueries.length > 0 || removeQueries.length > 0,
        'Must have queries to add or remove',
      );
      const start = performance.now();

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
      const clients = this.#getClients();
      const pokers = startPoke(clients, newVersion);
      for (const patch of queryPatches) {
        await pokers.addPatch(patch);
      }

      // Removing queries is easy. The pipelines are dropped, and the CVR
      // updater handles the updates and pokes.
      for (const q of removeQueries) {
        this.#pipelines.removeQuery(q.id);
        // Remove per-query server metrics when query is deleted
        this.#inspectorDelegate.removeQuery(q.id);
        // Clean up thrashing detection for removed queries
        this.#queryReplacements.delete(q.id);
      }

      let totalProcessTime = 0;
      const timer = new TimeSliceTimer(lc);
      const pipelines = this.#pipelines;
      const hydrations = this.#hydrations;
      const hydrationTime = this.#hydrationTime;
      // oxlint-disable-next-line @typescript-eslint/no-this-alias
      const self = this;

      // yield at the very beginning so that the first time slice
      // is properly processed by the time-slice queue.
      await yieldProcess(lc);

      function* generateRowChanges(slowHydrateThreshold: number) {
        for (const q of addQueries) {
          lc = lc
            .withContext('hash', q.id)
            .withContext('transformationHash', q.transformationHash);
          lc.debug?.(`adding pipeline for query`, q.ast);

          yield* pipelines.addQuery(
            q.transformationHash,
            q.id,
            q.ast,
            timer.startWithoutYielding(),
          );
          const elapsed = timer.stop();
          totalProcessTime += elapsed;

          self.#addQueryMaterializationServerMetric(q.id, elapsed);

          if (elapsed > slowHydrateThreshold) {
            lc.warn?.('Slow query materialization', elapsed, q.ast);
          }
          manualSpan(tracer, 'vs.addAndConsumeQuery', elapsed, {
            hash: q.id,
            transformationHash: q.transformationHash,
          });
        }
        hydrations.add(1);
        hydrationTime.record(totalProcessTime / 1000);
      }
      // #processChanges does batched de-duping of rows. Wrap all pipelines in
      // a single generator in order to maximize de-duping.
      await this.#processChanges(
        lc,
        timer,
        generateRowChanges(this.#slowHydrateThreshold),
        updater,
        pokers,
      );

      for (const patch of await updater.deleteUnreferencedRows(lc)) {
        await pokers.addPatch(patch);
      }

      // Commit the changes and update the CVR snapshot.
      this.#cvr = await this.#flushUpdater(lc, updater);

      const finalVersion = this.#cvr.version;

      // Before ending the poke, catch up clients that were behind the old CVR.
      await this.#catchupClients(
        lc,
        cvr,
        finalVersion,
        addQueries.map(q => q.id),
        pokers,
      );

      // Signal clients to commit.
      await pokers.end(finalVersion);

      const wallTime = performance.now() - start;
      lc.info?.(
        `finished processing queries (process: ${totalProcessTime} ms, wall: ${wallTime} ms)`,
      );
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
    usePokers?: PokeHandler,
  ) {
    return startAsyncSpan(tracer, 'vs.#catchupClients', async span => {
      current ??= cvr.version;
      const clients = this.#getClients();
      const pokers = usePokers ?? startPoke(clients, cvr.version);
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

      // The configPatches Promise will be awaited, and exceptions propagated,
      // after the rowPatches are processed. However, a catch handler must be
      // installed on the Promise in the meantime in order to avoid Node
      // crashing with an unhandled rejection error.
      configPatches.catch(() => {});

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
          await pokers.addPatch(patchToVersion);
          rowPatchCount++;
        }
      }
      span.setAttribute('rowPatchCount', rowPatchCount);
      if (rowPatchCount) {
        lc.debug?.(`sent ${rowPatchCount} row patches`);
      }

      // Then await the config patches which were fetched in parallel.
      for (const patch of await configPatches) {
        await pokers.addPatch(patch);
      }

      if (!usePokers) {
        await pokers.end(cvr.version);
      }
    });
  }

  #processChanges(
    lc: LogContext,
    timer: TimeSliceTimer,
    changes: Iterable<RowChange | 'yield'>,
    updater: CVRQueryDrivenUpdater,
    pokers: PokeHandler,
  ) {
    return startAsyncSpan(tracer, 'vs.#processChanges', async () => {
      const start = performance.now();

      const rows = new CustomKeyMap<RowID, RowUpdate>(rowIDString);
      let total = 0;

      const processBatch = () =>
        startAsyncSpan(tracer, 'processBatch', async () => {
          const wallElapsed = performance.now() - start;
          total += rows.size;
          lc.debug?.(
            `processing ${rows.size} (of ${total}) rows (${wallElapsed} ms)`,
          );
          const patches = await updater.received(lc, rows);

          for (const patch of patches) {
            await pokers.addPatch(patch);
          }
          rows.clear();
        });

      await startAsyncSpan(tracer, 'loopingChanges', async span => {
        for (const change of changes) {
          if (change === 'yield') {
            await timer.yieldProcess('yield in processChanges');
            continue;
          }
          const {type, queryID, table, rowKey, row} = change;
          const rowID: RowID = {schema: '', table, rowKey: rowKey as RowKey};

          let parsedRow = rows.get(rowID);
          if (!parsedRow) {
            parsedRow = {refCounts: {}};
            rows.set(rowID, parsedRow);
          }
          parsedRow.refCounts[queryID] ??= 0;

          const updateVersion = (row: Row) => {
            // IVM can output multiple versions of a row as it goes through its
            // intermediate stages. Always update the version and contents;
            // the last version will reflect the final state.
            const {version, contents} = contentsAndVersion(row);
            parsedRow.version = version;
            parsedRow.contents = contents;
          };
          switch (type) {
            case 'add':
              updateVersion(row);
              parsedRow.refCounts[queryID]++;
              break;
            case 'edit':
              updateVersion(row);
              // No update to refCounts.
              break;
            case 'remove':
              parsedRow.refCounts[queryID]--;
              break;
            default:
              unreachable(type);
          }

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
  ): Promise<'success' | ResetPipelinesSignal> {
    return startAsyncSpan(tracer, 'vs.#advancePipelines', async () => {
      assert(
        this.#pipelines.initialized(),
        'pipelines must be initialized (advancePipelines',
      );
      const start = performance.now();

      const timer = new TimeSliceTimer(lc);
      const {version, numChanges, changes} = this.#pipelines.advance(timer);
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
      const pokers = startPoke(
        this.#getClients(cvr.version),
        updater.updatedVersion(),
      );
      lc.debug?.(`applying ${numChanges} to advance to ${version}`);

      try {
        await this.#processChanges(
          lc,
          await timer.start(),
          changes,
          updater,
          pokers,
        );
      } catch (e) {
        if (e instanceof ResetPipelinesSignal) {
          await pokers.cancel();
          return e;
        }
        throw e;
      }

      // Commit the changes and update the CVR snapshot.
      this.#cvr = await this.#flushUpdater(lc, updater);
      const finalVersion = this.#cvr.version;

      // Signal clients to commit.
      await pokers.end(finalVersion);

      const elapsed = performance.now() - start;
      lc.info?.(
        `finished processing advancement of ${numChanges} changes (${elapsed} ms)`,
      );
      this.#transactionAdvanceTime.record(elapsed / 1000);
      return 'success';
    });
  }

  inspect(context: SyncContext, msg: InspectUpMessage): Promise<void> {
    return this.#runInLockForClient(context, msg, this.#handleInspect);
  }

  // oxlint-disable-next-line require-await
  #handleInspect = async (
    lc: LogContext,
    clientID: string,
    body: InspectUpBody,
    cvr: CVRSnapshot,
  ): Promise<void> => {
    const client = must(this.#clients.get(clientID));
    return handleInspect(
      lc,
      body,
      cvr,
      client,
      this.#inspectorDelegate,
      this.id,
      this.#cvrStore,
      this.#config,
      this.#getHeaderOptions(this.#queryConfig.forwardCookies ?? false),
      this.userQueryURL,
      this.#authData,
    );
  };

  stop(): Promise<void> {
    this.#lc.info?.('stopping view syncer');
    this.#initialized.reject('shut down before initialization completed');
    this.#stateChanges.cancel();
    return this.#stopped.promise;
  }

  #cleanup(err?: unknown) {
    this.#stopTTLClockInterval();
    this.#stopExpireTimer();

    this.#pipelines.destroy();
    for (const client of this.#clients.values()) {
      if (err) {
        client.fail(err);
      } else {
        client.close(`closed clientGroupID=${this.id}`);
      }
    }
  }

  /**
   * Test helper: Manually mark initialization as complete.
   * This should only be used in tests that don't call initConnection().
   */
  markInitialized() {
    this.#initialized.resolve('initialized');
  }
}

// Update CVR after every 10000 rows.
const CURSOR_PAGE_SIZE = 10000;

// A global Lock acts as a queue to run a single IVM time slice per iteration
// of the node event loop, thus bounding I/O delay to the duration of a single
// time slice.
//
// Refresher:
// https://nodejs.org/en/learn/asynchronous-work/event-loop-timers-and-nexttick#phases-overview
//
// Note that recursive use of setImmediate() (i.e. calling setImmediate() from
// within a setImmediate() callback), results in enqueuing the latter
// callback in the *next* event loop iteration, as documented in:
// https://nodejs.org/api/timers.html#setimmediatecallback-args
//
// This effectively achieves the desired one-per-event-loop-iteration behavior.
const timeSliceQueue = new Lock();

async function yieldProcess(lc: LogContext) {
  async function wait() {
    if (isPriorityOpRunning()) {
      const start = Date.now();
      lc.debug?.('yieldProcess waiting for priority op');
      await noPriorityOpRunningPromise();
      lc.debug?.(
        `yieldProcess waited for priority op ${Date.now() - start} ms`,
      );
    }
    await new Promise(setImmediate);
    if (isPriorityOpRunning()) {
      lc.debug?.('yieldProcess recursing because priority op is running');
      await wait();
    }
  }
  await timeSliceQueue.withLock(wait);
}

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
    throw new ClientNotFoundError('Client not found');
  }

  if (cmpVersions(client, cvr) > 0) {
    // Client is ahead of a non-empty CVR.
    throw new ProtocolError({
      kind: ErrorKind.InvalidConnectionRequestBaseCookie,
      message: `CVR is at version ${versionString(cvr)}`,
      origin: ErrorOrigin.ZeroCache,
    });
  }
}

export function pickToken(
  lc: LogContext,
  previousToken: TokenData | undefined,
  newToken: TokenData | undefined,
) {
  if (previousToken === undefined) {
    lc.debug?.(`No previous token, using new token`);
    return newToken;
  }

  if (newToken) {
    if (previousToken.decoded.sub !== newToken.decoded.sub) {
      throw new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message:
          'The user id in the new token does not match the previous token. Client groups are pinned to a single user.',
        origin: ErrorOrigin.ZeroCache,
      });
    }

    if (previousToken.decoded.iat === undefined) {
      lc.debug?.(`No issued at time for the existing token, using new token`);
      // No issued at time for the existing token? We take the most recently received token.
      return newToken;
    }

    if (newToken.decoded.iat === undefined) {
      throw new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message:
          'The new token does not have an issued at time but the prior token does. Tokens for a client group must either all have issued at times or all not have issued at times',
        origin: ErrorOrigin.ZeroCache,
      });
    }

    // The new token is newer, so we take it.
    if (previousToken.decoded.iat < newToken.decoded.iat) {
      lc.debug?.(`New token is newer, using it`);
      return newToken;
    }

    // if the new token is older or the same, we keep the existing token.
    lc.debug?.(`New token is older or the same, using existing token`);
    return previousToken;
  }

  // previousToken !== undefined but newToken is undefined
  throw new ProtocolError({
    kind: ErrorKind.Unauthorized,
    message:
      'No token provided. An unauthenticated client cannot connect to an authenticated client group.',
    origin: ErrorOrigin.ZeroCache,
  });
}

/**
 * A query must be expired for all clients in order to be considered
 * expired.
 */
function expired(
  ttlClock: TTLClock,
  q: InternalQueryRecord | ClientQueryRecord | CustomQueryRecord,
): boolean {
  if (q.type === 'internal') {
    return false;
  }

  for (const clientState of Object.values(q.clientState)) {
    const {ttl, inactivatedAt} = clientState;
    if (inactivatedAt === undefined) {
      return false;
    }

    const clampedTTL = clampTTL(ttl);
    if (
      ttlClockAsNumber(inactivatedAt) + clampedTTL >
      ttlClockAsNumber(ttlClock)
    ) {
      return false;
    }
  }
  return true;
}

function hasExpiredQueries(cvr: CVRSnapshot): boolean {
  const {ttlClock} = cvr;
  for (const q of Object.values(cvr.queries)) {
    if (expired(ttlClock, q)) {
      return true;
    }
  }
  return false;
}

export class TimeSliceTimer {
  #total = 0;
  #start = 0;
  #lc: LogContext;

  constructor(lc: LogContext) {
    this.#lc = lc;
  }

  async start() {
    // yield at the very beginning so that the first time slice
    // is properly processed by the time-slice queue.
    await yieldProcess(this.#lc);
    return this.startWithoutYielding();
  }

  startWithoutYielding() {
    this.#total = 0;
    this.#startLap();
    return this;
  }

  async yieldProcess(_msgForTesting?: string) {
    this.#stopLap();
    await yieldProcess(this.#lc);
    this.#startLap();
  }

  #startLap() {
    assert(this.#start === 0, 'already running');
    this.#start = performance.now();
  }

  elapsedLap() {
    assert(this.#start !== 0, 'not running');
    return performance.now() - this.#start;
  }

  #stopLap() {
    assert(this.#start !== 0, 'not running');
    this.#total += performance.now() - this.#start;
    this.#start = 0;
  }

  /** @returns the total elapsed time */
  stop(): number {
    this.#stopLap();
    return this.#total;
  }

  /**
   * @returns the elapsed time. This can be called while the Timer is running
   *          or after it has been stopped.
   */
  totalElapsed(): number {
    return this.#start === 0
      ? this.#total
      : this.#total + performance.now() - this.#start;
  }
}
