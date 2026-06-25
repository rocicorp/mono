import {pid} from 'node:process';
import type {MessagePort} from 'node:worker_threads';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {WebSocketServer, type ServerOptions, type WebSocket} from 'ws';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {
  isProtocolError,
  ProtocolError,
} from '../../../zero-protocol/src/error.ts';
import {resolveAuth, type Auth, type ValidateLegacyJWT} from '../auth/auth.ts';
import {tokenConfigOptions} from '../auth/jwt.ts';
import {type ZeroConfig} from '../config/zero-config.ts';
import {getOrCreateGauge} from '../observability/metrics.ts';
import {
  recordConnectionAttempted,
  recordConnectionSuccess,
  setActiveClientGroupsGetter,
} from '../server/anonymous-otel-start.ts';
import type {Mutagen} from '../services/mutagen/mutagen.ts';
import type {Pusher} from '../services/mutagen/pusher.ts';
import type {ReplicaState} from '../services/replicator/replicator.ts';
import {ServiceRunner} from '../services/runner.ts';
import type {
  ActivityBasedService,
  Service,
  SingletonService,
} from '../services/service.ts';
import type {ConnectionContextManager} from '../services/view-syncer/connection-context-manager.ts';
import {DrainCoordinator} from '../services/view-syncer/drain-coordinator.ts';
import type {ViewSyncer} from '../services/view-syncer/view-syncer.ts';
import type {Worker} from '../types/processes.ts';
import type {Subscription} from '../types/subscription.ts';
import {installWebSocketReceiver} from '../types/websocket-handoff.ts';
import type {ConnectParams} from './connect-params.ts';
import {Connection, sendError} from './connection.ts';
import {createNotifierFrom, subscribeTo} from './replicator.ts';
import {SyncerWsMessageHandler} from './syncer-ws-message-handler.ts';

export type SyncerWorkerData = {
  replicatorPort: MessagePort;
};

export type ReplicaReadyState = {
  readonly watermark: string;
  readonly replicaReadyTimeMs: number;
};

export type ServingLagViewSyncer = Pick<
  ViewSyncer,
  'createdAtMs' | 'servedVersion'
>;

export const MAX_REPLICA_READY_STATES = 10_000;

function boundReplicaReadyStates(
  replicaReadyStates: ReplicaReadyState[],
): void {
  if (replicaReadyStates.length > MAX_REPLICA_READY_STATES) {
    replicaReadyStates.splice(
      0,
      replicaReadyStates.length - MAX_REPLICA_READY_STATES,
    );
  }
}

function pruneReplicaReadyStates(
  replicaReadyStates: ReplicaReadyState[],
  firstNeededIndex: number,
): void {
  if (firstNeededIndex > 0) {
    replicaReadyStates.splice(0, firstNeededIndex);
  }

  boundReplicaReadyStates(replicaReadyStates);
}

export function computeMaxServingLagMs(
  now: number,
  replicaReadyStates: ReplicaReadyState[],
  viewSyncers: Iterable<ServingLagViewSyncer>,
): number {
  let maxLagMs = 0;
  let firstNeededIndex = replicaReadyStates.length;

  for (const viewSyncer of viewSyncers) {
    const firstUnservedIndex = replicaReadyStates.findIndex(
      ({replicaReadyTimeMs, watermark}) =>
        replicaReadyTimeMs >= viewSyncer.createdAtMs &&
        (viewSyncer.servedVersion === null ||
          viewSyncer.servedVersion < watermark),
    );

    if (firstUnservedIndex === -1) {
      continue;
    }

    firstNeededIndex = Math.min(firstNeededIndex, firstUnservedIndex);
    maxLagMs = Math.max(
      maxLagMs,
      now - replicaReadyStates[firstUnservedIndex].replicaReadyTimeMs,
    );
  }

  pruneReplicaReadyStates(replicaReadyStates, firstNeededIndex);

  return Math.max(0, maxLagMs);
}

function getWebSocketServerOptions(config: ZeroConfig): ServerOptions {
  const options: ServerOptions = {
    noServer: true,
    maxPayload: config.websocketMaxPayloadBytes,
  };

  if (config.websocketCompression) {
    options.perMessageDeflate = true;

    if (config.websocketCompressionOptions) {
      try {
        const compressionOptions = JSON.parse(
          config.websocketCompressionOptions,
        );
        options.perMessageDeflate = compressionOptions;
      } catch (e) {
        throw new Error(
          `Failed to parse ZERO_WEBSOCKET_COMPRESSION_OPTIONS: ${String(e)}. Expected valid JSON.`,
        );
      }
    }
  }

  return options;
}

/**
 * The Syncer worker receives websocket handoffs for "/sync" connections
 * from the Dispatcher in the main thread, and creates websocket
 * {@link Connection}s with a corresponding {@link ViewSyncer}, {@link Mutagen},
 * and {@link Subscription} to version notifications from the Replicator
 * worker.
 */
export class Syncer implements SingletonService {
  readonly id = `syncer-${pid}`;
  readonly #lc: LogContext;
  readonly #viewSyncers: ServiceRunner<ViewSyncer & ActivityBasedService>;
  readonly #mutagens: ServiceRunner<Mutagen & Service> | undefined;
  readonly #pushers: ServiceRunner<Pusher & Service> | undefined;
  readonly #connections = new Map<string, Connection>();
  readonly #drainCoordinator = new DrainCoordinator();
  readonly #parent: Worker;
  readonly #wss: WebSocketServer;
  readonly #stopped = resolver();
  readonly #config: ZeroConfig;
  readonly #validateLegacyJWT: ValidateLegacyJWT | undefined;
  readonly #replicaReadyStates: ReplicaReadyState[] = [];

  constructor(
    lc: LogContext,
    config: ZeroConfig,
    viewSyncerFactory: (
      id: string,
      sub: Subscription<ReplicaState>,
      drainCoordinator: DrainCoordinator,
    ) => ViewSyncer & ActivityBasedService,
    mutagenFactory: ((id: string) => Mutagen & Service) | undefined,
    pusherFactory:
      | ((
          id: string,
          connContextManager: ConnectionContextManager,
        ) => Pusher & Service)
      | undefined,
    parent: Worker,
    validateLegacyJWT: ValidateLegacyJWT | undefined,
  ) {
    this.#config = config;
    this.#validateLegacyJWT = validateLegacyJWT;
    // Relays notifications from the parent thread subscription
    // to ViewSyncers within this thread.
    const notifier = createNotifierFrom(lc, parent, state =>
      this.#recordReplicaReadyState(state),
    );
    subscribeTo(lc, parent);

    this.#lc = lc;
    this.#viewSyncers = new ServiceRunner(
      lc,
      id => viewSyncerFactory(id, notifier.subscribe(), this.#drainCoordinator),
      v => v.keepalive(),
    );
    if (mutagenFactory) {
      this.#mutagens = new ServiceRunner(lc, mutagenFactory, m => m.hasRefs());
    }
    if (pusherFactory) {
      this.#pushers = new ServiceRunner(
        lc,
        id =>
          pusherFactory(
            id,
            this.#viewSyncers.getService(id).connContextManager,
          ),
        p => p.hasRefs(),
      );
    }
    this.#parent = parent;
    this.#wss = new WebSocketServer(getWebSocketServerOptions(config));

    installWebSocketReceiver(
      lc,
      this.#wss,
      this.#createConnection,
      this.#parent,
    );

    setActiveClientGroupsGetter(() => this.#viewSyncers.size);

    getOrCreateGauge(
      'sync',
      'active-client-groups',
      'Number of active client groups',
    ).addCallback(result => result.observe(this.#viewSyncers.size));

    getOrCreateGauge(
      'sync',
      'queries',
      'Active queries (pipelines) across all client groups',
    ).addCallback(result => {
      let total = 0;
      for (const vs of this.#viewSyncers.getServices()) {
        total += vs.queryCount;
      }
      result.observe(total);
    });

    getOrCreateGauge(
      'sync',
      'rows',
      'Tracked rows across all client groups',
    ).addCallback(result => {
      let total = 0;
      for (const vs of this.#viewSyncers.getServices()) {
        total += vs.rowCount;
      }
      result.observe(total);
    });

    getOrCreateGauge('sync', 'serving-lag', {
      description:
        'Maximum time active ViewSyncer client groups have had unserved ' +
        'replica changes. A change is served after IVM advancement, CVR flush, ' +
        'and pokeEnd.',
      unit: 'millisecond',
    }).addCallback(result => {
      result.observe(
        computeMaxServingLagMs(
          Date.now(),
          this.#replicaReadyStates,
          this.#viewSyncers.getServices(),
        ),
      );
    });
  }

  #recordReplicaReadyState(state: ReplicaState): void {
    if (
      state.watermark === undefined ||
      state.replicaReadyTimeMs === undefined
    ) {
      return;
    }
    const last = this.#replicaReadyStates.at(-1);
    if (last && last.watermark >= state.watermark) {
      return;
    }
    this.#replicaReadyStates.push({
      watermark: state.watermark,
      replicaReadyTimeMs: state.replicaReadyTimeMs,
    });
    if (this.#viewSyncers.size === 0) {
      this.#replicaReadyStates.length = 0;
      return;
    }
    boundReplicaReadyStates(this.#replicaReadyStates);
  }

  readonly #createConnection = async (ws: WebSocket, params: ConnectParams) => {
    this.#lc.debug?.(
      'creating connection',
      params.clientGroupID,
      params.clientID,
    );
    recordConnectionAttempted();
    const {clientID, clientGroupID, auth, userID} = params;
    const hasProvidedAuth = auth !== undefined && auth !== '';
    const incomingUserID = userID ?? null;

    if (hasProvidedAuth) {
      const tokenOptions = tokenConfigOptions(this.#config.auth ?? {});

      const hasPushOrMutate =
        this.#config?.push?.url !== undefined ||
        this.#config?.mutate?.url !== undefined;
      const hasQueries =
        this.#config?.query?.url !== undefined ||
        this.#config?.getQueries?.url !== undefined;

      // must either have one of the token options set or have custom mutations & queries enabled
      const hasExactlyOneTokenOption = tokenOptions.length === 1;
      const hasCustomEndpoints = hasPushOrMutate && hasQueries;
      if (!hasExactlyOneTokenOption && !hasCustomEndpoints) {
        throw new Error(
          'Exactly one of jwk, secret, or jwksUrl must be set in order to verify tokens but actually the following were set: ' +
            JSON.stringify(tokenOptions) +
            '. You may also set both ZERO_MUTATE_URL and ZERO_QUERY_URL to enable custom mutations and queries without passing token verification options.',
        );
      }
    }

    let initialAuth: Auth | undefined;

    // Verify JWT BEFORE touching existing connections - prevents unauthenticated
    // attackers from force-disconnecting legitimate users via DoS.
    try {
      initialAuth = await resolveAuth(
        this.#lc
          .withContext('clientGroupID', clientGroupID)
          .withContext('clientID', clientID),
        // no previous auth, since this is a new connection, and resolveAuth is
        // connection scoped, not client group scoped
        undefined,
        incomingUserID,
        auth,
        this.#validateLegacyJWT,
      );
    } catch (e) {
      if (isProtocolError(e)) {
        this.#lc.warn?.(
          'Rejecting sync connection during initial auth resolution',
          {
            clientGroupID,
            clientID,
            incomingUserID,
            hasProvidedAuth,
            errorKind: e.message,
          },
        );
        sendError(this.#lc, ws, e.errorBody);
        ws.close(3000, e.errorBody.message);
        return;
      }
      throw e;
    }

    const viewSyncer = this.#viewSyncers.getService(clientGroupID);
    const connContextManager = viewSyncer.connContextManager;
    const group = connContextManager.getGroupState();

    // TODO(0xcadams): we only check for user ID mismatch here if the group is
    // already validated. This prevents wrong-user reconnects from evicting a
    // healthy connection, but it does not protect against same-user reconnects
    // with an invalid opaque token. The long-term fix is to keep the replacement
    // connection pending until its auth is fully validated, and only then replace
    // the existing socket.
    if (
      group.pinnedUser !== undefined &&
      group.pinnedUser.id !== incomingUserID
    ) {
      const error = new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message:
          'Client groups are pinned to a single userID. Connection userID does not match existing client group userID.',
        origin: ErrorOrigin.ZeroCache,
      });
      sendError(this.#lc, ws, error.errorBody);
      ws.close(3000, error.message);
      return;
    }

    // Check for and close existing connections AFTER auth is validated
    const existing = this.#connections.get(clientID);
    if (existing) {
      this.#lc.debug?.(
        `client ${clientID} already connected, closing existing connection`,
      );
      existing.close(`replaced by ${params.wsID}`);
    }

    connContextManager.registerConnection(
      {clientID, wsID: params.wsID},
      params,
      initialAuth,
    );

    const mutagen = this.#mutagens?.getService(clientGroupID);
    const pusher = this.#pushers?.getService(clientGroupID);
    // a new connection is using the mutagen and pusher. Bump their ref counts.
    mutagen?.ref();
    pusher?.ref();

    let connection: Connection;
    try {
      connection = new Connection(
        this.#lc,
        params,
        ws,
        new SyncerWsMessageHandler(
          this.#lc,
          params,
          connContextManager,
          viewSyncer,
          mutagen,
          pusher,
        ),
        () => {
          connContextManager.closeConnection({
            clientID,
            wsID: params.wsID,
          });
          if (this.#connections.get(clientID) === connection) {
            this.#connections.delete(clientID);
          }
          // Connection is closed. We can unref the mutagen and pusher.
          // If their ref counts are zero, they will stop themselves and set themselves invalid.
          mutagen?.unref();
          pusher?.unref();
        },
      );
    } catch (e) {
      connContextManager.closeConnection({clientID, wsID: params.wsID});
      mutagen?.unref();
      pusher?.unref();
      throw e;
    }

    this.#connections.set(clientID, connection);

    connection.init() && recordConnectionSuccess();

    if (params.initConnectionMsg) {
      this.#lc.debug?.(
        'handling init connection message from sec header',
        params.clientGroupID,
        params.clientID,
      );
      await connection.handleInitConnection(
        JSON.stringify(params.initConnectionMsg),
      );
    }
  };

  run() {
    return this.#stopped.promise;
  }

  /**
   * Graceful shutdown involves shutting down view syncers one at a time, pausing
   * for the duration of view syncer's hydration between each one. This paces the
   * disconnects to avoid creating a backlog of hydrations in the receiving server
   * when the clients reconnect.
   */
  async drain() {
    const start = Date.now();
    this.#lc.info?.(`draining ${this.#viewSyncers.size} view-syncers`);

    this.#drainCoordinator.drainNextIn(0);

    while (this.#viewSyncers.size) {
      await this.#drainCoordinator.forceDrainTimeout;

      // Pick an arbitrary view syncer to force drain.
      for (const vs of this.#viewSyncers.getServices()) {
        this.#lc.debug?.(`draining view-syncer ${vs.id} (forced)`);
        // When this drain or an elective drain completes, the forceDrainTimeout will
        // resolve after the next drain interval.
        void vs.stop();
        break;
      }
    }
    this.#lc.info?.(`finished draining (${Date.now() - start} ms)`);
  }

  stop() {
    this.#wss.close();
    this.#stopped.resolve();
    return promiseVoid;
  }
}
