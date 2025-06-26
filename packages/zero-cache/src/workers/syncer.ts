import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {type JWTPayload} from 'jose';
import {pid} from 'node:process';
import {MessagePort} from 'node:worker_threads';
import {WebSocketServer, type WebSocket} from 'ws';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {verifyToken} from '../auth/jwt.ts';
import {type AuthConfig, type ZeroConfig} from '../config/zero-config.ts';
import type {Mutagen} from '../services/mutagen/mutagen.ts';
import type {Pusher} from '../services/mutagen/pusher.ts';
import type {ReplicaState} from '../services/replicator/replicator.ts';
import {ServiceRunner} from '../services/runner.ts';
import type {
  ActivityBasedService,
  Service,
  SingletonService,
} from '../services/service.ts';
import {DrainCoordinator} from '../services/view-syncer/drain-coordinator.ts';
import type {ViewSyncer} from '../services/view-syncer/view-syncer.ts';
import type {Worker} from '../types/processes.ts';
import {Subscription} from '../types/subscription.ts';
import {installWebSocketReceiver} from '../types/websocket-handoff.ts';
import type {ConnectParams} from './connect-params.ts';
import {Connection, sendError} from './connection.ts';
import {createNotifierFrom, subscribeTo} from './replicator.ts';
import {SyncerWsMessageHandler} from './syncer-ws-message-handler.ts';
import {
  recordConnectionSuccess,
  recordConnectionAttempted,
} from '../server/anonymous-otel-start.ts';

export type SyncerWorkerData = {
  replicatorPort: MessagePort;
};

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
  readonly #mutagens: ServiceRunner<Mutagen & Service>;
  readonly #pushers: ServiceRunner<Pusher & Service> | undefined;
  readonly #connections = new Map<string, Connection>();
  readonly #drainCoordinator = new DrainCoordinator();
  readonly #parent: Worker;
  readonly #wss: WebSocketServer;
  readonly #stopped = resolver();
  readonly #authConfig: AuthConfig;

  constructor(
    lc: LogContext,
    config: ZeroConfig,
    viewSyncerFactory: (
      id: string,
      sub: Subscription<ReplicaState>,
      drainCoordinator: DrainCoordinator,
    ) => ViewSyncer & ActivityBasedService,
    mutagenFactory: (id: string) => Mutagen & Service,
    pusherFactory: ((id: string) => Pusher & Service) | undefined,
    parent: Worker,
  ) {
    this.#authConfig = config.auth;
    // Relays notifications from the parent thread subscription
    // to ViewSyncers within this thread.
    const notifier = createNotifierFrom(lc, parent);
    subscribeTo(lc, parent);

    this.#lc = lc;
    this.#viewSyncers = new ServiceRunner(
      lc,
      id => viewSyncerFactory(id, notifier.subscribe(), this.#drainCoordinator),
      v => v.keepalive(),
    );
    this.#mutagens = new ServiceRunner(lc, mutagenFactory, m => m.hasRefs());
    if (pusherFactory) {
      this.#pushers = new ServiceRunner(lc, pusherFactory, p => p.hasRefs());
    }
    this.#parent = parent;
    this.#wss = new WebSocketServer({noServer: true});

    installWebSocketReceiver(
      lc,
      this.#wss,
      this.#createConnection,
      this.#parent,
    );
  }

  readonly #createConnection = async (ws: WebSocket, params: ConnectParams) => {
    this.#lc.debug?.(
      'creating connection',
      params.clientGroupID,
      params.clientID,
    );
    recordConnectionAttempted();
    const {clientID, clientGroupID, auth, userID} = params;
    const existing = this.#connections.get(clientID);
    if (existing) {
      this.#lc.debug?.(
        `client ${clientID} already connected, closing existing connection`,
      );
      existing.close(`replaced by ${params.wsID}`);
    }

    let decodedToken: JWTPayload | undefined;
    if (auth) {
      try {
        decodedToken = await verifyToken(this.#authConfig, auth, {
          subject: userID,
        });
        this.#lc.debug?.(
          `Received auth token ${auth} for clientID ${clientID}, decoded: ${JSON.stringify(decodedToken)}`,
        );
      } catch (e) {
        sendError(
          this.#lc,
          ws,
          {
            kind: ErrorKind.AuthInvalidated,
            message: `Failed to decode auth token: ${String(e)}`,
          },
          e,
        );
        ws.close(3000, 'Failed to decode JWT');
        return;
      }
    } else {
      this.#lc.debug?.(`No auth token received for clientID ${clientID}`);
    }

    const mutagen = this.#mutagens.getService(clientGroupID);
    const pusher = this.#pushers?.getService(clientGroupID);
    // a new connection is using the mutagen and pusher. Bump their ref counts.
    mutagen.ref();
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
          auth !== undefined && decodedToken !== undefined
            ? {
                raw: auth,
                decoded: decodedToken,
              }
            : undefined,
          this.#viewSyncers.getService(clientGroupID),
          mutagen,
          pusher,
        ),
        () => {
          if (this.#connections.get(clientID) === connection) {
            this.#connections.delete(clientID);
          }
          // Connection is closed. We can unref the mutagen and pusher.
          // If their ref counts are zero, they will stop themselves and set themselves invalid.
          mutagen.unref();
          pusher?.unref();
        },
      );
    } catch (e) {
      mutagen.unref();
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
