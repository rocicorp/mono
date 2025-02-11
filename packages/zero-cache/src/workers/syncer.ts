import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {type JWTPayload} from 'jose';
import {pid} from 'node:process';
import {MessagePort} from 'node:worker_threads';
import {WebSocketServer, type WebSocket} from 'ws';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import * as ErrorKind from '../../../zero-protocol/src/error-kind-enum.ts';
import {verifyToken} from '../auth/jwt.ts';
import {type AuthConfig, type ZeroConfig} from '../config/zero-config.ts';
import type {ConnectParams} from '../services/dispatcher/connect-params.ts';
import {installWebSocketReceiver} from '../services/dispatcher/websocket-handoff.ts';
import type {Mutagen} from '../services/mutagen/mutagen.ts';
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
import {Connection, sendError} from './connection.ts';
import {createNotifierFrom, subscribeTo} from './replicator.ts';
import {SyncerWsMessageHandler} from './syncer-ws-message-handler.ts';
import type {Pusher} from '../services/mutagen/pusher.ts';

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
    this.#mutagens = new ServiceRunner(lc, mutagenFactory);
    if (pusherFactory) {
      this.#pushers = new ServiceRunner(lc, pusherFactory);
    }
    this.#parent = parent;
    this.#wss = new WebSocketServer({noServer: true});

    installWebSocketReceiver(this.#wss, this.#createConnection, this.#parent);
  }

  readonly #createConnection = async (ws: WebSocket, params: ConnectParams) => {
    const {clientID, clientGroupID, auth, userID} = params;
    const existing = this.#connections.get(clientID);
    if (existing) {
      existing.close(`replaced by ${params.wsID}`);
    }

    let decodedToken: JWTPayload | undefined;
    if (auth) {
      try {
        decodedToken = await verifyToken(this.#authConfig, auth, {
          subject: userID,
        });
      } catch (e) {
        sendError(this.#lc, ws, {
          kind: ErrorKind.AuthInvalidated,
          message: `Failed to decode auth token: ${String(e)}`,
        });
        ws.close(3000, 'Failed to decode JWT');
      }
    }

    const connection = new Connection(
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
        this.#mutagens.getService(clientGroupID),
        this.#pushers?.getService(clientGroupID),
      ),
      () => {
        if (this.#connections.get(clientID) === connection) {
          this.#connections.delete(clientID);
        }
      },
    );
    this.#connections.set(clientID, connection);

    connection.init();
    if (params.initConnectionMsg) {
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
