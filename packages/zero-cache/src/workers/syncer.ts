import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import assert from 'assert';
import {jwtVerify, type JWTPayload} from 'jose';
import {pid} from 'process';
import {must} from '../../../shared/src/must.js';
import {promiseVoid} from '../../../shared/src/resolved-promises.js';
import {sleep} from '../../../shared/src/sleep.js';
import {MessagePort} from 'worker_threads';
import {WebSocketServer, type WebSocket} from 'ws';
import {type ZeroConfig} from '../config/zero-config.js';
import type {ConnectParams} from '../services/dispatcher/connect-params.js';
import {installWebSocketReceiver} from '../services/dispatcher/websocket-handoff.js';
import type {Mutagen} from '../services/mutagen/mutagen.js';
import type {ReplicaState} from '../services/replicator/replicator.js';
import {ServiceRunner} from '../services/runner.js';
import type {
  ActivityBasedService,
  Service,
  SingletonService,
} from '../services/service.js';
import type {ViewSyncer} from '../services/view-syncer/view-syncer.js';
import type {Worker} from '../types/processes.js';
import {Subscription} from '../types/subscription.js';
import {Connection} from './connection.js';
import {createNotifierFrom, subscribeTo} from './replicator.js';

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
  readonly #connections = new Map<string, Connection>();
  readonly #parent: Worker;
  readonly #wss: WebSocketServer;
  readonly #stopped = resolver();
  #jwtSecretBytes: Uint8Array | undefined;

  constructor(
    lc: LogContext,
    config: ZeroConfig,
    viewSyncerFactory: (
      id: string,
      sub: Subscription<ReplicaState>,
    ) => ViewSyncer & ActivityBasedService,
    mutagenFactory: (id: string) => Mutagen & Service,
    parent: Worker,
  ) {
    // Relays notifications from the parent thread subscription
    // to ViewSyncers within this thread.
    const notifier = createNotifierFrom(lc, parent);
    subscribeTo(lc, parent);

    this.#lc = lc;
    this.#viewSyncers = new ServiceRunner(
      lc,
      id => viewSyncerFactory(id, notifier.subscribe()),
      v => v.keepalive(),
    );
    this.#mutagens = new ServiceRunner(lc, mutagenFactory);
    this.#parent = parent;
    this.#wss = new WebSocketServer({noServer: true});

    if (config.jwtSecret) {
      this.#jwtSecretBytes = new TextEncoder().encode(config.jwtSecret);
    }

    installWebSocketReceiver(this.#wss, this.#createConnection, this.#parent);
  }

  readonly #createConnection = async (ws: WebSocket, params: ConnectParams) => {
    const {clientID, clientGroupID, auth, userID} = params;
    const existing = this.#connections.get(clientID);
    if (existing) {
      existing.close();
    }

    let decodedToken: JWTPayload | undefined;
    if (auth) {
      try {
        decodedToken = await decodeAndCheckToken(
          auth,
          this.#jwtSecretBytes,
          userID,
        );
      } catch (e) {
        this.#lc.error?.('Failed to decode JWT', e);
        ws.close(3000, 'Failed to decode JWT');
      }
    }

    const connection = new Connection(
      this.#lc,
      decodedToken ?? {},
      this.#viewSyncers.getService(clientGroupID),
      this.#mutagens.getService(clientGroupID),
      params,
      ws,
      () => {
        if (this.#connections.get(clientID) === connection) {
          this.#connections.delete(clientID);
        }
      },
    );
    this.#connections.set(clientID, connection);
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
    for (const viewSyncer of this.#viewSyncers.getServices()) {
      const hydrationTimeMs = viewSyncer.totalHydrationTimeMs();
      await viewSyncer.stop();
      await sleep(hydrationTimeMs);
    }
    this.#lc.info?.(`finished draining (${Date.now() - start} ms)`);
  }

  stop() {
    this.#wss.close();
    this.#stopped.resolve();
    return promiseVoid;
  }
}

export async function decodeAndCheckToken(
  auth: string,
  secret: Uint8Array | undefined,
  userID: string,
) {
  assert(
    secret,
    'JWT secret was not set in `zero.config.ts`. Set this to the secret that you use to sign JWTs.',
  );
  const decodedToken = (await jwtVerify(auth, secret)).payload;
  must(decodedToken, 'Failed to verify JWT');
  assert(
    decodedToken.sub === userID,
    'JWT subject does not match the userID that Zero was constructed with.',
  );
  return decodedToken;
}
