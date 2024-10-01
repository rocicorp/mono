import {LogContext} from '@rocicorp/logger';
import assert from 'assert';
import {jwtVerify, type JWTPayload} from 'jose';
import {must} from 'shared/src/must.js';
import {MessagePort} from 'worker_threads';
import WebSocket from 'ws';
import {type ZeroConfig} from '../config/zero-config.js';
import type {ConnectParams} from '../services/dispatcher/connect-params.js';
import {installWebSocketReceiver} from '../services/dispatcher/websocket-handoff.js';
import type {Mutagen} from '../services/mutagen/mutagen.js';
import type {ReplicaState} from '../services/replicator/replicator.js';
import {ServiceRunner} from '../services/runner.js';
import type {ActivityBasedService, Service} from '../services/service.js';
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
export class Syncer {
  readonly #lc: LogContext;
  readonly #viewSyncers: ServiceRunner<ViewSyncer & ActivityBasedService>;
  readonly #mutagens: ServiceRunner<Mutagen & Service>;
  readonly #connections = new Map<string, Connection>();
  readonly #parent: Worker;
  readonly #wss: WebSocket.Server;
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
    this.#wss = new WebSocket.Server({noServer: true});

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
      decodedToken = await decodeAndCheckToken(
        auth,
        this.#jwtSecretBytes,
        userID,
      );
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

  run() {}

  stop() {
    this.#wss.close();
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
