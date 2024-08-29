import {LogContext} from '@rocicorp/logger';
import {IncomingMessage} from 'http';
import {assert} from 'shared/src/asserts.js';
import {Duplex} from 'stream';
import {MessagePort} from 'worker_threads';
import WebSocket from 'ws';
import {
  ConnectParams,
  getConnectParams,
} from '../services/dispatcher/connect-params.js';
import {installWebSocketReceiver} from '../services/dispatcher/websocket-handoff.js';
import {Mutagen} from '../services/mutagen/mutagen.js';
import {ReplicaVersionReady} from '../services/replicator/replicator.js';
import {ServiceRunner} from '../services/runner.js';
import {ActivityBasedService, Service} from '../services/service.js';
import {ViewSyncer} from '../services/view-syncer/view-syncer.js';
import {Subscription} from '../types/subscription.js';
import {Connection} from './connection.js';
import {createNotifier} from './replicator.js';

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
  readonly #parentPort: MessagePort;
  readonly #wss: WebSocket.Server;

  constructor(
    lc: LogContext,
    viewSyncerFactory: (
      id: string,
      sub: Subscription<ReplicaVersionReady>,
    ) => ViewSyncer & ActivityBasedService,
    mutagenFactory: (id: string) => Mutagen & Service,
    parentPort: MessagePort,
    workerData: SyncerWorkerData,
  ) {
    const {replicatorPort} = workerData;
    assert(replicatorPort instanceof MessagePort);

    // Relays notifications from the Replicator thread subscription
    // to ViewSyncers within this thread.
    const notifier = createNotifier(replicatorPort);

    this.#lc = lc;
    this.#viewSyncers = new ServiceRunner(
      lc,
      id => viewSyncerFactory(id, notifier.addSubscription()),
      v => v.keepalive(),
    );
    this.#mutagens = new ServiceRunner(lc, mutagenFactory);
    this.#parentPort = parentPort;
    this.#wss = new WebSocket.Server({noServer: true});
  }

  #createConnection(ws: WebSocket, params: ConnectParams) {
    const {clientID, clientGroupID} = params;
    const existing = this.#connections.get(clientID);
    if (existing) {
      existing.close();
    }
    const connection = new Connection(
      this.#lc,
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
  }

  run() {
    installWebSocketReceiver<ConnectParams>(
      this.#wss,
      this.#parentPort,
      (ws, params) => this.#createConnection(ws, params),
    );
  }

  /**
   * Creates a new WebSocket connection from an in-thread handoff.
   * This is used for debugging in the single-thread configuration.
   */
  handleUpgrade(message: IncomingMessage, socket: Duplex, head: Buffer) {
    const {url} = message;
    const {params, error} = getConnectParams(
      new URL(url ?? '', 'http://unused/'),
    );
    if (error !== null) {
      socket.write(`HTTP/1.1 400 Bad Request\r\n${String(error)}`);
      return;
    }
    this.#wss.handleUpgrade(message, socket, head, ws =>
      this.#createConnection(ws, params),
    );
  }

  stop() {
    this.#wss.close();
  }
}
