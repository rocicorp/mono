import {LogContext} from '@rocicorp/logger';
import {MessagePort} from 'worker_threads';
import WebSocket from 'ws';
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

  constructor(
    lc: LogContext,
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
    subscribeTo(parent);

    this.#lc = lc;
    this.#viewSyncers = new ServiceRunner(
      lc,
      id => viewSyncerFactory(id, notifier.subscribe()),
      v => v.keepalive(),
    );
    this.#mutagens = new ServiceRunner(lc, mutagenFactory);
    this.#parent = parent;
    this.#wss = new WebSocket.Server({noServer: true});

    installWebSocketReceiver(this.#wss, this.#createConnection, this.#parent);
  }

  readonly #createConnection = (ws: WebSocket, params: ConnectParams) => {
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
  };

  run() {}

  stop() {
    this.#wss.close();
  }
}
