import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import * as v from '../../../../shared/src/valita.ts';
import {
  downstreamSchema,
  type ChangeStreamer,
  type ChangeStreamerService,
  type SizedDownstream,
  type SubscriberContext,
} from '../../services/change-streamer/change-streamer.ts';
import {
  snapshotMessageSchema,
  type SnapshotMessage,
} from '../../services/change-streamer/snapshot.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {Incarnation} from './incarnation.ts';
import type {Trace} from './trace.ts';

/** A replication-manager incarnation, as the network sees it. */
export type SimServer = {
  readonly incarnation: Incarnation;
  readonly service: ChangeStreamerService;
};

/**
 * When a client's `subscribe()` resolves, relative to the server registering
 * the subscriber. The two orders are both real, and the code has to be right in
 * both.
 */
export type SubscribeOrder =
  // Over a websocket, the call resolves when the socket opens, before the
  // server has finished registration.
  | 'socket'
  // In-process, it resolves once the server has registered the subscriber.
  | 'registered';

export class ConnectionRefused extends Error {
  override readonly name = 'ConnectionRefused';
}

/** Taps every connection, for the oracles and the census. */
export type NetworkObserver = {
  opened?(conn: Connection): void;
  /** `data` has reached the client, which has not consumed it yet. */
  received?(conn: Connection, data: SizedDownstream): void;
  /** A reservation's status has reached the client. */
  reserved?(conn: Connection, msg: SnapshotMessage): void;
  closed?(conn: Connection, by: 'client' | 'server' | 'fence'): void;
};

/**
 * The network between view-syncers and replication-managers.
 *
 * Subscriptions cross it as the JSON strings the server produces, and are
 * parsed on the client with the production schema. What a client has consumed
 * is what it has ACKed: consuming a message releases the server's entry for it,
 * which is what advances `Subscriber.acked`. The simulator decides when that
 * happens by pausing a connection and granting it messages one pull at a time.
 *
 * Fencing an incarnation severs its connections, as a process exit closes its
 * sockets. A fenced server's connections end on the client after what the
 * client already received; a fenced client's connections are canceled on the
 * server. Calls a fenced client makes never resolve.
 */
export class SimNetwork {
  readonly #trace: Trace;
  readonly #observer: NetworkObserver;
  readonly #connections = new Set<Connection>();
  readonly #fencedWatchers = new WeakSet<Incarnation>();
  readonly #now: () => number;
  #server: SimServer | undefined;
  #nextID = 1;

  constructor(
    trace: Trace,
    observer: NetworkObserver = {},
    now: () => number = () => 0,
  ) {
    this.#trace = trace;
    this.#observer = observer;
    this.#now = now;
  }

  /** Routes new connections to `server`, or refuses them. */
  route(server: SimServer | undefined): void {
    this.#server = server;
    if (server) {
      this.#watch(server.incarnation);
    }
    this.#trace.emit('net', 0, 'route', {server: server?.incarnation.name});
  }

  get server(): SimServer | undefined {
    return this.#server;
  }

  /**
   * The ChangeStreamer through which `client`'s code reaches a server: the
   * routed one, or `server` alone if given (as a backup replicator reaches
   * only its own replication-manager).
   */
  clientFor(
    client: Incarnation,
    order: () => SubscribeOrder,
    server?: SimServer | undefined,
  ): SimChangeStreamerClient {
    this.#watch(client);
    return new SimChangeStreamerClient(this, client, order, server);
  }

  connections(): Connection[] {
    return [...this.#connections];
  }

  connectionsOf(client: Incarnation): Connection[] {
    return [...this.#connections].filter(c => c.client === client);
  }

  /** Internal: opens a connection from `client` to the routed server. */
  open(
    client: Incarnation,
    kind: Connection['kind'],
    target?: SimServer | undefined,
  ): Connection | null {
    if (client.fenced) {
      return null;
    }
    const server = target ?? this.#server;
    if (!server || server.incarnation.fenced) {
      this.#trace.emit(client.node, client.number, 'net.refused', {kind});
      throw new ConnectionRefused(`no replication-manager for ${kind}`);
    }
    const conn = new Connection(
      this.#nextID++,
      kind,
      client,
      server,
      this.#trace,
      this.#observer,
      closed => this.#connections.delete(closed),
      this.#now(),
    );
    this.#connections.add(conn);
    this.#observer.opened?.(conn);
    return conn;
  }

  #watch(incarnation: Incarnation): void {
    if (this.#fencedWatchers.has(incarnation)) {
      return;
    }
    this.#fencedWatchers.add(incarnation);
    incarnation.onFence(() => {
      for (const conn of [...this.#connections]) {
        if (
          conn.client === incarnation ||
          conn.server.incarnation === incarnation
        ) {
          conn.sever();
        }
      }
      if (this.#server?.incarnation === incarnation) {
        this.route(undefined);
      }
    });
  }
}

/** A client-side value and the release of the server's entry for it. */
type Delivery<T> = {
  readonly value: T;
  readonly consumed: () => void;
};

/**
 * One simulated socket: a `/changes` subscription or a `/snapshot`
 * reservation.
 */
export class Connection {
  readonly id: number;
  readonly kind: 'changes' | 'snapshot';
  readonly client: Incarnation;
  readonly server: SimServer;
  /** Virtual time at which the connection opened. */
  readonly openedAt: number;
  readonly #trace: Trace;
  readonly #observer: NetworkObserver;
  readonly #onClosed: (conn: Connection) => void;
  readonly #held: Delivery<unknown>[] = [];
  readonly downstream: Subscription<unknown, Delivery<unknown>>;
  #source: Source<unknown> | undefined;
  #paused = false;
  #credit = 0;
  #serverEnded = false;
  #closed = false;
  ctx: SubscriberContext | undefined;

  constructor(
    id: number,
    kind: 'changes' | 'snapshot',
    client: Incarnation,
    server: SimServer,
    trace: Trace,
    observer: NetworkObserver,
    onClosed: (conn: Connection) => void,
    openedAt: number,
  ) {
    this.id = id;
    this.openedAt = openedAt;
    this.kind = kind;
    this.client = client;
    this.server = server;
    this.#trace = trace;
    this.#observer = observer;
    this.#onClosed = onClosed;
    this.downstream = new Subscription<unknown, Delivery<unknown>>(
      {
        consumed: delivery => delivery.consumed(),
        cleanup: () => this.#closeFromClient(),
      },
      delivery => delivery.value,
    );
  }

  get closed(): boolean {
    return this.#closed;
  }

  get paused(): boolean {
    return this.#paused;
  }

  /** Messages the server has sent that the client has not been given yet. */
  get held(): number {
    return this.#held.length;
  }

  /** Stops giving the client messages until {@link pull} or {@link resume}. */
  pause(): void {
    this.#paused = true;
    this.#credit = 0;
  }

  /** Gives a paused client up to `n` more messages. */
  pull(n: number): void {
    this.#credit += n;
    this.#flush();
  }

  resume(): void {
    this.#paused = false;
    this.#credit = 0;
    this.#flush();
  }

  /** Ends the connection from the client, cleanly or with `err`. */
  disconnect(err?: Error): void {
    if (err) {
      this.downstream.fail(err);
    } else {
      this.downstream.cancel();
    }
  }

  /** Internal: starts carrying `source` to the client. */
  attach(
    source: Source<unknown>,
    serialize: (value: unknown) => string,
    parse: (json: string) => unknown,
  ): void {
    if (this.#closed) {
      this.#runOnServer(() => source.cancel());
      return;
    }
    this.#source = source;
    const {pipeline} = source;
    if (!pipeline) {
      throw new Error('the simulated network carries only pipelined sources');
    }
    void this.#runOnServer(async () => {
      try {
        for await (const {value, consumed} of pipeline) {
          if (this.#closed) {
            break;
          }
          const parsed = parse(serialize(value));
          if (this.kind === 'changes') {
            this.#observer.received?.(this, parsed as SizedDownstream);
          } else {
            this.#observer.reserved?.(this, parsed as SnapshotMessage);
          }
          this.#held.push({value: parsed, consumed});
          this.#flush();
        }
      } catch {
        // A failed source closes the socket; the client sees it end.
      }
      this.#endFromServer();
    });
  }

  /** Internal: the server could not accept the connection. */
  reject(err: unknown): void {
    this.#trace.emit(this.client.node, this.client.number, 'net.rejected', {
      id: this.id,
      kind: this.kind,
      err,
    });
    this.#endFromServer();
  }

  /** Internal: a process on one end is gone. */
  sever(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#held.splice(0);
    this.#trace.emit('net', 0, 'net.severed', {
      id: this.id,
      kind: this.kind,
      client: this.client.name,
      server: this.server.incarnation.name,
    });
    this.#onClosed(this);
    this.#observer.closed?.(this, 'fence');
    if (this.server.incarnation.fenced) {
      // What the client already received it still processes.
      if (!this.client.fenced) {
        this.client.run(() => this.downstream.end());
      }
    } else {
      const source = this.#source;
      this.#runOnServer(() => source?.cancel());
    }
  }

  #flush(): void {
    while (this.#held.length && (!this.#paused || this.#credit > 0)) {
      if (this.#paused) {
        this.#credit--;
      }
      this.downstream.push(this.#held.shift() as Delivery<unknown>);
    }
    if (this.#serverEnded && this.#held.length === 0) {
      this.downstream.end();
    }
  }

  #endFromServer(): void {
    if (this.#serverEnded) {
      return;
    }
    this.#serverEnded = true;
    // Messages already sent arrive before the close; the client ends once it
    // has been given them.
    this.#flush();
  }

  #closeFromClient(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#held.splice(0);
    this.#trace.emit(this.client.node, this.client.number, 'net.closed', {
      id: this.id,
      kind: this.kind,
    });
    this.#onClosed(this);
    this.#observer.closed?.(this, this.#serverEnded ? 'server' : 'client');
    const source = this.#source;
    this.#runOnServer(() => source?.cancel());
  }

  #runOnServer<T>(fn: () => T): T | undefined {
    const {incarnation} = this.server;
    return incarnation.fenced ? undefined : incarnation.run(fn);
  }
}

const parseDownstream = (json: string): SizedDownstream => ({
  data: v.parse(BigIntJSON.parse(json), downstreamSchema, 'passthrough'),
  size: json.length,
});

const parseSnapshotMessage = (json: string): SnapshotMessage =>
  v.parse(BigIntJSON.parse(json), snapshotMessageSchema, 'passthrough');

/**
 * What a view-syncer's replicator and restore code call instead of
 * `ChangeStreamerHttpClient`.
 */
export class SimChangeStreamerClient implements ChangeStreamer {
  readonly #network: SimNetwork;
  readonly #client: Incarnation;
  readonly #order: () => SubscribeOrder;
  readonly #server: SimServer | undefined;

  constructor(
    network: SimNetwork,
    client: Incarnation,
    order: () => SubscribeOrder,
    server: SimServer | undefined,
  ) {
    this.#network = network;
    this.#client = client;
    this.#order = order;
    this.#server = server;
  }

  async subscribe(ctx: SubscriberContext): Promise<Source<SizedDownstream>> {
    const conn = this.#network.open(this.#client, 'changes', this.#server);
    if (conn === null) {
      return never();
    }
    conn.ctx = ctx;
    const order = this.#order();
    const registered = conn.server.incarnation.run(() =>
      conn.server.service.subscribe(ctx),
    );
    const attach = (source: Source<string>) =>
      conn.attach(source, value => value as string, parseDownstream);
    if (order === 'registered') {
      try {
        attach(await registered);
      } catch (e) {
        conn.reject(e);
      }
    } else {
      registered.then(attach, e => conn.reject(e));
    }
    return conn.downstream as Source<SizedDownstream>;
  }

  async reserveSnapshot(taskID: string): Promise<Source<SnapshotMessage>> {
    const conn = this.#network.open(this.#client, 'snapshot', this.#server);
    if (conn === null) {
      return never();
    }
    try {
      const source = await conn.server.incarnation.run(() =>
        conn.server.service.startSnapshotReservation(taskID),
      );
      conn.attach(
        source,
        value => BigIntJSON.stringify(value as SnapshotMessage),
        parseSnapshotMessage,
      );
    } catch (e) {
      conn.reject(e);
    }
    return conn.downstream as Source<SnapshotMessage>;
  }
}

function never<T>(): Promise<T> {
  return new Promise<T>(() => {});
}
