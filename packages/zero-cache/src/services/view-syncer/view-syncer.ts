import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import type {
  ChangeDesiredQueriesBody,
  ChangeDesiredQueriesMessage,
  Downstream,
  InitConnectionMessage,
} from 'zero-protocol';
import type {AST} from 'zql/src/zql/ast2/ast.js';
import type {PostgresDB} from '../../types/pg.js';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import {ReplicaVersionReady} from '../replicator/replicator.js';
import type {ActivityBasedService} from '../service.js';
import {ClientHandler} from './client-handler.js';
import {CVRStore} from './cvr-store.js';
import {CVRConfigDrivenUpdater, type CVRSnapshot} from './cvr.js';
import {Snapshotter} from './snapshotter.js';

export type SyncContext = {
  readonly clientID: string;
  readonly wsID: string;
  readonly baseCookie: string | null;
};

export interface ViewSyncer {
  initConnection(
    ctx: SyncContext,
    msg: InitConnectionMessage,
  ): Promise<CancelableAsyncIterable<Downstream>>;

  changeDesiredQueries(
    ctx: SyncContext,
    msg: ChangeDesiredQueriesMessage,
  ): Promise<void>;
}

type IdleToken = {
  timeoutID?: ReturnType<typeof setTimeout>;
};

const DEFAULT_KEEPALIVE_MS = 30_000;

export class ViewSyncerService implements ViewSyncer, ActivityBasedService {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #db: PostgresDB;
  readonly #replicaDbFile: string;
  readonly #versionChanges: Subscription<ReplicaVersionReady>;
  readonly #keepaliveMs: number;

  // Serialize on this lock for:
  // (1) storage or database-dependent operations
  // (2) updating member variables.
  readonly #lock = new Lock();
  readonly #clients = new Map<string, ClientHandler>();
  #cvr: CVRSnapshot | undefined;
  #snapshots: Snapshotter | undefined;

  constructor(
    lc: LogContext,
    clientGroupID: string,
    db: PostgresDB,
    replicaDbFile: string,
    versionChanges: Subscription<ReplicaVersionReady>,
    keepaliveMs = DEFAULT_KEEPALIVE_MS,
  ) {
    this.id = clientGroupID;
    this.#lc = lc
      .withContext('component', 'view-syncer')
      .withContext('serviceID', this.id);
    this.#db = db;
    this.#replicaDbFile = replicaDbFile;
    this.#versionChanges = versionChanges;
    this.#keepaliveMs = keepaliveMs;
  }

  async run(): Promise<void> {
    try {
      await this.#lock.withLock(async () => {
        const cvrStore = new CVRStore(this.#lc, this.#db, this.id);
        this.#cvr = await cvrStore.load();
      });

      this.#lc.info?.('view-syncer started', this.#cvr?.version);

      for await (const _ of this.#versionChanges) {
        await this.#lock.withLock(() => {
          if (this.#snapshots === undefined) {
            this.#snapshots = new Snapshotter(this.#lc, this.#replicaDbFile);
          } else {
            this.#snapshots.advance();
          }
          // TODO:
          // - Initialize or update pipelines.
          // - Wait for client sockets to clear (i.e. backpressure).
        });
      }

      this.#cleanup();
    } catch (e) {
      this.#lc.error?.(e);
      this.#cleanup(e);
    } finally {
      this.#lc.info?.('view-syncer stopped');
    }
  }

  // The idleToken is an object associated with an idle timeout function,
  // the latter of which checks the token with identity equality before
  // executing. Setting the #idleToken to a new object or to `null`
  // effectively cancels the previous timeout.
  #idleToken: IdleToken | null = null;

  #startIdleTimer(reason: string) {
    if (this.#idleToken) {
      // Previous timeout is canceled for efficiency
      // (but not necessary for correctness).
      clearTimeout(this.#idleToken?.timeoutID);
      this.#lc.debug?.(`${reason}. resetting idle timer`);
    } else {
      this.#lc.debug?.(`${reason}. starting idle timer`);
    }

    const idleToken: IdleToken = {};
    this.#idleToken = idleToken;

    idleToken.timeoutID = setTimeout(() => {
      // If #idleToken has changed, this timeout is effectively canceled.
      if (this.#idleToken === idleToken) {
        this.#lc.info?.('shutting down after idle timeout');
        this.#versionChanges.cancel(); // Note: #versionChanges.active becomes false.
      }
    }, this.#keepaliveMs);
  }

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
    if (!this.#versionChanges.active) {
      return false;
    }
    if (this.#idleToken) {
      // Resets the idle timer for another `keepaliveMs`.
      this.#startIdleTimer('received keepalive');
    }
    return true;
  }

  #deleteClient(clientID: string, client: ClientHandler): Promise<void> {
    return this.#lock.withLock(() => {
      const c = this.#clients.get(clientID);
      if (c === client) {
        this.#clients.delete(clientID);

        if (this.#clients.size === 0) {
          this.#startIdleTimer('no more clients');
        }
      }
    });
  }

  async initConnection(
    ctx: SyncContext,
    initConnectionMessage: InitConnectionMessage,
  ): Promise<CancelableAsyncIterable<Downstream>> {
    const {clientID, wsID, baseCookie} = ctx;
    const lc = this.#lc
      .withContext('clientID', clientID)
      .withContext('wsID', wsID);
    lc.debug?.('initConnection', initConnectionMessage);

    // Setup the downstream connection.
    const downstream = Subscription.create<Downstream>({
      cleanup: (_, err) => {
        err
          ? lc.error?.(`client closed with error`, err)
          : lc.info?.('client closed');
        void this.#deleteClient(clientID, client);
      },
    });

    const client = new ClientHandler(
      lc,
      this.id,
      clientID,
      wsID,
      baseCookie,
      downstream,
    );

    // Note: It is tempting to try to re-use #runInLockForClient(), but for
    // the initConnection case the client is not yet in the #clients map, and
    // it must be added from within the lock, and only after the desired
    // queries have been processed.
    await this.#lock.withLock(async () => {
      await this.#patchQueries(lc, client, initConnectionMessage[1]);

      // Update #clients, close any previous connection.
      this.#clients.get(clientID)?.close();
      this.#clients.set(clientID, client);

      // Clear and cancel any idle timeout.
      if (this.#idleToken) {
        clearTimeout(this.#idleToken.timeoutID);
        this.#idleToken = null;
      }
    });
    return downstream;
  }

  async changeDesiredQueries(
    ctx: SyncContext,
    msg: ChangeDesiredQueriesMessage,
  ): Promise<void> {
    await this.#runInLockForClient<ChangeDesiredQueriesBody>(
      ctx,
      msg,
      (lc, client, body) => this.#patchQueries(lc, client, body),
    );
  }

  async #runInLockForClient<B, M extends [cmd: string, B] = [string, B]>(
    ctx: SyncContext,
    msg: M,
    fn: (lc: LogContext, client: ClientHandler, body: B) => Promise<void>,
  ): Promise<void> {
    const {clientID, wsID} = ctx;
    const lc = this.#lc
      .withContext('clientID', clientID)
      .withContext('wsID', wsID);

    const [cmd, body] = msg;
    lc.debug?.(cmd, body);

    const client = this.#clients.get(clientID);
    if (client?.wsID !== wsID) {
      // Only respond to messages of the currently connected client.
      // Past connections may have been dropped due to an error, so consider them invalid.
      lc.info?.(`client no longer connected. dropping ${cmd} message`);
      return;
    }

    try {
      await this.#lock.withLock(() => fn(lc, client, body));
    } catch (e) {
      lc.error?.(`closing connection with error`, e);
      client.fail(e);
      throw e;
    }
  }

  // Must be called from within #lock.
  async #patchQueries(
    lc: LogContext,
    client: ClientHandler,
    {desiredQueriesPatch}: ChangeDesiredQueriesBody,
  ) {
    assert(this.#cvr, 'CVR must be loaded before patching queries');

    // Apply patches requested in the initConnectionMessage.
    const {clientID} = client;
    const cvrStore = new CVRStore(this.#lc, this.#db, this.#cvr.id);
    const updater = new CVRConfigDrivenUpdater(cvrStore, this.#cvr);

    const added: {id: string; ast: AST}[] = [];
    for (const patch of desiredQueriesPatch) {
      switch (patch.op) {
        case 'put':
          added.push(
            ...updater.putDesiredQueries(clientID, {[patch.hash]: patch.ast}),
          );
          break;
        case 'del':
          updater.deleteDesiredQueries(clientID, [patch.hash]);
          break;
        case 'clear':
          updater.clearDesiredQueries(clientID);
          break;
      }
    }

    this.#cvr = await updater.flush(lc);

    // TODO: Hydrate / update clients, etc.
  }

  // eslint-disable-next-line require-await
  async stop(): Promise<void> {
    this.#lc.info?.('stopping view syncer');
    this.#versionChanges.cancel();
  }

  #cleanup(err?: unknown) {
    for (const client of this.#clients.values()) {
      if (err) {
        client.fail(err);
      } else {
        client.close();
      }
    }
  }
}
