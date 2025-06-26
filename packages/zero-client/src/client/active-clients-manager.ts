import {resolver} from '@rocicorp/resolver';
import {BroadcastChannel} from '../../../shared/src/broadcast-channel.ts';
import {getBrowserGlobal} from '../../../shared/src/browser-env.ts';
import type {MaybePromise} from '../../../shared/src/types.ts';

/**
 * The prefix for the keys used for the locks and the broadcast channels.
 */
const keyPrefix = 'zero-active';

function toLockName(clientGroupID: string, clientID: string): string {
  return `${keyPrefix}/${clientGroupID}/${clientID}`;
}

function toBroadcastChannelName(clientGroupID: string): string {
  return `${keyPrefix}/${clientGroupID}`;
}

function fromLockName(
  lockKey: string | undefined,
): {clientGroupID: string; clientID: string} | undefined {
  if (!lockKey || !lockKey.startsWith(keyPrefix)) {
    return undefined;
  }
  const parts = lockKey.slice(keyPrefix.length).split('/');
  if (parts.length !== 3) {
    return undefined;
  }
  return {
    clientGroupID: parts[1],
    clientID: parts[2],
  };
}

function ignoreAbortError(e: unknown) {
  if (e instanceof Error && e.name === 'AbortError') {
    // Ignore the AbortError, it is expected when the signal is aborted.
    return;
  }
  throw e;
}

/**
 *  When we do not have the `navigator.locks` API available, we will keep track
 *  of the "locks" in memory.
 */
class AllMockLocks extends Set<{
  name: string;
  mode: 'exclusive';
}> {
  #listeners: Set<(clientGroupID: string, clientID: string) => void> =
    new Set();

  addListener(
    listener: (clientGroupID: string, clientID: string) => void,
  ): () => void {
    this.#listeners.add(listener);

    return () => {
      this.#listeners.delete(listener);
    };
  }

  delete(item: {name: string; mode: 'exclusive'}): boolean {
    const removed = super.delete(item);
    if (removed) {
      const client = fromLockName(item.name);
      if (client) {
        for (const listener of this.#listeners) {
          listener(client.clientGroupID, client.clientID);
        }
      }
    }
    return removed;
  }
}

const USE = false;

const allMockLocks = new AllMockLocks();

/**
 * A class that lists the active clients in a client group. It uses the
 * `navigator.locks` API to manage locks for each client. The class is designed
 * to be used in a browser environment where the `navigator.locks` API is
 * available.
 *
 * When navigator.locks is not available, it will return a set only containing
 * the clients in the current scripting context (window, worker, etc).
 *
 * It uses one exclusive lock per client, identified by a combination of
 * `clientGroupID` and `clientID`. Then the `query` method is used to get the
 * list of all clients that hold or are waiting for locks in the same client
 * group.
 *
 * It also tries to get a shared lock for each client in the group, so that it
 * can be notified when the exclusive lock is released. This allows the class to
 * keep track of the active clients in the group and notify when an existing
 * client is removed.
 *
 * The class also uses a `BroadcastChannel` to notify other clients in the
 * same client group when a new client is added. This allows the class to keep
 * track of the active clients in the group and notify when a new client is
 * added.
 */
export class ActiveClientsManager {
  readonly clientGroupID: string;
  readonly clientID: string;
  readonly #resolver = resolver<void>();
  readonly #lockManager = getBrowserGlobal('navigator')?.locks;
  readonly #signal: AbortSignal;
  readonly #unlisteners: (() => void)[] = [];
  readonly #clientLockManager: ClientLockManager;
  #activeClients: Set<string> = new Set();

  /**
   * A callback that is called when the list of active clients changes. It
   * receives a `Set` of client IDs that are currently active in the client group.
   */
  onChange: ((activeClients: ReadonlySet<string>) => void) | undefined;

  constructor(clientGroupID: string, clientID: string, signal: AbortSignal) {
    this.clientGroupID = clientGroupID;
    this.clientID = clientID;
    this.#signal = signal;
    this.#activeClients.add(clientID);

    this.#clientLockManager = new NativeClientLockManager(
      this.#lockManager!,
      signal,
    );

    const name = toLockName(clientGroupID, clientID);

    // The BroadcastChannel is used to notify other clients in the same client
    // group when a new client is added. It listens for messages that contain
    // the lock name, which is used to identify the client. When a message is
    // received, it checks if the client belongs to the same client group and
    // adds it to the list of active clients. It also adds a shared lock for
    // the client, so that it can be notified when the exclusive lock is
    // released.
    const channel = new BroadcastChannel(toBroadcastChannelName(clientGroupID));
    channel.addEventListener(
      'message',
      e => {
        const client = fromLockName(e.data);
        if (client?.clientGroupID === this.clientGroupID) {
          this.#addClient(client.clientID);
          this.#notifyClientActivated(client.clientID);
        }
      },
      {signal},
    );

    let mockLock: {name: string; mode: 'exclusive'};

    if (USE) {
      this.#clientLockManager
        .request(name, 'exclusive', () => this.#resolver.promise)
        .catch(ignoreAbortError);
    } else {
      if (this.#lockManager) {
        this.#lockManager
          .request(name, {signal}, async () => {
            await this.#resolver.promise;
          })
          .catch(ignoreAbortError);
      } else {
        mockLock = {name, mode: 'exclusive'};
        allMockLocks.add(mockLock);
      }
    }

    signal.addEventListener(
      'abort',
      () => {
        if (USE) {
          void this.#clientLockManager.release(name, () =>
            this.#resolver.resolve(),
          );
        } else {
          if (!this.#lockManager) {
            for (const unlisten of this.#unlisteners) {
              unlisten();
            }
            allMockLocks.delete(mockLock);
            this.#unlisteners.length = 0;
          }
          this.#resolver.resolve();
        }
      },
      {once: true},
    );

    void this.getActiveClients().then(activeClients => {
      for (const clientID of activeClients) {
        if (clientID !== this.clientID) {
          this.#addClient(clientID);
        }
      }

      this.#activeClients = activeClients;
      if (this.#activeClients.size > 1) {
        // One for the current client, so if there are more than one, we notify
        // that the list of active clients has changed.
        this.#onChange();
      }

      channel.postMessage(name);
    });
  }

  async getActiveClients(): Promise<Set<string>> {
    const activeClients: Set<string> = new Set();

    if (USE) {
      for await (const lockName of this.#clientLockManager.queryExclusive()) {
        const client = fromLockName(lockName);
        if (client?.clientGroupID === this.clientGroupID) {
          activeClients.add(client.clientID);
        }
      }
    } else {
      const add = (info: Iterable<LockInfo> | undefined) => {
        for (const lock of info ?? []) {
          if (lock.mode === 'exclusive') {
            const client = fromLockName(lock.name);
            if (client?.clientGroupID === this.clientGroupID) {
              activeClients.add(client.clientID);
            }
          }
        }
      };

      if (!this.#lockManager) {
        add(allMockLocks);
      } else {
        const snapshot = await this.#lockManager.query();
        add(snapshot.held);
        add(snapshot.pending);
      }
    }
    return activeClients;
  }

  /**
   * This gets called when a new client is added to the client group.
   *
   * It will request a shared lock for the client, and when the exclusive lock
   * is released, it will notify that the client has been deactivated.
   */
  #addClient(clientID: string): void {
    const name = toLockName(this.clientGroupID, clientID);
    if (USE) {
      this.#clientLockManager
        .request(name, 'shared', () => this.#notifyClientInactivated(clientID))
        .catch(ignoreAbortError);
    } else {
      if (this.#lockManager) {
        this.#lockManager
          .request(name, {mode: 'shared', signal: this.#signal}, () =>
            this.#notifyClientInactivated(clientID),
          )
          .catch(ignoreAbortError);
      } else {
        // For the mock locks we will add a listener that will notify us when the
        // lock is deleted from the `allMockLocks` set.
        const listener = (clientGroupID: string, clientID2: string) => {
          if (
            clientID === clientID2 &&
            clientGroupID === this.clientGroupID &&
            clientID2 !== this.clientID
          ) {
            unlisten();
            this.#notifyClientInactivated(clientID);
          }
        };
        const unlisten = allMockLocks.addListener(listener);
        this.#unlisteners.push(unlisten);
      }
    }
  }

  #notifyClientInactivated(clientID: string) {
    const removed = this.#activeClients.delete(clientID);
    if (removed) {
      this.#onChange();
    }
  }

  #onChange() {
    // This method is called when the list of active clients changes.
    this.onChange?.(this.#activeClients);
  }

  #notifyClientActivated(clientID: string) {
    if (this.#activeClients.has(clientID)) {
      return;
    }
    this.#activeClients.add(clientID);
    this.#onChange();
  }
}

interface ClientLockManager {
  request(
    name: string,
    mode: 'exclusive' | 'shared',
    fn: () => MaybePromise<void>,
  ): Promise<void>;
  release(name: string, fn: () => void): void;
  queryExclusive(): AsyncIterable<string>;
}

class NativeClientLockManager implements ClientLockManager {
  readonly #locks: LockManager;
  readonly #signal: AbortSignal;

  constructor(locks: LockManager, signal: AbortSignal) {
    this.#locks = locks;
    this.#signal = signal;
  }

  request(
    name: string,
    mode: 'exclusive' | 'shared',
    fn: () => Promise<void>,
  ): Promise<void> {
    return this.#locks.request(name, {mode, signal: this.#signal}, fn);
  }

  release(_name: string, fn: () => void): void {
    fn();
  }

  async *queryExclusive(): AsyncIterable<string> {
    const snapshot = await this.#locks.query();
    for (const lock of [
      ...(snapshot.held ?? []),
      ...(snapshot.pending ?? []),
    ]) {
      if (lock.mode === 'exclusive' && lock.name) {
        yield lock.name;
      }
    }
  }
}

const mockLockNames = new Set<string>();

const mockListeners: Set<(name: string) => void> = new Set();

class MockClientLockManager implements ClientLockManager {
  readonly #listeners: Set<(name: string) => void> = new Set();

  request(
    name: string,
    mode: 'exclusive' | 'shared',
    fn: () => void | Promise<void>,
  ): Promise<void> {
    if (mode === 'exclusive') {
      mockLockNames.add(name);
    } else {
      mode satisfies 'shared';

      // For the mock locks we will add a listener that will notify us when the
      // lock is deleted from the `allMockLocks` set.
      const listener = (removed: string) => {
        if (removed === name) {
          mockListeners.delete(listener);
          return fn();
        }
      };
      mockListeners.add(listener);
      this.#listeners.add(listener);
    }
    return Promise.resolve();
  }

  release(name: string, fn: () => void): void {
    mockLockNames.delete(name);
    for (const listener of mockListeners) {
      listener(name);
    }
    fn();
  }

  async *queryExclusive(): AsyncIterable<string> {
    for (const lock of allMockLocks) {
      yield lock.name;
    }
  }
}
