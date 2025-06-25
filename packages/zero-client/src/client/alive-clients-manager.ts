import {resolver} from '@rocicorp/resolver';
import {BroadcastChannel} from '../../../shared/src/broadcast-channel.ts';
import {getBrowserGlobal} from '../../../shared/src/browser-env.ts';

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

const allMockLocks = new AllMockLocks();

/**
 * A class that lists the active clients in a client group. It uses the
 * `navigator.locks` API to manage locks for each client. The class is designed
 * to be used in a browser environment where the `navigator.locks` API is
 * available.
 *
 * When navigator.locks is not available, it will return a set only containing
 * the current clientID.
 *
 * It uses one lock per client, identified by a combination of `clientGroupID`
 * and `clientID`. Then the `query` method is used to get the list of all
 * clients that hold or are waiting for locks in the same client group.
 */
export class ActiveClientsManager {
  readonly clientGroupID: string;
  readonly clientID: string;
  readonly #resolver = resolver<void>();
  readonly #lockManager = getBrowserGlobal('navigator')?.locks;
  readonly #signal: AbortSignal;
  readonly #unlisteners: (() => void)[] = [];
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

    const name = toLockName(clientGroupID, clientID);

    const channel = new BroadcastChannel(toBroadcastChannelName(clientGroupID));
    channel.addEventListener(
      'message',
      e => {
        const client = fromLockName(e.data);
        if (client?.clientGroupID === this.clientGroupID) {
          this.#addListener(client.clientID);
          this.#notifyClientActivated(client.clientID);
        }
      },
      {signal},
    );

    let mockLock: {name: string; mode: 'exclusive'};

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

    signal.addEventListener(
      'abort',
      () => {
        if (!this.#lockManager) {
          allMockLocks.delete(mockLock);
          for (const unlisten of this.#unlisteners) {
            unlisten();
          }
          this.#unlisteners.length = 0;
        }
        this.#resolver.resolve();
      },
      {once: true},
    );

    void this.getActiveClients().then(activeClients => {
      for (const clientID of activeClients) {
        if (clientID !== this.clientID) {
          this.#addListener(clientID);
        }
      }

      // We will add the current client to the set
      this.#activeClients = activeClients;
      if (this.#activeClients.size > 1) {
        // If there are other clients, we will add the current client to the
        // set of active clients.
        this.#onChange();
      }

      channel.postMessage(name);
    });
  }

  async getActiveClients(): Promise<Set<string>> {
    const activeClients: Set<string> = new Set();

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
    return activeClients;
  }

  #addListener(clientID: string): void {
    const name = toLockName(this.clientGroupID, clientID);
    if (this.#lockManager) {
      this.#lockManager
        .request(name, {mode: 'shared', signal: this.#signal}, () => {
          // This callback is called when the exclusive lock is released and we
          // can get the shared lock.
          this.#notifyClientInactivated(clientID);
        })
        .catch(ignoreAbortError);
    } else {
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

  #notifyClientInactivated(clientID: string) {
    const removed = this.#activeClients.delete(clientID);
    if (removed) {
      this.#onChange();
    }
  }

  #onChange() {
    // This method is called when the list of active clients changes.
    // It will notify the listeners that the list has changed.
    this.onChange?.(this.#activeClients);
  }

  #notifyClientActivated(clientID: string) {
    if (this.#activeClients.has(clientID)) {
      // If the client is already in the set, we do not need to add it again.
      return;
    }
    this.#activeClients.add(clientID);
    this.#onChange();
  }
}
