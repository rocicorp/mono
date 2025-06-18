import {resolver} from '@rocicorp/resolver';
import {getBrowserGlobal} from '../../../shared/src/browser-env.ts';

const lockKeyPrefix = 'zero-active-clients';

function toLockKey(clientGroupID: string, clientID: string): string {
  return `${lockKeyPrefix}/${clientGroupID}/${clientID}`;
}

function fromLockKey(
  lockKey: string | undefined,
): {clientGroupID: string; clientID: string} | undefined {
  if (!lockKey || !lockKey.startsWith(lockKeyPrefix)) {
    return undefined;
  }
  const parts = lockKey.slice(lockKeyPrefix.length).split('/');
  if (parts.length !== 3) {
    return undefined;
  }
  return {
    clientGroupID: parts[1],
    clientID: parts[2],
  };
}

// When we do not have the `navigator.locks` API available, we will keep track
// of the "locks" in memory.
const allMockLocks = new Set<{name: string}>();

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

  constructor(clientGroupID: string, clientID: string, signal: AbortSignal) {
    this.clientGroupID = clientGroupID;
    this.clientID = clientID;

    const name = toLockKey(clientGroupID, clientID);
    let mockLock: {name: string};

    if (this.#lockManager) {
      this.#lockManager
        .request(name, {signal}, () => this.#resolver.promise)
        .catch(e => {
          if (e.name !== 'AbortError') {
            throw e;
          }
        });
    } else {
      mockLock = {name};
      allMockLocks.add(mockLock);
    }

    signal.addEventListener(
      'abort',
      () => {
        if (!this.#lockManager) {
          allMockLocks.delete(mockLock);
        }
        this.#resolver.resolve();
      },
      {once: true},
    );
  }

  async getActiveClients(): Promise<Set<string>> {
    const activeClients: Set<string> = new Set();

    const add = (info: Iterable<{name?: string}> | undefined) => {
      for (const lock of info ?? []) {
        const client = fromLockKey(lock.name);
        if (client?.clientGroupID === this.clientGroupID) {
          activeClients.add(client.clientID);
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
}
