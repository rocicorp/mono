import {resolver} from '@rocicorp/resolver';
import {getBrowserGlobal} from '../../../shared/src/browser-env.ts';

const lockKeyPrefix = 'zero-alive';

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

const lockManager = getBrowserGlobal('navigator')?.locks;

/**
 * A class that lists the alive clients in a client group. It uses the
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
export class AliveClientsManager {
  readonly clientGroupID: string;
  readonly clientID: string;
  readonly #resolver = resolver<void>();
  #closed = false;

  constructor(clientGroupID: string, clientID: string) {
    this.clientGroupID = clientGroupID;
    this.clientID = clientID;

    void lockManager?.request(
      toLockKey(clientGroupID, clientID),
      () => this.#resolver.promise,
    );
  }

  async getAliveClients(): Promise<Set<string>> {
    const aliveClients: Set<string> = new Set([this.clientID]);
    if (!lockManager) {
      return aliveClients;
    }

    const snapshot = await lockManager.query();
    const add = (info: LockInfo[] | undefined) => {
      for (const lock of info ?? []) {
        const client = fromLockKey(lock.name);
        if (client?.clientGroupID === this.clientGroupID) {
          aliveClients.add(client.clientID);
        }
      }
    };

    add(snapshot.held);
    add(snapshot.pending);
    return aliveClients;
  }

  close(): void {
    this.#resolver.resolve();
    this.#closed = true;
  }

  get closed(): boolean {
    return this.#closed;
  }
}
