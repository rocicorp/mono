import type {LogContext} from '@rocicorp/logger';
import type {Store} from '../../../replicache/src/dag/store.ts';
import {
  confirmDeletedClients,
  getDeletedClients,
  type DeletedClients,
} from '../../../replicache/src/deleted-clients.ts';
import type {ClientGroupID} from '../../../replicache/src/sync/ids.ts';
import {
  withRead,
  withWrite,
} from '../../../replicache/src/with-transactions.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import type {
  DeleteClientsBody,
  DeleteClientsMessage,
} from '../../../zero-protocol/src/delete-clients.ts';

function filterAndAssert(
  deletedClients: DeletedClients,
  clientGroupID: ClientGroupID,
  clientID: string,
  caller: string,
): string[] {
  const clientIDs = deletedClients
    .filter(dc => dc.clientGroupID === clientGroupID)
    .map(dc => dc.clientID);
  for (const cid of clientIDs) {
    assert(cid !== clientID, `cannot delete self in ${caller}`);
  }
  return clientIDs;
}

/**
 * Replicache will tell us when it deletes clients from the persistent storage
 * due to GC. When this happens we tell the server about the deleted clients.
 * Replicache also store the deleted clients in IDB in case the server is
 * currently offline.
 *
 * The server will reply with the client it actually deleted. When we get that
 * we remove those IDs from our local storage.
 */
export class DeleteClientsManager {
  readonly #send: (msg: DeleteClientsMessage) => void;
  readonly #lc: LogContext;
  readonly #dagStore: Store;
  readonly #clientGroupID: Promise<ClientGroupID>;
  readonly #clientID: string;

  constructor(
    send: (msg: DeleteClientsMessage) => void,
    dagStore: Store,
    lc: LogContext,
    clientGroupID: Promise<ClientGroupID>,
    clientID: string,
  ) {
    this.#send = send;
    this.#dagStore = dagStore;
    this.#lc = lc;
    this.#clientGroupID = clientGroupID;
    this.#clientID = clientID;
  }

  /**
   * This gets called by Replicache when it deletes clients from the persistent
   * storage.
   */
  async onClientsDeleted(deletedClients: DeletedClients): Promise<void> {
    this.#lc.debug?.('DeletedClientsManager, send:', deletedClients);
    const clientGroupID = await this.#clientGroupID;
    const clientIDs = filterAndAssert(
      deletedClients,
      clientGroupID,
      this.#clientID,
      'onClientsDeleted',
    );
    this.#send([
      'deleteClients',
      {
        clientIDs,
      },
    ]);
  }

  /**
   * Zero calls this after it connects to ensure that the server knows about all
   * the clients that might have been deleted locally since the last connection.
   */
  async sendDeletedClientsToServer(): Promise<void> {
    const clientGroupID = await this.#clientGroupID;
    const deleted = await withRead(this.#dagStore, dagRead =>
      getDeletedClients(dagRead),
    );

    const clientIDs = filterAndAssert(
      deleted,
      clientGroupID,
      this.#clientID,
      'sendDeletedClientsToServer',
    );

    if (clientIDs.length > 0) {
      this.#send(['deleteClients', {clientIDs}]);
      this.#lc.debug?.('DeletedClientsManager, send:', deleted);
    }
  }

  /**
   * This is called as a response to the server telling us which clients it
   * actually deleted.
   */
  clientsDeletedOnServer(deletedClients: DeleteClientsBody): Promise<void> {
    const {clientIDs = [], clientGroupIDs = []} = deletedClients;
    if (clientIDs.length > 0 || clientGroupIDs.length > 0) {
      // Get the deleted clients from the dag and remove the ones from the server.
      // then write them back to the dag.
      return withWrite(this.#dagStore, async dagWrite => {
        this.#lc.debug?.('clientsDeletedOnServer:', clientIDs, clientGroupIDs);
        await confirmDeletedClients(dagWrite, clientIDs, clientGroupIDs);
      });
    }
    return promiseVoid;
  }

  async getDeletedClients(): Promise<DeletedClients> {
    const deletedClients = await withRead(this.#dagStore, read =>
      getDeletedClients(read),
    );
    const clientGroupID = await this.#clientGroupID;
    filterAndAssert(
      deletedClients,
      clientGroupID,
      this.#clientID,
      'getDeletedClients',
    );
    return deletedClients.filter(d => d.clientGroupID === clientGroupID);
  }
}
