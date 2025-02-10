import type {LogContext} from '@rocicorp/logger';
import type {Store} from '../../../replicache/src/dag/store.ts';
import {
  getDeletedClients,
  removeDeletedClients,
} from '../../../replicache/src/deleted-clients.ts';
import type {ClientID} from '../../../replicache/src/sync/ids.ts';
import {
  withRead,
  withWrite,
} from '../../../replicache/src/with-transactions.ts';
import type {DeleteClientsMessage} from '../../../zero-protocol/src/delete-clients.ts';

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

  constructor(
    send: (msg: DeleteClientsMessage) => void,
    dagStore: Store,
    lc: LogContext,
  ) {
    this.#send = send;
    this.#dagStore = dagStore;
    this.#lc = lc;
  }

  /**
   * This gets called by Replicache when it deletes clients from the persistent
   * storage.
   */
  onClientsDeleted(clientIDs: ClientID[]): void {
    this.#lc.debug?.('DeletedClientsManager, send:', clientIDs);
    this.#send(['deleteClients', {clientIDs}]);
  }

  /**
   * Zero calls this after it connects to ensure that the server knows about all
   * the clients that might have been deleted locally since the last connection.
   */
  async sendDeletedClientsToServer(): Promise<void> {
    const deletedClients = await withRead(this.#dagStore, dagRead =>
      getDeletedClients(dagRead),
    );
    if (deletedClients.length > 0) {
      this.#send(['deleteClients', {clientIDs: deletedClients}]);
      this.#lc.debug?.('DeletedClientsManager, send:', deletedClients);
    }
  }

  /**
   * This is called as a response to the server telling us which clients it
   * actually deleted.
   */
  clientsDeletedOnServer(clientIDs: ClientID[]): Promise<void> {
    // Get the deleted clients from the dag and remove the ones from the server.
    // then write them back to the dag.
    return withWrite(this.#dagStore, async dagWrite => {
      this.#lc.debug?.('clientsDeletedOnServer:', clientIDs);
      await removeDeletedClients(dagWrite, clientIDs);
    });
  }

  getDeletedClients(): Promise<ClientID[]> {
    return withRead(this.#dagStore, getDeletedClients);
  }
}
