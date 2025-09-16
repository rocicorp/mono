/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/await-thenable, @typescript-eslint/no-misused-promises, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-floating-promises, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/require-await, @typescript-eslint/no-empty-object-type, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error */
import {beforeEach, expect, test, vi, type Mock} from 'vitest';
import type {Store} from '../../../replicache/src/dag/store.ts';
import {TestStore} from '../../../replicache/src/dag/test-store.ts';
import {
  getDeletedClients,
  setDeletedClients,
} from '../../../replicache/src/deleted-clients.ts';
import {
  withRead,
  withWrite,
} from '../../../replicache/src/with-transactions.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {DeleteClientsMessage} from '../../../zero-protocol/src/delete-clients.ts';
import {DeleteClientsManager} from './delete-clients-manager.ts';

let send: Mock<(msg: DeleteClientsMessage) => void>;
let dagStore: Store;
const lc = createSilentLogContext();
let manager: DeleteClientsManager;
const clientGroupID = 'cg1';

beforeEach(() => {
  send = vi.fn<(msg: DeleteClientsMessage) => void>();
  dagStore = new TestStore();
  manager = new DeleteClientsManager(
    send,
    dagStore,
    lc,
    Promise.resolve(clientGroupID),
  );
  return async () => {
    await dagStore.close();
  };
});

test('onClientsDeleted', async () => {
  await manager.onClientsDeleted([
    {clientGroupID, clientID: 'a'},
    {clientGroupID, clientID: 'b'},
  ]);
  expect(send).toBeCalledWith(['deleteClients', {clientIDs: ['a', 'b']}]);
});

test('clientsDeletedOnServer', async () => {
  const cg = clientGroupID;
  await withWrite(dagStore, dagWrite =>
    setDeletedClients(dagWrite, [
      {clientGroupID: cg, clientID: 'c'},
      {clientGroupID: cg, clientID: 'd'},
      {clientGroupID: cg, clientID: 'e'},
    ]),
  );
  await manager.clientsDeletedOnServer({clientIDs: ['c', 'd']});
  expect(await withRead(dagStore, getDeletedClients)).toEqual([
    {clientGroupID: cg, clientID: 'e'},
  ]);
});
