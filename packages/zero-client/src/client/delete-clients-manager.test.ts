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
const clientID = 'current-client-id';

beforeEach(() => {
  send = vi.fn<(msg: DeleteClientsMessage) => void>();
  dagStore = new TestStore();
  manager = new DeleteClientsManager(
    send,
    dagStore,
    lc,
    Promise.resolve(clientGroupID),
    clientID,
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

test('onClientsDeleted asserts if trying to delete self', async () => {
  await expect(
    manager.onClientsDeleted([
      {clientGroupID, clientID: 'a'},
      {clientGroupID, clientID},
    ]),
  ).rejects.toThrow('cannot delete self in onClientsDeleted');
});

test('sendDeletedClientsToServer asserts if trying to delete self', async () => {
  await withWrite(dagStore, dagWrite =>
    setDeletedClients(dagWrite, [
      {clientGroupID, clientID: 'a'},
      {clientGroupID, clientID},
    ]),
  );
  await expect(manager.sendDeletedClientsToServer()).rejects.toThrow(
    'cannot delete self in sendDeletedClientsToServer',
  );
});

test('getDeletedClients asserts if trying to delete self', async () => {
  await withWrite(dagStore, dagWrite =>
    setDeletedClients(dagWrite, [
      {clientGroupID, clientID: 'a'},
      {clientGroupID, clientID},
    ]),
  );
  await expect(manager.getDeletedClients()).rejects.toThrow(
    'cannot delete self in getDeletedClients',
  );
});

test('getDeletedClients returns deleted clients for current client group', async () => {
  await withWrite(dagStore, dagWrite =>
    setDeletedClients(dagWrite, [
      {clientGroupID, clientID: 'a'},
      {clientGroupID, clientID: 'b'},
      {clientGroupID: 'other-cg', clientID: 'c'},
    ]),
  );
  const deleted = await manager.getDeletedClients();
  expect(deleted).toEqual([
    {clientGroupID, clientID: 'a'},
    {clientGroupID, clientID: 'b'},
  ]);
});

test('sendDeletedClientsToServer sends only clients from current client group', async () => {
  await withWrite(dagStore, dagWrite =>
    setDeletedClients(dagWrite, [
      {clientGroupID, clientID: 'a'},
      {clientGroupID: 'other-cg', clientID: 'b'},
    ]),
  );
  await manager.sendDeletedClientsToServer();
  expect(send).toBeCalledWith(['deleteClients', {clientIDs: ['a']}]);
});

test('sendDeletedClientsToServer does not send if no deleted clients', async () => {
  await manager.sendDeletedClientsToServer();
  expect(send).not.toBeCalled();
});

test('onClientsDeleted filters by client group', async () => {
  await manager.onClientsDeleted([
    {clientGroupID, clientID: 'a'},
    {clientGroupID: 'other-cg', clientID: 'b'},
    {clientGroupID, clientID: 'c'},
  ]);
  expect(send).toBeCalledWith(['deleteClients', {clientIDs: ['a', 'c']}]);
});
