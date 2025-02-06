import {resolver} from '@rocicorp/resolver';
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
import {promiseNever} from '../../../shared/src/resolved-promises.ts';
import type {DeleteClientsMessage} from '../../../zero-protocol/src/delete-clients.ts';
import {
  DELAY_SEND_AFTER_CONNECT,
  DeleteClientsManager,
} from './delete-clients-manager.ts';

let send: Mock<(msg: DeleteClientsMessage) => void>;
let dagStore: Store;
const lc = createSilentLogContext();
let manager: DeleteClientsManager;

beforeEach(() => {
  vi.useFakeTimers();
  send = vi.fn<(msg: DeleteClientsMessage) => void>();
  dagStore = new TestStore();
  manager = new DeleteClientsManager(() => promiseNever, send, dagStore, lc);
  return async () => {
    await dagStore.close();
    vi.restoreAllMocks();
  };
});

test('onClientsDeleted', async () => {
  await manager.onClientsDeleted(['a', 'b']);
  expect(send).toBeCalledWith(['deleteClients', {clientIDs: ['a', 'b']}]);
  expect(await withRead(dagStore, getDeletedClients)).toEqual(['a', 'b']);
});

test('sendDeletedClientsToServer', async () => {
  // sends whatever is in the store
  await withWrite(dagStore, dagWrite =>
    setDeletedClients(dagWrite, ['c', 'd', 'e']),
  );
  await manager.sendDeletedClientsToServer();
  expect(send).toBeCalledWith(['deleteClients', {clientIDs: ['c', 'd', 'e']}]);
});

test('clientsDeletedOnServer', async () => {
  await withWrite(dagStore, dagWrite =>
    setDeletedClients(dagWrite, ['c', 'd', 'e']),
  );
  await manager.clientsDeletedOnServer(['c', 'd']);
  expect(await withRead(dagStore, getDeletedClients)).toEqual(['e']);
});

test('send deleted clients on connect after a delay', async () => {
  let {promise, resolve} = resolver<void>();
  const manager = new DeleteClientsManager(() => promise, send, dagStore, lc);
  await withWrite(dagStore, dagWrite =>
    setDeletedClients(dagWrite, ['a', 'b']),
  );

  expect(send).not.toBeCalled();
  await vi.advanceTimersByTimeAsync(5000);
  expect(send).not.toBeCalled();
  resolve();
  expect(send).not.toBeCalled();
  await vi.advanceTimersByTimeAsync(DELAY_SEND_AFTER_CONNECT - 1);
  expect(send).not.toBeCalled();
  await vi.advanceTimersByTimeAsync(1);
  expect(send).toBeCalledWith(['deleteClients', {clientIDs: ['a', 'b']}]);

  // Disconnect and reconnect
  await withWrite(dagStore, dagWrite =>
    setDeletedClients(dagWrite, ['c', 'd']),
  );
  send.mockClear();
  ({promise, resolve} = resolver<void>());
  manager.handleDisconnect();
  await vi.advanceTimersByTimeAsync(5000);
  expect(send).not.toBeCalled();
  resolve();
  expect(send).not.toBeCalled();
  await vi.advanceTimersByTimeAsync(DELAY_SEND_AFTER_CONNECT - 1);
  expect(send).not.toBeCalled();
  await vi.advanceTimersByTimeAsync(1);
  expect(send).toBeCalledWith(['deleteClients', {clientIDs: ['c', 'd']}]);
});
