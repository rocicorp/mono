import {test, expect} from '@jest/globals';
import {must} from 'shared/must.js';
import type {ClientMap} from '../types/client-state.js';
import {client, createSilentLogContext, Mocket} from '../util/test-utils.js';
import {handleClose} from './close.js';

test('handleClose deletes client map entry for client id if socket matches', () => {
  const lc = createSilentLogContext();
  const client1ID = 'clientID1';
  const client2ID = 'clientID2';
  const clientMap: ClientMap = new Map([
    client(client1ID, 'userID1', 'cg1'),
    client(client2ID, 'userID2', 'cg1'),
  ]);
  const expectedClientMap = new Map(clientMap.entries());
  // handleClose will delete
  expectedClientMap.delete(client1ID);
  handleClose(lc, clientMap, client1ID, must(clientMap.get(client1ID)?.socket));
  expect(clientMap).toEqual(expectedClientMap);
});

test('handleClose does not delete client map entry for client id if socket does not match', () => {
  const lc = createSilentLogContext();
  const client1ID = 'clientID1';
  const client2ID = 'clientID2';
  const clientMap: ClientMap = new Map([
    client(client1ID, 'userID1', 'cg1'),
    client(client2ID, 'userID2', 'cg1'),
  ]);
  const expectedClientMap = new Map(clientMap.entries());
  handleClose(lc, clientMap, client1ID, new Mocket());
  expect(clientMap).toEqual(expectedClientMap);
});
