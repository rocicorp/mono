import {test, expect} from '@jest/globals';
import type {ClientMap} from '../types/client-state.js';
import {Mocket} from '../util/test-utils.js';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {handleClose} from './close.js';

test('handleClose deletes client map entry for client id if socket matches', () => {
  const lc = createSilentLogContext();
  const client1ID = 'clientID1';
  const client1Socket = new Mocket();
  const client2ID = 'clientID2';
  const client2Socket = new Mocket();
  const clientMap: ClientMap = new Map(
    Object.entries({
      [client1ID]: {
        clientGroupID: 'cg1',
        socket: client1Socket,
        auth: {userID: 'userID1'},
        clockOffsetMs: 1000,
        debugPerf: false,
        sentInitialPresence: false,
      },
      [client2ID]: {
        clientGroupID: 'cg1',
        socket: client2Socket,
        auth: {userID: 'userID2'},
        clockOffsetMs: 2000,
        debugPerf: false,
        sentInitialPresence: true,
      },
    }),
  );
  handleClose(lc, clientMap, client1ID, client1Socket);
  // was deleted
  expect(clientMap.get(client1ID)).toBeUndefined();
  expect(clientMap.get(client2ID)).toEqual({
    clientGroupID: 'cg1',
    socket: client2Socket,
    auth: {userID: 'userID2'},
    clockOffsetMs: 2000,
    debugPerf: false,
    sentInitialPresence: true,
  });
});

test('handleClose does not delete client map entry for client id if socket does not match', () => {
  const lc = createSilentLogContext();
  const client1ID = 'clientID1';
  const client1Socket1 = new Mocket();
  const client1Socket2 = new Mocket();
  const client2ID = 'clientID2';
  const client2Socket = new Mocket();
  const clientMap: ClientMap = new Map(
    Object.entries({
      [client1ID]: {
        clientGroupID: 'cg1',
        socket: client1Socket2,
        auth: {userID: 'userID1'},
        clockOffsetMs: 1000,
        debugPerf: false,
        sentInitialPresence: false,
      },
      [client2ID]: {
        clientGroupID: 'cg1',
        socket: client2Socket,
        auth: {userID: 'userID2'},
        clockOffsetMs: 2000,
        debugPerf: false,
        sentInitialPresence: true,
      },
    }),
  );
  handleClose(lc, clientMap, client1ID, client1Socket1);
  expect(clientMap.get(client1ID)).toEqual({
    clientGroupID: 'cg1',
    socket: client1Socket2,
    auth: {userID: 'userID1'},
    clockOffsetMs: 1000,
    debugPerf: false,
    sentInitialPresence: false,
  });
  expect(clientMap.get(client2ID)).toEqual({
    clientGroupID: 'cg1',
    socket: client2Socket,
    auth: {userID: 'userID2'},
    clockOffsetMs: 2000,
    debugPerf: false,
    sentInitialPresence: true,
  });
});
