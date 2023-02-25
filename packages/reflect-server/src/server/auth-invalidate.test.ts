import {test, expect} from '@jest/globals';
import {ErrorKind} from 'reflect-protocol';
import {client, createSilentLogContext, Mocket} from '../util/test-utils.js';
import {handleAuthInvalidate} from './auth-invalidate.js';

function createClientMap() {
  return new Map([
    client('testClientID1_1', 'testUserID1', 'testClientGroup1', new Mocket()),
    client('testClientID1_2', 'testUserID1', 'testClientGroup1', new Mocket()),
    client('testClientID2_1', 'testUserID2', 'testClientGroup1', new Mocket()),
    client('testClientID2_2', 'testUserID2', 'testClientGroup2', new Mocket()),
    client('testClientID3_1', 'testUserID3', 'testClientGroup2', new Mocket()),
  ]);
}

test('without userId closes all connections and sends each an error message', () => {
  const clients = createClientMap();
  handleAuthInvalidate(createSilentLogContext(), clients);
  for (const client of clients.values()) {
    const mocket = client.socket as Mocket;
    expect(mocket.log).toEqual([
      ['send', JSON.stringify(['error', ErrorKind.AuthInvalidated, ''])],
      ['close'],
    ]);
  }
});

test('with userId closes all connections for that userID and sends each an error message', () => {
  const clients = createClientMap();
  handleAuthInvalidate(createSilentLogContext(), clients, 'testUserID2');
  for (const client of clients.values()) {
    const mocket = client.socket as Mocket;
    if (client.userData.userID === 'testUserID2') {
      expect(mocket.log).toEqual([
        ['send', JSON.stringify(['error', ErrorKind.AuthInvalidated, ''])],
        ['close'],
      ]);
    } else {
      expect(mocket.log).toEqual([]);
    }
  }
});
