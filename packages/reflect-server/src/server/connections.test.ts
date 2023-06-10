import {test, expect} from '@jest/globals';
import type {ClientState} from '../types/client-state.js';
import {client, Mocket} from '../util/test-utils.js';
import {closeConnections, getConnections} from './connections.js';

test('closeConnections', () => {
  const clients = new Map([
    client('client1_1', 'user1', 'clientgroup_1', new Mocket()),
    client('client2_1', 'user2', 'clientgroup_1', new Mocket()),
    client('client1_2', 'user1', 'clientgroup_2', new Mocket()),
    client('client3_1', 'user3', 'clientgroup_2', new Mocket()),
  ]);
  const predicate = (clientState: ClientState) =>
    clientState.auth.userID === 'user1';
  closeConnections(clients, predicate);
  expect((clients.get('client1_1')?.socket as Mocket).log).toEqual([['close']]);
  expect((clients.get('client2_1')?.socket as Mocket).log).toEqual([]);
  expect((clients.get('client1_2')?.socket as Mocket).log).toEqual([['close']]);
  expect((clients.get('client3_1')?.socket as Mocket).log).toEqual([]);
});

test('getConnections', () => {
  const clients = new Map([
    client('client1_1', 'user1', 'clientgroup_1', new Mocket()),
    client('client2_1', 'user2', 'clientgroup_1', new Mocket()),
    client('client1_2', 'user1', 'clientgroup_2', new Mocket()),
    client('client3_1', 'user3', 'clientgroup_2', new Mocket()),
  ]);
  const connections = getConnections(clients);
  expect(connections.length).toEqual(4);
  expect(connections).toEqual(
    expect.arrayContaining([
      {userID: 'user1', clientID: 'client1_1'},
      {userID: 'user2', clientID: 'client2_1'},
      {userID: 'user1', clientID: 'client1_2'},
      {userID: 'user3', clientID: 'client3_1'},
    ]),
  );
});
