import {expect, test} from '@jest/globals';
import type {JSONType, Mutation, Version} from 'reflect-protocol';
import type {WriteTransaction} from 'replicache';
import * as s from 'superstruct';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import type {ClientPoke} from '../types/client-poke.js';
import {
  clientRecordKey,
  ClientRecordMap,
  putClientRecord,
} from '../../src/types/client-record.js';
import type {ClientID} from '../../src/types/client-state.js';
import {UserValue, userValueKey} from '../../src/types/user-value.js';
import {versionKey} from '../../src/types/version.js';
import {processFrame} from '../process/process-frame.js';
import {connectedClientsKey} from '../types/connected-clients.js';
import {
  clientRecord,
  createSilentLogContext,
  mockMathRandom,
  mutation,
  userValue,
} from '../util/test-utils.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

mockMathRandom();

test('processFrame', async () => {
  const startVersion = 1;
  const disconnectHandlerWriteKey = (clientID: string) =>
    'test-disconnected-' + clientID;

  type Case = {
    name: string;
    mutations: Mutation[];
    clients: ClientID[];
    clientRecords: ClientRecordMap;
    connectedClients: ClientID[];
    expectedPokes: ClientPoke[];
    expectedUserValues: Map<string, UserValue>;
    expectedClientRecords: ClientRecordMap;
    expectedVersion: Version;
    expectedDisconnectedClients: ClientID[];
    disconnectHandlerThrows: boolean;
  };

  const mutators = new Map(
    Object.entries({
      put: async (
        tx: WriteTransaction,
        {key, value}: {key: string; value: JSONType},
      ) => {
        await tx.put(key, value);
      },
      del: async (tx: WriteTransaction, {key}: {key: string}) => {
        await tx.del(key);
      },
    }),
  );

  const records = new Map([
    ['c1', clientRecord('cg1', null, 1, 1)],
    ['c2', clientRecord('cg1', 1, 7, 1)],
    ['c3', clientRecord('cg2', 1, 7, 1)],
  ]);

  const cases: Case[] = [
    {
      name: 'no mutations, no clients',
      mutations: [],
      clients: [],
      clientRecords: records,
      connectedClients: [],
      expectedPokes: [],
      expectedUserValues: new Map(),
      expectedClientRecords: records,
      expectedVersion: startVersion,
      expectedDisconnectedClients: [],
      disconnectHandlerThrows: false,
    },
    {
      name: 'no mutations, one client',
      mutations: [],
      clients: ['c1'],
      clientRecords: records,
      connectedClients: ['c1'],
      expectedPokes: [],
      expectedUserValues: new Map(),
      expectedClientRecords: records,
      expectedVersion: startVersion,
      expectedDisconnectedClients: [],
      disconnectHandlerThrows: false,
    },
    {
      name: 'one mutation, one client',
      mutations: [mutation('c1', 2, 'put', {key: 'foo', value: 'bar'}, 100)],
      clients: ['c1'],
      clientRecords: records,
      connectedClients: ['c1'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: 100,
          },
        },
      ],
      expectedUserValues: new Map([
        ['foo', userValue('bar', startVersion + 1)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        ['c1', clientRecord('cg1', startVersion + 1, 2, startVersion + 1)],
      ]),
      expectedVersion: startVersion + 1,
      expectedDisconnectedClients: [],
      disconnectHandlerThrows: false,
    },
    {
      name: 'one mutation, two clients',
      mutations: [mutation('c1', 2, 'put', {key: 'foo', value: 'bar'}, 100)],
      clients: ['c1', 'c2'],
      clientRecords: records,
      connectedClients: ['c1', 'c2'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: 100,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: 100,
          },
        },
      ],
      expectedUserValues: new Map([
        ['foo', userValue('bar', startVersion + 1)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        ['c1', clientRecord('cg1', startVersion + 1, 2, startVersion + 1)],
        ['c2', clientRecord('cg1', startVersion + 1, 7, startVersion)],
      ]),
      expectedVersion: startVersion + 1,
      expectedDisconnectedClients: [],
      disconnectHandlerThrows: false,
    },
    {
      name: 'two mutations, three clients, two client groups',
      mutations: [
        mutation('c1', 2, 'put', {key: 'foo', value: 'bar'}, 100),
        mutation('c3', 8, 'put', {key: 'fuzzy', value: 'wuzzy'}, 120),
      ],
      clients: ['c1', 'c2', 'c3'],
      clientRecords: records,
      connectedClients: ['c1', 'c2', 'c3'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: 100,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: 100,
          },
        },
        {
          clientID: 'c3',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: 100,
          },
        },
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion + 1,
            cookie: startVersion + 2,
            lastMutationIDChanges: {},
            patch: [
              {
                op: 'put',
                key: 'fuzzy',
                value: 'wuzzy',
              },
            ],
            timestamp: 120,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion + 1,
            cookie: startVersion + 2,
            lastMutationIDChanges: {},
            patch: [
              {
                op: 'put',
                key: 'fuzzy',
                value: 'wuzzy',
              },
            ],
            timestamp: 120,
          },
        },
        {
          clientID: 'c3',
          poke: {
            baseCookie: startVersion + 1,
            cookie: startVersion + 2,
            lastMutationIDChanges: {c3: 8},
            patch: [
              {
                op: 'put',
                key: 'fuzzy',
                value: 'wuzzy',
              },
            ],
            timestamp: 120,
          },
        },
      ],
      expectedUserValues: new Map([
        ['foo', userValue('bar', startVersion + 1)],
        ['fuzzy', userValue('wuzzy', startVersion + 2)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        ['c1', clientRecord('cg1', startVersion + 2, 2, startVersion + 1)],
        ['c2', clientRecord('cg1', startVersion + 2, 7, startVersion)],
        ['c3', clientRecord('cg2', startVersion + 2, 8, startVersion + 2)],
      ]),
      expectedVersion: startVersion + 2,
      expectedDisconnectedClients: [],
      disconnectHandlerThrows: false,
    },
    {
      name: 'two mutations, one client, one key',
      mutations: [
        mutation('c1', 2, 'put', {key: 'foo', value: 'bar'}, 100),
        mutation('c1', 3, 'put', {key: 'foo', value: 'baz'}, 120),
      ],
      clients: ['c1'],
      clientRecords: records,
      connectedClients: ['c1'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: 100,
          },
        },
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion + 1,
            cookie: startVersion + 2,
            lastMutationIDChanges: {c1: 3},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'baz',
              },
            ],
            timestamp: 120,
          },
        },
      ],
      expectedUserValues: new Map([
        ['foo', userValue('baz', startVersion + 2)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        ['c1', clientRecord('cg1', startVersion + 2, 3, startVersion + 2)],
      ]),
      expectedVersion: startVersion + 2,
      expectedDisconnectedClients: [],
      disconnectHandlerThrows: false,
    },
    {
      name: 'no mutations, no clients, 1 client disconnects',
      mutations: [],
      clients: [],
      clientRecords: records,
      connectedClients: ['c1'],
      expectedPokes: [],
      expectedUserValues: new Map([
        [disconnectHandlerWriteKey('c1'), userValue(true, startVersion + 1)],
      ]),
      expectedClientRecords: records,
      expectedVersion: startVersion + 1,
      expectedDisconnectedClients: ['c1'],
      disconnectHandlerThrows: false,
    },
    {
      name: 'no mutations, no clients, 1 client disconnects, disconnect handler throws',
      mutations: [],
      clients: [],
      clientRecords: records,
      connectedClients: ['c1'],
      // No user values or pokes because only write was in disconnect handler which threw
      expectedPokes: [],
      expectedUserValues: new Map(),
      expectedClientRecords: records,
      // version not incremented for same reason
      expectedVersion: startVersion,
      expectedDisconnectedClients: ['c1'],
      disconnectHandlerThrows: true,
    },
    {
      name: 'no mutations, 1 client, 1 client disconnected',
      mutations: [],
      clients: ['c2'],
      clientRecords: records,
      connectedClients: ['c1', 'c2'],
      expectedPokes: [
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {},
            patch: [
              {
                key: 'test-disconnected-c1',
                op: 'put',
                value: true,
              },
            ],
            timestamp: undefined,
          },
        },
      ],
      expectedUserValues: new Map([
        [disconnectHandlerWriteKey('c1'), userValue(true, startVersion + 1)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        ['c2', clientRecord('cg1', startVersion + 1, 7, startVersion)],
      ]),
      expectedVersion: startVersion + 1,
      expectedDisconnectedClients: ['c1'],
      disconnectHandlerThrows: false,
    },
    {
      name: 'no mutations, 1 client, 2 clients disconnected',
      mutations: [],
      clients: ['c2'],
      clientRecords: records,
      connectedClients: ['c1', 'c2', 'c3'],
      expectedPokes: [
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {},
            patch: [
              {
                key: 'test-disconnected-c1',
                op: 'put',
                value: true,
              },
              {
                key: 'test-disconnected-c3',
                op: 'put',
                value: true,
              },
            ],
            timestamp: undefined,
          },
        },
      ],
      expectedUserValues: new Map([
        [disconnectHandlerWriteKey('c1'), userValue(true, startVersion + 1)],
        [disconnectHandlerWriteKey('c3'), userValue(true, startVersion + 1)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        ['c2', clientRecord('cg1', startVersion + 1, 7, startVersion)],
      ]),
      expectedVersion: startVersion + 1,
      expectedDisconnectedClients: ['c1', 'c3'],
      disconnectHandlerThrows: false,
    },
    {
      name: '1 mutation, 2 clients, 1 client disconnects',
      mutations: [mutation('c1', 2, 'put', {key: 'foo', value: 'bar'}, 100)],
      clients: ['c1', 'c2'],
      clientRecords: records,
      connectedClients: ['c1', 'c2', 'c3'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: 100,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: 100,
          },
        },
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion + 1,
            cookie: startVersion + 2,
            lastMutationIDChanges: {},
            patch: [
              {
                key: 'test-disconnected-c3',
                op: 'put',
                value: true,
              },
            ],
            timestamp: undefined,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion + 1,
            cookie: startVersion + 2,
            lastMutationIDChanges: {},
            patch: [
              {
                key: 'test-disconnected-c3',
                op: 'put',
                value: true,
              },
            ],
            timestamp: undefined,
          },
        },
      ],
      expectedUserValues: new Map([
        ['foo', userValue('bar', startVersion + 1)],
        [disconnectHandlerWriteKey('c3'), userValue(true, startVersion + 2)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        ['c1', clientRecord('cg1', startVersion + 2, 2, startVersion + 1)],
        ['c2', clientRecord('cg1', startVersion + 2, 7, startVersion)],
      ]),
      expectedVersion: startVersion + 2,
      expectedDisconnectedClients: ['c3'],
      disconnectHandlerThrows: false,
    },
  ];

  const durable = await getMiniflareDurableObjectStorage(id);
  for (const c of cases) {
    await durable.deleteAll();
    const storage = new DurableStorage(durable);

    await storage.put(versionKey, startVersion);
    for (const [clientID, record] of c.clientRecords) {
      await putClientRecord(clientID, record, storage);
    }
    await storage.put(connectedClientsKey, c.connectedClients);

    const disconnectCallClients: ClientID[] = [];
    const result = await processFrame(
      createSilentLogContext(),
      c.mutations,
      mutators,
      async write => {
        await write.put(disconnectHandlerWriteKey(write.clientID), true);
        disconnectCallClients.push(write.clientID);
        // Throw after writes to confirm they are not saved.
        if (c.disconnectHandlerThrows) {
          throw new Error('disconnectHandler threw');
        }
      },
      c.clients,
      storage,
    );

    expect(result).toEqual(c.expectedPokes);

    expect(disconnectCallClients.sort()).toEqual(
      c.expectedDisconnectedClients.sort(),
    );

    const expectedState = new Map([
      ...new Map<string, JSONType>(
        [...c.expectedUserValues].map(([key, value]) => [
          userValueKey(key),
          value,
        ]),
      ),
      ...new Map<string, JSONType>(
        [...c.expectedClientRecords].map(([key, value]) => [
          clientRecordKey(key),
          value,
        ]),
      ),
      [versionKey, c.expectedVersion],
      [connectedClientsKey, c.clients],
    ]);

    expect((await durable.list()).size).toEqual(expectedState.size);
    for (const [key, value] of expectedState) {
      expect(await storage.get(key, s.any())).toEqual(value);
    }
  }
});
