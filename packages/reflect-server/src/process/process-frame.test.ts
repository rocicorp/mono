import {test, expect} from '@jest/globals';
import * as s from 'superstruct';
import type {WriteTransaction} from 'replicache';
import type {JSONType} from '../../src/protocol/json.js';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import type {ClientPokeBody} from '../../src/types/client-poke-body.js';
import {
  clientRecordKey,
  ClientRecordMap,
  putClientRecord,
} from '../../src/types/client-record.js';
import type {ClientID} from '../../src/types/client-state.js';
import {UserValue, userValueKey} from '../../src/types/user-value.js';
import {Version, versionKey} from '../../src/types/version.js';
import {
  mutation,
  clientRecord,
  createSilentLogContext,
  userValue,
} from '../util/test-utils.js';
import {processFrame} from '../process/process-frame.js';
import {connectedClientsKey} from '../types/connected-clients.js';
import type {Mutation} from '../protocol/push.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

test('processFrame', async () => {
  const startTime = 100;
  const startVersion = 1;
  const endVersion = 2;
  const disconnectHandlerWriteKey = (clientID: string) =>
    'test-disconnected-' + clientID;

  type Case = {
    name: string;
    mutations: Mutation[];
    clients: ClientID[];
    clientRecords: ClientRecordMap;
    connectedClients: ClientID[];
    expectedPokes: ClientPokeBody[];
    expectedUserValues: Map<string, UserValue>;
    expectedClientRecords: ClientRecordMap;
    expectedVersion: Version;
    expectedDisconnectedClients: ClientID[];
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
    },
    {
      name: 'one mutation, one client',
      mutations: [mutation('c1', 2, 'put', {key: 'foo', value: 'bar'})],
      clients: ['c1'],
      clientRecords: records,
      connectedClients: ['c1'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: startTime,
          },
        },
      ],
      expectedUserValues: new Map([['foo', userValue('bar', endVersion)]]),
      expectedClientRecords: new Map([
        ...records,
        ['c1', clientRecord('cg1', endVersion, 2, endVersion)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: [],
    },
    {
      name: 'one mutation, two clients',
      mutations: [mutation('c1', 2, 'put', {key: 'foo', value: 'bar'})],
      clients: ['c1', 'c2'],
      clientRecords: records,
      connectedClients: ['c1', 'c2'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: startTime,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: startTime,
          },
        },
      ],
      expectedUserValues: new Map([['foo', userValue('bar', endVersion)]]),
      expectedClientRecords: new Map([
        ...records,
        ['c1', clientRecord('cg1', endVersion, 2, endVersion)],
        ['c2', clientRecord('cg1', endVersion, 7, 1)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: [],
    },
    {
      name: 'two mutations, three clients, two client groups',
      mutations: [
        mutation('c1', 2, 'put', {key: 'foo', value: 'bar'}),
        mutation('c3', 8, 'put', {key: 'fuzzy', value: 'wuzzy'}),
      ],
      clients: ['c1', 'c2', 'c3'],
      clientRecords: records,
      connectedClients: ['c1', 'c2', 'c3'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
              {
                op: 'put',
                key: 'fuzzy',
                value: 'wuzzy',
              },
            ],
            timestamp: startTime,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
              {
                op: 'put',
                key: 'fuzzy',
                value: 'wuzzy',
              },
            ],
            timestamp: startTime,
          },
        },
        {
          clientID: 'c3',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationIDChanges: {c3: 8},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
              {
                op: 'put',
                key: 'fuzzy',
                value: 'wuzzy',
              },
            ],
            timestamp: startTime,
          },
        },
      ],
      expectedUserValues: new Map([
        ['foo', userValue('bar', endVersion)],
        ['fuzzy', userValue('wuzzy', endVersion)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        ['c1', clientRecord('cg1', endVersion, 2, endVersion)],
        ['c2', clientRecord('cg1', endVersion, 7, 1)],
        ['c3', clientRecord('cg2', endVersion, 8, endVersion)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: [],
    },
    {
      name: 'two mutations, one client, one key',
      mutations: [
        mutation('c1', 2, 'put', {key: 'foo', value: 'bar'}),
        mutation('c1', 3, 'put', {key: 'foo', value: 'baz'}),
      ],
      clients: ['c1'],
      clientRecords: records,
      connectedClients: ['c1'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationIDChanges: {c1: 3},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'baz',
              },
            ],
            timestamp: startTime,
          },
        },
      ],
      expectedUserValues: new Map([['foo', userValue('baz', endVersion)]]),
      expectedClientRecords: new Map([
        ...records,
        ['c1', clientRecord('cg1', endVersion, 3, endVersion)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: [],
    },
    {
      name: 'no mutations, no clients, 1 client disconnects',
      mutations: [],
      clients: [],
      clientRecords: records,
      connectedClients: ['c1'],
      expectedPokes: [],
      expectedUserValues: new Map([
        [disconnectHandlerWriteKey('c1'), userValue(true, endVersion)],
      ]),
      expectedClientRecords: records,
      expectedVersion: endVersion,
      expectedDisconnectedClients: ['c1'],
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
            cookie: endVersion,
            lastMutationIDChanges: {},
            patch: [
              {
                key: 'test-disconnected-c1',
                op: 'put',
                value: true,
              },
            ],
            timestamp: 100,
          },
        },
      ],
      expectedUserValues: new Map([
        [disconnectHandlerWriteKey('c1'), userValue(true, endVersion)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        ['c2', clientRecord('cg1', endVersion, 7, 1)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: ['c1'],
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
            cookie: endVersion,
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
            timestamp: 100,
          },
        },
      ],
      expectedUserValues: new Map([
        [disconnectHandlerWriteKey('c1'), userValue(true, endVersion)],
        [disconnectHandlerWriteKey('c3'), userValue(true, endVersion)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        ['c2', clientRecord('cg1', endVersion, 7, 1)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: ['c1', 'c3'],
    },
    {
      name: 'one mutation, 2 clients, 1 client disconnects',
      mutations: [mutation('c1', 2, 'put', {key: 'foo', value: 'bar'})],
      clients: ['c1', 'c2'],
      clientRecords: records,
      connectedClients: ['c1', 'c2', 'c3'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
              {
                key: 'test-disconnected-c3',
                op: 'put',
                value: true,
              },
            ],
            timestamp: startTime,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
              {
                key: 'test-disconnected-c3',
                op: 'put',
                value: true,
              },
            ],
            timestamp: startTime,
          },
        },
      ],
      expectedUserValues: new Map([
        ['foo', userValue('bar', endVersion)],
        [disconnectHandlerWriteKey('c3'), userValue(true, endVersion)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        ['c1', clientRecord('cg1', endVersion, 2, endVersion)],
        ['c2', clientRecord('cg1', endVersion, 7, 1)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: ['c3'],
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
      },
      c.clients,
      storage,
      startTime,
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
