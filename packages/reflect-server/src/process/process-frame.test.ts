import {test, expect} from '@jest/globals';
import * as s from 'superstruct';
import type {WriteTransaction} from 'replicache';
import type {JSONType} from '../../src/protocol/json.js';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import type {ClientMutation} from '../../src/types/client-mutation.js';
import type {ClientPokeBody} from '../../src/types/client-poke-body.js';
import {ClientRecord, clientRecordKey} from '../../src/types/client-record.js';
import type {ClientID} from '../../src/types/client-state.js';
import {UserValue, userValueKey} from '../../src/types/user-value.js';
import {Version, versionKey} from '../../src/types/version.js';
import {
  clientMutation,
  clientRecord,
  createSilentLogContext,
  mockMathRandom,
  userValue,
} from '../util/test-utils.js';
import {processFrame} from '../../src/process/process-frame.js';
import {connectedClientsKey} from '../types/connected-clients.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

mockMathRandom();

test('processFrame', async () => {
  const records = new Map([
    [clientRecordKey('c1'), clientRecord(null, 1)],
    [clientRecordKey('c2'), clientRecord(1, 7)],
    [clientRecordKey('c3'), clientRecord(1, 7)],
  ]);
  const startTime = 100;
  const startVersion = 1;
  const endVersion = 2;
  const disconnectHandlerWriteKey = (clientID: string) =>
    'test-disconnected-' + clientID;

  type Case = {
    name: string;
    mutations: ClientMutation[];
    clients: ClientID[];
    connectedClients: ClientID[];
    expectedPokes: ClientPokeBody[];
    expectedUserValues: Map<string, UserValue>;
    expectedClientRecords: Map<string, ClientRecord>;
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

  const cases: Case[] = [
    {
      name: 'no mutations, no clients',
      mutations: [],
      clients: [],
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
      mutations: [clientMutation('c1', 2, 'put', {key: 'foo', value: 'bar'})],
      clients: ['c1'],
      connectedClients: ['c1'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationID: 2,
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: startTime,
            requestID: '4fxcm49g2j9',
          },
        },
      ],
      expectedUserValues: new Map([
        [userValueKey('foo'), userValue('bar', endVersion)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        [clientRecordKey('c1'), clientRecord(endVersion, 2)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: [],
      disconnectHandlerThrows: false,
    },
    {
      name: 'one mutation, two clients',
      mutations: [clientMutation('c1', 2, 'put', {key: 'foo', value: 'bar'})],
      clients: ['c1', 'c2'],
      connectedClients: ['c1', 'c2'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationID: 2,
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: startTime,
            requestID: '4fxcm49g2j9',
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationID: 7,
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: startTime,
            requestID: '4fxcm49g2j9',
          },
        },
      ],
      expectedUserValues: new Map([
        [userValueKey('foo'), userValue('bar', endVersion)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        [clientRecordKey('c1'), clientRecord(endVersion, 2)],
        [clientRecordKey('c2'), clientRecord(endVersion, 7)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: [],
      disconnectHandlerThrows: false,
    },
    {
      name: 'two mutations, one client, one key',
      mutations: [
        clientMutation('c1', 2, 'put', {key: 'foo', value: 'bar'}),
        clientMutation('c1', 3, 'put', {key: 'foo', value: 'baz'}),
      ],
      clients: ['c1'],
      connectedClients: ['c1'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationID: 3,
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'baz',
              },
            ],
            timestamp: startTime,
            requestID: '4fxcm49g2j9',
          },
        },
      ],
      expectedUserValues: new Map([
        [userValueKey('foo'), userValue('baz', endVersion)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        [clientRecordKey('c1'), clientRecord(endVersion, 3)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: [],
      disconnectHandlerThrows: false,
    },
    {
      name: 'no mutations, no clients, 1 client disconnects',
      mutations: [],
      clients: [],
      connectedClients: ['c1'],
      expectedPokes: [],
      expectedUserValues: new Map([
        [
          userValueKey(disconnectHandlerWriteKey('c1')),
          userValue(true, endVersion),
        ],
      ]),
      expectedClientRecords: records,
      expectedVersion: endVersion,
      expectedDisconnectedClients: ['c1'],
      disconnectHandlerThrows: false,
    },
    {
      name: 'no mutations, no clients, 1 client disconnects, disconnect handler throws',
      mutations: [],
      clients: [],
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
      name: 'no mutations, no clients, 1 client disconnects, disconnect handler throws',
      mutations: [],
      clients: [],
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
      connectedClients: ['c1', 'c2'],
      expectedPokes: [
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationID: 7,
            patch: [
              {
                key: 'test-disconnected-c1',
                op: 'put',
                value: true,
              },
            ],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
          },
        },
      ],
      expectedUserValues: new Map([
        [
          userValueKey(disconnectHandlerWriteKey('c1')),
          userValue(true, endVersion),
        ],
      ]),
      expectedClientRecords: new Map([
        ...records,
        [clientRecordKey('c2'), clientRecord(endVersion, 7)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: ['c1'],
      disconnectHandlerThrows: false,
    },
    {
      name: 'no mutations, 1 client, 1 client disconnected, disconnect handler throws',
      mutations: [],
      clients: ['c2'],
      connectedClients: ['c1', 'c2'],
      // No user values or pokes because only write was in disconnect handler which threw
      expectedPokes: [],
      expectedUserValues: new Map(),
      // version stays at startVersion for same reason
      expectedClientRecords: new Map([
        ...records,
        [clientRecordKey('c2'), clientRecord(startVersion, 7)],
      ]),
      expectedVersion: startVersion,
      expectedDisconnectedClients: ['c1'],
      disconnectHandlerThrows: true,
    },
    {
      name: 'no mutations, 1 client, 1 client disconnected, disconnect handler throws',
      mutations: [],
      clients: ['c2'],
      connectedClients: ['c1', 'c2'],
      // No user values or pokes because only write was in disconnect handler which threw
      expectedPokes: [],
      expectedUserValues: new Map(),
      // version stays at startVersion for same reason
      expectedClientRecords: new Map([
        ...records,
        [clientRecordKey('c2'), clientRecord(startVersion, 7)],
      ]),
      expectedVersion: startVersion,
      expectedDisconnectedClients: ['c1'],
      disconnectHandlerThrows: true,
    },
    {
      name: 'no mutations, 1 client, 2 clients disconnected',
      mutations: [],
      clients: ['c2'],
      connectedClients: ['c1', 'c2', 'c3'],
      expectedPokes: [
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationID: 7,
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
            requestID: '4fxcm49g2j9',
          },
        },
      ],
      expectedUserValues: new Map([
        [
          userValueKey(disconnectHandlerWriteKey('c1')),
          userValue(true, endVersion),
        ],
        [
          userValueKey(disconnectHandlerWriteKey('c3')),
          userValue(true, endVersion),
        ],
      ]),
      expectedClientRecords: new Map([
        ...records,
        [clientRecordKey('c2'), clientRecord(endVersion, 7)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: ['c1', 'c3'],
      disconnectHandlerThrows: false,
    },
    {
      name: 'one mutation, 2 clients, 1 client disconnects',
      mutations: [clientMutation('c1', 2, 'put', {key: 'foo', value: 'bar'})],
      clients: ['c1', 'c2'],
      connectedClients: ['c1', 'c2', 'c3'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationID: 2,
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
            requestID: '4fxcm49g2j9',
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationID: 7,
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
            requestID: '4fxcm49g2j9',
          },
        },
      ],
      expectedUserValues: new Map([
        [userValueKey('foo'), userValue('bar', endVersion)],
        [
          userValueKey(disconnectHandlerWriteKey('c3')),
          userValue(true, endVersion),
        ],
      ]),
      expectedClientRecords: new Map([
        ...records,
        [clientRecordKey('c1'), clientRecord(endVersion, 2)],
        [clientRecordKey('c2'), clientRecord(endVersion, 7)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: ['c3'],
      disconnectHandlerThrows: false,
    },
    {
      name: 'one mutation, 2 clients, 1 client disconnects, disconnect handler throws',
      mutations: [clientMutation('c1', 2, 'put', {key: 'foo', value: 'bar'})],
      clients: ['c1', 'c2'],
      connectedClients: ['c1', 'c2', 'c3'],
      // No patch for writes from disconnect handler because it threw
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationID: 2,
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: startTime,
            requestID: '4fxcm49g2j9',
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: endVersion,
            lastMutationID: 7,
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: startTime,
            requestID: '4fxcm49g2j9',
          },
        },
      ],
      // writes from disconnect handler not present because it threw
      expectedUserValues: new Map([
        [userValueKey('foo'), userValue('bar', endVersion)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        [clientRecordKey('c1'), clientRecord(endVersion, 2)],
        [clientRecordKey('c2'), clientRecord(endVersion, 7)],
      ]),
      expectedVersion: endVersion,
      expectedDisconnectedClients: ['c3'],
      disconnectHandlerThrows: true,
    },
  ];

  const durable = await getMiniflareDurableObjectStorage(id);

  for (const c of cases) {
    await durable.deleteAll();
    const storage = new DurableStorage(durable);

    await storage.put(versionKey, startVersion);
    for (const [key, value] of records) {
      await storage.put(key, value);
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
      startTime,
    );

    expect(result).toEqual(c.expectedPokes);

    expect(disconnectCallClients.sort()).toEqual(
      c.expectedDisconnectedClients.sort(),
    );

    const expectedState = new Map([
      ...(c.expectedUserValues as Map<string, JSONType>),
      ...(c.expectedClientRecords as Map<string, JSONType>),
      [versionKey, c.expectedVersion],
      [connectedClientsKey, c.clients],
    ]);

    expect((await durable.list()).size).toEqual(expectedState.size);
    for (const [key, value] of expectedState) {
      expect(await storage.get(key, s.any())).toEqual(value);
    }
  }
});
