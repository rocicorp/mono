import {
  afterEach,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import type {Version} from 'reflect-protocol';
import type {Env, WriteTransaction} from 'reflect-shared';
import {jsonSchema} from 'shared/src/json-schema.js';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import {
  ClientRecordMap,
  clientRecordKey,
  putClientRecord,
} from '../../src/types/client-record.js';
import type {ClientID, ClientMap} from '../../src/types/client-state.js';
import {
  UserValue,
  putUserValue,
  userValueKey,
} from '../../src/types/user-value.js';
import {versionKey} from '../../src/types/version.js';
import {processFrame} from '../process/process-frame.js';
import type {ClientPoke} from '../types/client-poke.js';
import {
  connectedClientsKey,
  putConnectedClients,
} from '../types/connected-clients.js';
import type {PendingMutation} from '../types/mutation.js';
import {
  client,
  clientRecord,
  createSilentLogContext,
  mockMathRandom,
  pendingMutation,
  userValue,
} from '../util/test-utils.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();
const startTime = 1000;
const env: Env = {env: 'dood'};

const TWO_WEEKS = 2 * 7 * 24 * 60 * 60 * 1000;

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(startTime);
});

afterEach(() => {
  jest.restoreAllMocks();
});

mockMathRandom();

describe('processFrame', () => {
  const startVersion = 1;
  const disconnectHandlerWriteKey = (clientID: string) =>
    'test-disconnected-' + clientID;

  type Case = {
    name: string;
    pendingMutations: PendingMutation[];
    numPendingMutationsToProcess: number;
    clients: ClientMap;
    clientRecords: ClientRecordMap;
    initialUserValues?: Record<string, UserValue>;
    storedConnectedClients: ClientID[];
    expectedPokes: ClientPoke[];
    expectedUserValues: Map<string, UserValue>;
    expectedClientRecords: ClientRecordMap;
    expectedVersion: Version;
    expectedDisconnectedClients: ClientID[];
    expectedConnectedClients: ClientID[];
    disconnectHandlerThrows: boolean;
  };

  const mutators = new Map(
    Object.entries({
      put: async (
        tx: WriteTransaction,
        {key, value}: {key: string; value: ReadonlyJSONValue},
      ) => {
        expect(tx.env).toEqual(env);
        await tx.set(key, value);
      },
      del: async (tx: WriteTransaction, {key}: {key: string}) => {
        expect(tx.env).toEqual(env);
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
      pendingMutations: [],
      numPendingMutationsToProcess: 0,
      clients: new Map(),
      clientRecords: records,
      storedConnectedClients: [],
      expectedPokes: [],
      expectedUserValues: new Map(),
      expectedClientRecords: records,
      expectedVersion: startVersion,
      expectedDisconnectedClients: [],
      expectedConnectedClients: [],
      disconnectHandlerThrows: false,
    },
    {
      name: 'no mutations, one client',
      pendingMutations: [],
      numPendingMutationsToProcess: 0,
      clients: new Map([client('c1', 'u1', 'cg1')]),
      clientRecords: records,
      storedConnectedClients: ['c1'],
      expectedPokes: [],
      expectedUserValues: new Map(),
      expectedClientRecords: records,
      expectedVersion: startVersion,
      expectedDisconnectedClients: [],
      expectedConnectedClients: ['c1'],
      disconnectHandlerThrows: false,
    },
    {
      name: 'one mutation, one client',
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 100,
          name: 'put',
          args: {key: 'foo', value: 'bar'},
        }),
      ],
      numPendingMutationsToProcess: 1,
      clients: new Map([client('c1', 'u1', 'cg1')]),
      clientRecords: records,
      storedConnectedClients: ['c1'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            presence: [],
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
      expectedConnectedClients: ['c1'],
      disconnectHandlerThrows: false,
    },
    {
      name: 'one mutation, two clients',
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 100,
          name: 'put',
          args: {key: 'foo', value: 'bar'},
        }),
      ],
      numPendingMutationsToProcess: 1,
      clients: new Map([client('c1', 'u1', 'cg1'), client('c2', 'u2', 'cg1')]),
      clientRecords: records,
      storedConnectedClients: ['c1', 'c2'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            presence: [],
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
            presence: [],
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
      expectedConnectedClients: ['c1', 'c2'],
      disconnectHandlerThrows: false,
    },
    {
      name: 'one mutation, two clients, debugPerf',
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: {
            normalizedTimestamp: 100,
            originTimestamp: 1000,
            serverReceivedTimestamp: startTime - 100,
          },
          name: 'put',
          args: {key: 'foo', value: 'bar'},
        }),
      ],
      numPendingMutationsToProcess: 1,
      clients: new Map([
        client('c1', 'u1', 'cg1', undefined, undefined, true /* debugPerf */),
        client('c2', 'u2', 'cg1'),
      ]),
      clientRecords: records,
      storedConnectedClients: ['c1', 'c2'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            presence: [],
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
            ],
            timestamp: 100,
            debugOriginTimestamp: 1000,
            debugServerReceivedTimestamp: startTime - 100,
            debugServerSentTimestamp: startTime,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            presence: [],
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
      expectedConnectedClients: ['c1', 'c2'],
      disconnectHandlerThrows: false,
    },
    {
      name: 'two mutations, three clients, two client groups',
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 100,
          name: 'put',
          args: {key: 'foo', value: 'bar'},
        }),
        pendingMutation({
          clientID: 'c3',
          clientGroupID: 'cg2',
          id: 8,
          timestamps: 120,
          name: 'put',
          args: {
            key: 'fuzzy',
            value: 'wuzzy',
          },
        }),
      ],
      numPendingMutationsToProcess: 2,
      clients: new Map([
        client('c1', 'u1', 'cg1'),
        client('c2', 'u2', 'cg1'),
        client('c3', 'u3', 'cg2'),
      ]),
      clientRecords: records,
      storedConnectedClients: ['c1', 'c2', 'c3'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            presence: [],
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
            presence: [],
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
            presence: [],
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
            presence: [],
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
            presence: [],
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
            presence: [],
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
      expectedConnectedClients: ['c1', 'c2', 'c3'],
      disconnectHandlerThrows: false,
    },
    {
      name: 'two mutations, one client, one key',
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 100,
          name: 'put',
          args: {key: 'foo', value: 'bar'},
        }),
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: 120,
          name: 'put',
          args: {key: 'foo', value: 'baz'},
        }),
      ],
      numPendingMutationsToProcess: 2,
      clients: new Map([client('c1', 'u1', 'cg1')]),
      clientRecords: records,
      storedConnectedClients: ['c1'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            presence: [],
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
            presence: [],
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
      expectedConnectedClients: ['c1'],
      disconnectHandlerThrows: false,
    },
    {
      name: 'no mutations, no clients, 1 client disconnects',
      pendingMutations: [],
      numPendingMutationsToProcess: 0,
      clients: new Map(),
      clientRecords: records,
      storedConnectedClients: ['c1'],
      expectedPokes: [],
      expectedUserValues: new Map([
        [disconnectHandlerWriteKey('c1'), userValue(true, startVersion + 1)],
      ]),
      expectedClientRecords: records,
      expectedVersion: startVersion + 1,
      expectedDisconnectedClients: ['c1'],
      expectedConnectedClients: [],
      disconnectHandlerThrows: false,
    },
    {
      name: 'no mutations, no clients, 1 client disconnects, disconnect handler throws',
      pendingMutations: [],
      numPendingMutationsToProcess: 0,
      clients: new Map(),
      clientRecords: records,
      storedConnectedClients: ['c1'],
      // No user values or pokes because only write was in disconnect handler which threw
      expectedPokes: [],
      expectedUserValues: new Map(),
      expectedClientRecords: records,
      // version not incremented for same reason
      expectedVersion: startVersion,
      expectedDisconnectedClients: ['c1'],
      expectedConnectedClients: [],
      disconnectHandlerThrows: true,
    },
    {
      name: 'no mutations, 1 client, 1 client disconnected',
      pendingMutations: [],
      numPendingMutationsToProcess: 0,
      clients: new Map([client('c2', 'u2', 'cg1')]),
      clientRecords: records,
      storedConnectedClients: ['c1', 'c2'],
      expectedPokes: [
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {},
            presence: [],
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
      expectedConnectedClients: ['c2'],
      disconnectHandlerThrows: false,
    },
    {
      name: 'no mutations, 1 client, 2 clients disconnected',
      pendingMutations: [],
      numPendingMutationsToProcess: 0,
      clients: new Map([client('c2', 'u2', 'cg1')]),
      clientRecords: records,
      storedConnectedClients: ['c1', 'c2', 'c3'],
      expectedPokes: [
        {
          clientID: 'c2',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {},
            presence: [],
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
      expectedConnectedClients: ['c2'],
      disconnectHandlerThrows: false,
    },
    {
      name: '1 mutation, 2 clients, 1 client disconnects',
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 100,
          name: 'put',
          args: {key: 'foo', value: 'bar'},
        }),
      ],
      numPendingMutationsToProcess: 1,
      clients: new Map([client('c1', 'u1', 'cg1'), client('c2', 'u2', 'cg1')]),
      clientRecords: records,
      storedConnectedClients: ['c1', 'c2', 'c3'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            presence: [],
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
            presence: [],
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
            presence: [],
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
            presence: [],
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
      expectedConnectedClients: ['c1', 'c2'],
      disconnectHandlerThrows: false,
    },
    {
      name: '1 mutation, 2 clients, 1 client disconnects but has pending not process in this frame',
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 100,
          name: 'put',
          args: {key: 'foo', value: 'bar'},
        }),
        pendingMutation({
          clientID: 'c3',
          clientGroupID: 'cg2',
          id: 8,
          timestamps: 120,
          name: 'put',
          args: {
            key: 'fuzzy',
            value: 'wuzzy',
          },
        }),
      ],
      numPendingMutationsToProcess: 1,
      clients: new Map([client('c1', 'u1', 'cg1'), client('c2', 'u2', 'cg1')]),
      clientRecords: records,
      storedConnectedClients: ['c1', 'c2', 'c3'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            presence: [],
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
            presence: [],
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
      expectedConnectedClients: ['c1', 'c2', 'c3'],
      disconnectHandlerThrows: false,
    },
    {
      name: '1 mutation, 2 clients, 1 client disconnects and has pending processed in this frame',
      pendingMutations: [
        pendingMutation({
          clientID: 'c3',
          clientGroupID: 'cg2',
          id: 8,
          timestamps: 100,
          name: 'put',
          args: {
            key: 'fuzzy',
            value: 'wuzzy',
          },
        }),
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 120,
          name: 'put',
          args: {key: 'foo', value: 'bar'},
        }),
      ],
      numPendingMutationsToProcess: 1,
      clients: new Map([client('c1', 'u1', 'cg1'), client('c2', 'u2', 'cg1')]),
      clientRecords: records,
      storedConnectedClients: ['c1', 'c2', 'c3'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {},
            presence: [],
            patch: [
              {
                op: 'put',
                key: 'fuzzy',
                value: 'wuzzy',
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
            lastMutationIDChanges: {},
            presence: [],
            patch: [
              {
                op: 'put',
                key: 'fuzzy',
                value: 'wuzzy',
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
            presence: [],
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
            presence: [],
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
        ['fuzzy', userValue('wuzzy', startVersion + 1)],
        [disconnectHandlerWriteKey('c3'), userValue(true, startVersion + 2)],
      ]),
      expectedClientRecords: new Map([
        ...records,
        ['c1', clientRecord('cg1', startVersion + 2, 1, startVersion)],
        ['c2', clientRecord('cg1', startVersion + 2, 7, startVersion)],
        ['c3', clientRecord('cg2', startVersion, 8, startVersion + 1)],
      ]),
      expectedVersion: startVersion + 2,
      expectedDisconnectedClients: ['c3'],
      expectedConnectedClients: ['c1', 'c2'],
      disconnectHandlerThrows: false,
    },
    {
      name: '1 mutation, 2 clients. 1 client should be garbage collected',
      initialUserValues: {
        '-/c/c1/a': userValue('aa', startVersion),
        '-/c/c2/b': userValue('bb', startVersion),
        '-/c/c2/c': userValue('cc', startVersion, true),
      },
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 100,
          name: 'put',
          args: {key: 'foo', value: 'bar'},
        }),
      ],
      numPendingMutationsToProcess: 1,
      clients: new Map([client('c1', 'u1', 'cg1')]),
      clientRecords: new Map([
        ['c1', clientRecord('cg1', null, 1, 1)],
        ['c2', clientRecord('cg2', 1, 7, 1, startTime - TWO_WEEKS)],
      ]),
      storedConnectedClients: ['c1'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            presence: [],
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
            presence: [],
            patch: [{op: 'del', key: '-/c/c2/b'}],
            timestamp: undefined,
          },
        },
      ],
      expectedUserValues: new Map([
        ['foo', userValue('bar', startVersion + 1)],
        ['-/c/c1/a', userValue('aa', startVersion)],
        ['-/c/c2/b', userValue('bb', startVersion + 2, true)],
        // The next one was already deleted so no update to startVersion
        ['-/c/c2/c', userValue('cc', startVersion, true)],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', startVersion + 2, 2, startVersion + 1)],
      ]),
      expectedVersion: startVersion + 2,
      expectedDisconnectedClients: [],
      expectedConnectedClients: ['c1'],
      disconnectHandlerThrows: false,
    },
    {
      name: '1 mutation, 3 clients. 1 client should be garbage collected. 1 got disconnected',
      initialUserValues: {
        '-/c/c1/a': userValue('aa', startVersion),
        '-/c/c2/b': userValue('bb', startVersion),
        '-/c/c2/c': userValue('cc', startVersion, true),
        '-/c/c3/d': userValue('dd', startVersion),
      },
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 100,
          name: 'put',
          args: {key: 'foo', value: 'bar'},
        }),
      ],
      numPendingMutationsToProcess: 1,
      clients: new Map([client('c1', 'u1', 'cg1')]),
      clientRecords: new Map([
        ['c1', clientRecord('cg1', null, 1, 1)],
        ['c2', clientRecord('cg2', 1, 7, 1, startTime - TWO_WEEKS)],
        ['c3', clientRecord('cg3', null, 1, 1)],
      ]),
      storedConnectedClients: ['c1', 'c3'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {c1: 2},
            presence: [],
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
            presence: [],
            patch: [{op: 'del', key: '-/c/c2/b'}],
            timestamp: undefined,
          },
        },
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion + 2,
            cookie: startVersion + 3,
            lastMutationIDChanges: {},
            presence: [],
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
        ['-/c/c1/a', userValue('aa', startVersion)],
        ['-/c/c2/b', userValue('bb', startVersion + 2, true)],
        // The next one was already deleted so no update to startVersion
        ['-/c/c2/c', userValue('cc', startVersion, true)],
        ['-/c/c3/d', userValue('dd', startVersion)],
        ['test-disconnected-c3', userValue(true, startVersion + 3)],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', startVersion + 3, 2, startVersion + 1)],
        ['c3', clientRecord('cg3', null, 1, startVersion)],
      ]),
      expectedVersion: startVersion + 3,
      expectedDisconnectedClients: ['c3'],
      expectedConnectedClients: ['c1'],
      disconnectHandlerThrows: false,
    },

    {
      name: '0 mutations, 2 clients. 1 client should be garbage collected',
      initialUserValues: {
        '-/c/c1/a': userValue('aa', startVersion),
        '-/c/c2/b': userValue('bb', startVersion),
        '-/c/c2/c': userValue('cc', startVersion, true),
      },
      pendingMutations: [],
      numPendingMutationsToProcess: 0,
      clients: new Map([client('c1', 'u1', 'cg1')]),
      clientRecords: new Map([
        ['c1', clientRecord('cg1', null, 1, 1)],
        ['c2', clientRecord('cg2', 1, 7, 1, startTime - TWO_WEEKS)],
      ]),
      storedConnectedClients: ['c1'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: startVersion,
            cookie: startVersion + 1,
            lastMutationIDChanges: {},
            presence: [],
            patch: [{op: 'del', key: '-/c/c2/b'}],
            timestamp: undefined,
          },
        },
      ],
      expectedUserValues: new Map([
        ['-/c/c1/a', userValue('aa', startVersion)],
        ['-/c/c2/b', userValue('bb', startVersion + 1, true)],
        // The next one was already deleted so no update to startVersion
        ['-/c/c2/c', userValue('cc', startVersion, true)],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', startVersion + 1, 1, startVersion)],
      ]),
      expectedVersion: startVersion + 1,
      expectedDisconnectedClients: [],
      expectedConnectedClients: ['c1'],
      disconnectHandlerThrows: false,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const durable = await getMiniflareDurableObjectStorage(id);
      await durable.deleteAll();
      const storage = new DurableStorage(durable);

      await storage.put(versionKey, startVersion);
      for (const [clientID, record] of c.clientRecords) {
        await putClientRecord(clientID, record, storage);
      }
      await putConnectedClients(new Set(c.storedConnectedClients), storage);

      if (c.initialUserValues) {
        for (const [key, value] of Object.entries(c.initialUserValues)) {
          await putUserValue(key, value, storage);
        }
      }

      const disconnectCallClients: ClientID[] = [];
      const result = await processFrame(
        createSilentLogContext(),
        env,
        c.pendingMutations,
        c.numPendingMutationsToProcess,
        mutators,
        async write => {
          await write.set(disconnectHandlerWriteKey(write.clientID), true);
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
        ...new Map<string, ReadonlyJSONValue>(
          [...c.expectedUserValues].map(([key, value]) => [
            userValueKey(key),
            value,
          ]),
        ),
        ...new Map<string, ReadonlyJSONValue>(
          [...c.expectedClientRecords].map(([key, value]) => [
            clientRecordKey(key),
            value,
          ]),
        ),
        [versionKey, c.expectedVersion],
        [connectedClientsKey, [...c.expectedConnectedClients]],
      ]);

      expect((await durable.list()).size).toEqual(expectedState.size);
      for (const [key, value] of expectedState) {
        expect(await storage.get(key, jsonSchema)).toEqual(value);
      }
    });
  }
});
