import {test, expect} from '@jest/globals';
import {DurableStorage} from '../storage/durable-storage.js';
import type {ClientPoke} from '../types/client-poke.js';
import {ClientRecordMap, putClientRecord} from '../types/client-record.js';
import type {ClientID} from '../types/client-state.js';
import {putUserValue, UserValue} from '../types/user-value.js';
import {fastForwardRoom} from '../ff/fast-forward.js';
import {mockMathRandom} from '../util/test-utils.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

mockMathRandom();

test('fastForward', async () => {
  type Case = {
    name: string;
    state: Map<string, UserValue>;
    clientRecords: ClientRecordMap;
    clients: ClientID[];
    expectedError?: string;
    expectedPokes?: ClientPoke[];
  };

  const CURRENT_VERSION_FOR_TEST = 42;

  const cases: Case[] = [
    {
      name: 'no clients',
      state: new Map([['foo', {value: 'bar', version: 1, deleted: false}]]),
      clientRecords: new Map([
        [
          'c1',
          {
            lastMutationID: 1,
            baseCookie: 0,
            clientGroupID: 'cg1',
            lastMutationIDVersion: 1,
          },
        ],
      ]),
      clients: [],
      expectedPokes: [],
    },
    {
      name: 'no data',
      state: new Map(),
      clientRecords: new Map([
        [
          'c1',
          {
            lastMutationID: 1,
            baseCookie: 10,
            clientGroupID: 'cg1',
            lastMutationIDVersion: 20,
          },
        ],
      ]),
      clients: ['c1'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 10,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {c1: 1},
            patch: [],
            timestamp: undefined,
          },
        },
      ],
    },
    {
      name: 'up to date',
      state: new Map(),
      clientRecords: new Map([
        [
          'c1',
          {
            baseCookie: CURRENT_VERSION_FOR_TEST,
            clientGroupID: 'cg1',
            lastMutationID: 1,
            lastMutationIDVersion: 20,
          },
        ],
      ]),
      clients: ['c1'],
      expectedPokes: [],
    },
    {
      name: 'one client two changes',
      state: new Map([
        [
          'foo',
          {value: 'bar', version: CURRENT_VERSION_FOR_TEST, deleted: false},
        ],
        [
          'hot',
          {value: 'dog', version: CURRENT_VERSION_FOR_TEST, deleted: true},
        ],
      ]),
      clientRecords: new Map([
        [
          'c1',
          {
            lastMutationID: 3,
            baseCookie: 40,
            clientGroupID: 'cg1',
            lastMutationIDVersion: 41,
          },
        ],
      ]),
      clients: ['c1'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 40,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {c1: 3},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
              {
                op: 'del',
                key: 'hot',
              },
            ],
            timestamp: undefined,
          },
        },
      ],
    },
    {
      name: 'two clients different changes',
      state: new Map([
        ['foo', {value: 'bar', version: 41, deleted: false}],
        [
          'hot',
          {value: 'dog', version: CURRENT_VERSION_FOR_TEST, deleted: true},
        ],
      ]),
      clientRecords: new Map([
        [
          'c1',
          {
            lastMutationID: 3,
            baseCookie: 40,
            clientGroupID: 'cg1',
            lastMutationIDVersion: 41,
          },
        ],
        [
          'c2',
          {
            lastMutationID: 4,
            baseCookie: 41,
            clientGroupID: 'cg1',
            lastMutationIDVersion: CURRENT_VERSION_FOR_TEST,
          },
        ],
      ]),
      clients: ['c1', 'c2'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 40,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {c1: 3, c2: 4},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
              {
                op: 'del',
                key: 'hot',
              },
            ],
            timestamp: undefined,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: 41,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {c2: 4},
            patch: [
              {
                op: 'del',
                key: 'hot',
              },
            ],
            timestamp: undefined,
          },
        },
      ],
    },
    {
      name: 'two clients with changes but only one active',
      state: new Map([
        ['foo', {value: 'bar', version: 41, deleted: false}],
        [
          'hot',
          {value: 'dog', version: CURRENT_VERSION_FOR_TEST, deleted: true},
        ],
      ]),
      clientRecords: new Map([
        [
          'c1',
          {
            lastMutationID: 3,
            baseCookie: 40,
            clientGroupID: 'cg1',
            lastMutationIDVersion: 41,
          },
        ],
        [
          'c2',
          {
            lastMutationID: 4,
            baseCookie: 41,
            clientGroupID: 'cg1',
            lastMutationIDVersion: CURRENT_VERSION_FOR_TEST,
          },
        ],
      ]),
      clients: ['c1'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 40,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {c1: 3, c2: 4},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
              {
                op: 'del',
                key: 'hot',
              },
            ],
            timestamp: undefined,
          },
        },
      ],
    },
    {
      name: 'no data, but multiple client groups w/ last mutation id changes',
      state: new Map(),
      clientRecords: new Map([
        [
          'c1',
          {
            lastMutationID: 2,
            baseCookie: 30,
            clientGroupID: 'cg1',
            lastMutationIDVersion: 41,
          },
        ],
        [
          'c2',
          {
            lastMutationID: 3,
            baseCookie: 30,
            clientGroupID: 'cg1',
            lastMutationIDVersion: 31,
          },
        ],
        [
          'c3',
          {
            lastMutationID: 4,
            baseCookie: 40,
            clientGroupID: 'cg1',
            lastMutationIDVersion: 31,
          },
        ],
        [
          'c4',
          {
            lastMutationID: 5,
            baseCookie: 30,
            clientGroupID: 'cg2',
            lastMutationIDVersion: 40,
          },
        ],
        [
          'c5',
          {
            lastMutationID: 6,
            baseCookie: 40,
            clientGroupID: 'cg2',
            lastMutationIDVersion: 40,
          },
        ],
      ]),
      clients: ['c1', 'c2', 'c3', 'c4', 'c5'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 30,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {c1: 2, c2: 3, c3: 4},
            patch: [],
            timestamp: undefined,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: 30,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {c1: 2, c2: 3, c3: 4},
            patch: [],
            timestamp: undefined,
          },
        },
        {
          clientID: 'c3',
          poke: {
            baseCookie: 40,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {c1: 2},
            patch: [],
            timestamp: undefined,
          },
        },
        {
          clientID: 'c4',
          poke: {
            baseCookie: 30,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {c4: 5, c5: 6},
            patch: [],
            timestamp: undefined,
          },
        },
        {
          clientID: 'c5',
          poke: {
            baseCookie: 40,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {},
            patch: [],
            timestamp: undefined,
          },
        },
      ],
    },
    {
      name: 'two client groups each with two clients w different changes',
      state: new Map([
        ['foo', {value: 'bar', version: 41, deleted: false}],
        [
          'hot',
          {value: 'dog', version: CURRENT_VERSION_FOR_TEST, deleted: true},
        ],
      ]),
      clientRecords: new Map([
        [
          'c1',
          {
            lastMutationID: 3,
            baseCookie: 40,
            clientGroupID: 'cg1',
            lastMutationIDVersion: 41,
          },
        ],
        [
          'c2',
          {
            lastMutationID: 4,
            baseCookie: 41,
            clientGroupID: 'cg1',
            lastMutationIDVersion: CURRENT_VERSION_FOR_TEST,
          },
        ],
        [
          'c3',
          {
            lastMutationID: 3,
            baseCookie: 40,
            clientGroupID: 'cg2',
            lastMutationIDVersion: 41,
          },
        ],
        [
          'c4',
          {
            lastMutationID: 4,
            baseCookie: 41,
            clientGroupID: 'cg2',
            lastMutationIDVersion: CURRENT_VERSION_FOR_TEST,
          },
        ],
      ]),
      clients: ['c1', 'c2', 'c3', 'c4'],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 40,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {c1: 3, c2: 4},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
              {
                op: 'del',
                key: 'hot',
              },
            ],
            timestamp: undefined,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: 41,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {c2: 4},
            patch: [
              {
                op: 'del',
                key: 'hot',
              },
            ],
            timestamp: undefined,
          },
        },
        {
          clientID: 'c3',
          poke: {
            baseCookie: 40,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {c3: 3, c4: 4},
            patch: [
              {
                op: 'put',
                key: 'foo',
                value: 'bar',
              },
              {
                op: 'del',
                key: 'hot',
              },
            ],
            timestamp: undefined,
          },
        },
        {
          clientID: 'c4',
          poke: {
            baseCookie: 41,
            cookie: CURRENT_VERSION_FOR_TEST,
            lastMutationIDChanges: {c4: 4},
            patch: [
              {
                op: 'del',
                key: 'hot',
              },
            ],
            timestamp: undefined,
          },
        },
      ],
    },
  ];

  const durable = await getMiniflareDurableObjectStorage(id);

  for (const c of cases) {
    await durable.deleteAll();
    const storage = new DurableStorage(durable);
    for (const [clientID, clientRecord] of c.clientRecords) {
      await putClientRecord(clientID, clientRecord, storage);
    }
    for (const [key, value] of c.state) {
      await putUserValue(key, value, storage);
    }

    const pokes = await fastForwardRoom(c.clients, 42, storage);

    expect(pokes).toEqual(c.expectedPokes);
  }
});
