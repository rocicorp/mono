import {describe, test, expect} from '@jest/globals';
import type {WriteTransaction} from 'replicache';
import {DurableStorage} from '../storage/durable-storage.js';
import type {ClientPoke} from '../types/client-poke.js';
import {
  ClientRecordMap,
  getClientRecord,
  putClientRecord,
} from '../types/client-record.js';
import type {ClientMap} from '../types/client-state.js';
import {getUserValue, UserValue} from '../types/user-value.js';
import {getVersion, versionKey} from '../types/version.js';
import type {Version} from 'reflect-protocol';
import {
  client,
  clientRecord,
  createSilentLogContext,
  fail,
  mockMathRandom,
  pendingMutation,
} from '../util/test-utils.js';
import {processRoom} from '../process/process-room.js';
import type {PendingMutation} from '../types/mutation.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

mockMathRandom();

const pendingMutations1: PendingMutation[] = [
  pendingMutation({
    clientID: 'c1',
    clientGroupID: 'cg1',
    id: 2,
    timestamps: 50,
    name: 'inc',
  }),
  pendingMutation({
    clientID: 'c3',
    clientGroupID: 'cg2',
    id: 5,
    timestamps: 50,
    name: 'inc',
  }),
  pendingMutation({
    clientID: 'c1',
    clientGroupID: 'cg1',
    id: 3,
    timestamps: 100,
    name: 'inc',
  }),
  pendingMutation({
    clientID: 'c2',
    clientGroupID: 'cg1',
    id: 2,
    timestamps: 10,
    name: 'inc',
  }),
];

const expectedPokesForPendingMutations1: ClientPoke[] = [
  {
    clientID: 'c1',
    poke: {
      baseCookie: 1,
      cookie: 2,
      lastMutationIDChanges: {c1: 2},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 1,
        },
      ],
      timestamp: 50,
    },
  },
  {
    clientID: 'c2',
    poke: {
      baseCookie: 1,
      cookie: 2,
      lastMutationIDChanges: {c1: 2},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 1,
        },
      ],
      timestamp: 50,
    },
  },
  {
    clientID: 'c3',
    poke: {
      baseCookie: 1,
      cookie: 2,
      lastMutationIDChanges: {},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 1,
        },
      ],
      timestamp: 50,
    },
  },
  {
    clientID: 'c4',
    poke: {
      baseCookie: 1,
      cookie: 2,
      lastMutationIDChanges: {},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 1,
        },
      ],
      timestamp: 50,
    },
  },

  {
    clientID: 'c1',
    poke: {
      baseCookie: 2,
      cookie: 3,
      lastMutationIDChanges: {},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 2,
        },
      ],
      timestamp: 50,
    },
  },
  {
    clientID: 'c2',
    poke: {
      baseCookie: 2,
      cookie: 3,
      lastMutationIDChanges: {},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 2,
        },
      ],
      timestamp: 50,
    },
  },
  {
    clientID: 'c3',
    poke: {
      baseCookie: 2,
      cookie: 3,
      lastMutationIDChanges: {c3: 5},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 2,
        },
      ],
      timestamp: 50,
    },
  },
  {
    clientID: 'c4',
    poke: {
      baseCookie: 2,
      cookie: 3,
      lastMutationIDChanges: {},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 2,
        },
      ],
      timestamp: 50,
    },
  },

  {
    clientID: 'c1',
    poke: {
      baseCookie: 3,
      cookie: 4,
      lastMutationIDChanges: {c1: 3},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 3,
        },
      ],
      timestamp: 100,
    },
  },
  {
    clientID: 'c2',
    poke: {
      baseCookie: 3,
      cookie: 4,
      lastMutationIDChanges: {c1: 3},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 3,
        },
      ],
      timestamp: 100,
    },
  },
  {
    clientID: 'c3',
    poke: {
      baseCookie: 3,
      cookie: 4,
      lastMutationIDChanges: {},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 3,
        },
      ],
      timestamp: 100,
    },
  },
  {
    clientID: 'c4',
    poke: {
      baseCookie: 3,
      cookie: 4,
      lastMutationIDChanges: {},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 3,
        },
      ],
      timestamp: 100,
    },
  },

  {
    clientID: 'c1',
    poke: {
      baseCookie: 4,
      cookie: 5,
      lastMutationIDChanges: {c2: 2},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 4,
        },
      ],
      timestamp: 10,
    },
  },
  {
    clientID: 'c2',
    poke: {
      baseCookie: 4,
      cookie: 5,
      lastMutationIDChanges: {c2: 2},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 4,
        },
      ],
      timestamp: 10,
    },
  },
  {
    clientID: 'c3',
    poke: {
      baseCookie: 4,
      cookie: 5,
      lastMutationIDChanges: {},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 4,
        },
      ],
      timestamp: 10,
    },
  },
  {
    clientID: 'c4',
    poke: {
      baseCookie: 4,
      cookie: 5,
      lastMutationIDChanges: {},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 4,
        },
      ],
      timestamp: 10,
    },
  },
];

describe('processRoom', () => {
  type Case = {
    name: string;
    clientRecords: ClientRecordMap;
    headVersion: Version;
    clients: ClientMap;
    pendingMutations: PendingMutation[];
    expectedError?: string;
    expectedPokes?: ClientPoke[];
    expectedUserValues?: Map<string, UserValue>;
    expectedClientRecords?: ClientRecordMap;
    expectedVersion: Version;
  };

  const cases: Case[] = [
    {
      name: 'no client record',
      clientRecords: new Map(),
      pendingMutations: [],
      headVersion: 42,
      clients: new Map([client('c1', 'u1', 'cg1')]),
      expectedUserValues: new Map(),
      expectedError: 'Error: Client record not found: c1',
      expectedVersion: 42,
    },
    {
      name: 'no mutations, clients out of date',
      clientRecords: new Map([
        ['c1', clientRecord('cg1')],
        ['c2', clientRecord('cg1')],
        ['c3', clientRecord('cg2')],
      ]),
      headVersion: 2,
      clients: new Map([
        client('c1', 'u1', 'cg1'),
        client('c2', 'u2', 'cg1'),
        client('c3', 'u3', 'cg2'),
      ]),
      pendingMutations: [],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: null,
            cookie: 2,
            lastMutationIDChanges: {c1: 1, c2: 1},
            patch: [],
            timestamp: undefined,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: null,
            cookie: 2,
            lastMutationIDChanges: {c1: 1, c2: 1},
            patch: [],
            timestamp: undefined,
          },
        },
        {
          clientID: 'c3',
          poke: {
            baseCookie: null,
            cookie: 2,
            lastMutationIDChanges: {c3: 1},
            patch: [],
            timestamp: undefined,
          },
        },
      ],
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 2)],
        ['c2', clientRecord('cg1', 2)],
        ['c3', clientRecord('cg2', 2)],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 2,
    },
    {
      name: 'no mutations, one client out of date',
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 2)],
        ['c2', clientRecord('cg1')],
        ['c3', clientRecord('cg2', 2)],
      ]),
      headVersion: 2,
      clients: new Map([
        client('c1', 'u1', 'cg1'),
        client('c2', 'u2', 'cg1'),
        client('c3', 'u3', 'cg2'),
      ]),
      pendingMutations: [],
      expectedPokes: [
        {
          clientID: 'c2',
          poke: {
            baseCookie: null,
            cookie: 2,
            lastMutationIDChanges: {c1: 1, c2: 1},
            patch: [],
            timestamp: undefined,
          },
        },
      ],
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 2)],
        ['c2', clientRecord('cg1', 2)],
        ['c3', clientRecord('cg2', 2)],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 2,
    },
    {
      name: 'one mutation',
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      headVersion: 1,
      clients: new Map([client('c1', 'u1', 'cg1')]),
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 300,
          name: 'inc',
        }),
      ],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 1,
            cookie: 2,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                key: 'count',
                op: 'put',
                value: 1,
              },
            ],
            timestamp: 300,
          },
        },
      ],
      expectedClientRecords: new Map([['c1', clientRecord('cg1', 2, 2, 2)]]),
      expectedUserValues: new Map(),
      expectedVersion: 2,
    },
    {
      name: 'mutations before range are included',
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      headVersion: 1,
      clients: new Map([client('c1', 'u1', 'cg1')]),
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 50,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: 100,
          name: 'inc',
        }),
      ],
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 1,
            cookie: 2,
            lastMutationIDChanges: {c1: 2},
            patch: [
              {
                op: 'put',
                key: 'count',
                value: 1,
              },
            ],
            timestamp: 50,
          },
        },
        {
          clientID: 'c1',
          poke: {
            baseCookie: 2,
            cookie: 3,
            lastMutationIDChanges: {c1: 3},
            patch: [
              {
                op: 'put',
                key: 'count',
                value: 2,
              },
            ],
            timestamp: 100,
          },
        },
      ],
      expectedClientRecords: new Map([['c1', clientRecord('cg1', 3, 3, 3)]]),
      expectedUserValues: new Map(),
      expectedVersion: 3,
    },
    {
      name: 'mutations in different client groups',
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1)],
        ['c2', clientRecord('cg1', 1)],
        ['c3', clientRecord('cg2', 1, 4, 1)],
        ['c4', clientRecord('cg3', 1)],
      ]),
      headVersion: 1,
      clients: new Map([
        client('c1', 'u1', 'cg1'),
        client('c2', 'u2', 'cg1'),
        client('c3', 'u3', 'cg2'),
        client('c4', 'u4', 'cg3'),
      ]),
      pendingMutations: pendingMutations1,
      expectedPokes: expectedPokesForPendingMutations1,
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 5, 3, 4)],
        ['c2', clientRecord('cg1', 5, 2, 5)],
        ['c3', clientRecord('cg2', 5, 5, 3)],
        ['c4', clientRecord('cg3', 5)],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 5,
    },
    {
      name: '2 clients need to be fast forwarded, and mutations in different client groups',
      clientRecords: new Map([
        ['c1', clientRecord('cg1', null)],
        ['c2', clientRecord('cg1', 1)],
        ['c3', clientRecord('cg2', null, 4, 1)],
        ['c4', clientRecord('cg3', 1)],
      ]),
      headVersion: 1,
      clients: new Map([
        client('c1', 'u1', 'cg1'),
        client('c2', 'u2', 'cg1'),
        client('c3', 'u3', 'cg2'),
        client('c4', 'u4', 'cg3'),
      ]),
      pendingMutations: pendingMutations1,
      expectedPokes: [
        // fast forward pokes
        {
          clientID: 'c1',
          poke: {
            baseCookie: null,
            cookie: 1,
            lastMutationIDChanges: {
              c1: 1,
              c2: 1,
            },
            patch: [],
            timestamp: undefined,
          },
        },
        {
          clientID: 'c3',
          poke: {
            baseCookie: null,
            cookie: 1,
            lastMutationIDChanges: {
              c3: 4,
            },
            patch: [],
            timestamp: undefined,
          },
        },
        ...expectedPokesForPendingMutations1,
      ],
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 5, 3, 4)],
        ['c2', clientRecord('cg1', 5, 2, 5)],
        ['c3', clientRecord('cg2', 5, 5, 3)],
        ['c4', clientRecord('cg3', 5)],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 5,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const mutators = new Map(
        Object.entries({
          inc: async (tx: WriteTransaction) => {
            let count = ((await tx.get('count')) as number) ?? 0;
            count++;
            await tx.put('count', count);
          },
        }),
      );

      const durable = await getMiniflareDurableObjectStorage(id);
      await durable.deleteAll();
      const storage = new DurableStorage(durable);
      await storage.put(versionKey, c.headVersion);
      for (const [clientID, record] of c.clientRecords) {
        await putClientRecord(clientID, record, storage);
      }

      const p = processRoom(
        createSilentLogContext(),
        c.clients,
        c.pendingMutations,
        mutators,
        () => Promise.resolve(),
        storage,
      );
      if (c.expectedError) {
        try {
          await p;
          fail('Expected error');
        } catch (e) {
          expect(String(e)).toEqual(c.expectedError);
        }
      } else {
        const pokes = await p;
        expect(pokes).toEqual(c.expectedPokes);
      }

      for (const [clientID, record] of c.expectedClientRecords ?? new Map()) {
        expect(await getClientRecord(clientID, storage)).toEqual(record);
      }

      for (const [key, value] of c.expectedUserValues ?? new Map()) {
        expect(await getUserValue(key, storage)).toEqual(value);
      }

      expect(await getVersion(storage)).toEqual(c.expectedVersion);
    });
  }
});
