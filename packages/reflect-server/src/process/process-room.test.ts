import {describe, expect, test} from '@jest/globals';
import type {Poke, Version} from 'reflect-protocol';
import type {ClientID, Env, WriteTransaction} from 'reflect-shared/src/mod.js';
import {processRoom} from '../process/process-room.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {
  ClientRecordMap,
  IncludeDeleted,
  getClientRecord,
  putClientRecord,
} from '../types/client-record.js';
import type {ClientMap} from '../types/client-state.js';
import {putConnectedClients} from '../types/connected-clients.js';
import type {PendingMutation} from '../types/mutation.js';
import {UserValue, getUserValue} from '../types/user-value.js';
import {getVersion, versionKey} from '../types/version.js';
import {
  client,
  clientRecord,
  fail,
  mockMathRandom,
  pendingMutation,
} from '../util/test-utils.js';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();
const env: Env = {env: 'dawg'};

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

const expectedPokesForPendingMutations1: [ClientID, Poke[]][] = [
  [
    'c1',
    [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c1: 2},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
        timestamp: 50,
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
        timestamp: 50,
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c1: 3},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
        timestamp: 100,
      },
      {
        baseCookie: 4,
        cookie: 5,
        lastMutationIDChanges: {c2: 2},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 4,
          },
        ],
        timestamp: 10,
      },
    ],
  ],
  [
    'c2',
    [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c1: 2},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
        timestamp: 50,
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
        timestamp: 50,
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c1: 3},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
        timestamp: 100,
      },
      {
        baseCookie: 4,
        cookie: 5,
        lastMutationIDChanges: {c2: 2},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 4,
          },
        ],
        timestamp: 10,
      },
    ],
  ],
  [
    'c3',
    [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
        timestamp: 50,
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c3: 5},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
        timestamp: 50,
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
        timestamp: 100,
      },
      {
        baseCookie: 4,
        cookie: 5,
        lastMutationIDChanges: {},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 4,
          },
        ],
        timestamp: 10,
      },
    ],
  ],
  [
    'c4',
    [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
        timestamp: 50,
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
        timestamp: 50,
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
        timestamp: 100,
      },
      {
        baseCookie: 4,
        cookie: 5,
        lastMutationIDChanges: {},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 4,
          },
        ],
        timestamp: 10,
      },
    ],
  ],
];

describe('processRoom', () => {
  type Case = {
    name: string;
    clientRecords: ClientRecordMap;
    headVersion: Version;
    clients: ClientMap;
    storedConnectedClients: ClientID[];
    pendingMutations: PendingMutation[];
    expectedError?: string;
    expectedPokes?: Map<ClientID, Poke[]>;
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
      storedConnectedClients: ['c1'],
      expectedUserValues: new Map(),
      expectedError: 'Error: Client record not found: c1',
      expectedVersion: 42,
    },
    {
      name: 'no mutations, clients out of date',
      clientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1'})],
        ['c2', clientRecord({clientGroupID: 'cg1'})],
        ['c3', clientRecord({clientGroupID: 'cg2'})],
      ]),
      headVersion: 2,
      clients: new Map([
        client('c1', 'u1', 'cg1'),
        client('c2', 'u2', 'cg1'),
        client('c3', 'u3', 'cg2'),
      ]),
      storedConnectedClients: ['c1', 'c2', 'c3'],
      pendingMutations: [],
      expectedPokes: new Map<ClientID, Poke[]>([
        [
          'c1',
          [
            {
              baseCookie: null,
              cookie: 2,
              lastMutationIDChanges: {c1: 1, c2: 1},
              presence: [],
              patch: [],
              timestamp: undefined,
            },
          ],
        ],
        [
          'c2',
          [
            {
              baseCookie: null,
              cookie: 2,
              lastMutationIDChanges: {c1: 1, c2: 1},
              presence: [],
              patch: [],
              timestamp: undefined,
            },
          ],
        ],
        [
          'c3',
          [
            {
              baseCookie: null,
              cookie: 2,
              lastMutationIDChanges: {c3: 1},
              presence: [],
              patch: [],
              timestamp: undefined,
            },
          ],
        ],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 2})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 2})],
        ['c3', clientRecord({clientGroupID: 'cg2', baseCookie: 2})],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 2,
    },
    {
      name: 'no mutations, one client out of date',
      clientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 2})],
        ['c2', clientRecord({clientGroupID: 'cg1'})],
        ['c3', clientRecord({clientGroupID: 'cg2', baseCookie: 2})],
      ]),
      headVersion: 2,
      clients: new Map([
        client('c1', 'u1', 'cg1'),
        client('c2', 'u2', 'cg1'),
        client('c3', 'u3', 'cg2'),
      ]),
      storedConnectedClients: ['c1', 'c2', 'c3'],
      pendingMutations: [],
      expectedPokes: new Map<ClientID, Poke[]>([
        [
          'c2',
          [
            {
              baseCookie: null,
              cookie: 2,
              lastMutationIDChanges: {c1: 1, c2: 1},
              presence: [],
              patch: [],
              timestamp: undefined,
            },
          ],
        ],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 2})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 2})],
        ['c3', clientRecord({clientGroupID: 'cg2', baseCookie: 2})],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 2,
    },
    {
      name: 'one mutation',
      clientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 1})],
      ]),
      headVersion: 1,
      clients: new Map([client('c1', 'u1', 'cg1')]),
      storedConnectedClients: ['c1'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 300,
          name: 'inc',
        }),
      ],
      expectedPokes: new Map<ClientID, Poke[]>([
        [
          'c1',
          [
            {
              baseCookie: 1,
              cookie: 2,
              lastMutationIDChanges: {c1: 2},
              presence: [],
              patch: [
                {
                  key: 'count',
                  op: 'put',
                  value: 1,
                },
              ],
              timestamp: 300,
            },
          ],
        ],
      ]),
      expectedClientRecords: new Map([
        [
          'c1',
          clientRecord({
            clientGroupID: 'cg1',
            baseCookie: 2,
            lastMutationID: 2,
            lastMutationIDVersion: 2,
          }),
        ],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 2,
    },
    {
      name: 'mutations before range are included',
      clientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 1})],
      ]),
      headVersion: 1,
      clients: new Map([client('c1', 'u1', 'cg1')]),
      storedConnectedClients: ['c1'],
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
      expectedPokes: new Map<ClientID, Poke[]>([
        [
          'c1',
          [
            {
              baseCookie: 1,
              cookie: 2,
              lastMutationIDChanges: {c1: 2},
              presence: [],
              patch: [
                {
                  op: 'put',
                  key: 'count',
                  value: 1,
                },
              ],
              timestamp: 50,
            },
            {
              baseCookie: 2,
              cookie: 3,
              lastMutationIDChanges: {c1: 3},
              presence: [],
              patch: [
                {
                  op: 'put',
                  key: 'count',
                  value: 2,
                },
              ],
              timestamp: 100,
            },
          ],
        ],
      ]),
      expectedClientRecords: new Map([
        [
          'c1',
          clientRecord({
            clientGroupID: 'cg1',
            baseCookie: 3,
            lastMutationID: 3,
            lastMutationIDVersion: 3,
          }),
        ],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 3,
    },
    {
      name: 'mutations in different client groups',
      clientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 1})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 1})],
        [
          'c3',
          clientRecord({
            clientGroupID: 'cg2',
            baseCookie: 1,
            lastMutationID: 4,
            lastMutationIDVersion: 1,
          }),
        ],
        ['c4', clientRecord({clientGroupID: 'cg3', baseCookie: 1})],
      ]),
      headVersion: 1,
      clients: new Map([
        client('c1', 'u1', 'cg1'),
        client('c2', 'u2', 'cg1'),
        client('c3', 'u3', 'cg2'),
        client('c4', 'u4', 'cg3'),
      ]),
      storedConnectedClients: ['c1', 'c2', 'c3', 'c4'],
      pendingMutations: pendingMutations1,
      expectedPokes: new Map(expectedPokesForPendingMutations1),
      expectedClientRecords: new Map([
        [
          'c1',
          clientRecord({
            clientGroupID: 'cg1',
            baseCookie: 5,
            lastMutationID: 3,
            lastMutationIDVersion: 4,
          }),
        ],
        [
          'c2',
          clientRecord({
            clientGroupID: 'cg1',
            baseCookie: 5,
            lastMutationID: 2,
            lastMutationIDVersion: 5,
          }),
        ],
        [
          'c3',
          clientRecord({
            clientGroupID: 'cg2',
            baseCookie: 5,
            lastMutationID: 5,
            lastMutationIDVersion: 3,
          }),
        ],
        ['c4', clientRecord({clientGroupID: 'cg3', baseCookie: 5})],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 5,
    },
    {
      name: '2 clients need to be fast forwarded, and mutations in different client groups',
      clientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: null})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 1})],
        [
          'c3',
          clientRecord({
            clientGroupID: 'cg2',
            baseCookie: null,
            lastMutationID: 4,
            lastMutationIDVersion: 1,
          }),
        ],
        ['c4', clientRecord({clientGroupID: 'cg3', baseCookie: 1})],
      ]),
      headVersion: 1,
      clients: new Map([
        client('c1', 'u1', 'cg1'),
        client('c2', 'u2', 'cg1'),
        client('c3', 'u3', 'cg2'),
        client('c4', 'u4', 'cg3'),
      ]),
      storedConnectedClients: ['c1', 'c2', 'c3', 'c4'],
      pendingMutations: pendingMutations1,
      expectedPokes: (() => {
        const pokesByClientID = new Map(expectedPokesForPendingMutations1);
        pokesByClientID.set('c1', [
          {
            baseCookie: null,
            cookie: 1,
            lastMutationIDChanges: {
              c1: 1,
              c2: 1,
            },
            presence: [],
            patch: [],
            timestamp: undefined,
          },
          ...(pokesByClientID.get('c1') ?? []),
        ]);
        pokesByClientID.set('c3', [
          {
            baseCookie: null,
            cookie: 1,
            lastMutationIDChanges: {
              c3: 4,
            },
            presence: [],
            patch: [],
            timestamp: undefined,
          },
          ...(pokesByClientID.get('c3') ?? []),
        ]);
        return pokesByClientID;
      })(),
      expectedClientRecords: new Map([
        [
          'c1',
          clientRecord({
            clientGroupID: 'cg1',
            baseCookie: 5,
            lastMutationID: 3,
            lastMutationIDVersion: 4,
          }),
        ],
        [
          'c2',
          clientRecord({
            clientGroupID: 'cg1',
            baseCookie: 5,
            lastMutationID: 2,
            lastMutationIDVersion: 5,
          }),
        ],
        [
          'c3',
          clientRecord({
            clientGroupID: 'cg2',
            baseCookie: 5,
            lastMutationID: 5,
            lastMutationIDVersion: 3,
          }),
        ],
        ['c4', clientRecord({clientGroupID: 'cg3', baseCookie: 5})],
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
            await tx.set('count', count);
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
      await putConnectedClients(new Set(c.storedConnectedClients), storage);

      const p = processRoom(
        createSilentLogContext(),
        env,
        c.clients,
        c.pendingMutations,
        c.pendingMutations.length,
        mutators,
        () => Promise.resolve(),
        () => Promise.resolve(),
        storage,
        () => true,
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
        expect(
          await getClientRecord(clientID, IncludeDeleted.Include, storage),
        ).toEqual(record);
      }

      for (const [key, value] of c.expectedUserValues ?? new Map()) {
        expect(await getUserValue(key, storage)).toEqual(value);
      }

      expect(await getVersion(storage)).toEqual(c.expectedVersion);
    });
  }
});
