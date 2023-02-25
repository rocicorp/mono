import {test, expect} from '@jest/globals';
import type {WriteTransaction} from 'replicache';
import {DurableStorage} from '../storage/durable-storage.js';
import type {ClientPokeBody} from '../types/client-poke-body.js';
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
  mutation,
  clientRecord,
  createSilentLogContext,
  fail,
  mockMathRandom,
} from '../util/test-utils.js';
import {processRoom} from '../process/process-room.js';
import type {PendingMutationMap} from '../types/mutation.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

mockMathRandom();

test('processRoom', async () => {
  type Case = {
    name: string;
    clientRecords: ClientRecordMap;
    headVersion: Version;
    clients: ClientMap;
    pendingMutations: PendingMutationMap;
    expectedError?: string;
    expectedPokes?: ClientPokeBody[];
    expectedUserValues?: Map<string, UserValue>;
    expectedClientRecords?: ClientRecordMap;
    expectedVersion: Version;
  };

  const startTime = 100;

  const cases: Case[] = [
    {
      name: 'no client record',
      clientRecords: new Map(),
      pendingMutations: new Map(),
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
      pendingMutations: new Map(),
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: null,
            cookie: 2,
            lastMutationIDChanges: {c1: 1, c2: 1},
            patch: [],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: null,
            cookie: 2,
            lastMutationIDChanges: {c1: 1, c2: 1},
            patch: [],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
          },
        },
        {
          clientID: 'c3',
          poke: {
            baseCookie: null,
            cookie: 2,
            lastMutationIDChanges: {c3: 1},
            patch: [],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
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
      pendingMutations: new Map(),
      expectedPokes: [
        {
          clientID: 'c2',
          poke: {
            baseCookie: null,
            cookie: 2,
            lastMutationIDChanges: {c1: 1, c2: 1},
            patch: [],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
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
      pendingMutations: new Map([
        ['cg1', [mutation('c1', 2, 'inc', null, 300)]],
      ]),
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
            timestamp: 100,
            requestID: '4fxcm49g2j9',
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
      pendingMutations: new Map([
        [
          'cg1',
          [
            mutation('c1', 2, 'inc', null, 50),
            mutation('c1', 3, 'inc', null, 100),
          ],
        ],
      ]),
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 1,
            // even though two mutations play we only bump version at most once per frame
            cookie: 2,
            // two mutations played
            lastMutationIDChanges: {c1: 3},
            patch: [
              // two count mutations played, leaving value at 2
              {
                op: 'put',
                key: 'count',
                value: 2,
              },
            ],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
          },
        },
      ],
      expectedClientRecords: new Map([['c1', clientRecord('cg1', 2, 3, 2)]]),
      expectedUserValues: new Map(),
      expectedVersion: 2,
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
      pendingMutations: new Map([
        [
          'cg1',
          [
            mutation('c1', 2, 'inc', null, 50),
            mutation('c1', 3, 'inc', null, 100),
            mutation('c2', 2, 'inc', null, 10),
          ],
        ],
        ['cg2', [mutation('c3', 5, 'inc', null, 50)]],
      ]),
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 1,
            cookie: 2,
            lastMutationIDChanges: {c1: 3, c2: 2},
            patch: [
              // four inc mutations played, leaving value at 4
              {
                op: 'put',
                key: 'count',
                value: 4,
              },
            ],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: 1,
            cookie: 2,
            lastMutationIDChanges: {c1: 3, c2: 2},
            patch: [
              // four inc mutations played, leaving value at 4
              {
                op: 'put',
                key: 'count',
                value: 4,
              },
            ],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
          },
        },
        {
          clientID: 'c3',
          poke: {
            baseCookie: 1,
            cookie: 2,
            lastMutationIDChanges: {c3: 5},
            patch: [
              // four inc mutations played, leaving value at 4
              {
                op: 'put',
                key: 'count',
                value: 4,
              },
            ],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
          },
        },
        {
          clientID: 'c4',
          poke: {
            baseCookie: 1,
            cookie: 2,
            // no mutation id changes in cg3
            lastMutationIDChanges: {},
            patch: [
              // four inc mutations played, leaving value at 4
              {
                op: 'put',
                key: 'count',
                value: 4,
              },
            ],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
          },
        },
      ],
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 2, 3, 2)],
        ['c2', clientRecord('cg1', 2, 2, 2)],
        ['c3', clientRecord('cg2', 2, 5, 2)],
        ['c4', clientRecord('cg3', 2)],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 2,
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
      pendingMutations: new Map([
        [
          'cg1',
          [
            mutation('c1', 2, 'inc', null, 50),
            mutation('c1', 3, 'inc', null, 100),
            mutation('c2', 2, 'inc', null, 10),
          ],
        ],
        ['cg2', [mutation('c3', 5, 'inc', null, 50)]],
      ]),
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
            timestamp: 100,
            requestID: '4fxcm49g2j9',
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
            timestamp: 100,
            requestID: '4fxcm49g2j9',
          },
        },
        // process mutations pokes
        {
          clientID: 'c1',
          poke: {
            baseCookie: 1,
            cookie: 2,
            lastMutationIDChanges: {c1: 3, c2: 2},
            patch: [
              // four inc mutations played, leaving value at 4
              {
                op: 'put',
                key: 'count',
                value: 4,
              },
            ],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: 1,
            cookie: 2,
            lastMutationIDChanges: {c1: 3, c2: 2},
            patch: [
              // four inc mutations played, leaving value at 4
              {
                op: 'put',
                key: 'count',
                value: 4,
              },
            ],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
          },
        },
        {
          clientID: 'c3',
          poke: {
            baseCookie: 1,
            cookie: 2,
            lastMutationIDChanges: {c3: 5},
            patch: [
              // four inc mutations played, leaving value at 4
              {
                op: 'put',
                key: 'count',
                value: 4,
              },
            ],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
          },
        },
        {
          clientID: 'c4',
          poke: {
            baseCookie: 1,
            cookie: 2,
            // no mutation id changes in cg3
            lastMutationIDChanges: {},
            patch: [
              // four inc mutations played, leaving value at 4
              {
                op: 'put',
                key: 'count',
                value: 4,
              },
            ],
            timestamp: 100,
            requestID: '4fxcm49g2j9',
          },
        },
      ],
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 2, 3, 2)],
        ['c2', clientRecord('cg1', 2, 2, 2)],
        ['c3', clientRecord('cg2', 2, 5, 2)],
        ['c4', clientRecord('cg3', 2)],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 2,
    },
  ];
  const durable = await getMiniflareDurableObjectStorage(id);

  const mutators = new Map(
    Object.entries({
      inc: async (tx: WriteTransaction) => {
        let count = ((await tx.get('count')) as number) ?? 0;
        count++;
        await tx.put('count', count);
      },
    }),
  );

  for (const c of cases) {
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
      startTime,
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
  }
});
