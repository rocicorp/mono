import {test, expect, beforeEach} from '@jest/globals';
import {LogContext} from '@rocicorp/logger';
import type {Mutation} from 'reflect-protocol';
import {handlePush} from '../server/push.js';
import {resolver} from '../util/resolver.js';
import {randomID} from '../util/rand.js';
import {client, clientRecord, Mocket, mutation} from '../util/test-utils.js';
import type {ClientMap} from '../types/client-state.js';
import type {PendingMutationMap} from '../types/mutation.js';
import {ClientRecordMap, putClientRecord} from '../types/client-record.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {must} from '../util/must.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

let s1: Mocket;
beforeEach(() => {
  s1 = new Mocket();
});
const clientID = 'c1';

test('handlePush', async () => {
  type Case = {
    name: string;
    clientMap: ClientMap;
    pendingMutations: PendingMutationMap;
    clientRecords: ClientRecordMap;
    mutations: Mutation[];
    expectedPendingMutations: PendingMutationMap;
    expectedClientRecords: ClientRecordMap;
    expectedErrorAndSocketClosed?: string;
  };

  const cases: Case[] = [
    {
      name: 'no mutations',
      clientMap: new Map([client(clientID, 'u1', 'cg1', s1)]),
      pendingMutations: new Map(),
      mutations: [],
      clientRecords: new Map([[clientID, clientRecord('cg1', 1, 2, 1)]]),
      expectedPendingMutations: new Map(),
      expectedClientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
      ]),
    },
    {
      name: 'empty pending, single mutation',
      clientMap: new Map([client(clientID, 'u1', 'cg1', s1)]),
      pendingMutations: new Map(),
      mutations: [mutation(clientID, 3)],
      clientRecords: new Map([[clientID, clientRecord('cg1', 1, 2, 1)]]),
      expectedPendingMutations: new Map([['cg1', [mutation(clientID, 3)]]]),
      expectedClientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
      ]),
    },
    {
      name: 'empty pending, multiple mutations',
      clientMap: new Map([
        client(clientID, 'u1', 'cg1', s1),
        client('c2', 'u2', 'cg1'),
      ]),
      pendingMutations: new Map(),
      mutations: [
        mutation(clientID, 3),
        mutation('c2', 5),
        mutation(clientID, 4),
      ],
      clientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg1', 1, 4, 1)],
      ]),
      expectedPendingMutations: new Map([
        [
          'cg1',
          [mutation(clientID, 3), mutation('c2', 5), mutation(clientID, 4)],
        ],
      ]),
      expectedClientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg1', 1, 4, 1)],
      ]),
    },

    {
      name: 'empty pending, multiple mutations, new client',
      clientMap: new Map([client(clientID, 'u1', 'cg1', s1)]),
      pendingMutations: new Map(),
      mutations: [
        mutation(clientID, 3),
        mutation('c2', 1),
        mutation(clientID, 4),
      ],
      clientRecords: new Map([[clientID, clientRecord('cg1', 1, 2, 1)]]),
      expectedPendingMutations: new Map([
        [
          'cg1',
          [mutation(clientID, 3), mutation('c2', 1), mutation(clientID, 4)],
        ],
      ]),
      expectedClientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg1', null, 0, null)],
      ]),
    },
    {
      name: 'already applied according to client record',
      clientMap: new Map([
        client(clientID, 'u1', 'cg1', s1),
        client('c2', 'u2', 'cg1'),
      ]),
      pendingMutations: new Map(),
      mutations: [
        mutation(clientID, 3), // already applied
        mutation('c2', 5),
        mutation(clientID, 4),
      ],
      clientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 3, 1)],
        ['c2', clientRecord('cg1', 1, 4, 1)],
      ]),
      expectedPendingMutations: new Map([
        ['cg1', [mutation(clientID, 4), mutation('c2', 5)]],
      ]),
      expectedClientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg1', 1, 4, 1)],
      ]),
    },
    {
      name: 'pending duplicates',
      clientMap: new Map([
        client(clientID, 'u1', 'cg1', s1),
        client('c2', 'u2', 'cg1'),
      ]),
      pendingMutations: new Map([
        ['cg1', [mutation(clientID, 3), mutation(clientID, 4)]],
      ]),
      mutations: [
        mutation(clientID, 3),
        mutation('c2', 5),
        mutation(clientID, 4),
      ],
      clientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg1', 1, 4, 1)],
      ]),
      expectedPendingMutations: new Map([
        [
          'cg1',
          [mutation(clientID, 3), mutation(clientID, 4), mutation('c2', 5)],
        ],
      ]),
      expectedClientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg1', 1, 4, 1)],
      ]),
    },
    {
      name: 'unexpected client group id is an error',
      clientMap: new Map([
        client(clientID, 'u1', 'cg1', s1),
        client('c2', 'u2', 'cg2'),
      ]),
      pendingMutations: new Map(),
      mutations: [
        mutation(clientID, 3),
        mutation('c2', 5),
        mutation(clientID, 4),
      ],
      clientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg2', 1, 4, 1)],
      ]),
      // no mutations enqueued
      expectedPendingMutations: new Map(),
      expectedClientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg2', 1, 4, 1)],
      ]),
      expectedErrorAndSocketClosed:
        'Push with clientGroupID cg1 contains mutation for client c2 which belongs to clientGroupID cg2.',
    },
    {
      name: 'unexpected mutation id for new client is an error, client not recorded',
      clientMap: new Map([client(clientID, 'u1', 'cg1', s1)]),
      pendingMutations: new Map(),
      mutations: [
        mutation(clientID, 3),
        mutation('c2', 2), // 1 is expected
        mutation(clientID, 4),
      ],
      clientRecords: new Map([[clientID, clientRecord('cg1', 1, 2, 1)]]),
      // no mutations enqueued
      expectedPendingMutations: new Map(),
      // new client not recorded
      expectedClientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
      ]),
      expectedErrorAndSocketClosed:
        'Push contains unexpected mutation id 2 for client c2. Expected mutation id 1.',
    },
    {
      name: 'unexpected mutation id for existing client',
      clientMap: new Map([client(clientID, 'u1', 'cg1', s1)]),
      pendingMutations: new Map(),
      mutations: [
        mutation(clientID, 3),
        mutation('c2', 6), // 5 is expected
        mutation(clientID, 4),
      ],
      clientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg1', 1, 4, 1)],
      ]),
      // no mutations enqueued
      expectedPendingMutations: new Map(),
      // new client not recorded
      expectedClientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg1', 1, 4, 1)],
      ]),
      expectedErrorAndSocketClosed:
        'Push contains unexpected mutation id 6 for client c2. Expected mutation id 5.',
    },
    // TODO tests for timestamp adjustments
  ];
  const durable = await getMiniflareDurableObjectStorage(id);

  // Special LC that waits for a requestID to be added to the context.
  class TestLogContext extends LogContext {
    resolver = resolver<unknown>();

    addContext(key: string, value?: unknown): LogContext {
      if (key === 'requestID') {
        this.resolver.resolve(value);
      }
      return super.addContext(key, value);
    }
  }

  for (const c of cases) {
    s1.log.length = 0;
    await durable.deleteAll();

    const storage = new DurableStorage(durable);
    for (const [clientID, record] of c.clientRecords) {
      await putClientRecord(clientID, record, storage);
    }

    const requestID = randomID();
    const push = {
      clientGroupID: 'cg1',
      mutations: c.mutations,
      pushVersion: 1,
      schemaVersion: '',
      timestamp: 42,
      requestID,
    };

    const lc = new TestLogContext();
    await handlePush(
      lc,
      storage,
      must(c.clientMap.get(clientID)),
      c.clientMap,
      c.pendingMutations,
      push,
      () => 42,
      () => undefined,
    );

    expect(await lc.resolver.promise).toEqual(requestID);
    if (c.expectedErrorAndSocketClosed !== undefined) {
      expect(s1.log.length).toEqual(2);
      const [type, message] = s1.log[0];
      expect(type).toEqual('send');
      expect(message).toContain(c.expectedErrorAndSocketClosed);
      expect(s1.log[1][0]).toEqual('close');
    } else {
      expect(s1.log).toEqual([]);
    }
  }
});
