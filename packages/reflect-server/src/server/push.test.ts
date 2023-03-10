import {describe, test, expect} from '@jest/globals';
import {LogContext} from '@rocicorp/logger';
import type {Mutation} from 'reflect-protocol';
import {handlePush} from '../server/push.js';
import {resolver} from '../util/resolver.js';
import {randomID} from '../util/rand.js';
import {
  client,
  clientRecord,
  Mocket,
  mutation,
  pendingMutation,
  SilentLogSink,
} from '../util/test-utils.js';
import type {ClientID, ClientMap, ClientState} from '../types/client-state.js';
import type {PendingMutation} from '../types/mutation.js';
import {
  ClientRecordMap,
  listClientRecords,
  putClientRecord,
} from '../types/client-record.js';
import {DurableStorage} from '../storage/durable-storage.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

const s1: Mocket = new Mocket();
const clientID = 'c1';
const clientGroupID = 'cg1';

function clientMapSansSockets(
  clientMap: ClientMap,
): Map<ClientID, Omit<ClientState, 'socket'>> {
  return new Map(
    [...clientMap.entries()].map(([clientID, clientState]) => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const {socket, ...clientStateSansSocket} = clientState;
      return [clientID, clientStateSansSocket];
    }),
  );
}

describe('handlePush', () => {
  type Case = {
    name: string;
    clientMap: ClientMap;
    pendingMutations: PendingMutation[];
    clientRecords: ClientRecordMap;
    mutations: Mutation[];
    now?: number;
    pushTimestamp?: number;
    expectedClientMap?: ClientMap;
    expectedPendingMutations: PendingMutation[];
    expectedClientRecords?: ClientRecordMap;
    expectedErrorAndSocketClosed?: string;
  };

  const cases: Case[] = [
    {
      name: 'no mutations',
      clientMap: new Map([client(clientID, 'u1', 'cg1', s1, 0)]),
      pendingMutations: [],
      mutations: [],
      clientRecords: new Map([[clientID, clientRecord('cg1', 1, 2, 1)]]),
      expectedPendingMutations: [],
    },
    {
      name: 'empty pending, single mutation',
      clientMap: new Map([client(clientID, 'u1', 'cg1', s1, 0)]),
      pendingMutations: [],
      mutations: [mutation(clientID, 3, 10)],
      clientRecords: new Map([[clientID, clientRecord('cg1', 1, 2, 1)]]),
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamp: 10,
          pusherClientIDs: new Set([clientID]),
        }),
      ],
    },
    {
      name: 'empty pending, multiple mutations',
      clientMap: new Map([
        client(clientID, 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1'),
      ]),
      pendingMutations: [],
      mutations: [
        mutation(clientID, 3, 10),
        mutation('c2', 5, 20),
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg1', 1, 4, 1)],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamp: 10,
          pusherClientIDs: new Set([clientID]),
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 5,
          timestamp: undefined,
          pusherClientIDs: new Set([clientID]),
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 4,
          timestamp: 20,
          pusherClientIDs: new Set([clientID]),
        }),
      ],
    },

    {
      name: 'empty pending, multiple mutations, new client',
      clientMap: new Map([client(clientID, 'u1', 'cg1', s1, 0)]),
      pendingMutations: [],
      mutations: [
        mutation(clientID, 3, 10),
        mutation('c2', 1, 20),
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([[clientID, clientRecord('cg1', 1, 2, 1)]]),
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamp: 10,
          pusherClientIDs: new Set([clientID]),
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 1,
          timestamp: undefined,
          pusherClientIDs: new Set([clientID]),
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 4,
          timestamp: 20,
          pusherClientIDs: new Set([clientID]),
        }),
      ],
      expectedClientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg1', null, 0, null)],
      ]),
    },
    {
      name: 'already applied according to client record',
      clientMap: new Map([
        client(clientID, 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1'),
      ]),
      pendingMutations: [],
      mutations: [
        mutation(clientID, 3, 10), // already applied
        mutation('c2', 5, 20),
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 3, 1)],
        ['c2', clientRecord('cg1', 1, 4, 1)],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 5,
          timestamp: undefined,
          pusherClientIDs: new Set([clientID]),
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 4,
          timestamp: 20,
          pusherClientIDs: new Set([clientID]),
        }),
      ],
    },
    {
      name: 'pending duplicates',
      clientMap: new Map([
        client(clientID, 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1'),
      ]),
      pendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamp: 10,
          pusherClientIDs: new Set(['c3']),
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 4,
          timestamp: 20,
          pusherClientIDs: new Set(['c3']),
        }),
      ],
      mutations: [
        mutation(clientID, 3, 10),
        mutation('c2', 5, 20),
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg1', 1, 4, 1)],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamp: 10,
          pusherClientIDs: new Set([clientID, 'c3']),
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 5,
          timestamp: undefined,
          pusherClientIDs: new Set([clientID]),
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 4,
          timestamp: 20,
          pusherClientIDs: new Set([clientID, 'c3']),
        }),
      ],
    },
    {
      name: 'unexpected client group id is an error',
      clientMap: new Map([
        client(clientID, 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg2'),
      ]),
      pendingMutations: [],
      mutations: [
        mutation(clientID, 3, 10),
        mutation('c2', 5, 20),
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg2', 1, 4, 1)],
      ]),
      // no mutations enqueued
      expectedPendingMutations: [],
      expectedErrorAndSocketClosed:
        'Push for client c1 with clientGroupID cg1 contains mutation for client c2 which belongs to clientGroupID cg2.',
    },
    {
      name: 'unexpected mutation id for new client is an error, client not recorded',
      clientMap: new Map([client(clientID, 'u1', 'cg1', s1, 0)]),
      pendingMutations: [],
      mutations: [
        mutation(clientID, 3, 10),
        mutation('c2', 2, 20), // 1 is expected
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([[clientID, clientRecord('cg1', 1, 2, 1)]]),
      // no mutations enqueued
      expectedPendingMutations: [],
      // new client not recorded, so no expectedClientRecords
      expectedErrorAndSocketClosed:
        'Push contains unexpected mutation id 2 for client c2. Expected mutation id 1.',
    },
    {
      name: 'unexpected mutation id for existing client',
      clientMap: new Map([client(clientID, 'u1', 'cg1', s1, 0)]),
      pendingMutations: [],
      mutations: [
        mutation(clientID, 3, 10),
        mutation('c2', 6, 20), // 5 is expected
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([
        [clientID, clientRecord('cg1', 1, 2, 1)],
        ['c2', clientRecord('cg1', 1, 4, 1)],
      ]),
      // no mutations enqueued
      expectedPendingMutations: [],
      // new client not recorded, so no expectedClientRecords
      expectedErrorAndSocketClosed:
        'Push contains unexpected mutation id 6 for client c2. Expected mutation id 5.',
    },
    // TODO tests for timestamp adjustments
  ];

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
    test(c.name, async () => {
      const durable = await getMiniflareDurableObjectStorage(id);
      await durable.deleteAll();
      const storage = new DurableStorage(durable);
      s1.log.length = 0;

      for (const [clientID, record] of c.clientRecords) {
        await putClientRecord(clientID, record, storage);
      }
      expect(await listClientRecords(storage)).toEqual(c.clientRecords);

      const requestID = randomID();
      const push = {
        clientGroupID,
        mutations: c.mutations,
        pushVersion: 1,
        schemaVersion: '',
        timestamp: 42,
        requestID,
      };

      const lc = new TestLogContext('info', new SilentLogSink());
      const pendingMutationsPrePush = [...c.pendingMutations];
      const clientMapPrePush = new Map(c.clientMap);
      const clientRecordsPrePush = new Map(c.clientRecords);
      let processUntilDoneCallCount = 0;
      await handlePush(
        lc,
        storage,
        clientID,
        c.clientMap,
        c.pendingMutations,
        push,
        () => 42,
        () => {
          processUntilDoneCallCount++;
        },
      );

      expect(await lc.resolver.promise).toEqual(requestID);
      expect(clientMapSansSockets(c.clientMap)).toEqual(
        clientMapSansSockets(c.expectedClientMap ?? clientMapPrePush),
      );
      if (c.expectedErrorAndSocketClosed !== undefined) {
        expect(processUntilDoneCallCount).toEqual(0);
        expect(s1.log.length).toEqual(2);
        const [type, message] = s1.log[0];
        expect(type).toEqual('send');
        expect(message).toContain(c.expectedErrorAndSocketClosed);
        expect(s1.log[1][0]).toEqual('close');
        expect(c.pendingMutations).toEqual(pendingMutationsPrePush);
        expect(await listClientRecords(storage)).toEqual(clientRecordsPrePush);
      } else {
        expect(processUntilDoneCallCount).toEqual(1);
        expect(s1.log).toEqual([]);
        expect(c.pendingMutations).toEqual(c.expectedPendingMutations);
        expect(await listClientRecords(storage)).toEqual(
          c.expectedClientRecords ?? clientRecordsPrePush,
        );
      }
    });
  }
});
