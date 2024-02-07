import {describe, expect, test} from '@jest/globals';
import {LogContext} from '@rocicorp/logger';
import type {Mutation, PushBody} from 'reflect-protocol';
import {handlePush} from '../server/push.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {
  ClientRecordMap,
  listClientRecords,
  putClientRecord,
} from '../types/client-record.js';
import type {ClientID, ClientMap, ClientState} from '../types/client-state.js';
import type {PendingMutation} from '../types/mutation.js';
import {randomID} from '../util/rand.js';
import {resolver} from '../util/resolver.js';
import {
  Mocket,
  SilentLogSink,
  client,
  clientRecord,
  mutation,
  pendingMutation,
} from '../util/test-utils.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

const startTime = 50;
const s1: Mocket = new Mocket();
const clientID = 'c1';
const clientGroupID = 'cg1';

function timestamps(timestamp: number): {
  normalizedTimestamp: number;
  originTimestamp: number;
  serverReceivedTimestamp: number;
} {
  return {
    normalizedTimestamp: timestamp,
    originTimestamp: timestamp,
    serverReceivedTimestamp: startTime,
  };
}

function clientMapSansSockets(
  clientMap: ClientMap,
): Map<ClientID, Omit<ClientState, 'socket'>> {
  return new Map(
    [...clientMap.entries()].map(([clientID, clientState]) => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const {socket: _, ...clientStateSansSocket} = clientState;
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
      clientMap: new Map([client(clientID, 'u1', clientGroupID, s1, 0)]),
      pendingMutations: [],
      mutations: [],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      expectedPendingMutations: [],
    },
    {
      name: 'empty pending, single mutation',
      clientMap: new Map([client(clientID, 'u1', clientGroupID, s1, 0)]),
      pendingMutations: [],
      mutations: [mutation(clientID, 3, 10)],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamps: timestamps(10),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
      ],
    },
    {
      name: 'empty pending, multiple mutations',
      clientMap: new Map([client(clientID, 'u1', clientGroupID, s1, 0)]),
      pendingMutations: [],
      mutations: [mutation(clientID, 3, 10), mutation(clientID, 4, 20)],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamps: timestamps(10),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 4,
          timestamps: timestamps(20),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
      ],
    },
    {
      name: 'empty pending, multiple mutations, no timestamps for old mutations relative to push timestamp',
      clientMap: new Map([client(clientID, 'u1', clientGroupID, s1, 0)]),
      pendingMutations: [],
      mutations: [
        mutation(clientID, 3, 10),
        mutation(clientID, 4, 50),
        mutation(clientID, 5, 51),
      ],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      pushTimestamp: 100,
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamps: undefined,
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 4,
          timestamps: undefined,
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 5,
          timestamps: timestamps(51),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
      ],
    },
    {
      name: 'empty pending, multiple mutations, no timestamps for mutations from other clients',
      clientMap: new Map([
        client(clientID, 'u1', clientGroupID, s1, 0),
        client('c2', 'u2', clientGroupID),
      ]),
      pendingMutations: [],
      mutations: [
        mutation(clientID, 3, 10),
        mutation('c2', 5, 20),
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
        [
          'c2',
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 4,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamps: timestamps(10),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 5,
          timestamps: undefined,
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 4,
          timestamps: timestamps(20),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
      ],
    },
    {
      name: 'empty pending, multiple mutations, new client',
      clientMap: new Map([client(clientID, 'u1', clientGroupID, s1, 0)]),
      pendingMutations: [],
      mutations: [
        mutation(clientID, 3, 10),
        mutation('c2', 1, 20),
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
            lastSeen: 50,
            userID: 'u1',
          }),
        ],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamps: timestamps(10),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 1,
          timestamps: undefined,
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 4,
          timestamps: timestamps(20),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
      ],
      expectedClientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
            lastSeen: 50,
            userID: 'u1',
          }),
        ],
        [
          'c2',
          clientRecord({
            clientGroupID,
            baseCookie: null,
            lastMutationID: 0,
            lastMutationIDVersion: null,
            lastSeen: 50,
            userID: 'u1',
          }),
        ],
      ]),
    },
    {
      name: 'already applied according to client record',
      clientMap: new Map([
        client(clientID, 'u1', clientGroupID, s1, 0),
        client('c2', 'u2', clientGroupID),
      ]),
      pendingMutations: [],
      mutations: [
        mutation(clientID, 3, 10), // already applied
        mutation('c2', 5, 20),
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 3,
            lastMutationIDVersion: 1,
          }),
        ],
        [
          'c2',
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 4,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 5,
          timestamps: undefined,
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 4,
          timestamps: timestamps(20),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
      ],
    },
    {
      name: 'pending duplicates',
      clientMap: new Map([
        client(clientID, 'u1', clientGroupID, s1, 0),
        client('c2', 'u2', clientGroupID),
      ]),
      pendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamps: timestamps(10),
          pusherClientIDs: new Set(['c3']),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 4,
          timestamps: timestamps(20),
          pusherClientIDs: new Set(['c3']),
          auth: {userID: 'u1'},
        }),
      ],
      mutations: [
        mutation(clientID, 3, 10),
        mutation('c2', 5, 20),
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
        [
          'c2',
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 4,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamps: timestamps(10),
          pusherClientIDs: new Set([clientID, 'c3']),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 5,
          timestamps: undefined,
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 4,
          timestamps: timestamps(20),
          pusherClientIDs: new Set([clientID, 'c3']),
          auth: {userID: 'u1'},
        }),
      ],
    },
    {
      name: 'unexpected client group id is an error',
      clientMap: new Map([
        client(clientID, 'u1', clientGroupID, s1, 0),
        client('c2', 'u2', 'cg2'),
      ]),
      pendingMutations: [],
      mutations: [
        mutation(clientID, 3, 10),
        mutation('c2', 5, 20),
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
        [
          'c2',
          clientRecord({
            clientGroupID: 'cg2',
            baseCookie: 1,
            lastMutationID: 4,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      // no mutations enqueued
      expectedPendingMutations: [],
      expectedErrorAndSocketClosed:
        'Push for client c1 with clientGroupID cg1 contains mutation for client c2 which belongs to clientGroupID cg2.',
    },
    {
      name: 'unexpected mutation id for new client is an error, client not recorded',
      clientMap: new Map([client(clientID, 'u1', clientGroupID, s1, 0)]),
      pendingMutations: [],
      mutations: [
        mutation(clientID, 3, 10),
        mutation('c2', 2, 20), // 1 is expected
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      // no mutations enqueued
      expectedPendingMutations: [],
      // new client not recorded, so no expectedClientRecords
      expectedErrorAndSocketClosed:
        'Push contains unexpected mutation id 2 for client c2. Expected mutation id 1.',
    },
    {
      name: 'unexpected mutation id for existing client',
      clientMap: new Map([client(clientID, 'u1', clientGroupID, s1, 0)]),
      pendingMutations: [],
      mutations: [
        mutation(clientID, 3, 10),
        mutation('c2', 6, 20), // 5 is expected
        mutation(clientID, 4, 20),
      ],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
        [
          'c2',
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 4,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      // no mutations enqueued
      expectedPendingMutations: [],
      // new client not recorded, so no expectedClientRecords
      expectedErrorAndSocketClosed:
        'Push contains unexpected mutation id 6 for client c2. Expected mutation id 5.',
    },
    // clock offset tests
    {
      name: 'clock offset is initialized if undefined',
      clientMap: new Map([
        client(clientID, 'u1', clientGroupID, s1, undefined),
      ]),
      pendingMutations: [],
      mutations: [mutation(clientID, 3, 10)],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      pushTimestamp: 5,
      now: 500,
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamps: {
            normalizedTimestamp: 10 + 495,
            originTimestamp: 10,
            serverReceivedTimestamp: 500,
          },
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
      ],
      expectedClientMap: new Map([
        client(clientID, 'u1', clientGroupID, s1, 495),
      ]),
    },
    {
      name: 'uses existing clock offset',
      clientMap: new Map([client(clientID, 'u1', clientGroupID, s1, 495)]),
      pendingMutations: [],
      mutations: [mutation(clientID, 3, 10)],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      pushTimestamp: 5,
      now: 700, // offset would be 695 if not reused
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamps: {
            normalizedTimestamp: 10 + 495,
            originTimestamp: 10,
            serverReceivedTimestamp: 700,
          },
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
      ],
      expectedClientMap: new Map([
        client(clientID, 'u1', clientGroupID, s1, 495),
      ]),
    },
    {
      name: 'resets clock offset if changes by greater than 1 second',
      clientMap: new Map([client(clientID, 'u1', clientGroupID, s1, 100)]),
      pendingMutations: [],
      mutations: [mutation(clientID, 3, 10)],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      pushTimestamp: 5,
      now: 1110,
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 3,
          timestamps: {
            normalizedTimestamp: 10 + 1105,
            originTimestamp: 10,
            serverReceivedTimestamp: 1110,
          },
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
      ],
      expectedClientMap: new Map([
        client(clientID, 'u1', clientGroupID, s1, 1105),
      ]),
    },
    {
      name: 'if push has an error clock offset is not initialized',
      clientMap: new Map([
        client(clientID, 'u1', clientGroupID, s1, undefined),
      ]),
      pendingMutations: [],
      mutations: [mutation(clientID, 3, 10), mutation(clientID, 5, 20)],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      pushTimestamp: 5,
      now: 500,
      // no mutations enqueued
      expectedPendingMutations: [],
      expectedErrorAndSocketClosed:
        'Push contains unexpected mutation id 5 for client c1. Expected mutation id 4.',
    },
    // end clock offset tests
    {
      name: 'orders by normalized timestamp when possible',
      clientMap: new Map([
        client(clientID, 'u1', clientGroupID, s1, 0),
        client('c2', 'u2', clientGroupID, undefined, 0),
      ]),
      pendingMutations: [
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 3,
          timestamps: timestamps(10),
          pusherClientIDs: new Set(['c2']),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 4,
          timestamps: timestamps(30),
          pusherClientIDs: new Set(['c2']),
          auth: {userID: 'u1'},
        }),
      ],
      mutations: [
        mutation(clientID, 5, 1),
        mutation(clientID, 6, 9),
        mutation(clientID, 7, 10),
        mutation(clientID, 8, 29),
        mutation(clientID, 9, 30),
        mutation(clientID, 10, 70),
      ],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 4,
            lastMutationIDVersion: 1,
          }),
        ],
        [
          'c2',
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 5,
          timestamps: timestamps(1),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 6,
          timestamps: timestamps(9),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 3,
          timestamps: timestamps(10),
          pusherClientIDs: new Set(['c2']),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 7,
          timestamps: timestamps(10),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 8,
          timestamps: timestamps(29),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 4,
          timestamps: timestamps(30),
          pusherClientIDs: new Set(['c2']),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 9,
          timestamps: timestamps(30),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 10,
          timestamps: timestamps(70),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
      ],
    },
    {
      name: 'orders by order pushed by pusher if in same client group',
      clientMap: new Map([
        client(clientID, 'u1', clientGroupID, s1, 0),
        client('c2', 'u2', clientGroupID, undefined, 0),
      ]),
      pendingMutations: [
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 3,
          timestamps: timestamps(10),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 4,
          timestamps: timestamps(30),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
      ],
      mutations: [
        mutation(clientID, 5, 5),
        mutation('c2', 5, 20),
        mutation(clientID, 6, 25),
        mutation(clientID, 7, 80),
        mutation(clientID, 8, 70),
      ],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 4,
            lastMutationIDVersion: 1,
          }),
        ],
        [
          'c2',
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 3,
          timestamps: timestamps(10),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 4,
          timestamps: timestamps(30),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 5,
          timestamps: timestamps(5),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID,
          id: 5,
          timestamps: undefined,
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 6,
          timestamps: timestamps(25),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 7,
          timestamps: timestamps(80),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 8,
          timestamps: timestamps(70),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
      ],
    },
    {
      name: 'does not by order pushed by pusher if different client group',
      clientMap: new Map([
        client(clientID, 'u1', clientGroupID, s1, 0),
        client('c2', 'u2', 'cg2', undefined, 0),
      ]),
      pendingMutations: [
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg2',
          id: 3,
          timestamps: timestamps(10),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u2'},
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg2',
          id: 4,
          timestamps: timestamps(30),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u2'},
        }),
      ],
      mutations: [
        mutation(clientID, 5, 5),
        mutation(clientID, 6, 25),
        mutation(clientID, 7, 80),
        mutation(clientID, 8, 70),
      ],
      clientRecords: new Map([
        [
          clientID,
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 4,
            lastMutationIDVersion: 1,
          }),
        ],
        [
          'c2',
          clientRecord({
            clientGroupID,
            baseCookie: 1,
            lastMutationID: 2,
            lastMutationIDVersion: 1,
          }),
        ],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID,
          clientGroupID,
          id: 5,
          timestamps: timestamps(5),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg2',
          id: 3,
          timestamps: timestamps(10),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u2'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 6,
          timestamps: timestamps(25),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg2',
          id: 4,
          timestamps: timestamps(30),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u2'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 7,
          timestamps: timestamps(80),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
        pendingMutation({
          clientID,
          clientGroupID,
          id: 8,
          timestamps: timestamps(70),
          pusherClientIDs: new Set([clientID]),
          auth: {userID: 'u1'},
        }),
      ],
    },
  ];

  // Special LC that waits for a requestID to be added to the context.
  class TestLogContext extends LogContext {
    resolver = resolver<unknown>();

    withContext(key: string, value?: unknown): LogContext {
      if (key === 'requestID') {
        this.resolver.resolve(value);
      }
      return super.withContext(key, value);
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
      const push: PushBody = {
        clientGroupID,
        mutations: c.mutations,
        pushVersion: 1,
        schemaVersion: '',
        timestamp: c.pushTimestamp ?? startTime,
        requestID,
      };

      const lc = new TestLogContext('info', undefined, new SilentLogSink());
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
        () => c.now ?? startTime,
        () => {
          processUntilDoneCallCount++;
        },
      );

      expect(await lc.resolver.promise).toEqual(requestID);
      if (c.expectedErrorAndSocketClosed !== undefined) {
        expect(processUntilDoneCallCount).toEqual(0);
        expect(s1.log.length).toEqual(2);
        const [type, message] = s1.log[0];
        expect(type).toEqual('send');
        expect(message).toContain(c.expectedErrorAndSocketClosed);
        expect(s1.log[1][0]).toEqual('close');
        expect(c.pendingMutations).toEqual(pendingMutationsPrePush);
        expect(await listClientRecords(storage)).toEqual(clientRecordsPrePush);
        expect(clientMapSansSockets(c.clientMap)).toEqual(
          clientMapSansSockets(clientMapPrePush),
        );
      } else {
        expect(processUntilDoneCallCount).toEqual(1);
        expect(s1.log).toEqual([]);
        expect(c.pendingMutations).toEqual(c.expectedPendingMutations);
        expect(await listClientRecords(storage)).toEqual(
          c.expectedClientRecords ?? clientRecordsPrePush,
        );
        expect(clientMapSansSockets(c.clientMap)).toEqual(
          clientMapSansSockets(c.expectedClientMap ?? clientMapPrePush),
        );
      }
    });
  }
});
