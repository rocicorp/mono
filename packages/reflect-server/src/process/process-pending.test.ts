import {
  afterEach,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import type {LogContext} from '@rocicorp/logger';
import type {PokeBody, Version} from 'reflect-protocol';
import type {WriteTransaction} from 'reflect-shared/src/types.js';
import {BufferSizer} from 'shared/src/buffer-sizer.js';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import {
  ClientRecordMap,
  getClientRecord,
  putClientRecord,
} from '../../src/types/client-record.js';
import type {ClientID, ClientMap} from '../../src/types/client-state.js';
import {UserValue, getUserValue} from '../../src/types/user-value.js';
import {getVersion, putVersion} from '../../src/types/version.js';
import {processPending} from '../process/process-pending.js';
import {
  getConnectedClients,
  putConnectedClients,
} from '../types/connected-clients.js';
import type {PendingMutation} from '../types/mutation.js';
import {
  Mocket,
  client,
  clientRecord,
  createSilentLogContext,
  mockMathRandom,
  pendingMutation,
} from '../util/test-utils.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

mockMathRandom();

type MissableRecord = {
  now: number;
  missed: boolean;
  bufferNeededMs: number;
};

class FakeBufferSizer extends BufferSizer {
  readonly missableRecords: MissableRecord[] = [];
  testBufferSizeMs = 0;

  constructor() {
    super(
      // values unused
      {
        initialBufferSizeMs: 0,
        maxBufferSizeMs: 0,
        minBufferSizeMs: 0,
        adjustBufferSizeIntervalMs: 1,
      },
    );
  }

  override get bufferSizeMs() {
    return this.testBufferSizeMs;
  }

  override recordMissable(
    now: number,
    missed: boolean,
    bufferNeededMs: number,
    _lc: LogContext,
  ): void {
    this.missableRecords.push({now, missed, bufferNeededMs});
  }
}

const START_TIME = 1000;
beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(START_TIME);
});

afterEach(() => {
  jest.restoreAllMocks();
});

describe('processPending', () => {
  type Case = {
    name: string;
    version: Version;
    clientRecords: ClientRecordMap;
    clients: ClientMap;
    storedConnectedClients: ClientID[];
    pendingMutations: PendingMutation[];
    maxProcessedMutationTimestamp: number;
    bufferSizeMs?: number; // default 200
    maxMutationsToProcess?: number; // default Number.MAX_SAFE_INTEGER
    expectedError?: string;
    expectedVersion: Version;
    expectedPokes?: Map<ClientID, PokeBody>;
    expectedUserValues?: Map<string, UserValue>;
    expectedClientRecords?: ClientRecordMap;
    expectedPendingMutations?: PendingMutation[];
    expectNothingToProcess?: boolean;
    expectedMaxProcessedMutationTimestamp?: number;
    expectedMissableRecords: MissableRecord[];
  };

  const s1 = new Mocket();
  const s2 = new Mocket();
  const s3 = new Mocket();

  const cases: Case[] = [
    {
      name: 'no pending mutations connects or disconnects',
      version: 1,
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      clients: new Map(),
      storedConnectedClients: [],
      pendingMutations: [],
      maxProcessedMutationTimestamp: 500,
      expectedVersion: 1,
      expectedPokes: new Map(),
      expectedUserValues: new Map(),
      expectNothingToProcess: true,
      expectedClientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      expectedMissableRecords: [],
    },
    {
      name: 'no pending mutations, but connect pending',
      version: 3,
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 3)],
        ['c2', clientRecord('cg1', 1)],
      ]),
      clients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client(
          'c2',
          'u2',
          'cg1',
          s2,
          0,
          false,
          false /* sentInitialPresence */,
        ),
      ]),
      storedConnectedClients: ['c1'],
      pendingMutations: [],
      maxProcessedMutationTimestamp: 500,
      expectedVersion: 4,
      // newly connected client is fast forwarded
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
              {
                baseCookie: 3,
                cookie: 4,
                lastMutationIDChanges: {},
                presence: [{op: 'put', key: 'c2', value: 1}],
                patch: [],
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
        [
          'c2',
          {
            pokes: [
              {
                baseCookie: 1,
                cookie: 3,
                lastMutationIDChanges: {},
                presence: [
                  {op: 'clear'},
                  {op: 'put', key: 'c1', value: 1},
                  {op: 'put', key: 'c2', value: 1},
                ],
                patch: [],
              },
              {
                baseCookie: 3,
                cookie: 4,
                lastMutationIDChanges: {},
                presence: [],
                patch: [],
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map(),
      expectNothingToProcess: false,
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 4)],
        ['c2', clientRecord('cg1', 4)],
      ]),
      expectedMissableRecords: [],
    },
    {
      name: 'no pending mutations, but disconnect pending',
      version: 1,
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1)],
        ['c2', clientRecord('cg1', 1)],
      ]),
      clients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
      storedConnectedClients: ['c1', 'c2'],
      pendingMutations: [],
      maxProcessedMutationTimestamp: 500,
      // version updated by disconnectHandler
      expectedVersion: 2,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
              {
                baseCookie: 1,
                cookie: 2,
                lastMutationIDChanges: {},
                presence: [
                  {
                    op: 'del',
                    key: 'c2',
                  },
                ],
                patch: [],
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map(),
      expectNothingToProcess: false,
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 2)],
        ['c2', clientRecord('cg1', 1)],
      ]),
      expectedMissableRecords: [],
    },
    {
      name: 'one client, one mutation, all processed',
      version: 1,
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      clients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
      storedConnectedClients: ['c1'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 750,
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 700,
      expectedVersion: 2,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
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
                timestamp: 750,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 1, version: 2, deleted: false}],
      ]),
      expectedClientRecords: new Map([['c1', clientRecord('cg1', 2, 2, 2)]]),
      expectedMaxProcessedMutationTimestamp: 750,
      expectedMissableRecords: [
        {
          bufferNeededMs: 0,
          missed: false,
          now: START_TIME,
        },
      ],
    },
    {
      name: 'one client, one mutation, all processed, debugPerf',
      version: 1,
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      clients: new Map([
        client(
          'c1',
          'u1',
          'cg1',
          s1,
          0,
          true /* debugPerf */,
          true /* sentInitialPresence */,
        ),
      ]),
      storedConnectedClients: ['c1'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: {
            normalizedTimestamp: 750,
            originTimestamp: 600,
            serverReceivedTimestamp: 700,
          },
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 700,
      expectedVersion: 2,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
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
                timestamp: 750,
                debugOriginTimestamp: 600,
                debugServerReceivedTimestamp: 700,
                debugServerSentTimestamp: START_TIME,
              },
            ],
            requestID: '4fxcm49g2j9',
            debugServerBufferMs: 200,
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 1, version: 2, deleted: false}],
      ]),
      expectedClientRecords: new Map([['c1', clientRecord('cg1', 2, 2, 2)]]),
      expectedMaxProcessedMutationTimestamp: 750,
      expectedMissableRecords: [
        {
          bufferNeededMs: -50,
          missed: false,
          now: START_TIME,
        },
      ],
    },
    {
      name: 'three clients, two client groups, three mutations, all processed',
      version: 1,
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1)],
        ['c2', clientRecord('cg1', 1)],
        ['c3', clientRecord('cg2', 1)],
      ]),
      clients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1', s2, 0),
        client('c3', 'u3', 'cg2', s3, 0),
      ]),
      storedConnectedClients: ['c1', 'c2', 'c3'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 700,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 720,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c3',
          clientGroupID: 'cg2',
          id: 2,
          timestamps: 740,
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 700,
      expectedVersion: 4,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
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
                timestamp: 700,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                presence: [],
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 720,
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
                timestamp: 740,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
        [
          'c2',
          {
            pokes: [
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
                timestamp: 700,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                presence: [],
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 720,
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
                timestamp: 740,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
        [
          'c3',
          {
            pokes: [
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
                timestamp: 700,
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
                timestamp: 720,
              },
              {
                baseCookie: 3,
                cookie: 4,
                lastMutationIDChanges: {c3: 2},
                presence: [],
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 3,
                  },
                ],
                timestamp: 740,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 3, version: 4, deleted: false}],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 4, 2, 2)],
        ['c2', clientRecord('cg1', 4, 2, 3)],
        ['c3', clientRecord('cg2', 4, 2, 4)],
      ]),
      expectedMaxProcessedMutationTimestamp: 740,
      expectedMissableRecords: [
        {
          bufferNeededMs: 0,
          missed: false,
          now: START_TIME,
        },
      ],
    },
    {
      name: 'three clients, two client groups, three mutations, only 2 processed due to maxMutationsToProcess',
      version: 1,
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1)],
        ['c2', clientRecord('cg1', 1)],
        ['c3', clientRecord('cg2', 1)],
      ]),
      clients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1', s2, 0),
        client('c3', 'u3', 'cg2', s3, 0),
      ]),
      storedConnectedClients: ['c1', 'c2', 'c3'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 700,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 720,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c3',
          clientGroupID: 'cg2',
          id: 2,
          timestamps: 740,
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 700,
      maxMutationsToProcess: 2,
      expectedVersion: 3,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
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
                timestamp: 700,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                presence: [],
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 720,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
        [
          'c2',
          {
            pokes: [
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
                timestamp: 700,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                presence: [],
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 720,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
        [
          'c3',
          {
            pokes: [
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
                timestamp: 700,
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
                timestamp: 720,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 2, version: 3, deleted: false}],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 3, 2, 2)],
        ['c2', clientRecord('cg1', 3, 2, 3)],
        ['c3', clientRecord('cg2', 3, 1, 1)],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID: 'c3',
          clientGroupID: 'cg2',
          id: 2,
          timestamps: 740,
          name: 'inc',
        }),
      ],
      expectedMaxProcessedMutationTimestamp: 720,
      expectedMissableRecords: [
        {
          bufferNeededMs: 0,
          missed: false,
          now: START_TIME,
        },
      ],
    },
    {
      name: 'two clients, two client groups, four mutations all w timestamps, two processed',
      version: 1,
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1)],
        ['c2', clientRecord('cg1', 1)],
      ]),
      clients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1', s2, 0),
      ]),
      storedConnectedClients: ['c1', 'c2'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 790,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 800,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: 801,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: 810,
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 700,
      expectedVersion: 3,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
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
                timestamp: 790,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                presence: [],
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 800,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
        [
          'c2',
          {
            pokes: [
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
                timestamp: 790,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                presence: [],
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 800,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 2, version: 3, deleted: false}],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 3, 2, 2)],
        ['c2', clientRecord('cg1', 3, 2, 3)],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: 801,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: 810,
          name: 'inc',
        }),
      ],
      expectedMaxProcessedMutationTimestamp: 800,
      expectedMissableRecords: [
        {
          bufferNeededMs: 0,
          missed: false,
          now: START_TIME,
        },
      ],
    },
    {
      name: 'two clients, two client groups, four mutations all w timestamps, three processed, different bufferNeededMs',
      version: 1,
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1)],
        ['c2', clientRecord('cg1', 1)],
      ]),
      clients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1', s2, 0),
      ]),
      storedConnectedClients: ['c1', 'c2'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: {
            normalizedTimestamp: 840,
            originTimestamp: 640,
            serverReceivedTimestamp: 910,
          },
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: {
            normalizedTimestamp: 850,
            originTimestamp: 650,
            serverReceivedTimestamp: 915,
          },
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: {
            normalizedTimestamp: 860,
            originTimestamp: 660,
            serverReceivedTimestamp: 935,
          },
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: {
            normalizedTimestamp: 870,
            originTimestamp: 670,
            serverReceivedTimestamp: 940,
          },
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 700,
      bufferSizeMs: 140,
      expectedVersion: 4,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
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
                timestamp: 840,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                presence: [],
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 850,
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
                timestamp: 860,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
        [
          'c2',
          {
            pokes: [
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
                timestamp: 840,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                presence: [],
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 850,
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
                timestamp: 860,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 3, version: 4, deleted: false}],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 4, 3, 4, 1000)],
        ['c2', clientRecord('cg1', 4, 2, 3, 1000)],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: {
            normalizedTimestamp: 870,
            originTimestamp: 670,
            serverReceivedTimestamp: 940,
          },
          name: 'inc',
        }),
      ],
      expectedMaxProcessedMutationTimestamp: 860,
      expectedMissableRecords: [
        {
          bufferNeededMs: 75,
          missed: false,
          now: START_TIME,
        },
      ],
    },
    {
      name: 'two clients, two client groups, four mutations some with undefined timestamps, two processed',
      version: 1,
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1)],
        ['c2', clientRecord('cg1', 1)],
      ]),
      clients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1', s2, 0),
      ]),
      storedConnectedClients: ['c1', 'c2'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 790,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: undefined,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: 801,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: undefined,
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 700,
      expectedVersion: 3,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
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
                timestamp: 790,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                presence: [],
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: undefined,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
        [
          'c2',
          {
            pokes: [
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
                timestamp: 790,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                presence: [],
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: undefined,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 2, version: 3, deleted: false}],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 3, 2, 2, 1000)],
        ['c2', clientRecord('cg1', 3, 2, 3, 1000)],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: 801,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: undefined,
          name: 'inc',
        }),
      ],
      expectedMaxProcessedMutationTimestamp: 790,
      expectedMissableRecords: [
        {
          bufferNeededMs: 0,
          missed: false,
          now: START_TIME,
        },
      ],
    },
    {
      name: 'one client, one mutation, all processed, passed maxProcessedMutationTimestamp is greater than processed',
      version: 1,
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      clients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
      storedConnectedClients: ['c1'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: {
            normalizedTimestamp: 750,
            originTimestamp: 500,
            serverReceivedTimestamp: 850,
          },
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 800,
      expectedVersion: 2,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
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
                timestamp: 750,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 1, version: 2, deleted: false}],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 2, 2, 2, 1000)],
      ]),
      expectedMaxProcessedMutationTimestamp: 800,
      expectedMissableRecords: [
        {
          bufferNeededMs: 100,
          missed: true,
          now: START_TIME,
        },
      ],
    },
    {
      name: 'one client, two mutations, all processed, maxProcessedMutationTimestamp returned is not last mutation',
      version: 1,
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      clients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
      storedConnectedClients: ['c1'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: 750,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 3,
          timestamps: 720,
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 700,
      expectedVersion: 3,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
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
                timestamp: 750,
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
                timestamp: 720,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 2, version: 3, deleted: false}],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 3, 3, 3, 1000)],
      ]),
      expectedMaxProcessedMutationTimestamp: 750,
      expectedMissableRecords: [
        {
          bufferNeededMs: 0,
          missed: false,
          now: START_TIME,
        },
      ],
    },
  ];

  const env = {boo: 'far'};

  const mutators = new Map(
    Object.entries({
      inc: async (tx: WriteTransaction) => {
        expect(tx.env).toEqual(env);
        let count = ((await tx.get('count')) as number) ?? 0;
        count++;
        await tx.set('count', count);
      },
    }),
  );

  for (const c of cases) {
    test(c.name, async () => {
      const durable = await getMiniflareDurableObjectStorage(id);
      await durable.deleteAll();
      const storage = new DurableStorage(durable);
      await putVersion(c.version, storage);
      for (const [clientID, record] of c.clientRecords) {
        await putClientRecord(clientID, record, storage);
      }
      await putConnectedClients(new Set(c.storedConnectedClients), storage);
      for (const [, clientState] of c.clients) {
        (clientState.socket as Mocket).log.length = 0;
      }
      const fakeBufferSizer = new FakeBufferSizer();
      fakeBufferSizer.testBufferSizeMs = c.bufferSizeMs ?? 200;
      const p = processPending(
        createSilentLogContext(),
        env,
        storage,
        c.clients,
        c.pendingMutations,
        mutators,
        () => Promise.resolve(),
        () => Promise.resolve(),
        c.maxProcessedMutationTimestamp,
        fakeBufferSizer,
        c.maxMutationsToProcess ?? Number.MAX_SAFE_INTEGER,
        () => true,
      );
      if (c.expectedError) {
        let expectedE;
        try {
          await p;
        } catch (e) {
          expectedE = String(e);
        }
        expect(expectedE).toEqual(c.expectedError);
        return;
      }

      expect(await p).toEqual({
        maxProcessedMutationTimestamp:
          c.expectedMaxProcessedMutationTimestamp ??
          c.maxProcessedMutationTimestamp,
        nothingToProcess: c.expectNothingToProcess ?? false,
      });
      expect(c.pendingMutations).toEqual(c.expectedPendingMutations ?? []);
      expect(await getConnectedClients(storage)).toEqual(
        new Set(c.clients.keys()),
      );

      expect(c.expectedError).toBeUndefined;
      expect(await getVersion(storage)).toEqual(c.expectedVersion);
      for (const [clientID, clientState] of c.clients) {
        expect(clientState.sentInitialPresence).toEqual(true);
        const mocket = clientState.socket as Mocket;
        const expectedPoke = c.expectedPokes?.get(clientID);
        if (!expectedPoke) {
          expect(mocket.log.length).toEqual(0);
        } else {
          expect(mocket.log[0][0]).toEqual('send');
          expect(JSON.parse(mocket.log[0][1] as string)).toEqual([
            'poke',
            expectedPoke,
          ]);
        }
      }
      for (const [expKey, expValue] of c.expectedUserValues ?? new Map()) {
        expect(await getUserValue(expKey, storage)).toEqual(expValue);
      }
      for (const [expClientID, expRecord] of c.expectedClientRecords ??
        new Map()) {
        expect(await getClientRecord(expClientID, storage)).toEqual(expRecord);
      }
      expect(fakeBufferSizer.missableRecords).toEqual(
        c.expectedMissableRecords,
      );
    });
  }
});
