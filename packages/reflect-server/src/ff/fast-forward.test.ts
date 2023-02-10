import {test, expect} from '@jest/globals';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import type {ClientPokeBody} from '../../src/types/client-poke-body.js';
import {
  ClientRecord,
  getClientRecord,
  putClientRecord,
} from '../../src/types/client-record.js';
import type {ClientID} from '../../src/types/client-state.js';
import {putUserValue, UserValue} from '../../src/types/user-value.js';
import {must} from '../../src/util/must.js';
import {fastForwardRoom} from '../../src/ff/fast-forward.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

test('fastForward', async () => {
  type Case = {
    name: string;
    state: Map<string, UserValue>;
    clientRecords: Map<string, ClientRecord>;
    clients: ClientID[];
    timestamp: number;
    expectedError?: string;
    expectedPokes?: ClientPokeBody[];
  };

  const cases: Case[] = [
    {
      name: 'no clients',
      state: new Map([['foo', {value: 'bar', version: 1, deleted: false}]]),
      clientRecords: new Map([['c1', {lastMutationID: 1, baseCookie: 0}]]),
      clients: [],
      timestamp: 1,
      expectedPokes: [],
    },
    {
      name: 'no data',
      state: new Map(),
      clientRecords: new Map([['c1', {lastMutationID: 1, baseCookie: 0}]]),
      clients: ['c1'],
      timestamp: 1,
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 0,
            cookie: 42,
            lastMutationID: 1,
            patch: [],
            timestamp: 1,
          },
        },
      ],
    },
    {
      name: 'up to date',
      state: new Map(),
      clientRecords: new Map([['c1', {lastMutationID: 1, baseCookie: 42}]]),
      clients: ['c1'],
      timestamp: 1,
      expectedPokes: [],
    },
    {
      name: 'one client two changes',
      state: new Map([
        ['foo', {value: 'bar', version: 42, deleted: false}],
        ['hot', {value: 'dog', version: 42, deleted: true}],
      ]),
      clientRecords: new Map([['c1', {lastMutationID: 3, baseCookie: 41}]]),
      clients: ['c1'],
      timestamp: 1,
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 41,
            cookie: 42,
            lastMutationID: 3,
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
            timestamp: 1,
          },
        },
      ],
    },
    {
      name: 'two clients different changes',
      state: new Map([
        ['foo', {value: 'bar', version: 41, deleted: false}],
        ['hot', {value: 'dog', version: 42, deleted: true}],
      ]),
      clientRecords: new Map([
        ['c1', {lastMutationID: 3, baseCookie: 40}],
        ['c2', {lastMutationID: 1, baseCookie: 41}],
      ]),
      clients: ['c1', 'c2'],
      timestamp: 1,
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 40,
            cookie: 42,
            lastMutationID: 3,
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
            timestamp: 1,
          },
        },
        {
          clientID: 'c2',
          poke: {
            baseCookie: 41,
            cookie: 42,
            lastMutationID: 1,
            patch: [
              {
                op: 'del',
                key: 'hot',
              },
            ],
            timestamp: 1,
          },
        },
      ],
    },
    {
      name: 'two clients with changes but only one active',
      state: new Map([
        ['foo', {value: 'bar', version: 41, deleted: false}],
        ['hot', {value: 'dog', version: 42, deleted: true}],
      ]),
      clientRecords: new Map([
        ['c1', {lastMutationID: 3, baseCookie: 40}],
        ['c2', {lastMutationID: 1, baseCookie: 41}],
      ]),
      clients: ['c1'],
      timestamp: 1,
      expectedPokes: [
        {
          clientID: 'c1',
          poke: {
            baseCookie: 40,
            cookie: 42,
            lastMutationID: 3,
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
            timestamp: 1,
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

    const gcr = async (clientID: ClientID) =>
      must(await getClientRecord(clientID, storage));

    const pokes = await fastForwardRoom(
      c.clients,
      gcr,
      42,
      storage,
      c.timestamp,
    );

    expect(pokes).toEqual(c.expectedPokes);
  }
});
