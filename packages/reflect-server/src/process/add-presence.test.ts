import {describe, expect, test} from '@jest/globals';
import type {Poke, Version} from 'reflect-protocol';
import {DurableStorage} from '../storage/durable-storage.js';
import {
  listClientRecords,
  putClientRecord,
  type ClientRecordMap,
} from '../types/client-record.js';
import type {ClientID, ClientMap} from '../types/client-state.js';
import {getVersion, putVersion} from '../types/version.js';
import {client, clientRecord} from '../util/test-utils.js';
import {addPresence} from './add-presence.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

describe('addPresence', () => {
  type Case = {
    name: string;
    clients: ClientMap;
    pokesByClientID: Map<ClientID, Poke[]>;
    previousConnectedClients: ClientID[];
    nextConnectedClients: ClientID[];
    clientRecords: ClientRecordMap;
    version: Version;
    expectedPokes: Map<ClientID, Poke[]>;
    expectedClientRecords: ClientRecordMap;
    expectedVersion: Version;
  };

  const cases: Case[] = [
    {
      name: '1 client connects no fast forward poke',
      clients: new Map([
        client(
          'c1',
          'u1',
          'cg1',
          undefined,
          undefined,
          false,
          false /* sentInitialPresence */,
        ),
      ]),
      pokesByClientID: new Map(),
      clientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
      ]),
      version: 3,
      previousConnectedClients: [],
      nextConnectedClients: ['c1'],
      expectedPokes: new Map([
        [
          'c1',
          [
            {
              baseCookie: 3,
              cookie: 4,
              lastMutationIDChanges: {},
              presence: [
                {
                  op: 'clear',
                },
                {
                  op: 'put',
                  key: 'c1',
                  value: 1,
                },
              ],
              patch: [],
            },
          ],
        ],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 4})],
      ]),
      expectedVersion: 4,
    },
    {
      name: '1 client connects has fast forward poke',
      clients: new Map([
        client(
          'c1',
          'u1',
          'cg1',
          undefined,
          undefined,
          false,
          false /* sentInitialPresence */,
        ),
      ]),
      pokesByClientID: new Map([
        [
          'c1',
          [
            {
              baseCookie: 1,
              cookie: 3,
              lastMutationIDChanges: {
                c2: 2,
              },
              presence: [],
              patch: [{op: 'put', key: 'foo', value: 'bar'}],
            },
          ],
        ],
      ]),
      clientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
      ]),
      version: 3,
      previousConnectedClients: [],
      nextConnectedClients: ['c1'],
      expectedPokes: new Map([
        [
          'c1',
          [
            {
              baseCookie: 1,
              cookie: 3,
              lastMutationIDChanges: {
                c2: 2,
              },
              presence: [
                {
                  op: 'clear',
                },
                {
                  op: 'put',
                  key: 'c1',
                  value: 1,
                },
              ],
              patch: [{op: 'put', key: 'foo', value: 'bar'}],
            },
          ],
        ],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
      ]),
      expectedVersion: 3,
    },
    {
      name: '1 client connects no fast forward poke, 1 already connected no poke',
      clients: new Map([
        // c1 already connected and sentInitialPresence true
        client('c1', 'u1', 'cg1'),
        client(
          'c2',
          'u2',
          'cg1',
          undefined,
          undefined,
          false,
          false /* sentInitialPresence */,
        ),
      ]),
      pokesByClientID: new Map(),
      clientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
      ]),
      version: 3,
      previousConnectedClients: ['c1'],
      nextConnectedClients: ['c1', 'c2'],
      expectedPokes: new Map([
        [
          'c1',
          [
            {
              baseCookie: 3,
              cookie: 4,
              lastMutationIDChanges: {},
              presence: [
                {
                  op: 'put',
                  key: 'c2',
                  value: 1,
                },
              ],
              patch: [],
            },
          ],
        ],
        [
          'c2',
          [
            {
              baseCookie: 3,
              cookie: 4,
              lastMutationIDChanges: {},
              presence: [
                {
                  op: 'clear',
                },
                {
                  op: 'put',
                  key: 'c1',
                  value: 1,
                },
                {
                  op: 'put',
                  key: 'c2',
                  value: 1,
                },
              ],
              patch: [],
            },
          ],
        ],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 4})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 4})],
      ]),
      expectedVersion: 4,
    },
    {
      name: '1 client connects has fast forward poke, 1 already connected, plus mutation poke',
      clients: new Map([
        // c1 already connected and sentInitialPresence true
        client('c1', 'u1', 'cg1'),
        client(
          'c2',
          'u2',
          'cg1',
          undefined,
          undefined,
          false,
          false /* sentInitialPresence */,
        ),
      ]),
      pokesByClientID: new Map([
        [
          'c1',
          [
            {
              baseCookie: 2,
              cookie: 3,
              lastMutationIDChanges: {
                c1: 3,
              },
              presence: [],
              patch: [{op: 'put', key: 'fuzzy', value: 'wuzzy'}],
            },
          ],
        ],
        [
          'c2',
          [
            {
              baseCookie: 1,
              cookie: 2,
              lastMutationIDChanges: {
                c2: 2,
              },
              presence: [],
              patch: [{op: 'put', key: 'foo', value: 'bar'}],
            },
            {
              baseCookie: 2,
              cookie: 3,
              lastMutationIDChanges: {
                c1: 3,
              },
              presence: [],
              patch: [{op: 'put', key: 'fuzzy', value: 'wuzzy'}],
            },
          ],
        ],
      ]),
      clientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
      ]),
      version: 3,
      previousConnectedClients: ['c1'],
      nextConnectedClients: ['c1', 'c2'],
      expectedPokes: new Map([
        [
          'c1',
          [
            {
              baseCookie: 2,
              cookie: 3,
              lastMutationIDChanges: {
                c1: 3,
              },
              presence: [
                {
                  op: 'put',
                  key: 'c2',
                  value: 1,
                },
              ],
              patch: [{op: 'put', key: 'fuzzy', value: 'wuzzy'}],
            },
          ],
        ],
        [
          'c2',
          [
            {
              baseCookie: 1,
              cookie: 2,
              lastMutationIDChanges: {
                c2: 2,
              },
              presence: [
                {
                  op: 'clear',
                },
                {
                  op: 'put',
                  key: 'c1',
                  value: 1,
                },
                {
                  op: 'put',
                  key: 'c2',
                  value: 1,
                },
              ],
              patch: [{op: 'put', key: 'foo', value: 'bar'}],
            },
            {
              baseCookie: 2,
              cookie: 3,
              lastMutationIDChanges: {
                c1: 3,
              },
              presence: [],
              patch: [{op: 'put', key: 'fuzzy', value: 'wuzzy'}],
            },
          ],
        ],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
      ]),
      expectedVersion: 3,
    },
    {
      name: '1 client disconnects no mutation pokes',
      clients: new Map([client('c1', 'u1', 'cg1'), client('c2', 'u2', 'cg1')]),
      pokesByClientID: new Map(),
      clientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
        ['c3', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
      ]),
      version: 3,
      previousConnectedClients: ['c1', 'c2', 'c3'],
      nextConnectedClients: ['c1', 'c2'],
      expectedPokes: new Map([
        [
          'c1',
          [
            {
              baseCookie: 3,
              cookie: 4,
              lastMutationIDChanges: {},
              presence: [
                {
                  op: 'del',
                  key: 'c3',
                },
              ],
              patch: [],
            },
          ],
        ],
        [
          'c2',
          [
            {
              baseCookie: 3,
              cookie: 4,
              lastMutationIDChanges: {},
              presence: [
                {
                  op: 'del',
                  key: 'c3',
                },
              ],
              patch: [],
            },
          ],
        ],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 4})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 4})],
        ['c3', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
      ]),
      expectedVersion: 4,
    },
    {
      name: '2 clients disconnects no mutation pokes',
      clients: new Map([client('c1', 'u1', 'cg1'), client('c2', 'u2', 'cg1')]),
      pokesByClientID: new Map(),
      clientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
        ['c3', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
        ['c4', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
      ]),
      version: 3,
      previousConnectedClients: ['c1', 'c2', 'c3', 'c4'],
      nextConnectedClients: ['c1', 'c2'],
      expectedPokes: new Map([
        [
          'c1',
          [
            {
              baseCookie: 3,
              cookie: 4,
              lastMutationIDChanges: {},
              presence: [
                {
                  op: 'del',
                  key: 'c3',
                },
                {
                  op: 'del',
                  key: 'c4',
                },
              ],
              patch: [],
            },
          ],
        ],
        [
          'c2',
          [
            {
              baseCookie: 3,
              cookie: 4,
              lastMutationIDChanges: {},
              presence: [
                {
                  op: 'del',
                  key: 'c3',
                },
                {
                  op: 'del',
                  key: 'c4',
                },
              ],
              patch: [],
            },
          ],
        ],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 4})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 4})],
        ['c3', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
        ['c4', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
      ]),
      expectedVersion: 4,
    },
    {
      name: '1 client disconnects, 1 client connects no fast forward poke, 1 already connected, no mutation poke',
      clients: new Map([
        // c1 already connected and sentInitialPresence true
        client('c1', 'u1', 'cg1'),
        client(
          'c2',
          'u2',
          'cg1',
          undefined,
          undefined,
          false,
          false /* sentInitialPresence */,
        ),
      ]),
      pokesByClientID: new Map(),
      clientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
        ['c3', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
      ]),
      version: 3,
      previousConnectedClients: ['c1', 'c3'],
      nextConnectedClients: ['c1', 'c2'],
      expectedPokes: new Map([
        [
          'c1',
          [
            {
              baseCookie: 3,
              cookie: 4,
              lastMutationIDChanges: {},
              presence: [
                {
                  op: 'put',
                  key: 'c2',
                  value: 1,
                },
                {
                  op: 'del',
                  key: 'c3',
                },
              ],
              patch: [],
            },
          ],
        ],
        [
          'c2',
          [
            {
              baseCookie: 3,
              cookie: 4,
              lastMutationIDChanges: {},
              presence: [
                {
                  op: 'clear',
                },
                {
                  op: 'put',
                  key: 'c1',
                  value: 1,
                },
                {
                  op: 'put',
                  key: 'c2',
                  value: 1,
                },
              ],
              patch: [],
            },
          ],
        ],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord({clientGroupID: 'cg1', baseCookie: 4})],
        ['c2', clientRecord({clientGroupID: 'cg1', baseCookie: 4})],
        ['c3', clientRecord({clientGroupID: 'cg1', baseCookie: 3})],
      ]),
      expectedVersion: 4,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const durable = await getMiniflareDurableObjectStorage(id);
      await durable.deleteAll();
      const storage = new DurableStorage(durable);

      await putVersion(c.version, storage);
      for (const [clientID, record] of c.clientRecords) {
        await putClientRecord(clientID, record, storage);
      }

      const {pokesByClientID} = c;
      await addPresence(
        c.clients,
        pokesByClientID,
        storage,
        new Set(c.previousConnectedClients),
        new Set(c.nextConnectedClients),
      );

      pokesByClientID;

      expect(await getVersion(storage)).toEqual(c.expectedVersion);
      expect(await listClientRecords(storage)).toEqual(c.expectedClientRecords);
      expect(pokesByClientID).toEqual(c.expectedPokes);
    });
  }
});
