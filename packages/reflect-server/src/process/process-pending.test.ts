import {test, expect} from '@jest/globals';
import type {WriteTransaction} from 'replicache';
import type {PokeBody} from '../../src/protocol/poke.js';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import {
  ClientRecordMap,
  getClientRecord,
  putClientRecord,
} from '../../src/types/client-record.js';
import type {ClientMap} from '../../src/types/client-state.js';
import {getUserValue, UserValue} from '../../src/types/user-value.js';
import {getVersion, putVersion, Version} from '../../src/types/version.js';
import {
  client,
  mutation,
  clientRecord,
  createSilentLogContext,
  fail,
  Mocket,
} from '../util/test-utils.js';
import {processPending} from '../../src/process/process-pending.js';
import type {PendingMutationMap} from 'src/types/mutation.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

test('processPending', async () => {
  type Case = {
    name: string;
    version: Version;
    clientRecords: ClientRecordMap;
    clients: ClientMap;
    pendingMutations: PendingMutationMap;
    expectedError?: string;
    expectedClients: ClientMap;
    expectedVersion: Version;
    expectedPokes?: Map<Mocket, PokeBody[]>;
    expectedUserValues?: Map<string, UserValue>;
    expectedClientRecords?: ClientRecordMap;
  };

  const s1 = new Mocket();
  const s2 = new Mocket();
  const s3 = new Mocket();

  const cases: Case[] = [
    {
      name: 'none pending',
      version: 1,
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      clients: new Map(),
      pendingMutations: new Map(),
      expectedClients: new Map(),
      expectedVersion: 1,
      expectedPokes: new Map(),
      expectedUserValues: new Map(),
      expectedClientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
    },
    {
      name: 'one client, one mutation',
      version: 1,
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      clients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
      pendingMutations: new Map([
        ['cg1', [mutation('c1', 2, 'inc', null, 100)]],
      ]),
      expectedClients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
      expectedVersion: 2,
      expectedPokes: new Map([
        [
          s1,
          [
            {
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
              timestamp: 100,
            },
          ],
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 1, version: 2, deleted: false}],
      ]),
      expectedClientRecords: new Map([['c1', clientRecord('cg1', 2, 2, 2)]]),
    },
    {
      name: 'three clients, two client groups, three mutations',
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
      pendingMutations: new Map([
        [
          'cg1',
          [
            mutation('c1', 2, 'inc', null, 100),
            mutation('c2', 2, 'inc', null, 120),
          ],
        ],
        ['cg2', [mutation('c3', 2, 'inc', null, 140)]],
      ]),
      expectedClients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1', s2, 0),
        client('c3', 'u3', 'cg2', s3, 0),
      ]),
      expectedVersion: 2,
      expectedPokes: new Map([
        [
          s1,
          [
            {
              baseCookie: 1,
              cookie: 2,
              lastMutationIDChanges: {c1: 2, c2: 2},
              patch: [
                {
                  op: 'put',
                  key: 'count',
                  value: 3,
                },
              ],
              timestamp: 100,
            },
          ],
        ],
        [
          s2,
          [
            {
              baseCookie: 1,
              cookie: 2,
              lastMutationIDChanges: {c1: 2, c2: 2},
              patch: [
                {
                  op: 'put',
                  key: 'count',
                  value: 3,
                },
              ],
              timestamp: 100,
            },
          ],
        ],
        [
          s3,
          [
            {
              baseCookie: 1,
              cookie: 2,
              lastMutationIDChanges: {c3: 2},
              patch: [
                {
                  op: 'put',
                  key: 'count',
                  value: 3,
                },
              ],
              timestamp: 100,
            },
          ],
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 3, version: 2, deleted: false}],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 2, 2, 2)],
        ['c2', clientRecord('cg1', 2, 2, 2)],
        ['c3', clientRecord('cg2', 2, 2, 2)],
      ]),
    },
  ];

  const mutators = new Map(
    Object.entries({
      inc: async (tx: WriteTransaction) => {
        let count = ((await tx.get('count')) as number) ?? 0;
        count++;
        await tx.put('count', count);
      },
    }),
  );

  const startTime = 100;
  const durable = await getMiniflareDurableObjectStorage(id);

  for (const c of cases) {
    await durable.deleteAll();
    const storage = new DurableStorage(durable);
    await putVersion(c.version, storage);
    for (const [clientID, record] of c.clientRecords) {
      await putClientRecord(clientID, record, storage);
    }
    for (const [, clientState] of c.clients) {
      (clientState.socket as Mocket).log.length = 0;
    }
    const p = processPending(
      createSilentLogContext(),
      storage,
      c.clients,
      c.pendingMutations,
      mutators,
      () => Promise.resolve(),
      startTime,
    );
    if (c.expectedError) {
      try {
        await p;
        fail('should have thrown');
      } catch (e) {
        expect(String(e)).toEqual(c.expectedError);
      }
      continue;
    }

    await p;
    expect(c.clients).toEqual(c.expectedClients);
    expect(c.pendingMutations.size).toEqual(0);

    expect(c.expectedError).toBeUndefined;
    expect(await getVersion(storage)).toEqual(c.expectedVersion);
    for (const [mocket, clientPokes] of c.expectedPokes ?? []) {
      expect(mocket.log).toEqual(
        clientPokes.map(poke => ['send', JSON.stringify(['poke', poke])]),
      );
    }
    for (const [expKey, expValue] of c.expectedUserValues ?? new Map()) {
      expect(await getUserValue(expKey, storage)).toEqual(expValue);
    }
    for (const [expClientID, expRecord] of c.expectedClientRecords ??
      new Map()) {
      expect(await getClientRecord(expClientID, storage)).toEqual(expRecord);
    }
  }
});
