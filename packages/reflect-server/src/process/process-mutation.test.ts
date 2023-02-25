import {test, expect} from '@jest/globals';
import type {WriteTransaction} from 'replicache';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import {
  ClientRecord,
  getClientRecord,
  putClientRecord,
} from '../../src/types/client-record.js';
import {getUserValue} from '../../src/types/user-value.js';
import {getVersion} from '../../src/types/version.js';
import {
  mutation,
  clientRecord,
  createSilentLogContext,
} from '../util/test-utils.js';
import {
  MutatorMap,
  processMutation,
} from '../../src/process/process-mutation.js';
import type {Mutation} from 'reflect-protocol';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();
const version = 2;

test('processMutation', async () => {
  type Case = {
    name: string;
    existingRecord?: ClientRecord;
    mutation: Mutation;
    expectedError?: string;
    expectedRecord?: ClientRecord;
    expectAppWrite: boolean;
    expectVersionWrite: boolean;
  };

  const cases: Case[] = [
    {
      name: 'clientID not found',
      mutation: mutation('c1', 1),
      expectedError: 'Error: Client c1 not found',
      expectAppWrite: false,
      expectVersionWrite: false,
    },
    {
      name: 'duplicate mutation',
      existingRecord: clientRecord('cg1', null, 1, 1),
      mutation: mutation('c1', 1),
      expectedRecord: clientRecord('cg1', null, 1, 1),
      expectAppWrite: false,
      expectVersionWrite: false,
    },
    {
      name: 'ooo mutation',
      existingRecord: clientRecord('cg1', null, 1, 1),
      mutation: mutation('c1', 3),
      expectedRecord: clientRecord('cg1', null, 1, 1),
      expectAppWrite: false,
      expectVersionWrite: false,
    },
    {
      name: 'unknown mutator',
      existingRecord: clientRecord('cg1', null, 1, 1),
      mutation: mutation('c1', 2, 'unknown'),
      expectedRecord: clientRecord('cg1', null, 2, version),
      expectAppWrite: false,
      expectVersionWrite: true,
    },
    {
      name: 'mutator throws',
      existingRecord: clientRecord('cg1', null, 1, 1),
      mutation: mutation('c1', 2, 'throws'),
      expectedRecord: clientRecord('cg1', null, 2, version),
      expectAppWrite: false,
      expectVersionWrite: true,
    },
    {
      name: 'success',
      existingRecord: clientRecord('cg1', null, 1, 1),
      mutation: mutation('c1', 2, 'foo'),
      expectedRecord: clientRecord('cg1', null, 2, version),
      expectAppWrite: true,
      expectVersionWrite: true,
    },
  ];

  const mutators: MutatorMap = new Map([
    [
      'foo',
      async (tx: WriteTransaction) => {
        await tx.put('foo', 'bar');
      },
    ],
    [
      'throws',
      // eslint-disable-next-line require-await
      async () => {
        throw new Error('bonk');
      },
    ],
  ]);

  const durable = await getMiniflareDurableObjectStorage(id);

  for (const c of cases) {
    const storage = new DurableStorage(durable);
    const {clientID} = c.mutation;

    if (c.existingRecord) {
      await putClientRecord(clientID, c.existingRecord, storage);
    }

    let err: string | undefined;
    try {
      await processMutation(
        createSilentLogContext(),
        c.mutation,
        mutators,
        storage,
        version,
      );
    } catch (e) {
      err = String(e);
    }

    expect(err).toEqual(c.expectedError);
    expect(await getClientRecord(clientID, storage)).toEqual(c.expectedRecord);
    expect(await getUserValue('foo', storage)).toEqual(
      c.expectAppWrite ? {version, deleted: false, value: 'bar'} : undefined,
    );

    const expectedVersion = c.expectVersionWrite ? version : undefined;
    expect(await getVersion(storage)).toEqual(expectedVersion);
  }
});
