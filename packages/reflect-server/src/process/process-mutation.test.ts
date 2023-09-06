import {expect, test} from '@jest/globals';
import type {AuthData, WriteTransaction} from 'reflect-shared';
import {
  MutatorMap,
  processMutation,
} from '../../src/process/process-mutation.js';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import {
  ClientRecord,
  getClientRecord,
  putClientRecord,
} from '../../src/types/client-record.js';
import {getUserValue} from '../../src/types/user-value.js';
import {getVersion} from '../../src/types/version.js';
import type {PendingMutation} from '../types/mutation.js';
import {
  clientRecord,
  createSilentLogContext,
  pendingMutation,
} from '../util/test-utils.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();
const version = 2;
const auth: AuthData = {userID: 'testUser1', foo: 'bar'};

test('processMutation', async () => {
  type Case = {
    name: string;
    existingRecord?: ClientRecord;
    pendingMutation: PendingMutation;
    expectedError?: string;
    expectedRecord?: ClientRecord;
    expectAppWrite: boolean;
    expectVersionWrite: boolean;
  };

  const cases: Case[] = [
    {
      name: 'clientID not found',
      pendingMutation: pendingMutation({
        clientID: 'c1',
        clientGroupID: 'cg1',
        id: 1,
        timestamps: 100,
        auth,
      }),
      expectedError: 'Error: Client c1 not found',
      expectAppWrite: false,
      expectVersionWrite: false,
    },
    {
      name: 'duplicate mutation',
      existingRecord: clientRecord('cg1', null, 1, 1),
      pendingMutation: pendingMutation({
        clientID: 'c1',
        clientGroupID: 'cg1',
        id: 1,
        timestamps: 100,
        auth,
      }),
      expectedRecord: clientRecord('cg1', null, 1, 1),
      expectAppWrite: false,
      expectVersionWrite: false,
    },
    {
      name: 'ooo mutation',
      existingRecord: clientRecord('cg1', null, 1, 1),
      pendingMutation: pendingMutation({
        clientID: 'c1',
        clientGroupID: 'cg1',
        id: 3,
        timestamps: 100,
        auth,
      }),
      expectedRecord: clientRecord('cg1', null, 1, 1),
      expectAppWrite: false,
      expectVersionWrite: false,
    },
    {
      name: 'unknown mutator',
      existingRecord: clientRecord('cg1', null, 1, 1),
      pendingMutation: pendingMutation({
        clientID: 'c1',
        clientGroupID: 'cg1',
        id: 2,
        timestamps: 100,
        name: 'unknown',
        auth,
      }),
      expectedRecord: clientRecord('cg1', null, 2, version),
      expectAppWrite: false,
      expectVersionWrite: true,
    },
    {
      name: 'mutator throws',
      existingRecord: clientRecord('cg1', null, 1, 1),
      pendingMutation: pendingMutation({
        clientID: 'c1',
        clientGroupID: 'cg1',
        id: 2,
        timestamps: 100,
        name: 'throws',
        auth,
      }),
      expectedRecord: clientRecord('cg1', null, 2, version),
      expectAppWrite: false,
      expectVersionWrite: true,
    },
    {
      name: 'success',
      existingRecord: clientRecord('cg1', null, 1, 1),
      pendingMutation: pendingMutation({
        clientID: 'c1',
        clientGroupID: 'cg1',
        id: 2,
        timestamps: 100,
        name: 'foo',
        auth,
      }),
      expectedRecord: clientRecord('cg1', null, 2, version),
      expectAppWrite: true,
      expectVersionWrite: true,
    },
  ];

  const mutators: MutatorMap = new Map([
    [
      'foo',
      async (tx: WriteTransaction) => {
        expect(tx.auth).toEqual(auth);
        await tx.put('foo', 'bar');
      },
    ],
    [
      'throws',
      async (tx: WriteTransaction) => {
        expect(tx.auth).toEqual(auth);
        await tx.put('foo', 'bar');
        throw new Error('bonk');
      },
    ],
  ]);

  const durable = await getMiniflareDurableObjectStorage(id);

  for (const c of cases) {
    const storage = new DurableStorage(durable);
    const {clientID} = c.pendingMutation;

    if (c.existingRecord) {
      await putClientRecord(clientID, c.existingRecord, storage);
    }

    let err: string | undefined;
    try {
      await processMutation(
        createSilentLogContext(),
        c.pendingMutation,
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
