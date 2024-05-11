import {describe, expect, test} from '@jest/globals';
import type {
  AuthData,
  Env,
  WriteTransaction,
} from 'reflect-shared/src/types.js';
import {
  MutatorMap,
  processMutation,
} from '../../src/process/process-mutation.js';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import {
  ClientRecord,
  IncludeDeleted,
  getClientRecord,
  putClientRecord,
} from '../../src/types/client-record.js';
import {getUserValue} from '../../src/types/user-value.js';
import {getVersion} from '../../src/types/version.js';
import type {PendingMutation} from '../types/mutation.js';
import {clientRecord, pendingMutation} from '../util/test-utils.js';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();
const version = 2;
const userID = 'testUser1';
const auth: AuthData = {userID, foo: 'bar'};
const env: Env = {env: 'baby'};

describe('processMutation', () => {
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
      existingRecord: clientRecord({
        clientGroupID: 'cg1',
        baseCookie: null,
        lastMutationID: 1,
        lastMutationIDVersion: 1,
        userID,
      }),
      pendingMutation: pendingMutation({
        clientID: 'c1',
        clientGroupID: 'cg1',
        id: 1,
        timestamps: 100,
        auth,
      }),
      expectedRecord: clientRecord({
        clientGroupID: 'cg1',
        baseCookie: null,
        lastMutationID: 1,
        lastMutationIDVersion: 1,
      }),
      expectAppWrite: false,
      expectVersionWrite: false,
    },
    {
      name: 'ooo mutation',
      existingRecord: clientRecord({
        clientGroupID: 'cg1',
        baseCookie: null,
        lastMutationID: 1,
        lastMutationIDVersion: 1,
        userID,
      }),
      pendingMutation: pendingMutation({
        clientID: 'c1',
        clientGroupID: 'cg1',
        id: 3,
        timestamps: 100,
        auth,
      }),
      expectedRecord: clientRecord({
        clientGroupID: 'cg1',
        baseCookie: null,
        lastMutationID: 1,
        lastMutationIDVersion: 1,
      }),
      expectAppWrite: false,
      expectVersionWrite: false,
    },
    {
      name: 'unknown mutator',
      existingRecord: clientRecord({
        clientGroupID: 'cg1',
        baseCookie: null,
        lastMutationID: 1,
        lastMutationIDVersion: 1,
        userID,
      }),
      pendingMutation: pendingMutation({
        clientID: 'c1',
        clientGroupID: 'cg1',
        id: 2,
        timestamps: 100,
        name: 'unknown',
        auth,
      }),
      expectedRecord: clientRecord({
        clientGroupID: 'cg1',
        baseCookie: null,
        lastMutationID: 2,
        lastMutationIDVersion: version,
      }),
      expectAppWrite: false,
      expectVersionWrite: true,
    },
    {
      name: 'mutator throws',
      existingRecord: clientRecord({
        clientGroupID: 'cg1',
        baseCookie: null,
        lastMutationID: 1,
        lastMutationIDVersion: 1,
        userID,
      }),
      pendingMutation: pendingMutation({
        clientID: 'c1',
        clientGroupID: 'cg1',
        id: 2,
        timestamps: 100,
        name: 'throws',
        auth,
      }),
      expectedRecord: clientRecord({
        clientGroupID: 'cg1',
        baseCookie: null,
        lastMutationID: 2,
        lastMutationIDVersion: version,
      }),
      expectAppWrite: false,
      expectVersionWrite: true,
    },
    {
      name: 'success',
      existingRecord: clientRecord({
        clientGroupID: 'cg1',
        baseCookie: null,
        lastMutationID: 1,
        lastMutationIDVersion: 1,
        userID,
      }),
      pendingMutation: pendingMutation({
        clientID: 'c1',
        clientGroupID: 'cg1',
        id: 2,
        timestamps: 100,
        name: 'foo',
        auth,
      }),
      expectedRecord: clientRecord({
        clientGroupID: 'cg1',
        baseCookie: null,
        lastMutationID: 2,
        lastMutationIDVersion: version,
      }),
      expectAppWrite: true,
      expectVersionWrite: true,
    },
    {
      name: 'mutation from deleted client',
      existingRecord: clientRecord({
        clientGroupID: 'cg1',
        baseCookie: null,
        lastMutationID: 1,
        lastMutationIDVersion: 1,
        userID,
        deleted: true,
      }),
      pendingMutation: pendingMutation({
        clientID: 'c1',
        clientGroupID: 'cg1',
        id: 2,
        timestamps: 100,
        name: 'foo',
        auth,
      }),
      expectedRecord: clientRecord({
        clientGroupID: 'cg1',
        baseCookie: null,
        lastMutationID: 2,
        lastMutationIDVersion: version,
        deleted: true,
      }),
      expectAppWrite: false,
      expectVersionWrite: true,
    },
  ];

  const mutators: MutatorMap = new Map([
    [
      'foo',
      async (tx: WriteTransaction) => {
        expect(tx.auth).toEqual(auth);
        expect(tx.env).toEqual(env);
        await tx.set('foo', 'bar');
      },
    ],
    [
      'throws',
      async (tx: WriteTransaction) => {
        expect(tx.auth).toEqual(auth);
        expect(tx.env).toEqual(env);
        await tx.set('foo', 'bar');
        throw new Error('bonk');
      },
    ],
  ]);

  for (const c of cases) {
    test(c.name, async () => {
      const durable = await getMiniflareDurableObjectStorage(id);
      const storage = new DurableStorage(durable);
      const {clientID} = c.pendingMutation;

      if (c.existingRecord) {
        await putClientRecord(clientID, c.existingRecord, storage);
      }

      let err: string | undefined;
      try {
        await processMutation(
          createSilentLogContext(),
          env,
          c.pendingMutation,
          mutators,
          storage,
          version,
        );
      } catch (e) {
        err = String(e);
      }

      expect(err).toEqual(c.expectedError);
      expect(
        await getClientRecord(clientID, IncludeDeleted.Include, storage),
      ).toEqual(c.expectedRecord);
      expect(await getUserValue('foo', storage)).toEqual(
        c.expectAppWrite ? {version, deleted: false, value: 'bar'} : undefined,
      );

      const expectedVersion = c.expectVersionWrite ? version : undefined;
      expect(await getVersion(storage)).toEqual(expectedVersion);
    });
  }
});
