import {describe, expect, test} from '@jest/globals';
import type {WriteTransaction} from 'reflect-shared/src/types.js';
import type {RoomStartHandler} from '../server/room-start.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {getUserValue} from '../types/user-value.js';
import {getVersion, putVersion} from '../types/version.js';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {processRoomStart} from './process-room-start.js';

const {roomDO} = getMiniflareBindings();

describe('processRoomStart', () => {
  type Case = {
    name: string;
    onRoomStart: RoomStartHandler;
    startingVersion?: number;
    endingVersion?: number;
    expectedError?: string;
    expectFooBar: boolean;
  };

  const cases: Case[] = [
    {
      name: 'no room handler',
      onRoomStart: () => Promise.resolve(),
      expectFooBar: false,
    },
    {
      name: 'room handler with no starting version',
      onRoomStart: async (tx: WriteTransaction) => {
        await tx.set('foo', 'bar');
      },
      endingVersion: 1,
      expectFooBar: true,
    },
    {
      name: 'room handler with starting version',
      onRoomStart: async (tx: WriteTransaction) => {
        await tx.set('foo', 'bar');
      },
      startingVersion: 12,
      endingVersion: 13,
      expectFooBar: true,
    },
    {
      name: 'throwing room handler',
      onRoomStart: () => Promise.reject('tossed!'),
      startingVersion: 12,
      endingVersion: 12,
      expectFooBar: false,
      expectedError: 'tossed!',
    },
    {
      name: 'room start handler is passed roomID and env',
      onRoomStart: (tx: WriteTransaction, roomID: string) => {
        expect(roomID).toEqual('testRoomID');
        expect(tx.env).toEqual({env: 'yo'});
        return Promise.resolve();
      },
      expectFooBar: false,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const id = roomDO.newUniqueId();
      const durable = await getMiniflareDurableObjectStorage(id);
      const storage = new DurableStorage(durable);

      if (c.startingVersion !== undefined) {
        await putVersion(c.startingVersion, storage);
      }

      let err: string | undefined;
      try {
        await processRoomStart(
          createSilentLogContext(),
          {env: 'yo'},
          c.onRoomStart,
          storage,
          'testRoomID',
        );
      } catch (e) {
        err = String(e);
      }

      expect(err).toEqual(c.expectedError);
      expect(await getUserValue('foo', storage)).toEqual(
        c.expectFooBar
          ? {version: c.endingVersion, deleted: false, value: 'bar'}
          : undefined,
      );
      expect(await getVersion(storage)).toEqual(c.endingVersion);
    });
  }
});
