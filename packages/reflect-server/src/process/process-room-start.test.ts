import {describe, test, expect} from '@jest/globals';
import {DurableStorage} from '../storage/durable-storage.js';
import {getUserValue} from '../types/user-value.js';
import {getVersion, putVersion} from '../types/version.js';
import {createSilentLogContext} from '../util/test-utils.js';
import {processRoomStart} from './process-room-start.js';
import type {RoomStartHandler} from '../server/room-start.js';
import type {WriteTransaction} from 'replicache';

const {roomDO} = getMiniflareBindings();

describe('processRoomStart', () => {
  type Case = {
    name: string;
    roomStartHandler: RoomStartHandler;
    startingVersion?: number;
    endingVersion?: number;
    expectedError?: string;
    expectFooBar: boolean;
  };

  const cases: Case[] = [
    {
      name: 'no room handler',
      roomStartHandler: () => Promise.resolve(),
      expectFooBar: false,
    },
    {
      name: 'room handler with no starting version',
      roomStartHandler: async (tx: WriteTransaction) => {
        await tx.put('foo', 'bar');
      },
      endingVersion: 1,
      expectFooBar: true,
    },
    {
      name: 'room handler with starting version',
      roomStartHandler: async (tx: WriteTransaction) => {
        await tx.put('foo', 'bar');
      },
      startingVersion: 12,
      endingVersion: 13,
      expectFooBar: true,
    },
    {
      name: 'throwing room handler',
      roomStartHandler: () => Promise.reject('tossed!'),
      startingVersion: 12,
      endingVersion: 12,
      expectFooBar: false,
      expectedError: 'tossed!',
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
          c.roomStartHandler,
          storage,
        );
      } catch (e) {
        err = String(e);
      }

      expect(err).toEqual(c.expectedError);
      expect(await getUserValue('foo', storage)).toEqual(
        c.expectFooBar
          ? {version: c.startingVersion ?? 0, deleted: false, value: 'bar'}
          : undefined,
      );
      expect(await getVersion(storage)).toEqual(c.endingVersion);
    });
  }
});
