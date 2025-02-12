import {describe, expect, test} from 'vitest';
import {TestStore} from './dag/test-store.ts';
import {
  DELETED_CLIENTS_HEAD_NAME,
  getDeletedClients,
} from './deleted-clients.ts';
import {deepFreeze} from './frozen-json.ts';
import {withRead, withWrite} from './with-transactions.ts';

describe('legacy format should also work', async () => {
  const dagStore = new TestStore();

  test.each([
    [[], {clientIDs: [], clientGroupIDs: []}],
    [['a'], {clientIDs: ['a'], clientGroupIDs: []}],
    [['a', 'b'], {clientIDs: ['a', 'b'], clientGroupIDs: []}],
    [
      {clientIDs: ['a'], clientGroupIDs: ['b']},
      {clientIDs: ['a'], clientGroupIDs: ['b']},
    ],
    [
      {clientIDs: ['a', 'b'], clientGroupIDs: ['b']},
      {clientIDs: ['a', 'b'], clientGroupIDs: ['b']},
    ],
    [
      {clientIDs: ['a'], clientGroupIDs: ['b', 'c']},
      {clientIDs: ['a'], clientGroupIDs: ['b', 'c']},
    ],
    [
      {clientIDs: ['a'], clientGroupIDs: ['c', 'b']},
      {clientIDs: ['a'], clientGroupIDs: ['c', 'b']},
    ],
    [
      {clientIDs: [], clientGroupIDs: ['a', 'b', 'c']},
      {clientIDs: [], clientGroupIDs: ['a', 'b', 'c']},
    ],
    [
      {clientIDs: ['a', 'b'], clientGroupIDs: []},
      {clientIDs: ['a', 'b'], clientGroupIDs: []},
    ],
  ])('legacy format %j', async (chunkData, expected) => {
    await withWrite(dagStore, async dagWrite => {
      const chunk = dagWrite.createChunk(deepFreeze(chunkData), []);
      await dagWrite.putChunk(chunk);
      await dagWrite.setHead(DELETED_CLIENTS_HEAD_NAME, chunk.hash);
    });

    const deletedClients = await withRead(dagStore, getDeletedClients);
    expect(deletedClients).toEqual(expected);
  });
});
