import {expect, test} from '@jest/globals';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import {ReplicacheTransaction} from '../../src/storage/replicache-transaction.js';
import {randInt} from '../util/rand.js';
import {
  decodeUserValueVersionKey,
  userValueVersionIndexPrefix,
  userValueVersionInfoSchema,
  userValueVersionKey,
} from '../types/user-value.js';
import {backfillVersionIndex} from './backfill-version-index.js';
import {createSilentLogContext} from '../util/test-utils.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

test('version index backfill', async () => {
  const durable = await getMiniflareDurableObjectStorage(id);
  await durable.deleteAll();
  const storage = new DurableStorage(durable);

  for (let i = 0; i < 50; i++) {
    const tx = new ReplicacheTransaction(
      storage,
      'c1',
      100 + i, // mutationID
      101 + i, // version
      undefined,
    );

    await Promise.all([
      tx.put(`foo${i}`, 'bar'),
      tx.put(`bar${i}`, 'baz'),
      tx.put(`baz${i}`, 'foo'),
    ]);

    // Delete one of the keys in the previous batch.
    if (i > 0) {
      const randomKey = [`foo${i - 1}`, `bar${i - 1}`, `baz${i - 1}`][
        randInt(0, 2)
      ];
      expect(await tx.del(randomKey)).toBe(true);
    }
    await storage.flush();
  }

  const correctVersionIndex = await storage.list(
    {prefix: userValueVersionIndexPrefix},
    userValueVersionInfoSchema,
  );
  expect(correctVersionIndex.size).toBe(150); // 50 * 3 puts

  // Now simulate a corrupted index by randomly
  // (0) leaving them alone, or
  // (1) removing entries, or
  // (2) changing them to an older version to simulate rolling back and forth.
  for (const [key] of correctVersionIndex) {
    const {userKey, version} = decodeUserValueVersionKey(key);
    switch (randInt(0, 2)) {
      case 1:
        await storage.del(key);
        break;
      case 2:
        await storage.put(
          userValueVersionKey(userKey, Math.floor(version / 2)),
          randInt(0, 1) === 0 ? {} : {deleted: true},
        );
        await storage.del(key);
        break;
    }
  }

  // Sanity check that the "corrupted" version index is now different from the original.
  const corruptedVersionIndex = await storage.list(
    {prefix: userValueVersionIndexPrefix},
    userValueVersionInfoSchema,
  );
  expect(corruptedVersionIndex).not.toEqual(correctVersionIndex);

  // Run the backfill.
  await backfillVersionIndex(createSilentLogContext(), storage);

  // Check that the index is restored.
  const backfilledVersionIndex = await storage.list(
    {prefix: userValueVersionIndexPrefix},
    userValueVersionInfoSchema,
  );
  expect(backfilledVersionIndex).toEqual(correctVersionIndex);
});
