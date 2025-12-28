import {LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import {assert, assertNotUndefined} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import {asyncIterableToArray} from '../async-iterable-to-array.ts';
import {BTreeRead} from '../btree/read.ts';
import {mustGetHeadHash} from '../dag/store.ts';
import {TestStore} from '../dag/test-store.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {deepFreeze, type FrozenJSONValue} from '../frozen-json.ts';
import {withRead, withWriteNoImplicitCommit} from '../with-transactions.ts';
import {DEFAULT_HEAD_NAME, commitFromHead} from './commit.ts';
import {encodeIndexKey} from './index.ts';
import {readIndexesForRead} from './read.ts';
import {initDB} from './test-helpers.ts';
import {newWriteLocal} from './write.ts';

type FormatVersion = Enum<typeof FormatVersion>;

describe('basics w/ commit', () => {
  const t = async (formatVersion: FormatVersion) => {
    const clientID = 'client-id';
    const ds = new TestStore();
    const lc = new LogContext();
    await initDB(
      await ds.write(),
      DEFAULT_HEAD_NAME,
      clientID,
      {},
      formatVersion,
    );

    // Put.
    await withWriteNoImplicitCommit(ds, async dagWrite => {
      const headHash = await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite);
      const w = await newWriteLocal(
        headHash,
        'mutator_name',
        JSON.stringify([]),
        null,
        dagWrite,
        42,
        clientID,
        formatVersion,
      );
      await w.put(lc, 'foo', 'bar');
      // Assert we can read the same value from within this transaction.;
      const val = await w.get('foo');
      expect(val).toEqual('bar');
      await w.commit(DEFAULT_HEAD_NAME);
    });

    // As well as after it has committed.
    await withWriteNoImplicitCommit(ds, async dagWrite => {
      const w = await newWriteLocal(
        await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite),
        'mutator_name',
        JSON.stringify(null),
        null,
        dagWrite,
        42,
        clientID,
        formatVersion,
      );
      const val = await w.get('foo');
      expect(val).toEqual('bar');
    });

    // Del.
    await withWriteNoImplicitCommit(ds, async dagWrite => {
      const w = await newWriteLocal(
        await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite),
        'mutator_name',
        JSON.stringify([]),
        null,
        dagWrite,
        42,
        clientID,
        formatVersion,
      );
      await w.del(lc, 'foo');
      // Assert it is gone while still within this transaction.
      const val = await w.get('foo');
      expect(val).toBeUndefined();
      await w.commit(DEFAULT_HEAD_NAME);
    });

    // As well as after it has committed.
    await withWriteNoImplicitCommit(ds, async dagWrite => {
      const w = await newWriteLocal(
        await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite),
        'mutator_name',
        JSON.stringify(null),
        null,
        dagWrite,
        42,
        clientID,
        formatVersion,
      );
      const val = await w.get(`foo`);
      expect(val).toBeUndefined();
    });
  };

  test('dd31', () => t(FormatVersion.Latest));
});

describe('basics w/ putCommit', () => {
  const t = async (formatVersion: FormatVersion) => {
    const clientID = 'client-id';
    const ds = new TestStore();
    const lc = new LogContext();
    await initDB(
      await ds.write(),
      DEFAULT_HEAD_NAME,
      clientID,
      {},
      formatVersion,
    );

    // Put.
    const commit1 = await withWriteNoImplicitCommit(ds, async dagWrite => {
      const w = await newWriteLocal(
        await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite),
        'mutator_name',
        JSON.stringify([]),
        null,
        dagWrite,
        42,
        clientID,
        formatVersion,
      );
      await w.put(lc, 'foo', 'bar');
      // Assert we can read the same value from within this transaction.;
      const val = await w.get('foo');
      expect(val).toEqual('bar');
      const commit = await w.putCommit();
      await dagWrite.setHead('test', commit.chunk.hash);
      await dagWrite.commit();
      return commit;
    });

    // As well as from the Commit that was put.
    await withWriteNoImplicitCommit(ds, async dagWrite => {
      const w = await newWriteLocal(
        commit1.chunk.hash,
        'mutator_name',
        JSON.stringify(null),
        null,
        dagWrite,
        42,
        clientID,
        formatVersion,
      );
      const val = await w.get('foo');
      expect(val).toEqual('bar');
    });

    // Del.
    const commit2 = await withWriteNoImplicitCommit(ds, async dagWrite => {
      const w = await newWriteLocal(
        commit1.chunk.hash,
        'mutator_name',
        JSON.stringify([]),
        null,
        dagWrite,
        42,
        clientID,
        formatVersion,
      );
      await w.del(lc, 'foo');
      // Assert it is gone while still within this transaction.
      const val = await w.get('foo');
      expect(val).toBeUndefined();
      const commit = await w.putCommit();
      await dagWrite.setHead('test', commit.chunk.hash);
      await dagWrite.commit();
      return commit;
    });

    // As well as from the commit after it was put.
    await withWriteNoImplicitCommit(ds, async dagWrite => {
      const w = await newWriteLocal(
        commit2.chunk.hash,
        'mutator_name',
        JSON.stringify(null),
        null,
        dagWrite,
        42,
        clientID,
        formatVersion,
      );
      const val = await w.get(`foo`);
      expect(val).toBeUndefined();
    });
  };
  test('dd31', () => t(FormatVersion.Latest));
});

test('clear', async () => {
  const formatVersion = FormatVersion.Latest;
  const clientID = 'client-id';
  const ds = new TestStore();
  const lc = new LogContext();
  await withWriteNoImplicitCommit(ds, dagWrite =>
    initDB(
      dagWrite,
      DEFAULT_HEAD_NAME,
      clientID,

      {
        idx: {prefix: '', jsonPointer: '', allowEmpty: false},
      },
      FormatVersion.Latest,
    ),
  );
  await withWriteNoImplicitCommit(ds, async dagWrite => {
    const w = await newWriteLocal(
      await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite),
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
      FormatVersion.Latest,
    );
    await w.put(lc, 'foo', 'bar');
    await w.commit(DEFAULT_HEAD_NAME);
  });

  await withWriteNoImplicitCommit(ds, async dagWrite => {
    const w = await newWriteLocal(
      await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite),
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
      FormatVersion.Latest,
    );
    await w.put(lc, 'hot', 'dog');

    const keys = await asyncIterableToArray(w.map.keys());
    expect(keys).toHaveLength(2);
    let index = w.indexes.get('idx');
    assertNotUndefined(index);
    {
      const keys = await asyncIterableToArray(index.map.keys());
      expect(keys).toHaveLength(2);
    }

    await w.clear();
    const keys2 = await asyncIterableToArray(w.map.keys());
    expect(keys2).toHaveLength(0);
    index = w.indexes.get('idx');
    assertNotUndefined(index);
    {
      const keys = await asyncIterableToArray(index.map.keys());
      expect(keys).toHaveLength(0);
    }

    await w.commit(DEFAULT_HEAD_NAME);
  });

  await withRead(ds, async dagRead => {
    const c = await commitFromHead(DEFAULT_HEAD_NAME, dagRead);
    const r = new BTreeRead(dagRead, formatVersion, c.valueHash);
    const indexes = readIndexesForRead(c, dagRead, formatVersion);
    const keys = await asyncIterableToArray(r.keys());
    expect(keys).toHaveLength(0);
    const index = indexes.get('idx');
    assertNotUndefined(index);
    {
      const keys = await asyncIterableToArray(index.map.keys());
      expect(keys).toHaveLength(0);
    }
  });
});

test('mutationID on newWriteLocal', async () => {
  const clientID = 'client-id';
  const ds = new TestStore();
  const lc = new LogContext();
  await withWriteNoImplicitCommit(ds, dagWrite =>
    initDB(
      dagWrite,
      DEFAULT_HEAD_NAME,
      clientID,

      {
        idx: {prefix: '', jsonPointer: '', allowEmpty: false},
      },
      FormatVersion.Latest,
    ),
  );
  await withWriteNoImplicitCommit(ds, async dagWrite => {
    const headHash = await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite);
    const w = await newWriteLocal(
      headHash,
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
      FormatVersion.Latest,
    );
    await w.put(lc, 'foo', 'bar');
    await w.commit(DEFAULT_HEAD_NAME);
    expect(await w.getMutationID()).equals(1);
  });

  await withWriteNoImplicitCommit(ds, async dagWrite => {
    const headHash = await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite);
    const w = await newWriteLocal(
      headHash,
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
      FormatVersion.Latest,
    );
    await w.put(lc, 'hot', 'dog');
    await w.commit(DEFAULT_HEAD_NAME);
    expect(await w.getMutationID()).equals(2);
  });
});

test('putMany', async () => {
  const clientID = 'client-id';
  const ds = new TestStore();
  const lc = new LogContext();
  await withWriteNoImplicitCommit(ds, dagWrite =>
    initDB(
      dagWrite,
      DEFAULT_HEAD_NAME,
      clientID,

      {
        idx: {prefix: '', jsonPointer: '', allowEmpty: false},
      },
      FormatVersion.Latest,
    ),
  );
  await withWriteNoImplicitCommit(ds, async dagWrite => {
    const headHash = await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite);
    const w = await newWriteLocal(
      headHash,
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
      FormatVersion.Latest,
    );
    await w.putMany(lc, [
      ['a', 'A'],
      ['b', 'B'],
    ]);
    await w.commit(DEFAULT_HEAD_NAME);
  });

  await withWriteNoImplicitCommit(ds, async dagWrite => {
    const headHash = await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite);
    const w = await newWriteLocal(
      headHash,
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
      FormatVersion.Latest,
    );
    expect(await w.get('a')).toBe('A');
    expect(await w.get('b')).toBe('B');
    const idx = w.indexes.get('idx')?.map;
    assert(idx);

    expect(await asyncIterableToArray(idx.entries())).toEqual([
      [encodeIndexKey(['A', 'a']), 'A', 26],
      [encodeIndexKey(['B', 'b']), 'B', 26],
    ]);
  });
});

test('putMany with existing entries and indexes', async () => {
  const clientID = 'client-id';
  const ds = new TestStore();
  const lc = new LogContext();
  await withWriteNoImplicitCommit(ds, dagWrite =>
    initDB(
      dagWrite,
      DEFAULT_HEAD_NAME,
      clientID,
      {
        idx: {prefix: '', jsonPointer: '/value', allowEmpty: false},
      },
      FormatVersion.Latest,
    ),
  );

  // First, add some initial entries
  await withWriteNoImplicitCommit(ds, async dagWrite => {
    const headHash = await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite);
    const w = await newWriteLocal(
      headHash,
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
      FormatVersion.Latest,
    );
    await w.put(lc, 'x', deepFreeze({value: 'X'}));
    await w.put(lc, 'y', deepFreeze({value: 'Y'}));
    await w.commit(DEFAULT_HEAD_NAME);
  });

  // Now use putMany to add more entries and update existing ones
  await withWriteNoImplicitCommit(ds, async dagWrite => {
    const headHash = await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite);
    const w = await newWriteLocal(
      headHash,
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
      FormatVersion.Latest,
    );
    await w.putMany(lc, [
      ['a', deepFreeze({value: 'A'})],
      ['b', deepFreeze({value: 'B'})],
      ['x', deepFreeze({value: 'X_updated'})], // Update existing
      ['z', deepFreeze({value: 'Z'})],
    ]);
    await w.commit(DEFAULT_HEAD_NAME);
  });

  // Verify all entries and indexes
  await withWriteNoImplicitCommit(ds, async dagRead => {
    const headHash = await mustGetHeadHash(DEFAULT_HEAD_NAME, dagRead);
    const w = await newWriteLocal(
      headHash,
      'mutator_name',
      JSON.stringify([]),
      null,
      dagRead,
      42,
      clientID,
      FormatVersion.Latest,
    );

    expect(await w.get('a')).toEqual({value: 'A'});
    expect(await w.get('b')).toEqual({value: 'B'});
    expect(await w.get('x')).toEqual({value: 'X_updated'});
    expect(await w.get('y')).toEqual({value: 'Y'});
    expect(await w.get('z')).toEqual({value: 'Z'});

    const idx = w.indexes.get('idx')?.map;
    assert(idx);

    const indexEntries = await asyncIterableToArray(idx.keys());
    expect(indexEntries).toHaveLength(5);
    // Verify index contains all the values
    expect(await idx.has(encodeIndexKey(['A', 'a']))).toBe(true);
    expect(await idx.has(encodeIndexKey(['B', 'b']))).toBe(true);
    expect(await idx.has(encodeIndexKey(['X_updated', 'x']))).toBe(true);
    expect(await idx.has(encodeIndexKey(['Y', 'y']))).toBe(true);
    expect(await idx.has(encodeIndexKey(['Z', 'z']))).toBe(true);
  });
});

test('putMany with large batch and indexes', async () => {
  const clientID = 'client-id';
  const ds = new TestStore();
  const lc = new LogContext();
  await withWriteNoImplicitCommit(ds, dagWrite =>
    initDB(
      dagWrite,
      DEFAULT_HEAD_NAME,
      clientID,
      {
        idx: {prefix: '', jsonPointer: '/id', allowEmpty: false},
      },
      FormatVersion.Latest,
    ),
  );

  // Use putMany with a large batch
  await withWriteNoImplicitCommit(ds, async dagWrite => {
    const headHash = await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite);
    const w = await newWriteLocal(
      headHash,
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
      FormatVersion.Latest,
    );

    const entries: Array<[string, FrozenJSONValue]> = [];
    for (let i = 0; i < 100; i++) {
      entries.push([`key${i.toString().padStart(3, '0')}`, {id: i}]);
    }

    await w.putMany(lc, entries);
    await w.commit(DEFAULT_HEAD_NAME);
  });

  // Verify entries are accessible
  await withWriteNoImplicitCommit(ds, async dagWrite => {
    const headHash = await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite);
    const w = await newWriteLocal(
      headHash,
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
      FormatVersion.Latest,
    );

    // Check some entries
    expect(await w.get('key000')).toEqual({id: 0});
    expect(await w.get('key050')).toEqual({id: 50});
    expect(await w.get('key099')).toEqual({id: 99});

    // Count all entries to verify bulk load worked
    const allKeys = await asyncIterableToArray(w.map.keys());
    expect(allKeys).toHaveLength(100);
  });
});
