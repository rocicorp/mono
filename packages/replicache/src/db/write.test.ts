import {LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import {assertNotUndefined} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import {asyncIterableToArray} from '../async-iterable-to-array.ts';
import {BTreeRead} from '../btree/read.ts';
import {mustGetHeadHash} from '../dag/store.ts';
import {TestStore} from '../dag/test-store.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {withRead, withWriteNoImplicitCommit} from '../with-transactions.ts';
import {DEFAULT_HEAD_NAME, commitFromHead} from './commit.ts';
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
      expect(val).to.deep.equal('bar');
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
      expect(val).to.deep.equal('bar');
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
      expect(val).to.be.undefined;
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
      expect(val).to.be.undefined;
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
      expect(val).to.deep.equal('bar');
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
      expect(val).to.deep.equal('bar');
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
      expect(val).to.be.undefined;
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
      expect(val).to.be.undefined;
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
    expect(keys).to.have.lengthOf(2);
    let index = w.indexes.get('idx');
    assertNotUndefined(index);
    {
      const keys = await asyncIterableToArray(index.map.keys());
      expect(keys).to.have.lengthOf(2);
    }

    await w.clear();
    const keys2 = await asyncIterableToArray(w.map.keys());
    expect(keys2).to.have.lengthOf(0);
    index = w.indexes.get('idx');
    assertNotUndefined(index);
    {
      const keys = await asyncIterableToArray(index.map.keys());
      expect(keys).to.have.lengthOf(0);
    }

    await w.commit(DEFAULT_HEAD_NAME);
  });

  await withRead(ds, async dagRead => {
    const c = await commitFromHead(DEFAULT_HEAD_NAME, dagRead);
    const r = new BTreeRead(dagRead, formatVersion, c.valueHash);
    const indexes = readIndexesForRead(c, dagRead, formatVersion);
    const keys = await asyncIterableToArray(r.keys());
    expect(keys).to.have.lengthOf(0);
    const index = indexes.get('idx');
    assertNotUndefined(index);
    {
      const keys = await asyncIterableToArray(index.map.keys());
      expect(keys).to.have.lengthOf(0);
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
