import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';
import {assertNotUndefined} from '../asserts';
import * as dag from '../dag/mod';
import {DEFAULT_HEAD_NAME, IndexRecord} from './commit';
import {
  readCommit,
  readCommitForBTreeRead,
  readIndexesForRead,
  whenceHash,
  whenceHead,
} from './read';
import {newWriteIndexChange, newWriteLocal} from './write';
import {encodeIndexKey} from './index';
import {asyncIterableToArray} from '../async-iterable-to-array';
import {BTreeRead} from '../btree/mod';
import {toInternalValue, ToInternalValueReason} from '../internal-value';
import {initDB} from './test-helpers';
import type {IndexDefinitions} from '../index-defs';
import type {Writable} from '../writable';
import {commitFromHead} from './mod';

test('basics w/ commit', async () => {
  const clientID = 'client-id';
  const ds = new dag.TestStore();
  const lc = new LogContext();
  await initDB(await ds.write(), DEFAULT_HEAD_NAME, clientID);

  // Put.
  await ds.withWrite(async dagWrite => {
    const w = await newWriteLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
    );
    await w.put(lc, 'foo', 'bar');
    // Assert we can read the same value from within this transaction.;
    const val = await w.get('foo');
    expect(val).to.deep.equal('bar');
    await w.commit(DEFAULT_HEAD_NAME);
  });

  // As well as after it has committed.
  await ds.withWrite(async dagWrite => {
    const w = await newWriteLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify(null),
      null,
      dagWrite,
      42,
      clientID,
    );
    const val = await w.get('foo');
    expect(val).to.deep.equal('bar');
  });

  // Del.
  await ds.withWrite(async dagWrite => {
    const w = await newWriteLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
    );
    await w.del(lc, 'foo');
    // Assert it is gone while still within this transaction.
    const val = await w.get('foo');
    expect(val).to.be.undefined;
    await w.commit(DEFAULT_HEAD_NAME);
  });

  // As well as after it has committed.
  await ds.withWrite(async dagWrite => {
    const w = await newWriteLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify(null),
      null,
      dagWrite,
      42,
      clientID,
    );
    const val = await w.get(`foo`);
    expect(val).to.be.undefined;
  });
});

test('basics w/ putCommit', async () => {
  const clientID = 'client-id';
  const ds = new dag.TestStore();
  const lc = new LogContext();
  await initDB(await ds.write(), DEFAULT_HEAD_NAME, clientID);

  // Put.
  const commit1 = await ds.withWrite(async dagWrite => {
    const w = await newWriteLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
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
  await ds.withWrite(async dagWrite => {
    const w = await newWriteLocal(
      whenceHash(commit1.chunk.hash),
      'mutator_name',
      JSON.stringify(null),
      null,
      dagWrite,
      42,
      clientID,
    );
    const val = await w.get('foo');
    expect(val).to.deep.equal('bar');
  });

  // Del.
  const commit2 = await ds.withWrite(async dagWrite => {
    const w = await newWriteLocal(
      whenceHash(commit1.chunk.hash),
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
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
  await ds.withWrite(async dagWrite => {
    const w = await newWriteLocal(
      whenceHash(commit2.chunk.hash),
      'mutator_name',
      JSON.stringify(null),
      null,
      dagWrite,
      42,
      clientID,
    );
    const val = await w.get(`foo`);
    expect(val).to.be.undefined;
  });
});

test('index commit type constraints', async () => {
  if (DD31) {
    // db.Write createIndex is going away
    return;
  }
  const clientID = 'client-id';
  const ds = new dag.TestStore();
  const lc = new LogContext();
  await initDB(await ds.write(), DEFAULT_HEAD_NAME, clientID);

  // Test that local changes cannot create or drop an index.
  const w = await newWriteLocal(
    whenceHead(DEFAULT_HEAD_NAME),
    'mutator_name',
    JSON.stringify([]),
    null,
    await ds.write(),
    42,
    clientID,
  );

  let err;
  try {
    await w.createIndex(lc, 'foo', '', '', false);
  } catch (e) {
    err = e;
  }
  expect(err).to.be.an.instanceof(Error);
  expect(err).to.have.property('message', 'Not allowed');

  err = undefined;
  try {
    await w.dropIndex('foo');
  } catch (e) {
    err = e;
  }
  expect(err).to.be.an.instanceof(Error);
  expect(err).to.have.property('message', 'Not allowed');
});

test('clear', async () => {
  const clientID = 'client-id';
  const ds = new dag.TestStore();
  const lc = new LogContext();
  await ds.withWrite(dagWrite =>
    initDB(
      dagWrite,
      DEFAULT_HEAD_NAME,
      clientID,
      DD31
        ? {
            idx: {prefix: '', jsonPointer: '', allowEmpty: false},
          }
        : {},
    ),
  );
  await ds.withWrite(async dagWrite => {
    const w = await newWriteLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
    );
    await w.put(lc, 'foo', 'bar');
    await w.commit(DEFAULT_HEAD_NAME);
  });

  if (!DD31) {
    await ds.withWrite(async dagWrite => {
      const w = await newWriteIndexChange(
        whenceHead(DEFAULT_HEAD_NAME),
        dagWrite,
        clientID,
      );
      await w.createIndex(lc, 'idx', '', '', false);
      await w.commit(DEFAULT_HEAD_NAME);
    });
  }

  await ds.withWrite(async dagWrite => {
    const w = await newWriteLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
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

  await ds.withRead(async dagRead => {
    const [, c, r] = await readCommitForBTreeRead(
      whenceHead(DEFAULT_HEAD_NAME),
      dagRead,
    );
    const indexes = readIndexesForRead(c, dagRead);
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

test('create and drop index', async () => {
  if (DD31) {
    return;
  }
  const t = async (writeBeforeIndexing: boolean) => {
    const clientID = 'client-id';
    const ds = new dag.TestStore();
    const lc = new LogContext();
    await ds.withWrite(dagWrite =>
      initDB(dagWrite, DEFAULT_HEAD_NAME, clientID),
    );

    if (writeBeforeIndexing) {
      await ds.withWrite(async dagWrite => {
        const w = await newWriteLocal(
          whenceHead(DEFAULT_HEAD_NAME),
          'mutator_name',
          JSON.stringify([]),
          null,
          dagWrite,
          42,
          clientID,
        );
        for (let i = 0; i < 3; i++) {
          await w.put(
            lc,
            `k${i}`,
            toInternalValue({s: `s${i}`}, ToInternalValueReason.Test),
          );
        }
        await w.commit(DEFAULT_HEAD_NAME);
      });
    }

    const indexName = 'i1';
    await ds.withWrite(async dagWrite => {
      const w = await newWriteIndexChange(
        whenceHead(DEFAULT_HEAD_NAME),
        dagWrite,
        clientID,
      );
      await w.createIndex(lc, indexName, '', '/s', false);
      await w.commit(DEFAULT_HEAD_NAME);
    });

    if (!writeBeforeIndexing) {
      await ds.withWrite(async dagWrite => {
        const w = await newWriteLocal(
          whenceHead(DEFAULT_HEAD_NAME),
          'mutator_name',
          JSON.stringify([]),
          null,
          dagWrite,
          42,
          clientID,
        );
        for (let i = 0; i < 3; i++) {
          await w.put(
            lc,
            `k${i}`,
            toInternalValue({s: `s${i}`}, ToInternalValueReason.Test),
          );
        }
        await w.commit(DEFAULT_HEAD_NAME);
      });
    }

    await ds.withRead(async dagRead => {
      const [, c] = await readCommit(whenceHead(DEFAULT_HEAD_NAME), dagRead);
      const {indexes} = c;
      expect(indexes).to.have.lengthOf(1);
      const idx = indexes[0];
      expect(idx.definition.name).to.equal(indexName);
      expect(idx.definition.prefix).to.be.empty;
      expect(idx.definition.jsonPointer).to.equal('/s');
      expect(idx.definition.allowEmpty).to.be.false;
      const indexMap = new BTreeRead(dagRead, idx.valueHash);

      const entries = await asyncIterableToArray(indexMap);
      expect(entries).to.have.lengthOf(3);
      for (let i = 0; i < 3; i++) {
        expect(entries[i][0]).to.deep.equal(encodeIndexKey([`s${i}`, `k${i}`]));
      }
    });

    // Ensure drop works.
    await ds.withWrite(async dagWrite => {
      const w = await newWriteIndexChange(
        whenceHead(DEFAULT_HEAD_NAME),
        dagWrite,
        clientID,
      );
      await w.dropIndex(indexName);
      await w.commit(DEFAULT_HEAD_NAME);
      const [, c] = await readCommit(whenceHead(DEFAULT_HEAD_NAME), dagWrite);
      const {indexes} = c;
      expect(indexes).to.be.empty;
    });
  };

  await t(true);
  await t(false);
});

test('legacy index definitions imply allowEmpty = false', async () => {
  if (DD31) {
    return;
  }
  const clientID = 'client-id';
  const ds = new dag.TestStore();
  const lc = new LogContext();
  await ds.withWrite(dagWrite => initDB(dagWrite, DEFAULT_HEAD_NAME, clientID));

  const indexName = 'legacyIndex';
  await ds.withWrite(async dagWrite => {
    const w = await newWriteIndexChange(
      whenceHead(DEFAULT_HEAD_NAME),
      dagWrite,
      clientID,
    );
    await w.createIndex(lc, indexName, '', '', false);
    await w.commit(DEFAULT_HEAD_NAME);
  });

  await ds.withWrite(async dagWrite => {
    const [, commit] = await readCommit(
      whenceHead(DEFAULT_HEAD_NAME),
      dagWrite,
    );
    // tweak the index def to look like an old one
    const indexDef = commit.indexes[0].definition as {allowEmpty?: boolean};
    indexDef.allowEmpty = undefined;
  });

  await ds.withWrite(async dagWrite => {
    const w = await newWriteIndexChange(
      whenceHead(DEFAULT_HEAD_NAME),
      dagWrite,
      clientID,
    );

    let error: unknown;
    try {
      await w.createIndex(lc, indexName, '', '', false);
    } catch (e) {
      error = e;
    }
    expect(error).to.be.undefined;

    await w.commit(DEFAULT_HEAD_NAME);
  });
});

test('resync indexes', async () => {
  if (DD31) {
    return;
  }

  const t = async (
    indexesBefore: IndexDefinitions,
    indexesToSync: IndexDefinitions,
    postTest = (
      _before: readonly IndexRecord[],
      _after: readonly IndexRecord[],
    ) => {
      // noop
    },
  ) => {
    const clientID = 'client-id';
    const dagStore = new dag.TestStore();
    const lc = new LogContext();
    await dagStore.withWrite(dagWrite =>
      initDB(dagWrite, DEFAULT_HEAD_NAME, clientID),
    );

    await dagStore.withWrite(async dagWrite => {
      const w = await newWriteIndexChange(
        whenceHead(DEFAULT_HEAD_NAME),
        dagWrite,
        clientID,
      );
      for (const [name, value] of Object.entries(indexesBefore)) {
        await w.createIndex(
          lc,
          name,
          value.prefix ?? '',
          value.jsonPointer,
          value.allowEmpty ?? false,
        );
      }

      await w.commit(DEFAULT_HEAD_NAME);
    });

    const indexesA = await dagStore.withRead(async dagRead => {
      return (await commitFromHead(DEFAULT_HEAD_NAME, dagRead)).indexes;
    });

    await dagStore.withWrite(async dagWrite => {
      const w = await newWriteIndexChange(
        whenceHead(DEFAULT_HEAD_NAME),
        dagWrite,
        clientID,
      );
      await w.syncIndexes(lc, indexesToSync);
      await w.commit(DEFAULT_HEAD_NAME);
    });

    await dagStore.withRead(async dagRead => {
      const commit = await commitFromHead(DEFAULT_HEAD_NAME, dagRead);
      const {indexes} = commit;
      expect(indexRecordsToIndexDefinitions(indexes)).to.deep.equal(
        indexesToSync,
      );
    });

    const indexesB = await dagStore.withRead(async dagRead => {
      return (await commitFromHead(DEFAULT_HEAD_NAME, dagRead)).indexes;
    });

    postTest(indexesA, indexesB);
  };

  await t({}, {});

  await t(
    {},
    {
      i1: {prefix: '', jsonPointer: '/s', allowEmpty: false},
    },
  );

  await t(
    {
      i1: {prefix: '', jsonPointer: '/s', allowEmpty: false},
    },
    {},
  );

  await t(
    {
      i1: {prefix: '', jsonPointer: '/s', allowEmpty: false},
    },
    {
      i1: {prefix: '', jsonPointer: '/s', allowEmpty: false},
    },
  );

  // Change in prefix
  await t(
    {
      i1: {prefix: '', jsonPointer: '/s', allowEmpty: false},
    },
    {
      i1: {prefix: 'ppp', jsonPointer: '/s', allowEmpty: false},
    },
  );

  // Change in allowEmpty
  await t(
    {
      i1: {prefix: '', jsonPointer: '/s', allowEmpty: false},
    },
    {
      i1: {prefix: '', jsonPointer: '/s', allowEmpty: true},
    },
  );

  // Change in jsonPointer
  await t(
    {
      i1: {prefix: '', jsonPointer: '/s', allowEmpty: false},
    },
    {
      i1: {prefix: '', jsonPointer: '/s/p', allowEmpty: false},
    },
  );

  // Change in name
  await t(
    {
      i1: {prefix: '', jsonPointer: '/s', allowEmpty: false},
    },
    {
      i2: {prefix: '', jsonPointer: '/s', allowEmpty: false},
    },
  );

  // Order
  await t(
    {
      i1: {prefix: 'i1', jsonPointer: '/s', allowEmpty: false},
      i2: {prefix: 'i2', jsonPointer: '/s', allowEmpty: false},
    },
    {
      i2: {prefix: 'i2', jsonPointer: '/s', allowEmpty: false},
      i1: {prefix: 'i1', jsonPointer: '/s', allowEmpty: false},
    },
  );

  // Add more indexes
  await t(
    {
      i1: {prefix: '', jsonPointer: '/s', allowEmpty: false},
    },
    {
      i1: {prefix: '', jsonPointer: '/s', allowEmpty: false},
      i2: {prefix: '', jsonPointer: '/s', allowEmpty: false},
    },
  );

  // Remove indexes
  await t(
    {
      i1: {prefix: '', jsonPointer: '/s', allowEmpty: false},
      i2: {prefix: '', jsonPointer: '/s', allowEmpty: false},
    },
    {
      i1: {prefix: '', jsonPointer: '/s', allowEmpty: false},
    },
  );

  // Rename indexes
  await t(
    {
      i1: {prefix: 'a', jsonPointer: '/s', allowEmpty: false},
      i2: {prefix: 'b', jsonPointer: '/s', allowEmpty: false},
    },
    {
      i3: {prefix: 'b', jsonPointer: '/s', allowEmpty: false},
      i4: {prefix: 'a', jsonPointer: '/s', allowEmpty: false},
    },
  );

  await t(
    {
      i1: {prefix: 'a', jsonPointer: '/s', allowEmpty: false},
    },
    {
      i2: {prefix: 'a', jsonPointer: '/s', allowEmpty: false},
    },
    (before: readonly IndexRecord[], after: readonly IndexRecord[]) => {
      expect(before[0].definition.name).to.equal('i1');
      expect(after[0].definition.name).to.equal('i2');
      expect(before[0].valueHash).to.equal(after[0].valueHash);
    },
  );

  await t(
    {
      i1: {prefix: 'a', jsonPointer: '/s', allowEmpty: false},
      i2: {prefix: 'b', jsonPointer: '/s', allowEmpty: false},
    },
    {
      i1: {prefix: 'b', jsonPointer: '/s', allowEmpty: false},
      i2: {prefix: 'a', jsonPointer: '/s', allowEmpty: false},
    },
    (before: readonly IndexRecord[], after: readonly IndexRecord[]) => {
      expect(before[0].valueHash).equal(after[1].valueHash);
      expect(before[1].valueHash).equal(after[0].valueHash);
    },
  );

  await t(
    {
      i1: {prefix: 'a', jsonPointer: '/s', allowEmpty: false},
    },
    {
      i2: {prefix: 'a', jsonPointer: '/s', allowEmpty: false},
      i3: {prefix: 'a', jsonPointer: '/s', allowEmpty: false},
    },
    (before: readonly IndexRecord[], after: readonly IndexRecord[]) => {
      expect(before[0].valueHash).equal(after[0].valueHash);
      expect(before[0].valueHash).equal(after[1].valueHash);
    },
  );
});

function indexRecordsToIndexDefinitions(
  indexes: readonly IndexRecord[],
): IndexDefinitions {
  const defs: Writable<IndexDefinitions> = {};
  for (const index of indexes) {
    defs[index.definition.name] = {
      prefix: index.definition.prefix,
      jsonPointer: index.definition.jsonPointer,
      allowEmpty: index.definition.allowEmpty,
    };
  }
  return defs;
}
