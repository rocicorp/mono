import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';
import {assertNotUndefined} from '../asserts';
import * as dag from '../dag/mod';
import {DEFAULT_HEAD_NAME} from './commit';
import {
  readCommit,
  readCommitForBTreeRead,
  readIndexesForRead,
  whenceHead,
} from './read';
import {initDB, Write} from './write';
import {encodeIndexKey} from './index';
import {asyncIterableToArray} from '../async-iterable-to-array';
import {BTreeRead} from '../btree/mod';

test('basics', async () => {
  const ds = new dag.TestStore();
  const lc = new LogContext();
  await initDB(await ds.write(), DEFAULT_HEAD_NAME);

  // Put.
  await ds.withWrite(async dagWrite => {
    const w = await Write.newLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
    );
    await w.put(lc, 'foo', 'bar');
    // Assert we can read the same value from within this transaction.;
    const val = await w.get('foo');
    expect(val).to.deep.equal('bar');
    await w.commit(DEFAULT_HEAD_NAME);
  });

  // As well as after it has committed.
  await ds.withWrite(async dagWrite => {
    const w = await Write.newLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify(null),
      null,
      dagWrite,
      42,
    );
    const val = await w.get('foo');
    expect(val).to.deep.equal('bar');
  });

  // Del.
  await ds.withWrite(async dagWrite => {
    const w = await Write.newLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
    );
    await w.del(lc, 'foo');
    // Assert it is gone while still within this transaction.
    const val = await w.get('foo');
    expect(val).to.be.undefined;
    await w.commit(DEFAULT_HEAD_NAME);
  });

  // As well as after it has committed.
  await ds.withWrite(async dagWrite => {
    const w = await Write.newLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify(null),
      null,
      dagWrite,
      42,
    );
    const val = await w.get(`foo`);
    expect(val).to.be.undefined;
  });
});

test('index commit type constraints', async () => {
  const ds = new dag.TestStore();
  const lc = new LogContext();
  await initDB(await ds.write(), DEFAULT_HEAD_NAME);

  // Test that local changes cannot create or drop an index.
  const w = await Write.newLocal(
    whenceHead(DEFAULT_HEAD_NAME),
    'mutator_name',
    JSON.stringify([]),
    null,
    await ds.write(),
    42,
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
  const ds = new dag.TestStore();
  const lc = new LogContext();
  await ds.withWrite(dagWrite => initDB(dagWrite, DEFAULT_HEAD_NAME));
  await ds.withWrite(async dagWrite => {
    const w = await Write.newLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
    );
    await w.put(lc, 'foo', 'bar');
    await w.commit(DEFAULT_HEAD_NAME);
  });

  await ds.withWrite(async dagWrite => {
    const w = await Write.newIndexChange(
      whenceHead(DEFAULT_HEAD_NAME),
      dagWrite,
    );
    await w.createIndex(lc, 'idx', '', '', false);
    await w.commit(DEFAULT_HEAD_NAME);
  });

  await ds.withWrite(async dagWrite => {
    const w = await Write.newLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
    );
    await w.put(lc, 'hot', 'dog');

    const keys = await asyncIterableToArray(w.map.keys());
    expect(keys).to.have.lengthOf(2);
    let index = w.indexes.get('idx');
    assertNotUndefined(index);
    await index.withMap(dagWrite, async map => {
      const keys = await asyncIterableToArray(map.keys());
      expect(keys).to.have.lengthOf(2);
    });

    await w.clear();
    const keys2 = await asyncIterableToArray(w.map.keys());
    expect(keys2).to.have.lengthOf(0);
    index = w.indexes.get('idx');
    assertNotUndefined(index);
    await index.withMap(dagWrite, async map => {
      const keys = await asyncIterableToArray(map.keys());
      expect(keys).to.have.lengthOf(0);
    });

    await w.commit(DEFAULT_HEAD_NAME);
  });

  await ds.withRead(async dagRead => {
    const [, c, r] = await readCommitForBTreeRead(
      whenceHead(DEFAULT_HEAD_NAME),
      dagRead,
    );
    const indexes = readIndexesForRead(c);
    const keys = await asyncIterableToArray(r.keys());
    expect(keys).to.have.lengthOf(0);
    const index = indexes.get('idx');
    assertNotUndefined(index);
    await index.withMap(dagRead, async map => {
      const keys = await asyncIterableToArray(map.keys());
      expect(keys).to.have.lengthOf(0);
    });
  });
});

test('create and drop index', async () => {
  const t = async (writeBeforeIndexing: boolean) => {
    const ds = new dag.TestStore();
    const lc = new LogContext();
    await ds.withWrite(dagWrite => initDB(dagWrite, DEFAULT_HEAD_NAME));

    if (writeBeforeIndexing) {
      await ds.withWrite(async dagWrite => {
        const w = await Write.newLocal(
          whenceHead(DEFAULT_HEAD_NAME),
          'mutator_name',
          JSON.stringify([]),
          null,
          dagWrite,
          42,
        );
        for (let i = 0; i < 3; i++) {
          await w.put(lc, `k${i}`, {s: `s${i}`});
        }
        await w.commit(DEFAULT_HEAD_NAME);
      });
    }

    const indexName = 'i1';
    await ds.withWrite(async dagWrite => {
      const w = await Write.newIndexChange(
        whenceHead(DEFAULT_HEAD_NAME),
        dagWrite,
      );
      await w.createIndex(lc, indexName, '', '/s', false);
      await w.commit(DEFAULT_HEAD_NAME);
    });

    if (!writeBeforeIndexing) {
      await ds.withWrite(async dagWrite => {
        const w = await Write.newLocal(
          whenceHead(DEFAULT_HEAD_NAME),
          'mutator_name',
          JSON.stringify([]),
          null,
          dagWrite,
          42,
        );
        for (let i = 0; i < 3; i++) {
          await w.put(lc, `k${i}`, {s: `s${i}`});
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
      expect(idx.definition.keyPrefix).to.be.empty;
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
      const w = await Write.newIndexChange(
        whenceHead(DEFAULT_HEAD_NAME),
        dagWrite,
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
  const ds = new dag.TestStore();
  const lc = new LogContext();
  await ds.withWrite(dagWrite => initDB(dagWrite, DEFAULT_HEAD_NAME));

  const indexName = 'legacyIndex';
  await ds.withWrite(async dagWrite => {
    const w = await Write.newIndexChange(
      whenceHead(DEFAULT_HEAD_NAME),
      dagWrite,
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
    const w = await Write.newIndexChange(
      whenceHead(DEFAULT_HEAD_NAME),
      dagWrite,
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
