import {assert} from '@esm-bundle/chai';
import * as dag from '../dag/mod';
import * as db from '../db/mod';
import {fakeHash, makeNewTempHashFunction, parse as parseHash} from '../hash';
import {BTreeWrite} from '../btree/write';
import {FixupTransformer} from './fixup-transformer';
import type {ReadonlyJSONValue} from '../json';
import {toInternalValue, ToInternalValueReason} from '../internal-value.js';

test('fixup of a single snapshot commit with empty btree', async () => {
  const memdag = new dag.TestStore(
    undefined,
    makeNewTempHashFunction(),
    () => undefined,
  );

  const [headHash, valueHash] = await memdag.withWrite(async dagWrite => {
    const tree = new BTreeWrite(dagWrite);
    const valueHash = await tree.flush();
    const c = db.newSnapshot(
      dagWrite.createChunk,
      null,
      0,
      null,
      valueHash,
      [],
    );
    await dagWrite.putChunk(c.chunk);
    await dagWrite.setHead('test', c.chunk.hash);
    await dagWrite.commit();
    return [c.chunk.hash, valueHash];
  });

  const snapshot = memdag.kvStore.snapshot();

  assert.deepEqual(snapshot, {
    'c/t/000000000000000000000000000000/d': [0, []],
    'c/t/000000000000000000000000000001/d': {
      meta: {type: 3, basisHash: null, lastMutationID: 0, cookieJSON: null},
      valueHash: 't/000000000000000000000000000000',
      indexes: [],
    },
    'c/t/000000000000000000000000000001/m': [
      't/000000000000000000000000000000',
    ],
    'h/test': 't/000000000000000000000000000001',
    'c/t/000000000000000000000000000000/r': 1,
    'c/t/000000000000000000000000000001/r': 1,
  });

  const mappings = new Map([
    [headHash, fakeHash('head')],
    [valueHash, fakeHash('value')],
  ]);

  const newHeadHash = await memdag.withWrite(async dagWrite => {
    const transformer = new FixupTransformer(dagWrite, mappings);
    const newHeadHash = await transformer.transformCommit(headHash);

    await dagWrite.setHead('test', newHeadHash);
    await dagWrite.commit();
    return newHeadHash;
  });

  assert.equal(newHeadHash, fakeHash('head'));

  assert.deepEqual(memdag.kvStore.snapshot(), {
    'h/test': 'fake000000000000000000000000head',
    'c/fake00000000000000000000000value/d': [0, []],
    'c/fake000000000000000000000000head/d': {
      meta: {type: 3, basisHash: null, lastMutationID: 0, cookieJSON: null},
      valueHash: 'fake00000000000000000000000value',
      indexes: [],
    },
    'c/fake000000000000000000000000head/m': [
      'fake00000000000000000000000value',
    ],
    'c/fake00000000000000000000000value/r': 1,
    'c/fake000000000000000000000000head/r': 1,
  });

  // Now add a local commit on top of the snapshot commit.
  {
    const headHash = await memdag.withWrite(async dagWrite => {
      const c = db.newLocal(
        dagWrite.createChunk,
        newHeadHash,
        1,
        'test',
        toInternalValue({v: 42}, ToInternalValueReason.Test),
        null,
        fakeHash('value'),
        [],
        42,
      );
      await dagWrite.putChunk(c.chunk);
      await dagWrite.setHead('test', c.chunk.hash);
      await dagWrite.commit();
      return c.chunk.hash;
    });

    assert.deepEqual(memdag.kvStore.snapshot(), {
      'c/fake000000000000000000000000head/d': {
        indexes: [],
        meta: {
          basisHash: null,
          cookieJSON: null,
          lastMutationID: 0,
          type: 3,
        },
        valueHash: 'fake00000000000000000000000value',
      },
      'c/fake000000000000000000000000head/m': [
        'fake00000000000000000000000value',
      ],
      'c/fake000000000000000000000000head/r': 1,
      'c/fake00000000000000000000000value/d': [0, []],
      'c/fake00000000000000000000000value/r': 2,
      'c/t/000000000000000000000000000002/d': {
        indexes: [],
        meta: {
          basisHash: 'fake000000000000000000000000head',
          mutationID: 1,
          mutatorArgsJSON: {
            v: 42,
          },
          mutatorName: 'test',
          originalHash: null,
          timestamp: 42,
          type: 2,
        },
        valueHash: 'fake00000000000000000000000value',
      },
      'c/t/000000000000000000000000000002/m': [
        'fake00000000000000000000000value',
        'fake000000000000000000000000head',
      ],
      'c/t/000000000000000000000000000002/r': 1,
      'h/test': 't/000000000000000000000000000002',
    });

    const mappings = new Map([[headHash, fakeHash('head2')]]);

    const newHeadHash2 = await memdag.withWrite(async dagWrite => {
      const transformer = new FixupTransformer(dagWrite, mappings);
      const newHeadHash = await transformer.transformCommit(headHash);

      await dagWrite.setHead('test', newHeadHash);
      await dagWrite.commit();
      return newHeadHash;
    });

    assert.deepEqual(memdag.kvStore.snapshot(), {
      'c/fake000000000000000000000000head/d': {
        indexes: [],
        meta: {
          basisHash: null,
          cookieJSON: null,
          lastMutationID: 0,
          type: 3,
        },
        valueHash: 'fake00000000000000000000000value',
      },
      'c/fake000000000000000000000000head/m': [
        'fake00000000000000000000000value',
      ],
      'c/fake000000000000000000000000head/r': 1,
      'c/fake00000000000000000000000head2/d': {
        indexes: [],
        meta: {
          basisHash: 'fake000000000000000000000000head',
          mutationID: 1,
          mutatorArgsJSON: {
            v: 42,
          },
          mutatorName: 'test',
          originalHash: null,
          timestamp: 42,
          type: 2,
        },
        valueHash: 'fake00000000000000000000000value',
      },
      'c/fake00000000000000000000000head2/m': [
        'fake00000000000000000000000value',
        'fake000000000000000000000000head',
      ],
      'c/fake00000000000000000000000head2/r': 1,
      'c/fake00000000000000000000000value/d': [0, []],
      'c/fake00000000000000000000000value/r': 2,
      'h/test': 'fake00000000000000000000000head2',
    });
    assert.equal(newHeadHash2, fakeHash('head2'));
  }
});

test('fixup base snapshot when there is a local commit on top of it', async () => {
  const memdag = new dag.TestStore(
    undefined,
    makeNewTempHashFunction(),
    () => undefined,
  );

  const [snapshotCommit, localCommit, valueHash] = await memdag.withWrite(
    async dagWrite => {
      const tree = new BTreeWrite(dagWrite);
      const valueHash = await tree.flush();
      const snapshotCommit = db.newSnapshot(
        dagWrite.createChunk,
        null,
        0,
        null,
        valueHash,
        [],
      );
      await dagWrite.putChunk(snapshotCommit.chunk);

      const localCommit = db.newLocal(
        dagWrite.createChunk,
        snapshotCommit.chunk.hash,
        1,
        'test',
        toInternalValue({v: 42}, ToInternalValueReason.Test),
        null,
        fakeHash('value'),
        [],
        42,
      );
      await dagWrite.putChunk(localCommit.chunk);

      await dagWrite.setHead('test', localCommit.chunk.hash);
      await dagWrite.commit();
      return [snapshotCommit, localCommit, valueHash];
    },
  );

  assert.deepEqual(memdag.kvStore.snapshot(), {
    'c/fake00000000000000000000000value/r': 1,
    'c/t/000000000000000000000000000000/d': [0, []],
    'c/t/000000000000000000000000000000/r': 1,
    'c/t/000000000000000000000000000001/d': {
      indexes: [],
      meta: {
        basisHash: null,
        cookieJSON: null,
        lastMutationID: 0,
        type: 3,
      },
      valueHash: 't/000000000000000000000000000000',
    },
    'c/t/000000000000000000000000000001/m': [
      't/000000000000000000000000000000',
    ],
    'c/t/000000000000000000000000000001/r': 1,
    'c/t/000000000000000000000000000002/d': {
      indexes: [],
      meta: {
        basisHash: 't/000000000000000000000000000001',
        mutationID: 1,
        mutatorArgsJSON: {
          v: 42,
        },
        mutatorName: 'test',
        originalHash: null,
        timestamp: 42,
        type: 2,
      },
      valueHash: 'fake00000000000000000000000value',
    },
    'c/t/000000000000000000000000000002/m': [
      'fake00000000000000000000000value',
      't/000000000000000000000000000001',
    ],
    'c/t/000000000000000000000000000002/r': 1,
    'h/test': 't/000000000000000000000000000002',
  });

  // These mappings do not contain the local commit. This is simulating that a
  // local commit happened after we got the result back from the perdag persist
  // part.
  const mappings = new Map([
    [snapshotCommit.chunk.hash, fakeHash('snapshot')],
    [valueHash, fakeHash('value')],
  ]);

  const newLocalCommitHash = await memdag.withWrite(async dagWrite => {
    const transformer = new FixupTransformer(dagWrite, mappings);
    const newLocalCommitHash = await transformer.transformCommit(
      localCommit.chunk.hash,
    );

    await dagWrite.setHead('test', newLocalCommitHash);
    await dagWrite.commit();
    return newLocalCommitHash;
  });

  assert.notEqual(newLocalCommitHash, localCommit.chunk.hash);

  assert.deepEqual(memdag.kvStore.snapshot(), {
    'c/fake00000000000000000000000value/d': [0, []],
    'c/fake00000000000000000000000value/r': 2,
    'c/fake00000000000000000000snapshot/d': {
      indexes: [],
      meta: {
        basisHash: null,
        cookieJSON: null,
        lastMutationID: 0,
        type: 3,
      },
      valueHash: 'fake00000000000000000000000value',
    },
    'c/fake00000000000000000000snapshot/m': [
      'fake00000000000000000000000value',
    ],
    'c/fake00000000000000000000snapshot/r': 1,
    'c/t/000000000000000000000000000003/d': {
      indexes: [],
      meta: {
        basisHash: 'fake00000000000000000000snapshot',
        mutationID: 1,
        mutatorArgsJSON: {
          v: 42,
        },
        mutatorName: 'test',
        originalHash: null,
        timestamp: 42,
        type: 2,
      },
      valueHash: 'fake00000000000000000000000value',
    },
    'c/t/000000000000000000000000000003/m': [
      'fake00000000000000000000000value',
      'fake00000000000000000000snapshot',
    ],
    'c/t/000000000000000000000000000003/r': 1,
    'h/test': 't/000000000000000000000000000003',
  });
});

async function makeBTree(
  dagWrite: dag.Write,
  entries: [string, ReadonlyJSONValue][],
): Promise<BTreeWrite> {
  const tree = new BTreeWrite(dagWrite, undefined, 2, 4, () => 1, 0);
  for (const [k, v] of entries) {
    await tree.put(k, toInternalValue(v, ToInternalValueReason.Test));
  }
  return tree;
}

test('fixup of a single snapshot commit with a btree with internal nodes', async () => {
  const memdag = new dag.TestStore(
    undefined,
    makeNewTempHashFunction(),
    () => undefined,
  );

  const entries = Object.entries({
    a: 0,
    b: 1,
    c: 2,
    d: 3,
    e: 4,
    f: 5,
  });

  const [headHash, valueHash] = await memdag.withWrite(async dagWrite => {
    const tree = await makeBTree(dagWrite, entries);

    const valueHash = await tree.flush();
    const c = db.newSnapshot(
      dagWrite.createChunk,
      null,
      0,
      null,
      valueHash,
      [],
    );
    await dagWrite.putChunk(c.chunk);
    await dagWrite.setHead('test', c.chunk.hash);
    await dagWrite.commit();
    return [c.chunk.hash, valueHash];
  });

  assert.deepEqual(
    memdag.kvStore.snapshot(),

    {
      'c/t/000000000000000000000000000000/d': [
        0,
        [
          ['a', 0],
          ['b', 1],
        ],
      ],
      'c/t/000000000000000000000000000001/d': [
        0,
        [
          ['c', 2],
          ['d', 3],
          ['e', 4],
          ['f', 5],
        ],
      ],
      'c/t/000000000000000000000000000002/d': [
        1,
        [
          ['b', 't/000000000000000000000000000000'],
          ['f', 't/000000000000000000000000000001'],
        ],
      ],
      'c/t/000000000000000000000000000002/m': [
        't/000000000000000000000000000000',
        't/000000000000000000000000000001',
      ],
      'c/t/000000000000000000000000000003/d': {
        meta: {
          type: 3,
          basisHash: null,
          lastMutationID: 0,
          cookieJSON: null,
        },
        valueHash: 't/000000000000000000000000000002',
        indexes: [],
      },
      'c/t/000000000000000000000000000003/m': [
        't/000000000000000000000000000002',
      ],
      'h/test': 't/000000000000000000000000000003',
      'c/t/000000000000000000000000000000/r': 1,
      'c/t/000000000000000000000000000001/r': 1,
      'c/t/000000000000000000000000000002/r': 1,
      'c/t/000000000000000000000000000003/r': 1,
    },
  );

  const mappings = new Map([
    [headHash, fakeHash('head')],
    [valueHash, fakeHash('value')],
    [parseHash('t/000000000000000000000000000000'), fakeHash('data0')],
    [parseHash('t/000000000000000000000000000001'), fakeHash('data1')],
  ]);

  const newHeadHash = await memdag.withWrite(async dagWrite => {
    const transformer = new FixupTransformer(dagWrite, mappings);
    const newHeadHash = await transformer.transformCommit(headHash);

    await dagWrite.setHead('test', newHeadHash);
    await dagWrite.commit();
    return newHeadHash;
  });

  assert.equal(newHeadHash, fakeHash('head'));

  assert.deepEqual(
    memdag.kvStore.snapshot(),

    {
      'h/test': 'fake000000000000000000000000head',
      'c/fake00000000000000000000000data0/d': [
        0,
        [
          ['a', 0],
          ['b', 1],
        ],
      ],
      'c/fake00000000000000000000000data1/d': [
        0,
        [
          ['c', 2],
          ['d', 3],
          ['e', 4],
          ['f', 5],
        ],
      ],
      'c/fake00000000000000000000000value/d': [
        1,
        [
          ['b', 'fake00000000000000000000000data0'],
          ['f', 'fake00000000000000000000000data1'],
        ],
      ],
      'c/fake00000000000000000000000value/m': [
        'fake00000000000000000000000data0',
        'fake00000000000000000000000data1',
      ],
      'c/fake000000000000000000000000head/d': {
        meta: {
          type: 3,
          basisHash: null,
          lastMutationID: 0,
          cookieJSON: null,
        },
        valueHash: 'fake00000000000000000000000value',
        indexes: [],
      },
      'c/fake000000000000000000000000head/m': [
        'fake00000000000000000000000value',
      ],
      'c/fake00000000000000000000000data0/r': 1,
      'c/fake00000000000000000000000data1/r': 1,
      'c/fake00000000000000000000000value/r': 1,
      'c/fake000000000000000000000000head/r': 1,
    },
  );
});

test('fixup of a base snapshot with an index', async () => {
  const memdag = new dag.TestStore(
    undefined,
    makeNewTempHashFunction(),
    () => undefined,
  );

  const entries = Object.entries({
    a: {a: '0'},
    b: {a: '1'},
    c: {a: '2'},
    d: {a: '3'},
    e: {a: '4'},
    f: {b: '5'},
  });

  const indexEntries: [string, ReadonlyJSONValue][] = [
    [db.encodeIndexKey(['0', 'a']), {a: '0'}],
    [db.encodeIndexKey(['1', 'b']), {a: '1'}],
    [db.encodeIndexKey(['2', 'c']), {a: '2'}],
    [db.encodeIndexKey(['3', 'd']), {a: '3'}],
    [db.encodeIndexKey(['4', 'e']), {a: '4'}],
  ];

  const [headHash, valueHash, indexHash] = await memdag.withWrite(
    async dagWrite => {
      const tree = await makeBTree(dagWrite, entries);
      const valueHash = await tree.flush();

      const indexTree = await makeBTree(dagWrite, indexEntries);
      const indexHash = await indexTree.flush();

      const c = db.newSnapshot(dagWrite.createChunk, null, 0, null, valueHash, [
        {
          definition: {
            jsonPointer: '/a',
            keyPrefix: '',
            name: 'idx',
          },
          valueHash: indexHash,
        },
      ]);
      await dagWrite.putChunk(c.chunk);
      await dagWrite.setHead('test', c.chunk.hash);
      await dagWrite.commit();
      return [c.chunk.hash, valueHash, indexHash];
    },
  );

  const snapshot = memdag.kvStore.snapshot();

  assert.deepEqual(snapshot, {
    'c/t/000000000000000000000000000000/d': [
      0,
      [
        ['a', {a: '0'}],
        ['b', {a: '1'}],
      ],
    ],
    'c/t/000000000000000000000000000001/d': [
      0,
      [
        ['c', {a: '2'}],
        ['d', {a: '3'}],
        ['e', {a: '4'}],
        ['f', {b: '5'}],
      ],
    ],
    'c/t/000000000000000000000000000002/d': [
      1,
      [
        ['b', 't/000000000000000000000000000000'],
        ['f', 't/000000000000000000000000000001'],
      ],
    ],
    'c/t/000000000000000000000000000002/m': [
      't/000000000000000000000000000000',
      't/000000000000000000000000000001',
    ],
    'c/t/000000000000000000000000000003/d': [
      0,
      [
        ['\u00000\u0000a', {a: '0'}],
        ['\u00001\u0000b', {a: '1'}],
      ],
    ],
    'c/t/000000000000000000000000000004/d': [
      0,
      [
        ['\u00002\u0000c', {a: '2'}],
        ['\u00003\u0000d', {a: '3'}],
        ['\u00004\u0000e', {a: '4'}],
      ],
    ],
    'c/t/000000000000000000000000000005/d': [
      1,
      [
        ['\u00001\u0000b', 't/000000000000000000000000000003'],
        ['\u00004\u0000e', 't/000000000000000000000000000004'],
      ],
    ],
    'c/t/000000000000000000000000000005/m': [
      't/000000000000000000000000000003',
      't/000000000000000000000000000004',
    ],
    'c/t/000000000000000000000000000006/d': {
      meta: {type: 3, basisHash: null, lastMutationID: 0, cookieJSON: null},
      valueHash: 't/000000000000000000000000000002',
      indexes: [
        {
          definition: {jsonPointer: '/a', keyPrefix: '', name: 'idx'},
          valueHash: 't/000000000000000000000000000005',
        },
      ],
    },
    'c/t/000000000000000000000000000006/m': [
      't/000000000000000000000000000002',
      't/000000000000000000000000000005',
    ],
    'h/test': 't/000000000000000000000000000006',
    'c/t/000000000000000000000000000000/r': 1,
    'c/t/000000000000000000000000000001/r': 1,
    'c/t/000000000000000000000000000003/r': 1,
    'c/t/000000000000000000000000000004/r': 1,
    'c/t/000000000000000000000000000002/r': 1,
    'c/t/000000000000000000000000000005/r': 1,
    'c/t/000000000000000000000000000006/r': 1,
  });

  const mappings = new Map([
    [headHash, fakeHash('head')],
    [valueHash, fakeHash('value')],
    [indexHash, fakeHash('indecs')],
    [parseHash('t/000000000000000000000000000000'), fakeHash('data0')],
    [parseHash('t/000000000000000000000000000001'), fakeHash('data1')],
    [parseHash('t/000000000000000000000000000003'), fakeHash('data3')],
    [parseHash('t/000000000000000000000000000004'), fakeHash('data4')],
  ]);

  const newHeadHash = await memdag.withWrite(async dagWrite => {
    const transformer = new FixupTransformer(dagWrite, mappings);
    const newHeadHash = await transformer.transformCommit(headHash);

    await dagWrite.setHead('test', newHeadHash);
    await dagWrite.commit();
    return newHeadHash;
  });

  assert.equal(newHeadHash, fakeHash('head'));

  assert.deepEqual(memdag.kvStore.snapshot(), {
    'c/fake000000000000000000000000head/d': {
      indexes: [
        {
          definition: {
            jsonPointer: '/a',
            keyPrefix: '',
            name: 'idx',
          },
          valueHash: 'fake0000000000000000000000indecs',
        },
      ],
      meta: {
        basisHash: null,
        cookieJSON: null,
        lastMutationID: 0,
        type: 3,
      },
      valueHash: 'fake00000000000000000000000value',
    },
    'c/fake000000000000000000000000head/m': [
      'fake00000000000000000000000value',
      'fake0000000000000000000000indecs',
    ],
    'c/fake000000000000000000000000head/r': 1,
    'c/fake00000000000000000000000data0/d': [
      0,
      [
        [
          'a',
          {
            a: '0',
          },
        ],
        [
          'b',
          {
            a: '1',
          },
        ],
      ],
    ],
    'c/fake00000000000000000000000data0/r': 1,
    'c/fake00000000000000000000000data1/d': [
      0,
      [
        [
          'c',
          {
            a: '2',
          },
        ],
        [
          'd',
          {
            a: '3',
          },
        ],
        [
          'e',
          {
            a: '4',
          },
        ],
        [
          'f',
          {
            b: '5',
          },
        ],
      ],
    ],
    'c/fake00000000000000000000000data1/r': 1,
    'c/fake00000000000000000000000data3/d': [
      0,
      [
        [
          '\u00000\u0000a',
          {
            a: '0',
          },
        ],
        [
          '\u00001\u0000b',
          {
            a: '1',
          },
        ],
      ],
    ],
    'c/fake00000000000000000000000data3/r': 1,
    'c/fake00000000000000000000000data4/d': [
      0,
      [
        [
          '\u00002\u0000c',
          {
            a: '2',
          },
        ],
        [
          '\u00003\u0000d',
          {
            a: '3',
          },
        ],
        [
          '\u00004\u0000e',
          {
            a: '4',
          },
        ],
      ],
    ],
    'c/fake00000000000000000000000data4/r': 1,
    'c/fake0000000000000000000000indecs/d': [
      1,
      [
        ['\u00001\u0000b', 'fake00000000000000000000000data3'],
        ['\u00004\u0000e', 'fake00000000000000000000000data4'],
      ],
    ],
    'c/fake0000000000000000000000indecs/m': [
      'fake00000000000000000000000data3',
      'fake00000000000000000000000data4',
    ],
    'c/fake0000000000000000000000indecs/r': 1,
    'c/fake00000000000000000000000value/d': [
      1,
      [
        ['b', 'fake00000000000000000000000data0'],
        ['f', 'fake00000000000000000000000data1'],
      ],
    ],
    'c/fake00000000000000000000000value/m': [
      'fake00000000000000000000000data0',
      'fake00000000000000000000000data1',
    ],
    'c/fake00000000000000000000000value/r': 1,
    'h/test': 'fake000000000000000000000000head',
  });
});
