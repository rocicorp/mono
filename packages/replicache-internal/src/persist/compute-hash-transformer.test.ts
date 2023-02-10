import {assert} from '@esm-bundle/chai';
import * as dag from '../dag/mod';
import * as db from '../db/mod';
import * as utf8 from '../utf8';
import {
  BYTE_LENGTH,
  Hash,
  makeNewFakeHashFunction,
  parse as parseHash,
} from '../hash';
import {BTreeWrite} from '../btree/write';
import {ComputeHashTransformer} from './compute-hash-transformer';
import {encode} from '../base32-encode';
import type {ReadonlyJSONValue} from '../json';

test('fix hashes up of a single snapshot commit with empty btree', async () => {
  const clientID = 'client-id';
  const memdag = new dag.TestStore(
    undefined,
    makeNewFakeHashFunction('t/aaa'),
    () => undefined,
  );

  const [headChunk, treeChunk] = await memdag.withWrite(async dagWrite => {
    const tree = new BTreeWrite(dagWrite);
    const valueHash = await tree.flush();
    const c = DD31
      ? db.newSnapshotDD31(
          dagWrite.createChunk,
          null,
          {[clientID]: 0},
          null,
          valueHash,
          [],
        )
      : db.newSnapshot(dagWrite.createChunk, null, 0, null, valueHash, []);
    await dagWrite.putChunk(c.chunk);
    await dagWrite.setHead('test', c.chunk.hash);
    await dagWrite.commit();
    return [c.chunk, await dagWrite.getChunk(valueHash)];
  });

  const snapshot = memdag.kvStore.snapshot();
  assert.deepEqual(snapshot, {
    'c/t/aaa000000000000000000000000000/d': [0, []],
    'c/t/aaa000000000000000000000000001/d': {
      meta: makeSnapshotMetaForTesting(clientID),
      valueHash: 't/aaa000000000000000000000000000',
      indexes: [],
    },
    'c/t/aaa000000000000000000000000001/m': [
      't/aaa000000000000000000000000000',
    ],
    'h/test': 't/aaa000000000000000000000000001',
    'c/t/aaa000000000000000000000000000/r': 1,
    'c/t/aaa000000000000000000000000001/r': 1,
  });

  if (!treeChunk) {
    assert.fail();
  }

  const gatheredChunk = new Map([
    [headChunk.hash, headChunk],
    [treeChunk.hash, treeChunk],
  ]);

  const hashFunc = async (v: ReadonlyJSONValue): Promise<Hash> => {
    const buf = await crypto.subtle.digest(
      'SHA-512',
      utf8.encode(JSON.stringify(v)),
    );
    const buf2 = new Uint8Array(buf, 0, BYTE_LENGTH);
    return encode(buf2) as unknown as Hash;
  };

  const transformer = new ComputeHashTransformer(gatheredChunk, hashFunc);
  const newHeadHash = await transformer.transformCommit(headChunk.hash);
  assert.equal(
    newHeadHash,
    parseHash(
      DD31
        ? '0tb6u6bkfvivbu1f0sjkl58kjmj13ac2'
        : '9lrb08p9b7jqo8oad3aef60muj4td8ke',
    ),
  );

  assert.deepEqual(Object.fromEntries(transformer.fixedChunks), {
    mdcncodijhl6jk2o8bb7m0hg15p3sf24: {
      data: [0, []],
      hash: 'mdcncodijhl6jk2o8bb7m0hg15p3sf24',
      meta: [],
    },
    [DD31
      ? '0tb6u6bkfvivbu1f0sjkl58kjmj13ac2'
      : '9lrb08p9b7jqo8oad3aef60muj4td8ke']: {
      data: {
        indexes: [],
        meta: makeSnapshotMetaForTesting(clientID),
        valueHash: 'mdcncodijhl6jk2o8bb7m0hg15p3sf24',
      },
      hash: DD31
        ? '0tb6u6bkfvivbu1f0sjkl58kjmj13ac2'
        : '9lrb08p9b7jqo8oad3aef60muj4td8ke',
      meta: ['mdcncodijhl6jk2o8bb7m0hg15p3sf24'],
    },
  });

  {
    // And again but only with the snapshot commit
    memdag.kvStore.restoreSnapshot(snapshot);

    const gatheredChunk = new Map([[headChunk.hash, headChunk]]);

    const transformer = new ComputeHashTransformer(gatheredChunk, hashFunc);
    const newHeadHash = await transformer.transformCommit(headChunk.hash);
    assert.equal(
      newHeadHash,
      parseHash(
        DD31
          ? 'cm6vbtkg2m7cuhkadd3pkoskes2pl9ea'
          : '1fgr18o8m8p503lt3e9oaoct8k9ch47b',
      ),
    );

    assert.deepEqual(Object.fromEntries(transformer.fixedChunks), {
      [DD31
        ? 'cm6vbtkg2m7cuhkadd3pkoskes2pl9ea'
        : '1fgr18o8m8p503lt3e9oaoct8k9ch47b']: {
        data: {
          indexes: [],
          meta: makeSnapshotMetaForTesting(clientID),
          valueHash: 't/aaa000000000000000000000000000',
        },
        hash: DD31
          ? 'cm6vbtkg2m7cuhkadd3pkoskes2pl9ea'
          : '1fgr18o8m8p503lt3e9oaoct8k9ch47b',
        meta: ['t/aaa000000000000000000000000000'],
      },
    });
  }
});

function makeSnapshotMetaForTesting(
  clientID: string,
): ReadonlyJSONValue | undefined {
  return DD31
    ? {
        basisHash: null,
        cookieJSON: null,
        lastMutationIDs: {[clientID]: 0},
        type: 3,
      }
    : {
        basisHash: null,
        cookieJSON: null,
        lastMutationID: 0,
        type: 3,
      };
}
