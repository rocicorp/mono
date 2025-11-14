import {describe, expect, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import {Chunk, type Refs, toRefs} from '../dag/chunk.ts';
import {TestStore} from '../dag/test-store.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {deepFreeze} from '../frozen-json.ts';
import {type Hash, fakeHash, makeNewFakeHashFunction} from '../hash.ts';
import {withRead} from '../with-transactions.ts';
import type {Commit} from './commit.ts';
import {
  type ChunkIndexDefinition,
  type CommitData,
  type Meta,
  baseSnapshotFromHash,
  chunkIndexDefinitionEqualIgnoreName,
  commitChain,
  newLocalDD31 as commitNewLocalDD31,
  newSnapshotDD31 as commitNewSnapshotDD31,
  fromChunk,
  getMutationID,
  localMutations,
  localMutationsGreaterThan,
  makeCommitData,
} from './commit.ts';
import * as MetaType from './meta-type-enum.ts';
import {ChainBuilder} from './test-helpers.ts';

type FormatVersion = Enum<typeof FormatVersion>;

describe('base snapshot', () => {
  const t = async (formatVersion: FormatVersion) => {
    const clientID = 'client-id';
    const store = new TestStore();
    const b = new ChainBuilder(store, undefined, formatVersion);
    await b.addGenesis(clientID);
    let genesisHash = b.chain[0].chunk.hash;
    await withRead(store, async dagRead => {
      expect(
        (await baseSnapshotFromHash(genesisHash, dagRead)).chunk.hash,
      ).toBe(genesisHash);
    });

    await b.addLocal(clientID);
    assert(formatVersion >= FormatVersion.DD31);
    await b.addLocal(clientID);
    genesisHash = b.chain[0].chunk.hash;
    await withRead(store, async dagRead => {
      expect(
        (
          await baseSnapshotFromHash(
            b.chain[b.chain.length - 1].chunk.hash,
            dagRead,
          )
        ).chunk.hash,
      ).toBe(genesisHash);
    });

    await b.addSnapshot(undefined, clientID);
    const baseHash = await withRead(store, async dagRead => {
      const baseHash = await dagRead.getHead('main');
      expect(
        (
          await baseSnapshotFromHash(
            b.chain[b.chain.length - 1].chunk.hash,
            dagRead,
          )
        ).chunk.hash,
      ).toBe(baseHash);
      return baseHash;
    });

    await b.addLocal(clientID);
    await b.addLocal(clientID);
    await withRead(store, async dagRead => {
      expect(
        (
          await baseSnapshotFromHash(
            b.chain[b.chain.length - 1].chunk.hash,
            dagRead,
          )
        ).chunk.hash,
      ).toBe(baseHash);
    });
  };

  test('DD31', () => t(FormatVersion.Latest));
});

describe('local mutations', () => {
  const t = async (formatVersion: FormatVersion) => {
    const clientID = 'client-id';
    const store = new TestStore();
    const b = new ChainBuilder(store, undefined, formatVersion);
    await b.addGenesis(clientID);
    const genesisHash = b.chain[0].chunk.hash;
    await withRead(store, async dagRead => {
      expect(await localMutations(genesisHash, dagRead)).toHaveLength(0);
    });

    await b.addLocal(clientID);
    assert(formatVersion >= FormatVersion.DD31);

    await b.addLocal(clientID);

    const headHash = b.chain[b.chain.length - 1].chunk.hash;
    const commits = await withRead(store, dagRead =>
      localMutations(headHash, dagRead),
    );
    expect(commits).toEqual([
      b.chain[formatVersion >= FormatVersion.DD31 ? 2 : 3],
      b.chain[1],
    ]);
  };

  test('DD31', () => t(FormatVersion.Latest));
});
test('local mutations greater than', async () => {
  const clientID1 = 'client-id-1';
  const clientID2 = 'client-id-2';
  const store = new TestStore();
  const b = new ChainBuilder(store);
  await b.addGenesis(clientID1);
  const genesisCommit = b.chain[0];
  await withRead(store, async dagRead => {
    expect(
      await localMutationsGreaterThan(
        genesisCommit,
        {[clientID1]: 0, [clientID2]: 0},
        dagRead,
      ),
    ).toHaveLength(0);
  });
  await b.addLocal(clientID1);
  await b.addLocal(clientID2);
  await b.addLocal(clientID2);
  await b.addLocal(clientID1);
  await b.addLocal(clientID1);
  const headCommit = b.chain[b.chain.length - 1];

  expect(
    await withRead(store, dagRead =>
      localMutationsGreaterThan(headCommit, {}, dagRead),
    ),
  ).toEqual([]);

  expect(
    await withRead(store, dagRead =>
      localMutationsGreaterThan(
        headCommit,
        {[clientID1]: 0, [clientID2]: 0},
        dagRead,
      ),
    ),
  ).toEqual([b.chain[5], b.chain[4], b.chain[3], b.chain[2], b.chain[1]]);

  expect(
    await withRead(store, dagRead =>
      localMutationsGreaterThan(
        headCommit,
        {[clientID1]: 1, [clientID2]: 1},
        dagRead,
      ),
    ),
  ).toEqual([b.chain[5], b.chain[4], b.chain[3]]);

  expect(
    await withRead(store, dagRead =>
      localMutationsGreaterThan(
        headCommit,
        {[clientID1]: 2, [clientID2]: 1},
        dagRead,
      ),
    ),
  ).toEqual([b.chain[5], b.chain[3]]);

  expect(
    await withRead(store, dagRead =>
      localMutationsGreaterThan(headCommit, {[clientID2]: 1}, dagRead),
    ),
  ).toEqual([b.chain[3]]);

  expect(
    await withRead(store, dagRead =>
      localMutationsGreaterThan(
        headCommit,
        {[clientID1]: 3, [clientID2]: 2},
        dagRead,
      ),
    ),
  ).toEqual([]);
});

describe('chain', () => {
  const t = async (formatVersion: FormatVersion) => {
    const clientID = 'client-id';
    const store = new TestStore();
    const b = new ChainBuilder(store, undefined, formatVersion);
    await b.addGenesis(clientID);

    let got: Commit<Meta>[] = await withRead(store, dagRead =>
      commitChain(b.chain[b.chain.length - 1].chunk.hash, dagRead),
    );

    expect(got).toHaveLength(1);
    expect(got[0]).toEqual(b.chain[0]);

    await b.addSnapshot(undefined, clientID);
    await b.addLocal(clientID);
    assert(formatVersion >= FormatVersion.DD31);
    await b.addLocal(clientID);

    const headHash = b.chain[b.chain.length - 1].chunk.hash;
    got = await withRead(store, dagRead => commitChain(headHash, dagRead));
    expect(got).toHaveLength(3);
    expect(got[0]).toEqual(b.chain[3]);
    expect(got[1]).toEqual(b.chain[2]);
    expect(got[2]).toEqual(b.chain[1]);
  };

  test('dd31', () => t(FormatVersion.Latest));
});

test('load roundtrip', () => {
  const clientID = 'client-id';
  const t = (chunk: Chunk, expected: Commit<Meta> | Error) => {
    {
      if (expected instanceof Error) {
        expect(() => fromChunk(chunk), expected.message).toThrow(expected);
      } else {
        const actual = fromChunk(chunk);
        expect(actual).toEqual(expected);
      }
    }
  };
  const original = fakeHash('face1');
  const valueHash = fakeHash('face2');
  const emptyStringHash = fakeHash('000');
  const hashHash = fakeHash('face3');
  const baseSnapshotHash = fakeHash('face4');
  const timestamp = 42;

  for (const basisHash of [emptyStringHash, hashHash]) {
    t(
      makeCommit(
        {
          type: MetaType.LocalDD31,
          basisHash,
          baseSnapshotHash,
          mutationID: 0,
          mutatorName: 'mutator-name',
          mutatorArgsJSON: 42,
          originalHash: original,
          timestamp,
          clientID,
        },
        valueHash,
        basisHash === null ? [valueHash] : [valueHash, basisHash],
      ),
      commitNewLocalDD31(
        createChunk,
        basisHash,
        baseSnapshotHash,
        0,
        'mutator-name',
        42,
        original,
        valueHash,
        [],
        timestamp,
        clientID,
      ),
    );
  }

  t(
    makeCommit(
      {
        type: MetaType.LocalDD31,
        basisHash: fakeHash('ba515'),
        baseSnapshotHash: fakeHash('ba516'),
        mutationID: 0,
        mutatorName: '',
        mutatorArgsJSON: 43,
        originalHash: emptyStringHash,
        timestamp,
        clientID,
      },
      fakeHash('face4'),
      [fakeHash('001'), fakeHash('002')],
    ),
    new Error('Missing mutator name'),
  );

  t(
    makeCommit(
      {
        type: MetaType.LocalDD31,
        basisHash: emptyStringHash,
        mutationID: 0,
        // @ts-expect-error We are testing invalid types
        mutatorName: null,
        mutatorArgsJSON: 43,
        originalHash: emptyStringHash,
        clientID,
      },
      fakeHash('face4'),
      ['a', 'b'],
    ),
    new Error('Invalid type: null, expected string'),
  );

  for (const basisHash of [fakeHash('000'), fakeHash('face3')]) {
    t(
      makeCommit(
        {
          type: MetaType.LocalDD31,
          basisHash,
          baseSnapshotHash,
          mutationID: 0,
          mutatorName: 'mutator-name',
          mutatorArgsJSON: 44,
          originalHash: null,
          timestamp,
          clientID,
        },
        fakeHash('face6'),
        basisHash === null
          ? [fakeHash('face6')]
          : [fakeHash('face6'), basisHash],
      ),
      commitNewLocalDD31(
        createChunk,
        basisHash,
        baseSnapshotHash,
        0,
        'mutator-name',
        44,
        null,
        fakeHash('face6'),
        [],
        timestamp,
        clientID,
      ),
    );
  }

  t(
    makeCommit(
      {
        type: MetaType.LocalDD31,
        basisHash: emptyStringHash,
        baseSnapshotHash,
        mutationID: 0,
        mutatorName: 'mutator-name',
        mutatorArgsJSON: 45,
        originalHash: emptyStringHash,
        timestamp,
        clientID,
      },
      //@ts-expect-error we are testing invalid types
      null,
      ['a', 'b'],
    ),
    new Error('Invalid type: null, expected string'),
  );

  const cookie = deepFreeze({foo: 'bar', order: 1});
  for (const basisHash of [null, fakeHash('000'), fakeHash('face3')]) {
    t(
      makeCommit(
        {
          type: MetaType.SnapshotDD31,
          basisHash,
          lastMutationIDs: {[clientID]: 0},
          cookieJSON: cookie,
        },
        fakeHash('face6'),
        [fakeHash('face6')],
      ),
      commitNewSnapshotDD31(
        createChunk,
        basisHash,
        {[clientID]: 0},
        cookie,
        fakeHash('face6'),
        [],
      ),
    );
  }

  t(
    makeCommit(
      // @ts-expect-error we are testing invalid types
      {
        type: MetaType.SnapshotDD31,
        basisHash: emptyStringHash,
        lastMutationIDs: {[clientID]: 0},
        // missing cookieJSON
      },
      fakeHash('face6'),
      [fakeHash('face6'), fakeHash('000')],
    ),
    new Error('Invalid type: undefined, expected JSON value'),
  );
});

test('accessors', async () => {
  const clientID = 'client-id';

  const originalHash = fakeHash('face7');
  const basisHash = fakeHash('face8');
  const baseSnapshotHash = fakeHash('face9');
  const valueHash = fakeHash('face4');
  const timestamp = 42;
  const local = fromChunk(
    makeCommit(
      {
        type: MetaType.LocalDD31,
        basisHash,
        baseSnapshotHash,
        mutationID: 1,
        mutatorName: 'foo_mutator',
        mutatorArgsJSON: 42,
        originalHash,
        timestamp,
        clientID,
      },
      valueHash,
      [valueHash, basisHash],
    ),
  );
  const lm = local.meta;
  if (lm.type === MetaType.LocalDD31) {
    expect(lm.mutationID).toBe(1);
    expect(lm.mutatorName).toBe('foo_mutator');
    expect(lm.mutatorArgsJSON).toBe(42);
    expect(lm.originalHash).toBe(originalHash);
    expect(lm.timestamp).toBe(timestamp);
    expect(lm.clientID).toBe(clientID);
  } else {
    throw new Error('unexpected type');
  }
  expect(local.meta.basisHash).toBe(basisHash);
  expect(local.valueHash).toBe(valueHash);

  const fakeRead = {
    // oxlint-disable-next-line require-await
    async mustGetChunk() {
      // This test does not read from the dag and if it does, lets just fail.
      throw new Error('Method not implemented.');
    },
  };

  expect(await local.getNextMutationID(clientID, fakeRead)).toBe(2);

  const snapshot = fromChunk(
    makeCommit(
      {
        type: MetaType.SnapshotDD31,
        basisHash: fakeHash('face9'),
        lastMutationIDs: {[clientID]: 2},
        cookieJSON: 'cookie 2',
      },
      fakeHash('face10'),
      [fakeHash('face10'), fakeHash('face9')],
    ),
  );
  const sm = snapshot.meta;
  if (sm.type === MetaType.SnapshotDD31) {
    expect(sm.lastMutationIDs[clientID]).toBe(2);
  } else {
    throw new Error('unexpected type');
  }
  expect(sm.cookieJSON).toEqual('cookie 2');
  expect(sm.basisHash).toBe(fakeHash('face9'));
  expect(snapshot.valueHash).toBe(fakeHash('face10'));
  expect(await snapshot.getNextMutationID(clientID, fakeRead)).toBe(3);
});

const chunkHasher = makeNewFakeHashFunction('face55');

const hashMapper: Map<string, Hash> = new Map();

function createChunk<V>(data: V, refs: Refs): Chunk<V> {
  const s = JSON.stringify(data);
  let hash = hashMapper.get(s);
  if (!hash) {
    hash = chunkHasher();
    hashMapper.set(s, hash);
  }

  return new Chunk(hash, data, refs);
}

function makeCommit<M extends Meta>(
  meta: M,
  valueHash: Hash,
  refs: Hash[],
): Chunk<CommitData<M>> {
  const data: CommitData<M> = makeCommitData(meta, valueHash, []);
  return createChunk(data, toRefs(refs));
}

test('getMutationID across commits with different clients', async () => {
  // In DD31 the commits can be from different clients.

  const clientID = 'client-id';
  const clientID2 = 'client-id-2';
  const store = new TestStore();
  const b = new ChainBuilder(store);
  await b.addGenesis(clientID);
  await b.addLocal(clientID);
  await b.addLocal(clientID);
  await b.addLocal(clientID2);

  // oxlint-disable-next-line @typescript-eslint/no-non-null-assertion
  const local = b.chain.at(-1)!;
  await withRead(store, async dagRead => {
    expect(await local.getMutationID(clientID, dagRead)).toBe(2);
    expect(await local.getMutationID(clientID2, dagRead)).toBe(1);
  });

  await withRead(store, async dagRead => {
    expect(await getMutationID(clientID, dagRead, local.meta)).toBe(2);
    expect(await getMutationID(clientID2, dagRead, local.meta)).toBe(1);
  });
});

test('chunkIndexDefinitionEqualIgnoreName', () => {
  const t = (a: ChunkIndexDefinition, b = a) => {
    expect(chunkIndexDefinitionEqualIgnoreName(a, b)).toBe(true);
  };
  const f = (a: ChunkIndexDefinition, b = a) => {
    expect(chunkIndexDefinitionEqualIgnoreName(a, b)).toBe(false);
  };

  t({name: 'a', jsonPointer: '/a', keyPrefix: ''});
  t({name: 'a', jsonPointer: '/a', keyPrefix: 'x', allowEmpty: true});
  t({name: 'a', jsonPointer: '/a', keyPrefix: 'x', allowEmpty: false});

  t(
    {name: 'a', jsonPointer: '/a', keyPrefix: ''},
    {name: 'a', jsonPointer: '/a', keyPrefix: '', allowEmpty: false},
  );
  f(
    {name: 'a', jsonPointer: '/a', keyPrefix: ''},
    {name: 'a', jsonPointer: '/a', keyPrefix: '', allowEmpty: true},
  );

  t(
    {name: 'a', jsonPointer: '/a', keyPrefix: ''},
    {name: 'b', jsonPointer: '/a', keyPrefix: ''},
  );

  f(
    {name: 'a', jsonPointer: '/a', keyPrefix: ''},
    {name: 'a', jsonPointer: '/b', keyPrefix: ''},
  );

  f(
    {name: 'a', jsonPointer: '/a', keyPrefix: ''},
    {name: 'a', jsonPointer: '/a', keyPrefix: 'x'},
  );

  f(
    {name: 'a', jsonPointer: '/a', keyPrefix: '', allowEmpty: true},
    {name: 'a', jsonPointer: '/a', keyPrefix: '', allowEmpty: false},
  );

  f(
    {name: 'a', jsonPointer: '/a', keyPrefix: '', allowEmpty: true},
    {name: 'a', jsonPointer: '/a', keyPrefix: ''},
  );
});
