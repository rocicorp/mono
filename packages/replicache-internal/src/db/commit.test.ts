import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod';
import {
  Commit,
  CommitData,
  fromChunk,
  IndexChangeMeta,
  Meta,
  MetaType,
  newIndexChange as commitNewIndexChange,
  newLocal as commitNewLocal,
  newSnapshot as commitNewSnapshot,
  newSnapshotDD31 as commitNewSnapshotDD31,
  SnapshotMeta,
  chain as commitChain,
  localMutations,
  baseSnapshotFromHash,
  assertSnapshotMetaDD31,
  assertSnapshotMeta,
  SnapshotMetaDD31,
  localMutationsGreaterThan,
  chunkIndexDefinitionEqualIgnoreName,
  ChunkIndexDefinition,
} from './commit';
import {
  addGenesis,
  addIndexChange,
  addLocal,
  addSnapshot,
  Chain,
} from './test-helpers';
import {Hash, fakeHash, makeNewFakeHashFunction} from '../hash';
import {
  toInternalValue,
  InternalValue,
  ToInternalValueReason,
} from '../internal-value';
import type {ClientID} from '../sync/client-id';

test('base snapshot', async () => {
  const clientID = 'client-id';
  const store = new dag.TestStore();
  const chain: Chain = [];
  await addGenesis(chain, store, clientID);
  let genesisHash = chain[0].chunk.hash;
  await store.withRead(async dagRead => {
    expect(
      (await baseSnapshotFromHash(genesisHash, dagRead)).chunk.hash,
    ).to.equal(genesisHash);
  });

  await addLocal(chain, store, clientID);
  if (!DD31) {
    await addIndexChange(chain, store, clientID);
  }
  await addLocal(chain, store, clientID);
  genesisHash = chain[0].chunk.hash;
  await store.withRead(async dagRead => {
    expect(
      (await baseSnapshotFromHash(chain[chain.length - 1].chunk.hash, dagRead))
        .chunk.hash,
    ).to.equal(genesisHash);
  });

  await addSnapshot(chain, store, undefined, clientID);
  const baseHash = await store.withRead(async dagRead => {
    const baseHash = await dagRead.getHead('main');
    expect(
      (await baseSnapshotFromHash(chain[chain.length - 1].chunk.hash, dagRead))
        .chunk.hash,
    ).to.equal(baseHash);
    return baseHash;
  });

  await addLocal(chain, store, clientID);
  await addLocal(chain, store, clientID);
  await store.withRead(async dagRead => {
    expect(
      (await baseSnapshotFromHash(chain[chain.length - 1].chunk.hash, dagRead))
        .chunk.hash,
    ).to.equal(baseHash);
  });
});

test('local mutations', async () => {
  const clientID = 'client-id';
  const store = new dag.TestStore();
  const chain: Chain = [];
  await addGenesis(chain, store, clientID);
  const genesisHash = chain[0].chunk.hash;
  await store.withRead(async dagRead => {
    expect(await localMutations(genesisHash, dagRead)).to.have.lengthOf(0);
  });

  await addLocal(chain, store, clientID);
  if (!DD31) {
    await addIndexChange(chain, store, clientID);
  }
  await addLocal(chain, store, clientID);
  if (!DD31) {
    await addIndexChange(chain, store, clientID);
  }
  const headHash = chain[chain.length - 1].chunk.hash;
  const commits = await store.withRead(dagRead =>
    localMutations(headHash, dagRead),
  );
  expect(commits).to.deep.equal([chain[DD31 ? 2 : 3], chain[1]]);
});

test('local mutations greater than', async () => {
  if (!DD31) {
    return;
  }
  const clientID1 = 'client-id-1';
  const clientID2 = 'client-id-2';
  const store = new dag.TestStore();
  const chain: Chain = [];
  await addGenesis(chain, store, clientID1);
  const genesisCommit = chain[0];
  await store.withRead(async dagRead => {
    expect(
      await localMutationsGreaterThan(
        genesisCommit,
        {[clientID1]: 0, [clientID2]: 0},
        dagRead,
      ),
    ).to.have.lengthOf(0);
  });
  await addLocal(chain, store, clientID1);
  await addLocal(chain, store, clientID2);
  await addLocal(chain, store, clientID2);
  await addLocal(chain, store, clientID1);
  await addLocal(chain, store, clientID1);
  const headCommit = chain[chain.length - 1];

  expect(
    await store.withRead(async dagRead => {
      return await localMutationsGreaterThan(headCommit, {}, dagRead);
    }),
  ).to.deep.equal([]);

  expect(
    await store.withRead(async dagRead => {
      return await localMutationsGreaterThan(
        headCommit,
        {[clientID1]: 0, [clientID2]: 0},
        dagRead,
      );
    }),
  ).to.deep.equal([chain[5], chain[4], chain[3], chain[2], chain[1]]);

  expect(
    await store.withRead(async dagRead => {
      return await localMutationsGreaterThan(
        headCommit,
        {[clientID1]: 1, [clientID2]: 1},
        dagRead,
      );
    }),
  ).to.deep.equal([chain[5], chain[4], chain[3]]);

  expect(
    await store.withRead(async dagRead => {
      return await localMutationsGreaterThan(
        headCommit,
        {[clientID1]: 2, [clientID2]: 1},
        dagRead,
      );
    }),
  ).to.deep.equal([chain[5], chain[3]]);

  expect(
    await store.withRead(async dagRead => {
      return await localMutationsGreaterThan(
        headCommit,
        {[clientID2]: 1},
        dagRead,
      );
    }),
  ).to.deep.equal([chain[3]]);

  expect(
    await store.withRead(async dagRead => {
      return await localMutationsGreaterThan(
        headCommit,
        {[clientID1]: 3, [clientID2]: 2},
        dagRead,
      );
    }),
  ).to.deep.equal([]);
});

test('chain', async () => {
  const clientID = 'client-id';
  const store = new dag.TestStore();
  const chain: Chain = [];
  await addGenesis(chain, store, clientID);

  let got = await store.withRead(dagRead =>
    commitChain(chain[chain.length - 1].chunk.hash, dagRead),
  );

  expect(got).to.have.lengthOf(1);
  expect(got[0]).to.deep.equal(chain[0]);

  await addSnapshot(chain, store, undefined, clientID);
  await addLocal(chain, store, clientID);
  if (!DD31) {
    await addIndexChange(chain, store, clientID);
  } else {
    await addLocal(chain, store, clientID);
  }
  const headHash = chain[chain.length - 1].chunk.hash;
  got = await store.withRead(dagRead => commitChain(headHash, dagRead));
  expect(got).to.have.lengthOf(3);
  expect(got[0]).to.deep.equal(chain[3]);
  expect(got[1]).to.deep.equal(chain[2]);
  expect(got[2]).to.deep.equal(chain[1]);
});

test('load roundtrip', async () => {
  const clientID = 'client-id';
  const t = (chunk: dag.Chunk<unknown>, expected: Commit<Meta> | Error) => {
    {
      if (expected instanceof Error) {
        expect(() => fromChunk(chunk)).to.throw(
          expected.constructor,
          expected.message,
        );
      } else {
        const actual = fromChunk(chunk);
        expect(actual).to.deep.equal(expected);
      }
    }
  };
  const original = fakeHash('face1');
  const valueHash = fakeHash('face2');
  const emptyStringHash = fakeHash('000');
  const hashHash = fakeHash('face3');
  const timestamp = 42;

  for (const basisHash of [emptyStringHash, hashHash]) {
    t(
      await makeCommit(
        {
          type: MetaType.Local,
          basisHash,
          mutationID: 0,
          mutatorName: 'mutname',
          mutatorArgsJSON: 42,
          originalHash: original,
          timestamp,
        },
        valueHash,
        basisHash === null ? [valueHash] : [valueHash, basisHash],
        clientID,
      ),
      commitNewLocal(
        createChunk,
        basisHash,
        0,
        'mutname',
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
    await makeCommit(
      {
        type: MetaType.Local,
        basisHash: fakeHash('ba515'),
        mutationID: 0,
        mutatorName: '',
        mutatorArgsJSON: 43,
        originalHash: emptyStringHash,
        timestamp,
      },
      fakeHash('face4'),
      [fakeHash('000'), fakeHash('000')],
      clientID,
    ),
    new Error('Missing mutator name'),
  );
  t(
    await makeCommit(
      {
        type: MetaType.Local,
        basisHash: emptyStringHash,
        mutationID: 0,
        // @ts-expect-error We are testing invalid types
        mutatorName: undefined,
        mutatorArgsJSON: 43,
        originalHash: emptyStringHash,
      },
      fakeHash('face4'),
      ['', ''],
      clientID,
    ),
    new Error('Invalid type: undefined, expected string'),
  );

  for (const basisHash of [fakeHash('000'), fakeHash('face3')]) {
    t(
      await makeCommit(
        {
          type: MetaType.Local,
          basisHash,
          mutationID: 0,
          mutatorName: 'mutname',
          mutatorArgsJSON: 44,
          originalHash: null,
          timestamp,
        },
        fakeHash('face6'),
        basisHash === null
          ? [fakeHash('face6')]
          : [fakeHash('face6'), basisHash],
        clientID,
      ),
      commitNewLocal(
        createChunk,
        basisHash,
        0,
        'mutname',
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
    await makeCommit(
      {
        type: MetaType.Local,
        basisHash: emptyStringHash,
        mutationID: 0,
        mutatorName: 'mutname',
        mutatorArgsJSON: 45,
        originalHash: emptyStringHash,
        timestamp,
      },
      //@ts-expect-error we are testing invalid types
      undefined,
      ['', ''],
      clientID,
    ),
    new Error('Invalid type: undefined, expected string'),
  );

  const cookie = toInternalValue({foo: 'bar'}, ToInternalValueReason.Test);
  for (const basisHash of [null, fakeHash('000'), fakeHash('face3')]) {
    t(
      await makeCommit(
        makeSnapshotMeta(
          basisHash ?? null,
          0,
          toInternalValue({foo: 'bar'}, ToInternalValueReason.Test),
          clientID,
        ),
        fakeHash('face6'),
        [fakeHash('face6')],
        clientID,
      ),
      DD31
        ? commitNewSnapshotDD31(
            createChunk,
            basisHash,
            {[clientID]: 0},
            cookie,
            fakeHash('face6'),
            [],
          )
        : commitNewSnapshot(
            createChunk,
            basisHash,
            0,
            cookie,
            fakeHash('face6'),
            [],
          ),
    );
  }
  t(
    await makeCommit(
      makeSnapshotMeta(
        emptyStringHash,
        0,
        // @ts-expect-error we are testing invalid types
        undefined,
        clientID,
      ),
      fakeHash('face6'),
      [fakeHash('face6'), fakeHash('000')],
      clientID,
    ),
    new Error('Invalid type: undefined, expected JSON value'),
  );

  for (const basisHash of [fakeHash('000'), fakeHash('face3')]) {
    t(
      await makeCommit(
        makeIndexChangeMeta(basisHash, 0),
        fakeHash('face2'),
        basisHash === null
          ? [fakeHash('face2')]
          : [fakeHash('face2'), basisHash],
        clientID,
      ),
      commitNewIndexChange(createChunk, basisHash, 0, fakeHash('face2'), []),
    );
  }
});

test('accessors', async () => {
  const clientID = 'client-id';

  const originalHash = fakeHash('face7');
  const basisHash = fakeHash('face8');
  const valueHash = fakeHash('face4');
  const timestamp = 42;
  const local = fromChunk(
    await makeCommit(
      {
        type: MetaType.Local,
        basisHash,
        mutationID: 1,
        mutatorName: 'foo_mutator',
        mutatorArgsJSON: 42,
        originalHash,
        timestamp,
      },
      valueHash,
      [valueHash, basisHash],
      clientID,
    ),
  );
  const lm = local.meta;
  if (lm.type === MetaType.Local) {
    expect(lm.mutationID).to.equal(1);
    expect(lm.mutatorName).to.equal('foo_mutator');
    expect(lm.mutatorArgsJSON).to.equal(42);
    expect(lm.originalHash).to.equal(originalHash);
    expect(lm.timestamp).equal(timestamp);
  } else {
    throw new Error('unexpected type');
  }
  expect(local.meta.basisHash).to.equal(basisHash);
  expect(local.valueHash).to.equal(valueHash);

  const fakeRead = {
    async mustGetChunk() {
      // This test does not read from the dag and if it does, lets just fail.
      throw new Error('Method not implemented.');
    },
  };

  expect(await local.getNextMutationID(clientID, fakeRead)).to.equal(2);

  const snapshot = fromChunk(
    await makeCommit(
      makeSnapshotMeta(fakeHash('face9'), 2, 'cookie 2', clientID),
      fakeHash('face10'),
      [fakeHash('face10'), fakeHash('face9')],
      clientID,
    ),
  );
  const sm = snapshot.meta;
  if (sm.type === MetaType.Snapshot) {
    if (DD31) {
      assertSnapshotMetaDD31(sm);
      expect(sm.lastMutationIDs[clientID]).to.equal(2);
    } else {
      assertSnapshotMeta(sm);
      expect(sm.lastMutationID).to.equal(2);
    }
    expect(sm.cookieJSON).to.deep.equal('cookie 2');
    expect(sm.cookieJSON).to.deep.equal('cookie 2');
  } else {
    throw new Error('unexpected type');
  }
  expect(snapshot.meta.basisHash).to.equal(fakeHash('face9'));
  expect(snapshot.valueHash).to.equal(fakeHash('face10'));
  expect(await snapshot.getNextMutationID(clientID, fakeRead)).to.equal(3);

  const indexChange = fromChunk(
    await makeCommit(
      makeIndexChangeMeta(fakeHash('face11'), 3),
      fakeHash('face12'),
      [fakeHash('face12'), fakeHash('face11')],
      clientID,
    ),
  );
  const ic = indexChange.meta;
  if (ic.type === MetaType.IndexChange) {
    expect(ic.lastMutationID).to.equal(3);
  } else {
    throw new Error('unexpected type');
  }
  expect(indexChange.meta.basisHash).to.equal(fakeHash('face11'));
  expect(indexChange.valueHash).to.equal(fakeHash('face12'));
  if (!DD31) {
    // In DD31 IndexChange commits do not have mutationID(s).
    // See: 'getMutationID with IndexChange commits' test below
    expect(await indexChange.getMutationID(clientID, fakeRead)).to.equal(3);
  }
});

const chunkHasher = makeNewFakeHashFunction('face55');

const hashMapper: Map<string, Hash> = new Map();

function createChunk<V>(data: V, refs: readonly Hash[]): dag.Chunk<V> {
  const s = JSON.stringify(data);
  let hash = hashMapper.get(s);
  if (!hash) {
    hash = chunkHasher();
    hashMapper.set(s, hash);
  }

  return dag.createChunkWithHash(hash, data, refs);
}

async function makeCommit<M extends Meta>(
  meta: M,
  valueHash: Hash,
  refs: Hash[],
  clientID: ClientID,
): Promise<dag.Chunk<CommitData<M>>> {
  if (DD31) {
    if (meta.type === MetaType.Local) {
      meta = {...meta, clientID};
    }
  }
  const data: CommitData<M> = {
    meta,
    valueHash,
    indexes: [],
  };
  return createChunk(data, refs);
}

function makeSnapshotMeta(
  basisHash: Hash | null,
  lastMutationID: number,
  cookieJSON: InternalValue,
  clientID: ClientID,
): SnapshotMeta | SnapshotMetaDD31 {
  if (DD31) {
    return {
      type: MetaType.Snapshot,
      basisHash,
      lastMutationIDs: {[clientID]: lastMutationID},
      cookieJSON,
    };
  }
  return {
    type: MetaType.Snapshot,
    basisHash,
    lastMutationID,
    cookieJSON,
  };
}

function makeIndexChangeMeta(
  basisHash: Hash,
  lastMutationID: number,
): IndexChangeMeta {
  return {
    type: MetaType.IndexChange,
    basisHash,
    lastMutationID,
  };
}

test('getMutationID across commits with different clients', async () => {
  // In DD31 the commits can be from different clients.
  if (!DD31) {
    return;
  }

  const clientID = 'client-id';
  const clientID2 = 'client-id-2';
  const store = new dag.TestStore();
  const chain: Chain = [];
  await addGenesis(chain, store, clientID);
  await addLocal(chain, store, clientID);
  await addLocal(chain, store, clientID);
  await addLocal(chain, store, clientID2);

  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  const local = chain.at(-1)!;
  await store.withRead(async dagRead => {
    expect(await local.getMutationID(clientID, dagRead)).to.equal(2);
    expect(await local.getMutationID(clientID2, dagRead)).to.equal(1);
  });
});

test('chunkIndexDefinitionEqualIgnoreName', () => {
  const t = (a: ChunkIndexDefinition, b = a) => {
    expect(chunkIndexDefinitionEqualIgnoreName(a, b)).true;
  };
  const f = (a: ChunkIndexDefinition, b = a) => {
    expect(chunkIndexDefinitionEqualIgnoreName(a, b)).false;
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
