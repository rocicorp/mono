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
  baseSnapshot,
  assertSnapshotMetaDD31,
  assertSnapshotMeta,
  SnapshotMetaDD31,
  localMutationsGreaterThan,
} from './commit';
import {
  addGenesis,
  addIndexChange,
  addLocal,
  addSnapshot,
  Chain,
} from './test-helpers';
import {Hash, fakeHash} from '../hash';
import {makeTestChunkHasher} from '../dag/chunk';
import {
  toInternalValue,
  InternalValue,
  ToInternalValueReason,
} from '../internal-value.js';
import type {ClientID} from '../sync/client-id.js';

test('base snapshot', async () => {
  const clientID = 'client-id';
  const store = new dag.TestStore();
  const chain: Chain = [];
  await addGenesis(chain, store, clientID);
  let genesisHash = chain[0].chunk.hash;
  await store.withRead(async dagRead => {
    expect((await baseSnapshot(genesisHash, dagRead)).chunk.hash).to.equal(
      genesisHash,
    );
  });

  await addLocal(chain, store, clientID);
  await addIndexChange(chain, store, clientID);
  await addLocal(chain, store, clientID);
  genesisHash = chain[0].chunk.hash;
  await store.withRead(async dagRead => {
    expect(
      (await baseSnapshot(chain[chain.length - 1].chunk.hash, dagRead)).chunk
        .hash,
    ).to.equal(genesisHash);
  });

  await addSnapshot(chain, store, undefined, clientID);
  const baseHash = await store.withRead(async dagRead => {
    const baseHash = await dagRead.getHead('main');
    expect(
      (await baseSnapshot(chain[chain.length - 1].chunk.hash, dagRead)).chunk
        .hash,
    ).to.equal(baseHash);
    return baseHash;
  });

  await addLocal(chain, store, clientID);
  await addLocal(chain, store, clientID);
  await store.withRead(async dagRead => {
    expect(
      (await baseSnapshot(chain[chain.length - 1].chunk.hash, dagRead)).chunk
        .hash,
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
  await addIndexChange(chain, store, clientID);
  await addLocal(chain, store, clientID);
  await addIndexChange(chain, store, clientID);
  const headHash = chain[chain.length - 1].chunk.hash;
  const commits = await store.withRead(dagRead =>
    localMutations(headHash, dagRead),
  );
  expect(commits).to.have.lengthOf(2);
  expect(commits[0]).to.deep.equal(chain[3]);
  expect(commits[1]).to.deep.equal(chain[1]);
});

test.only('local mutations greater than', async () => {
  if (DD31) {
    const clientID1 = 'client-id-1';
    const clientID2 = 'client-id-2';
    const store = new dag.TestStore();
    const chain: Chain = [];
    await addGenesis(chain, store, clientID1);
    const genesisHash = chain[0].chunk.hash;
    await store.withRead(async dagRead => {
      expect(
        await localMutationsGreaterThan(
          genesisHash,
          {[clientID1]: 0, [clientID2]: 0},
          dagRead,
        ),
      ).to.have.lengthOf(0);
    });
    await addLocal(chain, store, clientID1);
    await addIndexChange(chain, store, clientID1);
    await addLocal(chain, store, clientID2);
    await addIndexChange(chain, store, clientID2);
    await addLocal(chain, store, clientID2);
    await addIndexChange(chain, store, clientID2);
    await addLocal(chain, store, clientID1);
    await addIndexChange(chain, store, clientID1);
    await addLocal(chain, store, clientID1);
    await addIndexChange(chain, store, clientID1);
    const headHash = chain[chain.length - 1].chunk.hash;

    expect(
      await store.withRead(async dagRead => {
        return await localMutationsGreaterThan(headHash, {}, dagRead);
      }),
    ).to.deep.equal([]);

    expect(
      await store.withRead(async dagRead => {
        return await localMutationsGreaterThan(
          headHash,
          {[clientID1]: 0, [clientID2]: 0},
          dagRead,
        );
      }),
    ).to.deep.equal([chain[9], chain[7], chain[5], chain[3], chain[1]]);

    expect(
      await store.withRead(async dagRead => {
        return await localMutationsGreaterThan(
          headHash,
          {[clientID1]: 1, [clientID2]: 1},
          dagRead,
        );
      }),
    ).to.deep.equal([chain[9], chain[7], chain[5]]);

    expect(
      await store.withRead(async dagRead => {
        return await localMutationsGreaterThan(
          headHash,
          {[clientID1]: 2, [clientID2]: 1},
          dagRead,
        );
      }),
    ).to.deep.equal([chain[9], chain[5]]);

    expect(
      await store.withRead(async dagRead => {
        return await localMutationsGreaterThan(
          headHash,
          {[clientID2]: 1},
          dagRead,
        );
      }),
    ).to.deep.equal([chain[5]]);

    expect(
      await store.withRead(async dagRead => {
        return await localMutationsGreaterThan(
          headHash,
          {[clientID1]: 3, [clientID2]: 2},
          dagRead,
        );
      }),
    ).to.deep.equal([]);
  }
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
  await addIndexChange(chain, store, clientID);
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
  const original = fakeHash('original');
  const valueHash = fakeHash('value');
  const emptyStringHash = fakeHash('');
  const hashHash = fakeHash('hash');
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
        basisHash: fakeHash('basis'),
        mutationID: 0,
        mutatorName: '',
        mutatorArgsJSON: 43,
        originalHash: emptyStringHash,
        timestamp,
      },
      fakeHash('valuehash'),
      [fakeHash(''), fakeHash('')],
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
      fakeHash('valuehash'),
      ['', ''],
      clientID,
    ),
    new Error('Invalid type: undefined, expected string'),
  );

  for (const basisHash of [fakeHash(''), fakeHash('hash')]) {
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
        fakeHash('vh'),
        basisHash === null ? [fakeHash('vh')] : [fakeHash('vh'), basisHash],
        clientID,
      ),
      commitNewLocal(
        createChunk,
        basisHash,
        0,
        'mutname',
        44,
        null,
        fakeHash('vh'),
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
  for (const basisHash of [null, fakeHash(''), fakeHash('hash')]) {
    t(
      await makeCommit(
        makeSnapshotMeta(
          basisHash ?? null,
          0,
          toInternalValue({foo: 'bar'}, ToInternalValueReason.Test),
          clientID,
        ),
        fakeHash('vh'),
        [fakeHash('vh')],
        clientID,
      ),
      DD31
        ? commitNewSnapshotDD31(
            createChunk,
            basisHash,
            {[clientID]: 0},
            cookie,
            fakeHash('vh'),
            [],
          )
        : commitNewSnapshot(
            createChunk,
            basisHash,
            0,
            cookie,
            fakeHash('vh'),
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
      fakeHash('vh'),
      [fakeHash('vh'), fakeHash('')],
      clientID,
    ),
    new Error('Invalid type: undefined, expected JSON value'),
  );

  for (const basisHash of [fakeHash(''), fakeHash('hash')]) {
    t(
      await makeCommit(
        makeIndexChangeMeta(basisHash, 0),
        fakeHash('value'),
        basisHash === null
          ? [fakeHash('value')]
          : [fakeHash('value'), basisHash],
        clientID,
      ),
      commitNewIndexChange(createChunk, basisHash, 0, fakeHash('value'), []),
    );
  }
});

test('accessors', async () => {
  const clientID = 'client-id';

  const originalHash = fakeHash('originalhash');
  const basisHash = fakeHash('basishash');
  const valueHash = fakeHash('valuehash');
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
      makeSnapshotMeta(fakeHash('basishash2'), 2, 'cookie 2', clientID),
      fakeHash('valuehash2'),
      [fakeHash('valuehash2'), fakeHash('basishash2')],
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
  expect(snapshot.meta.basisHash).to.equal(fakeHash('basishash2'));
  expect(snapshot.valueHash).to.equal(fakeHash('valuehash2'));
  expect(await snapshot.getNextMutationID(clientID, fakeRead)).to.equal(3);

  const indexChange = fromChunk(
    await makeCommit(
      makeIndexChangeMeta(fakeHash('basishash3'), 3),
      fakeHash('valuehash3'),
      [fakeHash('valuehash3'), fakeHash('basishash3')],
      clientID,
    ),
  );
  const ic = indexChange.meta;
  if (ic.type === MetaType.IndexChange) {
    expect(ic.lastMutationID).to.equal(3);
  } else {
    throw new Error('unexpected type');
  }
  expect(indexChange.meta.basisHash).to.equal(fakeHash('basishash3'));
  expect(indexChange.valueHash).to.equal(fakeHash('valuehash3'));
  expect(await indexChange.getMutationID(clientID, fakeRead)).to.equal(3);
});

const chunkHasher = makeTestChunkHasher('test');

function createChunk<V>(data: V, refs: readonly Hash[]): dag.Chunk<V> {
  return dag.createChunk(data, refs, chunkHasher);
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
