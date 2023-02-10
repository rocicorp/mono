import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod';
import {Hash, makeNewFakeHashFunction} from '../hash';
import {
  addGenesis,
  addIndexChange,
  addLocal,
  addSnapshot,
  Chain,
} from '../db/test-helpers';
import {GatherVisitor} from './gather-visitor';
import {TestMemStore} from '../kv/test-mem-store';
import {sortByHash} from '../dag/test-store';
import type {JSONObject} from '../json.js';

class TestChunkLocationTracker implements dag.ChunkLocationTracker {
  readonly memOnlyChunkHashes = new Set<Hash>();
  async isMemOnlyChunkHash(chunkHash: Hash): Promise<boolean> {
    return this.memOnlyChunkHashes.has(chunkHash);
  }
  async chunksPersisted(_chunkHashes: Iterable<Hash>): Promise<void> {
    throw new Error('Unexpected call to chunksPersisted');
  }
}

test('dag with no temp hashes gathers nothing', async () => {
  const clientID = 'client-id';
  const dagStore = new dag.TestStore();
  const chunkLocationTracker = new TestChunkLocationTracker();

  const chain: Chain = [];
  await addGenesis(chain, dagStore, clientID);
  await addLocal(chain, dagStore, clientID);
  if (!DD31) {
    await addIndexChange(chain, dagStore, clientID);
  }
  await addLocal(chain, dagStore, clientID);

  await dagStore.withRead(async dagRead => {
    for (const commit of chain) {
      const visitor = new GatherVisitor(chunkLocationTracker, dagRead);
      await visitor.visitCommit(commit.chunk.hash);
      expect(visitor.gatheredChunks).to.be.empty;
    }
  });

  await addSnapshot(chain, dagStore, undefined, clientID);

  await dagStore.withRead(async dagRead => {
    const visitor = new GatherVisitor(chunkLocationTracker, dagRead);
    await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
    expect(visitor.gatheredChunks).to.be.empty;
  });
});

test('dag with only temp hashes gathers everything', async () => {
  const clientID = 'client-id';
  const chunkLocationTracker = new TestChunkLocationTracker();
  const kvStore = new TestMemStore();
  const hashFunc = makeNewFakeHashFunction();
  const dagStore = new dag.TestStore(
    kvStore,
    () => {
      const hash = hashFunc();
      chunkLocationTracker.memOnlyChunkHashes.add(hash);
      return hash;
    },
    () => void 0,
  );
  const chain: Chain = [];

  const testGatheredChunks = async () => {
    await dagStore.withRead(async dagRead => {
      const visitor = new GatherVisitor(chunkLocationTracker, dagRead);
      await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
      expect(dagStore.chunks()).to.deep.equal(
        sortByHash(visitor.gatheredChunks.values()),
      );
    });
  };

  await addGenesis(chain, dagStore, clientID);
  await addLocal(chain, dagStore, clientID);
  await testGatheredChunks();

  if (!DD31) {
    await addIndexChange(chain, dagStore, clientID);
  }
  await addLocal(chain, dagStore, clientID);
  await testGatheredChunks();

  await addSnapshot(chain, dagStore, undefined, clientID);
  await testGatheredChunks();
});

test('dag with some permanent hashes and some temp hashes on top', async () => {
  const clientID = 'client-id';
  const chunkLocationTracker = new TestChunkLocationTracker();
  const hashFunc = makeNewFakeHashFunction();
  const kvStore = new TestMemStore();
  const perdag = new dag.TestStore(kvStore, hashFunc);
  const chain: Chain = [];

  await addGenesis(chain, perdag, clientID);
  await addLocal(chain, perdag, clientID);

  await perdag.withRead(async dagRead => {
    const visitor = new GatherVisitor(chunkLocationTracker, dagRead);
    await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
    expect(visitor.gatheredChunks).to.be.empty;
  });

  const memdag = new dag.TestStore(
    kvStore,
    () => {
      const hash = hashFunc();
      chunkLocationTracker.memOnlyChunkHashes.add(hash);
      return hash;
    },
    () => void 0,
  );

  await addLocal(chain, memdag, clientID);

  await memdag.withRead(async dagRead => {
    const visitor = new GatherVisitor(chunkLocationTracker, dagRead);
    await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
    const meta: JSONObject = {
      basisHash: 'face0000-0000-4000-8000-000000000003',
      mutationID: 2,
      mutatorArgsJSON: [2],
      mutatorName: 'mutator_name_2',
      originalHash: null,
      timestamp: 42,
      type: 2,
    };
    if (DD31) {
      meta.clientID = clientID;
    }
    expect(Object.fromEntries(visitor.gatheredChunks)).to.deep.equal({
      'face0000-0000-4000-8000-000000000004': {
        data: [0, [['local', '2']]],
        hash: 'face0000-0000-4000-8000-000000000004',
        meta: [],
      },
      'face0000-0000-4000-8000-000000000005': {
        data: {
          indexes: [],
          meta,
          valueHash: 'face0000-0000-4000-8000-000000000004',
        },
        hash: 'face0000-0000-4000-8000-000000000005',
        meta: [
          'face0000-0000-4000-8000-000000000004',
          'face0000-0000-4000-8000-000000000003',
        ],
      },
    });
  });

  if (DD31) {
    await addSnapshot(
      chain,
      perdag,
      Object.entries({
        a: 1,
        b: 2,
        c: 3,
        d: 4,
      }),
      clientID,
      undefined,
      undefined,
      {4: {prefix: 'local', jsonPointer: '', allowEmpty: false}},
    );
    await addLocal(chain, memdag, clientID, []);
  } else {
    await addSnapshot(
      chain,
      perdag,
      Object.entries({
        a: 1,
        b: 2,
        c: 3,
        d: 4,
      }),
      clientID,
    );
    await addIndexChange(chain, memdag, clientID);
  }

  await memdag.withRead(async dagRead => {
    const visitor = new GatherVisitor(chunkLocationTracker, dagRead);
    await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
    expect(Object.fromEntries(visitor.gatheredChunks)).to.deep.equal(
      DD31
        ? {
            'face0000-0000-4000-8000-000000000009': {
              data: {
                indexes: [
                  {
                    definition: {
                      allowEmpty: false,
                      jsonPointer: '',
                      name: '4',
                      prefix: 'local',
                    },
                    valueHash: 'face0000-0000-4000-8000-000000000006',
                  },
                ],
                meta: {
                  basisHash: 'face0000-0000-4000-8000-000000000008',
                  clientID: 'client-id',
                  mutationID: 4,
                  mutatorArgsJSON: [4],
                  mutatorName: 'mutator_name_4',
                  originalHash: null,
                  timestamp: 42,
                  type: 2,
                },
                valueHash: 'face0000-0000-4000-8000-000000000007',
              },
              hash: 'face0000-0000-4000-8000-000000000009',
              meta: [
                'face0000-0000-4000-8000-000000000007',
                'face0000-0000-4000-8000-000000000008',
                'face0000-0000-4000-8000-000000000006',
              ],
            },
          }
        : {
            'face0000-0000-4000-8000-000000000008': {
              data: [0, [['\u00002\u0000local', '2']]],
              hash: 'face0000-0000-4000-8000-000000000008',
              meta: [],
            },
            'face0000-0000-4000-8000-000000000009': {
              data: {
                indexes: [
                  {
                    definition: {
                      jsonPointer: '',
                      prefix: 'local',
                      name: '4',
                      allowEmpty: false,
                    },
                    valueHash: 'face0000-0000-4000-8000-000000000008',
                  },
                ],
                meta: {
                  basisHash: 'face0000-0000-4000-8000-000000000007',
                  lastMutationID: 3,
                  type: 1,
                },
                valueHash: 'face0000-0000-4000-8000-000000000006',
              },
              hash: 'face0000-0000-4000-8000-000000000009',
              meta: [
                'face0000-0000-4000-8000-000000000006',
                'face0000-0000-4000-8000-000000000007',
                'face0000-0000-4000-8000-000000000008',
              ],
            },
          },
    );
  });
});
