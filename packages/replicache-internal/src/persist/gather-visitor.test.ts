import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod';
import {makeNewTempHashFunction, newTempHash} from '../hash';
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

test('dag with no temp hashes gathers nothing', async () => {
  const clientID = 'client-id';
  const dagStore = new dag.TestStore();

  const chain: Chain = [];
  await addGenesis(chain, dagStore, clientID);
  await addLocal(chain, dagStore, clientID);
  await addIndexChange(chain, dagStore, clientID);
  await addLocal(chain, dagStore, clientID);

  await dagStore.withRead(async dagRead => {
    for (const commit of chain) {
      const visitor = new GatherVisitor(dagRead);
      await visitor.visitCommit(commit.chunk.hash);
      expect(visitor.gatheredChunks).to.be.empty;
    }
  });

  await addSnapshot(chain, dagStore, undefined, clientID);

  await dagStore.withRead(async dagRead => {
    const visitor = new GatherVisitor(dagRead);
    await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
    expect(visitor.gatheredChunks).to.be.empty;
  });
});

test('dag with only temp hashes gathers everything', async () => {
  const clientID = 'client-id';
  const kvStore = new TestMemStore();
  const dagStore = new dag.TestStore(kvStore, newTempHash, () => void 0);
  const chain: Chain = [];

  const testGatheredChunks = async () => {
    await dagStore.withRead(async dagRead => {
      const visitor = new GatherVisitor(dagRead);
      await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
      expect(dagStore.chunks()).to.deep.equal(
        sortByHash(visitor.gatheredChunks.values()),
      );
    });
  };

  await addGenesis(chain, dagStore, clientID);
  await addLocal(chain, dagStore, clientID);
  await testGatheredChunks();

  await addIndexChange(chain, dagStore, clientID);
  await addLocal(chain, dagStore, clientID);
  await testGatheredChunks();

  await addSnapshot(chain, dagStore, undefined, clientID);
  await testGatheredChunks();
});

test('dag with some permanent hashes and some temp hashes on top', async () => {
  const clientID = 'client-id';
  const kvStore = new TestMemStore();
  const perdag = new dag.TestStore(kvStore);
  const chain: Chain = [];

  await addGenesis(chain, perdag, clientID);
  await addLocal(chain, perdag, clientID);

  await perdag.withRead(async dagRead => {
    const visitor = new GatherVisitor(dagRead);
    await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
    expect(visitor.gatheredChunks).to.be.empty;
  });

  const memdag = new dag.TestStore(
    kvStore,
    makeNewTempHashFunction(),
    () => void 0,
  );

  await addLocal(chain, memdag, clientID);

  await memdag.withRead(async dagRead => {
    const visitor = new GatherVisitor(dagRead);
    await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
    const meta: JSONObject = {
      basisHash: 'fakehash000000000000000000000003',
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
      't/000000000000000000000000000000': {
        data: [0, [['local', '2']]],
        hash: 't/000000000000000000000000000000',
        meta: [],
      },
      't/000000000000000000000000000001': {
        data: {
          indexes: [],
          meta,
          valueHash: 't/000000000000000000000000000000',
        },
        hash: 't/000000000000000000000000000001',
        meta: [
          't/000000000000000000000000000000',
          'fakehash000000000000000000000003',
        ],
      },
    });
  });

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

  await memdag.withRead(async dagRead => {
    const visitor = new GatherVisitor(dagRead);
    await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
    expect(Object.fromEntries(visitor.gatheredChunks)).to.deep.equal({
      't/000000000000000000000000000002': {
        data: [0, [['\u00002\u0000local', '2']]],
        hash: 't/000000000000000000000000000002',
        meta: [],
      },
      't/000000000000000000000000000003': {
        data: {
          indexes: [
            {
              definition: {
                jsonPointer: '',
                prefix: 'local',
                name: '4',
                allowEmpty: false,
              },
              valueHash: 't/000000000000000000000000000002',
            },
          ],
          meta: {
            basisHash: 'fakehash000000000000000000000005',
            lastMutationID: 3,
            type: 1,
          },
          valueHash: 'fakehash000000000000000000000004',
        },
        hash: 't/000000000000000000000000000003',
        meta: [
          'fakehash000000000000000000000004',
          'fakehash000000000000000000000005',
          't/000000000000000000000000000002',
        ],
      },
    });
  });
});
