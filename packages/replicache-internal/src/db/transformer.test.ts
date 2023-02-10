import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod';
import {
  addGenesis,
  addIndexChange,
  addLocal,
  addSnapshot,
  Chain,
} from './test-helpers';
import {Commit, fromHash as commitFromHash} from './commit';
import type {IndexRecord, Meta} from './commit';
import {Transformer} from './transformer';
import {Hash, makeNewTempHashFunction} from '../hash';
import {BTreeRead, BTreeWrite, Entry} from '../btree/mod';
import type {DataNode} from '../btree/node';
import {assert} from '../asserts';
import type {InternalValue} from '../internal-value.js';

test('transformBTreeInternalEntry - noop', async () => {
  const dagStore = new dag.TestStore();

  await dagStore.withWrite(async dagWrite => {
    const transformer = new Transformer(dagWrite);

    const dataNode: DataNode = [0, [['k', 42]]];
    const chunk = dagWrite.createChunk(dataNode, []);
    await dagWrite.putChunk(chunk);
    const entry: Entry<Hash> = ['key', chunk.hash];

    expect(await transformer.transformBTreeInternalEntry(entry)).to.equal(
      entry,
    );

    expect(transformer.mappings).to.be.empty;
  });
});

test('transformBTreeNode - noop', async () => {
  const dagStore = new dag.TestStore();

  await dagStore.withWrite(async dagWrite => {
    const transformer = new Transformer(dagWrite);

    const map = new BTreeWrite(dagWrite);
    await map.put('key', 'value');
    const valueHash = await map.flush();

    expect(await transformer.transformBTreeNode(valueHash)).to.equal(valueHash);
    expect(transformer.mappings).to.be.empty;
  });
});

test('transformCommit - noop', async () => {
  const dagStore = new dag.TestStore();

  const testChain = async (chain: Commit<Meta>[]) => {
    await dagStore.withWrite(async write => {
      const transformer = new Transformer(write);

      for (const commit of chain) {
        const h = commit.chunk.hash;
        expect(await transformer.transformCommit(h)).to.equal(h);
        expect(transformer.mappings).to.be.empty;
      }
    });
  };

  const chain: Chain = [];
  await addGenesis(chain, dagStore);
  await addLocal(chain, dagStore);
  await addIndexChange(chain, dagStore);
  await addLocal(chain, dagStore);
  await testChain(chain);

  await addSnapshot(chain, dagStore, [['k', 42]]);
  await addLocal(chain, dagStore);
  await testChain(chain.slice(-2));
});

test('transformIndexRecord - noop', async () => {
  const dagStore = new dag.TestStore();

  await dagStore.withWrite(async write => {
    const transformer = new Transformer(write);

    const map = new BTreeWrite(write);
    await map.put('key', 'value');
    const valueHash = await map.flush();

    const index: IndexRecord = {
      definition: {
        jsonPointer: '',
        keyPrefix: '',
        name: 'index',
      },
      valueHash,
    };

    expect(await transformer.transformIndexRecord(index)).to.equal(index);
    expect(transformer.mappings).to.be.empty;
  });
});

test('transforms data entry', async () => {
  const dagStore = new dag.TestStore(
    undefined,
    makeNewTempHashFunction(),
    () => undefined,
  );

  class TestTransformer extends Transformer {
    override async transformBTreeDataEntry(
      entry: Entry<InternalValue>,
    ): Promise<Entry<InternalValue>> {
      if (entry[0] === 'k') {
        return ['k', entry[0] + ' - Changed!'];
      }
      return entry;
    }
  }

  const chain: Chain = [];
  await addGenesis(chain, dagStore);
  await addSnapshot(chain, dagStore, [['k', 42]]);
  await addLocal(chain, dagStore);

  await dagStore.withWrite(async write => {
    const transformer = new TestTransformer(write);

    const h = chain[2].chunk.hash;
    const h2 = await transformer.transformCommit(h);

    await write.setHead('test', h2);
    await write.commit();

    expect(Object.fromEntries(transformer.mappings)).to.deep.equal({
      't/000000000000000000000000000002': 't/000000000000000000000000000007',
      't/000000000000000000000000000003': 't/000000000000000000000000000008',
      't/000000000000000000000000000004': 't/000000000000000000000000000006',
      ['t/000000000000000000000000000005']: 't/000000000000000000000000000009',
    });
  });

  await dagStore.withRead(async read => {
    const headHash = await read.getHead('test');
    assert(headHash);
    const commit = await commitFromHash(headHash, read);
    const map = new BTreeRead(read, commit.valueHash);
    expect(await map.get('k')).to.equal('k - Changed!');
  });
});
