import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod';
import {jsonObjectTestData} from '../../perf/data';
import {addGenesis, addLocal, Chain} from '../db/test-helpers';
import type {JSONValue} from '../json';
import {ViewerVisitor} from './viewer';

function createEntries(prefix: string) {
  const entries: [string, JSONValue][] = [];
  for (let i = 0; i < 500; i++) {
    entries.push([prefix + i, jsonObjectTestData(5000)]);
  }
  return entries;
}

test('verify direction of commit graph is correct', async () => {
  const dagStore = new dag.TestStore();
  const chain: Chain = [];

  await addGenesis(chain, dagStore);
  await addLocal(chain, dagStore, createEntries('a'));
  // await addIndexChange(chain, dagStore);
  await addLocal(chain, dagStore, createEntries('b'));

  await dagStore.withRead(async dagRead => {
    const visitor = new ViewerVisitor(dagRead);
    await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
    console.log(visitor.commitDotFileGraph);
    // expect(visitor.commitDotFileGraph).to.contain(
    //   '"fakehash000000000000000000000008" -> "fakehash000000000000000000000005"',
    // );
    // expect(visitor.commitDotFileGraph).to.contain(
    //   '"fakehash000000000000000000000005" -> "fakehash000000000000000000000003"',
    // );
    // expect(visitor.commitDotFileGraph).to.contain(
    //   '"fakehash000000000000000000000003" -> "fakehash000000000000000000000001"',
    // );
  });
});
