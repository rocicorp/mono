import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod';
import {addGenesis, addIndexChange, addLocal, Chain} from '../db/test-helpers';
import {ViewerVisitor} from './viewer';

test('verify direction of commit graph is correct', async () => {
  const dagStore = new dag.TestStore();
  const chain: Chain = [];
  await addGenesis(chain, dagStore);
  await addLocal(chain, dagStore);
  await addIndexChange(chain, dagStore);
  await addLocal(chain, dagStore);

  await dagStore.withRead(async dagRead => {
    const visitor = new ViewerVisitor(dagRead);
    await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
    expect(visitor.getCommitDotFileGraph).to.contain(
      '"fakehash000000000000000000000005" -> "fakehash000000000000000000000008"',
    );
    expect(visitor.getCommitDotFileGraph).to.contain(
      '"fakehash000000000000000000000003" -> "fakehash000000000000000000000005"',
    );
    expect(visitor.getCommitDotFileGraph).to.contain(
      '"fakehash000000000000000000000001" -> "fakehash000000000000000000000003"',
    );
  });
});
