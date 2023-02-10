import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod';
import type * as sync from '../sync/mod';
import {assertHash, fakeHash, Hash} from '../hash';
import {
  assertHasBranchState,
  Branch,
  branchHasPendingMutations,
  BranchMap,
  BranchStateNotFoundError,
  deleteBranch,
  getBranch,
  getBranches,
  hasBranchState,
  mutatorNamesEqual,
  setBranch,
  setBranches,
} from './branches';

const headBranch1Hash = fakeHash('b1');
const headBranch2Hash = fakeHash('b2');
const headBranch3Hash = fakeHash('b3');

type PartialBranch = Partial<Branch> & Pick<Branch, 'headHash'>;

export function makeBranchMap(
  partialBranches: Record<sync.BranchID, PartialBranch>,
): BranchMap {
  const branchMap = new Map();
  for (const [branchID, partialBranch] of Object.entries(partialBranches)) {
    branchMap.set(branchID, makeBranch(partialBranch));
  }
  return branchMap;
}

export function makeBranch(partialBranch: PartialBranch): Branch {
  return {
    mutatorNames: [],
    indexes: {},
    mutationIDs: {},
    lastServerAckdMutationIDs: {},
    ...partialBranch,
  };
}

test('getBranches with no existing BranchMap in dag store', async () => {
  const dagStore = new dag.TestStore();
  await dagStore.withRead(async (read: dag.Read) => {
    const readBranchMap = await getBranches(read);
    expect(readBranchMap.size).to.equal(0);
  });
});

async function testSetBranches(
  partialBranchMap: Record<sync.BranchID, PartialBranch>,
  dagStore: dag.Store,
) {
  const branchMap = makeBranchMap(partialBranchMap);
  await dagStore.withWrite(async (write: dag.Write) => {
    const returnBranchMap = await setBranches(branchMap, write);
    expect(returnBranchMap).to.deep.equal(branchMap);
    const readBranchMap = await getBranches(write);
    expect(readBranchMap).to.deep.equal(branchMap);
    await write.commit();
  });
  await dagStore.withRead(async (read: dag.Read) => {
    const readBranchMap = await getBranches(read);
    expect(readBranchMap).to.deep.equal(branchMap);
  });
}

test('setBranches and getBranches', async () => {
  const dagStore = new dag.TestStore();
  await testSetBranches(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    dagStore,
  );
});

async function testSetBranchesSequence(
  partialBranchMap1: Record<sync.BranchID, PartialBranch>,
  partialBranchMap2: Record<sync.BranchID, PartialBranch>,
  dagStore: dag.Store,
) {
  await testSetBranches(partialBranchMap1, dagStore);
  await testSetBranches(partialBranchMap2, dagStore);
}

async function testSetBranchesSequenceThrowsError(
  partialBranchMap1: Record<sync.BranchID, PartialBranch>,
  partialBranchMap2: Record<sync.BranchID, PartialBranch>,
  expectedErrorMsg: string,
  dagStore: dag.Store,
) {
  await testSetBranches(partialBranchMap1, dagStore);
  const branchMap2 = makeBranchMap(partialBranchMap2);
  await dagStore.withWrite(async (write: dag.Write) => {
    let expectedE: unknown;
    try {
      await setBranches(branchMap2, write);
    } catch (e) {
      expectedE = e;
    }
    expect(expectedE).instanceOf(Error).property('message', expectedErrorMsg);
  });
}

test('setBranches and getBranches sequence', async () => {
  await testSetBranchesSequence(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    {
      branch3: {
        headHash: headBranch3Hash,
        // note the order of these names shouldn't matter
        mutatorNames: ['mutator2', 'mutator1'],
        indexes: {idx1: {jsonPointer: '/bar'}},
        mutationIDs: {c2: 4},
        lastServerAckdMutationIDs: {c2: 2},
      },
    },
    new dag.TestStore(),
  );
});

test('setBranches throws error if indexes are modified', async () => {
  await testSetBranchesSequenceThrowsError(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/bar'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    },
    "A branch's index definitions must never change.",
    new dag.TestStore(),
  );
});

test('setBranches does not throw error if indexes differ only by default value presence', async () => {
  await testSetBranchesSequence(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo', prefix: '', allowEmpty: false}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    },
    new dag.TestStore(),
  );
});

test('setBranches does not throw error if indexes differ only by order', async () => {
  await testSetBranchesSequence(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}, idx2: {jsonPointer: '/bar'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {
          idx2: {jsonPointer: '/bar'},
          idx1: {jsonPointer: '/foo'},
        },
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    },
    new dag.TestStore(),
  );
});

test('setBranches throws error if mutatorNames are modified', async () => {
  await testSetBranchesSequenceThrowsError(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    },
    "A branch's mutatorNames must never change.",
    new dag.TestStore(),
  );
});

test('setBranches does not throw error if mutatorNames differ only by order', async () => {
  await testSetBranchesSequence(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator2', 'mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    },
    new dag.TestStore(),
  );
});

test('setBranches throws error if mutatorNames is not a set', async () => {
  await testSetBranchesSequenceThrowsError(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1', 'mutator1'],
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    "A branch's mutatorNames must be a set.",
    new dag.TestStore(),
  );
});

async function testSetBranch(
  partialBranchMap1: Record<sync.BranchID, PartialBranch>,
  partialBranchEntryToSet: [sync.BranchID, PartialBranch],
  expectedPartialBranchMap: Record<sync.BranchID, PartialBranch>,
  dagStore: dag.Store,
) {
  await testSetBranches(partialBranchMap1, dagStore);
  const expectedBranchMap = makeBranchMap(expectedPartialBranchMap);
  await dagStore.withWrite(async (write: dag.Write) => {
    const [branchID, partialBranch] = partialBranchEntryToSet;
    const returnBranchMap = await setBranch(
      branchID,
      makeBranch(partialBranch),
      write,
    );
    expect(returnBranchMap).to.deep.equal(expectedBranchMap);
    const readBranchMap = await getBranches(write);
    expect(readBranchMap).to.deep.equal(expectedBranchMap);
    await write.commit();
  });

  await dagStore.withRead(async (read: dag.Read) => {
    const readBranchMap = await getBranches(read);
    expect(readBranchMap).to.deep.equal(expectedBranchMap);
  });
}

async function testSetBranchThrowsError(
  partialBranchMap1: Record<sync.BranchID, PartialBranch>,
  partialBranchEntryToSet: [sync.BranchID, PartialBranch],
  expectedErrorMsg: string,
  dagStore: dag.Store,
) {
  const branchMap1 = makeBranchMap(partialBranchMap1);
  await dagStore.withWrite(async (write: dag.Write) => {
    const returnBranchMap1 = await setBranches(branchMap1, write);
    expect(returnBranchMap1).to.deep.equal(branchMap1);
    const readBranchMap1 = await getBranches(write);
    expect(readBranchMap1).to.deep.equal(branchMap1);
    await write.commit();
  });
  await dagStore.withRead(async (read: dag.Read) => {
    const readBranchMap1 = await getBranches(read);
    expect(readBranchMap1).to.deep.equal(readBranchMap1);
  });

  await dagStore.withWrite(async (write: dag.Write) => {
    const [branchID, partialBranch] = partialBranchEntryToSet;
    const branch = makeBranch(partialBranch);
    let expectedE: unknown;
    try {
      await setBranch(branchID, branch, write);
    } catch (e) {
      expectedE = e;
    }
    expect(expectedE).instanceOf(Error).property('message', expectedErrorMsg);
  });
}

test('setBranch', async () => {
  await testSetBranch(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    [
      'branch3',
      {
        headHash: headBranch3Hash,
        // note the order of these names shouldn't matter
        mutatorNames: ['mutator2', 'mutator1'],
        indexes: {idx1: {jsonPointer: '/bar'}},
        mutationIDs: {c2: 4},
        lastServerAckdMutationIDs: {c2: 2},
      },
    ],
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
      branch3: {
        headHash: headBranch3Hash,
        // note the order of these names shouldn't matter
        mutatorNames: ['mutator2', 'mutator1'],
        indexes: {idx1: {jsonPointer: '/bar'}},
        mutationIDs: {c2: 4},
        lastServerAckdMutationIDs: {c2: 2},
      },
    },
    new dag.TestStore(),
  );
});

test('setBranch throws error if indexes are modified', async () => {
  await testSetBranchThrowsError(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    [
      'branch1',
      {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/bar'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    ],
    "A branch's index definitions must never change.",
    new dag.TestStore(),
  );
});

test('setBranch does not throw error if indexes differ only by default value presence', async () => {
  await testSetBranch(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    [
      'branch1',
      {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo', prefix: '', allowEmpty: false}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    ],
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo', prefix: '', allowEmpty: false}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    new dag.TestStore(),
  );
});

test('setBranch does not throw error if indexes differ only by order', async () => {
  await testSetBranch(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}, idx2: {jsonPointer: '/bar'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    [
      'branch1',
      {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {
          idx2: {jsonPointer: '/bar'},
          idx1: {jsonPointer: '/foo'},
        },
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    ],
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}, idx2: {jsonPointer: '/bar'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    new dag.TestStore(),
  );
});

test('setBranch throws error if mutatorNames are modified', async () => {
  await testSetBranchThrowsError(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    [
      'branch1',
      {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    ],
    "A branch's mutatorNames must never change.",
    new dag.TestStore(),
  );
});

test('setBranch does not throw error if mutatorNames differ only by order', async () => {
  await testSetBranch(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    [
      'branch1',
      {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator2', 'mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    ],
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator2', 'mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    new dag.TestStore(),
  );
});

test('setBranch throws error if mutatorNames is not a set', async () => {
  await testSetBranchThrowsError(
    {
      branch1: {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1'],
      },
      branch2: {
        headHash: headBranch2Hash,
      },
    },
    [
      'branch1',
      {
        headHash: headBranch1Hash,
        mutatorNames: ['mutator1', 'mutator1'],
      },
    ],
    "A branch's mutatorNames must be a set.",
    new dag.TestStore(),
  );
});

test('deleteBranch', async () => {
  const dagStore = new dag.TestStore();
  const branch2 = makeBranch({
    headHash: headBranch2Hash,
  });
  const branchMap1 = makeBranchMap({
    branch1: {
      headHash: headBranch1Hash,
    },
    branch2,
  });

  await dagStore.withWrite(async (write: dag.Write) => {
    await setBranches(branchMap1, write);
    await write.commit();
  });

  await dagStore.withWrite(async (write: dag.Write) => {
    const returnBranchMap = await deleteBranch('branch3', write);
    expect(returnBranchMap).to.deep.equal(branchMap1);
    const readBranchMap = await getBranches(write);
    expect(readBranchMap).to.deep.equal(branchMap1);
    await write.commit();
  });

  await dagStore.withRead(async (read: dag.Read) => {
    const readBranchMap = await getBranches(read);
    expect(readBranchMap).to.deep.equal(branchMap1);
  });

  const expectedBranchAfterDeletingBranch1 = makeBranchMap({branch2});
  await dagStore.withWrite(async (write: dag.Write) => {
    const returnBranchMap = await deleteBranch('branch1', write);
    expect(returnBranchMap).to.deep.equal(expectedBranchAfterDeletingBranch1);
    const readBranchMap = await getBranches(write);
    expect(readBranchMap).to.deep.equal(expectedBranchAfterDeletingBranch1);
    await write.commit();
  });

  await dagStore.withRead(async (read: dag.Read) => {
    const readBranchMap = await getBranches(read);
    expect(readBranchMap).to.deep.equal(expectedBranchAfterDeletingBranch1);
  });
});

async function expectRefs(expected: Hash[], dagStore: dag.Store) {
  await dagStore.withRead(async (read: dag.Read) => {
    const branchesHash = await read.getHead('branches');
    assertHash(branchesHash);
    const branchesChunk = await read.getChunk(branchesHash);
    expect(branchesChunk?.meta).to.deep.equal(expected);
  });
}

test('setBranches properly manages refs to branch heads when branches are removed and added', async () => {
  const dagStore = new dag.TestStore();
  const branch1HeadHash = headBranch1Hash;
  const branch2HeadHash = headBranch2Hash;

  const branchMap1 = makeBranchMap({
    branch1: {
      headHash: branch1HeadHash,
    },
    branch2: {
      headHash: branch2HeadHash,
    },
  });

  const branch3HeadHash = fakeHash('baeada1');
  const branchMap2 = makeBranchMap({
    branch3: {
      headHash: branch3HeadHash,
    },
  });

  await dagStore.withWrite(async (write: dag.Write) => {
    await setBranches(branchMap1, write);
    await write.commit();
  });
  await expectRefs([branch1HeadHash, branch2HeadHash], dagStore);

  await dagStore.withWrite(async (write: dag.Write) => {
    await setBranches(branchMap2, write);
    await write.commit();
  });

  await expectRefs([branch3HeadHash], dagStore);
});

test("setBranches properly manages refs to branch heads when a branch's head changes", async () => {
  const dagStore = new dag.TestStore();
  const branch1V1HeadHash = fakeHash('b11');
  const branch1V2HeadHash = fakeHash('b12');
  const branch2HeadHash = fakeHash('b2');

  const branch1V1 = makeBranch({
    headHash: branch1V1HeadHash,
  });
  const branch1V2 = makeBranch({
    headHash: branch1V2HeadHash,
  });
  const branch2 = makeBranch({
    headHash: branch2HeadHash,
  });

  const branchMap1 = makeBranchMap({
    branch1: branch1V1,
    branch2,
  });
  const branchMap2 = makeBranchMap({
    branch1: branch1V2,
    branch2,
  });

  await dagStore.withWrite(async (write: dag.Write) => {
    await setBranches(branchMap1, write);
    await write.commit();
  });
  await expectRefs([branch1V1HeadHash, branch2HeadHash], dagStore);

  await dagStore.withWrite(async (write: dag.Write) => {
    await setBranches(branchMap2, write);
    await write.commit();
  });
  await expectRefs([branch1V2HeadHash, branch2HeadHash], dagStore);
});

test('setBranch properly manages refs to branch heads when a branch is added', async () => {
  const dagStore = new dag.TestStore();
  const branch1HeadHash = fakeHash('b1');
  const branch2HeadHash = fakeHash('b2');
  const branch3HeadHash = fakeHash('b3');

  const branchMap1 = makeBranchMap({
    branch1: {
      headHash: branch1HeadHash,
    },
    branch2: {
      headHash: branch2HeadHash,
    },
  });

  await dagStore.withWrite(async (write: dag.Write) => {
    await setBranches(branchMap1, write);
    await write.commit();
  });
  await expectRefs([branch1HeadHash, branch2HeadHash], dagStore);

  await dagStore.withWrite(async (write: dag.Write) => {
    await setBranch(
      'branch3',
      makeBranch({
        headHash: branch3HeadHash,
      }),
      write,
    );
    await write.commit();
  });

  await expectRefs(
    [branch1HeadHash, branch2HeadHash, branch3HeadHash],
    dagStore,
  );
});

test("setBranch properly manages refs to branch heads when a branch's head changes", async () => {
  const dagStore = new dag.TestStore();
  const branch1V1HeadHash = fakeHash('b11');
  const branch1V2HeadHash = fakeHash('b12');
  const branch2HeadHash = fakeHash('b2');

  const branchMap1 = makeBranchMap({
    branch1: {
      headHash: branch1V1HeadHash,
    },
    branch2: {
      headHash: branch2HeadHash,
    },
  });

  await dagStore.withWrite(async (write: dag.Write) => {
    await setBranches(branchMap1, write);
    await write.commit();
  });
  await expectRefs([branch1V1HeadHash, branch2HeadHash], dagStore);

  await dagStore.withWrite(async (write: dag.Write) => {
    await setBranch(
      'branch1',
      makeBranch({
        headHash: branch1V2HeadHash,
      }),
      write,
    );
    await write.commit();
  });

  await expectRefs([branch1V2HeadHash, branch2HeadHash], dagStore);
});

test('deleteBranch properly manages refs to branch heads', async () => {
  const dagStore = new dag.TestStore();
  const branch1HeadHash = headBranch1Hash;
  const branch2HeadHash = headBranch2Hash;

  const branchMap1 = makeBranchMap({
    branch1: {
      headHash: branch1HeadHash,
    },
    branch2: {
      headHash: branch2HeadHash,
    },
  });

  await dagStore.withWrite(async (write: dag.Write) => {
    await setBranches(branchMap1, write);
    await write.commit();
  });
  await expectRefs([branch1HeadHash, branch2HeadHash], dagStore);

  await dagStore.withWrite(async (write: dag.Write) => {
    await deleteBranch('branch1', write);
    await write.commit();
  });

  await expectRefs([branch2HeadHash], dagStore);
});

test('getBranch', async () => {
  const dagStore = new dag.TestStore();
  const branch1 = makeBranch({
    headHash: headBranch1Hash,
  });
  const branchMap = makeBranchMap({
    branch1,
    branch2: {
      headHash: headBranch2Hash,
    },
  });
  await dagStore.withWrite(async (write: dag.Write) => {
    await setBranches(branchMap, write);
    await write.commit();
  });

  await dagStore.withRead(async (read: dag.Read) => {
    const readBranch1 = await getBranch('branch1', read);
    expect(readBranch1).to.deep.equal(branch1);
  });
});

test('hasBranchState', async () => {
  const dagStore = new dag.TestStore();
  await dagStore.withRead(async (read: dag.Read) => {
    expect(await hasBranchState('branch1', read)).to.be.false;
    expect(await hasBranchState('branch2', read)).to.be.false;
  });

  await dagStore.withWrite(async (write: dag.Write) => {
    await setBranches(
      makeBranchMap({
        branch1: {headHash: headBranch1Hash},
      }),
      write,
    );
    await write.commit();
  });

  await dagStore.withRead(async (read: dag.Read) => {
    expect(await hasBranchState('branch1', read)).to.be.true;
    expect(await hasBranchState('branch2', read)).to.be.false;
  });
});

test('assertHasBranchState', async () => {
  const dagStore = new dag.TestStore();
  await dagStore.withWrite(async (write: dag.Write) => {
    await setBranches(
      makeBranchMap({
        branch1: {headHash: headBranch1Hash},
      }),
      write,
    );
    await write.commit();
  });

  await dagStore.withRead(async (read: dag.Read) => {
    await assertHasBranchState('branch1', read);
    let expectedE;
    try {
      await assertHasBranchState('branch2', read);
    } catch (e) {
      expectedE = e;
    }
    expect(expectedE).instanceOf(BranchStateNotFoundError);
  });
});

test('mutatorNamesEqual', () => {
  const t = (a: string[], b: string[] = a) => {
    expect(mutatorNamesEqual(new Set(a), b)).true;
    expect(mutatorNamesEqual(new Set(b), a)).true;
  };
  const f = (a: string[], b: string[] = a) => {
    expect(mutatorNamesEqual(new Set(a), b)).false;
    expect(mutatorNamesEqual(new Set(b), a)).false;
  };

  t([]);
  t(['a']);
  t(['a', 'b']);
  t(['a', 'b'], ['b', 'a']);
  t(['a', 'b', 'c']);
  t(['a', 'b', 'c'], ['c', 'b', 'a']);

  f([], ['b']);
  f(['a'], ['b']);
  f(['a', 'b'], ['b', 'c']);
});

test('branchHasPendingMutations', () => {
  expect(
    branchHasPendingMutations(
      makeBranch({
        headHash: fakeHash('f'),
        mutationIDs: {},
        lastServerAckdMutationIDs: {},
      }),
    ),
  ).to.be.false;
  expect(
    branchHasPendingMutations(
      makeBranch({
        headHash: fakeHash('f1'),
        mutationIDs: {client1: 1},
        lastServerAckdMutationIDs: {},
      }),
    ),
  ).to.be.true;
  expect(
    branchHasPendingMutations(
      makeBranch({
        headHash: fakeHash('f1'),
        mutationIDs: {client1: 1},
        lastServerAckdMutationIDs: {client1: 1},
      }),
    ),
  ).to.be.false;
  expect(
    branchHasPendingMutations(
      makeBranch({
        headHash: fakeHash('f1'),
        mutationIDs: {client1: 0},
        lastServerAckdMutationIDs: {},
      }),
    ),
  ).to.be.false;
  expect(
    branchHasPendingMutations(
      makeBranch({
        headHash: fakeHash('f1'),
        mutationIDs: {client1: 1, client2: 2},
        lastServerAckdMutationIDs: {client1: 1, client2: 1},
      }),
    ),
  ).to.be.true;
  expect(
    branchHasPendingMutations(
      makeBranch({
        headHash: fakeHash('f1'),
        mutationIDs: {client1: 0, client2: 2},
        lastServerAckdMutationIDs: {client2: 2},
      }),
    ),
  ).to.be.false;
});
