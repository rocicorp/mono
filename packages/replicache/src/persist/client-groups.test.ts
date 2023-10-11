import {expect} from 'chai';
import type {Read, Store, Write} from '../dag/store.js';
import {TestStore} from '../dag/test-store.js';
import {Hash, assertHash, fakeHash} from '../hash.js';
import type {ClientGroupID} from '../sync/ids.js';
import {withRead, withWriteNoImplicitCommit} from '../with-transactions.js';
import {
  ClientGroup,
  ClientGroupMap,
  clientGroupHasPendingMutations,
  deleteClientGroup,
  disableClientGroup,
  getClientGroup,
  getClientGroups,
  mutatorNamesEqual,
  setClientGroup,
  setClientGroups,
} from './client-groups.js';

const headClientGroup1Hash = fakeHash('b1');
const headClientGroup2Hash = fakeHash('b2');
const headClientGroup3Hash = fakeHash('b3');

type PartialClientGroup = Partial<ClientGroup> & Pick<ClientGroup, 'headHash'>;

export function makeClientGroupMap(
  partialClientGroups: Record<ClientGroupID, PartialClientGroup>,
): ClientGroupMap {
  const clientGroupMap = new Map();
  for (const [clientGroupID, partialClientGroup] of Object.entries(
    partialClientGroups,
  )) {
    clientGroupMap.set(clientGroupID, makeClientGroup(partialClientGroup));
  }
  return clientGroupMap;
}

export function makeClientGroup(
  partialClientGroup: PartialClientGroup,
): ClientGroup {
  return {
    mutatorNames: [],
    indexes: {},
    mutationIDs: {},
    lastServerAckdMutationIDs: {},
    disabled: false,
    ...partialClientGroup,
  };
}

test('getClientGroups with no existing ClientGroupMap in dag store', async () => {
  const dagStore = new TestStore();
  await withRead(dagStore, async (read: Read) => {
    const readClientGroupMap = await getClientGroups(read);
    expect(readClientGroupMap.size).to.equal(0);
  });
});

async function testSetClientGroups(
  partialClientGroupMap: Record<ClientGroupID, PartialClientGroup>,
  dagStore: Store,
) {
  const clientGroupMap = makeClientGroupMap(partialClientGroupMap);
  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    const returnClientGroupMap = await setClientGroups(clientGroupMap, write);
    expect(returnClientGroupMap).to.deep.equal(clientGroupMap);
    const readClientGroupMap = await getClientGroups(write);
    expect(readClientGroupMap).to.deep.equal(clientGroupMap);
    await write.commit();
  });
  await withRead(dagStore, async (read: Read) => {
    const readClientGroupMap = await getClientGroups(read);
    expect(readClientGroupMap).to.deep.equal(clientGroupMap);
  });
}

test('setClientGroups and getClientGroups', async () => {
  const dagStore = new TestStore();
  await testSetClientGroups(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    dagStore,
  );
});

async function testSetClientGroupsSequence(
  partialClientGroupMap1: Record<ClientGroupID, PartialClientGroup>,
  partialClientGroupMap2: Record<ClientGroupID, PartialClientGroup>,
  dagStore: Store,
) {
  await testSetClientGroups(partialClientGroupMap1, dagStore);
  await testSetClientGroups(partialClientGroupMap2, dagStore);
}

async function testSetClientGroupsSequenceThrowsError(
  partialClientGroupMap1: Record<ClientGroupID, PartialClientGroup>,
  partialClientGroupMap2: Record<ClientGroupID, PartialClientGroup>,
  expectedErrorMsg: string,
  dagStore: Store,
) {
  await testSetClientGroups(partialClientGroupMap1, dagStore);
  const clientGroupMap2 = makeClientGroupMap(partialClientGroupMap2);
  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    let expectedE: unknown;
    try {
      await setClientGroups(clientGroupMap2, write);
    } catch (e) {
      expectedE = e;
    }
    expect(expectedE).instanceOf(Error).property('message', expectedErrorMsg);
  });
}

test('setClientGroups and getClientGroups sequence', async () => {
  await testSetClientGroupsSequence(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    {
      'client-group-3': {
        headHash: headClientGroup3Hash,
        // note the order of these names shouldn't matter
        mutatorNames: ['mutator2', 'mutator1'],
        indexes: {idx1: {jsonPointer: '/bar'}},
        mutationIDs: {c2: 4},
        lastServerAckdMutationIDs: {c2: 2},
      },
    },
    new TestStore(),
  );
});

test('setClientGroups throws error if indexes are modified', async () => {
  await testSetClientGroupsSequenceThrowsError(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/bar'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    },
    "A client group's index definitions must never change.",
    new TestStore(),
  );
});

test('setClientGroups does not throw error if indexes differ only by default value presence', async () => {
  await testSetClientGroupsSequence(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo', prefix: '', allowEmpty: false}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    },
    new TestStore(),
  );
});

test('setClientGroups does not throw error if indexes differ only by order', async () => {
  await testSetClientGroupsSequence(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}, idx2: {jsonPointer: '/bar'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {
          idx2: {jsonPointer: '/bar'},
          idx1: {jsonPointer: '/foo'},
        },
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    },
    new TestStore(),
  );
});

test('setClientGroups throws error if mutatorNames are modified', async () => {
  await testSetClientGroupsSequenceThrowsError(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    },
    "A client group's mutatorNames must never change.",
    new TestStore(),
  );
});

test('setClientGroups does not throw error if mutatorNames differ only by order', async () => {
  await testSetClientGroupsSequence(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator2', 'mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    },
    new TestStore(),
  );
});

test('setClientGroups throws error if mutatorNames is not a set', async () => {
  await testSetClientGroupsSequenceThrowsError(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1', 'mutator1'],
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    "A client group's mutatorNames must be a set.",
    new TestStore(),
  );
});

async function testSetClientGroup(
  partialClientGroupMap1: Record<ClientGroupID, PartialClientGroup>,
  partialClientGroupEntryToSet: [ClientGroupID, PartialClientGroup],
  expectedPartialClientGroupMap: Record<ClientGroupID, PartialClientGroup>,
  dagStore: Store,
) {
  await testSetClientGroups(partialClientGroupMap1, dagStore);
  const expectedClientGroupMap = makeClientGroupMap(
    expectedPartialClientGroupMap,
  );
  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    const [clientGroupID, partialClientGroup] = partialClientGroupEntryToSet;
    const returnClientGroupMap = await setClientGroup(
      clientGroupID,
      makeClientGroup(partialClientGroup),
      write,
    );
    expect(returnClientGroupMap).to.deep.equal(expectedClientGroupMap);
    const readClientGroupMap = await getClientGroups(write);
    expect(readClientGroupMap).to.deep.equal(expectedClientGroupMap);
    await write.commit();
  });

  await withRead(dagStore, async (read: Read) => {
    const readClientGroupMap = await getClientGroups(read);
    expect(readClientGroupMap).to.deep.equal(expectedClientGroupMap);
  });
}

async function testSetClientGroupThrowsError(
  partialClientGroupMap1: Record<ClientGroupID, PartialClientGroup>,
  partialClientGroupEntryToSet: [ClientGroupID, PartialClientGroup],
  expectedErrorMsg: string,
  dagStore: Store,
) {
  const clientGroupMap1 = makeClientGroupMap(partialClientGroupMap1);
  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    const returnClientGroupMap1 = await setClientGroups(clientGroupMap1, write);
    expect(returnClientGroupMap1).to.deep.equal(clientGroupMap1);
    const readClientGroupMap1 = await getClientGroups(write);
    expect(readClientGroupMap1).to.deep.equal(clientGroupMap1);
    await write.commit();
  });
  await withRead(dagStore, async (read: Read) => {
    const readClientGroupMap1 = await getClientGroups(read);
    expect(readClientGroupMap1).to.deep.equal(readClientGroupMap1);
  });

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    const [clientGroupID, partialClientGroup] = partialClientGroupEntryToSet;
    const clientGroup = makeClientGroup(partialClientGroup);
    let expectedE: unknown;
    try {
      await setClientGroup(clientGroupID, clientGroup, write);
    } catch (e) {
      expectedE = e;
    }
    expect(expectedE).instanceOf(Error).property('message', expectedErrorMsg);
  });
}

test('setClientGroup', async () => {
  await testSetClientGroup(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    [
      'client-group-3',
      {
        headHash: headClientGroup3Hash,
        // note the order of these names shouldn't matter
        mutatorNames: ['mutator2', 'mutator1'],
        indexes: {idx1: {jsonPointer: '/bar'}},
        mutationIDs: {c2: 4},
        lastServerAckdMutationIDs: {c2: 2},
      },
    ],
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
      'client-group-3': {
        headHash: headClientGroup3Hash,
        // note the order of these names shouldn't matter
        mutatorNames: ['mutator2', 'mutator1'],
        indexes: {idx1: {jsonPointer: '/bar'}},
        mutationIDs: {c2: 4},
        lastServerAckdMutationIDs: {c2: 2},
      },
    },
    new TestStore(),
  );
});

test('setClientGroup throws error if indexes are modified', async () => {
  await testSetClientGroupThrowsError(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    [
      'client-group-1',
      {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/bar'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    ],
    "A client group's index definitions must never change.",
    new TestStore(),
  );
});

test('setClientGroup does not throw error if indexes differ only by default value presence', async () => {
  await testSetClientGroup(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    [
      'client-group-1',
      {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo', prefix: '', allowEmpty: false}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    ],
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo', prefix: '', allowEmpty: false}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    new TestStore(),
  );
});

test('setClientGroup does not throw error if indexes differ only by order', async () => {
  await testSetClientGroup(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}, idx2: {jsonPointer: '/bar'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    [
      'client-group-1',
      {
        headHash: headClientGroup1Hash,
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
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}, idx2: {jsonPointer: '/bar'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    new TestStore(),
  );
});

test('setClientGroup throws error if mutatorNames are modified', async () => {
  await testSetClientGroupThrowsError(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    [
      'client-group-1',
      {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    ],
    "A client group's mutatorNames must never change.",
    new TestStore(),
  );
});

test('setClientGroup does not throw error if mutatorNames differ only by order', async () => {
  await testSetClientGroup(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1', 'mutator2'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    [
      'client-group-1',
      {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator2', 'mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
    ],
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator2', 'mutator1'],
        indexes: {idx1: {jsonPointer: '/foo'}},
        mutationIDs: {c1: 4},
        lastServerAckdMutationIDs: {c1: 2},
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    new TestStore(),
  );
});

test('setClientGroup throws error if mutatorNames is not a set', async () => {
  await testSetClientGroupThrowsError(
    {
      'client-group-1': {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1'],
      },
      'client-group-2': {
        headHash: headClientGroup2Hash,
      },
    },
    [
      'client-group-1',
      {
        headHash: headClientGroup1Hash,
        mutatorNames: ['mutator1', 'mutator1'],
      },
    ],
    "A client group's mutatorNames must be a set.",
    new TestStore(),
  );
});

test('deleteClientGroup', async () => {
  const dagStore = new TestStore();
  const clientGroup2 = makeClientGroup({
    headHash: headClientGroup2Hash,
  });
  const clientGroupMap1 = makeClientGroupMap({
    'client-group-1': {
      headHash: headClientGroup1Hash,
    },
    'client-group-2': clientGroup2,
  });

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await setClientGroups(clientGroupMap1, write);
    await write.commit();
  });

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    const returnClientGroupMap = await deleteClientGroup(
      'client-group-3',
      write,
    );
    expect(returnClientGroupMap).to.deep.equal(clientGroupMap1);
    const readClientGroupMap = await getClientGroups(write);
    expect(readClientGroupMap).to.deep.equal(clientGroupMap1);
    await write.commit();
  });

  await withRead(dagStore, async (read: Read) => {
    const readClientGroupMap = await getClientGroups(read);
    expect(readClientGroupMap).to.deep.equal(clientGroupMap1);
  });

  const expectedClientGroupAfterDeletingClientGroup1 = makeClientGroupMap({
    'client-group-2': clientGroup2,
  });
  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    const returnClientGroupMap = await deleteClientGroup(
      'client-group-1',
      write,
    );
    expect(Object.fromEntries(returnClientGroupMap)).to.deep.equal(
      Object.fromEntries(expectedClientGroupAfterDeletingClientGroup1),
    );
    const readClientGroupMap = await getClientGroups(write);
    expect(readClientGroupMap).to.deep.equal(
      expectedClientGroupAfterDeletingClientGroup1,
    );
    await write.commit();
  });

  await withRead(dagStore, async (read: Read) => {
    const readClientGroupMap = await getClientGroups(read);
    expect(readClientGroupMap).to.deep.equal(
      expectedClientGroupAfterDeletingClientGroup1,
    );
  });
});

async function expectRefs(expected: Hash[], dagStore: Store) {
  await withRead(dagStore, async (read: Read) => {
    const clientGroupsHash = await read.getHead('client-groups');
    assertHash(clientGroupsHash);
    const clientGroupsChunk = await read.getChunk(clientGroupsHash);
    expect(clientGroupsChunk?.meta).to.deep.equal(expected);
  });
}

test('setClientGroups properly manages refs to client group heads when client group are removed and added', async () => {
  const dagStore = new TestStore();
  const clientGroup1HeadHash = headClientGroup1Hash;
  const clientGroup2HeadHash = headClientGroup2Hash;

  const clientGroupMap1 = makeClientGroupMap({
    'client-group-1': {
      headHash: clientGroup1HeadHash,
    },
    'client-group-2': {
      headHash: clientGroup2HeadHash,
    },
  });

  const clientGroup3HeadHash = fakeHash('baeada1');
  const clientGroupMap2 = makeClientGroupMap({
    'client-group-3': {
      headHash: clientGroup3HeadHash,
    },
  });

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await setClientGroups(clientGroupMap1, write);
    await write.commit();
  });
  await expectRefs([clientGroup1HeadHash, clientGroup2HeadHash], dagStore);

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await setClientGroups(clientGroupMap2, write);
    await write.commit();
  });

  await expectRefs([clientGroup3HeadHash], dagStore);
});

test("setClientGroup properly manages refs to client group heads when a client group's head changes", async () => {
  const dagStore = new TestStore();
  const clientGroup1V1HeadHash = fakeHash('b11');
  const clientGroup1V2HeadHash = fakeHash('b12');
  const clientGroup2HeadHash = fakeHash('b2');

  const clientGroup1V1 = makeClientGroup({
    headHash: clientGroup1V1HeadHash,
  });
  const clientGroup1V2 = makeClientGroup({
    headHash: clientGroup1V2HeadHash,
  });
  const clientGroup2 = makeClientGroup({
    headHash: clientGroup2HeadHash,
  });

  const clientGroupMap1 = makeClientGroupMap({
    'client-group-1': clientGroup1V1,
    'client-group-2': clientGroup2,
  });
  const clientGroupMap2 = makeClientGroupMap({
    'client-group-1': clientGroup1V2,
    'client-group-2': clientGroup2,
  });

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await setClientGroups(clientGroupMap1, write);
    await write.commit();
  });
  await expectRefs([clientGroup1V1HeadHash, clientGroup2HeadHash], dagStore);

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await setClientGroups(clientGroupMap2, write);
    await write.commit();
  });
  await expectRefs([clientGroup1V2HeadHash, clientGroup2HeadHash], dagStore);
});

test('setClientGroup properly manages refs to client group heads when a client group is added', async () => {
  const dagStore = new TestStore();
  const clientGroup1HeadHash = fakeHash('b1');
  const clientGroup2HeadHash = fakeHash('b2');
  const clientGroup3HeadHash = fakeHash('b3');

  const clientGroupMap1 = makeClientGroupMap({
    'client-group-1': {
      headHash: clientGroup1HeadHash,
    },
    'client-group-2': {
      headHash: clientGroup2HeadHash,
    },
  });

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await setClientGroups(clientGroupMap1, write);
    await write.commit();
  });
  await expectRefs([clientGroup1HeadHash, clientGroup2HeadHash], dagStore);

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await setClientGroup(
      'client-group-3',
      makeClientGroup({
        headHash: clientGroup3HeadHash,
      }),
      write,
    );
    await write.commit();
  });

  await expectRefs(
    [clientGroup1HeadHash, clientGroup2HeadHash, clientGroup3HeadHash],
    dagStore,
  );
});

test("setClientGroup properly manages refs to client group heads when a client group's head changes", async () => {
  const dagStore = new TestStore();
  const clientGroup1V1HeadHash = fakeHash('b11');
  const clientGroup1V2HeadHash = fakeHash('b12');
  const clientGroup2HeadHash = fakeHash('b2');

  const clientGroupMap1 = makeClientGroupMap({
    'client-group-1': {
      headHash: clientGroup1V1HeadHash,
    },
    'client-group-2': {
      headHash: clientGroup2HeadHash,
    },
  });

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await setClientGroups(clientGroupMap1, write);
    await write.commit();
  });
  await expectRefs([clientGroup1V1HeadHash, clientGroup2HeadHash], dagStore);

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await setClientGroup(
      'client-group-1',
      makeClientGroup({
        headHash: clientGroup1V2HeadHash,
      }),
      write,
    );
    await write.commit();
  });

  await expectRefs([clientGroup1V2HeadHash, clientGroup2HeadHash], dagStore);
});

test('deleteClientGroup properly manages refs to client group heads', async () => {
  const dagStore = new TestStore();
  const clientGroup1HeadHash = headClientGroup1Hash;
  const clientGroup2HeadHash = headClientGroup2Hash;

  const clientGroupMap1 = makeClientGroupMap({
    'client-group-1': {
      headHash: clientGroup1HeadHash,
    },
    'client-group-2': {
      headHash: clientGroup2HeadHash,
    },
  });

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await setClientGroups(clientGroupMap1, write);
    await write.commit();
  });
  await expectRefs([clientGroup1HeadHash, clientGroup2HeadHash], dagStore);

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await deleteClientGroup('client-group-1', write);
    await write.commit();
  });

  await expectRefs([clientGroup2HeadHash], dagStore);
});

test('getClientGroup', async () => {
  const dagStore = new TestStore();
  const clientGroup1 = makeClientGroup({
    headHash: headClientGroup1Hash,
  });
  const clientGroupMap = makeClientGroupMap({
    'client-group-1': clientGroup1,
    'client-group-2': {
      headHash: headClientGroup2Hash,
    },
  });
  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await setClientGroups(clientGroupMap, write);
    await write.commit();
  });

  await withRead(dagStore, async (read: Read) => {
    const readClientGroup1 = await getClientGroup('client-group-1', read);
    expect(readClientGroup1).to.deep.equal(clientGroup1);
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

test('clientGroupHasPendingMutations', () => {
  expect(
    clientGroupHasPendingMutations(
      makeClientGroup({
        headHash: fakeHash('f'),
        mutationIDs: {},
        lastServerAckdMutationIDs: {},
      }),
    ),
  ).to.be.false;
  expect(
    clientGroupHasPendingMutations(
      makeClientGroup({
        headHash: fakeHash('f1'),
        mutationIDs: {client1: 1},
        lastServerAckdMutationIDs: {},
      }),
    ),
  ).to.be.true;
  expect(
    clientGroupHasPendingMutations(
      makeClientGroup({
        headHash: fakeHash('f1'),
        mutationIDs: {client1: 1},
        lastServerAckdMutationIDs: {client1: 1},
      }),
    ),
  ).to.be.false;
  expect(
    clientGroupHasPendingMutations(
      makeClientGroup({
        headHash: fakeHash('f1'),
        mutationIDs: {client1: 0},
        lastServerAckdMutationIDs: {},
      }),
    ),
  ).to.be.false;
  expect(
    clientGroupHasPendingMutations(
      makeClientGroup({
        headHash: fakeHash('f1'),
        mutationIDs: {client1: 1, client2: 2},
        lastServerAckdMutationIDs: {client1: 1, client2: 1},
      }),
    ),
  ).to.be.true;
  expect(
    clientGroupHasPendingMutations(
      makeClientGroup({
        headHash: fakeHash('f1'),
        mutationIDs: {client1: 0, client2: 2},
        lastServerAckdMutationIDs: {client2: 2},
      }),
    ),
  ).to.be.false;
});

test('Disable Client Group', async () => {
  const dagStore = new TestStore();
  const clientGroup2 = makeClientGroup({
    headHash: headClientGroup2Hash,
  });
  const clientGroupMap1 = makeClientGroupMap({
    'client-group-1': {
      headHash: headClientGroup1Hash,
    },
    'client-group-2': clientGroup2,
  });

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await setClientGroups(clientGroupMap1, write);
    await write.commit();
  });

  async function testDisabledState(tx: Read) {
    const readClientGroup1 = await getClientGroup('client-group-1', tx);
    expect(readClientGroup1?.disabled).true;
    const readClientGroup2 = await getClientGroup('client-group-2', tx);
    expect(readClientGroup2?.disabled).false;

    const readClientGroupMap = await getClientGroups(tx);
    expect(readClientGroupMap.get('client-group-1')?.disabled).true;
    expect(readClientGroupMap.get('client-group-2')?.disabled).false;
  }

  await withWriteNoImplicitCommit(dagStore, async (write: Write) => {
    await disableClientGroup('client-group-1', write);
    await testDisabledState(write);
    await write.commit();
  });

  await withRead(dagStore, testDisabledState);
});
