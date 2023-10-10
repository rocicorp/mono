import {LogContext} from '@rocicorp/logger';
import {expect} from 'chai';
import sinon from 'sinon';
import {BTreeRead} from '../btree/read.js';
import type {Read} from '../dag/store.js';
import {TestStore} from '../dag/test-store.js';
import {FormatVersion} from '../format-version.js';
import type {Hash} from '../hash.js';
import {SYNC_HEAD_NAME} from '../sync/sync-head-name.js';
import type {WriteTransaction} from '../transactions.js';
import {withRead, withWrite} from '../with-transactions.js';
import {
  Commit,
  LocalMeta,
  LocalMetaDD31,
  LocalMetaSDD,
  Meta,
  SnapshotMetaDD31,
  SnapshotMetaSDD,
  assertLocalCommitDD31,
  commitFromHead,
  commitIsLocal,
  commitIsLocalDD31,
} from './commit.js';
import {rebaseMutationAndCommit, rebaseMutationAndPutCommit} from './rebase.js';
import {ChainBuilder} from './test-helpers.js';

teardown(() => {
  sinon.restore();
});

async function createMutationSequenceFixture() {
  const formatVersion: FormatVersion = FormatVersion.Latest;
  const clientID = 'test_client_id';
  const store = new TestStore();
  const b = new ChainBuilder(store, undefined, formatVersion);
  await b.addGenesis(clientID);
  await b.addSnapshot([['foo', 'bar']], clientID);
  await b.addLocal(clientID);
  const localCommit1 = b.chain[b.chain.length - 1] as Commit<LocalMetaDD31>;
  await b.addLocal(clientID);
  const localCommit2 = b.chain[b.chain.length - 1] as Commit<LocalMetaDD31>;
  const syncChain = await b.addSyncSnapshot(1, clientID);
  const syncSnapshotCommit = syncChain[0] as Commit<SnapshotMetaDD31>;

  const testMutator1 = async (tx: WriteTransaction, args?: unknown) => {
    await tx.put('whiz', 'bang');
    expect(args).to.deep.equal(localCommit1.meta.mutatorArgsJSON);
    fixture.testMutator1CallCount++;
  };
  const testMutator2 = async (tx: WriteTransaction, args?: unknown) => {
    await tx.put('fuzzy', 'wuzzy');
    expect(args).to.deep.equal(localCommit2.meta.mutatorArgsJSON);
    fixture.testMutator2CallCount++;
  };

  const fixture = {
    formatVersion,
    clientID,
    store,
    localCommit1,
    localCommit2,
    testMutator1CallCount: 0,
    testMutator2CallCount: 0,
    syncSnapshotCommit,
    mutators: {
      [localCommit1.meta.mutatorName]: testMutator1,
      [localCommit2.meta.mutatorName]: testMutator2,
    },
    expectRebasedCommit1: async (
      rebasedCommit: Commit<Meta>,
      btreeRead: BTreeRead,
    ) => {
      expect(commitIsLocal(rebasedCommit)).to.be.true;
      if (commitIsLocal(rebasedCommit)) {
        const rebasedCommitLocalMeta: LocalMeta = rebasedCommit.meta;
        expect(rebasedCommitLocalMeta.basisHash).to.equal(
          syncSnapshotCommit.chunk.hash,
        );
        expect(rebasedCommitLocalMeta.mutationID).to.equal(
          localCommit1.meta.mutationID,
        );
        expect(rebasedCommitLocalMeta.mutatorName).to.equal(
          localCommit1.meta.mutatorName,
        );
        expect(rebasedCommitLocalMeta.originalHash).to.equal(
          localCommit1.chunk.hash,
        );
        expect(rebasedCommitLocalMeta.timestamp).to.equal(
          localCommit1.meta.timestamp,
        );

        if (commitIsLocalDD31(rebasedCommit)) {
          assertLocalCommitDD31(localCommit1);
          const rebasedCommitLocalMeta: LocalMetaDD31 = rebasedCommit.meta;
          expect(rebasedCommitLocalMeta.clientID).to.equal(
            localCommit1.meta.clientID,
          );
        }
      }
      expect(await btreeRead.get('fuzzy')).to.be.undefined;
      expect(await btreeRead.get('foo')).to.equal('bar');
      expect(await btreeRead.get('whiz')).to.equal('bang');
    },
    expectRebasedCommit2: async (
      rebasedCommit: Commit<Meta>,
      btreeRead: BTreeRead,
      expectedBasis: Hash,
    ) => {
      expect(commitIsLocal(rebasedCommit)).to.be.true;
      if (commitIsLocal(rebasedCommit)) {
        const rebasedCommitLocalMeta: LocalMeta = rebasedCommit.meta;
        expect(rebasedCommitLocalMeta.basisHash).to.equal(expectedBasis);
        expect(rebasedCommitLocalMeta.mutationID).to.equal(
          localCommit2.meta.mutationID,
        );
        expect(rebasedCommitLocalMeta.mutatorName).to.equal(
          localCommit2.meta.mutatorName,
        );
        expect(rebasedCommitLocalMeta.originalHash).to.equal(
          localCommit2.chunk.hash,
        );
        expect(rebasedCommitLocalMeta.timestamp).to.equal(
          localCommit2.meta.timestamp,
        );
        if (commitIsLocalDD31(rebasedCommit)) {
          assertLocalCommitDD31(localCommit2);
          const rebasedCommitLocalMeta: LocalMetaDD31 = rebasedCommit.meta;
          expect(rebasedCommitLocalMeta.clientID).to.equal(
            localCommit2.meta.clientID,
          );
        }
      }
      expect(await btreeRead.get('fuzzy')).to.equal('wuzzy');
      expect(await btreeRead.get('foo')).to.equal('bar');
      expect(await btreeRead.get('whiz')).to.equal('bang');
    },
  };
  return fixture;
}

async function createMissingMutatorFixture() {
  const formatVersion: FormatVersion = FormatVersion.Latest;
  const consoleErrorStub = sinon.stub(console, 'error');
  const clientID = 'test_client_id';
  const store = new TestStore();
  const b = new ChainBuilder(store, undefined, formatVersion);
  await b.addGenesis(clientID);
  await b.addSnapshot([['foo', 'bar']], clientID);
  await b.addLocal(clientID);
  const localCommit = b.chain[b.chain.length - 1] as Commit<LocalMetaDD31>;
  const syncChain = await b.addSyncSnapshot(1, clientID);
  const syncSnapshotCommit = syncChain[0] as Commit<SnapshotMetaDD31>;

  const fixture = {
    formatVersion,
    clientID,
    store,
    localCommit,
    syncSnapshotCommit,
    mutators: {},
    expectRebasedCommit: async (
      rebasedCommit: Commit<Meta>,
      btreeRead: BTreeRead,
    ) => {
      expect(commitIsLocal(rebasedCommit)).to.be.true;
      if (commitIsLocal(rebasedCommit)) {
        const rebasedCommitLocalMeta: LocalMeta = rebasedCommit.meta;
        expect(rebasedCommitLocalMeta.basisHash).to.equal(
          syncSnapshotCommit.chunk.hash,
        );
        expect(rebasedCommitLocalMeta.mutationID).to.equal(
          localCommit.meta.mutationID,
        );
        expect(rebasedCommitLocalMeta.mutatorName).to.equal(
          localCommit.meta.mutatorName,
        );
        expect(rebasedCommitLocalMeta.originalHash).to.equal(
          localCommit.chunk.hash,
        );
        expect(rebasedCommitLocalMeta.timestamp).to.equal(
          localCommit.meta.timestamp,
        );
        if (commitIsLocalDD31(rebasedCommit)) {
          assertLocalCommitDD31(localCommit);
          const rebasedCommitLocalMeta: LocalMetaDD31 = rebasedCommit.meta;
          expect(rebasedCommitLocalMeta.clientID).to.equal(
            localCommit.meta.clientID,
          );
        }
      }
      expect(await btreeRead.get('foo')).to.equal('bar');
    },
    expectMissingMutatorErrorLog: () => {
      expect(consoleErrorStub.callCount).to.equal(1);
      const {args} = consoleErrorStub.getCall(0);
      expect(args[0]).to.equal(
        `Cannot rebase unknown mutator ${localCommit.meta.mutatorName}`,
      );
    },
  };
  return fixture;
}

async function commitAndBTree(
  name = SYNC_HEAD_NAME,
  read: Read,
  formatVersion: FormatVersion,
): Promise<[Commit<Meta>, BTreeRead]> {
  const commit = await commitFromHead(name, read);
  const btreeRead = new BTreeRead(read, formatVersion, commit.valueHash);
  return [commit, btreeRead];
}

suite('rebaseMutationAndCommit', () => {
  test('with sequence of mutations', async () => {
    const fixture = await createMutationSequenceFixture();
    const hashOfRebasedLocalCommit1 = await withWrite(fixture.store, write =>
      rebaseMutationAndCommit(
        fixture.localCommit1,
        write,
        fixture.syncSnapshotCommit.chunk.hash,
        SYNC_HEAD_NAME,
        fixture.mutators,
        new LogContext(),
        fixture.clientID,
        fixture.formatVersion,
      ),
    );
    expect(fixture.testMutator1CallCount).to.equal(1);
    expect(fixture.testMutator2CallCount).to.equal(0);
    await withRead(fixture.store, async read => {
      const [rebasedLocalCommit1, btreeRead] = await commitAndBTree(
        SYNC_HEAD_NAME,
        read,
        fixture.formatVersion,
      );
      expect(hashOfRebasedLocalCommit1).to.equal(
        rebasedLocalCommit1.chunk.hash,
      );
      await fixture.expectRebasedCommit1(rebasedLocalCommit1, btreeRead);
    });
    const hashOfRebasedLocalCommit2 = await withWrite(fixture.store, write =>
      rebaseMutationAndCommit(
        fixture.localCommit2,
        write,
        hashOfRebasedLocalCommit1,
        SYNC_HEAD_NAME,
        fixture.mutators,
        new LogContext(),
        fixture.clientID,
        fixture.formatVersion,
      ),
    );
    expect(fixture.testMutator1CallCount).to.equal(1);
    expect(fixture.testMutator2CallCount).to.equal(1);
    await withRead(fixture.store, async read => {
      const [rebasedLocalCommit2, btreeRead] = await commitAndBTree(
        SYNC_HEAD_NAME,
        read,
        fixture.formatVersion,
      );
      expect(hashOfRebasedLocalCommit2).to.equal(
        rebasedLocalCommit2.chunk.hash,
      );
      await fixture.expectRebasedCommit2(
        rebasedLocalCommit2,
        btreeRead,
        hashOfRebasedLocalCommit1,
      );
    });
  });

  test("with missing mutator, still rebases but doesn't modify btree", async () => {
    const fixture = await createMissingMutatorFixture();
    const hashOfRebasedLocalCommit = await withWrite(fixture.store, write =>
      rebaseMutationAndCommit(
        fixture.localCommit,
        write,
        fixture.syncSnapshotCommit.chunk.hash,
        SYNC_HEAD_NAME,
        {}, // empty
        new LogContext(),
        fixture.clientID,
        fixture.formatVersion,
      ),
    );
    await withRead(fixture.store, async read => {
      const [rebasedLocalCommit, btreeRead] = await commitAndBTree(
        SYNC_HEAD_NAME,
        read,
        fixture.formatVersion,
      );
      expect(hashOfRebasedLocalCommit).to.equal(rebasedLocalCommit.chunk.hash);
      await fixture.expectRebasedCommit(rebasedLocalCommit, btreeRead);
      await fixture.expectMissingMutatorErrorLog();
    });
  });

  test("throws error if DD31 and mutationClientID does not match mutation's clientID", async () => {
    await testThrowsErrorOnClientIDMismatch('commit', FormatVersion.Latest);
  });

  test("throws error if SDD and mutationClientID does not match mutation's clientID", async () => {
    await testThrowsErrorOnClientIDMismatch('commit', FormatVersion.SDD);
  });

  test("throws error if next mutation id for mutationClientID does not match mutation's mutationID", async () => {
    await testThrowsErrorOnMutationIDMismatch('commit');
  });
});

suite('rebaseMutationAndPutCommit', () => {
  test('with sequence of mutations', async () => {
    const TEST_HEAD_NAME = 'test-head';
    const fixture = await createMutationSequenceFixture();
    const hashOfRebasedLocalCommit1 = await withWrite(
      fixture.store,
      async (write): Promise<Hash> => {
        const commit = await rebaseMutationAndPutCommit(
          fixture.localCommit1,
          write,
          fixture.syncSnapshotCommit.chunk.hash,
          fixture.mutators,
          new LogContext(),
          fixture.clientID,
          fixture.formatVersion,
        );
        await fixture.expectRebasedCommit1(
          commit,
          new BTreeRead(write, fixture.formatVersion, commit.valueHash),
        );
        await write.setHead(TEST_HEAD_NAME, commit.chunk.hash);
        await write.commit();
        return commit.chunk.hash;
      },
    );
    expect(fixture.testMutator1CallCount).to.equal(1);
    expect(fixture.testMutator2CallCount).to.equal(0);
    await withRead(fixture.store, async read => {
      const [rebasedLocalCommit1, btreeRead] = await commitAndBTree(
        TEST_HEAD_NAME,
        read,
        fixture.formatVersion,
      );
      expect(hashOfRebasedLocalCommit1).to.equal(
        rebasedLocalCommit1.chunk.hash,
      );
      await fixture.expectRebasedCommit1(rebasedLocalCommit1, btreeRead);
    });
    const hashOfRebasedLocalCommit2 = await withWrite(
      fixture.store,
      async write => {
        const commit = await rebaseMutationAndPutCommit(
          fixture.localCommit2,
          write,
          hashOfRebasedLocalCommit1,
          fixture.mutators,
          new LogContext(),
          fixture.clientID,
          fixture.formatVersion,
        );
        await fixture.expectRebasedCommit2(
          commit,
          new BTreeRead(write, fixture.formatVersion, commit.valueHash),
          hashOfRebasedLocalCommit1,
        );
        await write.setHead(TEST_HEAD_NAME, commit.chunk.hash);
        await write.commit();
        return commit.chunk.hash;
      },
    );
    expect(fixture.testMutator1CallCount).to.equal(1);
    expect(fixture.testMutator2CallCount).to.equal(1);
    await withRead(fixture.store, async read => {
      const [rebasedLocalCommit2, btreeRead] = await commitAndBTree(
        TEST_HEAD_NAME,
        read,
        fixture.formatVersion,
      );
      expect(hashOfRebasedLocalCommit2).to.equal(
        rebasedLocalCommit2.chunk.hash,
      );
      await fixture.expectRebasedCommit2(
        rebasedLocalCommit2,
        btreeRead,
        hashOfRebasedLocalCommit1,
      );
    });
  });

  test("with missing mutator, still rebases but doesn't modify btree", async () => {
    const TEST_HEAD_NAME = 'test-head';
    const fixture = await createMissingMutatorFixture();
    const hashOfRebasedLocalCommit = await withWrite(
      fixture.store,
      async write => {
        const commit = await rebaseMutationAndPutCommit(
          fixture.localCommit,
          write,
          fixture.syncSnapshotCommit.chunk.hash,
          {}, // empty
          new LogContext(),
          fixture.clientID,
          fixture.formatVersion,
        );
        await fixture.expectRebasedCommit(
          commit,
          new BTreeRead(write, fixture.formatVersion, commit.valueHash),
        );
        await write.setHead(TEST_HEAD_NAME, commit.chunk.hash);
        await write.commit();
        return commit.chunk.hash;
      },
    );
    await withRead(fixture.store, async read => {
      const [rebasedLocalCommit, btreeRead] = await commitAndBTree(
        TEST_HEAD_NAME,
        read,
        fixture.formatVersion,
      );
      expect(hashOfRebasedLocalCommit).to.equal(rebasedLocalCommit.chunk.hash);
      await fixture.expectRebasedCommit(rebasedLocalCommit, btreeRead);
      await fixture.expectMissingMutatorErrorLog();
    });
  });

  test("throws error if DD31 and mutationClientID does not match mutation's clientID", async () => {
    await testThrowsErrorOnClientIDMismatch('putCommit', FormatVersion.Latest);
  });

  test("throws error if SDD and mutationClientID does not match mutation's clientID", async () => {
    await testThrowsErrorOnClientIDMismatch('putCommit', FormatVersion.SDD);
  });

  test("throws error if next mutation id for mutationClientID does not match mutation's mutationID", async () => {
    await testThrowsErrorOnMutationIDMismatch('putCommit');
  });
});

async function testThrowsErrorOnClientIDMismatch(
  variant: 'commit' | 'putCommit',
  formatVersion: FormatVersion,
) {
  const clientID = 'test_client_id';
  const store = new TestStore();
  const b = new ChainBuilder(store, undefined, formatVersion);
  await b.addGenesis(clientID);
  await b.addSnapshot([['foo', 'bar']], clientID);
  await b.addLocal(clientID);
  const localCommit = b.chain[b.chain.length - 1] as Commit<LocalMetaSDD>;
  const syncChain = await b.addSyncSnapshot(1, clientID);
  const syncSnapshotCommit = syncChain[0] as Commit<SnapshotMetaSDD>;

  let testMutatorCallCount = 0;
  const testMutator = async (tx: WriteTransaction, args?: unknown) => {
    await tx.put('whiz', 'bang');
    expect(args).to.deep.equal(localCommit.meta.mutatorArgsJSON);
    testMutatorCallCount++;
  };
  await withWrite(store, async write => {
    try {
      variant === 'commit'
        ? await rebaseMutationAndCommit(
            localCommit,
            write,
            syncSnapshotCommit.chunk.hash,
            SYNC_HEAD_NAME,
            {
              [localCommit.meta.mutatorName]: testMutator,
            },
            new LogContext(),
            'wrong_client_id',
            formatVersion,
          )
        : await rebaseMutationAndPutCommit(
            localCommit,
            write,
            syncSnapshotCommit.chunk.hash,
            {
              [localCommit.meta.mutatorName]: testMutator,
            },
            new LogContext(),
            'wrong_client_id',
            formatVersion,
          );
    } catch (expected) {
      expect(formatVersion).to.be.greaterThanOrEqual(FormatVersion.DD31);
      return;
    }
    expect(formatVersion).to.be.lessThanOrEqual(FormatVersion.SDD);
  });
  expect(testMutatorCallCount).to.equal(
    formatVersion >= FormatVersion.DD31 ? 0 : 1,
  );
}

async function testThrowsErrorOnMutationIDMismatch(
  variant: 'commit' | 'putCommit',
) {
  const formatVersion = FormatVersion.SDD;
  const clientID = 'test_client_id';
  const store = new TestStore();
  const b = new ChainBuilder(store);
  await b.addGenesis(clientID);
  await b.addSnapshot([['foo', 'bar']], clientID);
  await b.addLocal(clientID);
  const localCommit1 = b.chain[b.chain.length - 1] as Commit<LocalMetaSDD>;
  await b.addLocal(clientID);
  const localCommit2 = b.chain[b.chain.length - 1] as Commit<LocalMetaSDD>;
  const syncChain = await b.addSyncSnapshot(1, clientID);
  const syncSnapshotCommit = syncChain[0] as Commit<SnapshotMetaSDD>;

  let testMutator1CallCount = 0;
  const testMutator1 = async (tx: WriteTransaction, args?: unknown) => {
    await tx.put('whiz', 'bang');
    expect(args).to.deep.equal(localCommit1.meta.mutatorArgsJSON);
    testMutator1CallCount++;
  };
  let testMutator2CallCount = 0;
  const testMutator2 = async (tx: WriteTransaction, args?: unknown) => {
    await tx.put('fuzzy', 'wuzzy');
    expect(args).to.deep.equal(localCommit2.meta.mutatorArgsJSON);
    testMutator2CallCount++;
  };
  const mutators = {
    [localCommit1.meta.mutatorName]: testMutator1,
    [localCommit2.meta.mutatorName]: testMutator2,
  };
  await withWrite(store, async write => {
    let expectedError;
    try {
      variant === 'commit'
        ? await rebaseMutationAndCommit(
            localCommit2,
            write,
            syncSnapshotCommit.chunk.hash,
            SYNC_HEAD_NAME,
            mutators,
            new LogContext(),
            clientID,
            formatVersion,
          )
        : await rebaseMutationAndPutCommit(
            localCommit2,
            write,
            syncSnapshotCommit.chunk.hash,
            mutators,
            new LogContext(),
            clientID,
            formatVersion,
          );
    } catch (e) {
      expectedError = e;
    }
    expect(String(expectedError)).contains('Inconsistent mutation ID');
  });
  expect(testMutator1CallCount).to.equal(0);
  expect(testMutator2CallCount).to.equal(0);
}
