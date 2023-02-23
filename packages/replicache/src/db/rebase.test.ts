import {expect} from '@esm-bundle/chai';
import {LogContext} from '@rocicorp/logger';
import sinon from 'sinon';
import * as dag from '../dag/mod.js';
import * as db from './mod.js';
import {ChainBuilder} from './test-helpers.js';
import type {WriteTransaction} from '../transactions.js';
import {rebaseMutationAndCommit, rebaseMutationAndPutCommit} from './rebase.js';
import {SYNC_HEAD_NAME} from '../sync/sync-head-name.js';
import {BTreeRead} from '../btree/read.js';
import type {Hash} from '../hash.js';
import {
  assertLocalCommitDD31,
  commitIsLocal,
  commitIsLocalDD31,
} from './commit.js';
import {withRead, withWrite} from '../with-transactions.js';

teardown(() => {
  sinon.restore();
});

async function createMutationSequenceFixture() {
  const clientID = 'test_client_id';
  const store = new dag.TestStore();
  const b = new ChainBuilder(store);
  await b.addGenesis(clientID);
  await b.addSnapshot([['foo', 'bar']], clientID);
  await b.addLocal(clientID);
  const localCommit1 = b.chain[b.chain.length - 1] as db.Commit<db.LocalMeta>;
  await b.addLocal(clientID);
  const localCommit2 = b.chain[b.chain.length - 1] as db.Commit<db.LocalMeta>;
  const syncChain = await b.addSyncSnapshot(1, clientID);
  const syncSnapshotCommit = syncChain[0] as db.Commit<db.SnapshotMetaSDD>;

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
      rebasedCommit: db.Commit<db.Meta>,
      btreeRead: BTreeRead,
    ) => {
      expect(commitIsLocal(rebasedCommit)).to.be.true;
      if (commitIsLocal(rebasedCommit)) {
        const rebasedCommitLocalMeta: db.LocalMeta = rebasedCommit.meta;
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
          const rebasedCommitLocalMeta: db.LocalMetaDD31 = rebasedCommit.meta;
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
      rebasedCommit: db.Commit<db.Meta>,
      btreeRead: BTreeRead,
      expectedBasis: Hash,
    ) => {
      expect(commitIsLocal(rebasedCommit)).to.be.true;
      if (commitIsLocal(rebasedCommit)) {
        const rebasedCommitLocalMeta: db.LocalMeta = rebasedCommit.meta;
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
          const rebasedCommitLocalMeta: db.LocalMetaDD31 = rebasedCommit.meta;
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
  const consoleErrorStub = sinon.stub(console, 'error');
  const clientID = 'test_client_id';
  const store = new dag.TestStore();
  const b = new ChainBuilder(store);
  await b.addGenesis(clientID);
  await b.addSnapshot([['foo', 'bar']], clientID);
  await b.addLocal(clientID);
  const localCommit = b.chain[b.chain.length - 1] as db.Commit<db.LocalMeta>;
  const syncChain = await b.addSyncSnapshot(1, clientID);
  const syncSnapshotCommit = syncChain[0] as db.Commit<db.SnapshotMeta>;

  const fixture = {
    clientID,
    store,
    localCommit,
    syncSnapshotCommit,
    mutators: {},
    expectRebasedCommit: async (
      rebasedCommit: db.Commit<db.Meta>,
      btreeRead: BTreeRead,
    ) => {
      expect(commitIsLocal(rebasedCommit)).to.be.true;
      if (commitIsLocal(rebasedCommit)) {
        const rebasedCommitLocalMeta: db.LocalMeta = rebasedCommit.meta;
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
          const rebasedCommitLocalMeta: db.LocalMetaDD31 = rebasedCommit.meta;
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

suite('rebaseMutationAndCommit', () => {
  test('with sequence of mutations', async () => {
    const fixture = await createMutationSequenceFixture();
    const hashOfRebasedLocalCommit1 = await withWrite(
      fixture.store,
      async write => {
        return await rebaseMutationAndCommit(
          fixture.localCommit1,
          write,
          fixture.syncSnapshotCommit.chunk.hash,
          SYNC_HEAD_NAME,
          fixture.mutators,
          new LogContext(),
          fixture.clientID,
        );
      },
    );
    expect(fixture.testMutator1CallCount).to.equal(1);
    expect(fixture.testMutator2CallCount).to.equal(0);
    await withRead(fixture.store, async read => {
      const [, rebasedLocalCommit1, btreeRead] =
        await db.readCommitForBTreeRead(db.whenceHead(SYNC_HEAD_NAME), read);
      expect(hashOfRebasedLocalCommit1).to.equal(
        rebasedLocalCommit1.chunk.hash,
      );
      await fixture.expectRebasedCommit1(rebasedLocalCommit1, btreeRead);
    });
    const hashOfRebasedLocalCommit2 = await withWrite(
      fixture.store,
      async write => {
        return await rebaseMutationAndCommit(
          fixture.localCommit2,
          write,
          hashOfRebasedLocalCommit1,
          SYNC_HEAD_NAME,
          fixture.mutators,
          new LogContext(),
          fixture.clientID,
        );
      },
    );
    expect(fixture.testMutator1CallCount).to.equal(1);
    expect(fixture.testMutator2CallCount).to.equal(1);
    await withRead(fixture.store, async read => {
      const [, rebasedLocalCommit2, btreeRead] =
        await db.readCommitForBTreeRead(db.whenceHead(SYNC_HEAD_NAME), read);
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
    const hashOfRebasedLocalCommit = await withWrite(
      fixture.store,
      async write => {
        return await rebaseMutationAndCommit(
          fixture.localCommit,
          write,
          fixture.syncSnapshotCommit.chunk.hash,
          SYNC_HEAD_NAME,
          {}, // empty
          new LogContext(),
          fixture.clientID,
        );
      },
    );
    await withRead(fixture.store, async read => {
      const [, rebasedLocalCommit, btreeRead] = await db.readCommitForBTreeRead(
        db.whenceHead(SYNC_HEAD_NAME),
        read,
      );
      expect(hashOfRebasedLocalCommit).to.equal(rebasedLocalCommit.chunk.hash);
      await fixture.expectRebasedCommit(rebasedLocalCommit, btreeRead);
      await fixture.expectMissingMutatorErrorLog();
    });
  });

  test("throws error if DD31 and mutationClientID does not match mutation's clientID", async () => {
    await testThrowsErrorOnClientIDMismatch('commit', true);
  });

  test("throws error if SDD and mutationClientID does not match mutation's clientID", async () => {
    await testThrowsErrorOnClientIDMismatch('commit', false);
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
      async write => {
        const commit = await rebaseMutationAndPutCommit(
          fixture.localCommit1,
          write,
          fixture.syncSnapshotCommit.chunk.hash,
          fixture.mutators,
          new LogContext(),
          fixture.clientID,
        );
        await fixture.expectRebasedCommit1(
          commit,
          new BTreeRead(write, commit.valueHash),
        );
        await write.setHead(TEST_HEAD_NAME, commit.chunk.hash);
        await write.commit();
        return commit.chunk.hash;
      },
    );
    expect(fixture.testMutator1CallCount).to.equal(1);
    expect(fixture.testMutator2CallCount).to.equal(0);
    await withRead(fixture.store, async read => {
      const [, rebasedLocalCommit1, btreeRead] =
        await db.readCommitForBTreeRead(db.whenceHead(TEST_HEAD_NAME), read);
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
        );
        await fixture.expectRebasedCommit2(
          commit,
          new BTreeRead(write, commit.valueHash),
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
      const [, rebasedLocalCommit2, btreeRead] =
        await db.readCommitForBTreeRead(db.whenceHead(TEST_HEAD_NAME), read);
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
        );
        await fixture.expectRebasedCommit(
          commit,
          new BTreeRead(write, commit.valueHash),
        );
        await write.setHead(TEST_HEAD_NAME, commit.chunk.hash);
        await write.commit();
        return commit.chunk.hash;
      },
    );
    await withRead(fixture.store, async read => {
      const [, rebasedLocalCommit, btreeRead] = await db.readCommitForBTreeRead(
        db.whenceHead(TEST_HEAD_NAME),
        read,
      );
      expect(hashOfRebasedLocalCommit).to.equal(rebasedLocalCommit.chunk.hash);
      await fixture.expectRebasedCommit(rebasedLocalCommit, btreeRead);
      await fixture.expectMissingMutatorErrorLog();
    });
  });

  test("throws error if DD31 and mutationClientID does not match mutation's clientID", async () => {
    await testThrowsErrorOnClientIDMismatch('putCommit', true);
  });

  test("throws error if SDD and mutationClientID does not match mutation's clientID", async () => {
    await testThrowsErrorOnClientIDMismatch('putCommit', false);
  });

  test("throws error if next mutation id for mutationClientID does not match mutation's mutationID", async () => {
    await testThrowsErrorOnMutationIDMismatch('putCommit');
  });
});

async function testThrowsErrorOnClientIDMismatch(
  variant: 'commit' | 'putCommit',
  dd31: boolean,
) {
  const clientID = 'test_client_id';
  const store = new dag.TestStore();
  const b = new ChainBuilder(store, undefined, dd31);
  await b.addGenesis(clientID);
  await b.addSnapshot([['foo', 'bar']], clientID);
  await b.addLocal(clientID);
  const localCommit = b.chain[b.chain.length - 1] as db.Commit<db.LocalMetaSDD>;
  const syncChain = await b.addSyncSnapshot(1, clientID);
  const syncSnapshotCommit = syncChain[0] as db.Commit<db.SnapshotMetaSDD>;

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
          );
    } catch (expected) {
      expect(dd31).to.be.true;
      return;
    }
    expect(dd31).to.be.false;
  });
  expect(testMutatorCallCount).to.equal(dd31 ? 0 : 1);
}

async function testThrowsErrorOnMutationIDMismatch(
  variant: 'commit' | 'putCommit',
) {
  const clientID = 'test_client_id';
  const store = new dag.TestStore();
  const b = new ChainBuilder(store);
  await b.addGenesis(clientID);
  await b.addSnapshot([['foo', 'bar']], clientID);
  await b.addLocal(clientID);
  const localCommit1 = b.chain[
    b.chain.length - 1
  ] as db.Commit<db.LocalMetaSDD>;
  await b.addLocal(clientID);
  const localCommit2 = b.chain[
    b.chain.length - 1
  ] as db.Commit<db.LocalMetaSDD>;
  const syncChain = await b.addSyncSnapshot(1, clientID);
  const syncSnapshotCommit = syncChain[0] as db.Commit<db.SnapshotMetaSDD>;

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
          )
        : await rebaseMutationAndPutCommit(
            localCommit2,
            write,
            syncSnapshotCommit.chunk.hash,
            mutators,
            new LogContext(),
            clientID,
          );
    } catch (e) {
      expectedError = e;
    }
    expect(String(expectedError)).contains('Inconsistent mutation ID');
  });
  expect(testMutator1CallCount).to.equal(0);
  expect(testMutator2CallCount).to.equal(0);
}
