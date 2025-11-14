import {LogContext} from '@rocicorp/logger';
import {afterEach, describe, expect, test, vi} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import {BTreeRead} from '../btree/read.ts';
import type {Read} from '../dag/store.ts';
import {TestStore} from '../dag/test-store.ts';
import * as FormatVersion from '../format-version-enum.ts';
import type {Hash} from '../hash.ts';
import {SYNC_HEAD_NAME} from '../sync/sync-head-name.ts';
import type {WriteTransaction} from '../transactions.ts';
import {withRead, withWriteNoImplicitCommit} from '../with-transactions.ts';
import {
  type Commit,
  type LocalMeta,
  type LocalMetaDD31,
  type Meta,
  type SnapshotMetaDD31,
  assertLocalCommitDD31,
  commitFromHead,
  commitIsLocal,
  commitIsLocalDD31,
} from './commit.ts';
import {rebaseMutationAndCommit, rebaseMutationAndPutCommit} from './rebase.ts';
import {ChainBuilder} from './test-helpers.ts';

type FormatVersion = Enum<typeof FormatVersion>;

afterEach(() => {
  vi.restoreAllMocks();
});

async function createMutationSequenceFixture() {
  const formatVersion = FormatVersion.Latest;
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
    await tx.set('whiz', 'bang');
    expect(args).toEqual(localCommit1.meta.mutatorArgsJSON);
    fixture.testMutator1CallCount++;
  };
  const testMutator2 = async (tx: WriteTransaction, args?: unknown) => {
    await tx.set('fuzzy', 'wuzzy');
    expect(args).toEqual(localCommit2.meta.mutatorArgsJSON);
    fixture.testMutator2CallCount++;
  };

  const fixture = {
    formatVersion: formatVersion as FormatVersion,
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
      expect(commitIsLocal(rebasedCommit)).toBe(true);
      if (commitIsLocal(rebasedCommit)) {
        const rebasedCommitLocalMeta: LocalMeta = rebasedCommit.meta;
        expect(rebasedCommitLocalMeta.basisHash).toBe(
          syncSnapshotCommit.chunk.hash,
        );
        expect(rebasedCommitLocalMeta.mutationID).toBe(
          localCommit1.meta.mutationID,
        );
        expect(rebasedCommitLocalMeta.mutatorName).toBe(
          localCommit1.meta.mutatorName,
        );
        expect(rebasedCommitLocalMeta.originalHash).toBe(
          localCommit1.chunk.hash,
        );
        expect(rebasedCommitLocalMeta.timestamp).toBe(
          localCommit1.meta.timestamp,
        );

        if (commitIsLocalDD31(rebasedCommit)) {
          assertLocalCommitDD31(localCommit1);
          const rebasedCommitLocalMeta: LocalMetaDD31 = rebasedCommit.meta;
          expect(rebasedCommitLocalMeta.clientID).toBe(
            localCommit1.meta.clientID,
          );
        }
      }
      expect(await btreeRead.get('fuzzy')).toBeUndefined();
      expect(await btreeRead.get('foo')).toBe('bar');
      expect(await btreeRead.get('whiz')).toBe('bang');
    },
    expectRebasedCommit2: async (
      rebasedCommit: Commit<Meta>,
      btreeRead: BTreeRead,
      expectedBasis: Hash,
    ) => {
      expect(commitIsLocal(rebasedCommit)).toBe(true);
      if (commitIsLocal(rebasedCommit)) {
        const rebasedCommitLocalMeta: LocalMeta = rebasedCommit.meta;
        expect(rebasedCommitLocalMeta.basisHash).toBe(expectedBasis);
        expect(rebasedCommitLocalMeta.mutationID).toBe(
          localCommit2.meta.mutationID,
        );
        expect(rebasedCommitLocalMeta.mutatorName).toBe(
          localCommit2.meta.mutatorName,
        );
        expect(rebasedCommitLocalMeta.originalHash).toBe(
          localCommit2.chunk.hash,
        );
        expect(rebasedCommitLocalMeta.timestamp).toBe(
          localCommit2.meta.timestamp,
        );
        if (commitIsLocalDD31(rebasedCommit)) {
          assertLocalCommitDD31(localCommit2);
          const rebasedCommitLocalMeta: LocalMetaDD31 = rebasedCommit.meta;
          expect(rebasedCommitLocalMeta.clientID).toBe(
            localCommit2.meta.clientID,
          );
        }
      }
      expect(await btreeRead.get('fuzzy')).toBe('wuzzy');
      expect(await btreeRead.get('foo')).toBe('bar');
      expect(await btreeRead.get('whiz')).toBe('bang');
    },
  };
  return fixture;
}

async function createMissingMutatorFixture() {
  const formatVersion = FormatVersion.Latest;
  const consoleErrorStub = vi.spyOn(console, 'error');
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
    formatVersion: formatVersion as FormatVersion,
    clientID,
    store,
    localCommit,
    syncSnapshotCommit,
    mutators: {},
    expectRebasedCommit: async (
      rebasedCommit: Commit<Meta>,
      btreeRead: BTreeRead,
    ) => {
      expect(commitIsLocal(rebasedCommit)).toBe(true);
      if (commitIsLocal(rebasedCommit)) {
        const rebasedCommitLocalMeta: LocalMeta = rebasedCommit.meta;
        expect(rebasedCommitLocalMeta.basisHash).toBe(
          syncSnapshotCommit.chunk.hash,
        );
        expect(rebasedCommitLocalMeta.mutationID).toBe(
          localCommit.meta.mutationID,
        );
        expect(rebasedCommitLocalMeta.mutatorName).toBe(
          localCommit.meta.mutatorName,
        );
        expect(rebasedCommitLocalMeta.originalHash).toBe(
          localCommit.chunk.hash,
        );
        expect(rebasedCommitLocalMeta.timestamp).toBe(
          localCommit.meta.timestamp,
        );
        if (commitIsLocalDD31(rebasedCommit)) {
          assertLocalCommitDD31(localCommit);
          const rebasedCommitLocalMeta: LocalMetaDD31 = rebasedCommit.meta;
          expect(rebasedCommitLocalMeta.clientID).toBe(
            localCommit.meta.clientID,
          );
        }
      }
      expect(await btreeRead.get('foo')).toBe('bar');
    },
    expectMissingMutatorErrorLog: () => {
      expect(consoleErrorStub).toBeCalledTimes(1);
      const args = consoleErrorStub.mock.calls[0];
      expect(args[0]).toBe(
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

describe('rebaseMutationAndCommit', () => {
  test('with sequence of mutations', async () => {
    const fixture = await createMutationSequenceFixture();
    const hashOfRebasedLocalCommit1 = await withWriteNoImplicitCommit(
      fixture.store,
      write =>
        rebaseMutationAndCommit(
          fixture.localCommit1,
          write,
          fixture.syncSnapshotCommit.chunk.hash,
          SYNC_HEAD_NAME,
          fixture.mutators,
          new LogContext(),
          fixture.clientID,
          fixture.formatVersion,
          undefined,
        ),
    );
    expect(fixture.testMutator1CallCount).toBe(1);
    expect(fixture.testMutator2CallCount).toBe(0);
    await withRead(fixture.store, async read => {
      const [rebasedLocalCommit1, btreeRead] = await commitAndBTree(
        SYNC_HEAD_NAME,
        read,
        fixture.formatVersion,
      );
      expect(hashOfRebasedLocalCommit1).toBe(rebasedLocalCommit1.chunk.hash);
      await fixture.expectRebasedCommit1(rebasedLocalCommit1, btreeRead);
    });
    const hashOfRebasedLocalCommit2 = await withWriteNoImplicitCommit(
      fixture.store,
      write =>
        rebaseMutationAndCommit(
          fixture.localCommit2,
          write,
          hashOfRebasedLocalCommit1,
          SYNC_HEAD_NAME,
          fixture.mutators,
          new LogContext(),
          fixture.clientID,
          fixture.formatVersion,
          undefined,
        ),
    );
    expect(fixture.testMutator1CallCount).toBe(1);
    expect(fixture.testMutator2CallCount).toBe(1);
    await withRead(fixture.store, async read => {
      const [rebasedLocalCommit2, btreeRead] = await commitAndBTree(
        SYNC_HEAD_NAME,
        read,
        fixture.formatVersion,
      );
      expect(hashOfRebasedLocalCommit2).toBe(rebasedLocalCommit2.chunk.hash);
      await fixture.expectRebasedCommit2(
        rebasedLocalCommit2,
        btreeRead,
        hashOfRebasedLocalCommit1,
      );
    });
  });

  test("with missing mutator, still rebases but doesn't modify btree", async () => {
    const fixture = await createMissingMutatorFixture();
    const hashOfRebasedLocalCommit = await withWriteNoImplicitCommit(
      fixture.store,
      write =>
        rebaseMutationAndCommit(
          fixture.localCommit,
          write,
          fixture.syncSnapshotCommit.chunk.hash,
          SYNC_HEAD_NAME,
          {}, // empty
          new LogContext(),
          fixture.clientID,
          fixture.formatVersion,
          undefined,
        ),
    );
    await withRead(fixture.store, async read => {
      const [rebasedLocalCommit, btreeRead] = await commitAndBTree(
        SYNC_HEAD_NAME,
        read,
        fixture.formatVersion,
      );
      expect(hashOfRebasedLocalCommit).toBe(rebasedLocalCommit.chunk.hash);
      await fixture.expectRebasedCommit(rebasedLocalCommit, btreeRead);
      await fixture.expectMissingMutatorErrorLog();
    });
  });

  test("throws error if DD31 and mutationClientID does not match mutation's clientID", async () => {
    await testThrowsErrorOnClientIDMismatch('commit', FormatVersion.Latest);
  });

  test("throws error if next mutation id for mutationClientID does not match mutation's mutationID", async () => {
    await testThrowsErrorOnMutationIDMismatch('commit');
  });
});

describe('rebaseMutationAndPutCommit', () => {
  test('with sequence of mutations', async () => {
    const TEST_HEAD_NAME = 'test-head';
    const fixture = await createMutationSequenceFixture();
    const hashOfRebasedLocalCommit1 = await withWriteNoImplicitCommit(
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
          undefined,
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
    expect(fixture.testMutator1CallCount).toBe(1);
    expect(fixture.testMutator2CallCount).toBe(0);
    await withRead(fixture.store, async read => {
      const [rebasedLocalCommit1, btreeRead] = await commitAndBTree(
        TEST_HEAD_NAME,
        read,
        fixture.formatVersion,
      );
      expect(hashOfRebasedLocalCommit1).toBe(rebasedLocalCommit1.chunk.hash);
      await fixture.expectRebasedCommit1(rebasedLocalCommit1, btreeRead);
    });
    const hashOfRebasedLocalCommit2 = await withWriteNoImplicitCommit(
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
          undefined,
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
    expect(fixture.testMutator1CallCount).toBe(1);
    expect(fixture.testMutator2CallCount).toBe(1);
    await withRead(fixture.store, async read => {
      const [rebasedLocalCommit2, btreeRead] = await commitAndBTree(
        TEST_HEAD_NAME,
        read,
        fixture.formatVersion,
      );
      expect(hashOfRebasedLocalCommit2).toBe(rebasedLocalCommit2.chunk.hash);
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
    const hashOfRebasedLocalCommit = await withWriteNoImplicitCommit(
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
          undefined,
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
      expect(hashOfRebasedLocalCommit).toBe(rebasedLocalCommit.chunk.hash);
      await fixture.expectRebasedCommit(rebasedLocalCommit, btreeRead);
      await fixture.expectMissingMutatorErrorLog();
    });
  });

  test("throws error if DD31 and mutationClientID does not match mutation's clientID", async () => {
    await testThrowsErrorOnClientIDMismatch('putCommit', FormatVersion.Latest);
  });

  test("throws error if next mutation id for mutationClientID does not match mutation's mutationID", async () => {
    await testThrowsErrorOnMutationIDMismatch('putCommit');
  });
});

async function testThrowsErrorOnClientIDMismatch(
  variant: 'commit' | 'putCommit',
  formatVersion: FormatVersion,
) {
  assert(formatVersion >= FormatVersion.DD31);
  const clientID = 'test_client_id';
  const store = new TestStore();
  const b = new ChainBuilder(store, undefined, formatVersion);
  await b.addGenesis(clientID);
  await b.addSnapshot([['foo', 'bar']], clientID);
  await b.addLocal(clientID);
  const localCommit = b.chain[b.chain.length - 1] as Commit<LocalMetaDD31>;
  const syncChain = await b.addSyncSnapshot(1, clientID);
  const syncSnapshotCommit = syncChain[0] as Commit<SnapshotMetaDD31>;

  let testMutatorCallCount = 0;
  const testMutator = async (tx: WriteTransaction, args?: unknown) => {
    await tx.set('whiz', 'bang');
    expect(args).toEqual(localCommit.meta.mutatorArgsJSON);
    testMutatorCallCount++;
  };
  await withWriteNoImplicitCommit(store, async write => {
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
            undefined,
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
            undefined,
          );
    } catch {
      expect(formatVersion).toBeGreaterThanOrEqual(FormatVersion.DD31);
      return;
    }
    expect(formatVersion).toBeLessThanOrEqual(FormatVersion.SDD);
  });
  expect(testMutatorCallCount).toBe(
    formatVersion >= FormatVersion.DD31 ? 0 : 1,
  );
}

async function testThrowsErrorOnMutationIDMismatch(
  variant: 'commit' | 'putCommit',
) {
  const formatVersion = FormatVersion.DD31;
  const clientID = 'test_client_id';
  const store = new TestStore();
  const b = new ChainBuilder(store);
  await b.addGenesis(clientID);
  await b.addSnapshot([['foo', 'bar']], clientID);
  await b.addLocal(clientID);
  const localCommit1 = b.chain[b.chain.length - 1] as Commit<LocalMetaDD31>;
  await b.addLocal(clientID);
  const localCommit2 = b.chain[b.chain.length - 1] as Commit<LocalMetaDD31>;
  const syncChain = await b.addSyncSnapshot(1, clientID);
  const syncSnapshotCommit = syncChain[0] as Commit<SnapshotMetaDD31>;

  let testMutator1CallCount = 0;
  const testMutator1 = async (tx: WriteTransaction, args?: unknown) => {
    await tx.set('whiz', 'bang');
    expect(args).toEqual(localCommit1.meta.mutatorArgsJSON);
    testMutator1CallCount++;
  };
  let testMutator2CallCount = 0;
  const testMutator2 = async (tx: WriteTransaction, args?: unknown) => {
    await tx.set('fuzzy', 'wuzzy');
    expect(args).toEqual(localCommit2.meta.mutatorArgsJSON);
    testMutator2CallCount++;
  };
  const mutators = {
    [localCommit1.meta.mutatorName]: testMutator1,
    [localCommit2.meta.mutatorName]: testMutator2,
  };
  await withWriteNoImplicitCommit(store, async write => {
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
            undefined,
          )
        : await rebaseMutationAndPutCommit(
            localCommit2,
            write,
            syncSnapshotCommit.chunk.hash,
            mutators,
            new LogContext(),
            clientID,
            formatVersion,
            undefined,
          );
    } catch (e) {
      expectedError = e;
    }
    expect(String(expectedError)).contains('Inconsistent mutation ID');
  });
  expect(testMutator1CallCount).toBe(0);
  expect(testMutator2CallCount).toBe(0);
}
