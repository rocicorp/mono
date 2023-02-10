import {expect} from '@esm-bundle/chai';
import {LogContext} from '@rocicorp/logger';
import sinon from 'sinon';
import * as dag from '../dag/mod';
import * as db from '../db/mod';
import {addGenesis, addLocal, addSnapshot, Chain} from '../db/test-helpers';
import type {WriteTransaction} from '../transactions';
import {rebaseMutation} from './rebase';
import {addSyncSnapshot} from './test-helpers';

teardown(async () => {
  sinon.restore();
});

test('rebaseMutation', async () => {
  const clientID = 'test_client_id';
  const store = new dag.TestStore();
  const chain: Chain = [];
  await addGenesis(chain, store, clientID);
  await addSnapshot(chain, store, [['foo', 'bar']], clientID);
  await addLocal(chain, store, clientID);
  const localCommit = chain[chain.length - 1] as db.Commit<db.LocalMeta>;
  const syncChain = await addSyncSnapshot(chain, store, 1, clientID);
  const syncSnapshotCommit = syncChain[0] as db.Commit<db.SnapshotMeta>;

  let testMutatorCallCount = 0;
  const testMutator = async (tx: WriteTransaction, args?: unknown) => {
    await tx.put('whiz', 'bang');
    expect(args).to.deep.equal(localCommit.meta.mutatorArgsJSON);
    testMutatorCallCount++;
  };
  const hashOfRebasedLocalCommit = await store.withWrite(async write => {
    return await rebaseMutation(
      localCommit,
      write,
      syncSnapshotCommit.chunk.hash,
      {
        [localCommit.meta.mutatorName]: testMutator,
      },
      new LogContext(),
      clientID,
    );
  });
  expect(testMutatorCallCount).to.equal(1);
  await store.withRead(async read => {
    const [, rebasedLocalCommit, btreeRead] = await db.readCommitForBTreeRead(
      db.whenceHash(hashOfRebasedLocalCommit),
      read,
    );
    expect(rebasedLocalCommit.isLocal()).to.be.true;
    if (rebasedLocalCommit.isLocal()) {
      const rebasedCommitLocalMeta: db.LocalMeta = rebasedLocalCommit.meta;
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
    }
    expect(await btreeRead.get('foo')).to.equal('bar');
    expect(await btreeRead.get('whiz')).to.equal('bang');
  });
});

test('rebaseMutation with multiple mutations', async () => {
  const clientID = 'test_client_id';
  const store = new dag.TestStore();
  const chain: Chain = [];
  await addGenesis(chain, store, clientID);
  await addSnapshot(chain, store, [['foo', 'bar']], clientID);
  await addLocal(chain, store, clientID);
  const localCommit1 = chain[chain.length - 1] as db.Commit<db.LocalMeta>;
  await addLocal(chain, store, clientID);
  const localCommit2 = chain[chain.length - 1] as db.Commit<db.LocalMeta>;
  const syncChain = await addSyncSnapshot(chain, store, 1, clientID);
  const syncSnapshotCommit = syncChain[0] as db.Commit<db.SnapshotMeta>;

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
  const hashOfRebasedLocalCommit1 = await store.withWrite(async write => {
    return await rebaseMutation(
      localCommit1,
      write,
      syncSnapshotCommit.chunk.hash,
      mutators,
      new LogContext(),
      clientID,
    );
  });
  expect(testMutator1CallCount).to.equal(1);
  expect(testMutator2CallCount).to.equal(0);
  await store.withRead(async read => {
    const [, rebasedLocalCommit1, btreeRead] = await db.readCommitForBTreeRead(
      db.whenceHash(hashOfRebasedLocalCommit1),
      read,
    );
    expect(rebasedLocalCommit1.isLocal()).to.be.true;
    if (rebasedLocalCommit1.isLocal()) {
      const rebasedCommitLocalMeta: db.LocalMeta = rebasedLocalCommit1.meta;
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
    }
    expect(await btreeRead.get('fuzzy')).to.be.undefined;
    expect(await btreeRead.get('foo')).to.equal('bar');
    expect(await btreeRead.get('whiz')).to.equal('bang');
  });
  const hashOfRebasedLocalCommit2 = await store.withWrite(async write => {
    return await rebaseMutation(
      localCommit2,
      write,
      hashOfRebasedLocalCommit1,
      mutators,
      new LogContext(),
      clientID,
    );
  });
  expect(testMutator1CallCount).to.equal(1);
  expect(testMutator2CallCount).to.equal(1);
  await store.withRead(async read => {
    const [, rebasedLocalCommit2, btreeRead] = await db.readCommitForBTreeRead(
      db.whenceHash(hashOfRebasedLocalCommit2),
      read,
    );
    expect(rebasedLocalCommit2.isLocal()).to.be.true;
    if (rebasedLocalCommit2.isLocal()) {
      const rebasedCommitLocalMeta: db.LocalMeta = rebasedLocalCommit2.meta;
      expect(rebasedCommitLocalMeta.basisHash).to.equal(
        hashOfRebasedLocalCommit1,
      );
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
    }
    expect(await btreeRead.get('fuzzy')).to.equal('wuzzy');
    expect(await btreeRead.get('foo')).to.equal('bar');
    expect(await btreeRead.get('whiz')).to.equal('bang');
  });
});

test('rebaseMutation with missing mutator, still rebases but doesnt modify btree', async () => {
  const consoleErrorStub = sinon.stub(console, 'error');
  const clientID = 'test_client_id';
  const store = new dag.TestStore();
  const chain: Chain = [];
  await addGenesis(chain, store, clientID);
  await addSnapshot(chain, store, [['foo', 'bar']], clientID);
  await addLocal(chain, store, clientID);
  const localCommit = chain[chain.length - 1] as db.Commit<db.LocalMeta>;
  const syncChain = await addSyncSnapshot(chain, store, 1, clientID);
  const syncSnapshotCommit = syncChain[0] as db.Commit<db.SnapshotMeta>;

  const hashOfRebasedLocalCommit = await store.withWrite(async write => {
    return await rebaseMutation(
      localCommit,
      write,
      syncSnapshotCommit.chunk.hash,
      {}, // empty
      new LogContext(),
      clientID,
    );
  });
  await store.withRead(async read => {
    const [, rebasedLocalCommit, btreeRead] = await db.readCommitForBTreeRead(
      db.whenceHash(hashOfRebasedLocalCommit),
      read,
    );
    expect(rebasedLocalCommit.isLocal()).to.be.true;
    if (rebasedLocalCommit.isLocal()) {
      const rebasedCommitLocalMeta: db.LocalMeta = rebasedLocalCommit.meta;
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
    }
    expect(await btreeRead.get('foo')).to.equal('bar');
    expect(consoleErrorStub.callCount).to.equal(1);
    const {args} = consoleErrorStub.getCall(0);
    expect(args[0]).to.equal(
      `Cannot rebase unknown mutator ${localCommit.meta.mutatorName}`,
    );
  });
});

test("rebaseMutation throws error if DD31 and mutationClientID does not match mutation's clientID", async () => {
  const clientID = 'test_client_id';
  const store = new dag.TestStore();
  const chain: Chain = [];
  await addGenesis(chain, store, clientID);
  await addSnapshot(chain, store, [['foo', 'bar']], clientID);
  await addLocal(chain, store, clientID);
  const localCommit = chain[chain.length - 1] as db.Commit<db.LocalMeta>;
  const syncChain = await addSyncSnapshot(chain, store, 1, clientID);
  const syncSnapshotCommit = syncChain[0] as db.Commit<db.SnapshotMeta>;

  let testMutatorCallCount = 0;
  const testMutator = async (tx: WriteTransaction, args?: unknown) => {
    await tx.put('whiz', 'bang');
    expect(args).to.deep.equal(localCommit.meta.mutatorArgsJSON);
    testMutatorCallCount++;
  };
  await store.withWrite(async write => {
    try {
      await rebaseMutation(
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
      expect(DD31).to.be.true;
      return;
    }
    expect(DD31).to.be.false;
  });
  expect(testMutatorCallCount).to.equal(DD31 ? 0 : 1);
});

test("rebaseMutation throws error if next mutation id for mutationClientID does not match mutation's mutationID", async () => {
  const clientID = 'test_client_id';
  const store = new dag.TestStore();
  const chain: Chain = [];
  await addGenesis(chain, store, clientID);
  await addSnapshot(chain, store, [['foo', 'bar']], clientID);
  await addLocal(chain, store, clientID);
  const localCommit1 = chain[chain.length - 1] as db.Commit<db.LocalMeta>;
  await addLocal(chain, store, clientID);
  const localCommit2 = chain[chain.length - 1] as db.Commit<db.LocalMeta>;
  const syncChain = await addSyncSnapshot(chain, store, 1, clientID);
  const syncSnapshotCommit = syncChain[0] as db.Commit<db.SnapshotMeta>;

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
  await store.withWrite(async write => {
    let expectedError;
    try {
      await rebaseMutation(
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
});
