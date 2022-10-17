import {expect} from '@esm-bundle/chai';
import {assert, assertNotNull, assertNotUndefined} from '../asserts';
import * as dag from '../dag/mod';
import * as db from '../db/mod';
import type * as sync from '../sync/mod';
import {
  ChainBuilder,
  createMutatorName,
  getChunkSnapshot,
} from '../db/test-helpers';
import {assertHash, Hash, makeNewFakeHashFunction} from '../hash';
import {
  initClient,
  assertClientDD31,
  Client,
  setClients,
  getClients,
  ClientStateNotFoundError,
  ClientDD31,
} from './clients';
import {assertLocalMetaDD31, assertSnapshotCommitDD31} from '../db/commit.js';
import {LogContext} from '@rocicorp/logger';
import {Branch, BRANCHES_HEAD_NAME, getBranch, setBranch} from './branches';
import {persistDD31} from './persist-dd31';
import type {WriteTransaction} from '../transactions';
import type {JSONValue} from '../json';
import type {MutatorDefs} from '../mod';
import sinon from 'sinon';

const PERDAG_TEST_SETUP_HEAD_NAME = 'test-setup-head';

enum PersistedExpectation {
  SNAPSHOT,
  SNAPSHOT_AND_LOCALS,
  LOCALS,
  NOTHING,
}

suite('persistDD31', () => {
  if (!DD31) {
    return;
  }
  let memdag: dag.LazyStore,
    perdag: dag.TestStore,
    memdagChainBuilder: ChainBuilder,
    perdagBranchChainBuilder: ChainBuilder,
    clients: {clientID: sync.ClientID; client: Client}[],
    branchID: sync.BranchID,
    testPersist: (
      persistedExpectation: PersistedExpectation,
      onGatherMemOnlyChunksForTest?: () => Promise<void>,
    ) => Promise<void>;

  setup(async () => {
    ({
      memdag,
      perdag,
      memdagChainBuilder,
      perdagBranchChainBuilder,
      clients,
      branchID,
      testPersist,
    } = await setupPersistTest());
  });

  teardown(async () => {
    await memdag.close();
    await perdag.close();
  });

  async function setupSnapshots(options?: {
    memdagCookie?: string;
    perdagBranchCookie?: string;
    memdagValueMap?: [string, JSONValue][];
    memdagMutationIDs?: Record<sync.ClientID, number>;
    perdagBranchMutationIDs?: Record<sync.ClientID, number>;
  }) {
    const {
      memdagCookie = 'cookie1',
      perdagBranchCookie = 'cookie1',
      memdagValueMap = [],
      memdagMutationIDs = {},
      perdagBranchMutationIDs = {},
    } = options || {};
    await perdagBranchChainBuilder.addGenesis(clients[0].clientID);
    const perdagBranchSnapshot = await perdagBranchChainBuilder.addSnapshot(
      [],
      clients[0].clientID,
      perdagBranchCookie,
      perdagBranchMutationIDs,
    );
    const perdagBranchHeadHash = perdagBranchSnapshot.chunk.hash;

    await memdagChainBuilder.addGenesis(clients[0].clientID);
    const memdagSnapshot = await memdagChainBuilder.addSnapshot(
      memdagValueMap,
      clients[0].clientID,
      memdagCookie,
      memdagMutationIDs,
    );
    const memdagHeadHash = memdagSnapshot.chunk.hash;

    await setupBranch(perdagBranchHeadHash, {
      mutationIDs: perdagBranchMutationIDs,
      lastServerAckdMutationIDs: perdagBranchMutationIDs,
    });

    return {perdagBranchHeadHash, memdagHeadHash};
  }

  /**
   * When used with setupSnapshots creates the following history graphs:
   *
   * perdag branch:
   *   Snapshot
   *      <- Local Client 0 MutationID 1 (perdagBranchLocalCommit1Client0M1)
   *      <- Local Client 1 MutationID 1 (perdagBranchLocalCommit2Client1M1)
   *      <- Local Client 2 MutationID 1 (perdagBranchLocalCommit3Client2M1)
   *      <- perdag branch head (perdagBranchHeadHash)
   *
   * maindag:
   *   Snapshot
   *      <- Local Client 0 MutationID 1 (memdagLocalCommit1Client0M1)
   *      <- Local Client 1 MutationID 1 (memdagLocalCommit2Client1M1)
   *      <- Local Client 0 MutationID 2 (memdagLocalCommit3Client0M2)
   *      <- memdag DEFAULT_HEAD_NAME head (memdagHeadHash)
   *
   * Also correctly sets the perdag branch map info for the branch.
   */
  async function setupLocals() {
    const {
      perdagBranchLocalCommit1Client0M1,
      perdagBranchLocalCommit2Client1M1,
      perdagBranchLocalCommit3Client2M1,
      perdagBranchHeadHash,
    } = await setupPerdagBranchLocals();

    const {
      memdagLocalCommit1Client0M1,
      memdagLocalCommit2Client1M1,
      memdagLocalCommit3Client0M2,
      memdagHeadHash,
    } = await setupMemdagLocals();

    return {
      perdagBranchLocalCommit1Client0M1,
      perdagBranchLocalCommit2Client1M1,
      perdagBranchLocalCommit3Client2M1,
      perdagBranchHeadHash,
      memdagLocalCommit1Client0M1,
      memdagLocalCommit2Client1M1,
      memdagLocalCommit3Client0M2,
      memdagHeadHash,
    };
  }

  /**
   * See setupLocals comment.
   */
  async function setupMemdagLocals() {
    const memdagLocalCommit1Client0M1 = await memdagChainBuilder.addLocal(
      clients[0].clientID,
    );
    const memdagLocalCommit2Client1M1 = await memdagChainBuilder.addLocal(
      clients[1].clientID,
    );
    const memdagLocalCommit3Client0M2 = await memdagChainBuilder.addLocal(
      clients[0].clientID,
    );
    const memdagHeadHash = memdagLocalCommit3Client0M2.chunk.hash;
    return {
      memdagLocalCommit1Client0M1,
      memdagLocalCommit2Client1M1,
      memdagLocalCommit3Client0M2,
      memdagHeadHash,
    };
  }

  /**
   * See setupLocals comment.
   */
  async function setupPerdagBranchLocals() {
    const perdagBranchLocalCommit1Client0M1 =
      await perdagBranchChainBuilder.addLocal(clients[0].clientID);
    const perdagBranchLocalCommit2Client1M1 =
      await perdagBranchChainBuilder.addLocal(clients[1].clientID);
    const perdagBranchLocalCommit3Client2M1 =
      await perdagBranchChainBuilder.addLocal(clients[2].clientID);
    const perdagBranchHeadHash = perdagBranchLocalCommit3Client2M1.chunk.hash;
    await setupBranch(perdagBranchHeadHash, {
      mutationIDs: {
        [clients[0].clientID]: 1,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
    });
    return {
      perdagBranchLocalCommit1Client0M1,
      perdagBranchLocalCommit2Client1M1,
      perdagBranchLocalCommit3Client2M1,
      perdagBranchHeadHash,
    };
  }

  async function setupBranch(
    perdagBranchHeadHash: Hash,
    branchPartial?: Partial<Branch>,
  ) {
    await perdag.withWrite(async perdagWrite => {
      const branch = await getBranch(branchID, perdagWrite);
      assertNotUndefined(branch);
      await setBranch(
        branchID,
        {
          ...branch,
          ...branchPartial,
          headHash: perdagBranchHeadHash,
        },
        perdagWrite,
      );
      await perdagWrite.commit();
    });
  }

  async function getBranchAndHeadHashes() {
    const memdagHeadHash = await memdag.withRead(memdagRead => {
      return memdagRead.getHead(db.DEFAULT_HEAD_NAME);
    });
    assertNotUndefined(memdagHeadHash);

    const branch = await perdag.withRead(async perdagRead => {
      const branch = await getBranch(branchID, perdagRead);
      assertNotUndefined(branch);
      return branch;
    });
    const perdagBranchHeadHash = branch.headHash;
    return {memdagHeadHash, perdagBranchHeadHash, branch};
  }

  test('equal snapshot cookies no locals', async () => {
    const {perdagBranchHeadHash, memdagHeadHash} = await setupSnapshots();
    await perdagBranchChainBuilder.removeHead();

    const branchSnapshot = await getBranchHelper(perdag, branchID);
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);
    const perdagBranchSnapshot = await getChunkSnapshot(
      perdag,
      perdagBranchHeadHash,
    );

    await testPersist(PersistedExpectation.NOTHING);

    const afterPersist = await getBranchAndHeadHashes();
    // memdag and perdag branch both unchanged
    expect(afterPersist.branch).to.deep.equal(branchSnapshot);
    expect(afterPersist.memdagHeadHash).to.equal(memdagHeadHash);
    expect(
      await getChunkSnapshot(memdag, afterPersist.memdagHeadHash),
    ).to.deep.equal(memdagSnapshot);
    expect(
      await getChunkSnapshot(perdag, afterPersist.perdagBranchHeadHash),
    ).to.deep.equal(perdagBranchSnapshot);
  });

  test('equal snapshot cookies with locals', async () => {
    await setupSnapshots();
    const {
      perdagBranchLocalCommit3Client2M1,
      perdagBranchHeadHash,
      memdagLocalCommit3Client0M2,
      memdagHeadHash,
    } = await setupLocals();
    await perdagBranchChainBuilder.removeHead();

    const branchSnapshot = await getBranchHelper(perdag, branchID);
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);

    await testPersist(PersistedExpectation.LOCALS);

    const afterPersist = await getBranchAndHeadHashes();
    expect(afterPersist.branch).to.deep.equal({
      ...branchSnapshot,
      mutationIDs: {
        [clients[0].clientID]: 2,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
      headHash: afterPersist.perdagBranchHeadHash,
    });
    // memdag unchanged
    expect(afterPersist.memdagHeadHash).to.equal(memdagHeadHash);
    expect(
      await getChunkSnapshot(memdag, afterPersist.memdagHeadHash),
    ).to.deep.equal(memdagSnapshot);
    // memdagLocalCommit3Client0M2 rebased on to perdag branch
    // rest of perdag branch unchanged
    await perdag.withRead(async perdagRead => {
      const afterPersistPerdagBranchLocalCommit4 = await db.commitFromHash(
        afterPersist.perdagBranchHeadHash,
        perdagRead,
      );
      expectRebasedLocal(
        afterPersistPerdagBranchLocalCommit4,
        memdagLocalCommit3Client0M2,
      );
      assertNotNull(afterPersistPerdagBranchLocalCommit4.meta.basisHash);
      const afterPersistPerdagBranchLocalCommit3 = await db.commitFromHash(
        afterPersistPerdagBranchLocalCommit4.meta.basisHash,
        perdagRead,
      );
      expect(afterPersistPerdagBranchLocalCommit3.chunk.hash).to.equal(
        perdagBranchHeadHash,
      );
      expect(afterPersistPerdagBranchLocalCommit3).to.deep.equal(
        perdagBranchLocalCommit3Client2M1,
      );
    });
  });

  test('memdag older snapshot no locals', async () => {
    const {perdagBranchHeadHash, memdagHeadHash} = await setupSnapshots({
      perdagBranchCookie: 'cookie2',
      memdagCookie: 'cookie1',
    });
    await perdagBranchChainBuilder.removeHead();

    const branchSnapshot = await getBranchHelper(perdag, branchID);
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);
    const perdagBranchSnapshot = await getChunkSnapshot(
      perdag,
      perdagBranchHeadHash,
    );

    await testPersist(PersistedExpectation.NOTHING);

    const afterPersist = await getBranchAndHeadHashes();
    // memdag and perdag branch both unchanged
    expect(afterPersist.branch).to.deep.equal(branchSnapshot);
    expect(afterPersist.memdagHeadHash).to.equal(memdagHeadHash);
    expect(
      await getChunkSnapshot(memdag, afterPersist.memdagHeadHash),
    ).to.deep.equal(memdagSnapshot);
    expect(
      await getChunkSnapshot(perdag, afterPersist.perdagBranchHeadHash),
    ).to.deep.equal(perdagBranchSnapshot);
  });

  test('memdag older snapshot with locals', async () => {
    await setupSnapshots({
      perdagBranchCookie: 'cookie2',
      memdagCookie: 'cookie1',
    });
    const {
      perdagBranchLocalCommit3Client2M1,
      perdagBranchHeadHash,
      memdagLocalCommit3Client0M2,
      memdagHeadHash,
    } = await setupLocals();
    await perdagBranchChainBuilder.removeHead();

    const branchSnapshot = await getBranchHelper(perdag, branchID);
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);

    await testPersist(PersistedExpectation.LOCALS);

    const afterPersist = await getBranchAndHeadHashes();
    expect(afterPersist.branch).to.deep.equal({
      ...branchSnapshot,
      mutationIDs: {
        [clients[0].clientID]: 2,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
      headHash: afterPersist.perdagBranchHeadHash,
    });
    // memdag unchanged
    expect(afterPersist.memdagHeadHash).to.equal(memdagHeadHash);
    expect(
      await getChunkSnapshot(memdag, afterPersist.memdagHeadHash),
    ).to.deep.equal(memdagSnapshot);
    // memdagLocalCommit3Client0M2 rebased on to perdag branch
    // rest of perdag branch unchanged
    await perdag.withRead(async perdagRead => {
      const afterPersistPerdagBranchLocalCommit4 = await db.commitFromHash(
        afterPersist.perdagBranchHeadHash,
        perdagRead,
      );
      expectRebasedLocal(
        afterPersistPerdagBranchLocalCommit4,
        memdagLocalCommit3Client0M2,
      );
      assertNotNull(afterPersistPerdagBranchLocalCommit4.meta.basisHash);
      const afterPersistPerdagBranchLocalCommit3 = await db.commitFromHash(
        afterPersistPerdagBranchLocalCommit4.meta.basisHash,
        perdagRead,
      );
      expect(afterPersistPerdagBranchLocalCommit3.chunk.hash).to.equal(
        perdagBranchHeadHash,
      );
      expect(afterPersistPerdagBranchLocalCommit3).to.deep.equal(
        perdagBranchLocalCommit3Client2M1,
      );
    });
  });

  test('memdag older snapshot with locals, perdag snapshot contains memdag local', async () => {
    await setupSnapshots({
      perdagBranchCookie: 'cookie2',
      // memdagLocalCommit3Client0M2 is already reflect in perdag snapshot
      perdagBranchMutationIDs: {
        [clients[0].clientID]: 2,
      },
      memdagCookie: 'cookie1',
    });

    const {memdagHeadHash} = await await setupMemdagLocals();

    // perdag branch:
    //   Snapshot
    //    <- Local Client 1 MutationID 1 (perdagBranchLocalCommit1Client1M1)
    //    <- Local Client 2 MutationID 1 (perdagBranchLocalCommit2Client2M1)
    //    <- perdag branch head (perdagBranchHeadHash)
    await perdagBranchChainBuilder.addLocal(clients[1].clientID);
    const perdagBranchLocalCommit2Client2M1 =
      await perdagBranchChainBuilder.addLocal(clients[2].clientID);
    const perdagBranchHeadHash = perdagBranchLocalCommit2Client2M1.chunk.hash;
    assertNotUndefined(perdagBranchHeadHash);
    await setupBranch(perdagBranchHeadHash, {
      mutationIDs: {
        [clients[0].clientID]: 2,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
    });
    await perdagBranchChainBuilder.removeHead();

    const branchSnapshot = await getBranchHelper(perdag, branchID);
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);

    await testPersist(PersistedExpectation.NOTHING);

    const afterPersist = await getBranchAndHeadHashes();
    expect(afterPersist.branch).to.deep.equal({
      ...branchSnapshot,
      mutationIDs: {
        [clients[0].clientID]: 2,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
      headHash: afterPersist.perdagBranchHeadHash,
    });
    // memdag unchanged
    expect(afterPersist.memdagHeadHash).to.equal(memdagHeadHash);
    expect(
      await getChunkSnapshot(memdag, afterPersist.memdagHeadHash),
    ).to.deep.equal(memdagSnapshot);
    // perdag unchanged, no snapshot to persist and nothing to rebase
    expect(afterPersist.perdagBranchHeadHash).to.equal(perdagBranchHeadHash);
  });

  test('memdag newer snapshot no locals', async () => {
    await setupSnapshots({
      perdagBranchCookie: 'cookie1',
      memdagCookie: 'cookie2',
      memdagValueMap: [
        ['k1', 'value1'],
        ['k2', 'value2'],
      ],
      memdagMutationIDs: {
        [clients[0].clientID]: 1,
        [clients[1].clientID]: 2,
      },
    });
    await perdagBranchChainBuilder.removeHead();

    const branchSnapshot = await getBranchHelper(perdag, branchID);
    await testPersist(PersistedExpectation.SNAPSHOT);

    const afterPersist = await getBranchAndHeadHashes();

    expect(afterPersist.branch).to.deep.equal({
      ...branchSnapshot,
      headHash: afterPersist.perdagBranchHeadHash,
      lastServerAckdMutationIDs: {
        [clients[0].clientID]: 1,
        [clients[1].clientID]: 2,
      },
      mutationIDs: {
        [clients[0].clientID]: 1,
        [clients[1].clientID]: 2,
      },
    });
    // memdag and perdag branch snapshots should be indentical
    // (memdag snapshot written to perdag branch with temp
    // hashes replace with permanent hashes, and then memdag
    // fixed up with permanent hashes)
    expect(
      await getChunkSnapshot(memdag, afterPersist.memdagHeadHash),
    ).to.deep.equal(
      await getChunkSnapshot(perdag, afterPersist.perdagBranchHeadHash),
    );
    // expect values from memdag snapshot are persisted to perdag branch
    await perdag.withRead(async perdagRead => {
      const [, , btreeRead] = await db.readCommitForBTreeRead(
        db.whenceHash(afterPersist.perdagBranchHeadHash),
        perdagRead,
      );
      expect(await btreeRead.get('k1')).to.equal('value1');
      expect(await btreeRead.get('k2')).to.equal('value2');
    });
  });

  test('memdag newer snapshot with locals', async () => {
    const memdagCookie = 'cookie2';
    const memdagMutationIDs = {
      [clients[0].clientID]: 1,
      [clients[2].clientID]: 2,
    };
    await setupSnapshots({
      perdagBranchCookie: 'cookie1',
      memdagCookie,
      memdagValueMap: [
        ['k1', 'value1'],
        ['k2', 'value2'],
      ],
      memdagMutationIDs,
    });

    const {perdagBranchLocalCommit2Client1M1} = await setupPerdagBranchLocals();

    // memdag:
    //   Snapshot
    //    <- Local Client 1 MutationID 1 (memdagLocalCommit1Client1M1)
    //    <- Local Client 0 MutationID 2 (memdagLocalCommit2Client0M2)
    //    <- memdag DEFAULT_HEAD_NAME head (memdagHeadHash)
    await memdagChainBuilder.addLocal(clients[1].clientID);
    const memdagLocalCommit2Client0M2 = await memdagChainBuilder.addLocal(
      clients[0].clientID,
    );
    await perdagBranchChainBuilder.removeHead();

    const branchSnapshot = await getBranchHelper(perdag, branchID);
    await testPersist(PersistedExpectation.SNAPSHOT_AND_LOCALS);

    const afterPersist = await getBranchAndHeadHashes();

    expect(afterPersist.branch).to.deep.equal({
      ...branchSnapshot,
      headHash: afterPersist.perdagBranchHeadHash,
      lastServerAckdMutationIDs: {
        [clients[0].clientID]: 1,
        [clients[2].clientID]: 2,
      },
      mutationIDs: {
        [clients[0].clientID]: 2,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 2,
      },
    });
    const afterPersistPerdagBaseSnapshotHash = await perdag.withRead(
      async perdagRead => {
        const afterPersistPerdagBranchLocalCommit2 = await db.commitFromHash(
          afterPersist.perdagBranchHeadHash,
          perdagRead,
        );
        expectRebasedLocal(
          afterPersistPerdagBranchLocalCommit2,
          memdagLocalCommit2Client0M2,
        );
        assertNotNull(afterPersistPerdagBranchLocalCommit2.meta.basisHash);
        const afterPersistPerdagBranchLocalCommit1 = await db.commitFromHash(
          afterPersistPerdagBranchLocalCommit2.meta.basisHash,
          perdagRead,
        );
        expectRebasedLocal(
          afterPersistPerdagBranchLocalCommit1,
          perdagBranchLocalCommit2Client1M1,
        );
        assertNotNull(afterPersistPerdagBranchLocalCommit1.meta.basisHash);
        const afterPersistPerdagBranchBaseSnapshot = await db.commitFromHash(
          afterPersistPerdagBranchLocalCommit1.meta.basisHash,
          perdagRead,
        );
        assertSnapshotCommitDD31(afterPersistPerdagBranchBaseSnapshot);
        expect(afterPersistPerdagBranchBaseSnapshot.meta.cookieJSON).to.equal(
          memdagCookie,
        );
        expect(
          afterPersistPerdagBranchBaseSnapshot.meta.lastMutationIDs,
        ).to.deep.equal(memdagMutationIDs);

        // expect values from memdag snapshot are persisted to perdag branch
        const [, , btreeRead] = await db.readCommitForBTreeRead(
          db.whenceHash(afterPersist.perdagBranchHeadHash),
          perdagRead,
        );
        expect(await btreeRead.get('k1')).to.equal('value1');
        expect(await btreeRead.get('k2')).to.equal('value2');
        return afterPersistPerdagBranchBaseSnapshot.chunk.hash;
      },
    );

    const afterPersistMemdagBaseSnapshotHash = await memdag.withRead(
      async memdagRead => {
        const baseSnapshot = await db.baseSnapshotFromHash(
          afterPersist.memdagHeadHash,
          memdagRead,
        );
        return baseSnapshot.chunk.hash;
      },
    );

    // memdag and perdag branch snapshots should be indentical
    // (memdag snapshot written to perdag branch with temp
    // hashes replace with permanent hashes, and then memdag
    // fixed up with permanent hashes)
    expect(
      await getChunkSnapshot(memdag, afterPersistPerdagBaseSnapshotHash),
    ).to.deep.equal(
      await getChunkSnapshot(perdag, afterPersistMemdagBaseSnapshotHash),
    );
  });

  test('memdag newer snapshot with locals, but then older after chunks hashed', async () => {
    const memdagCookie = 'cookie2';
    await setupSnapshots({
      perdagBranchCookie: 'cookie1',
      memdagCookie,
      memdagValueMap: [
        ['k1', 'value1'],
        ['k2', 'value2'],
      ],
    });

    const {memdagLocalCommit3Client0M2, memdagHeadHash} = await setupLocals();

    await perdagBranchChainBuilder.removeHead();

    let perdagBranchUpdatedToNewerSnapshot = false;
    const updatedPerdagBranchCookie = 'cookie3';
    let updatedPerdagBranchHeadHash: undefined | Hash;
    let updatedPerdagBranchSnapshot;
    async function ensurePerdagBranchUpdatedToNewerSnapshot() {
      if (perdagBranchUpdatedToNewerSnapshot) {
        return;
      }
      perdagBranchUpdatedToNewerSnapshot = true;
      const updatedPerdagBranchChainBuilder: ChainBuilder = new ChainBuilder(
        perdag,
        PERDAG_TEST_SETUP_HEAD_NAME,
      );
      const mutationIDs = {
        [clients[0].clientID]: 1,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      };
      await updatedPerdagBranchChainBuilder.addGenesis(clients[0].clientID);
      const updatePerdagBranchSnapshot =
        await updatedPerdagBranchChainBuilder.addSnapshot(
          [],
          clients[0].clientID,
          updatedPerdagBranchCookie,
          mutationIDs,
        );
      updatedPerdagBranchHeadHash = updatePerdagBranchSnapshot.chunk.hash;
      assertNotUndefined(updatedPerdagBranchHeadHash);
      await setupBranch(updatedPerdagBranchHeadHash, {
        mutationIDs,
        lastServerAckdMutationIDs: mutationIDs,
      });
      await perdagBranchChainBuilder.removeHead();
      updatedPerdagBranchSnapshot = await getChunkSnapshot(
        perdag,
        updatedPerdagBranchHeadHash,
      );
    }

    const branchSnapshot = await getBranchHelper(perdag, branchID);
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);

    await testPersist(PersistedExpectation.LOCALS, async () => {
      await ensurePerdagBranchUpdatedToNewerSnapshot();
    });

    const afterPersist = await getBranchAndHeadHashes();

    expect(afterPersist.branch).to.deep.equal({
      ...branchSnapshot,
      headHash: afterPersist.perdagBranchHeadHash,
      lastServerAckdMutationIDs: {
        [clients[0].clientID]: 1,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
      mutationIDs: {
        [clients[0].clientID]: 2,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
    });

    // memdag unchanged
    expect(afterPersist.memdagHeadHash).to.equal(memdagHeadHash);
    expect(
      await getChunkSnapshot(memdag, afterPersist.memdagHeadHash),
    ).to.deep.equal(memdagSnapshot);
    // memdagLocalCommit3Client0M2 rebased on to perdag branch
    // (with basis updatedPerdagBranchSnapshot)
    const afterPersistPerdagBranchBaseSnapshotHash = await perdag.withRead(
      async perdagRead => {
        const afterPersistPerdagBranchLocalCommit1 = await db.commitFromHash(
          afterPersist.perdagBranchHeadHash,
          perdagRead,
        );
        expectRebasedLocal(
          afterPersistPerdagBranchLocalCommit1,
          memdagLocalCommit3Client0M2,
        );

        assertNotNull(afterPersistPerdagBranchLocalCommit1.meta.basisHash);
        const afterPersistPerdagBranchBaseSnapshot = await db.commitFromHash(
          afterPersistPerdagBranchLocalCommit1.meta.basisHash,
          perdagRead,
        );
        assertSnapshotCommitDD31(afterPersistPerdagBranchBaseSnapshot);
        expect(afterPersistPerdagBranchBaseSnapshot.meta.cookieJSON).to.equal(
          updatedPerdagBranchCookie,
        );
        return afterPersistPerdagBranchBaseSnapshot.chunk.hash;
      },
    );

    expect(
      await getChunkSnapshot(perdag, afterPersistPerdagBranchBaseSnapshotHash),
    ).to.deep.equal(updatedPerdagBranchSnapshot);
  });

  test('persist throws a ClientStateNotFoundError if client is missing', async () => {
    await setupSnapshots();

    await perdag.withWrite(async perdagWrite => {
      const clientMap = await getClients(perdagWrite);
      const newClientMap = new Map(clientMap);
      newClientMap.delete(clients[0].clientID);
      await setClients(newClientMap, perdagWrite);
      await perdagWrite.commit();
    });

    let err;
    try {
      await testPersist(PersistedExpectation.NOTHING);
    } catch (e) {
      err = e;
    }
    expect(err)
      .to.be.an.instanceof(ClientStateNotFoundError)
      .property('id', clients[0].clientID);
  });
});

async function setupPersistTest() {
  const hashFunction = makeNewFakeHashFunction();
  const perdag = new dag.TestStore(undefined, hashFunction);
  const memdag = new dag.LazyStore(
    perdag,
    100 * 2 ** 20, // 100 MB,
    hashFunction,
    assertHash,
  );
  const chunksPersistedSpy = sinon.spy(memdag, 'chunksPersisted');

  const mutatorNames = Array.from({length: 10}, (_, index) => {
    return createMutatorName(index);
  });
  const mutators: MutatorDefs = {};
  for (let i = 0; i < mutatorNames.length; i++) {
    mutators[mutatorNames[i]] = async (
      tx: WriteTransaction,
      args: JSONValue,
    ) => {
      await tx.put(`key-${i}`, args);
    };
  }

  let branchID: undefined | sync.BranchID;
  const createClient = async () => {
    const [cID, c] = await initClient(
      new LogContext(),
      perdag,
      mutatorNames,
      {},
    );
    assertClientDD31(c);
    assert(branchID === undefined || c.branchID === branchID);
    branchID = c.branchID;
    return {
      clientID: cID,
      client: c,
    };
  };
  const clients: {clientID: sync.ClientID; client: ClientDD31}[] = [];
  for (let i = 0; i < 3; i++) {
    clients.push(await createClient());
  }

  assertNotUndefined(branchID);

  const testPersist = async (
    persistedExpectation: PersistedExpectation,
    onGatherMemOnlyChunksForTest = async () => {
      return;
    },
  ) => {
    const chunksPersistedSpyCountPrePersist = chunksPersistedSpy.callCount;
    const perdagChunkHashesPrePersist = perdag.chunkHashes();
    await persistDD31(
      new LogContext(),
      clients[0].clientID,
      memdag,
      memdag,
      perdag,
      mutators,
      () => false,
      onGatherMemOnlyChunksForTest,
    );
    const persistedChunkHashes = new Set<Hash>();
    const branchesHeadHash = await perdag.withRead(read => {
      return read.getHead(BRANCHES_HEAD_NAME);
    });
    for (const hash of perdag.chunkHashes()) {
      if (!perdagChunkHashesPrePersist.has(hash) && hash !== branchesHeadHash) {
        persistedChunkHashes.add(hash);
      }
    }
    switch (persistedExpectation) {
      case PersistedExpectation.SNAPSHOT:
        expect(persistedChunkHashes.size).to.be.greaterThan(0);
        expect(chunksPersistedSpy.callCount).to.equal(
          chunksPersistedSpyCountPrePersist + 1,
        );
        expect(new Set(chunksPersistedSpy.lastCall.args[0])).to.deep.equal(
          persistedChunkHashes,
        );
        break;
      case PersistedExpectation.SNAPSHOT_AND_LOCALS:
        expect(persistedChunkHashes.size).to.be.greaterThan(0);
        expect(chunksPersistedSpy.callCount).to.equal(
          chunksPersistedSpyCountPrePersist + 1,
        );
        // Persisted chunks is a superset of chunks passed to
        // chunksPersisted
        expect([...persistedChunkHashes]).to.include.members(
          chunksPersistedSpy.lastCall.args[0],
        );
        break;
      case PersistedExpectation.LOCALS:
        expect(persistedChunkHashes.size).to.be.greaterThan(0);
        expect(chunksPersistedSpy.callCount).to.equal(
          chunksPersistedSpyCountPrePersist,
        );
        break;
      case PersistedExpectation.NOTHING:
        expect(persistedChunkHashes.size).to.equal(0);
        expect(chunksPersistedSpy.callCount).to.equal(
          chunksPersistedSpyCountPrePersist,
        );
        break;
    }
  };

  return {
    memdag,
    perdag,
    memdagChainBuilder: new ChainBuilder(memdag),
    perdagBranchChainBuilder: new ChainBuilder(
      perdag,
      PERDAG_TEST_SETUP_HEAD_NAME,
    ),
    clients,
    branchID,
    testPersist,
  };
}
function getBranchHelper(perdag: dag.TestStore, branchID: string) {
  return perdag.withRead(perdagRead => {
    return getBranch(branchID, perdagRead);
  });
}

function expectRebasedLocal(
  rebased: db.Commit<db.Meta>,
  original: db.Commit<db.Meta>,
) {
  expect(rebased.chunk.hash).to.not.equal(original.chunk.hash);
  const rebasedMeta = rebased.meta;
  assertLocalMetaDD31(rebasedMeta);
  const originalMeta = original.meta;
  assertLocalMetaDD31(originalMeta);
  expect(rebasedMeta.clientID).to.equal(originalMeta.clientID);
  expect(rebasedMeta.mutationID).to.equal(originalMeta.mutationID);
  expect(rebasedMeta.mutatorName).to.equal(originalMeta.mutatorName);
  expect(rebasedMeta.mutatorArgsJSON).to.deep.equal(
    originalMeta.mutatorArgsJSON,
  );
}
