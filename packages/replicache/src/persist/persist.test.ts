import {expect} from '@esm-bundle/chai';
import {assert, assertNotNull, assertNotUndefined} from '../asserts.js';
import * as dag from '../dag/mod.js';
import * as db from '../db/mod.js';
import type * as sync from '../sync/mod.js';
import {
  ChainBuilder,
  createMutatorName,
  getChunkSnapshot,
} from '../db/test-helpers.js';
import {assertHash, Hash, makeNewFakeHashFunction} from '../hash.js';
import {
  setClients,
  getClients,
  ClientStateNotFoundError,
  ClientDD31,
  initClientDD31,
  Client,
} from './clients.js';
import {assertLocalMetaDD31, assertSnapshotCommitDD31} from '../db/commit.js';
import {LogContext} from '@rocicorp/logger';
import {
  ClientGroup,
  CLIENT_GROUPS_HEAD_NAME,
  getClientGroup,
  setClientGroup,
} from './client-groups.js';
import {persistDD31} from './persist.js';
import type {WriteTransaction} from '../transactions.js';
import type {JSONValue} from '../json.js';
import type {MutatorDefs} from '../mod.js';
import sinon from 'sinon';
import {promiseVoid} from '../resolved-promises.js';
import {withRead, withWrite} from '../with-transactions.js';

const PERDAG_TEST_SETUP_HEAD_NAME = 'test-setup-head';

enum PersistedExpectation {
  SNAPSHOT,
  SNAPSHOT_AND_LOCALS,
  LOCALS,
  NOTHING,
}

suite('persistDD31', () => {
  let memdag: dag.LazyStore,
    perdag: dag.TestStore,
    memdagChainBuilder: ChainBuilder,
    perdagClientGroupChainBuilder: ChainBuilder,
    clients: {clientID: sync.ClientID; client: Client}[],
    clientGroupID: sync.ClientGroupID,
    testPersist: (
      persistedExpectation: PersistedExpectation,
      onGatherMemOnlyChunksForTest?: () => Promise<void>,
    ) => Promise<void>;

  setup(async () => {
    ({
      memdag,
      perdag,
      memdagChainBuilder,
      perdagClientGroupChainBuilder,
      clients,
      clientGroupID,
      testPersist,
    } = await setupPersistTest());
  });

  teardown(async () => {
    await memdag.close();
    await perdag.close();
  });

  async function setupSnapshots(options?: {
    memdagCookie?: string;
    perdagClientGroupCookie?: string;
    memdagValueMap?: [string, JSONValue][];
    memdagMutationIDs?: Record<sync.ClientID, number>;
    perdagClientGroupMutationIDs?: Record<sync.ClientID, number>;
  }) {
    const {
      memdagCookie = 'cookie1',
      perdagClientGroupCookie: perdagClientGroupCookie = 'cookie1',
      memdagValueMap = [],
      memdagMutationIDs = {},
      perdagClientGroupMutationIDs: perdagClientGroupMutationIDs = {},
    } = options || {};
    await perdagClientGroupChainBuilder.addGenesis(clients[0].clientID);
    const perdagClientGroupSnapshot =
      await perdagClientGroupChainBuilder.addSnapshot(
        [],
        clients[0].clientID,
        perdagClientGroupCookie,
        perdagClientGroupMutationIDs,
      );
    const perdagClientGroupHeadHash = perdagClientGroupSnapshot.chunk.hash;

    await memdagChainBuilder.addGenesis(clients[0].clientID);
    const memdagSnapshot = await memdagChainBuilder.addSnapshot(
      memdagValueMap,
      clients[0].clientID,
      memdagCookie,
      memdagMutationIDs,
    );
    const memdagHeadHash = memdagSnapshot.chunk.hash;

    await setupClientGroup(perdagClientGroupHeadHash, {
      mutationIDs: perdagClientGroupMutationIDs,
      lastServerAckdMutationIDs: perdagClientGroupMutationIDs,
    });

    return {perdagClientGroupHeadHash, memdagHeadHash};
  }

  /**
   * When used with setupSnapshots creates the following history graphs:
   *
   * perdag client group:
   *   Snapshot
   *      <- Local Client 0 MutationID 1 (perdagClientGroupLocalCommit1Client0M1)
   *      <- Local Client 1 MutationID 1 (perdagClientGroupLocalCommit2Client1M1)
   *      <- Local Client 2 MutationID 1 (perdagClientGroupLocalCommit3Client2M1)
   *      <- perdag client group head (perdagClientGroupHeadHash)
   *
   * maindag:
   *   Snapshot
   *      <- Local Client 0 MutationID 1 (memdagLocalCommit1Client0M1)
   *      <- Local Client 1 MutationID 1 (memdagLocalCommit2Client1M1)
   *      <- Local Client 0 MutationID 2 (memdagLocalCommit3Client0M2)
   *      <- memdag DEFAULT_HEAD_NAME head (memdagHeadHash)
   *
   * Also correctly sets the perdag client group map info for the client group.
   */
  async function setupLocals() {
    const {
      perdagClientGroupLocalCommit1Client0M1,
      perdagClientGroupLocalCommit2Client1M1,
      perdagClientGroupLocalCommit3Client2M1,
      perdagClientGroupHeadHash,
    } = await setupPerdagClientGroupLocals();

    const {
      memdagLocalCommit1Client0M1,
      memdagLocalCommit2Client1M1,
      memdagLocalCommit3Client0M2,
      memdagHeadHash,
    } = await setupMemdagLocals();

    return {
      perdagClientGroupLocalCommit1Client0M1,
      perdagClientGroupLocalCommit2Client1M1,
      perdagClientGroupLocalCommit3Client2M1,
      perdagClientGroupHeadHash,
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
  async function setupPerdagClientGroupLocals() {
    const perdagClientGroupLocalCommit1Client0M1 =
      await perdagClientGroupChainBuilder.addLocal(clients[0].clientID);
    const perdagClientGroupLocalCommit2Client1M1 =
      await perdagClientGroupChainBuilder.addLocal(clients[1].clientID);
    const perdagClientGroupLocalCommit3Client2M1 =
      await perdagClientGroupChainBuilder.addLocal(clients[2].clientID);
    const perdagClientGroupHeadHash =
      perdagClientGroupLocalCommit3Client2M1.chunk.hash;
    await setupClientGroup(perdagClientGroupHeadHash, {
      mutationIDs: {
        [clients[0].clientID]: 1,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
    });
    return {
      perdagClientGroupLocalCommit1Client0M1,
      perdagClientGroupLocalCommit2Client1M1,
      perdagClientGroupLocalCommit3Client2M1,
      perdagClientGroupHeadHash,
    };
  }

  async function setupClientGroup(
    perdagClientGroupHeadHash: Hash,
    clientGroupPartial?: Partial<ClientGroup>,
  ) {
    await withWrite(perdag, async perdagWrite => {
      const clientGroup = await getClientGroup(clientGroupID, perdagWrite);
      assertNotUndefined(clientGroup);
      await setClientGroup(
        clientGroupID,
        {
          ...clientGroup,
          ...clientGroupPartial,
          headHash: perdagClientGroupHeadHash,
        },
        perdagWrite,
      );
      await perdagWrite.commit();
    });
  }

  async function getClientGroupAndHeadHashes() {
    const memdagHeadHash = await withRead(memdag, memdagRead => {
      return memdagRead.getHead(db.DEFAULT_HEAD_NAME);
    });
    assertNotUndefined(memdagHeadHash);

    const clientGroup = await withRead(perdag, async perdagRead => {
      const clientGroup = await getClientGroup(clientGroupID, perdagRead);
      assertNotUndefined(clientGroup);
      return clientGroup;
    });
    const perdagClientGroupHeadHash = clientGroup.headHash;
    return {memdagHeadHash, perdagClientGroupHeadHash, clientGroup};
  }

  test('equal snapshot cookies no locals', async () => {
    const {
      perdagClientGroupHeadHash: perdagClientGroupHeadHash,
      memdagHeadHash,
    } = await setupSnapshots();
    await perdagClientGroupChainBuilder.removeHead();

    const clientGroupSnapshot = await getClientGroupHelper(
      perdag,
      clientGroupID,
    );
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);
    const perdagClientGroupSnapshot = await getChunkSnapshot(
      perdag,
      perdagClientGroupHeadHash,
    );

    await testPersist(PersistedExpectation.NOTHING);

    const afterPersist = await getClientGroupAndHeadHashes();
    // memdag and perdag client group both unchanged
    expect(afterPersist.clientGroup).to.deep.equal(clientGroupSnapshot);
    expect(afterPersist.memdagHeadHash).to.equal(memdagHeadHash);
    expect(
      await getChunkSnapshot(memdag, afterPersist.memdagHeadHash),
    ).to.deep.equal(memdagSnapshot);
    expect(
      await getChunkSnapshot(perdag, afterPersist.perdagClientGroupHeadHash),
    ).to.deep.equal(perdagClientGroupSnapshot);
  });

  test('equal snapshot cookies with locals', async () => {
    await setupSnapshots();
    const {
      perdagClientGroupLocalCommit3Client2M1,
      perdagClientGroupHeadHash,
      memdagLocalCommit3Client0M2,
      memdagHeadHash,
    } = await setupLocals();
    await perdagClientGroupChainBuilder.removeHead();

    const clientGroupSnapshot = await getClientGroupHelper(
      perdag,
      clientGroupID,
    );
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);

    await testPersist(PersistedExpectation.LOCALS);

    const afterPersist = await getClientGroupAndHeadHashes();
    expect(afterPersist.clientGroup).to.deep.equal({
      ...clientGroupSnapshot,
      mutationIDs: {
        [clients[0].clientID]: 2,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
      headHash: afterPersist.perdagClientGroupHeadHash,
    });
    // memdag unchanged
    expect(afterPersist.memdagHeadHash).to.equal(memdagHeadHash);
    expect(
      await getChunkSnapshot(memdag, afterPersist.memdagHeadHash),
    ).to.deep.equal(memdagSnapshot);
    // memdagLocalCommit3Client0M2 rebased on to perdag client group rest of
    // perdag client group unchanged
    await withRead(perdag, async perdagRead => {
      const afterPersistPerdagClientGroupLocalCommit4 = await db.commitFromHash(
        afterPersist.perdagClientGroupHeadHash,
        perdagRead,
      );
      expectRebasedLocal(
        afterPersistPerdagClientGroupLocalCommit4,
        memdagLocalCommit3Client0M2,
      );
      assertNotNull(afterPersistPerdagClientGroupLocalCommit4.meta.basisHash);
      const afterPersistPerdagClientGroupLocalCommit3 = await db.commitFromHash(
        afterPersistPerdagClientGroupLocalCommit4.meta.basisHash,
        perdagRead,
      );
      expect(afterPersistPerdagClientGroupLocalCommit3.chunk.hash).to.equal(
        perdagClientGroupHeadHash,
      );
      expect(afterPersistPerdagClientGroupLocalCommit3).to.deep.equal(
        perdagClientGroupLocalCommit3Client2M1,
      );
    });
  });

  test('memdag older snapshot no locals', async () => {
    const {
      perdagClientGroupHeadHash: perdagClientGroupHeadHash,
      memdagHeadHash,
    } = await setupSnapshots({
      perdagClientGroupCookie: 'cookie2',
      memdagCookie: 'cookie1',
    });
    await perdagClientGroupChainBuilder.removeHead();

    const clientGroupSnapshot = await getClientGroupHelper(
      perdag,
      clientGroupID,
    );
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);
    const perdagClientGroupSnapshot = await getChunkSnapshot(
      perdag,
      perdagClientGroupHeadHash,
    );

    await testPersist(PersistedExpectation.NOTHING);

    const afterPersist = await getClientGroupAndHeadHashes();
    // memdag and perdag client group both unchanged
    expect(afterPersist.clientGroup).to.deep.equal(clientGroupSnapshot);
    expect(afterPersist.memdagHeadHash).to.equal(memdagHeadHash);
    expect(
      await getChunkSnapshot(memdag, afterPersist.memdagHeadHash),
    ).to.deep.equal(memdagSnapshot);
    expect(
      await getChunkSnapshot(perdag, afterPersist.perdagClientGroupHeadHash),
    ).to.deep.equal(perdagClientGroupSnapshot);
  });

  test('memdag older snapshot with locals', async () => {
    await setupSnapshots({
      perdagClientGroupCookie: 'cookie2',
      memdagCookie: 'cookie1',
    });
    const {
      perdagClientGroupLocalCommit3Client2M1,
      perdagClientGroupHeadHash,
      memdagLocalCommit3Client0M2,
      memdagHeadHash,
    } = await setupLocals();
    await perdagClientGroupChainBuilder.removeHead();

    const clientGroupSnapshot = await getClientGroupHelper(
      perdag,
      clientGroupID,
    );
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);

    await testPersist(PersistedExpectation.LOCALS);

    const afterPersist = await getClientGroupAndHeadHashes();
    expect(afterPersist.clientGroup).to.deep.equal({
      ...clientGroupSnapshot,
      mutationIDs: {
        [clients[0].clientID]: 2,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
      headHash: afterPersist.perdagClientGroupHeadHash,
    });
    // memdag unchanged
    expect(afterPersist.memdagHeadHash).to.equal(memdagHeadHash);
    expect(
      await getChunkSnapshot(memdag, afterPersist.memdagHeadHash),
    ).to.deep.equal(memdagSnapshot);
    // memdagLocalCommit3Client0M2 rebased on to perdag client group rest of
    // perdag client group unchanged
    await withRead(perdag, async perdagRead => {
      const afterPersistPerdagClientGroupLocalCommit4 = await db.commitFromHash(
        afterPersist.perdagClientGroupHeadHash,
        perdagRead,
      );
      expectRebasedLocal(
        afterPersistPerdagClientGroupLocalCommit4,
        memdagLocalCommit3Client0M2,
      );
      assertNotNull(afterPersistPerdagClientGroupLocalCommit4.meta.basisHash);
      const afterPersistPerdagClientGroupLocalCommit3 = await db.commitFromHash(
        afterPersistPerdagClientGroupLocalCommit4.meta.basisHash,
        perdagRead,
      );
      expect(afterPersistPerdagClientGroupLocalCommit3.chunk.hash).to.equal(
        perdagClientGroupHeadHash,
      );
      expect(afterPersistPerdagClientGroupLocalCommit3).to.deep.equal(
        perdagClientGroupLocalCommit3Client2M1,
      );
    });
  });

  test('memdag older snapshot with locals, perdag snapshot contains memdag local', async () => {
    await setupSnapshots({
      perdagClientGroupCookie: 'cookie2',
      // memdagLocalCommit3Client0M2 is already reflect in perdag snapshot
      perdagClientGroupMutationIDs: {
        [clients[0].clientID]: 2,
      },
      memdagCookie: 'cookie1',
    });

    const {memdagHeadHash} = await await setupMemdagLocals();

    // perdag client group:
    //   Snapshot
    //    <- Local Client 1 MutationID 1 (perdagClientGroupLocalCommit1Client1M1)
    //    <- Local Client 2 MutationID 1 (perdagClientGroupLocalCommit2Client2M1)
    //    <- perdag client group head (perdagClientGroupHeadHash)
    await perdagClientGroupChainBuilder.addLocal(clients[1].clientID);
    const perdagClientGroupLocalCommit2Client2M1 =
      await perdagClientGroupChainBuilder.addLocal(clients[2].clientID);
    const perdagClientGroupHeadHash =
      perdagClientGroupLocalCommit2Client2M1.chunk.hash;
    assertNotUndefined(perdagClientGroupHeadHash);
    await setupClientGroup(perdagClientGroupHeadHash, {
      mutationIDs: {
        [clients[0].clientID]: 2,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
    });
    await perdagClientGroupChainBuilder.removeHead();

    const clientGroupSnapshot = await getClientGroupHelper(
      perdag,
      clientGroupID,
    );
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);

    await testPersist(PersistedExpectation.NOTHING);

    const afterPersist = await getClientGroupAndHeadHashes();
    expect(afterPersist.clientGroup).to.deep.equal({
      ...clientGroupSnapshot,
      mutationIDs: {
        [clients[0].clientID]: 2,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
      headHash: afterPersist.perdagClientGroupHeadHash,
    });
    // memdag unchanged
    expect(afterPersist.memdagHeadHash).to.equal(memdagHeadHash);
    expect(
      await getChunkSnapshot(memdag, afterPersist.memdagHeadHash),
    ).to.deep.equal(memdagSnapshot);
    // perdag unchanged, no snapshot to persist and nothing to rebase
    expect(afterPersist.perdagClientGroupHeadHash).to.equal(
      perdagClientGroupHeadHash,
    );
  });

  test('memdag newer snapshot no locals', async () => {
    await setupSnapshots({
      perdagClientGroupCookie: 'cookie1',
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
    await perdagClientGroupChainBuilder.removeHead();

    const clientGroupSnapshot = await getClientGroupHelper(
      perdag,
      clientGroupID,
    );
    await testPersist(PersistedExpectation.SNAPSHOT);

    const afterPersist = await getClientGroupAndHeadHashes();

    expect(afterPersist.clientGroup).to.deep.equal({
      ...clientGroupSnapshot,
      headHash: afterPersist.perdagClientGroupHeadHash,
      lastServerAckdMutationIDs: {
        [clients[0].clientID]: 1,
        [clients[1].clientID]: 2,
      },
      mutationIDs: {
        [clients[0].clientID]: 1,
        [clients[1].clientID]: 2,
      },
    });
    // memdag and perdag client group snapshots should be identical (memdag
    // snapshot written to perdag client group with temp hashes replace with
    // permanent hashes, and then memdag fixed up with permanent hashes)
    expect(
      await getChunkSnapshot(memdag, afterPersist.memdagHeadHash),
    ).to.deep.equal(
      await getChunkSnapshot(perdag, afterPersist.perdagClientGroupHeadHash),
    );
    // expect values from memdag snapshot are persisted to perdag client group
    await withRead(perdag, async perdagRead => {
      const [, , btreeRead] = await db.readCommitForBTreeRead(
        db.whenceHash(afterPersist.perdagClientGroupHeadHash),
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
      perdagClientGroupCookie: 'cookie1',
      memdagCookie,
      memdagValueMap: [
        ['k1', 'value1'],
        ['k2', 'value2'],
      ],
      memdagMutationIDs,
    });

    const {perdagClientGroupLocalCommit2Client1M1} =
      await setupPerdagClientGroupLocals();

    // memdag:
    //   Snapshot
    //    <- Local Client 1 MutationID 1 (memdagLocalCommit1Client1M1)
    //    <- Local Client 0 MutationID 2 (memdagLocalCommit2Client0M2)
    //    <- memdag DEFAULT_HEAD_NAME head (memdagHeadHash)
    await memdagChainBuilder.addLocal(clients[1].clientID);
    const memdagLocalCommit2Client0M2 = await memdagChainBuilder.addLocal(
      clients[0].clientID,
    );
    await perdagClientGroupChainBuilder.removeHead();

    const clientGroupSnapshot = await getClientGroupHelper(
      perdag,
      clientGroupID,
    );
    await testPersist(PersistedExpectation.SNAPSHOT_AND_LOCALS);

    const afterPersist = await getClientGroupAndHeadHashes();

    expect(afterPersist.clientGroup).to.deep.equal({
      ...clientGroupSnapshot,
      headHash: afterPersist.perdagClientGroupHeadHash,
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
    const afterPersistPerdagBaseSnapshotHash = await withRead(
      perdag,
      async perdagRead => {
        const afterPersistPerdagClientGroupLocalCommit2 =
          await db.commitFromHash(
            afterPersist.perdagClientGroupHeadHash,
            perdagRead,
          );
        expectRebasedLocal(
          afterPersistPerdagClientGroupLocalCommit2,
          memdagLocalCommit2Client0M2,
        );
        assertNotNull(afterPersistPerdagClientGroupLocalCommit2.meta.basisHash);
        const afterPersistPerdagClientGroupLocalCommit1 =
          await db.commitFromHash(
            afterPersistPerdagClientGroupLocalCommit2.meta.basisHash,
            perdagRead,
          );
        expectRebasedLocal(
          afterPersistPerdagClientGroupLocalCommit1,
          perdagClientGroupLocalCommit2Client1M1,
        );
        assertNotNull(afterPersistPerdagClientGroupLocalCommit1.meta.basisHash);
        const afterPersistPerdagClientGroupBaseSnapshot =
          await db.commitFromHash(
            afterPersistPerdagClientGroupLocalCommit1.meta.basisHash,
            perdagRead,
          );
        assertSnapshotCommitDD31(afterPersistPerdagClientGroupBaseSnapshot);
        expect(
          afterPersistPerdagClientGroupBaseSnapshot.meta.cookieJSON,
        ).to.equal(memdagCookie);
        expect(
          afterPersistPerdagClientGroupBaseSnapshot.meta.lastMutationIDs,
        ).to.deep.equal(memdagMutationIDs);

        // expect values from memdag snapshot are persisted to perdag client group
        const [, , btreeRead] = await db.readCommitForBTreeRead(
          db.whenceHash(afterPersist.perdagClientGroupHeadHash),
          perdagRead,
        );
        expect(await btreeRead.get('k1')).to.equal('value1');
        expect(await btreeRead.get('k2')).to.equal('value2');
        return afterPersistPerdagClientGroupBaseSnapshot.chunk.hash;
      },
    );

    const afterPersistMemdagBaseSnapshotHash = await withRead(
      memdag,
      async memdagRead => {
        const baseSnapshot = await db.baseSnapshotFromHash(
          afterPersist.memdagHeadHash,
          memdagRead,
        );
        return baseSnapshot.chunk.hash;
      },
    );

    // memdag and perdag client group snapshots should be identical (memdag
    // snapshot written to perdag client group with temp hashes replace with
    // permanent hashes, and then memdag fixed up with permanent hashes)
    expect(
      await getChunkSnapshot(memdag, afterPersistPerdagBaseSnapshotHash),
    ).to.deep.equal(
      await getChunkSnapshot(perdag, afterPersistMemdagBaseSnapshotHash),
    );
  });

  test('memdag newer snapshot with locals, but then older after chunks hashed', async () => {
    const memdagCookie = 'cookie2';
    await setupSnapshots({
      perdagClientGroupCookie: 'cookie1',
      memdagCookie,
      memdagValueMap: [
        ['k1', 'value1'],
        ['k2', 'value2'],
      ],
    });

    const {memdagLocalCommit3Client0M2, memdagHeadHash} = await setupLocals();

    await perdagClientGroupChainBuilder.removeHead();

    let perdagClientGroupUpdatedToNewerSnapshot = false;
    const updatedPerdagClientGroupCookie = 'cookie3';
    let updatedPerdagClientGroupHeadHash: undefined | Hash;
    let updatedPerdagClientGroupSnapshot;
    async function ensurePerdagClientGroupUpdatedToNewerSnapshot() {
      if (perdagClientGroupUpdatedToNewerSnapshot) {
        return;
      }
      perdagClientGroupUpdatedToNewerSnapshot = true;
      const updatedPerdagClientGroupChainBuilder: ChainBuilder =
        new ChainBuilder(perdag, PERDAG_TEST_SETUP_HEAD_NAME);
      const mutationIDs = {
        [clients[0].clientID]: 1,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      };
      await updatedPerdagClientGroupChainBuilder.addGenesis(
        clients[0].clientID,
      );
      const updatePerdagClientGroupSnapshot =
        await updatedPerdagClientGroupChainBuilder.addSnapshot(
          [],
          clients[0].clientID,
          updatedPerdagClientGroupCookie,
          mutationIDs,
        );
      updatedPerdagClientGroupHeadHash =
        updatePerdagClientGroupSnapshot.chunk.hash;
      assertNotUndefined(updatedPerdagClientGroupHeadHash);
      await setupClientGroup(updatedPerdagClientGroupHeadHash, {
        mutationIDs,
        lastServerAckdMutationIDs: mutationIDs,
      });
      await perdagClientGroupChainBuilder.removeHead();
      updatedPerdagClientGroupSnapshot = await getChunkSnapshot(
        perdag,
        updatedPerdagClientGroupHeadHash,
      );
    }

    const clientGroupSnapshot = await getClientGroupHelper(
      perdag,
      clientGroupID,
    );
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);

    await testPersist(PersistedExpectation.LOCALS, async () => {
      await ensurePerdagClientGroupUpdatedToNewerSnapshot();
    });

    const afterPersist = await getClientGroupAndHeadHashes();

    expect(afterPersist.clientGroup).to.deep.equal({
      ...clientGroupSnapshot,
      headHash: afterPersist.perdagClientGroupHeadHash,
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
    // memdagLocalCommit3Client0M2 rebased on to perdag client group
    // (with basis updatedPerdagClientGroupSnapshot)
    const afterPersistPerdagClientGroupBaseSnapshotHash = await withRead(
      perdag,
      async perdagRead => {
        const afterPersistPerdagClientGroupLocalCommit1 =
          await db.commitFromHash(
            afterPersist.perdagClientGroupHeadHash,
            perdagRead,
          );
        expectRebasedLocal(
          afterPersistPerdagClientGroupLocalCommit1,
          memdagLocalCommit3Client0M2,
        );

        assertNotNull(afterPersistPerdagClientGroupLocalCommit1.meta.basisHash);
        const afterPersistPerdagClientGroupBaseSnapshot =
          await db.commitFromHash(
            afterPersistPerdagClientGroupLocalCommit1.meta.basisHash,
            perdagRead,
          );
        assertSnapshotCommitDD31(afterPersistPerdagClientGroupBaseSnapshot);
        expect(
          afterPersistPerdagClientGroupBaseSnapshot.meta.cookieJSON,
        ).to.equal(updatedPerdagClientGroupCookie);
        return afterPersistPerdagClientGroupBaseSnapshot.chunk.hash;
      },
    );

    expect(
      await getChunkSnapshot(
        perdag,
        afterPersistPerdagClientGroupBaseSnapshotHash,
      ),
    ).to.deep.equal(updatedPerdagClientGroupSnapshot);
  });

  test('persist throws a ClientStateNotFoundError if client is missing', async () => {
    await setupSnapshots();

    await withWrite(perdag, async perdagWrite => {
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
  const chunksPersistedCalls: Hash[][] = [];
  sinon
    .stub(memdag, 'chunksPersisted')
    .callsFake((chunkHashes: Iterable<Hash>) => {
      const chunkHashesArray = [...chunkHashes];
      chunksPersistedCalls.push(chunkHashesArray);
      return dag.LazyStore.prototype.chunksPersisted.apply(memdag, [
        chunkHashesArray,
      ]);
    });

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

  let clientGroupID: undefined | sync.ClientGroupID;
  const createClient = async () => {
    const [cID, c] = await initClientDD31(
      new LogContext(),
      perdag,
      mutatorNames,
      {},
    );
    assert(clientGroupID === undefined || c.clientGroupID === clientGroupID);
    clientGroupID = c.clientGroupID;
    return {
      clientID: cID,
      client: c,
    };
  };
  const clients: {clientID: sync.ClientID; client: ClientDD31}[] = [];
  for (let i = 0; i < 3; i++) {
    clients.push(await createClient());
  }

  assertNotUndefined(clientGroupID);

  const testPersist = async (
    persistedExpectation: PersistedExpectation,
    onGatherMemOnlyChunksForTest = () => promiseVoid,
  ) => {
    chunksPersistedCalls.length = 0;
    const perdagChunkHashesPrePersist = perdag.chunkHashes();
    await persistDD31(
      new LogContext(),
      clients[0].clientID,
      memdag,
      perdag,
      mutators,
      () => false,
      onGatherMemOnlyChunksForTest,
    );
    const persistedChunkHashes = new Set<Hash>();
    const clientGroupsHeadHash = await withRead(perdag, read => {
      return read.getHead(CLIENT_GROUPS_HEAD_NAME);
    });
    for (const hash of perdag.chunkHashes()) {
      if (
        !perdagChunkHashesPrePersist.has(hash) &&
        hash !== clientGroupsHeadHash
      ) {
        persistedChunkHashes.add(hash);
      }
    }
    switch (persistedExpectation) {
      case PersistedExpectation.SNAPSHOT:
        expect(persistedChunkHashes.size).to.be.greaterThan(0);
        expect(chunksPersistedCalls.length).to.equal(1);
        expect(new Set(chunksPersistedCalls[0])).to.deep.equal(
          persistedChunkHashes,
        );
        break;
      case PersistedExpectation.SNAPSHOT_AND_LOCALS:
        expect(persistedChunkHashes.size).to.be.greaterThan(0);
        expect(chunksPersistedCalls.length).to.equal(1);
        // Persisted chunks is a superset of chunks passed to
        // chunksPersisted
        expect([...persistedChunkHashes]).to.include.members(
          chunksPersistedCalls[0],
        );
        break;
      case PersistedExpectation.LOCALS:
        expect(persistedChunkHashes.size).to.be.greaterThan(0);
        expect(chunksPersistedCalls.length).to.equal(0);
        break;
      case PersistedExpectation.NOTHING:
        expect(persistedChunkHashes.size).to.equal(0);
        expect(chunksPersistedCalls.length).to.equal(0);
        break;
    }
  };

  return {
    memdag,
    perdag,
    memdagChainBuilder: new ChainBuilder(memdag),
    perdagClientGroupChainBuilder: new ChainBuilder(
      perdag,
      PERDAG_TEST_SETUP_HEAD_NAME,
    ),
    clients,
    clientGroupID,
    testPersist,
  };
}
function getClientGroupHelper(perdag: dag.TestStore, clientGroupID: string) {
  return withRead(perdag, perdagRead => {
    return getClientGroup(clientGroupID, perdagRead);
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
