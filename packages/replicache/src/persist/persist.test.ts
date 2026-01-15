import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {
  assert,
  assertNotNull,
  assertNotUndefined,
} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import {BTreeRead} from '../btree/read.ts';
import {LazyStore, LazyWrite} from '../dag/lazy-store.ts';
import {TestStore} from '../dag/test-store.ts';
import type {Commit} from '../db/commit.ts';
import {
  DEFAULT_HEAD_NAME,
  type Meta,
  assertLocalMetaDD31,
  assertSnapshotCommitDD31,
  baseSnapshotFromHash,
  commitFromHash,
} from '../db/commit.ts';
import {
  ChainBuilder,
  createMutatorName,
  getChunkSnapshot,
} from '../db/test-helpers.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {type Hash, assertHash, makeNewFakeHashFunction} from '../hash.ts';
import type {ClientGroupID, ClientID} from '../sync/ids.ts';
import type {WriteTransaction} from '../transactions.ts';
import type {MutatorDefs} from '../types.ts';
import {withRead, withWriteNoImplicitCommit} from '../with-transactions.ts';
import {
  CLIENT_GROUPS_HEAD_NAME,
  type ClientGroup,
  getClientGroup,
  setClientGroup,
} from './client-groups.ts';
import {
  CLIENTS_HEAD_NAME,
  type Client,
  type ClientMap,
  ClientStateNotFoundError,
  type ClientV6,
  assertClientV6,
  getClients,
  initClientV6,
  setClients,
} from './clients.ts';
import {makeClientID} from './make-client-id.ts';
import {persistDD31} from './persist.ts';
import * as PersistedExpectation from './persisted-expectation-enum.ts';

type FormatVersion = Enum<typeof FormatVersion>;
type PersistedExpectation = Enum<typeof PersistedExpectation>;

const PERDAG_TEST_SETUP_HEAD_NAME = 'test-setup-head';

describe('persistDD31', () => {
  let memdag: LazyStore,
    perdag: TestStore,
    memdagChainBuilder: ChainBuilder,
    perdagClientGroupChainBuilder: ChainBuilder,
    clients: {clientID: ClientID; client: Client}[],
    clientGroupID: ClientGroupID,
    testPersist: (
      persistedExpectation: PersistedExpectation,
      onGatherMemOnlyChunksForTest?: () => Promise<void>,
    ) => Promise<void>;

  beforeEach(async () => {
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

  afterEach(async () => {
    await memdag.close();
    await perdag.close();
    vi.restoreAllMocks();
  });

  async function setupSnapshots(options?: {
    memdagCookie?: string;
    perdagClientGroupCookie?: string;
    memdagValueMap?: [string, JSONValue][];
    memdagMutationIDs?: Record<ClientID, number>;
    perdagClientGroupMutationIDs?: Record<ClientID, number>;
  }) {
    const {
      memdagCookie = 'cookie1',
      perdagClientGroupCookie = 'cookie1',
      memdagValueMap = [],
      memdagMutationIDs = {},
      perdagClientGroupMutationIDs = {},
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
    await withWriteNoImplicitCommit(perdag, async perdagWrite => {
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

  async function getClientMapClientGroupAndHeadHashes() {
    const memdagHeadHash = await withRead(memdag, memdagRead =>
      memdagRead.getHead(DEFAULT_HEAD_NAME),
    );
    assertNotUndefined(memdagHeadHash);

    const [clientGroup, clientMap] = await withRead(
      perdag,
      async perdagRead => {
        const clientGroup = await getClientGroup(clientGroupID, perdagRead);
        assertNotUndefined(clientGroup);
        return [clientGroup, await getClients(perdagRead)];
      },
    );
    const perdagClientGroupHeadHash = clientGroup.headHash;
    return {memdagHeadHash, perdagClientGroupHeadHash, clientGroup, clientMap};
  }

  test('equal snapshot cookies no locals', async () => {
    const {perdagClientGroupHeadHash, memdagHeadHash} = await setupSnapshots();
    await perdagClientGroupChainBuilder.removeHead();

    const {clientMap, clientGroup} = await getClientMapAndClientGroup(
      perdag,
      clientGroupID,
    );
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);
    const perdagClientGroupSnapshot = await getChunkSnapshot(
      perdag,
      perdagClientGroupHeadHash,
    );

    await testPersist(PersistedExpectation.Nothing);

    const afterPersist = await getClientMapClientGroupAndHeadHashes();
    // memdag, perdag client group, perdag client map all unchanged
    expect(afterPersist.clientGroup).toEqual(clientGroup);
    expect(afterPersist.clientMap).toEqual(clientMap);
    expect(afterPersist.memdagHeadHash).toBe(memdagHeadHash);
    expect(await getChunkSnapshot(memdag, afterPersist.memdagHeadHash)).toEqual(
      memdagSnapshot,
    );
    expect(
      await getChunkSnapshot(perdag, afterPersist.perdagClientGroupHeadHash),
    ).toEqual(perdagClientGroupSnapshot);
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

    const {clientMap, clientGroup} = await getClientMapAndClientGroup(
      perdag,
      clientGroupID,
    );
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);

    await testPersist(PersistedExpectation.Locals);

    const afterPersist = await getClientMapClientGroupAndHeadHashes();
    expect(afterPersist.clientGroup).toEqual({
      ...clientGroup,
      mutationIDs: {
        [clients[0].clientID]: 2,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
      headHash: afterPersist.perdagClientGroupHeadHash,
    });
    expect(afterPersist.clientMap).toEqual(clientMap);
    // memdag unchanged
    expect(afterPersist.memdagHeadHash).toBe(memdagHeadHash);
    expect(await getChunkSnapshot(memdag, afterPersist.memdagHeadHash)).toEqual(
      memdagSnapshot,
    );
    // memdagLocalCommit3Client0M2 rebased on to perdag client group rest of
    // perdag client group unchanged
    await withRead(perdag, async perdagRead => {
      const afterPersistPerdagClientGroupLocalCommit4 = await commitFromHash(
        afterPersist.perdagClientGroupHeadHash,
        perdagRead,
      );
      expectRebasedLocal(
        afterPersistPerdagClientGroupLocalCommit4,
        memdagLocalCommit3Client0M2,
      );
      assertNotNull(afterPersistPerdagClientGroupLocalCommit4.meta.basisHash);
      const afterPersistPerdagClientGroupLocalCommit3 = await commitFromHash(
        afterPersistPerdagClientGroupLocalCommit4.meta.basisHash,
        perdagRead,
      );
      expect(afterPersistPerdagClientGroupLocalCommit3.chunk.hash).toBe(
        perdagClientGroupHeadHash,
      );
      expect(afterPersistPerdagClientGroupLocalCommit3).toEqual(
        perdagClientGroupLocalCommit3Client2M1,
      );
    });
  });

  test('memdag older snapshot no locals', async () => {
    const {perdagClientGroupHeadHash, memdagHeadHash} = await setupSnapshots({
      perdagClientGroupCookie: 'cookie2',
      memdagCookie: 'cookie1',
    });
    await perdagClientGroupChainBuilder.removeHead();

    const {clientMap, clientGroup} = await getClientMapAndClientGroup(
      perdag,
      clientGroupID,
    );
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);
    const perdagClientGroupSnapshot = await getChunkSnapshot(
      perdag,
      perdagClientGroupHeadHash,
    );

    await testPersist(PersistedExpectation.Nothing);

    const afterPersist = await getClientMapClientGroupAndHeadHashes();
    // memdag and perdag client group both unchanged
    expect(afterPersist.clientGroup).toEqual(clientGroup);
    expect(afterPersist.clientMap).toEqual(clientMap);
    expect(afterPersist.memdagHeadHash).toBe(memdagHeadHash);
    expect(await getChunkSnapshot(memdag, afterPersist.memdagHeadHash)).toEqual(
      memdagSnapshot,
    );
    expect(
      await getChunkSnapshot(perdag, afterPersist.perdagClientGroupHeadHash),
    ).toEqual(perdagClientGroupSnapshot);
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

    const {clientMap, clientGroup} = await getClientMapAndClientGroup(
      perdag,
      clientGroupID,
    );
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);

    await testPersist(PersistedExpectation.Locals);

    const afterPersist = await getClientMapClientGroupAndHeadHashes();
    expect(afterPersist.clientGroup).toEqual({
      ...clientGroup,
      mutationIDs: {
        [clients[0].clientID]: 2,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
      headHash: afterPersist.perdagClientGroupHeadHash,
    });
    expect(afterPersist.clientMap).toEqual(clientMap);
    // memdag unchanged
    expect(afterPersist.memdagHeadHash).toBe(memdagHeadHash);
    expect(await getChunkSnapshot(memdag, afterPersist.memdagHeadHash)).toEqual(
      memdagSnapshot,
    );
    // memdagLocalCommit3Client0M2 rebased on to perdag client group rest of
    // perdag client group unchanged
    await withRead(perdag, async perdagRead => {
      const afterPersistPerdagClientGroupLocalCommit4 = await commitFromHash(
        afterPersist.perdagClientGroupHeadHash,
        perdagRead,
      );
      expectRebasedLocal(
        afterPersistPerdagClientGroupLocalCommit4,
        memdagLocalCommit3Client0M2,
      );
      assertNotNull(afterPersistPerdagClientGroupLocalCommit4.meta.basisHash);
      const afterPersistPerdagClientGroupLocalCommit3 = await commitFromHash(
        afterPersistPerdagClientGroupLocalCommit4.meta.basisHash,
        perdagRead,
      );
      expect(afterPersistPerdagClientGroupLocalCommit3.chunk.hash).toBe(
        perdagClientGroupHeadHash,
      );
      expect(afterPersistPerdagClientGroupLocalCommit3).toEqual(
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

    const {memdagHeadHash} = await setupMemdagLocals();

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

    const {clientMap, clientGroup} = await getClientMapAndClientGroup(
      perdag,
      clientGroupID,
    );
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);

    await testPersist(PersistedExpectation.Nothing);

    const afterPersist = await getClientMapClientGroupAndHeadHashes();
    expect(afterPersist.clientGroup).toEqual({
      ...clientGroup,
      mutationIDs: {
        [clients[0].clientID]: 2,
        [clients[1].clientID]: 1,
        [clients[2].clientID]: 1,
      },
      headHash: afterPersist.perdagClientGroupHeadHash,
    });
    expect(afterPersist.clientMap).toEqual(clientMap);
    // memdag unchanged
    expect(afterPersist.memdagHeadHash).toBe(memdagHeadHash);
    expect(await getChunkSnapshot(memdag, afterPersist.memdagHeadHash)).toEqual(
      memdagSnapshot,
    );
    // perdag unchanged, no snapshot to persist and nothing to rebase
    expect(afterPersist.perdagClientGroupHeadHash).toBe(
      perdagClientGroupHeadHash,
    );
  });

  test('memdag newer snapshot no locals', async () => {
    const formatVersion = FormatVersion.Latest;
    const {memdagHeadHash: memdagSnapshotCommitHash} = await setupSnapshots({
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

    const {clientMap, clientGroup} = await getClientMapAndClientGroup(
      perdag,
      clientGroupID,
    );
    await testPersist(PersistedExpectation.Snapshot);

    const afterPersist = await getClientMapClientGroupAndHeadHashes();

    expect(afterPersist.clientGroup).toEqual({
      ...clientGroup,
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
    expectUpdatedClientPersistHash(
      clientMap,
      clients,
      memdagSnapshotCommitHash,
      afterPersist.clientMap,
    );
    // memdag and perdag client group snapshots should be identical (memdag
    // snapshot written to perdag client group with temp hashes replace with
    // permanent hashes, and then memdag fixed up with permanent hashes)
    expect(await getChunkSnapshot(memdag, afterPersist.memdagHeadHash)).toEqual(
      await getChunkSnapshot(perdag, afterPersist.perdagClientGroupHeadHash),
    );
    // expect values from memdag snapshot are persisted to perdag client group
    await withRead(perdag, async perdagRead => {
      const commit = await commitFromHash(
        afterPersist.perdagClientGroupHeadHash,
        perdagRead,
      );
      const btreeRead = new BTreeRead(
        perdagRead,
        formatVersion,
        commit.valueHash,
      );
      expect(await btreeRead.get('k1')).toBe('value1');
      expect(await btreeRead.get('k2')).toBe('value2');
    });
  });

  test('memdag newer snapshot with locals', async () => {
    const formatVersion = FormatVersion.Latest;
    const memdagCookie = 'cookie2';
    const memdagMutationIDs = {
      [clients[0].clientID]: 1,
      [clients[2].clientID]: 2,
    };
    const {memdagHeadHash: memdagSnapshotCommitHash} = await setupSnapshots({
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

    const {clientMap, clientGroup} = await getClientMapAndClientGroup(
      perdag,
      clientGroupID,
    );
    await testPersist(PersistedExpectation.SnapshotAndLocals);

    const afterPersist = await getClientMapClientGroupAndHeadHashes();

    expect(afterPersist.clientGroup).toEqual({
      ...clientGroup,
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
    expectUpdatedClientPersistHash(
      clientMap,
      clients,
      memdagSnapshotCommitHash,
      afterPersist.clientMap,
    );
    const afterPersistPerdagBaseSnapshotHash = await withRead(
      perdag,
      async perdagRead => {
        const afterPersistPerdagClientGroupLocalCommit2 = await commitFromHash(
          afterPersist.perdagClientGroupHeadHash,
          perdagRead,
        );
        expectRebasedLocal(
          afterPersistPerdagClientGroupLocalCommit2,
          memdagLocalCommit2Client0M2,
        );
        assertNotNull(afterPersistPerdagClientGroupLocalCommit2.meta.basisHash);
        const afterPersistPerdagClientGroupLocalCommit1 = await commitFromHash(
          afterPersistPerdagClientGroupLocalCommit2.meta.basisHash,
          perdagRead,
        );
        expectRebasedLocal(
          afterPersistPerdagClientGroupLocalCommit1,
          perdagClientGroupLocalCommit2Client1M1,
        );
        assertNotNull(afterPersistPerdagClientGroupLocalCommit1.meta.basisHash);
        const afterPersistPerdagClientGroupBaseSnapshot = await commitFromHash(
          afterPersistPerdagClientGroupLocalCommit1.meta.basisHash,
          perdagRead,
        );
        assertSnapshotCommitDD31(afterPersistPerdagClientGroupBaseSnapshot);
        expect(afterPersistPerdagClientGroupBaseSnapshot.meta.cookieJSON).toBe(
          memdagCookie,
        );
        expect(
          afterPersistPerdagClientGroupBaseSnapshot.meta.lastMutationIDs,
        ).toEqual(memdagMutationIDs);

        // expect values from memdag snapshot are persisted to perdag client group
        const commit = await commitFromHash(
          afterPersist.perdagClientGroupHeadHash,
          perdagRead,
        );
        const btreeRead = new BTreeRead(
          perdagRead,
          formatVersion,
          commit.valueHash,
        );
        expect(await btreeRead.get('k1')).toBe('value1');
        expect(await btreeRead.get('k2')).toBe('value2');
        return afterPersistPerdagClientGroupBaseSnapshot.chunk.hash;
      },
    );

    const afterPersistMemdagBaseSnapshotHash = await withRead(
      memdag,
      async memdagRead => {
        const baseSnapshot = await baseSnapshotFromHash(
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
    ).toEqual(
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

    const {clientMap, clientGroup} = await getClientMapAndClientGroup(
      perdag,
      clientGroupID,
    );
    const memdagSnapshot = await getChunkSnapshot(memdag, memdagHeadHash);

    await testPersist(PersistedExpectation.Locals, async () => {
      await ensurePerdagClientGroupUpdatedToNewerSnapshot();
    });

    const afterPersist = await getClientMapClientGroupAndHeadHashes();

    expect(afterPersist.clientGroup).toEqual({
      ...clientGroup,
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
    expect(afterPersist.clientMap).toEqual(clientMap);
    // memdag unchanged
    expect(afterPersist.memdagHeadHash).toBe(memdagHeadHash);
    expect(await getChunkSnapshot(memdag, afterPersist.memdagHeadHash)).toEqual(
      memdagSnapshot,
    );
    // memdagLocalCommit3Client0M2 rebased on to perdag client group
    // (with basis updatedPerdagClientGroupSnapshot)
    const afterPersistPerdagClientGroupBaseSnapshotHash = await withRead(
      perdag,
      async perdagRead => {
        const afterPersistPerdagClientGroupLocalCommit1 = await commitFromHash(
          afterPersist.perdagClientGroupHeadHash,
          perdagRead,
        );
        expectRebasedLocal(
          afterPersistPerdagClientGroupLocalCommit1,
          memdagLocalCommit3Client0M2,
        );

        assertNotNull(afterPersistPerdagClientGroupLocalCommit1.meta.basisHash);
        const afterPersistPerdagClientGroupBaseSnapshot = await commitFromHash(
          afterPersistPerdagClientGroupLocalCommit1.meta.basisHash,
          perdagRead,
        );
        assertSnapshotCommitDD31(afterPersistPerdagClientGroupBaseSnapshot);
        expect(afterPersistPerdagClientGroupBaseSnapshot.meta.cookieJSON).toBe(
          updatedPerdagClientGroupCookie,
        );
        return afterPersistPerdagClientGroupBaseSnapshot.chunk.hash;
      },
    );

    expect(
      await getChunkSnapshot(
        perdag,
        afterPersistPerdagClientGroupBaseSnapshotHash,
      ),
    ).toEqual(updatedPerdagClientGroupSnapshot);
  });

  test('persist throws a ClientStateNotFoundError if client is missing', async () => {
    await setupSnapshots();

    await withWriteNoImplicitCommit(perdag, async perdagWrite => {
      const clientMap = await getClients(perdagWrite);
      const newClientMap = new Map(clientMap);
      newClientMap.delete(clients[0].clientID);
      await setClients(newClientMap, perdagWrite);
      await perdagWrite.commit();
    });

    let err;
    try {
      await testPersist(PersistedExpectation.Nothing);
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(ClientStateNotFoundError);
    expect(err).toHaveProperty('id', clients[0].clientID);
  });
});

function expectUpdatedClientPersistHash(
  clientMap: ClientMap,
  clients: {clientID: ClientID; client: Client}[],
  memdagSnapshotCommitHash: Hash,
  afterPersistClientMap: ClientMap,
) {
  const expectedClientMap = new Map(clientMap);
  const persistingClient = clientMap.get(clients[0].clientID);
  assertClientV6(persistingClient);
  expectedClientMap.set(clients[0].clientID, {
    ...persistingClient,
    persistHash: memdagSnapshotCommitHash,
  });
  expect(afterPersistClientMap).toEqual(expectedClientMap);
}

async function setupPersistTest() {
  const formatVersion = FormatVersion.Latest;
  const hashFunction = makeNewFakeHashFunction();
  const perdag = new TestStore(undefined, hashFunction);
  const memdag = new LazyStore(
    perdag,
    100 * 2 ** 20, // 100 MB,
    hashFunction,
    assertHash,
  );
  const chunksPersistedSpy = vi.spyOn(LazyWrite.prototype, 'chunksPersisted');

  const mutatorNames = Array.from({length: 10}, (_, index) =>
    createMutatorName(index),
  );
  const mutators: MutatorDefs = {};
  for (let i = 0; i < mutatorNames.length; i++) {
    mutators[mutatorNames[i]] = async (
      tx: WriteTransaction,
      args: JSONValue,
    ) => {
      await tx.set(`key-${i}`, args);
    };
  }

  let clientGroupID: undefined | ClientGroupID;
  const createClient = async () => {
    const cID = makeClientID();
    const [c] = await initClientV6(
      cID,
      new LogContext(),
      perdag,
      mutatorNames,
      {},
      formatVersion,
      true,
    );
    assert(clientGroupID === undefined || c.clientGroupID === clientGroupID);
    clientGroupID = c.clientGroupID;
    return {
      clientID: cID,
      client: c,
    };
  };
  const clients: {clientID: ClientID; client: ClientV6}[] = [];
  for (let i = 0; i < 3; i++) {
    clients.push(await createClient());
  }

  assertNotUndefined(clientGroupID);

  const testPersist = async (
    persistedExpectation: PersistedExpectation,
    onGatherMemOnlyChunksForTest = () => promiseVoid,
  ) => {
    chunksPersistedSpy.mockClear();
    const perdagChunkHashesPrePersist = perdag.chunkHashes();
    await persistDD31(
      new LogContext(),
      clients[0].clientID,
      memdag,
      perdag,
      mutators,
      () => false,
      FormatVersion.Latest,
      undefined,
      onGatherMemOnlyChunksForTest,
    );
    const persistedChunkHashes: Hash[] = [];
    const clientGroupsHeadHash = await withRead(perdag, read =>
      read.getHead(CLIENT_GROUPS_HEAD_NAME),
    );
    const clientsHeadHash = await withRead(perdag, read =>
      read.getHead(CLIENTS_HEAD_NAME),
    );
    for (const hash of perdag.chunkHashes()) {
      if (
        !perdagChunkHashesPrePersist.has(hash) &&
        hash !== clientGroupsHeadHash &&
        hash !== clientsHeadHash
      ) {
        persistedChunkHashes.push(hash);
      }
    }
    switch (persistedExpectation) {
      case PersistedExpectation.Snapshot:
        expect(persistedChunkHashes.length).toBeGreaterThan(0);
        expect(chunksPersistedSpy).toHaveBeenCalledTimes(1);
        expect(chunksPersistedSpy.mock.calls[0][0]).toEqual(
          persistedChunkHashes,
        );
        break;
      case PersistedExpectation.SnapshotAndLocals:
        expect(persistedChunkHashes.length).toBeGreaterThan(0);
        expect(chunksPersistedSpy).toHaveBeenCalledTimes(1);
        // Persisted chunks is a superset of chunks passed to
        // chunksPersisted
        expect([...persistedChunkHashes]).toEqual(
          expect.arrayContaining([...chunksPersistedSpy.mock.calls[0][0]]),
        );
        break;
      case PersistedExpectation.Locals:
        expect(persistedChunkHashes.length).toBeGreaterThan(0);
        expect(chunksPersistedSpy).toHaveBeenCalledTimes(0);
        break;
      case PersistedExpectation.Nothing:
        expect(persistedChunkHashes.length).toBe(0);
        expect(chunksPersistedSpy).toHaveBeenCalledTimes(0);
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
function getClientMapAndClientGroup(
  perdag: TestStore,
  clientGroupID: string,
): Promise<{clientMap: ClientMap; clientGroup: ClientGroup | undefined}> {
  return withRead(perdag, async perdagRead => ({
    clientGroup: await getClientGroup(clientGroupID, perdagRead),
    clientMap: await getClients(perdagRead),
  }));
}

function expectRebasedLocal(rebased: Commit<Meta>, original: Commit<Meta>) {
  expect(rebased.chunk.hash).not.toBe(original.chunk.hash);
  const rebasedMeta = rebased.meta;
  assertLocalMetaDD31(rebasedMeta);
  const originalMeta = original.meta;
  assertLocalMetaDD31(originalMeta);
  expect(rebasedMeta.clientID).toBe(originalMeta.clientID);
  expect(rebasedMeta.mutationID).toBe(originalMeta.mutationID);
  expect(rebasedMeta.mutatorName).toBe(originalMeta.mutatorName);
  expect(rebasedMeta.mutatorArgsJSON).toEqual(originalMeta.mutatorArgsJSON);
}
