import {describe, expect, test, vi} from 'vitest';
import {assert} from '../../shared/src/asserts.ts';
import type {Enum} from '../../shared/src/enum.ts';
import {type JSONObject, assertJSONObject} from '../../shared/src/json.ts';
import {randomUint64} from '../../shared/src/random-uint64.ts';
import {LazyStore} from './dag/lazy-store.ts';
import {StoreImpl} from './dag/store-impl.ts';
import * as FormatVersion from './format-version-enum.ts';
import {
  createAndPersistClientWithPendingLocalDD31,
  createPerdag,
  createPushRequestBodyDD31,
  persistSnapshotDD31,
} from './mutation-recovery-test-helper.ts';
import {
  disableClientGroup,
  getClientGroup,
  getClientGroups,
} from './persist/client-groups.ts';
import {assertClientV6, getClient, getClients} from './persist/clients.ts';
import type {PullResponseV1} from './puller.ts';
import type {PushResponse} from './pusher.ts';
import type {ClientID} from './sync/ids.ts';
import {PULL_VERSION_DD31, type PullRequestV1} from './sync/pull.ts';
import {
  PUSH_VERSION_DD31,
  type PushRequestV1,
  assertPushRequestV1,
} from './sync/push.ts';
import {
  disableAllBackgroundProcesses,
  fetchMocker,
  initReplicacheTesting,
  replicacheForTesting,
  tickAFewTimes,
} from './test-util.ts';
import {withRead, withWriteNoImplicitCommit} from './with-transactions.ts';

type FormatVersion = Enum<typeof FormatVersion>;

// Add test for ClientV5, logic is same as ClientV6
describe('DD31', () => {
  initReplicacheTesting();

  async function testRecoveringMutationsOfClientV6(args: {
    schemaVersionOfClientWPendingMutations: string;
    schemaVersionOfClientRecoveringMutations: string;
    snapshotLastMutationIDs?: Record<ClientID, number> | undefined;
    snapshotLastMutationIDsAfterPull?: Record<ClientID, number> | undefined;
    pullLastMutationIDChanges?: Record<ClientID, number> | undefined;
    expectedLastServerAckdMutationIDs?: Record<ClientID, number> | undefined;
    pullResponse?: PullResponseV1 | undefined;
    pushResponse?: PushResponse | undefined;
    formatVersion: FormatVersion;
    expectClientGroupDisabled?: boolean;
  }) {
    vi.spyOn(console, 'error');

    const client1ID = 'client1';
    const client2ID = 'client2';

    const {
      schemaVersionOfClientWPendingMutations,
      schemaVersionOfClientRecoveringMutations,
      pullLastMutationIDChanges = {[client1ID]: 3, [client2ID]: 3},
      expectedLastServerAckdMutationIDs = {[client1ID]: 3, [client2ID]: 3},
      snapshotLastMutationIDsAfterPull,
      pullResponse: pullResponseArg,
      pushResponse,
      snapshotLastMutationIDs = {
        [client1ID]: 1,
        [client2ID]: 1,
      },
      formatVersion,
      expectClientGroupDisabled = false,
    } = args;

    assert(formatVersion >= FormatVersion.V6);

    const auth = '1';
    const pushURL = 'https://test.replicache.dev/push';
    const pullURL = 'https://test.replicache.dev/pull';
    const rep = await replicacheForTesting(
      `recoverMutations${schemaVersionOfClientRecoveringMutations}recovering${schemaVersionOfClientWPendingMutations}`,
      {
        auth,
        schemaVersion: schemaVersionOfClientRecoveringMutations,
        pushURL,
        pullURL,
        mutators: {
          dummy: async () => {
            //
          },
        },
      },
    );
    const profileID = await rep.profileID;

    await tickAFewTimes(vi);

    const testPerdag = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersionOfClientWPendingMutations,
      formatVersion,
    });

    const mutatorNames = ['mutator_name_2', 'mutator_name_3'];
    const client1PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client1ID,
        perdag: testPerdag,
        numLocal: 2,
        mutatorNames,
        cookie: 'cookie_0',
        formatVersion,
        snapshotLastMutationIDs,
      });

    const client2PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client2ID,
        perdag: testPerdag,
        numLocal: 2,
        mutatorNames,
        cookie: 'cookie_0',
        formatVersion,
        snapshotLastMutationIDs,
      });

    const client1 = await withRead(testPerdag, read =>
      getClient(client1ID, read),
    );
    assertClientV6(client1);

    const client2 = await withRead(testPerdag, read =>
      getClient(client2ID, read),
    );
    assertClientV6(client2);

    expect(client1.clientGroupID).toBe(client2.clientGroupID);
    expect(await rep.clientGroupID).not.toBe(client1.clientGroupID);

    const clientGroup = await withRead(testPerdag, read =>
      getClientGroup(client1.clientGroupID, read),
    );
    assert(clientGroup);

    fetchMocker.reset();
    fetchMocker.post(pushURL, pushResponse ?? 'ok');
    const pullResponse: PullResponseV1 = pullResponseArg ?? {
      cookie: 'cookie_2',
      lastMutationIDChanges: pullLastMutationIDChanges,
      patch: [],
    };

    fetchMocker.post(pullURL, async () => {
      if (snapshotLastMutationIDsAfterPull !== undefined) {
        await persistSnapshotDD31(
          client1ID,
          testPerdag,
          'cookie_1',
          mutatorNames,
          snapshotLastMutationIDsAfterPull,
          formatVersion,
        );
      }
      return pullResponse;
    });

    await rep.recoverMutations();

    const pushCalls = fetchMocker.bodies(pushURL);
    expect(pushCalls.length).toBe(1);
    expect(pushCalls[0]).toEqual({
      profileID,
      clientGroupID: client1.clientGroupID,
      mutations: [
        {
          clientID: client1ID,
          id: client1PendingLocalMetas[0].mutationID,
          name: client1PendingLocalMetas[0].mutatorName,
          args: client1PendingLocalMetas[0].mutatorArgsJSON,
          timestamp: client1PendingLocalMetas[0].timestamp,
        },
        {
          clientID: client1ID,
          id: client1PendingLocalMetas[1].mutationID,
          name: client1PendingLocalMetas[1].mutatorName,
          args: client1PendingLocalMetas[1].mutatorArgsJSON,
          timestamp: client1PendingLocalMetas[1].timestamp,
        },
        {
          clientID: client2ID,
          id: client2PendingLocalMetas[0].mutationID,
          name: client2PendingLocalMetas[0].mutatorName,
          args: client2PendingLocalMetas[0].mutatorArgsJSON,
          timestamp: client2PendingLocalMetas[0].timestamp,
        },
        {
          clientID: client2ID,
          id: client2PendingLocalMetas[1].mutationID,
          name: client2PendingLocalMetas[1].mutatorName,
          args: client2PendingLocalMetas[1].mutatorArgsJSON,
          timestamp: client2PendingLocalMetas[1].timestamp,
        },
      ],
      pushVersion: PUSH_VERSION_DD31,
      schemaVersion: schemaVersionOfClientWPendingMutations,
    });

    const pullCalls = fetchMocker.bodies(pullURL);

    if (pushResponse && pushResponse.error) {
      expect(pullCalls.length).toBe(0);
    } else {
      expect(pullCalls.length).toBe(1);
      const pullReq: PullRequestV1 = {
        profileID,
        clientGroupID: client1.clientGroupID,
        cookie: 'cookie_0',
        pullVersion: PULL_VERSION_DD31,
        schemaVersion: schemaVersionOfClientWPendingMutations,
      };
      expect(pullCalls[0]).toEqual(pullReq);
    }

    const updatedClient1 = await withRead(testPerdag, read =>
      getClient(client1ID, read),
    );
    assertClientV6(updatedClient1);

    expect(updatedClient1.clientGroupID).toEqual(client1.clientGroupID);

    const updatedClientGroup = await withRead(testPerdag, read =>
      getClientGroup(client1.clientGroupID, read),
    );

    assert(updatedClientGroup);

    const updatedClient2 = await withRead(testPerdag, read =>
      getClient(client2ID, read),
    );
    assertClientV6(updatedClient2);

    expect(updatedClient2.clientGroupID).toEqual(client2.clientGroupID);
    //expect(updatedClient2.headHash).toBe(client2.headHash);

    if ('error' in pullResponse || (pushResponse && 'error' in pushResponse)) {
      expect(updatedClientGroup.lastServerAckdMutationIDs).toEqual(
        clientGroup.lastServerAckdMutationIDs,
      );
    } else {
      expect(updatedClientGroup.lastServerAckdMutationIDs).toEqual(
        expectedLastServerAckdMutationIDs,
      );
    }
    expect(updatedClientGroup.mutationIDs).toEqual(clientGroup.mutationIDs);
    expect(updatedClientGroup.disabled).toBe(expectClientGroupDisabled);
  }

  for (const formatVersion of [FormatVersion.V6, FormatVersion.V7] as const) {
    describe(`v${formatVersion}`, () => {
      test('successfully recovering mutations of client with same schema version and replicache format version', async () => {
        await testRecoveringMutationsOfClientV6({
          schemaVersionOfClientWPendingMutations: 'testSchema1',
          schemaVersionOfClientRecoveringMutations: 'testSchema1',
          formatVersion,
        });
      });

      test('successfully recovering mutations of client with empty lastServerAckdMutationIDs', async () => {
        await testRecoveringMutationsOfClientV6({
          schemaVersionOfClientWPendingMutations: 'testSchema1',
          schemaVersionOfClientRecoveringMutations: 'testSchema1',
          snapshotLastMutationIDs: {},
          formatVersion,
        });
      });

      test('successfully recovering mutations of client with empty lastMutationIDChanges', async () => {
        await testRecoveringMutationsOfClientV6({
          schemaVersionOfClientWPendingMutations: 'testSchema1',
          schemaVersionOfClientRecoveringMutations: 'testSchema1',
          pullLastMutationIDChanges: {},
          expectedLastServerAckdMutationIDs: {
            client1: 1,
            client2: 1,
          },
          formatVersion,
        });
      });

      test('successfully recovering mutations some lastMutationIDChanges not applied to client groups lastServerAckdMutationIDs due to being smaller', async () => {
        await testRecoveringMutationsOfClientV6({
          schemaVersionOfClientWPendingMutations: 'testSchema1',
          schemaVersionOfClientRecoveringMutations: 'testSchema1',
          pullLastMutationIDChanges: {
            client1: 1,
            client2: 1,
          },
          snapshotLastMutationIDsAfterPull: {
            client1: 3,
            client2: 2,
          },
          expectedLastServerAckdMutationIDs: {
            client1: 3,
            client2: 2,
          },
          formatVersion,
        });
      });

      test('successfully recovering mutations no lastMutationIDChanges applied to client groups lastServerAckdMutationIDs due to being smaller', async () => {
        await testRecoveringMutationsOfClientV6({
          schemaVersionOfClientWPendingMutations: 'testSchema1',
          schemaVersionOfClientRecoveringMutations: 'testSchema1',
          pullLastMutationIDChanges: {
            client1: 2,
            client2: 1,
          },
          snapshotLastMutationIDsAfterPull: {
            client1: 1,
            client2: 2,
          },
          expectedLastServerAckdMutationIDs: {
            client1: 2,
            client2: 2,
          },
          formatVersion,
        });
      });

      test('successfully recovering mutations of client with different schema version but same replicache format version', async () => {
        await testRecoveringMutationsOfClientV6({
          schemaVersionOfClientWPendingMutations: 'testSchema1',
          schemaVersionOfClientRecoveringMutations: 'testSchema2',
          formatVersion,
        });
      });

      test('successfully recovering some but not all mutations of another client (pull does not acknowledge all)', async () => {
        await testRecoveringMutationsOfClientV6({
          schemaVersionOfClientWPendingMutations: 'testSchema1',
          schemaVersionOfClientRecoveringMutations: 'testSchema1',
          pullLastMutationIDChanges: {
            client1: 2,
            client2: 2,
          },
          expectedLastServerAckdMutationIDs: {
            client1: 2,
            client2: 2,
          },
          formatVersion,
        });
      });

      test('Pull returns VersionNotSupported', async () => {
        await testRecoveringMutationsOfClientV6({
          schemaVersionOfClientWPendingMutations: 'testSchema1',
          schemaVersionOfClientRecoveringMutations: 'testSchema1',
          pullResponse: {error: 'VersionNotSupported', versionType: 'pull'},
          formatVersion,
          expectClientGroupDisabled: true,
        });
      });

      test('Pull returns ClientStateNotFound', async () => {
        await testRecoveringMutationsOfClientV6({
          schemaVersionOfClientWPendingMutations: 'testSchema1',
          schemaVersionOfClientRecoveringMutations: 'testSchema1',
          pullResponse: {error: 'ClientStateNotFound'},
          formatVersion,
          expectClientGroupDisabled: true,
        });
      });

      test('Push returns VersionNotSupported', async () => {
        await testRecoveringMutationsOfClientV6({
          schemaVersionOfClientWPendingMutations: 'testSchema1',
          schemaVersionOfClientRecoveringMutations: 'testSchema1',
          pushResponse: {error: 'VersionNotSupported', versionType: 'pull'},
          formatVersion,
          expectClientGroupDisabled: true,
        });
      });

      test('Push returns ClientStateNotFound', async () => {
        await testRecoveringMutationsOfClientV6({
          schemaVersionOfClientWPendingMutations: 'testSchema1',
          schemaVersionOfClientRecoveringMutations: 'testSchema1',
          pushResponse: {error: 'ClientStateNotFound'},
          formatVersion,
          expectClientGroupDisabled: true,
        });
      });
    });
  }

  test('recovering mutations with pull disabled', async () => {
    const formatVersion = FormatVersion.Latest;
    const schemaVersionOfClientWPendingMutations = 'testSchema1';
    const schemaVersionOfClientRecoveringMutations = 'testSchema1';
    const client1ID = 'client1';
    const auth = '1';
    const pushURL = 'https://test.replicache.dev/push';
    const pullURL = ''; // pull disabled
    const rep = await replicacheForTesting(
      `recoverMutations${schemaVersionOfClientRecoveringMutations}recovering${schemaVersionOfClientWPendingMutations}`,
      {
        auth,
        schemaVersion: schemaVersionOfClientRecoveringMutations,
        pushURL,
        pullURL,
        mutators: {
          dummy() {
            //
          },
        },
      },
    );
    const profileID = await rep.profileID;

    await tickAFewTimes(vi);

    const testPerdag = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersionOfClientWPendingMutations,
      formatVersion,
    });

    const client1PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client1ID,
        perdag: testPerdag,
        numLocal: 2,
        mutatorNames: ['client1', 'mutator_name_2', 'mutator_name_3'],
        cookie: 1,
        formatVersion,
      });
    const client1 = await withRead(testPerdag, read =>
      getClient(client1ID, read),
    );
    assertClientV6(client1);

    fetchMocker.reset();
    fetchMocker.post(pushURL, 'ok');
    // Any unmatched URL will throw (catch-all)
    fetchMocker.post(undefined, () => {
      throw new Error('unexpected fetch in test');
    });

    await rep.recoverMutations();

    const pushCalls = fetchMocker.bodies(pushURL);
    expect(pushCalls.length).toBe(1);
    expect(pushCalls[0]).toEqual({
      profileID,
      clientGroupID: client1.clientGroupID,
      mutations: [
        {
          clientID: client1ID,
          id: client1PendingLocalMetas[0].mutationID,
          name: client1PendingLocalMetas[0].mutatorName,
          args: client1PendingLocalMetas[0].mutatorArgsJSON,
          timestamp: client1PendingLocalMetas[0].timestamp,
        },
        {
          clientID: client1ID,
          id: client1PendingLocalMetas[1].mutationID,
          name: client1PendingLocalMetas[1].mutatorName,
          args: client1PendingLocalMetas[1].mutatorArgsJSON,
          timestamp: client1PendingLocalMetas[1].timestamp,
        },
      ],
      pushVersion: PUSH_VERSION_DD31,
      schemaVersion: schemaVersionOfClientWPendingMutations,
    });

    // No need for unmatched check - the catch-all handler above would throw on any unexpected fetch

    const updatedClient1 = await withRead(testPerdag, read =>
      getClient(client1ID, read),
    );
    // unchanged
    expect(updatedClient1).toEqual(client1);
  });

  test('client does not attempt to recover mutations from IndexedDB with different replicache name', async () => {
    const formatVersion = FormatVersion.Latest;
    const clientWPendingMutationsID = 'client1';
    const schemaVersion = 'testSchema';
    const replicacheNameOfClientWPendingMutations = `${randomUint64().toString(
      36,
    )}:diffName-pendingClient`;
    const replicachePartialNameOfClientRecoveringMutations =
      'diffName-recoveringClient';

    const auth = '1';
    const pushURL = 'https://test.replicache.dev/push';
    const pullURL = 'https://test.replicache.dev/pull';
    const rep = await replicacheForTesting(
      replicachePartialNameOfClientRecoveringMutations,
      {
        auth,
        schemaVersion,
        pushURL,
        pullURL,
        mutators: {
          dummy() {
            //
          },
        },
      },
    );

    await tickAFewTimes(vi);

    const testPerdag = await createPerdag({
      replicacheName: replicacheNameOfClientWPendingMutations,
      schemaVersion,
      formatVersion,
    });

    await createAndPersistClientWithPendingLocalDD31({
      clientID: clientWPendingMutationsID,
      perdag: testPerdag,
      numLocal: 2,
      mutatorNames: ['client1', 'mutator_name_2', 'mutator_name_3'],
      cookie: 1,
      formatVersion,
    });
    const clientWPendingMutations = await withRead(testPerdag, read =>
      getClient(clientWPendingMutationsID, read),
    );
    assertClientV6(clientWPendingMutations);

    fetchMocker.reset();
    fetchMocker.post(pushURL, 'ok');
    const pullResponse: PullResponseV1 = {
      cookie: 'pull_cookie_1',
      lastMutationIDChanges: {},
      patch: [],
    };
    fetchMocker.post(pullURL, pullResponse);

    await rep.recoverMutations();

    expect(fetchMocker.bodies(pushURL).length).toBe(0);
    expect(fetchMocker.bodies(pullURL).length).toBe(0);
  });

  test('successfully recovering mutations of multiple clients with mix of schema versions and same replicache format version', async () => {
    // Same version as the Replicache instance.
    const formatVersion = FormatVersion.Latest;
    // These all have different mutator names to force unique client groups.
    const schemaVersionOfClients1Thru3AndClientRecoveringMutations =
      'testSchema1';
    const schemaVersionOfClient4 = 'testSchema2';
    // client1 has same schema version as recovering client and 2 mutations to recover
    const client1ID = 'client1';
    // client2 has same schema version as recovering client and no mutations to recover
    const client2ID = 'client2';
    // client3 has same schema version as recovering client and 1 mutation to recover
    const client3ID = 'client3';
    // client4 has different schema version than recovering client and 2 mutations to recover
    const client4ID = 'client4';
    const replicachePartialName = 'recoverMutationsMix';
    const auth = '1';
    const pushURL = 'https://test.replicache.dev/push';
    const pullURL = 'https://test.replicache.dev/pull';
    const rep = await replicacheForTesting(replicachePartialName, {
      auth,
      schemaVersion: schemaVersionOfClients1Thru3AndClientRecoveringMutations,
      pushURL,
      pullURL,
      mutators: {
        dummy() {
          //
        },
      },
    });
    const profileID = await rep.profileID;

    await tickAFewTimes(vi);

    const testPerdagForClients1Thru3 = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersionOfClients1Thru3AndClientRecoveringMutations,
      formatVersion: FormatVersion.V6,
    });

    const client1PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client1ID,
        perdag: testPerdagForClients1Thru3,
        numLocal: 2,
        mutatorNames: ['client1', 'mutator_name_2', 'mutator_name_3'],
        cookie: 1,
        formatVersion,
      });
    const client2PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client2ID,
        perdag: testPerdagForClients1Thru3,
        numLocal: 0,
        mutatorNames: ['client2'],
        cookie: 2,
        formatVersion,
      });
    expect(client2PendingLocalMetas.length).toBe(0);
    const client3PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client3ID,
        perdag: testPerdagForClients1Thru3,
        numLocal: 1,
        mutatorNames: ['client3', 'mutator_name_2'],
        cookie: 3,
        formatVersion,
      });

    const testPerdagForClient4 = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersionOfClient4,
      formatVersion: FormatVersion.V6,
    });
    const client4PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client4ID,
        perdag: testPerdagForClient4,
        numLocal: 2,
        mutatorNames: ['client4', 'mutator_name_2', 'mutator_name_3'],
        cookie: 4,
        formatVersion,
      });

    const clients1Thru3 = await withRead(testPerdagForClients1Thru3, read =>
      getClients(read),
    );
    const client1 = clients1Thru3.get(client1ID);
    assertClientV6(client1);
    const client2 = clients1Thru3.get(client2ID);
    assertClientV6(client2);
    const client3 = clients1Thru3.get(client3ID);
    assertClientV6(client3);
    const {clientGroup1, clientGroup2, clientGroup3} = await withRead(
      testPerdagForClients1Thru3,
      async read => {
        const clientGroup1 = await getClientGroup(client1.clientGroupID, read);
        assert(clientGroup1);
        const clientGroup2 = await getClientGroup(client2.clientGroupID, read);
        assert(clientGroup2);
        const clientGroup3 = await getClientGroup(client3.clientGroupID, read);
        assert(clientGroup3);
        return {clientGroup1, clientGroup2, clientGroup3};
      },
    );

    const client4 = await withRead(testPerdagForClient4, read =>
      getClient(client4ID, read),
    );
    assertClientV6(client4);
    const clientGroup4 = await withRead(testPerdagForClient4, read =>
      getClientGroup(client4.clientGroupID, read),
    );
    assert(clientGroup4);

    const pullRequestJsonBodies: JSONObject[] = [];
    fetchMocker.reset();
    fetchMocker.post(pushURL, 'ok');
    fetchMocker.post(pullURL, (_url: string, body: unknown) => {
      assertJSONObject(body);
      pullRequestJsonBodies.push(body);
      const {clientGroupID} = body;
      switch (clientGroupID) {
        case client1.clientGroupID:
          return {
            cookie: 'pull_cookie_1',
            lastMutationIDChanges: clientGroup1.mutationIDs,
            patch: [],
          };
        case client3.clientGroupID:
          return {
            cookie: 'pull_cookie_3',
            lastMutationIDChanges: clientGroup3.mutationIDs,
            patch: [],
          };
        case client4.clientGroupID:
          return {
            cookie: 'pull_cookie_4',
            lastMutationIDChanges: clientGroup4.mutationIDs,
            patch: [],
          };
        default:
          throw new Error(`Unexpected pull ${body}`);
      }
    });

    await rep.recoverMutations();

    const pushCalls = fetchMocker.bodies(pushURL);
    expect(pushCalls.length).toBe(3);
    expect(pushCalls[0]).toEqual(
      createPushRequestBodyDD31(
        profileID,
        client1.clientGroupID,
        client1ID,
        client1PendingLocalMetas,
        schemaVersionOfClients1Thru3AndClientRecoveringMutations,
      ),
    );
    expect(pushCalls[1]).toEqual(
      createPushRequestBodyDD31(
        profileID,
        client3.clientGroupID,
        client3ID,
        client3PendingLocalMetas,
        schemaVersionOfClients1Thru3AndClientRecoveringMutations,
      ),
    );
    expect(pushCalls[2]).toEqual(
      createPushRequestBodyDD31(
        profileID,
        client4.clientGroupID,
        client4ID,
        client4PendingLocalMetas,
        schemaVersionOfClient4,
      ),
    );

    expect(pullRequestJsonBodies.length).toBe(3);
    expect(pullRequestJsonBodies[0]).toEqual({
      clientGroupID: client1.clientGroupID,
      profileID,
      schemaVersion: schemaVersionOfClients1Thru3AndClientRecoveringMutations,
      cookie: 1,
      pullVersion: 1,
    });
    expect(pullRequestJsonBodies[1]).toEqual({
      clientGroupID: client3.clientGroupID,
      profileID,
      schemaVersion: schemaVersionOfClients1Thru3AndClientRecoveringMutations,
      cookie: 3,
      pullVersion: 1,
    });
    expect(pullRequestJsonBodies[2]).toEqual({
      profileID,
      clientGroupID: client4.clientGroupID,
      schemaVersion: schemaVersionOfClient4,
      cookie: 4,
      pullVersion: 1,
    });

    const updateClients1Thru3 = await withRead(
      testPerdagForClients1Thru3,
      read => getClients(read),
    );
    const updatedClient1 = updateClients1Thru3.get(client1ID);
    assertClientV6(updatedClient1);
    const updatedClient2 = updateClients1Thru3.get(client2ID);
    assertClientV6(updatedClient2);
    const updatedClient3 = updateClients1Thru3.get(client3ID);
    assertClientV6(updatedClient3);

    const updatedClientGroups = await withRead(
      testPerdagForClients1Thru3,
      read => getClientGroups(read),
    );
    const updatedClientGroup1 = updatedClientGroups.get(client1.clientGroupID);
    assert(updatedClientGroup1);
    const updatedClientGroup2 = updatedClientGroups.get(client2.clientGroupID);
    assert(updatedClientGroup2);
    const updatedClientGroup3 = updatedClientGroups.get(client3.clientGroupID);
    assert(updatedClientGroup3);

    const updatedClient4 = await withRead(testPerdagForClient4, read =>
      getClient(client4ID, read),
    );
    assertClientV6(updatedClient4);
    const updatedClientGroup4 = await withRead(testPerdagForClient4, read =>
      getClientGroup(client4.clientGroupID, read),
    );
    assert(updatedClientGroup4);

    expect(updatedClient1).toEqual(client1);
    expect(updatedClientGroup1).toEqual({
      ...clientGroup1,
      lastServerAckdMutationIDs: {
        ...clientGroup1.lastServerAckdMutationIDs,
        // lastServerAckdMutationIDs is updated to high mutationID as mutations
        // were recovered
        [client1ID]: clientGroup1.mutationIDs[client1ID],
      },
    });

    expect(updatedClient2).toEqual(client2);
    expect(updatedClientGroup2).toEqual(clientGroup2);

    expect(updatedClient3).toEqual(client3);
    expect(updatedClientGroup3).toEqual({
      ...clientGroup3,
      lastServerAckdMutationIDs: {
        ...clientGroup3.lastServerAckdMutationIDs,
        // lastServerAckdMutationIDs is updated to high mutationID as mutations
        // were recovered
        [client3ID]: clientGroup3.mutationIDs[client3ID],
      },
    });

    expect(updatedClient4).toEqual(client4);
    expect(updatedClientGroup4).toEqual({
      ...clientGroup4,
      lastServerAckdMutationIDs: {
        ...clientGroup4.lastServerAckdMutationIDs,
        // lastServerAckdMutationIDs is updated to high mutationID as mutations
        // were recovered
        [client4ID]: clientGroup4.mutationIDs[client4ID],
      },
    });
  });

  test('if a push error occurs, continues to try to recover other clients', async () => {
    const formatVersion = FormatVersion.Latest;
    const schemaVersion = 'testSchema1';
    // client1 has same schema version as recovering client and 2 mutations to recover
    const client1ID = 'client1';
    // client2 has same schema version as recovering client and 1 mutation to recover
    const client2ID = 'client2';
    // client3 has same schema version as recovering client and 1 mutation to recover
    const client3ID = 'client3';
    const replicachePartialName = 'recoverMutationsRobustToPushError';
    const auth = '1';
    const pushURL = 'https://test.replicache.dev/push';
    const pullURL = 'https://test.replicache.dev/pull';
    const rep = await replicacheForTesting(replicachePartialName, {
      auth,
      schemaVersion,
      pushURL,
      pullURL,
    });
    const profileID = await rep.profileID;

    await tickAFewTimes(vi);

    const testPerdag = await createPerdag({
      replicacheName: rep.name,
      schemaVersion,
      formatVersion: FormatVersion.V6,
    });

    const client1PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client1ID,
        perdag: testPerdag,
        numLocal: 2,
        mutatorNames: ['client1', 'mutator_name_2', 'mutator_name_3'],
        cookie: 1,
        formatVersion,
      });
    const client2PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client2ID,
        perdag: testPerdag,
        numLocal: 1,
        mutatorNames: ['client2', 'mutator_name_2'],
        cookie: 2,
        formatVersion,
      });
    const client3PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client3ID,
        perdag: testPerdag,
        numLocal: 1,
        mutatorNames: ['client3', 'mutator_name_2'],
        cookie: 3,
        formatVersion,
      });

    const clients = await withRead(testPerdag, read => getClients(read));
    const client1 = clients.get(client1ID);
    assertClientV6(client1);
    const client2 = clients.get(client2ID);
    assertClientV6(client2);
    const client3 = clients.get(client3ID);
    assertClientV6(client3);

    const {clientGroup1, clientGroup2, clientGroup3} = await withRead(
      testPerdag,
      async read => {
        const clientGroup1 = await getClientGroup(client1.clientGroupID, read);
        assert(clientGroup1);
        const clientGroup2 = await getClientGroup(client2.clientGroupID, read);
        assert(clientGroup2);
        const clientGroup3 = await getClientGroup(client3.clientGroupID, read);
        assert(clientGroup3);
        return {
          clientGroup1,
          clientGroup2,
          clientGroup3,
        };
      },
    );

    const pushRequestJSONBodies: PushRequestV1[] = [];
    const pullRequestJsonBodies: JSONObject[] = [];
    fetchMocker.reset();
    fetchMocker.post(pushURL, (_url: string, body: unknown) => {
      assertPushRequestV1(body);
      pushRequestJSONBodies.push(body);
      if (body.mutations.find(m => m.clientID === client2ID)) {
        throw new Error('test error in push');
      } else {
        return 'ok';
      }
    });
    fetchMocker.post(pullURL, (_url: string, body: unknown) => {
      assertJSONObject(body);
      pullRequestJsonBodies.push(body);
      const {clientID} = body;
      switch (clientID) {
        case client1ID:
          return {
            cookie: 'pull_cookie_1',
            lastMutationIDChanges: clientGroup1.lastServerAckdMutationIDs,
            patch: [],
          };
        case client3ID:
          return {
            cookie: 'pull_cookie_3',
            lastMutationIDChanges: clientGroup3.lastServerAckdMutationIDs,
            patch: [],
          };
        default:
          throw new Error(`Unexpected pull ${body}`);
      }
    });

    await rep.recoverMutations();

    expect(pushRequestJSONBodies.length).toBe(3);
    expect(pushRequestJSONBodies[0]).toEqual(
      createPushRequestBodyDD31(
        profileID,
        client1.clientGroupID,
        client1ID,
        client1PendingLocalMetas,
        schemaVersion,
      ),
    );
    expect(pushRequestJSONBodies[1]).toEqual(
      createPushRequestBodyDD31(
        profileID,
        client2.clientGroupID,
        client2ID,
        client2PendingLocalMetas,
        schemaVersion,
      ),
    );
    expect(pushRequestJSONBodies[2]).toEqual(
      createPushRequestBodyDD31(
        profileID,
        client3.clientGroupID,
        client3ID,
        client3PendingLocalMetas,
        schemaVersion,
      ),
    );

    expect(pullRequestJsonBodies.length).toBe(2);
    expect(pullRequestJsonBodies[0]).toEqual({
      profileID,
      clientGroupID: client1.clientGroupID,
      schemaVersion,
      cookie: 1,
      pullVersion: 1,
    });
    expect(pullRequestJsonBodies[1]).toEqual({
      profileID,
      clientGroupID: client3.clientGroupID,
      schemaVersion,
      cookie: 3,
      pullVersion: 1,
    });

    const updateClients = await withRead(testPerdag, read => getClients(read));
    const updatedClient1 = updateClients.get(client1ID);
    assertClientV6(updatedClient1);
    const updatedClient2 = updateClients.get(client2ID);
    assertClientV6(updatedClient2);
    const updatedClient3 = updateClients.get(client3ID);
    assertClientV6(updatedClient3);

    const updatedClientGroups = await withRead(testPerdag, read =>
      getClientGroups(read),
    );
    const updatedClientGroup1 = updatedClientGroups.get(client1.clientGroupID);
    assert(updatedClientGroup1);
    const updatedClientGroup2 = updatedClientGroups.get(client2.clientGroupID);
    assert(updatedClientGroup2);
    const updatedClientGroup3 = updatedClientGroups.get(client3.clientGroupID);
    assert(updatedClientGroup3);

    expect(updatedClient1).toEqual(client1);
    expect(updatedClientGroup1).toEqual(clientGroup1);

    expect(updatedClient2).toEqual(client2);
    expect(updatedClientGroup2).toEqual(clientGroup2);

    expect(updatedClient3).toEqual(client3);
    expect(updatedClientGroup3).toEqual(clientGroup3);
  });

  test('if an error occurs recovering one client, continues to try to recover other clients', async () => {
    const formatVersion = FormatVersion.Latest;
    const schemaVersion = 'testSchema1';
    // client1 has same schema version as recovering client and 2 mutations to recover
    const client1ID = 'client1';
    // client2 has same schema version as recovering client and 1 mutation to recover
    const client2ID = 'client2';
    // client3 has same schema version as recovering client and 1 mutation to recover
    const client3ID = 'client3';
    const replicachePartialName = 'recoverMutationsRobustToClientError';
    const auth = '1';
    const pushURL = 'https://test.replicache.dev/push';
    const pullURL = 'https://test.replicache.dev/pull';
    const rep = await replicacheForTesting(replicachePartialName, {
      auth,
      schemaVersion,
      pushURL,
      pullURL,
    });
    const profileID = await rep.profileID;

    await tickAFewTimes(vi);

    const testPerdag = await createPerdag({
      replicacheName: rep.name,
      schemaVersion,
      formatVersion: FormatVersion.V6,
    });

    const client1PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client1ID,
        perdag: testPerdag,
        numLocal: 2,
        mutatorNames: ['client1', 'mutator_name_2', 'mutator_name_3'],
        cookie: 1,
        formatVersion,
      });
    await createAndPersistClientWithPendingLocalDD31({
      clientID: client2ID,
      perdag: testPerdag,
      numLocal: 1,
      mutatorNames: ['client2', 'mutator_name_2'],
      cookie: 2,
      formatVersion,
    });
    const client3PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client3ID,
        perdag: testPerdag,
        numLocal: 1,
        mutatorNames: ['client3', 'mutator_name_2'],
        cookie: 3,
        formatVersion,
      });

    const clients = await withRead(testPerdag, read => getClients(read));
    const client1 = clients.get(client1ID);
    assertClientV6(client1);
    const client2 = clients.get(client2ID);
    assertClientV6(client2);
    const client3 = clients.get(client3ID);
    assertClientV6(client3);

    const {clientGroup1, clientGroup2, clientGroup3} = await withRead(
      testPerdag,
      async read => {
        const clientGroup1 = await getClientGroup(client1.clientGroupID, read);
        assert(clientGroup1);
        const clientGroup2 = await getClientGroup(client2.clientGroupID, read);
        assert(clientGroup2);
        const clientGroup3 = await getClientGroup(client3.clientGroupID, read);
        assert(clientGroup3);
        return {
          clientGroup1,
          clientGroup2,
          clientGroup3,
        };
      },
    );

    const pullRequestJsonBodies: JSONObject[] = [];
    fetchMocker.reset();
    fetchMocker.post(pushURL, 'ok');
    fetchMocker.post(pullURL, (_url: string, body: unknown) => {
      assertJSONObject(body);
      pullRequestJsonBodies.push(body);
      const {clientID} = body;
      switch (clientID) {
        case client1ID:
          return {
            cookie: 'pull_cookie_1',
            lastMutationIDChanges: clientGroup1.lastServerAckdMutationIDs,
            patch: [],
          };
        case client3ID:
          return {
            cookie: 'pull_cookie_3',
            lastMutationIDChanges: clientGroup3.lastServerAckdMutationIDs,
            patch: [],
          };
        default:
          throw new Error(`Unexpected pull ${body}`);
      }
    });

    const {write} = LazyStore.prototype;
    vi.spyOn(LazyStore.prototype, 'write')
      .mockImplementationOnce(function (this: LazyStore) {
        return write.call(this);
      })
      .mockImplementationOnce(() => {
        throw testErrorMsg;
      })
      .mockImplementation(function (this: LazyStore) {
        return write.call(this);
      });
    const testErrorMsg = 'Test dag.LazyStore.withWrite error';

    const consoleErrorStub = vi.spyOn(console, 'error');

    await rep.recoverMutations();

    expect(consoleErrorStub).toHaveBeenCalledTimes(1);
    // expect(consoleErrorStub.mock.calls[0].join(' ')).toContain(testErrorMsg);
    expect(consoleErrorStub.mock.calls[0]).toEqual([
      expect.any(String),
      expect.any(String),
      'Test dag.LazyStore.withWrite error',
    ]);

    const pushCalls = fetchMocker.bodies(pushURL);
    expect(pushCalls.length).toBe(2);
    expect(pushCalls[0]).toEqual(
      createPushRequestBodyDD31(
        profileID,
        client1.clientGroupID,
        client1ID,
        client1PendingLocalMetas,
        schemaVersion,
      ),
    );
    expect(pushCalls[1]).toEqual(
      createPushRequestBodyDD31(
        profileID,
        client3.clientGroupID,
        client3ID,
        client3PendingLocalMetas,
        schemaVersion,
      ),
    );

    expect(pullRequestJsonBodies.length).toBe(2);
    expect(pullRequestJsonBodies[0]).toEqual({
      profileID,
      clientGroupID: client1.clientGroupID,
      schemaVersion,
      cookie: 1,
      pullVersion: 1,
    });
    expect(pullRequestJsonBodies[1]).toEqual({
      profileID,
      clientGroupID: client3.clientGroupID,
      schemaVersion,
      cookie: 3,
      pullVersion: 1,
    });

    const updateClients = await withRead(testPerdag, read => getClients(read));
    const updatedClient1 = updateClients.get(client1ID);
    assertClientV6(updatedClient1);
    const updatedClient2 = updateClients.get(client2ID);
    assertClientV6(updatedClient2);
    const updatedClient3 = updateClients.get(client3ID);
    assertClientV6(updatedClient3);

    const updatedClientGroups = await withRead(testPerdag, read =>
      getClientGroups(read),
    );
    const updatedClientGroup1 = updatedClientGroups.get(client1.clientGroupID);
    assert(updatedClientGroup1);
    const updatedClientGroup2 = updatedClientGroups.get(client2.clientGroupID);
    assert(updatedClientGroup2);
    const updatedClientGroup3 = updatedClientGroups.get(client3.clientGroupID);
    assert(updatedClientGroup3);

    expect(updatedClient1).toEqual(client1);
    expect(updatedClientGroup1).toEqual(clientGroup1);

    expect(updatedClient2).toEqual(client2);
    expect(updatedClientGroup2).toEqual(clientGroup2);

    expect(updatedClient3).toEqual(client3);
    expect(updatedClientGroup3).toEqual(clientGroup3);
  });

  test('if an error occurs recovering one db, continues to try to recover clients from other dbs', async () => {
    const formatVersion = FormatVersion.Latest;
    const schemaVersionOfClient1 = 'testSchema1';
    const schemaVersionOfClient2 = 'testSchema2';
    const schemaVersionOfRecoveringClient = 'testSchemaOfRecovering';
    const client1ID = 'client1';
    const client2ID = 'client2';
    const replicachePartialName = 'recoverMutationsRobustToDBError';
    const auth = '1';
    const pushURL = 'https://test.replicache.dev/push';
    const pullURL = 'https://test.replicache.dev/pull';
    const rep = await replicacheForTesting(replicachePartialName, {
      auth,
      schemaVersion: schemaVersionOfRecoveringClient,
      pushURL,
      pullURL,
      mutators: {
        dummy() {
          //
        },
      },
    });
    const profileID = await rep.profileID;

    await tickAFewTimes(vi);

    const testPerdagForClient1 = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersionOfClient1,
      formatVersion,
    });
    await createAndPersistClientWithPendingLocalDD31({
      clientID: client1ID,
      perdag: testPerdagForClient1,
      numLocal: 1,
      mutatorNames: ['client1', 'mutator_name_2'],
      cookie: 1,
      formatVersion,
    });

    const testPerdagForClient2 = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersionOfClient2,
      formatVersion,
    });
    const client2PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client2ID,
        perdag: testPerdagForClient2,
        numLocal: 1,
        mutatorNames: ['client2', 'mutator_name_2'],
        cookie: 2,
        formatVersion,
      });

    const client1 = await withRead(testPerdagForClient1, read =>
      getClient(client1ID, read),
    );
    assertClientV6(client1);

    const client2 = await withRead(testPerdagForClient2, read =>
      getClient(client2ID, read),
    );
    assertClientV6(client2);

    const clientGroup1 = await withRead(testPerdagForClient1, read =>
      getClientGroup(client1.clientGroupID, read),
    );
    assert(clientGroup1);

    const clientGroup2 = await withRead(testPerdagForClient2, read =>
      getClientGroup(client2.clientGroupID, read),
    );
    assert(clientGroup2);

    const pullRequestJsonBodies: JSONObject[] = [];
    fetchMocker.reset();
    fetchMocker.post(pushURL, 'ok');
    fetchMocker.post(pullURL, (_url: string, body: unknown) => {
      assertJSONObject(body);
      pullRequestJsonBodies.push(body);
      const {clientGroupID} = body;
      switch (clientGroupID) {
        case client2.clientGroupID: {
          const pullResponse: PullResponseV1 = {
            cookie: 'pull_cookie_2',
            lastMutationIDChanges: {
              [client2ID]: clientGroup2.mutationIDs[client2ID] ?? 0,
            },
            patch: [],
          };

          return pullResponse;
        }
        default:
          throw new Error(`Unexpected pull ${body}`);
      }
    });

    const testErrorMsg = 'Test dag.StoreImpl.read error';
    const {read} = StoreImpl.prototype;
    vi.spyOn(StoreImpl.prototype, 'read')
      .mockImplementationOnce(function (this: StoreImpl) {
        return read.call(this);
      })
      .mockImplementationOnce(() => {
        throw testErrorMsg;
      })
      .mockImplementation(function (this: StoreImpl) {
        return read.call(this);
      });

    const consoleErrorStub = vi.spyOn(console, 'error');

    await rep.recoverMutations();

    expect(consoleErrorStub).toHaveBeenCalledTimes(1);
    // expect(consoleErrorStub.firstCall.args.join(' ')).toContain(testErrorMsg);
    expect(consoleErrorStub.mock.calls[0]).toEqual([
      expect.any(String),
      expect.any(String),
      'Test dag.StoreImpl.read error',
    ]);

    const pushCalls = fetchMocker.bodies(pushURL);
    expect(pushCalls.length).toBe(1);
    expect(pushCalls[0]).toEqual(
      createPushRequestBodyDD31(
        profileID,
        client2.clientGroupID,
        client2ID,
        client2PendingLocalMetas,
        schemaVersionOfClient2,
      ),
    );

    expect(pullRequestJsonBodies.length).toBe(1);
    expect(pullRequestJsonBodies[0]).toEqual({
      clientGroupID: client2.clientGroupID,
      profileID,
      schemaVersion: schemaVersionOfClient2,
      cookie: 2,
      pullVersion: PULL_VERSION_DD31,
    });

    const updatedClient1 = await withRead(testPerdagForClient1, read =>
      getClient(client1ID, read),
    );
    assertClientV6(updatedClient1);

    const updatedClient2 = await withRead(testPerdagForClient2, read =>
      getClient(client2ID, read),
    );
    assertClientV6(updatedClient2);

    const updatedClientGroup1 = await withRead(testPerdagForClient1, read =>
      getClientGroup(updatedClient1.clientGroupID, read),
    );
    assert(updatedClientGroup1);
    const updatedClientGroup2 = await withRead(testPerdagForClient2, read =>
      getClientGroup(updatedClient2.clientGroupID, read),
    );
    assert(updatedClientGroup2);

    expect(updatedClientGroup1.mutationIDs[client1ID]).toBe(
      clientGroup1.mutationIDs[client1ID],
    );
    // lastServerAckdMutationID not updated due to error when recovering this
    // client's db
    expect(updatedClientGroup1.lastServerAckdMutationIDs[client1ID]).toBe(
      clientGroup1.lastServerAckdMutationIDs[client1ID],
    );

    expect(updatedClientGroup2.mutationIDs[client2ID]).toBe(
      clientGroup2.mutationIDs[client2ID],
    );
    // lastServerAckdMutationID is updated to high mutationID as mutations
    // were recovered despite error in other db
    expect(updatedClientGroup2.lastServerAckdMutationIDs[client2ID]).toBe(
      clientGroup2.mutationIDs[client2ID],
    );
  });

  test('mutation recovery exits early if Replicache is closed', async () => {
    const formatVersion = FormatVersion.Latest;
    const schemaVersion = 'testSchema1';
    const client1ID = 'client1';
    const client2ID = 'client2';
    const replicachePartialName = 'recoverMutationsRobustToClientError';
    const auth = '1';
    const pushURL = 'https://test.replicache.dev/push';
    const pullURL = 'https://test.replicache.dev/pull';
    const rep = await replicacheForTesting(replicachePartialName, {
      auth,
      schemaVersion,
      pushURL,
      pullURL,
      mutators: {
        rep() {
          return;
        },
      },
    });
    const profileID = await rep.profileID;

    await tickAFewTimes(vi);

    const testPerdag = await createPerdag({
      replicacheName: rep.name,
      schemaVersion,
      formatVersion,
    });

    const client1PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client1ID,
        perdag: testPerdag,
        numLocal: 1,
        mutatorNames: ['mutator_name_2', 'client1'],
        cookie: 1,
        formatVersion,
      });

    await createAndPersistClientWithPendingLocalDD31({
      clientID: client2ID,
      perdag: testPerdag,
      numLocal: 1,
      // Different mutator names to ensure different client groups.
      mutatorNames: ['mutator_name_2', 'client2'],
      cookie: 2,
      formatVersion,
    });

    const clients = await withRead(testPerdag, read => getClients(read));
    const client1 = clients.get(client1ID);
    assertClientV6(client1);
    const client2 = clients.get(client2ID);
    assertClientV6(client2);

    const pullRequestJsonBodies: JSONObject[] = [];
    fetchMocker.reset();
    fetchMocker.post(pushURL, 'ok');
    fetchMocker.post(pullURL, (_url: string, body: unknown) => {
      assertJSONObject(body);
      pullRequestJsonBodies.push(body);
      const {clientID} = body;
      const resp: PullResponseV1 = {
        cookie: 'pull_cookie_1',
        lastMutationIDChanges: {
          [client1ID]: 0,
        },
        patch: [],
      };
      switch (clientID) {
        case client1ID:
          return resp;
        default:
          throw new Error(`Unexpected pull ${body}`);
      }
    });

    // At the end of recovering client1 close the recovering Replicache instance
    const {close} = LazyStore.prototype;
    vi.spyOn(LazyStore.prototype, 'close')
      .mockImplementationOnce(async () => {
        await rep.close();
      })
      .mockImplementation(function (this: LazyStore) {
        return close.call(this);
      });

    await rep.recoverMutations();

    const pushCalls = fetchMocker.bodies(pushURL);
    expect(pushCalls.length).toBe(1);
    expect(pushCalls[0]).toEqual(
      createPushRequestBodyDD31(
        profileID,
        client1.clientGroupID,
        client1ID,
        client1PendingLocalMetas,
        schemaVersion,
      ),
    );
  });

  test('mutation recovery is invoked at startup', async () => {
    const rep = await replicacheForTesting('mutation-recovery-startup-dd31');
    expect(rep.recoverMutationsFake).toHaveBeenCalledTimes(1);
    expect(rep.recoverMutationsFake).toHaveBeenCalledTimes(1);
    expect(await rep.recoverMutationsFake.mock.results[0].value).toBe(true);
  });

  test('mutation recovery returns early without running if push is disabled', async () => {
    const rep = await replicacheForTesting(
      'mutation-recovery-startup',
      {
        pullURL: 'https://diff.com/pull',
      },
      undefined,
      {
        useDefaultURLs: false,
      },
    );
    expect(rep.recoverMutationsFake).toHaveBeenCalledTimes(1);
    expect(await rep.recoverMutationsFake.mock.results[0].value).toBe(false);
    expect(await rep.recoverMutations()).toBe(false);
  });

  test('mutation recovery returns early when internal option enableMutationRecovery is false', async () => {
    const rep = await replicacheForTesting(
      'mutation-recovery-startup',
      {
        pullURL: 'https://diff.com/pull',
      },
      disableAllBackgroundProcesses,
    );
    expect(rep.recoverMutationsFake).toHaveBeenCalledTimes(1);
    expect(await rep.recoverMutationsFake.mock.results[0].value).toBe(false);
    expect(await rep.recoverMutations()).toBe(false);
  });

  test('mutation recovery is invoked on change from offline to online', async () => {
    const pullURL = 'https://test.replicache.dev/pull';
    const rep = await replicacheForTesting('mutation-recovery-online', {
      pullURL,
    });
    expect(rep.recoverMutationsFake).toHaveBeenCalledTimes(1);
    expect(rep.online).toBe(true);

    fetchMocker.post(pullURL, () => ({
      throws: new Error('Simulate fetch error in push'),
    }));

    rep.pullIgnorePromise();

    await tickAFewTimes(vi);
    expect(rep.online).toBe(false);
    expect(rep.recoverMutationsFake).toHaveBeenCalledTimes(1);

    const {clientID} = rep;
    fetchMocker.reset();
    fetchMocker.post(pullURL, {
      cookie: 'test_cookie',
      lastMutationIDChanges: {[clientID]: 2},
      patch: [],
    });

    rep.pullIgnorePromise();
    expect(rep.recoverMutationsFake).toHaveBeenCalledTimes(1);
    while (!rep.online) {
      await tickAFewTimes(vi);
    }
    expect(rep.recoverMutationsFake).toHaveBeenCalledTimes(2);
  });

  test('mutation recovery is invoked on 5 minute interval', async () => {
    const rep = await replicacheForTesting('mutation-recovery-startup-dd31-4');
    expect(rep.recoverMutationsFake).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(5 * 60 * 1000);
    expect(rep.recoverMutationsFake).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(5 * 60 * 1000);
    expect(rep.recoverMutationsFake).toHaveBeenCalledTimes(3);
  });

  async function testPushDisabled(
    schemaVersion1: string,
    schemaVersion2: string,
  ) {
    const formatVersion = FormatVersion.Latest;
    const client1ID = 'client1';
    const auth = '1';
    const pushURL = '';
    const pullURL = 'https://test.replicache.dev/pull';
    const rep = await replicacheForTesting('old-client-push-disabled', {
      auth,
      pushURL,
      pullURL,
      schemaVersion: schemaVersion1,
      mutators: {
        mutator_name_2: () => undefined,
      },
    });

    const testPerdagDD31 = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersion2,
      formatVersion,
    });

    await createAndPersistClientWithPendingLocalDD31({
      clientID: client1ID,
      perdag: testPerdagDD31,
      numLocal: 1,
      mutatorNames: ['client-1', 'mutator_name_2'],
      cookie: 'c1',
      formatVersion,
    });

    const client1 = await withRead(testPerdagDD31, read =>
      getClient(client1ID, read),
    );
    assertClientV6(client1);
    expect(client1.clientGroupID).not.toBe(await rep.clientGroupID);
    const clientGroup1 = await withRead(testPerdagDD31, read =>
      getClientGroup(client1.clientGroupID, read),
    );
    assert(clientGroup1);
    expect(clientGroup1.mutationIDs[client1ID]).toBe(2);

    const pullRequestJSONBodies: JSONObject[] = [];
    const pushRequestJSONBodies: JSONObject[] = [];
    fetchMocker.reset();
    fetchMocker.post(pushURL, (_url: string, body: unknown) => {
      assertJSONObject(body);
      pushRequestJSONBodies.push(body);
      throw new Error();
    });
    fetchMocker.post(pullURL, (_url: string, body: unknown) => {
      assertJSONObject(body);
      pullRequestJSONBodies.push(body);
      throw new Error();
    });

    await rep.recoverMutations();

    expect(pushRequestJSONBodies).toEqual([]);
    expect(pullRequestJSONBodies).toEqual([]);

    const updatedClient1 = await withRead(testPerdagDD31, read =>
      getClient(client1ID, read),
    );
    assertClientV6(updatedClient1);
    expect(updatedClient1).toEqual(client1);

    const updatedClientGroup1 = await withRead(testPerdagDD31, read =>
      getClientGroup(client1.clientGroupID, read),
    );
    expect(updatedClientGroup1).toEqual(clientGroup1);
  }

  test('pushDisabled so no recovery possible', async () => {
    await testPushDisabled('schema-version', 'schema-version');
  });

  test('pushDisabled so no recovery possible different perdag due to schema', async () => {
    await testPushDisabled('schema-version-1', 'schema-version-2');
  });

  async function testPullDisabled(
    schemaVersion1: string,
    schemaVersion2: string,
  ) {
    const formatVersion = FormatVersion.Latest;
    const client1ID = 'client1';
    const auth = '1';
    const pushURL = 'https://test.replicache.dev/push';
    const pullURL = '';
    const rep = await replicacheForTesting('old-client-pull-disabled', {
      auth,
      pushURL,
      pullURL,
      schemaVersion: schemaVersion1,
    });
    const profileID = await rep.profileID;

    const testPerdagDD31 = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersion2,
      formatVersion,
    });

    const client1PendingLocalMetasDD31 =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client1ID,
        perdag: testPerdagDD31,
        numLocal: 1,
        mutatorNames: ['client-1', 'mutator_name_2'],
        cookie: 'c2',
        formatVersion,
      });

    const client1 = await withRead(testPerdagDD31, read =>
      getClient(client1ID, read),
    );
    assertClientV6(client1);
    expect(client1.clientGroupID).not.toBe(await rep.clientGroupID);
    const clientGroup1 = await withRead(testPerdagDD31, read =>
      getClientGroup(client1.clientGroupID, read),
    );
    assert(clientGroup1);
    expect(clientGroup1.mutationIDs[client1ID]).toBe(2);

    const pullRequestJSONBodies: JSONObject[] = [];
    const pushRequestJSONBodies: JSONObject[] = [];
    fetchMocker.reset();
    fetchMocker.post(pushURL, (_url: string, body: unknown) => {
      assertJSONObject(body);
      pushRequestJSONBodies.push(body);
      return 'ok';
    });
    fetchMocker.post(pullURL, (_url: string, body: unknown) => {
      assertJSONObject(body);
      pullRequestJSONBodies.push(body);
      throw new Error();
    });

    await rep.recoverMutations();

    const pushRequestBody1: PushRequestV1 = {
      clientGroupID: client1.clientGroupID,
      mutations: [
        {
          clientID: client1ID,
          args: client1PendingLocalMetasDD31[0].mutatorArgsJSON,
          id: client1PendingLocalMetasDD31[0].mutationID,
          name: client1PendingLocalMetasDD31[0].mutatorName,
          timestamp: client1PendingLocalMetasDD31[0].timestamp,
        },
      ],
      profileID,
      pushVersion: PUSH_VERSION_DD31,
      schemaVersion: schemaVersion2,
    };
    expect(pushRequestJSONBodies).toEqual([pushRequestBody1]);

    expect(pullRequestJSONBodies).toEqual([]);

    const updatedClient1 = await withRead(testPerdagDD31, read =>
      getClient(client1ID, read),
    );
    assertClientV6(updatedClient1);
    expect(updatedClient1).toEqual(client1);

    const updatedClientGroup1 = await withRead(testPerdagDD31, read =>
      getClientGroup(client1.clientGroupID, read),
    );
    // This did not get updated because pull was disabled!
    expect(updatedClientGroup1).toEqual(clientGroup1);
  }

  test('pullDisabled so cannot confirm recovery', async () => {
    await testPullDisabled('schema-version', 'schema-version');
  });

  test('pullDisabled so cannot confirm recovery different perdag due to schema', async () => {
    await testPullDisabled('schema-version-1', 'schema-version-2');
  });

  async function testClientGroupDisabled(
    schemaVersion1: string,
    schemaVersion2: string,
  ) {
    const formatVersion = FormatVersion.Latest;
    const client1ID = 'client1';
    const client2ID = 'client2';
    const auth = '1';
    const pushURL = 'https://test.replicache.dev/push';
    const pullURL = 'https://test.replicache.dev/pull';
    const rep = await replicacheForTesting('client-group-to-recover-disabled', {
      auth,
      pushURL,
      pullURL,
      schemaVersion: schemaVersion1,
      mutators: {
        mutator_name_2: () => undefined,
      },
    });
    const profileID = await rep.profileID;

    const testPerdagDD31 = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersion2,
      formatVersion,
    });

    await createAndPersistClientWithPendingLocalDD31({
      clientID: client1ID,
      perdag: testPerdagDD31,
      numLocal: 1,
      mutatorNames: ['mutator_client1', 'mutator_name_2'],
      cookie: 'c1',
      formatVersion,
    });

    const client2PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31({
        clientID: client2ID,
        perdag: testPerdagDD31,
        numLocal: 1,
        // change mutator names to get a different client group from
        // client1
        mutatorNames: ['mutator_client2', 'mutator_name_2'],
        // This needs to be lexicographically greater than the cookie
        // used to create/persist client1ID above, otherwise the snapshot
        // created for this client won't get presisted as its cookie is not
        // considered newer than the snapshot its client group was forked from,
        // and we end up with rebase errors
        cookie: 'c2',
        formatVersion,
      });

    const client1 = await withRead(testPerdagDD31, read =>
      getClient(client1ID, read),
    );
    assertClientV6(client1);
    expect(client1.clientGroupID).not.toBe(await rep.clientGroupID);
    await withWriteNoImplicitCommit(testPerdagDD31, async write => {
      await disableClientGroup(client1.clientGroupID, write);
      await write.commit();
    });
    const clientGroup1 = await withRead(testPerdagDD31, read =>
      getClientGroup(client1.clientGroupID, read),
    );
    assert(clientGroup1);
    expect(clientGroup1.mutationIDs[client1ID]).toBe(2);

    const client2 = await withRead(testPerdagDD31, read =>
      getClient(client2ID, read),
    );
    assertClientV6(client2);
    expect(client2.clientGroupID).not.toBe(await rep.clientGroupID);
    expect(client2.clientGroupID).not.toBe(client1.clientGroupID);
    const clientGroup2 = await withRead(testPerdagDD31, read =>
      getClientGroup(client2.clientGroupID, read),
    );
    assert(clientGroup2);
    expect(clientGroup2.mutationIDs[client2ID]).toBe(2);

    fetchMocker.reset();

    const pullRequestJSONBodies: JSONObject[] = [];
    const pushRequestJSONBodies: JSONObject[] = [];
    fetchMocker.reset();
    fetchMocker.post(pushURL, (_url: string, body: unknown) => {
      assertJSONObject(body);
      pushRequestJSONBodies.push(body);
      return 'ok';
    });
    fetchMocker.post(pullURL, (_url: string, body: unknown) => {
      assertJSONObject(body);
      pullRequestJSONBodies.push(body);
      return {
        cookie: 'c3',
        lastMutationIDChanges: {[client2ID]: 2},
        patch: [],
      };
    });

    await rep.recoverMutations();

    // Client 2 is recovered, but client 1 is not since its client group
    // is disabled
    const pushRequestBody1: PushRequestV1 = {
      clientGroupID: client2.clientGroupID,
      mutations: [
        {
          clientID: client2ID,
          args: client2PendingLocalMetas[0].mutatorArgsJSON,
          id: client2PendingLocalMetas[0].mutationID,
          name: client2PendingLocalMetas[0].mutatorName,
          timestamp: client2PendingLocalMetas[0].timestamp,
        },
      ],
      profileID,
      pushVersion: PUSH_VERSION_DD31,
      schemaVersion: schemaVersion2,
    };
    expect(pushRequestJSONBodies).toEqual([pushRequestBody1]);
    const pullRequestBody1: PullRequestV1 = {
      profileID,
      clientGroupID: client2.clientGroupID,
      cookie: 'c2',
      pullVersion: PULL_VERSION_DD31,
      schemaVersion: schemaVersion2,
    };
    expect(pullRequestJSONBodies).toEqual([pullRequestBody1]);

    const updatedClient1 = await withRead(testPerdagDD31, read =>
      getClient(client1ID, read),
    );
    assertClientV6(updatedClient1);
    expect(updatedClient1).toEqual(client1);

    const updatedClientGroup1 = await withRead(testPerdagDD31, read =>
      getClientGroup(client1.clientGroupID, read),
    );
    // This did not get updated because the client group was disabled!
    expect(updatedClientGroup1).toEqual(clientGroup1);

    const updatedClient2 = await withRead(testPerdagDD31, read =>
      getClient(client2ID, read),
    );
    assertClientV6(updatedClient2);
    expect(updatedClient2).toEqual(client2);

    const updatedClientGroup2 = await withRead(testPerdagDD31, read =>
      getClientGroup(client2.clientGroupID, read),
    );
    // Updated with new lastServerAckdMutationIDs from pull response
    expect(updatedClientGroup2).toEqual({
      ...clientGroup2,
      lastServerAckdMutationIDs: {
        [client2ID]: 2,
      },
    });
  }

  test('disabled client group is not recovered', async () => {
    await testClientGroupDisabled('schema-version', 'schema-version');
  });

  test('disabled client group is not recovered, different perdag due to schema', async () => {
    await testClientGroupDisabled('schema-version-1', 'schema-version-2');
  });
});
