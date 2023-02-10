import {
  initReplicacheTesting,
  replicacheForTesting,
  tickAFewTimes,
  clock,
  disableAllBackgroundProcesses,
} from './test-util.js';
import {
  MutatorDefs,
  REPLICACHE_FORMAT_VERSION_DD31,
  REPLICACHE_FORMAT_VERSION_SDD,
} from './replicache.js';
import {ChainBuilder} from './db/test-helpers.js';
import type * as db from './db/mod.js';
import * as dag from './dag/mod.js';
import * as persist from './persist/mod.js';
import type * as sync from './sync/mod.js';
import {assertHash} from './hash.js';
import {assert} from './asserts.js';
import {expect} from '@esm-bundle/chai';
import {uuid} from './uuid.js';
import {assertJSONObject, JSONObject, ReadonlyJSONObject} from './json.js';
import sinon from 'sinon';

// fetch-mock has invalid d.ts file so we removed that on npm install.
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-expect-error
import fetchMock from 'fetch-mock/esm/client';
import {initClientWithClientID} from './persist/clients-test-helpers.js';
import {
  assertPushRequestDD31,
  PushRequestDD31,
  PushRequestSDD,
  PUSH_VERSION_DD31,
  PUSH_VERSION_SDD,
} from './sync/push.js';
import {assertClientDD31, assertClientSDD} from './persist/clients.js';
import {LogContext} from '@rocicorp/logger';
import type {PullResponseDD31, PullResponseSDD} from './puller.js';
import {
  PullRequestDD31,
  PullRequestSDD,
  PULL_VERSION_DD31,
  PULL_VERSION_SDD,
} from './sync/pull.js';
import {assertLocalMetaDD31} from './db/commit.js';
import {
  createAndPersistClientWithPendingLocalSDD,
  createPerdag,
} from './replicache-mutation-recovery.test.js';
import {stringCompare} from './string-compare.js';
import type {PushResponse} from './pusher.js';

async function createAndPersistClientWithPendingLocalDD31(
  clientID: sync.ClientID,
  perdag: dag.Store,
  numLocal: number,
  mutatorNames: string[],
  cookie: string | number,
): Promise<db.LocalMetaDD31[]> {
  const testMemdag = new dag.LazyStore(
    perdag,
    100 * 2 ** 20, // 100 MB,
    dag.uuidChunkHasher,
    assertHash,
  );

  const b = new ChainBuilder(testMemdag, undefined, true);

  await b.addGenesis(clientID);
  await b.addSnapshot([['unique', uuid()]], clientID, cookie);

  await initClientWithClientID(clientID, perdag, mutatorNames, {}, true);

  const localMetas: db.LocalMetaDD31[] = [];
  for (let i = 0; i < numLocal; i++) {
    await b.addLocal(clientID);
    const {meta} = b.chain[b.chain.length - 1];
    assertLocalMetaDD31(meta);
    localMetas.push(meta);
  }

  const mutators: MutatorDefs = Object.fromEntries(
    mutatorNames.map(n => [n, () => Promise.resolve()]),
  );

  await persist.persistDD31(
    new LogContext(),
    clientID,
    testMemdag,
    perdag,
    mutators,
    () => false,
  );

  return localMetas;
}

suite('DD31', () => {
  initReplicacheTesting();

  function createPushRequestBodyDD31(
    profileID: string,
    clientGroupID: sync.ClientGroupID,
    clientID: sync.ClientID,
    localMetas: db.LocalMetaDD31[],
    schemaVersion: string,
  ): ReadonlyJSONObject {
    return {
      profileID,
      clientGroupID,
      mutations: localMetas.map(localMeta => ({
        clientID,
        id: localMeta.mutationID,
        name: localMeta.mutatorName,
        args: localMeta.mutatorArgsJSON,
        timestamp: localMeta.timestamp,
      })),
      pushVersion: PUSH_VERSION_DD31,
      schemaVersion,
    };
  }

  async function testRecoveringMutationsOfClientDD31(args: {
    schemaVersionOfClientWPendingMutations: string;
    schemaVersionOfClientRecoveringMutations: string;
    numMutationsNotAcknowledgedByPull?: number;
    pullResponse?: PullResponseDD31 | undefined;
    pushResponse?: PushResponse | undefined;
  }) {
    sinon.stub(console, 'error');

    const {
      schemaVersionOfClientWPendingMutations,
      schemaVersionOfClientRecoveringMutations,
      numMutationsNotAcknowledgedByPull = 0,
      pullResponse: pullResponseArg,
      pushResponse,
    } = args;
    const client1ID = 'client1';
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

    await tickAFewTimes();

    const testPerdag = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersionOfClientWPendingMutations,
      replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_DD31,
    });

    const client1PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31(
        client1ID,
        testPerdag,
        2,
        ['client1', 'mutator_name_2', 'mutator_name_3'],
        1,
      );
    const client1 = await testPerdag.withRead(read =>
      persist.getClient(client1ID, read),
    );
    assertClientDD31(client1);
    const clientGroup1 = await testPerdag.withRead(read =>
      persist.getClientGroup(client1.clientGroupID, read),
    );
    assert(clientGroup1);

    fetchMock.reset();
    fetchMock.post(pushURL, pushResponse ?? 'ok');
    const pullLastMutationID =
      clientGroup1.mutationIDs[client1ID] - numMutationsNotAcknowledgedByPull;
    const pullResponse: PullResponseDD31 = pullResponseArg ?? {
      cookie: 'pull_cookie_1',
      lastMutationIDChanges: {
        [client1ID]: pullLastMutationID,
      },
      patch: [],
    };

    fetchMock.post(pullURL, pullResponse);

    await rep.recoverMutations();

    const pushCalls = fetchMock.calls(pushURL);
    expect(pushCalls.length).to.equal(1);
    expect(await pushCalls[0].request.json()).to.deep.equal({
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

    const pullCalls = fetchMock.calls(pullURL);

    if (pushResponse && pushResponse.error) {
      expect(pullCalls.length).to.equal(0);
    } else {
      expect(pullCalls.length).to.equal(1);
      const pullReq: PullRequestDD31 = {
        profileID,
        clientGroupID: client1.clientGroupID,
        cookie: 1,
        pullVersion: PULL_VERSION_DD31,
        schemaVersion: schemaVersionOfClientWPendingMutations,
      };
      expect(await pullCalls[0].request.json()).to.deep.equal(pullReq);
    }

    const updatedClient1 = await testPerdag.withRead(read =>
      persist.getClient(client1ID, read),
    );
    assertClientDD31(updatedClient1);

    expect(updatedClient1.clientGroupID).to.deep.equal(client1.clientGroupID);
    expect(updatedClient1.headHash).to.equal(client1.headHash);

    const updatedClientGroup1 = await testPerdag.withRead(read =>
      persist.getClientGroup(client1.clientGroupID, read),
    );

    assert(updatedClientGroup1);
    if ('error' in pullResponse || (pushResponse && 'error' in pushResponse)) {
      expect(updatedClientGroup1.lastServerAckdMutationIDs).to.deep.equal({
        [client1ID]: 1, // not acknowledged because pull failed
      });
    } else {
      expect(updatedClientGroup1.lastServerAckdMutationIDs).to.deep.equal({
        [client1ID]: pullLastMutationID,
      });
    }
    expect(updatedClientGroup1.mutationIDs).to.deep.equal({
      [client1ID]: clientGroup1.mutationIDs[client1ID],
    });
  }

  test('successfully recovering mutations of client with same schema version and replicache format version', async () => {
    await testRecoveringMutationsOfClientDD31({
      schemaVersionOfClientWPendingMutations: 'testSchema1',
      schemaVersionOfClientRecoveringMutations: 'testSchema1',
    });
  });

  test('successfully recovering mutations of client with different schema version but same replicache format version', async () => {
    await testRecoveringMutationsOfClientDD31({
      schemaVersionOfClientWPendingMutations: 'testSchema1',
      schemaVersionOfClientRecoveringMutations: 'testSchema2',
    });
  });

  test('successfully recovering some but not all mutations of another client (pull does not acknowledge all)', async () => {
    await testRecoveringMutationsOfClientDD31({
      schemaVersionOfClientWPendingMutations: 'testSchema1',
      schemaVersionOfClientRecoveringMutations: 'testSchema1',
      numMutationsNotAcknowledgedByPull: 1,
    });
  });

  test('Pull returns VersionNotSupported', async () => {
    await testRecoveringMutationsOfClientDD31({
      schemaVersionOfClientWPendingMutations: 'testSchema1',
      schemaVersionOfClientRecoveringMutations: 'testSchema1',
      numMutationsNotAcknowledgedByPull: 1,
      pullResponse: {error: 'VersionNotSupported', versionType: 'pull'},
    });
  });

  test('Pull returns ClientStateNotFound', async () => {
    await testRecoveringMutationsOfClientDD31({
      schemaVersionOfClientWPendingMutations: 'testSchema1',
      schemaVersionOfClientRecoveringMutations: 'testSchema1',
      numMutationsNotAcknowledgedByPull: 1,
      pullResponse: {error: 'ClientStateNotFound'},
    });
  });

  test('Push returns VersionNotSupported', async () => {
    await testRecoveringMutationsOfClientDD31({
      schemaVersionOfClientWPendingMutations: 'testSchema1',
      schemaVersionOfClientRecoveringMutations: 'testSchema1',
      numMutationsNotAcknowledgedByPull: 1,
      pushResponse: {error: 'VersionNotSupported', versionType: 'pull'},
    });
  });

  test('Push returns ClientStateNotFound', async () => {
    await testRecoveringMutationsOfClientDD31({
      schemaVersionOfClientWPendingMutations: 'testSchema1',
      schemaVersionOfClientRecoveringMutations: 'testSchema1',
      numMutationsNotAcknowledgedByPull: 1,
      pushResponse: {error: 'ClientStateNotFound'},
    });
  });

  test('recovering mutations with pull disabled', async () => {
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

    await tickAFewTimes();

    const testPerdag = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersionOfClientWPendingMutations,
      replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_DD31,
    });

    const client1PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31(
        client1ID,
        testPerdag,
        2,
        ['client1', 'mutator_name_2', 'mutator_name_3'],
        1,
      );
    const client1 = await testPerdag.withRead(read =>
      persist.getClient(client1ID, read),
    );
    assertClientDD31(client1);

    fetchMock.reset();
    fetchMock.post(pushURL, 'ok');
    fetchMock.catch(() => {
      throw new Error('unexpected fetch in test');
    });

    await rep.recoverMutations();

    const pushCalls = fetchMock.calls(pushURL);
    expect(pushCalls.length).to.equal(1, "didn't call push");
    expect(await pushCalls[0].request.json()).to.deep.equal({
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

    // Expect no unmatched fetches (only a push request should be sent, no pull)
    expect(fetchMock.calls('unmatched').length).to.equal(0);

    const updatedClient1 = await testPerdag.withRead(read =>
      persist.getClient(client1ID, read),
    );
    // unchanged
    expect(updatedClient1).to.deep.equal(client1);
  });

  test('client does not attempt to recover mutations from IndexedDB with different replicache name', async () => {
    const clientWPendingMutationsID = 'client1';
    const schemaVersion = 'testSchema';
    const replicacheNameOfClientWPendingMutations = `${uuid}:diffName-pendingClient`;
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

    await tickAFewTimes();

    const testPerdag = await createPerdag({
      replicacheName: replicacheNameOfClientWPendingMutations,
      schemaVersion,
      replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_DD31,
    });

    await createAndPersistClientWithPendingLocalDD31(
      clientWPendingMutationsID,
      testPerdag,
      2,
      ['client1', 'mutator_name_2', 'mutator_name_3'],
      1,
    );
    const clientWPendingMutations = await testPerdag.withRead(read =>
      persist.getClient(clientWPendingMutationsID, read),
    );
    assertClientDD31(clientWPendingMutations);

    fetchMock.reset();
    fetchMock.post(pushURL, 'ok');
    const pullResponse: PullResponseDD31 = {
      cookie: 'pull_cookie_1',
      lastMutationIDChanges: {},
      patch: [],
    };
    fetchMock.post(pullURL, pullResponse);

    await rep.recoverMutations();

    //
    expect(fetchMock.calls(pushURL).length).to.equal(0);
    expect(fetchMock.calls(pullURL).length).to.equal(0);
  });

  test('successfully recovering mutations of multiple clients with mix of schema versions and same replicache format version', async () => {
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

    await tickAFewTimes();

    const testPerdagForClients1Thru3 = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersionOfClients1Thru3AndClientRecoveringMutations,
      replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_DD31,
    });

    const client1PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31(
        client1ID,
        testPerdagForClients1Thru3,
        2,
        ['client1', 'mutator_name_2', 'mutator_name_3'],
        1,
      );
    const client2PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31(
        client2ID,
        testPerdagForClients1Thru3,
        0,
        ['client2'],
        2,
      );
    expect(client2PendingLocalMetas.length).to.equal(0);
    const client3PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31(
        client3ID,
        testPerdagForClients1Thru3,
        1,
        ['client3', 'mutator_name_2'],
        3,
      );

    const testPerdagForClient4 = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersionOfClient4,
      replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_DD31,
    });
    const client4PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31(
        client4ID,
        testPerdagForClient4,
        2,
        ['client4', 'mutator_name_2', 'mutator_name_3'],
        4,
      );

    const clients1Thru3 = await testPerdagForClients1Thru3.withRead(read =>
      persist.getClients(read),
    );
    const client1 = clients1Thru3.get(client1ID);
    assertClientDD31(client1);
    const client2 = clients1Thru3.get(client2ID);
    assertClientDD31(client2);
    const client3 = clients1Thru3.get(client3ID);
    assertClientDD31(client3);
    const {clientGroup1, clientGroup2, clientGroup3} =
      await testPerdagForClients1Thru3.withRead(async read => {
        const clientGroup1 = await persist.getClientGroup(
          client1.clientGroupID,
          read,
        );
        assert(clientGroup1);
        const clientGroup2 = await persist.getClientGroup(
          client2.clientGroupID,
          read,
        );
        assert(clientGroup2);
        const clientGroup3 = await persist.getClientGroup(
          client3.clientGroupID,
          read,
        );
        assert(clientGroup3);
        return {clientGroup1, clientGroup2, clientGroup3};
      });

    const client4 = await testPerdagForClient4.withRead(read =>
      persist.getClient(client4ID, read),
    );
    assertClientDD31(client4);
    const clientGroup4 = await testPerdagForClient4.withRead(read =>
      persist.getClientGroup(client4.clientGroupID, read),
    );
    assert(clientGroup4);

    const pullRequestJsonBodies: JSONObject[] = [];
    fetchMock.reset();
    fetchMock.post(pushURL, 'ok');
    fetchMock.post(
      pullURL,
      async (_url: string, _options: RequestInit, request: Request) => {
        const requestJson = await request.json();
        assertJSONObject(requestJson);
        pullRequestJsonBodies.push(requestJson);
        const {clientGroupID} = requestJson;
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
            throw new Error(`Unexpected pull ${requestJson}`);
        }
      },
    );

    await rep.recoverMutations();

    const pushCalls = fetchMock.calls(pushURL);
    expect(pushCalls.length).to.equal(3);
    expect(await pushCalls[0].request.json()).to.deep.equal(
      createPushRequestBodyDD31(
        profileID,
        client1.clientGroupID,
        client1ID,
        client1PendingLocalMetas,
        schemaVersionOfClients1Thru3AndClientRecoveringMutations,
      ),
    );
    expect(await pushCalls[1].request.json()).to.deep.equal(
      createPushRequestBodyDD31(
        profileID,
        client3.clientGroupID,
        client3ID,
        client3PendingLocalMetas,
        schemaVersionOfClients1Thru3AndClientRecoveringMutations,
      ),
    );
    expect(await pushCalls[2].request.json()).to.deep.equal(
      createPushRequestBodyDD31(
        profileID,
        client4.clientGroupID,
        client4ID,
        client4PendingLocalMetas,
        schemaVersionOfClient4,
      ),
    );

    expect(pullRequestJsonBodies.length).to.equal(3);
    expect(pullRequestJsonBodies[0]).to.deep.equal({
      clientGroupID: client1.clientGroupID,
      profileID,
      schemaVersion: schemaVersionOfClients1Thru3AndClientRecoveringMutations,
      cookie: 1,
      pullVersion: 1,
    });
    expect(pullRequestJsonBodies[1]).to.deep.equal({
      clientGroupID: client3.clientGroupID,
      profileID,
      schemaVersion: schemaVersionOfClients1Thru3AndClientRecoveringMutations,
      cookie: 3,
      pullVersion: 1,
    });
    expect(pullRequestJsonBodies[2]).to.deep.equal({
      profileID,
      clientGroupID: client4.clientGroupID,
      schemaVersion: schemaVersionOfClient4,
      cookie: 4,
      pullVersion: 1,
    });

    const updateClients1Thru3 = await testPerdagForClients1Thru3.withRead(
      read => persist.getClients(read),
    );
    const updatedClient1 = updateClients1Thru3.get(client1ID);
    assertClientDD31(updatedClient1);
    const updatedClient2 = updateClients1Thru3.get(client2ID);
    assertClientDD31(updatedClient2);
    const updatedClient3 = updateClients1Thru3.get(client3ID);
    assertClientDD31(updatedClient3);

    const updatedClientGroups = await testPerdagForClients1Thru3.withRead(
      read => persist.getClientGroups(read),
    );
    const updatedClientGroup1 = updatedClientGroups.get(client1.clientGroupID);
    assert(updatedClientGroup1);
    const updatedClientGroup2 = updatedClientGroups.get(client2.clientGroupID);
    assert(updatedClientGroup2);
    const updatedClientGroup3 = updatedClientGroups.get(client3.clientGroupID);
    assert(updatedClientGroup3);

    const updatedClient4 = await testPerdagForClient4.withRead(read =>
      persist.getClient(client4ID, read),
    );
    assertClientDD31(updatedClient4);
    const updatedClientGroup4 = await testPerdagForClient4.withRead(read =>
      persist.getClientGroup(client4.clientGroupID, read),
    );
    assert(updatedClientGroup4);

    expect(updatedClient1).to.deep.equal(client1);
    expect(updatedClientGroup1).to.deep.equal({
      ...clientGroup1,
      lastServerAckdMutationIDs: {
        ...clientGroup1.lastServerAckdMutationIDs,
        // lastServerAckdMutationIDs is updated to high mutationID as mutations
        // were recovered
        [client1ID]: clientGroup1.mutationIDs[client1ID],
      },
    });

    expect(updatedClient2).to.deep.equal(client2);
    expect(updatedClientGroup2).to.deep.equal(clientGroup2);

    expect(updatedClient3).to.deep.equal(client3);
    expect(updatedClientGroup3).to.deep.equal({
      ...clientGroup3,
      lastServerAckdMutationIDs: {
        ...clientGroup3.lastServerAckdMutationIDs,
        // lastServerAckdMutationIDs is updated to high mutationID as mutations
        // were recovered
        [client3ID]: clientGroup3.mutationIDs[client3ID],
      },
    });

    expect(updatedClient4).to.deep.equal(client4);
    expect(updatedClientGroup4).to.deep.equal({
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

    await tickAFewTimes();

    const testPerdag = await createPerdag({
      replicacheName: rep.name,
      schemaVersion,
      replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_DD31,
    });

    const client1PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31(
        client1ID,
        testPerdag,
        2,
        ['client1', 'mutator_name_2', 'mutator_name_3'],
        1,
      );
    const client2PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31(
        client2ID,
        testPerdag,
        1,
        ['client2', 'mutator_name_2'],
        2,
      );
    const client3PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31(
        client3ID,
        testPerdag,
        1,
        ['client3', 'mutator_name_2'],
        3,
      );

    const clients = await testPerdag.withRead(read => persist.getClients(read));
    const client1 = clients.get(client1ID);
    assertClientDD31(client1);
    const client2 = clients.get(client2ID);
    assertClientDD31(client2);
    const client3 = clients.get(client3ID);
    assertClientDD31(client3);

    const {clientGroup1, clientGroup2, clientGroup3} =
      await testPerdag.withRead(async read => {
        const clientGroup1 = await persist.getClientGroup(
          client1.clientGroupID,
          read,
        );
        assert(clientGroup1);
        const clientGroup2 = await persist.getClientGroup(
          client2.clientGroupID,
          read,
        );
        assert(clientGroup2);
        const clientGroup3 = await persist.getClientGroup(
          client3.clientGroupID,
          read,
        );
        assert(clientGroup3);
        return {
          clientGroup1,
          clientGroup2,
          clientGroup3,
        };
      });

    const pushRequestJSONBodies: PushRequestDD31[] = [];
    const pullRequestJsonBodies: JSONObject[] = [];
    fetchMock.reset();
    fetchMock.post(
      pushURL,
      async (_url: string, _options: RequestInit, request: Request) => {
        const requestJSON = await request.json();
        assertPushRequestDD31(requestJSON);
        pushRequestJSONBodies.push(requestJSON);
        if (requestJSON.mutations.find(m => m.clientID === client2ID)) {
          throw new Error('test error in push');
        } else {
          return 'ok';
        }
      },
    );
    fetchMock.post(
      pullURL,
      async (_url: string, _options: RequestInit, request: Request) => {
        const requestJson = await request.json();
        assertJSONObject(requestJson);
        pullRequestJsonBodies.push(requestJson);
        const {clientID} = requestJson;
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
            throw new Error(`Unexpected pull ${requestJson}`);
        }
      },
    );

    await rep.recoverMutations();

    expect(pushRequestJSONBodies.length).to.equal(3);
    expect(pushRequestJSONBodies[0]).to.deep.equal(
      createPushRequestBodyDD31(
        profileID,
        client1.clientGroupID,
        client1ID,
        client1PendingLocalMetas,
        schemaVersion,
      ),
    );
    expect(pushRequestJSONBodies[1]).to.deep.equal(
      createPushRequestBodyDD31(
        profileID,
        client2.clientGroupID,
        client2ID,
        client2PendingLocalMetas,
        schemaVersion,
      ),
    );
    expect(pushRequestJSONBodies[2]).to.deep.equal(
      createPushRequestBodyDD31(
        profileID,
        client3.clientGroupID,
        client3ID,
        client3PendingLocalMetas,
        schemaVersion,
      ),
    );

    expect(pullRequestJsonBodies.length).to.equal(2);
    expect(pullRequestJsonBodies[0]).to.deep.equal({
      profileID,
      clientGroupID: client1.clientGroupID,
      schemaVersion,
      cookie: 1,
      pullVersion: 1,
    });
    expect(pullRequestJsonBodies[1]).to.deep.equal({
      profileID,
      clientGroupID: client3.clientGroupID,
      schemaVersion,
      cookie: 3,
      pullVersion: 1,
    });

    const updateClients = await testPerdag.withRead(read =>
      persist.getClients(read),
    );
    const updatedClient1 = updateClients.get(client1ID);
    assertClientDD31(updatedClient1);
    const updatedClient2 = updateClients.get(client2ID);
    assertClientDD31(updatedClient2);
    const updatedClient3 = updateClients.get(client3ID);
    assertClientDD31(updatedClient3);

    const updatedClientGroups = await testPerdag.withRead(read =>
      persist.getClientGroups(read),
    );
    const updatedClientGroup1 = updatedClientGroups.get(client1.clientGroupID);
    assert(updatedClientGroup1);
    const updatedClientGroup2 = updatedClientGroups.get(client2.clientGroupID);
    assert(updatedClientGroup2);
    const updatedClientGroup3 = updatedClientGroups.get(client3.clientGroupID);
    assert(updatedClientGroup3);

    expect(updatedClient1).to.deep.equal(client1);
    expect(updatedClientGroup1).to.deep.equal(clientGroup1);

    expect(updatedClient2).to.deep.equal(client2);
    expect(updatedClientGroup2).to.deep.equal(clientGroup2);

    expect(updatedClient3).to.deep.equal(client3);
    expect(updatedClientGroup3).to.deep.equal(clientGroup3);
  });

  test('if an error occurs recovering one client, continues to try to recover other clients', async () => {
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

    await tickAFewTimes();

    const testPerdag = await createPerdag({
      replicacheName: rep.name,
      schemaVersion,
      replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_DD31,
    });

    const client1PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31(
        client1ID,
        testPerdag,
        2,
        ['client1', 'mutator_name_2', 'mutator_name_3'],
        1,
      );
    await createAndPersistClientWithPendingLocalDD31(
      client2ID,
      testPerdag,
      1,
      ['client2', 'mutator_name_2'],
      2,
    );
    const client3PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31(
        client3ID,
        testPerdag,
        1,
        ['client3', 'mutator_name_2'],
        3,
      );

    const clients = await testPerdag.withRead(read => persist.getClients(read));
    const client1 = clients.get(client1ID);
    assertClientDD31(client1);
    const client2 = clients.get(client2ID);
    assertClientDD31(client2);
    const client3 = clients.get(client3ID);
    assertClientDD31(client3);

    const {clientGroup1, clientGroup2, clientGroup3} =
      await testPerdag.withRead(async read => {
        const clientGroup1 = await persist.getClientGroup(
          client1.clientGroupID,
          read,
        );
        assert(clientGroup1);
        const clientGroup2 = await persist.getClientGroup(
          client2.clientGroupID,
          read,
        );
        assert(clientGroup2);
        const clientGroup3 = await persist.getClientGroup(
          client3.clientGroupID,
          read,
        );
        assert(clientGroup3);
        return {
          clientGroup1,
          clientGroup2,
          clientGroup3,
        };
      });

    const pullRequestJsonBodies: JSONObject[] = [];
    fetchMock.reset();
    fetchMock.post(pushURL, 'ok');
    fetchMock.post(
      pullURL,
      async (_url: string, _options: RequestInit, request: Request) => {
        const requestJson = await request.json();
        assertJSONObject(requestJson);
        pullRequestJsonBodies.push(requestJson);
        const {clientID} = requestJson;
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
            throw new Error(`Unexpected pull ${requestJson}`);
        }
      },
    );

    const lazyDagWithWriteStub = sinon.stub(
      dag.LazyStore.prototype,
      'withWrite',
    );
    const testErrorMsg = 'Test dag.LazyStore.withWrite error';
    lazyDagWithWriteStub.onSecondCall().throws(testErrorMsg);
    lazyDagWithWriteStub.callThrough();

    const consoleErrorStub = sinon.stub(console, 'error');

    await rep.recoverMutations();

    expect(consoleErrorStub.callCount).to.equal(1);
    expect(consoleErrorStub.firstCall.args.join(' ')).to.contain(testErrorMsg);

    const pushCalls = fetchMock.calls(pushURL);
    expect(pushCalls.length).to.equal(2);
    expect(await pushCalls[0].request.json()).to.deep.equal(
      createPushRequestBodyDD31(
        profileID,
        client1.clientGroupID,
        client1ID,
        client1PendingLocalMetas,
        schemaVersion,
      ),
    );
    expect(await pushCalls[1].request.json()).to.deep.equal(
      createPushRequestBodyDD31(
        profileID,
        client3.clientGroupID,
        client3ID,
        client3PendingLocalMetas,
        schemaVersion,
      ),
    );

    expect(pullRequestJsonBodies.length).to.equal(2);
    expect(pullRequestJsonBodies[0]).to.deep.equal({
      profileID,
      clientGroupID: client1.clientGroupID,
      schemaVersion,
      cookie: 1,
      pullVersion: 1,
    });
    expect(pullRequestJsonBodies[1]).to.deep.equal({
      profileID,
      clientGroupID: client3.clientGroupID,
      schemaVersion,
      cookie: 3,
      pullVersion: 1,
    });

    const updateClients = await testPerdag.withRead(read =>
      persist.getClients(read),
    );
    const updatedClient1 = updateClients.get(client1ID);
    assertClientDD31(updatedClient1);
    const updatedClient2 = updateClients.get(client2ID);
    assertClientDD31(updatedClient2);
    const updatedClient3 = updateClients.get(client3ID);
    assertClientDD31(updatedClient3);

    const updatedClientGroups = await testPerdag.withRead(read =>
      persist.getClientGroups(read),
    );
    const updatedClientGroup1 = updatedClientGroups.get(client1.clientGroupID);
    assert(updatedClientGroup1);
    const updatedClientGroup2 = updatedClientGroups.get(client2.clientGroupID);
    assert(updatedClientGroup2);
    const updatedClientGroup3 = updatedClientGroups.get(client3.clientGroupID);
    assert(updatedClientGroup3);

    expect(updatedClient1).to.deep.equal(client1);
    expect(updatedClientGroup1).to.deep.equal(clientGroup1);

    expect(updatedClient2).to.deep.equal(client2);
    expect(updatedClientGroup2).to.deep.equal(clientGroup2);

    expect(updatedClient3).to.deep.equal(client3);
    expect(updatedClientGroup3).to.deep.equal(clientGroup3);
  });

  test('if an error occurs recovering one db, continues to try to recover clients from other dbs', async () => {
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

    await tickAFewTimes();

    const testPerdagForClient1 = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersionOfClient1,
      replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_DD31,
    });
    await createAndPersistClientWithPendingLocalDD31(
      client1ID,
      testPerdagForClient1,
      1,
      ['client1', 'mutator_name_2'],
      1,
    );

    const testPerdagForClient2 = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersionOfClient2,
      replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_DD31,
    });
    const client2PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31(
        client2ID,
        testPerdagForClient2,
        1,
        ['client2', 'mutator_name_2'],
        2,
      );

    const client1 = await testPerdagForClient1.withRead(read =>
      persist.getClient(client1ID, read),
    );
    assertClientDD31(client1);

    const client2 = await testPerdagForClient2.withRead(read =>
      persist.getClient(client2ID, read),
    );
    assertClientDD31(client2);

    const clientGroup1 = await testPerdagForClient1.withRead(read =>
      persist.getClientGroup(client1.clientGroupID, read),
    );
    assert(clientGroup1);

    const clientGroup2 = await testPerdagForClient2.withRead(read =>
      persist.getClientGroup(client2.clientGroupID, read),
    );
    assert(clientGroup2);

    const pullRequestJsonBodies: JSONObject[] = [];
    fetchMock.reset();
    fetchMock.post(pushURL, 'ok');
    fetchMock.post(
      pullURL,
      async (_url: string, _options: RequestInit, request: Request) => {
        const requestJson = await request.json();
        assertJSONObject(requestJson);
        pullRequestJsonBodies.push(requestJson);
        const {clientGroupID} = requestJson;
        switch (clientGroupID) {
          case client2.clientGroupID: {
            const pullResponse: PullResponseDD31 = {
              cookie: 'pull_cookie_2',
              lastMutationIDChanges: {
                [client2ID]: clientGroup2.mutationIDs[client2ID] ?? 0,
              },
              patch: [],
            };

            return pullResponse;
          }
          default:
            throw new Error(`Unexpected pull ${requestJson}`);
        }
      },
    );

    const dagStoreWithReadStub = sinon.stub(
      dag.StoreImpl.prototype,
      'withRead',
    );
    const testErrorMsg = 'Test dag.StoreImpl.withRead error';
    dagStoreWithReadStub.onSecondCall().throws(testErrorMsg);
    dagStoreWithReadStub.callThrough();

    const consoleErrorStub = sinon.stub(console, 'error');

    await rep.recoverMutations();

    expect(consoleErrorStub.callCount).to.equal(1);
    expect(consoleErrorStub.firstCall.args.join(' ')).to.contain(testErrorMsg);

    const pushCalls = fetchMock.calls(pushURL);
    expect(pushCalls.length).to.equal(1);
    expect(await pushCalls[0].request.json()).to.deep.equal(
      createPushRequestBodyDD31(
        profileID,
        client2.clientGroupID,
        client2ID,
        client2PendingLocalMetas,
        schemaVersionOfClient2,
      ),
    );

    expect(pullRequestJsonBodies.length).to.equal(1);
    expect(pullRequestJsonBodies[0]).to.deep.equal({
      clientGroupID: client2.clientGroupID,
      profileID,
      schemaVersion: schemaVersionOfClient2,
      cookie: 2,
      pullVersion: PULL_VERSION_DD31,
    });

    const updatedClient1 = await testPerdagForClient1.withRead(read =>
      persist.getClient(client1ID, read),
    );
    assertClientDD31(updatedClient1);

    const updatedClient2 = await testPerdagForClient2.withRead(read =>
      persist.getClient(client2ID, read),
    );
    assertClientDD31(updatedClient2);

    const updatedClientGroup1 = await testPerdagForClient1.withRead(read =>
      persist.getClientGroup(updatedClient1.clientGroupID, read),
    );
    assert(updatedClientGroup1);
    const updatedClientGroup2 = await testPerdagForClient2.withRead(read =>
      persist.getClientGroup(updatedClient2.clientGroupID, read),
    );
    assert(updatedClientGroup2);

    expect(updatedClientGroup1.mutationIDs[client1ID]).equal(
      clientGroup1.mutationIDs[client1ID],
    );
    // lastServerAckdMutationID not updated due to error when recovering this
    // client's db
    expect(updatedClientGroup1.lastServerAckdMutationIDs[client1ID]).equal(
      clientGroup1.lastServerAckdMutationIDs[client1ID],
    );
    expect(updatedClient1.headHash).to.equal(client1.headHash);

    expect(updatedClientGroup2.mutationIDs[client2ID]).equal(
      clientGroup2.mutationIDs[client2ID],
    );
    // lastServerAckdMutationID is updated to high mutationID as mutations
    // were recovered despite error in other db
    expect(updatedClientGroup2.lastServerAckdMutationIDs[client2ID]).equal(
      clientGroup2.mutationIDs[client2ID],
    );
    expect(updatedClient2.headHash).to.equal(client2.headHash);
  });

  test('mutation recovery exits early if Replicache is closed', async () => {
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

    await tickAFewTimes();

    const testPerdag = await createPerdag({
      replicacheName: rep.name,
      schemaVersion,
      replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_DD31,
    });

    const client1PendingLocalMetas =
      await createAndPersistClientWithPendingLocalDD31(
        client1ID,
        testPerdag,
        1,
        ['mutator_name_2', 'client1'],
        1,
      );

    await createAndPersistClientWithPendingLocalDD31(
      client2ID,
      testPerdag,
      1,
      // Different mutator names to ensure different client groups.
      ['mutator_name_2', 'client2'],
      2,
    );

    const clients = await testPerdag.withRead(read => persist.getClients(read));
    const client1 = clients.get(client1ID);
    assertClientDD31(client1);
    const client2 = clients.get(client2ID);
    assertClientDD31(client2);

    const pullRequestJsonBodies: JSONObject[] = [];
    fetchMock.reset();
    fetchMock.post(pushURL, 'ok');
    fetchMock.post(
      pullURL,
      async (_url: string, _options: RequestInit, request: Request) => {
        const requestJson = await request.json();
        assertJSONObject(requestJson);
        pullRequestJsonBodies.push(requestJson);
        const {clientID} = requestJson;
        const resp: PullResponseDD31 = {
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
            throw new Error(`Unexpected pull ${requestJson}`);
        }
      },
    );

    // At the end of recovering client1 close the recovering Replicache instance
    const lazyDagWithWriteStub = sinon.stub(dag.LazyStore.prototype, 'close');
    lazyDagWithWriteStub.onFirstCall().callsFake(async () => {
      await rep.close();
    });
    lazyDagWithWriteStub.callThrough();

    await rep.recoverMutations();

    const pushCalls = fetchMock.calls(pushURL);
    expect(pushCalls.length).to.equal(1);
    expect(await pushCalls[0].request.json()).to.deep.equal(
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
    expect(rep.recoverMutationsSpy.callCount).to.equal(1);
    expect(rep.recoverMutationsSpy.callCount).to.equal(1);
    expect(await rep.recoverMutationsSpy.firstCall.returnValue).to.equal(true);
  });

  test('mutation recovery returns early without running if push is disabled', async () => {
    const rep = await replicacheForTesting(
      'mutation-recovery-startup',
      {
        pullURL: 'https://diff.com/pull',
      },
      {
        useDefaultURLs: false,
      },
    );
    expect(rep.recoverMutationsSpy.callCount).to.equal(1);
    expect(await rep.recoverMutationsSpy.firstCall.returnValue).to.equal(false);
    expect(await rep.recoverMutations()).to.equal(false);
  });

  test('mutation recovery returns early when internal option enableMutationRecovery is false', async () => {
    const rep = await replicacheForTesting('mutation-recovery-startup', {
      pullURL: 'https://diff.com/pull',
      ...disableAllBackgroundProcesses,
    });
    expect(rep.recoverMutationsSpy.callCount).to.equal(1);
    expect(await rep.recoverMutationsSpy.firstCall.returnValue).to.equal(false);
    expect(await rep.recoverMutations()).to.equal(false);
  });

  test('mutation recovery is invoked on change from offline to online', async () => {
    const pullURL = 'https://test.replicache.dev/pull';
    const rep = await replicacheForTesting('mutation-recovery-online', {
      pullURL,
    });
    expect(rep.recoverMutationsSpy.callCount).to.equal(1);
    expect(rep.online).to.equal(true);

    fetchMock.post(pullURL, () => {
      return {throws: new Error('Simulate fetch error in push')};
    });

    rep.pull();

    await tickAFewTimes();
    expect(rep.online).to.equal(false);
    expect(rep.recoverMutationsSpy.callCount).to.equal(1);

    const clientID = await rep.clientID;
    fetchMock.reset();
    fetchMock.post(pullURL, {
      cookie: 'test_cookie',
      lastMutationIDChanges: {[clientID]: 2},
      patch: [],
    });

    rep.pull();
    expect(rep.recoverMutationsSpy.callCount).to.equal(1);
    while (!rep.online) {
      await tickAFewTimes();
    }
    expect(rep.recoverMutationsSpy.callCount).to.equal(2);
  });

  test('mutation recovery is invoked on 5 minute interval', async () => {
    const rep = await replicacheForTesting('mutation-recovery-startup-dd31-4', {
      enableLicensing: false,
    });
    expect(rep.recoverMutationsSpy.callCount).to.equal(1);
    await clock.tickAsync(5 * 60 * 1000);
    expect(rep.recoverMutationsSpy.callCount).to.equal(2);
    await clock.tickAsync(5 * 60 * 1000);
    expect(rep.recoverMutationsSpy.callCount).to.equal(3);
  });

  suite('Recover mutations across replicache format versions', () => {
    test('DD31 client with one old SDD format client should be recovered', async () => {
      const client1ID = 'client1';
      const auth = '1';
      const pushURL = 'https://test.replicache.dev/push';
      const pullURL = 'https://test.replicache.dev/pull';
      const schemaVersion = 'schema-version-1';
      const rep = await replicacheForTesting('old-client-sdd-new-client-dd31', {
        auth,
        pushURL,
        pullURL,
        schemaVersion,
      });
      const profileID = await rep.profileID;

      const testPerdagSDD = await createPerdag({
        replicacheName: rep.name,
        schemaVersion,
        replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_SDD,
      });

      const client1PendingLocalMetasSDD =
        await createAndPersistClientWithPendingLocalSDD(
          client1ID,
          testPerdagSDD,
          1,
        );

      const client1 = await testPerdagSDD.withRead(read =>
        persist.getClient(client1ID, read),
      );
      assertClientSDD(client1);
      expect(client1.mutationID).to.equal(2);

      const pullRequestJSONBodies: JSONObject[] = [];
      const pushRequestJSONBodies: JSONObject[] = [];
      fetchMock.reset();
      fetchMock.post(
        pushURL,
        async (_url: string, _options: RequestInit, request: Request) => {
          const requestJSON = await request.json();
          assertJSONObject(requestJSON);
          pushRequestJSONBodies.push(requestJSON);
          return 'ok';
        },
      );
      fetchMock.post(
        pullURL,
        async (_url: string, _options: RequestInit, request: Request) => {
          const requestJSON = await request.json();
          assertJSONObject(requestJSON);
          pullRequestJSONBodies.push(requestJSON);
          switch (requestJSON.clientID) {
            case client1ID:
              return {
                cookie: 'pull_cookie_1',
                lastMutationID: client1.mutationID,
                patch: [],
              };
          }
          throw new Error();
        },
      );

      await rep.recoverMutations();

      const pushRequestBody: PushRequestSDD = {
        clientID: 'client1',
        mutations: [
          {
            args: client1PendingLocalMetasSDD[0].mutatorArgsJSON,
            id: client1PendingLocalMetasSDD[0].mutationID,
            name: client1PendingLocalMetasSDD[0].mutatorName,
            timestamp: client1PendingLocalMetasSDD[0].timestamp,
          },
        ],
        profileID,
        pushVersion: PUSH_VERSION_SDD,
        schemaVersion: 'schema-version-1',
      };
      expect(pushRequestJSONBodies).to.deep.equal([pushRequestBody]);
      const pullRequestBody: PullRequestSDD = {
        clientID: client1ID,
        cookie: 'cookie_1',
        lastMutationID: client1.lastServerAckdMutationID,
        profileID,
        pullVersion: PULL_VERSION_SDD,
        schemaVersion,
      };
      expect(pullRequestJSONBodies).to.deep.equal([pullRequestBody]);

      const updatedClient1 = await testPerdagSDD.withRead(read =>
        persist.getClient(client1ID, read),
      );
      expect(updatedClient1).to.deep.equal({
        ...client1,
        // This got updated by the mutation recovery!
        lastServerAckdMutationID: client1.mutationID,
      });
    });

    async function testWithSDDAndDD31(
      schemaVersion1: string,
      schemaVersion2: string,
    ) {
      const client1ID = 'client1';
      const client2ID = 'client2';
      const auth = '1';
      const pushURL = 'https://test.replicache.dev/push';
      const pullURL = 'https://test.replicache.dev/pull';
      const rep = await replicacheForTesting(
        'old-client-sdd-and-old-client-dd31-new-client-dd31',
        {
          auth,
          pushURL,
          pullURL,
          schemaVersion: schemaVersion1,
        },
      );
      const profileID = await rep.profileID;

      const testPerdagSDD = await createPerdag({
        replicacheName: rep.name,
        schemaVersion: schemaVersion1,
        replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_SDD,
      });

      const testPerdagDD31 = await createPerdag({
        replicacheName: rep.name,
        schemaVersion: schemaVersion2,
        replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_DD31,
      });

      const client1PendingLocalMetasSDD =
        await createAndPersistClientWithPendingLocalSDD(
          client1ID,
          testPerdagSDD,
          1,
        );

      const client2PendingLocalMetasDD31 =
        await createAndPersistClientWithPendingLocalDD31(
          client2ID,
          testPerdagDD31,
          1,
          ['client-2', 'mutator_name_2'],
          'c2',
        );

      const client1 = await testPerdagSDD.withRead(read =>
        persist.getClient(client1ID, read),
      );
      assertClientSDD(client1);
      expect(client1.mutationID).to.equal(2);

      const client2 = await testPerdagDD31.withRead(read =>
        persist.getClient(client2ID, read),
      );
      assertClientDD31(client2);
      const clientGroup2 = await testPerdagDD31.withRead(read =>
        persist.getClientGroup(client2.clientGroupID, read),
      );
      assert(clientGroup2);
      expect(clientGroup2.mutationIDs[client2ID]).to.equal(2);

      const pullRequestJSONBodies: JSONObject[] = [];
      const pushRequestJSONBodies: JSONObject[] = [];
      fetchMock.reset();
      fetchMock.post(
        pushURL,
        async (_url: string, _options: RequestInit, request: Request) => {
          const requestJSON = await request.json();
          assertJSONObject(requestJSON);
          pushRequestJSONBodies.push(requestJSON);
          return 'ok';
        },
      );
      fetchMock.post(
        pullURL,
        async (_url: string, _options: RequestInit, request: Request) => {
          const requestJSON = await request.json();
          assertJSONObject(requestJSON);
          pullRequestJSONBodies.push(requestJSON);
          if (requestJSON.clientID === client1ID) {
            const resp: PullResponseSDD = {
              cookie: 'pull_cookie_1',
              lastMutationID: client1.mutationID,
              patch: [],
            };
            return resp;
          }
          if (requestJSON.clientGroupID === client2.clientGroupID) {
            const resp: PullResponseDD31 = {
              cookie: 'c3',
              lastMutationIDChanges: clientGroup2.mutationIDs,
              patch: [],
            };
            return resp;
          }
          throw new Error();
        },
      );

      await rep.recoverMutations();

      const pushRequestBody1: PushRequestSDD = {
        clientID: 'client1',
        mutations: [
          {
            args: client1PendingLocalMetasSDD[0].mutatorArgsJSON,
            id: client1PendingLocalMetasSDD[0].mutationID,
            name: client1PendingLocalMetasSDD[0].mutatorName,
            timestamp: client1PendingLocalMetasSDD[0].timestamp,
          },
        ],
        profileID,
        pushVersion: PUSH_VERSION_SDD,
        schemaVersion: schemaVersion1,
      };
      const pushRequestBody2: PushRequestDD31 = {
        clientGroupID: client2.clientGroupID,
        mutations: [
          {
            clientID: client2ID,
            args: client2PendingLocalMetasDD31[0].mutatorArgsJSON,
            id: client2PendingLocalMetasDD31[0].mutationID,
            name: client2PendingLocalMetasDD31[0].mutatorName,
            timestamp: client2PendingLocalMetasDD31[0].timestamp,
          },
        ],
        profileID,
        pushVersion: PUSH_VERSION_DD31,
        schemaVersion: schemaVersion2,
      };

      const expectRequestBodies = (expected: unknown[], actual: unknown[]) => {
        expect(actual.length).to.equal(expected.length);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const sortByClientID = (a: any, b: any) =>
          stringCompare(
            a.clientID ?? a.clientGroupID,
            b.clientID ?? b.clientGroupID,
          );
        expect(actual.sort(sortByClientID)).to.deep.equal(
          expected.sort(sortByClientID),
        );
      };

      expectRequestBodies(pushRequestJSONBodies, [
        pushRequestBody2,
        pushRequestBody1,
      ]);
      const pullRequestBody1: PullRequestSDD = {
        clientID: client1ID,
        cookie: 'cookie_1',
        lastMutationID: client1.lastServerAckdMutationID,
        profileID,
        pullVersion: PULL_VERSION_SDD,
        schemaVersion: schemaVersion1,
      };
      const pullRequestBody2: PullRequestDD31 = {
        clientGroupID: client2.clientGroupID,
        cookie: 'c2',
        profileID,
        pullVersion: PULL_VERSION_DD31,
        schemaVersion: schemaVersion2,
      };
      expectRequestBodies(pullRequestJSONBodies, [
        pullRequestBody2,
        pullRequestBody1,
      ]);

      const updatedClient1 = await testPerdagSDD.withRead(read =>
        persist.getClient(client1ID, read),
      );
      assertClientSDD(updatedClient1);
      expect(updatedClient1).to.deep.equal({
        ...client1,
        // This got updated by the mutation recovery!
        lastServerAckdMutationID: client1.mutationID,
      });

      const updatedClient2 = await testPerdagDD31.withRead(read =>
        persist.getClient(client2ID, read),
      );
      assertClientDD31(updatedClient2);
      expect(updatedClient2).to.deep.equal(client2);

      const updatedClientGroup2 = await testPerdagDD31.withRead(read =>
        persist.getClientGroup(client2.clientGroupID, read),
      );
      expect(updatedClientGroup2).to.deep.equal({
        ...clientGroup2,
        lastServerAckdMutationIDs: {
          ...clientGroup2.lastServerAckdMutationIDs,
          // This got updated by the mutation recovery!
          [client2ID]: clientGroup2.mutationIDs[client2ID],
        },
      });
    }

    test('One SDD client and one DD31 client should be recovered', async () => {
      await testWithSDDAndDD31('test-schema-1', 'test-schema-1');
    });

    test('One SDD client and one DD31 client should be recovered different perdag due to schema', async () => {
      await testWithSDDAndDD31('test-schema-1', 'test-schema-2');
    });
  });

  async function testPushDisabled(
    schemaVersion1: string,
    schemaVersion2: string,
  ) {
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
        // eslint-disable-next-line @typescript-eslint/naming-convention
        mutator_name_2: () => {
          return;
        },
      },
    });

    const testPerdagDD31 = await createPerdag({
      replicacheName: rep.name,
      schemaVersion: schemaVersion2,
      replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_DD31,
    });

    await createAndPersistClientWithPendingLocalDD31(
      client1ID,
      testPerdagDD31,
      1,
      Object.keys(rep.mutate),
      'c1',
    );

    const client1 = await testPerdagDD31.withRead(read =>
      persist.getClient(client1ID, read),
    );
    assertClientDD31(client1);
    const clientGroup1 = await testPerdagDD31.withRead(read =>
      persist.getClientGroup(client1.clientGroupID, read),
    );
    assert(clientGroup1);
    expect(clientGroup1.mutationIDs[client1ID]).to.equal(2);

    const pullRequestJSONBodies: JSONObject[] = [];
    const pushRequestJSONBodies: JSONObject[] = [];
    fetchMock.reset();
    fetchMock.post(
      pushURL,
      async (_url: string, _options: RequestInit, request: Request) => {
        const requestJSON = await request.json();
        assertJSONObject(requestJSON);
        pushRequestJSONBodies.push(requestJSON);
        throw new Error();
      },
    );
    fetchMock.post(
      pullURL,
      async (_url: string, _options: RequestInit, request: Request) => {
        const requestJSON = await request.json();
        assertJSONObject(requestJSON);
        pullRequestJSONBodies.push(requestJSON);
        throw new Error();
      },
    );

    await rep.recoverMutations();

    expect(pushRequestJSONBodies).to.deep.equal([]);
    expect(pullRequestJSONBodies).to.deep.equal([]);

    const updatedClient1 = await testPerdagDD31.withRead(read =>
      persist.getClient(client1ID, read),
    );
    assertClientDD31(updatedClient1);
    expect(updatedClient1).to.deep.equal(client1);

    const updatedClientGroup1 = await testPerdagDD31.withRead(read =>
      persist.getClientGroup(client1.clientGroupID, read),
    );
    expect(updatedClientGroup1).to.deep.equal(clientGroup1);
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
      replicacheFormatVersion: REPLICACHE_FORMAT_VERSION_DD31,
    });

    const client1PendingLocalMetasDD31 =
      await createAndPersistClientWithPendingLocalDD31(
        client1ID,
        testPerdagDD31,
        1,
        ['client-1', 'mutator_name_2'],
        'c2',
      );

    const client1 = await testPerdagDD31.withRead(read =>
      persist.getClient(client1ID, read),
    );
    assertClientDD31(client1);
    const clientGroup1 = await testPerdagDD31.withRead(read =>
      persist.getClientGroup(client1.clientGroupID, read),
    );
    assert(clientGroup1);
    expect(clientGroup1.mutationIDs[client1ID]).to.equal(2);

    const pullRequestJSONBodies: JSONObject[] = [];
    const pushRequestJSONBodies: JSONObject[] = [];
    fetchMock.reset();
    fetchMock.post(
      pushURL,
      async (_url: string, _options: RequestInit, request: Request) => {
        const requestJSON = await request.json();
        assertJSONObject(requestJSON);
        pushRequestJSONBodies.push(requestJSON);
        return 'ok';
      },
    );
    fetchMock.post(
      pullURL,
      async (_url: string, _options: RequestInit, request: Request) => {
        const requestJSON = await request.json();
        assertJSONObject(requestJSON);
        pullRequestJSONBodies.push(requestJSON);
        throw new Error();
      },
    );

    await rep.recoverMutations();

    const pushRequestBody1: PushRequestDD31 = {
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
    expect(pushRequestJSONBodies).to.deep.equal([pushRequestBody1]);

    expect(pullRequestJSONBodies).to.deep.equal([]);

    const updatedClient1 = await testPerdagDD31.withRead(read =>
      persist.getClient(client1ID, read),
    );
    assertClientDD31(updatedClient1);
    expect(updatedClient1).to.deep.equal(client1);

    const updatedClientGroup1 = await testPerdagDD31.withRead(read =>
      persist.getClientGroup(client1.clientGroupID, read),
    );
    // This did not get updated because pull was disabled!
    expect(updatedClientGroup1).to.deep.equal(clientGroup1);
  }

  test('pullDisabled so cannot confirm recovery', async () => {
    await testPullDisabled('schema-version', 'schema-version');
  });

  test('pullDisabled so cannot confirm recovery different perdag due to schema', async () => {
    await testPullDisabled('schema-version-1', 'schema-version-2');
  });
});
