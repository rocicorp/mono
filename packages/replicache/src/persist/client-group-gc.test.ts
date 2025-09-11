import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, expect, test, vi} from 'vitest';
import {assertNotUndefined} from '../../../shared/src/asserts.ts';
import type {Read} from '../dag/store.ts';
import {TestStore} from '../dag/test-store.ts';
import {fakeHash} from '../hash.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {getLatestGCUpdate, initClientGroupGC} from './client-group-gc.ts';
import {
  type ClientGroup,
  type ClientGroupMap,
  getClientGroups,
  setClientGroup,
  setClientGroups,
} from './client-groups.ts';
import {makeClientV6, setClientsForTesting} from './clients-test-helpers.ts';

const START_TIME = 0;
const FIVE_MINS_IN_MS = 5 * 60 * 1000;
beforeEach(() => {
  vi.useFakeTimers({now: 0});
});

afterEach(() => {
  vi.restoreAllMocks();
});

function awaitLatestGCUpdate(): Promise<ClientGroupMap> {
  const latest = getLatestGCUpdate();
  assertNotUndefined(latest);
  return latest;
}

async function expectClientGroups(
  dagStore: TestStore,
  clientGroups: Record<string, ClientGroup>,
) {
  await withRead(dagStore, async (read: Read) => {
    const readClientGroupMap = await getClientGroups(read);
    expect(Object.fromEntries(readClientGroupMap)).to.deep.equal(clientGroups);
  });
}

test('initClientGroupGC starts 5 min interval that collects client groups that are not referred to by any clients and have no pending mutations', async () => {
  const dagStore = new TestStore();
  const clientGroup1 = {
    headHash: fakeHash('eadbac1'),
    mutatorNames: [],
    indexes: {},
    mutationIDs: {client1: 10},
    lastServerAckdMutationIDs: {},
    disabled: false,
  };
  const clientGroup2 = {
    headHash: fakeHash('eadbac2'),
    mutatorNames: [],
    indexes: {},
    mutationIDs: {client2: 2, client3: 3},
    lastServerAckdMutationIDs: {client2: 2, client3: 3},
    disabled: false,
  };
  const clientGroup3 = {
    headHash: fakeHash('eadbac3'),
    mutatorNames: [],
    indexes: {},
    mutationIDs: {},
    lastServerAckdMutationIDs: {},
    disabled: false,
  };
  const clientGroupMap = await withWrite(dagStore, async write => {
    const clientGroupMap = new Map(
      Object.entries({
        'client-group-1': clientGroup1,
        'client-group-2': clientGroup2,
        'client-group-3': clientGroup3,
      }),
    );
    await setClientGroups(clientGroupMap, write);
    return clientGroupMap;
  });
  const client1 = makeClientV6({
    heartbeatTimestampMs: START_TIME,
    refreshHashes: [fakeHash('eadce1')],
    clientGroupID: 'client-group-1',
  });
  const client2 = makeClientV6({
    heartbeatTimestampMs: START_TIME,
    refreshHashes: [fakeHash('eadce2')],
    clientGroupID: 'client-group-2',
  });
  const client3 = makeClientV6({
    heartbeatTimestampMs: START_TIME,
    refreshHashes: [fakeHash('eadce3')],
    clientGroupID: 'client-group-2',
  });
  await setClientsForTesting(
    new Map(
      Object.entries({
        client1,
        client2,
        client3,
      }),
    ),
    dagStore,
  );

  const enableMutationRecovery = true;
  const controller = new AbortController();
  initClientGroupGC(
    dagStore,
    enableMutationRecovery,
    new LogContext(),
    controller.signal,
  );

  await withRead(dagStore, async (read: Read) => {
    const readClientGroupMap = await getClientGroups(read);
    expect(readClientGroupMap).to.deep.equal(clientGroupMap);
  });

  await vi.advanceTimersByTimeAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client-group-1 is not collected because it is referred to by client1 and has pending mutations
  // client-group-2 is not collected because it is referred to by client2 and client3
  // client-group-3 is collected because it is not referred to by any client and has no pending mutations
  await expectClientGroups(dagStore, {
    'client-group-1': clientGroup1,
    'client-group-2': clientGroup2,
  });

  // Delete client1
  await setClientsForTesting(
    new Map(
      Object.entries({
        client2,
        client3,
      }),
    ),
    dagStore,
  );

  // nothing collected yet because gc has not run yet
  await expectClientGroups(dagStore, {
    'client-group-1': clientGroup1,
    'client-group-2': clientGroup2,
  });

  await vi.advanceTimersByTimeAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client-group-1 is not collected because it has pending mutations
  // client-group-2 is not collected because it is referred to by client2 and client3
  await expectClientGroups(dagStore, {
    'client-group-1': clientGroup1,
    'client-group-2': clientGroup2,
  });

  // update client-group-1 to have no pending mutations
  const updatedClientGroup1 = {
    ...clientGroup1,
    lastServerAckdMutationIDs: clientGroup1.mutationIDs,
  };
  await withWrite(dagStore, async write => {
    await setClientGroup('client-group-1', updatedClientGroup1, write);
  });

  // nothing collected yet because gc has not run yet
  await expectClientGroups(dagStore, {
    'client-group-1': updatedClientGroup1,
    'client-group-2': clientGroup2,
  });

  await vi.advanceTimersByTimeAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client-group-1 is collect because it is not referred to and has no pending mutations
  // client-group-2 is not collected because it is referred to by client2 and client3
  await expectClientGroups(dagStore, {'client-group-2': clientGroup2});

  // Delete client2
  await setClientsForTesting(
    new Map(
      Object.entries({
        client3,
      }),
    ),
    dagStore,
  );

  // nothing collected yet because gc has not run yet
  await expectClientGroups(dagStore, {'client-group-2': clientGroup2});

  await vi.advanceTimersByTimeAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client-group-2 is not collected because it is referred to by client3
  await expectClientGroups(dagStore, {'client-group-2': clientGroup2});

  // Delete client3
  await setClientsForTesting(new Map(Object.entries({})), dagStore);

  // nothing collected yet because gc has not run yet
  await expectClientGroups(dagStore, {'client-group-2': clientGroup2});

  await vi.advanceTimersByTimeAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client-group-2 is collected because it is not referred to and has pending mutations
  await expectClientGroups(dagStore, {});
});

test('initClientGroupGC starts 5 min interval that collects client groups that are not referred to by any clients and have no pending mutations. enableMutationRecovery set to false', async () => {
  const dagStore = new TestStore();
  const clientGroup1 = {
    headHash: fakeHash('eadbac1'),
    mutatorNames: [],
    indexes: {},
    mutationIDs: {client1: 10},
    lastServerAckdMutationIDs: {},
    disabled: false,
  };
  const clientGroup2 = {
    headHash: fakeHash('eadbac2'),
    mutatorNames: [],
    indexes: {},
    mutationIDs: {client2: 2, client3: 3},
    lastServerAckdMutationIDs: {client2: 2, client3: 3},
    disabled: false,
  };
  const clientGroup3 = {
    headHash: fakeHash('eadbac3'),
    mutatorNames: [],
    indexes: {},
    mutationIDs: {},
    lastServerAckdMutationIDs: {},
    disabled: false,
  };
  const clientGroupMap = await withWrite(dagStore, async write => {
    const clientGroupMap = new Map(
      Object.entries({
        'client-group-1': clientGroup1,
        'client-group-2': clientGroup2,
        'client-group-3': clientGroup3,
      }),
    );
    await setClientGroups(clientGroupMap, write);
    return clientGroupMap;
  });
  const client1 = makeClientV6({
    heartbeatTimestampMs: START_TIME,
    refreshHashes: [fakeHash('eadce1')],
    clientGroupID: 'client-group-1',
  });
  const client2 = makeClientV6({
    heartbeatTimestampMs: START_TIME,
    refreshHashes: [fakeHash('eadce2')],
    clientGroupID: 'client-group-2',
  });
  const client3 = makeClientV6({
    heartbeatTimestampMs: START_TIME,
    refreshHashes: [fakeHash('eadce3')],
    clientGroupID: 'client-group-2',
  });
  await setClientsForTesting(
    new Map(
      Object.entries({
        client1,
        client2,
        client3,
      }),
    ),
    dagStore,
  );

  const enableMutationRecovery = false;
  const controller = new AbortController();
  initClientGroupGC(
    dagStore,
    enableMutationRecovery,
    new LogContext(),
    controller.signal,
  );

  await withRead(dagStore, async (read: Read) => {
    const readClientGroupMap = await getClientGroups(read);
    expect(readClientGroupMap).to.deep.equal(clientGroupMap);
  });

  await vi.advanceTimersByTimeAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client-group-1 is not collected because it is referred to by client1 and has pending mutations
  // client-group-2 is not collected because it is referred to by client2 and client3
  // client-group-3 is collected because it is not referred to by any client and has no pending mutations
  await expectClientGroups(dagStore, {
    'client-group-1': clientGroup1,
    'client-group-2': clientGroup2,
  });

  // Delete client1
  await setClientsForTesting(
    new Map(
      Object.entries({
        client2,
        client3,
      }),
    ),
    dagStore,
  );

  // nothing collected yet because gc has not run yet
  await expectClientGroups(dagStore, {
    'client-group-1': clientGroup1,
    'client-group-2': clientGroup2,
  });

  await vi.advanceTimersByTimeAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client-group-1 is collected because we ignore pending mutations
  // client-group-2 is not collected because it is referred to by client2 and client3
  await expectClientGroups(dagStore, {
    'client-group-2': clientGroup2,
  });

  // Delete client2
  await setClientsForTesting(
    new Map(
      Object.entries({
        client3,
      }),
    ),
    dagStore,
  );

  // nothing collected yet because gc has not run yet
  await expectClientGroups(dagStore, {'client-group-2': clientGroup2});

  await vi.advanceTimersByTimeAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client-group-2 is not collected because it is referred to by client3
  await expectClientGroups(dagStore, {'client-group-2': clientGroup2});

  // Delete client3
  await setClientsForTesting(new Map(Object.entries({})), dagStore);

  // nothing collected yet because gc has not run yet
  await expectClientGroups(dagStore, {'client-group-2': clientGroup2});

  await vi.advanceTimersByTimeAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client-group-2 is collected because it is not referred to and has pending mutations
  await expectClientGroups(dagStore, {});
});
