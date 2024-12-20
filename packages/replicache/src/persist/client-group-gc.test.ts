import {LogContext} from '@rocicorp/logger';
import {type SinonFakeTimers, useFakeTimers} from 'sinon';
import {afterEach, beforeEach, expect, test} from 'vitest';
import {assertNotUndefined} from '../../../shared/src/asserts.js';
import type {Read} from '../dag/store.js';
import {TestStore} from '../dag/test-store.js';
import {fakeHash} from '../hash.js';
import {withRead, withWriteNoImplicitCommit} from '../with-transactions.js';
import {getLatestGCUpdate, initClientGroupGC} from './client-group-gc.js';
import {
  type ClientGroup,
  type ClientGroupMap,
  getClientGroups,
  setClientGroup,
  setClientGroups,
} from './client-groups.js';
import {makeClientV6, setClientsForTesting} from './clients-test-helpers.js';

let clock: SinonFakeTimers;
const START_TIME = 0;
const FIVE_MINS_IN_MS = 5 * 60 * 1000;
beforeEach(() => {
  clock = useFakeTimers(0);
});

afterEach(() => {
  clock.restore();
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
  const clientGroupMap = await withWriteNoImplicitCommit(
    dagStore,
    async write => {
      const clientGroupMap = new Map(
        Object.entries({
          'client-group-1': clientGroup1,
          'client-group-2': clientGroup2,
          'client-group-3': clientGroup3,
        }),
      );
      await setClientGroups(clientGroupMap, write);
      await write.commit();
      return clientGroupMap;
    },
  );
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

  const controller = new AbortController();
  initClientGroupGC(dagStore, new LogContext(), controller.signal);

  await withRead(dagStore, async (read: Read) => {
    const readClientGroupMap = await getClientGroups(read);
    expect(readClientGroupMap).to.deep.equal(clientGroupMap);
  });

  await clock.tickAsync(FIVE_MINS_IN_MS);
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

  await clock.tickAsync(FIVE_MINS_IN_MS);
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
  await withWriteNoImplicitCommit(dagStore, async write => {
    await setClientGroup('client-group-1', updatedClientGroup1, write);
    await write.commit();
    return clientGroupMap;
  });

  // nothing collected yet because gc has not run yet
  await expectClientGroups(dagStore, {
    'client-group-1': updatedClientGroup1,
    'client-group-2': clientGroup2,
  });

  await clock.tickAsync(FIVE_MINS_IN_MS);
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

  await clock.tickAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client-group-2 is not collected because it is referred to by client3
  await expectClientGroups(dagStore, {'client-group-2': clientGroup2});

  // Delete client3
  await setClientsForTesting(new Map(Object.entries({})), dagStore);

  // nothing collected yet because gc has not run yet
  await expectClientGroups(dagStore, {'client-group-2': clientGroup2});

  await clock.tickAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client-group-2 is collected because it is not referred to and has pending mutations
  await expectClientGroups(dagStore, {});
});
