import {SinonFakeTimers, useFakeTimers} from 'sinon';
import {assertNotUndefined} from '../asserts';
import {getLatestGCUpdate, initBranchGC} from './branch-gc';
import {
  Branch,
  BranchMap,
  getBranches,
  setBranch,
  setBranches,
} from './branches';
import * as dag from '../dag/mod';
import {fakeHash} from '../hash';
import {makeClient, setClientsForTesting} from './clients-test-helpers';
import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';

let clock: SinonFakeTimers;
const START_TIME = 0;
const FIVE_MINS_IN_MS = 5 * 60 * 1000;
setup(() => {
  clock = useFakeTimers(0);
});

teardown(() => {
  clock.restore();
});

function awaitLatestGCUpdate(): Promise<BranchMap> {
  const latest = getLatestGCUpdate();
  assertNotUndefined(latest);
  return latest;
}

async function expectBranches(
  dagStore: dag.TestStore,
  branches: Record<string, Branch>,
) {
  await dagStore.withRead(async (read: dag.Read) => {
    const readBranchMap = await getBranches(read);
    expect(Object.fromEntries(readBranchMap)).to.deep.equal(branches);
  });
}

test('initBranchGC starts 5 min interval that collects branches that are not referred to by any clients and have no pending mutations', async () => {
  if (!DD31) {
    return;
  }
  const dagStore = new dag.TestStore();
  const branch1 = {
    headHash: fakeHash('eadbac1'),
    mutatorNames: [],
    indexes: {},
    mutationIDs: {client1: 10},
    lastServerAckdMutationIDs: {},
  };
  const branch2 = {
    headHash: fakeHash('eadbac2'),
    mutatorNames: [],
    indexes: {},
    mutationIDs: {client2: 2, client3: 3},
    lastServerAckdMutationIDs: {client2: 2, client3: 3},
  };
  const branch3 = {
    headHash: fakeHash('eadbac3'),
    mutatorNames: [],
    indexes: {},
    mutationIDs: {},
    lastServerAckdMutationIDs: {},
  };
  const branchMap = await dagStore.withWrite(async write => {
    const branchMap = new Map(
      Object.entries({
        branch1,
        branch2,
        branch3,
      }),
    );
    await setBranches(branchMap, write);
    await write.commit();
    return branchMap;
  });
  const client1 = makeClient({
    heartbeatTimestampMs: START_TIME,
    headHash: fakeHash('eadce1'),
    branchID: 'branch1',
  });
  const client2 = makeClient({
    heartbeatTimestampMs: START_TIME,
    headHash: fakeHash('eadce2'),
    branchID: 'branch2',
  });
  const client3 = makeClient({
    heartbeatTimestampMs: START_TIME,
    headHash: fakeHash('eadce3'),
    branchID: 'branch2',
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
  initBranchGC(dagStore, new LogContext(), controller.signal);

  await dagStore.withRead(async (read: dag.Read) => {
    const readBranchMap = await getBranches(read);
    expect(readBranchMap).to.deep.equal(branchMap);
  });

  await clock.tickAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // branch1 is not collected because it is referred to by client1 and has pending mutations
  // branch2 is not collected because it is referred to by client2 and client3
  // branch3 is collected because it is not referred to by any client and has no pending mutations
  await expectBranches(dagStore, {branch1, branch2});

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
  await expectBranches(dagStore, {branch1, branch2});

  await clock.tickAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // branch1 is not collected because it has pending mutations
  // branch2 is not collected because it is referred to by client2 and client3
  await expectBranches(dagStore, {branch1, branch2});

  // update branch1 to have no pending mutations
  const updatedBranch1 = {
    ...branch1,
    lastServerAckdMutationIDs: branch1.mutationIDs,
  };
  await dagStore.withWrite(async write => {
    await setBranch('branch1', updatedBranch1, write);
    await write.commit();
    return branchMap;
  });

  // nothing collected yet because gc has not run yet
  await expectBranches(dagStore, {branch1: updatedBranch1, branch2});

  await clock.tickAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // branch1 is collect because it is not referred to and has no pending mutaitons
  // branch2 is not collected because it is referred to by client2 and client3
  await expectBranches(dagStore, {branch2});

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
  await expectBranches(dagStore, {branch2});

  await clock.tickAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // branch2 is not collected because it is referred to by client3
  await expectBranches(dagStore, {branch2});

  // Delete client3
  await setClientsForTesting(new Map(Object.entries({})), dagStore);

  // nothing collected yet because gc has not run yet
  await expectBranches(dagStore, {branch2});

  await clock.tickAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // branch2 is collected because it is not referred to and has pending mutations
  await expectBranches(dagStore, {});
});
