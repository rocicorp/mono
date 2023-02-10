import {
  addData,
  expectLogContext,
  initReplicacheTesting,
  makePullResponse,
  replicacheForTesting,
  ReplicacheTest,
  tickAFewTimes,
} from './test-util';
import {expect} from '@esm-bundle/chai';
import * as sinon from 'sinon';

// fetch-mock has invalid d.ts file so we removed that on npm install.
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-expect-error
import fetchMock from 'fetch-mock/esm/client';
import * as kv from './kv/mod';
import * as dag from './dag/mod';
import * as persist from './persist/mod';
import {assert, assertNotUndefined} from './asserts';
import {deleteClientForTesting} from './persist/clients-test-helpers';
import {assertClientDD31} from './persist/clients';
import type {MutatorDefs, WriteTransaction} from './mod';
import {deleteBranch} from './persist/branches';
import {assertHash} from './hash';

initReplicacheTesting();

let perdag: dag.Store | undefined;
teardown(async () => {
  await perdag?.close();
});

test('basic persist & load', async () => {
  const pullURL = 'https://diff.com/pull';
  const rep = await replicacheForTesting('persist-test', {
    pullURL,
  });
  const clientID = await rep.clientID;

  perdag = new dag.StoreImpl(
    new kv.IDBStore(rep.idbName),
    dag.uuidChunkHasher,
    assertHash,
  );

  const clientBeforePull = await perdag.withRead(read =>
    persist.getClient(clientID, read),
  );
  assertNotUndefined(clientBeforePull);

  let branchBeforePull: persist.Branch | undefined;
  if (DD31) {
    assertClientDD31(clientBeforePull);
    branchBeforePull = await perdag.withRead(read =>
      persist.getBranch(clientBeforePull.branchID, read),
    );
    assertNotUndefined(branchBeforePull);
  }

  fetchMock.postOnce(
    pullURL,
    makePullResponse(clientID, 2, [
      {
        op: 'put',
        key: 'a',
        value: 1,
      },
      {
        op: 'put',
        key: 'b',
        value: 2,
      },
    ]),
  );

  rep.pull();

  // maxWaitAttempts * waitMs should be at least PERSIST_TIMEOUT
  // plus some buffer for the persist process to complete
  const maxWaitAttempts = 20;
  const waitMs = 100;
  let waitAttempt = 0;
  const run = true;
  while (run) {
    if (waitAttempt++ > maxWaitAttempts) {
      throw new Error(
        `Persist did not complete in ${maxWaitAttempts * waitMs} ms`,
      );
    }
    await tickAFewTimes(waitMs);
    if (DD31) {
      assertClientDD31(clientBeforePull);
      assertNotUndefined(branchBeforePull);
      const branch: persist.Branch | undefined = await perdag.withRead(read =>
        persist.getBranch(clientBeforePull.branchID, read),
      );
      assertNotUndefined(branch);
      if (branchBeforePull.headHash !== branch.headHash) {
        // persist has completed
        break;
      }
    } else {
      const client: persist.Client | undefined = await perdag.withRead(read =>
        persist.getClient(clientID, read),
      );
      assertNotUndefined(client);
      if (clientBeforePull.headHash !== client.headHash) {
        // persist has completed
        break;
      }
    }
  }

  await rep.query(async tx => {
    expect(await tx.get('a')).to.equal(1);
    expect(await tx.get('b')).to.equal(2);
  });

  // If we create another instance it will lazy load the data from IDB
  const rep2 = await replicacheForTesting('persist-test', {
    pullURL,
  });
  await rep2.query(async tx => {
    expect(await tx.get('a')).to.equal(1);
    expect(await tx.get('b')).to.equal(2);
  });

  expect(await rep.clientID).to.not.equal(await rep2.clientID);

  await perdag.close();
});

suite('onClientStateNotFound', () => {
  test('Called in persist if collected', async () => {
    const consoleErrorStub = sinon.stub(console, 'error');

    const rep = await replicacheForTesting('called-in-persist', {
      mutators: {addData},
    });

    await rep.mutate.addData({foo: 'bar'});
    await rep.persist();

    const clientID = await rep.clientID;
    await deleteClientForTesting(clientID, rep.perdag);

    const onClientStateNotFound = sinon.fake();
    rep.onClientStateNotFound = onClientStateNotFound;
    await rep.persist();

    expect(onClientStateNotFound.callCount).to.equal(1);
    expect(onClientStateNotFound.lastCall.args).to.deep.equal([
      {type: 'NotFoundOnClient'},
    ]);
    expectLogContext(
      consoleErrorStub,
      0,
      rep,
      `Client state not found, clientID: ${clientID}`,
    );
  });

  test('Called in query if collected', async () => {
    const consoleErrorStub = sinon.stub(console, 'error');

    const name = 'called-in-query';
    const mutators = {
      addData,
    };
    const rep = await replicacheForTesting(name, {
      mutators,
    });

    await rep.mutate.addData({foo: 'bar'});
    await rep.persist();
    const clientID = await rep.clientID;
    await deleteClientForTesting(clientID, rep.perdag);
    await rep.close();

    const rep2 = await replicacheForTesting(name, {
      mutators,
    });

    const clientID2 = await rep2.clientID;

    await deleteClientForTesting(clientID2, rep2.perdag);
    if (DD31) {
      // Cannot simply gcBranches because the branch has pending mutations.
      await deleteBranchForTesting(rep2);
    }

    const onClientStateNotFound = sinon.fake();
    rep2.onClientStateNotFound = onClientStateNotFound;

    let e: unknown;
    try {
      await rep2.query(async tx => {
        await tx.get('foo');
      });
    } catch (err) {
      e = err;
    }
    expect(e).to.be.instanceOf(persist.ClientStateNotFoundError);
    expectLogContext(
      consoleErrorStub,
      0,
      rep2,
      `Client state not found, clientID: ${clientID2}`,
    );
    expect(onClientStateNotFound.lastCall.args).to.deep.equal([
      {type: 'NotFoundOnClient'},
    ]);
  });

  test('Called in mutate if collected', async () => {
    const consoleErrorStub = sinon.stub(console, 'error');
    const name = 'called-in-mutate';
    const mutators = {
      addData,
      async check(tx: WriteTransaction, key: string) {
        await tx.has(key);
      },
    };

    const rep = await replicacheForTesting(name, {
      mutators,
    });

    await rep.mutate.addData({foo: 'bar'});
    await rep.persist();
    const clientID = await rep.clientID;
    await deleteClientForTesting(clientID, rep.perdag);
    await rep.close();

    const rep2 = await replicacheForTesting(name, {
      mutators,
    });

    const clientID2 = await rep2.clientID;
    await deleteClientForTesting(clientID2, rep2.perdag);
    if (DD31) {
      // Cannot simply gcBranches because the branch has pending mutations.
      await deleteBranchForTesting(rep2);
    }

    const onClientStateNotFound = sinon.fake();
    rep2.onClientStateNotFound = onClientStateNotFound;

    let e: unknown;
    try {
      // Another mutate will trigger
      await rep2.mutate.check('x');
    } catch (err) {
      e = err;
    }

    expect(e).to.be.instanceOf(persist.ClientStateNotFoundError);
    expectLogContext(
      consoleErrorStub,
      0,
      rep2,
      `Client state not found, clientID: ${clientID2}`,
    );
    expect(onClientStateNotFound.lastCall.args).to.deep.equal([
      {type: 'NotFoundOnClient'},
    ]);
  });
});

suite('persist scheduling', () => {
  test('handles exceptions thrown in persist()', async () => {
    const rep = await replicacheForTesting('persist-test');

    // Safari does not have requestIdleTimeout so it waits for a second. We need
    // to wait to have all browsers have a chance to run persist before we
    // continue.
    await tickAFewTimes(20, 100);
    expect(rep.persistIsScheduled).to.be.false;

    const persistStub = sinon
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      .stub(rep, '_persist' as any)
      .callsFake(async () => {
        throw new Error('persist error');
      });

    let ex;
    let p;

    try {
      p = rep.schedulePersist();
      expect(rep.persistIsScheduled).to.be.true;
      await Promise.all([tickAFewTimes(20, 100), p]);
    } catch (e) {
      ex = e;
    }
    expect(ex).instanceOf(Error).property('message', 'persist error');
    expect(persistStub.callCount).to.equal(1);
    expect(rep.persistIsScheduled).to.be.false;

    // ensure that persist can be scheduled again
    ex = undefined;
    try {
      p = rep.schedulePersist();
      expect(rep.persistIsScheduled).to.be.true;
      await Promise.all([tickAFewTimes(20, 100), p]);
    } catch (e) {
      ex = e;
    }
    expect(ex).instanceOf(Error).property('message', 'persist error');
    expect(persistStub.callCount).to.equal(2);
    expect(rep.persistIsScheduled).to.be.false;
  });
});

async function deleteBranchForTesting<
  // eslint-disable-next-line @typescript-eslint/ban-types
  MD extends MutatorDefs = {},
>(rep: ReplicacheTest<MD>) {
  const branchID = await rep.branchID;
  assert(branchID);
  await rep.perdag.withWrite(async tx => {
    await deleteBranch(branchID, tx);
    await tx.commit();
  });
}
