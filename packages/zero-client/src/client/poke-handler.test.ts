import {LogContext} from '@rocicorp/logger';
import {expect} from 'chai';
import * as sinon from 'sinon';
import {PokeHandler} from './poke-handler.js';

let rafStub: sinon.SinonStub;

setup(() => {
  rafStub = sinon.stub(globalThis, 'requestAnimationFrame');
});

teardown(() => {
  sinon.restore();
});

test('all merge and play on first raf', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    clientID,
    logContext,
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c3: 2},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(1);
  const replicachePoke0 = replicachePokeStub.getCall(0).args[0];
  expect(replicachePoke0).to.deep.equal({
    baseCookie: 1,
    pullResponse: {
      cookie: 4,
      lastMutationIDChanges: {
        c2: 3,
        c3: 2,
      },
      patch: [
        {
          key: 'count',
          op: 'put',
          value: 1,
        },
        {
          key: 'count',
          op: 'put',
          value: 2,
        },
        {
          key: 'count',
          op: 'put',
          value: 3,
        },
      ],
    },
  });

  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await rafCallback1();
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(rafStub.callCount).to.equal(2);
});

test('playback over series of rafs', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    clientID,
    logContext,
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c3: 2},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(1);
  const replicachePoke0 = replicachePokeStub.getCall(0).args[0];
  expect(replicachePoke0).to.deep.equal({
    baseCookie: 1,
    pullResponse: {
      cookie: 4,
      lastMutationIDChanges: {
        c2: 3,
        c3: 2,
      },
      patch: [
        {
          key: 'count',
          op: 'put',
          value: 1,
        },
        {
          key: 'count',
          op: 'put',
          value: 2,
        },
        {
          key: 'count',
          op: 'put',
          value: 3,
        },
      ],
    },
  });

  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await rafCallback1();
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(rafStub.callCount).to.equal(2);

  const lastMutationIDChangeForSelf2 = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 4,
        cookie: 5,
        lastMutationIDChanges: {c2: 4},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 4,
          },
        ],
      },
      {
        baseCookie: 5,
        cookie: 6,
        lastMutationIDChanges: {c3: 3},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 5,
          },
        ],
      },
    ],
    requestID: 'requestID2',
  });

  expect(lastMutationIDChangeForSelf2).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(1);
  expect(rafStub.callCount).to.equal(3);

  const rafCallback2 = rafStub.getCall(2).args[0];
  await rafCallback2();

  expect(replicachePokeStub.callCount).to.equal(2);
  const replicachePoke1 = replicachePokeStub.getCall(1).args[0];
  expect(replicachePoke1).to.deep.equal({
    baseCookie: 4,
    pullResponse: {
      cookie: 6,
      lastMutationIDChanges: {
        c2: 4,
        c3: 3,
      },
      patch: [
        {
          key: 'count',
          op: 'put',
          value: 4,
        },
        {
          key: 'count',
          op: 'put',
          value: 5,
        },
      ],
    },
  });

  expect(rafStub.callCount).to.equal(4);

  const rafCallback3 = rafStub.getCall(3).args[0];
  await rafCallback3();
  expect(replicachePokeStub.callCount).to.equal(2);
  expect(rafStub.callCount).to.equal(4);
});

test('onOutOfOrderPoke is called if Replicache poke throws an unexpected base cookie error', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    clientID,
    logContext,
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
      },
    ],
    requestID: 'test-request-id',
  });

  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];

  replicachePokeStub.throws(new Error('unexpected base cookie for poke'));
  expect(outOfOrderPokeStub.callCount).to.equal(0);
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(1);
});

test('onDisconnect clears pending pokes', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    clientID,
    logContext,
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c3: 2},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  await pokeHandler.handleDisconnect();

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  // Not called because poke buffer was cleared by disconnect
  expect(replicachePokeStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf2 = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c3: 2},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
      },
    ],
    requestID: 'requestID2',
  });

  expect(lastMutationIDChangeForSelf2).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await rafCallback1();

  expect(replicachePokeStub.callCount).to.equal(1);
  const replicachePoke0 = replicachePokeStub.getCall(0).args[0];
  expect(replicachePoke0).to.deep.equal({
    baseCookie: 1,
    pullResponse: {
      cookie: 4,
      lastMutationIDChanges: {
        c2: 3,
        c3: 2,
      },
      patch: [
        {
          key: 'count',
          op: 'put',
          value: 1,
        },
        {
          key: 'count',
          op: 'put',
          value: 2,
        },
        {
          key: 'count',
          op: 'put',
          value: 3,
        },
      ],
    },
  });
});

test('handlePoke returns the last mutation id change for this client from poke message or undefined if none', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    clientID,
    logContext,
  );
  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
      },
    ],
    requestID: 'requestID1',
  });
  expect(lastMutationIDChangeForSelf).to.be.undefined;

  const lastMutationIDChangeForSelf2 = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c1: 2},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
      },
    ],
    requestID: 'requestID1',
  });
  expect(lastMutationIDChangeForSelf2).to.be.equal(2);

  const lastMutationIDChangeForSelf3 = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2, c1: 1},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c2: 4, c1: 3},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
      },
    ],
    requestID: 'requestID1',
  });
  expect(lastMutationIDChangeForSelf3).to.equal(3);
});
