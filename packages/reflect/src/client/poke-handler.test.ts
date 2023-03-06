import {expect} from '@esm-bundle/chai';
import {LogContext} from '@rocicorp/logger';
import * as sinon from 'sinon';
import {PokeHandler} from './poke-handler.js';

let clock: sinon.SinonFakeTimers;
let rafStub: sinon.SinonStub;

setup(() => {
  clock = sinon.useFakeTimers();
  rafStub = sinon.stub(window, 'requestAnimationFrame');
});

teardown(() => {
  sinon.restore();
});

test('playback all pokes dont have timestamps, all merge and play on first raf', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    Promise.resolve('c1'),
    Promise.resolve(new LogContext('error')),
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

test('playback all pokes have timestamps but are for this client so merge and play on first raf', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    Promise.resolve('c1'),
    Promise.resolve(new LogContext('error')),
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
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
        lastMutationIDChanges: {c1: 3},
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
        lastMutationIDChanges: {c1: 4},
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

  expect(lastMutationIDChangeForSelf).to.equal(4);

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
        c1: 4,
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

test('playback all pokes have timestamps without any merges', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    Promise.resolve('c1'),
    Promise.resolve(new LogContext('error')),
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
        timestamp: 100,
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
        timestamp: 120,
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
        timestamp: 140,
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await clock.tickAsync(250);
  expect(replicachePokeStub.callCount).to.equal(0);
  await rafCallback1();

  expect(replicachePokeStub.callCount).to.equal(1);
  const replicachePoke0 = replicachePokeStub.getCall(0).args[0];
  expect(replicachePoke0).to.deep.equal({
    baseCookie: 1,
    pullResponse: {
      cookie: 2,
      lastMutationIDChanges: {
        c2: 2,
      },
      patch: [
        {
          key: 'count',
          op: 'put',
          value: 1,
        },
      ],
    },
  });
  expect(rafStub.callCount).to.equal(3);

  const rafCallback2 = rafStub.getCall(2).args[0];
  await clock.tickAsync(20);
  expect(replicachePokeStub.callCount).to.equal(1);
  await rafCallback2();

  expect(replicachePokeStub.callCount).to.equal(2);
  const replicachePoke1 = replicachePokeStub.getCall(1).args[0];
  expect(replicachePoke1).to.deep.equal({
    baseCookie: 2,
    pullResponse: {
      cookie: 3,
      lastMutationIDChanges: {
        c2: 3,
      },
      patch: [
        {
          key: 'count',
          op: 'put',
          value: 2,
        },
      ],
    },
  });
  expect(rafStub.callCount).to.equal(4);

  const rafCallback3 = rafStub.getCall(3).args[0];
  await clock.tickAsync(20);
  expect(replicachePokeStub.callCount).to.equal(2);
  await rafCallback3();

  expect(replicachePokeStub.callCount).to.equal(3);
  const replicachePoke2 = replicachePokeStub.getCall(2).args[0];
  expect(replicachePoke2).to.deep.equal({
    baseCookie: 3,
    pullResponse: {
      cookie: 4,
      lastMutationIDChanges: {
        c3: 2,
      },
      patch: [
        {
          key: 'count',
          op: 'put',
          value: 3,
        },
      ],
    },
  });
  expect(rafStub.callCount).to.equal(5);

  const rafCallback4 = rafStub.getCall(4).args[0];
  await rafCallback4();
  expect(replicachePokeStub.callCount).to.equal(3);
  expect(rafStub.callCount).to.equal(5);
});

test('playback all pokes have timestamps, two pokes merge due to timing', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    Promise.resolve('c1'),
    Promise.resolve(new LogContext('error')),
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
        timestamp: 100,
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
        timestamp: 120,
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
        timestamp: 140,
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await clock.tickAsync(250);
  expect(replicachePokeStub.callCount).to.equal(0);
  await rafCallback1();

  expect(replicachePokeStub.callCount).to.equal(1);
  const replicachePoke0 = replicachePokeStub.getCall(0).args[0];
  expect(replicachePoke0).to.deep.equal({
    baseCookie: 1,
    pullResponse: {
      cookie: 2,
      lastMutationIDChanges: {
        c2: 2,
      },
      patch: [
        {
          key: 'count',
          op: 'put',
          value: 1,
        },
      ],
    },
  });
  expect(rafStub.callCount).to.equal(3);

  const rafCallback2 = rafStub.getCall(2).args[0];
  await clock.tickAsync(40);
  expect(replicachePokeStub.callCount).to.equal(1);
  await rafCallback2();

  expect(replicachePokeStub.callCount).to.equal(2);
  const replicachePoke1 = replicachePokeStub.getCall(1).args[0];
  expect(replicachePoke1).to.deep.equal({
    baseCookie: 2,
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
  expect(rafStub.callCount).to.equal(4);

  const rafCallback3 = rafStub.getCall(3).args[0];
  await rafCallback3();
  expect(replicachePokeStub.callCount).to.equal(2);
  expect(rafStub.callCount).to.equal(4);
});

test('playback pokes with no timestamp or for this client playback ASAP', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    Promise.resolve('c1'),
    Promise.resolve(new LogContext('error')),
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
        timestamp: 100,
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
        lastMutationIDChanges: {c1: 2},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
        timestamp: 140,
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(2);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await clock.tickAsync(250);
  expect(replicachePokeStub.callCount).to.equal(0);
  await rafCallback1();

  expect(replicachePokeStub.callCount).to.equal(1);
  const replicachePoke0 = replicachePokeStub.getCall(0).args[0];
  expect(replicachePoke0).to.deep.equal({
    baseCookie: 1,
    pullResponse: {
      cookie: 4,
      lastMutationIDChanges: {
        c2: 3,
        c1: 2,
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
  expect(rafStub.callCount).to.equal(3);

  const rafCallback2 = rafStub.getCall(2).args[0];
  await rafCallback2();
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(rafStub.callCount).to.equal(3);
});

test('playback sequence of poke messages', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    Promise.resolve('c1'),
    Promise.resolve(new LogContext('error')),
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
        timestamp: 100,
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
        timestamp: 120,
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await clock.tickAsync(250);
  expect(replicachePokeStub.callCount).to.equal(0);
  await rafCallback1();

  expect(replicachePokeStub.callCount).to.equal(1);
  const replicachePoke0 = replicachePokeStub.getCall(0).args[0];
  expect(replicachePoke0).to.deep.equal({
    baseCookie: 1,
    pullResponse: {
      cookie: 2,
      lastMutationIDChanges: {
        c2: 2,
      },
      patch: [
        {
          key: 'count',
          op: 'put',
          value: 1,
        },
      ],
    },
  });
  expect(rafStub.callCount).to.equal(3);

  const lastMutationIDChangeForSelf2 = await pokeHandler.handlePoke({
    pokes: [
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
        timestamp: 140,
      },
      {
        baseCookie: 4,
        cookie: 5,
        lastMutationIDChanges: {c3: 3},
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 4,
          },
        ],
        timestamp: 160,
      },
    ],
    requestID: 'requestID2',
  });
  expect(lastMutationIDChangeForSelf2).to.be.undefined;

  const rafCallback2 = rafStub.getCall(2).args[0];
  await clock.tickAsync(40);
  expect(replicachePokeStub.callCount).to.equal(1);
  await rafCallback2();

  expect(replicachePokeStub.callCount).to.equal(2);
  const replicachePoke1 = replicachePokeStub.getCall(1).args[0];
  expect(replicachePoke1).to.deep.equal({
    baseCookie: 2,
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
  expect(rafStub.callCount).to.equal(4);

  const rafCallback3 = rafStub.getCall(3).args[0];
  await clock.tickAsync(20);
  expect(replicachePokeStub.callCount).to.equal(2);
  await rafCallback3();

  expect(replicachePokeStub.callCount).to.equal(3);
  const replicachePoke2 = replicachePokeStub.getCall(2).args[0];
  expect(replicachePoke2).to.deep.equal({
    baseCookie: 4,
    pullResponse: {
      cookie: 5,
      lastMutationIDChanges: {
        c3: 3,
      },
      patch: [
        {
          key: 'count',
          op: 'put',
          value: 4,
        },
      ],
    },
  });
  expect(rafStub.callCount).to.equal(5);

  const rafCallback4 = rafStub.getCall(4).args[0];
  await rafCallback4();
  expect(replicachePokeStub.callCount).to.equal(3);
  expect(rafStub.callCount).to.equal(5);
});

test('onOutOfOrderPoke is called if replicache poke throws an unexpected base cookie error', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    Promise.resolve('c1'),
    Promise.resolve(new LogContext('error')),
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

test('onDisconnect clears pending pokes and playback offset', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    Promise.resolve('c1'),
    Promise.resolve(new LogContext('error')),
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
        timestamp: 100,
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
        timestamp: 120,
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
        timestamp: 140,
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await clock.tickAsync(250);
  expect(replicachePokeStub.callCount).to.equal(0);
  await rafCallback1();

  expect(replicachePokeStub.callCount).to.equal(1);
  const replicachePoke0 = replicachePokeStub.getCall(0).args[0];
  expect(replicachePoke0).to.deep.equal({
    baseCookie: 1,
    pullResponse: {
      cookie: 2,
      lastMutationIDChanges: {
        c2: 2,
      },
      patch: [
        {
          key: 'count',
          op: 'put',
          value: 1,
        },
      ],
    },
  });
  expect(rafStub.callCount).to.equal(3);

  await pokeHandler.handleDisconnect();

  const rafCallback2 = rafStub.getCall(2).args[0];
  await clock.tickAsync(40);
  expect(replicachePokeStub.callCount).to.equal(1);
  await rafCallback2();

  // raf not called again because buffer is empty
  expect(rafStub.callCount).to.equal(3);

  const lastMutationIDChangeForSelf2 = await pokeHandler.handlePoke({
    pokes: [
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
        timestamp: 220,
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
        timestamp: 240,
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf2).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(1);
  expect(rafStub.callCount).to.equal(4);

  const rafCallback3 = rafStub.getCall(3).args[0];
  expect(replicachePokeStub.callCount).to.equal(1);
  await rafCallback3();
  // not called not enough time has elapsed
  expect(replicachePokeStub.callCount).to.equal(1);

  expect(rafStub.callCount).to.equal(5);
  const rafCallback4 = rafStub.getCall(3).args[0];
  await clock.tickAsync(250 + 20);
  expect(replicachePokeStub.callCount).to.equal(1);
  await rafCallback4();
  expect(replicachePokeStub.callCount).to.equal(2);

  const replicachePoke1 = replicachePokeStub.getCall(1).args[0];
  expect(replicachePoke1).to.deep.equal({
    baseCookie: 2,
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
  expect(rafStub.callCount).to.equal(6);

  const rafCallback5 = rafStub.getCall(4).args[0];
  await rafCallback5();
  expect(replicachePokeStub.callCount).to.equal(2);
  expect(rafStub.callCount).to.equal(6);
});

test('handlePoke returns the last mutation id change for this client from poke message or undefined if none', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    outOfOrderPokeStub,
    Promise.resolve('c1'),
    Promise.resolve(new LogContext('error')),
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
