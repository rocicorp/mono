import {Context, LogContext, LogLevel} from '@rocicorp/logger';
import {BufferSizer} from 'shared/src/buffer-sizer.js';
import * as sinon from 'sinon';
import {afterEach, beforeEach, expect, test} from 'vitest';
import {
  PokeHandler,
  RESET_PLAYBACK_OFFSET_THRESHOLD_MS,
} from './poke-handler.js';
import {PresenceManager} from './presence-manager.js';

let clock: sinon.SinonFakeTimers;
let rafStub: sinon.SinonStub;
const startTime = 1678829450000;

// These independent test options allow us to adjust
// the constants used by PokeHandler without have to adjust
// all these tests.
const TEST_BUFFER_SIZER_OPTIONS = {
  initialBufferSizeMs: 250,
  maxBufferSizeMs: 1000,
  minBufferSizeMs: -1000,
  adjustBufferSizeIntervalMs: 10_000,
} as const;

beforeEach(() => {
  clock = sinon.useFakeTimers();
  clock.setSystemTime(startTime);
  rafStub = sinon.stub(globalThis, 'requestAnimationFrame');
});

afterEach(() => {
  sinon.restore();
});

test('playback all pokes dont have timestamps, all merge and play on first raf', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const presenceManager = new PresenceManager(clientID, logContext);
  const updatePresenceSpy = sinon.spy(presenceManager, 'updatePresence');
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    presenceManager,
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
        presence: [
          {
            op: 'put',
            key: 'c1',
            value: 1,
          },
        ],
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
        presence: [],
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
        presence: [
          {
            op: 'put',
            key: 'c3',
            value: 1,
          },
        ],
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
  expect(updatePresenceSpy.callCount).to.equal(0);
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

  expect(updatePresenceSpy.callCount).to.equal(1);
  const updatePresence0 = updatePresenceSpy.getCall(0).args[0];
  expect(updatePresence0).to.deep.equal([
    {
      op: 'put',
      key: 'c1',
      value: 1,
    },
    {
      op: 'put',
      key: 'c3',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await rafCallback1();
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(updatePresenceSpy.callCount).to.equal(1);
  expect(rafStub.callCount).to.equal(2);
});

test('playback all pokes have timestamps but are for this client so merge and play on first raf', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const presenceManager = new PresenceManager(clientID, logContext);
  const updatePresenceSpy = sinon.spy(presenceManager, 'updatePresence');
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    presenceManager,
    outOfOrderPokeStub,
    clientID,
    logContext,
    new BufferSizer(TEST_BUFFER_SIZER_OPTIONS),
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c1: 2},
        presence: [
          {
            op: 'put',
            key: 'c1',
            value: 1,
          },
        ],
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
        presence: [],
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
        presence: [
          {
            op: 'put',
            key: 'c3',
            value: 1,
          },
        ],
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
  expect(updatePresenceSpy.callCount).to.equal(0);
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

  expect(updatePresenceSpy.callCount).to.equal(1);
  const updatePresence0 = updatePresenceSpy.getCall(0).args[0];
  expect(updatePresence0).to.deep.equal([
    {
      op: 'put',
      key: 'c1',
      value: 1,
    },
    {
      op: 'put',
      key: 'c3',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await rafCallback1();
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(updatePresenceSpy.callCount).to.equal(1);
  expect(rafStub.callCount).to.equal(2);
});

test('playback all pokes have timestamps without any merges', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const presenceManager = new PresenceManager(clientID, logContext);
  const updatePresenceSpy = sinon.spy(presenceManager, 'updatePresence');
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    presenceManager,
    outOfOrderPokeStub,
    clientID,
    logContext,
    new BufferSizer(TEST_BUFFER_SIZER_OPTIONS),
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        presence: [
          {
            op: 'put',
            key: 'c1',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
        timestamp: startTime + 100,
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
        timestamp: startTime + 120,
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c3: 2},
        presence: [
          {
            op: 'put',
            key: 'c3',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
        timestamp: startTime + 140,
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await clock.tickAsync(250);
  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
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

  expect(updatePresenceSpy.callCount).to.equal(1);
  const updatePresence0 = updatePresenceSpy.getCall(0).args[0];
  expect(updatePresence0).to.deep.equal([
    {
      op: 'put',
      key: 'c1',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(3);

  const rafCallback2 = rafStub.getCall(2).args[0];
  await clock.tickAsync(20);
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(updatePresenceSpy.callCount).to.equal(1);
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

  expect(updatePresenceSpy.callCount).to.equal(2);
  const updatePresence1 = updatePresenceSpy.getCall(1).args[0];
  expect(updatePresence1).to.deep.equal([]);

  expect(rafStub.callCount).to.equal(4);

  const rafCallback3 = rafStub.getCall(3).args[0];
  await clock.tickAsync(20);
  expect(replicachePokeStub.callCount).to.equal(2);
  expect(updatePresenceSpy.callCount).to.equal(2);
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

  expect(updatePresenceSpy.callCount).to.equal(3);
  const updatePresence2 = updatePresenceSpy.getCall(2).args[0];
  expect(updatePresence2).to.deep.equal([
    {
      op: 'put',
      key: 'c3',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(5);

  const rafCallback4 = rafStub.getCall(4).args[0];
  await rafCallback4();
  expect(replicachePokeStub.callCount).to.equal(3);
  expect(updatePresenceSpy.callCount).to.equal(3);
  expect(rafStub.callCount).to.equal(5);
});

test('playback all pokes have timestamps, two pokes merge due to timing', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const presenceManager = new PresenceManager(clientID, logContext);
  const updatePresenceSpy = sinon.spy(presenceManager, 'updatePresence');
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    presenceManager,
    outOfOrderPokeStub,
    clientID,
    logContext,
    new BufferSizer(TEST_BUFFER_SIZER_OPTIONS),
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        presence: [
          {
            op: 'put',
            key: 'c1',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
        timestamp: startTime + 100,
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
        timestamp: startTime + 120,
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c3: 2},
        presence: [
          {
            op: 'put',
            key: 'c3',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
        timestamp: startTime + 140,
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await clock.tickAsync(250);
  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
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

  expect(updatePresenceSpy.callCount).to.equal(1);
  const updatePresence0 = updatePresenceSpy.getCall(0).args[0];
  expect(updatePresence0).to.deep.equal([
    {
      op: 'put',
      key: 'c1',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(3);

  const rafCallback2 = rafStub.getCall(2).args[0];
  await clock.tickAsync(40);
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(updatePresenceSpy.callCount).to.equal(1);
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

  expect(updatePresenceSpy.callCount).to.equal(2);
  const updatePresence1 = updatePresenceSpy.getCall(1).args[0];
  expect(updatePresence1).to.deep.equal([
    {
      op: 'put',
      key: 'c3',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(4);

  const rafCallback3 = rafStub.getCall(3).args[0];
  await rafCallback3();
  expect(replicachePokeStub.callCount).to.equal(2);
  expect(updatePresenceSpy.callCount).to.equal(2);
  expect(rafStub.callCount).to.equal(4);
});

test('playback pokes with no timestamp or for this client playback ASAP', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const presenceManager = new PresenceManager(clientID, logContext);
  const updatePresenceSpy = sinon.spy(presenceManager, 'updatePresence');
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    presenceManager,
    outOfOrderPokeStub,
    clientID,
    logContext,
    new BufferSizer(TEST_BUFFER_SIZER_OPTIONS),
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        presence: [
          {
            op: 'put',
            key: 'c1',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
        timestamp: startTime + 100,
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
        timestamp: undefined,
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c1: 2},
        presence: [
          {
            op: 'put',
            key: 'c3',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
        timestamp: startTime + 140,
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(2);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await clock.tickAsync(250);
  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
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

  expect(updatePresenceSpy.callCount).to.equal(1);
  const updatePresence0 = updatePresenceSpy.getCall(0).args[0];
  expect(updatePresence0).to.deep.equal([
    {
      op: 'put',
      key: 'c1',
      value: 1,
    },
    {
      op: 'put',
      key: 'c3',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(3);

  const rafCallback2 = rafStub.getCall(2).args[0];
  await rafCallback2();
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(updatePresenceSpy.callCount).to.equal(1);
  expect(rafStub.callCount).to.equal(3);
});

test('playback sequence of poke messages', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const presenceManager = new PresenceManager(clientID, logContext);
  const updatePresenceSpy = sinon.spy(presenceManager, 'updatePresence');
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    presenceManager,
    outOfOrderPokeStub,
    clientID,
    logContext,
    new BufferSizer(TEST_BUFFER_SIZER_OPTIONS),
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        presence: [
          {
            op: 'put',
            key: 'c1',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
        timestamp: startTime + 100,
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        presence: [
          {
            op: 'put',
            key: 'c2',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
        timestamp: startTime + 120,
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
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

  expect(updatePresenceSpy.callCount).to.equal(1);
  const updatePresence0 = updatePresenceSpy.getCall(0).args[0];
  expect(updatePresence0).to.deep.equal([
    {
      op: 'put',
      key: 'c1',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(3);

  const lastMutationIDChangeForSelf2 = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c3: 2},
        presence: [
          {
            op: 'put',
            key: 'c3',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
        timestamp: startTime + 140,
      },
      {
        baseCookie: 4,
        cookie: 5,
        lastMutationIDChanges: {c3: 3},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 4,
          },
        ],
        timestamp: startTime + 160,
      },
    ],
    requestID: 'requestID2',
  });
  expect(lastMutationIDChangeForSelf2).to.be.undefined;

  const rafCallback2 = rafStub.getCall(2).args[0];
  await clock.tickAsync(40);
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(updatePresenceSpy.callCount).to.equal(1);
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

  expect(updatePresenceSpy.callCount).to.equal(2);
  const updatePresence1 = updatePresenceSpy.getCall(1).args[0];
  expect(updatePresence1).to.deep.equal([
    {
      op: 'put',
      key: 'c2',
      value: 1,
    },
    {
      op: 'put',
      key: 'c3',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(4);

  const rafCallback3 = rafStub.getCall(3).args[0];
  await clock.tickAsync(20);
  expect(replicachePokeStub.callCount).to.equal(2);
  expect(updatePresenceSpy.callCount).to.equal(2);
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

  expect(updatePresenceSpy.callCount).to.equal(3);
  const updatePresence2 = updatePresenceSpy.getCall(2).args[0];
  expect(updatePresence2).to.deep.equal([]);

  expect(rafStub.callCount).to.equal(5);

  const rafCallback4 = rafStub.getCall(4).args[0];
  await rafCallback4();
  expect(replicachePokeStub.callCount).to.equal(3);
  expect(rafStub.callCount).to.equal(5);
});

test(`playback offset is reset for new pokes if timestamp offset delta is > ${RESET_PLAYBACK_OFFSET_THRESHOLD_MS}`, async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const presenceManager = new PresenceManager(clientID, logContext);
  const updatePresenceSpy = sinon.spy(presenceManager, 'updatePresence');
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    presenceManager,
    outOfOrderPokeStub,
    clientID,
    logContext,
    new BufferSizer(TEST_BUFFER_SIZER_OPTIONS),
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        presence: [
          {
            op: 'put',
            key: 'c1',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
        timestamp: startTime + 100,
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        presence: [
          {
            op: 'put',
            key: 'c2',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
        timestamp: startTime + 120,
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await clock.tickAsync(250);
  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
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

  expect(updatePresenceSpy.callCount).to.equal(1);
  const updatePresence0 = updatePresenceSpy.getCall(0).args[0];
  expect(updatePresence0).to.deep.equal([
    {
      op: 'put',
      key: 'c1',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(3);

  const lastMutationIDChangeForSelf2 = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c3: 2},
        presence: [
          {
            op: 'put',
            key: 'c3',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
        timestamp:
          startTime + 100 + 250 - (RESET_PLAYBACK_OFFSET_THRESHOLD_MS + 1),
      },
      {
        baseCookie: 4,
        cookie: 5,
        lastMutationIDChanges: {c3: 3},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 4,
          },
        ],
        timestamp:
          startTime + 100 + 250 - (RESET_PLAYBACK_OFFSET_THRESHOLD_MS + 1) + 60,
      },
    ],
    requestID: 'requestID2',
  });
  expect(lastMutationIDChangeForSelf2).to.be.undefined;

  const rafCallback2 = rafStub.getCall(2).args[0];
  await clock.tickAsync(20);
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(updatePresenceSpy.callCount).to.equal(1);
  await rafCallback2();

  expect(replicachePokeStub.callCount).to.equal(2);
  const replicachePoke1 = replicachePokeStub.getCall(1).args[0];
  // old poke plays back based on old offset
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

  expect(updatePresenceSpy.callCount).to.equal(2);
  const updatePresence1 = updatePresenceSpy.getCall(1).args[0];
  expect(updatePresence1).to.deep.equal([
    {
      op: 'put',
      key: 'c2',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(4);

  const rafCallback3 = rafStub.getCall(3).args[0];
  await clock.tickAsync(230);
  expect(replicachePokeStub.callCount).to.equal(2);
  expect(updatePresenceSpy.callCount).to.equal(2);
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

  expect(updatePresenceSpy.callCount).to.equal(3);
  const updatePresence2 = updatePresenceSpy.getCall(2).args[0];
  expect(updatePresence2).to.deep.equal([
    {
      op: 'put',
      key: 'c3',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(5);

  const rafCallback4 = rafStub.getCall(4).args[0];
  await clock.tickAsync(60);
  expect(replicachePokeStub.callCount).to.equal(3);
  expect(updatePresenceSpy.callCount).to.equal(3);
  await rafCallback4();

  expect(replicachePokeStub.callCount).to.equal(4);
  const replicachePoke3 = replicachePokeStub.getCall(3).args[0];
  expect(replicachePoke3).to.deep.equal({
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

  expect(updatePresenceSpy.callCount).to.equal(4);
  const updatePresence3 = updatePresenceSpy.getCall(3).args[0];
  expect(updatePresence3).to.deep.equal([]);

  expect(rafStub.callCount).to.equal(6);

  const rafCallback5 = rafStub.getCall(5).args[0];
  await rafCallback5();
  expect(replicachePokeStub.callCount).to.equal(4);
  expect(updatePresenceSpy.callCount).to.equal(4);
  expect(rafStub.callCount).to.equal(6);
});

test('playback stats', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const log: [LogLevel, Context | undefined, unknown[]][] = [];
  const logContext = new LogContext('debug', undefined, {
    log: (
      level: LogLevel,
      context: Context | undefined,
      ...args: unknown[]
    ) => {
      log.push([level, context, args]);
    },
  });
  const expectStats = (
    expectedStats: {
      timedPokeCount: number;
      missedTimedPokeCount: number;
      timedFrameCount: number;
      missedTimedFrameCount: number;
      avgLatency: number;
      recentLatencies: number[];
    },
    rafAtTime: number,
  ) => {
    expect(
      log.find(
        logCall =>
          logCall[0] === 'debug' &&
          logCall[2].find(
            logCallArg =>
              logCallArg ===
              'playback stats (misses / total = percent missed):',
          ),
      ),
    ).to.deep.equal([
      'debug',
      // eslint-disable-next-line @typescript-eslint/naming-convention
      {PokeHandler: undefined, rafAt: rafAtTime},
      [
        'playback stats (misses / total = percent missed):',
        '\npokes:',
        expectedStats.missedTimedPokeCount,
        '/',
        expectedStats.timedPokeCount,
        '=',
        expectedStats.missedTimedPokeCount / expectedStats.timedPokeCount,
        '\nframes:',
        expectedStats.missedTimedFrameCount,
        '/',
        expectedStats.timedFrameCount,
        '=',
        expectedStats.missedTimedFrameCount / expectedStats.timedFrameCount,
        '\navg poke latency:',
        expectedStats.avgLatency,
        '\nrecent poke latencies:',
        expectedStats.recentLatencies,
      ],
    ]);
  };
  const clientID = 'c1';
  const presenceManager = new PresenceManager(clientID, logContext);
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    presenceManager,
    outOfOrderPokeStub,
    clientID,
    logContext,
    new BufferSizer(TEST_BUFFER_SIZER_OPTIONS),
    3 /* maxRecentPokeLatenciesSize */,
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 1},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
        timestamp: undefined,
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c1: 2},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
        timestamp: startTime + 110,
        debugOriginTimestamp: startTime + 10,
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c2: 2},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
        timestamp: startTime + 120,
        debugOriginTimestamp: startTime + 20,
      },
      {
        baseCookie: 4,
        cookie: 5,
        lastMutationIDChanges: {c2: 3},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 4,
          },
        ],
        timestamp: startTime + 130,
        debugOriginTimestamp: startTime + 30,
      },
      {
        baseCookie: 5,
        cookie: 6,
        lastMutationIDChanges: {c2: 4},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 5,
          },
        ],
        timestamp: startTime + 150,
        debugOriginTimestamp: startTime + 100,
      },
      {
        baseCookie: 6,
        cookie: 7,
        lastMutationIDChanges: {c2: 5},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 6,
          },
        ],
        timestamp: startTime + 170,
        debugOriginTimestamp: startTime + 120,
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(2);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(1);
  const replicachePoke0 = replicachePokeStub.getCall(0).args[0];
  expect(replicachePoke0).to.deep.equal({
    baseCookie: 1,
    pullResponse: {
      cookie: 3,
      lastMutationIDChanges: {
        c2: 1,
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
      ],
    },
  });
  expect(rafStub.callCount).to.equal(2);

  // pokes that are not timed (no timestamp or for this client) and frames with
  // no timed pokes are not counted as they would inflate our stats
  expectStats(
    {
      missedTimedPokeCount: 0,
      timedPokeCount: 0,
      missedTimedFrameCount: 0,
      timedFrameCount: 0,
      avgLatency: NaN,
      recentLatencies: [],
    },
    0,
  );
  log.length = 0;

  const rafCallback1 = rafStub.getCall(1).args[0];
  await clock.tickAsync(270);
  expect(replicachePokeStub.callCount).to.equal(1);
  await rafCallback1();

  expect(replicachePokeStub.callCount).to.equal(2);
  const replicachePoke1 = replicachePokeStub.getCall(1).args[0];
  expect(replicachePoke1).to.deep.equal({
    baseCookie: 3,
    pullResponse: {
      cookie: 5,
      lastMutationIDChanges: {
        c2: 3,
      },
      patch: [
        {
          key: 'count',
          op: 'put',
          value: 3,
        },
        {
          key: 'count',
          op: 'put',
          value: 4,
        },
      ],
    },
  });
  expect(rafStub.callCount).to.equal(3);
  expectStats(
    {
      missedTimedPokeCount: 0,
      timedPokeCount: 2,
      missedTimedFrameCount: 0,
      timedFrameCount: 1,
      avgLatency: 245,
      recentLatencies: [240, 250],
    },
    270,
  );
  log.length = 0;

  const rafCallback2 = rafStub.getCall(2).args[0];
  await clock.tickAsync(40);
  expect(replicachePokeStub.callCount).to.equal(2);
  await rafCallback2();

  expect(replicachePokeStub.callCount).to.equal(3);
  const replicachePoke2 = replicachePokeStub.getCall(2).args[0];
  expect(replicachePoke2).to.deep.equal({
    baseCookie: 5,
    pullResponse: {
      cookie: 7,
      lastMutationIDChanges: {
        c2: 5,
      },
      patch: [
        {
          key: 'count',
          op: 'put',
          value: 5,
        },
        {
          key: 'count',
          op: 'put',
          value: 6,
        },
      ],
    },
  });
  expect(rafStub.callCount).to.equal(4);
  expectStats(
    {
      missedTimedPokeCount: 1,
      timedPokeCount: 4,
      missedTimedFrameCount: 1,
      timedFrameCount: 2,
      avgLatency: 222.5,
      // 250 is removed since constructed with maxRecentPokeLatenciesSize
      // of 3
      recentLatencies: [190, 210, 240],
    },
    310,
  );
  log.length = 0;
  const rafCallback3 = rafStub.getCall(3).args[0];
  await rafCallback3();
  expect(replicachePokeStub.callCount).to.equal(3);
  expect(rafStub.callCount).to.equal(4);
});

test('onOutOfOrderPoke is called if replicache poke throws an unexpected base cookie error', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const presenceManager = new PresenceManager(clientID, logContext);
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    presenceManager,
    outOfOrderPokeStub,
    clientID,
    logContext,
    new BufferSizer(TEST_BUFFER_SIZER_OPTIONS),
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        presence: [],
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
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const presenceManager = new PresenceManager(clientID, logContext);
  const updatePresenceSpy = sinon.spy(presenceManager, 'updatePresence');
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    presenceManager,
    outOfOrderPokeStub,
    clientID,
    logContext,
    new BufferSizer(TEST_BUFFER_SIZER_OPTIONS),
  );
  expect(rafStub.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        presence: [
          {
            op: 'put',
            key: 'c1',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
        timestamp: startTime + 100,
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
        timestamp: startTime + 120,
      },
      {
        baseCookie: 3,
        cookie: 4,
        lastMutationIDChanges: {c3: 2},
        presence: [
          {
            op: 'put',
            key: 'c2',
            value: 1,
          },
        ],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
        timestamp: startTime + 140,
      },
    ],
    requestID: 'requestID1',
  });

  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await clock.tickAsync(250);
  expect(replicachePokeStub.callCount).to.equal(0);
  expect(updatePresenceSpy.callCount).to.equal(0);
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

  expect(updatePresenceSpy.callCount).to.equal(1);
  const updatePresence0 = updatePresenceSpy.getCall(0).args[0];
  expect(updatePresence0).to.deep.equal([
    {
      op: 'put',
      key: 'c1',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(3);

  await pokeHandler.handleDisconnect();

  const rafCallback2 = rafStub.getCall(2).args[0];
  await clock.tickAsync(40);
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(updatePresenceSpy.callCount).to.equal(1);
  await rafCallback2();

  // raf not called again because buffer is empty
  expect(rafStub.callCount).to.equal(3);

  const lastMutationIDChangeForSelf2 = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        presence: [
          {
            op: 'put',
            key: 'c3',
            value: 1,
          },
        ],
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
        presence: [],
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
  expect(updatePresenceSpy.callCount).to.equal(1);
  expect(rafStub.callCount).to.equal(4);

  const rafCallback3 = rafStub.getCall(3).args[0];
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(updatePresenceSpy.callCount).to.equal(1);
  await rafCallback3();
  // not called not enough time has elapsed
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(updatePresenceSpy.callCount).to.equal(1);

  expect(rafStub.callCount).to.equal(5);
  const rafCallback4 = rafStub.getCall(3).args[0];
  await clock.tickAsync(250 + 20);
  expect(replicachePokeStub.callCount).to.equal(1);
  expect(updatePresenceSpy.callCount).to.equal(1);
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

  expect(updatePresenceSpy.callCount).to.equal(2);
  const updatePresence1 = updatePresenceSpy.getCall(1).args[0];
  expect(updatePresence1).to.deep.equal([
    {
      op: 'put',
      key: 'c3',
      value: 1,
    },
  ]);

  expect(rafStub.callCount).to.equal(6);

  const rafCallback5 = rafStub.getCall(4).args[0];
  await rafCallback5();
  expect(replicachePokeStub.callCount).to.equal(2);
  expect(updatePresenceSpy.callCount).to.equal(2);
  expect(rafStub.callCount).to.equal(6);
});

test('handlePoke returns the last mutation id change for this client from poke message or undefined if none', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const presenceManager = new PresenceManager(clientID, logContext);
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    presenceManager,
    outOfOrderPokeStub,
    clientID,
    logContext,
    new BufferSizer(TEST_BUFFER_SIZER_OPTIONS),
  );
  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        presence: [],
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
        presence: [],
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
        presence: [],
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
        presence: [],
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
        presence: [],
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
        presence: [],
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
        presence: [],
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

test('integration with BufferSizer', async () => {
  const outOfOrderPokeStub = sinon.stub();
  const replicachePokeStub = sinon.stub();
  const bufferSizer = new BufferSizer(TEST_BUFFER_SIZER_OPTIONS);
  const recordMissableSpy = sinon.spy(bufferSizer, 'recordMissable');
  const bufferSizeMsStub = sinon.stub(bufferSizer, 'bufferSizeMs');
  const clientID = 'c1';
  const logContext = new LogContext('error');
  const presenceManager = new PresenceManager(clientID, logContext);
  const pokeHandler = new PokeHandler(
    replicachePokeStub,
    presenceManager,
    outOfOrderPokeStub,
    clientID,
    logContext,
    bufferSizer,
  );
  expect(rafStub.callCount).to.equal(0);
  expect(recordMissableSpy.callCount).to.equal(0);

  const lastMutationIDChangeForSelf = await pokeHandler.handlePoke({
    pokes: [
      {
        baseCookie: 1,
        cookie: 2,
        lastMutationIDChanges: {c2: 2},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 1,
          },
        ],
        timestamp: startTime + 100,
      },
      {
        baseCookie: 2,
        cookie: 3,
        lastMutationIDChanges: {c2: 3},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 2,
          },
        ],
        timestamp: startTime + 120,
      },
    ],
    requestID: 'requestID1',
  });
  expect(lastMutationIDChangeForSelf).to.equal(undefined);

  expect(recordMissableSpy.callCount).to.equal(0);
  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(1);

  const rafCallback0 = rafStub.getCall(0).args[0];
  await rafCallback0();

  expect(recordMissableSpy.callCount).to.equal(0);
  expect(replicachePokeStub.callCount).to.equal(0);
  expect(rafStub.callCount).to.equal(2);

  const rafCallback1 = rafStub.getCall(1).args[0];
  await clock.tickAsync(250);
  expect(replicachePokeStub.callCount).to.equal(0);
  await rafCallback1();

  expect(recordMissableSpy.callCount).to.equal(1);
  const recordMissable0 = recordMissableSpy.getCall(0);
  expect(recordMissable0.args.slice(0, 3)).to.deep.equal([
    startTime + 250,
    false,
    18,
  ]);

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
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 3,
          },
        ],
        timestamp: startTime + 140,
      },
      {
        baseCookie: 4,
        cookie: 5,
        lastMutationIDChanges: {c3: 3},
        presence: [],
        patch: [
          {
            op: 'put',
            key: 'count',
            value: 4,
          },
        ],
        timestamp: startTime + 160,
      },
    ],
    requestID: 'requestID2',
  });
  expect(lastMutationIDChangeForSelf2).to.be.undefined;

  expect(recordMissableSpy.callCount).to.equal(1);

  const rafCallback2 = rafStub.getCall(2).args[0];
  await clock.tickAsync(40);
  expect(replicachePokeStub.callCount).to.equal(1);
  await rafCallback2();

  expect(recordMissableSpy.callCount).to.equal(2);
  const recordMissable1 = recordMissableSpy.getCall(1);
  expect(recordMissable1.args.slice(0, 3)).to.deep.equal([
    startTime + 250 + 40,
    true,
    228,
  ]);

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

  bufferSizeMsStub.get(() => 500);

  const rafCallback3 = rafStub.getCall(3).args[0];
  await clock.tickAsync(20);
  expect(replicachePokeStub.callCount).to.equal(2);
  await rafCallback3();

  // not called because of bigger buffer size
  expect(recordMissableSpy.callCount).to.equal(2);
  expect(replicachePokeStub.callCount).to.equal(2);

  expect(rafStub.callCount).to.equal(5);

  const rafCallback4 = rafStub.getCall(4).args[0];
  await clock.tickAsync(250);
  expect(replicachePokeStub.callCount).to.equal(2);
  await rafCallback4();

  expect(recordMissableSpy.callCount).to.equal(3);
  const recordMissable2 = recordMissableSpy.getCall(2);
  expect(recordMissable2.args.slice(0, 3)).to.deep.equal([
    startTime + 250 + 40 + 20 + 250,
    false,
    228,
  ]);

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
  expect(rafStub.callCount).to.equal(6);

  const rafCallback5 = rafStub.getCall(5).args[0];
  await rafCallback5();
  expect(recordMissableSpy.callCount).to.equal(3);
  expect(replicachePokeStub.callCount).to.equal(3);
  expect(rafStub.callCount).to.equal(6);
});
