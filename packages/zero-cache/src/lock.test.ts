import {expect} from 'vitest';
import {Lock, RWLock} from './lock.js';
import {afterEach, beforeEach, test, vi} from 'vitest';
import {TestLogSink} from '../../shared/src/logging-test-utils.ts';
import {LogContext} from '@rocicorp/logger';

/**
 * Creates a promise that resolves after [[ms]] milliseconds. Note that if you
 * pass in `0` no `setTimeout` is used and the promise resolves immediately. In
 * other words no macro task is used in that case.
 */
function sleep(ms: number): Promise<void> {
  if (ms === 0) {
    return Promise.resolve();
  }

  return new Promise(resolve => {
    setTimeout(() => resolve(), ms);
  });
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

test('Multiple reads', async () => {
  const lock = new RWLock();

  const log: string[] = [];

  const r1: Promise<number> = (async () => {
    const release = await lock.read();
    log.push('r1');
    release();
    return 1;
  })();

  const r2: Promise<number> = (async () => {
    const release = await lock.read();
    log.push('r2');
    release();
    return 2;
  })();
  const r3: Promise<void> = (async () => {
    const release = await lock.read();
    log.push('r3');
    release();
  })();

  const [v1, v2, v3] = await Promise.all([r1, r2, r3]);
  expect(v1).to.equal(1);
  expect(v2).to.equal(2);
  expect(v3).to.equal(undefined);
  expect(log).to.deep.equal(['r1', 'r2', 'r3']);
});

test('Multiple reads with sleep', async () => {
  const lock = new RWLock();

  const log: string[] = [];

  const r1: Promise<number> = (async () => {
    const release = await lock.read();
    await sleep(6);
    log.push('r1');
    release();
    return 1;
  })();
  const r2: Promise<number> = (async () => {
    const release = await lock.read();
    await sleep(4);
    log.push('r2');
    release();
    return 2;
  })();
  const r3: Promise<void> = (async () => {
    const release = await lock.read();
    await sleep(2);
    log.push('r3');
    release();
  })();

  await vi.runAllTimersAsync();

  const [v1, v2, v3] = await Promise.all([r1, r2, r3]);
  expect(v1).to.equal(1);
  expect(v2).to.equal(2);
  expect(v3).to.equal(undefined);

  expect(log).to.deep.equal(['r3', 'r2', 'r1']);
});

test('Multiple write', async () => {
  const lock = new RWLock();

  const log: string[] = [];

  const w1: Promise<number> = (async () => {
    const release = await lock.write();
    log.push('w1a');
    await sleep(6);
    log.push('w1b');
    release();
    return 1;
  })();
  const w2: Promise<number> = (async () => {
    const release = await lock.write();
    log.push('w2a');
    await sleep(4);
    log.push('w2b');
    release();
    return 2;
  })();
  const w3: Promise<void> = (async () => {
    const release = await lock.write();
    log.push('w3a');
    await sleep(2);
    log.push('w3b');
    release();
  })();

  await vi.runAllTimersAsync();

  const [v1, v2, v3] = await Promise.all([w1, w2, w3]);
  expect(v1).to.equal(1);
  expect(v2).to.equal(2);
  expect(v3).to.equal(undefined);

  expect(log).to.deep.equal(['w1a', 'w1b', 'w2a', 'w2b', 'w3a', 'w3b']);
});

test('Write then read', async () => {
  const lock = new RWLock();

  const log: string[] = [];

  const w1: Promise<number> = (async () => {
    const release = await lock.write();
    log.push('w1a');
    await sleep(6);
    log.push('w1b');
    release();
    return 1;
  })();
  const r2: Promise<number> = (async () => {
    const release = await lock.read();
    log.push('r2a');
    await sleep(4);
    log.push('r2b');
    release();
    return 2;
  })();
  const r3: Promise<void> = (async () => {
    const release = await lock.read();
    log.push('r3a');
    await sleep(2);
    log.push('r3b');
    release();
  })();

  await vi.runAllTimersAsync();

  const [v1, v2, v3] = await Promise.all([w1, r2, r3]);
  expect(v1).to.equal(1);
  expect(v2).to.equal(2);
  expect(v3).to.equal(undefined);

  expect(log).to.deep.equal(['w1a', 'w1b', 'r2a', 'r3a', 'r3b', 'r2b']);
});

test('Reads then writes', async () => {
  const lock = new RWLock();

  const log: string[] = [];

  const r1: Promise<number> = (async () => {
    const release = await lock.read();
    log.push('r1a');
    await sleep(8);
    log.push('r1b');
    release();
    return 1;
  })();
  const r2: Promise<number> = (async () => {
    const release = await lock.read();
    log.push('r2a');
    await sleep(6);
    log.push('r2b');
    release();
    return 2;
  })();
  const w3: Promise<void> = (async () => {
    const release = await lock.write();
    log.push('w3a');
    await sleep(4);
    log.push('w3b');
    release();
  })();
  const w4: Promise<number> = (async () => {
    const release = await lock.write();
    log.push('w4a');
    await sleep(2);
    log.push('w4b');
    release();
    return 4;
  })();

  await vi.runAllTimersAsync();

  const [v1, v2, v3, v4] = await Promise.all([r1, r2, w3, w4]);
  expect(v1).to.equal(1);
  expect(v2).to.equal(2);
  expect(v3).to.equal(undefined);
  expect(v4).to.equal(4);

  expect(log).to.deep.equal([
    'r1a',
    'r2a',
    'r2b',
    'r1b',
    'w3a',
    'w3b',
    'w4a',
    'w4b',
  ]);
});

test('Reads then writes (withRead)', async () => {
  const lock = new RWLock();

  const log: string[] = [];

  const r1: Promise<number> = lock.withRead(async () => {
    log.push('r1a');
    await sleep(8);
    log.push('r1b');
    return 1;
  });
  const r2: Promise<number> = lock.withRead(async () => {
    log.push('r2a');
    await sleep(6);
    log.push('r2b');
    return 2;
  });
  const w3: Promise<void> = lock.withWrite(async () => {
    log.push('w3a');
    await sleep(4);
    log.push('w3b');
  });
  const w4: Promise<number> = lock.withWrite(async () => {
    log.push('w4a');
    await sleep(2);
    log.push('w4b');
    return 4;
  });

  await vi.runAllTimersAsync();

  const [v1, v2, v3, v4] = await Promise.all([r1, r2, w3, w4]);
  expect(v1).to.equal(1);
  expect(v2).to.equal(2);
  expect(v3).to.equal(undefined);
  expect(v4).to.equal(4);

  expect(log).to.deep.equal([
    'r1a',
    'r2a',
    'r2b',
    'r1b',
    'w3a',
    'w3b',
    'w4a',
    'w4b',
  ]);
});

test('poll status', async () => {
  const testLogSink = new TestLogSink();
  const lc = new LogContext('debug', undefined, testLogSink);
  const lock = new Lock(lc);

  void lock.withLock(() => sleep(2_400));
  const lockPromise = lock.withLock(() => {});

  await vi.runAllTimersAsync();
  await lockPromise;
  expect(testLogSink.messages).toMatchInlineSnapshot(`
    [
      [
        "warn",
        {
          "component": "lock",
          "id": 0,
        },
        [
          "Lock is taking too long to resolve. It may be stuck.",
        ],
      ],
      [
        "warn",
        {
          "component": "lock",
          "id": 0,
        },
        [
          "Lock is taking too long to resolve. It may be stuck.",
        ],
      ],
    ]
  `);
});
