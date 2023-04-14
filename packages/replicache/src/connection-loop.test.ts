import {expect} from '@esm-bundle/chai';
import {sleep} from 'shared/sleep.js';
import {SinonFakeTimers, useFakeTimers} from 'sinon';
import {
  ConnectionLoop,
  ConnectionLoopDelegate,
  DEBOUNCE_DELAY_MS,
  MAX_DELAY_MS,
  MIN_DELAY_MS,
} from './connection-loop.js';
import {promiseTrue} from './resolved-promises.js';

let clock: SinonFakeTimers;
setup(() => {
  clock = useFakeTimers(0);
});

teardown(() => {
  clock.restore();
  loop?.close();
  loop = undefined;
});

let loop: ConnectionLoop | undefined;

const ps = new Set();

function send() {
  if (!loop) {
    throw new Error();
  }
  const p = loop.send();
  ps.add(p);
  return p;
}

async function waitForAll() {
  await Promise.allSettled(ps);
  ps.clear();
}

let counter = 0;
const log: string[] = [];

function createLoop(
  partialDelegate: Partial<ConnectionLoopDelegate> & {
    requestTime: number;
    invokeResult?: boolean | 'throw';
  } = {requestTime: 90},
): ConnectionLoop {
  log.length = 0;
  counter = 0;

  const delegate = {
    async invokeSend() {
      const c = counter++;
      const {requestTime = 90, invokeResult = true} = partialDelegate;
      log.push(`send:${c}:${Date.now()}`);
      await sleep(requestTime);
      log.push(
        `${
          invokeResult !== true ? 'false-or-throw' : 'true'
        }:${c}:${Date.now()}`,
      );
      if (invokeResult === 'throw') {
        throw Error('Intentional error');
      }
      return invokeResult;
    },

    watchdogTimer: null,
    debounceDelay: DEBOUNCE_DELAY_MS,
    maxConnections: 3,
    maxDelayMs: MAX_DELAY_MS,
    ...partialDelegate,
    get minDelayMs() {
      return partialDelegate.minDelayMs ?? MIN_DELAY_MS;
    },
    debug() {
      // intentionally empty
    },
  };

  return (loop = new ConnectionLoop(delegate));
}

test('basic sequential by awaiting', async () => {
  const requestTime = 200;
  const debounceDelay = 3;
  loop = createLoop({requestTime, debounceDelay});

  loop.send();
  await clock.runAllAsync();
  expect(Date.now()).to.equal(requestTime + debounceDelay);

  expect(log).to.deep.equal(['send:0:3', 'true:0:203']);

  loop.send();
  await clock.runAllAsync();

  loop.send();
  await clock.runAllAsync();

  expect(log).to.deep.equal([
    'send:0:3',
    'true:0:203',
    'send:1:206',
    'true:1:406',
    'send:2:409',
    'true:2:609',
  ]);
});

test('debounce', async () => {
  const debounceDelay = 50;
  const requestTime = 50;
  createLoop({
    requestTime,
    debounceDelay,
  });

  send();
  expect(log).to.deep.equal([]);
  await clock.tickAsync(20);
  send();
  expect(log).to.deep.equal([]);

  await clock.tickAsync(20);
  send();
  expect(log).to.deep.equal([]);

  await clock.tickAsync(20);
  send();
  expect(log).to.deep.equal(['send:0:50']);

  await clock.tickAsync(40);
  expect(log).to.deep.equal(['send:0:50', 'true:0:100']);

  await clock.runAllAsync();

  expect(log).to.deep.equal([
    'send:0:50',
    'true:0:100',
    'send:1:110',
    'true:1:160',
  ]);

  await waitForAll();
});

test('sync calls collapsed', async () => {
  const debounceDelay = 5;
  const requestTime = 50;
  createLoop({
    requestTime,
    debounceDelay,
  });

  send();
  expect(log).to.deep.equal([]);
  send();
  expect(log).to.deep.equal([]);
  send();
  expect(log).to.deep.equal([]);

  await clock.tickAsync(debounceDelay);
  expect(Date.now()).to.equal(debounceDelay);

  expect(log).to.deep.equal(['send:0:5']);

  await clock.tickAsync(requestTime);
  expect(Date.now()).to.equal(debounceDelay + requestTime);

  expect(log).to.deep.equal(['send:0:5', 'true:0:55']);

  await waitForAll();
});

test('concurrent connections', async () => {
  const debounceDelay = 5;
  const minDelay = 30;
  const maxConnections = 3;
  // The request time is selected to make the delay not adjust itself.
  const requestTime = minDelay * maxConnections;

  createLoop({
    requestTime,
    debounceDelay,
    maxConnections,
  });

  send();

  await clock.runToLastAsync();
  expect(Date.now()).to.equal(debounceDelay);

  expect(log).to.deep.equal(['send:0:5']);
  send();
  expect(log).to.deep.equal(['send:0:5']);

  await clock.tickAsync(minDelay);
  expect(Date.now()).to.equal(debounceDelay + minDelay);

  expect(log).to.deep.equal(['send:0:5', 'send:1:35']);

  send();
  await clock.tickAsync(minDelay);
  expect(Date.now()).to.equal(debounceDelay + 2 * minDelay);

  expect(log).to.deep.equal(['send:0:5', 'send:1:35', 'send:2:65']);

  send();
  await clock.tickAsync(minDelay);
  expect(Date.now()).to.equal(debounceDelay + 3 * minDelay);

  expect(log).to.deep.equal([
    'send:0:5',
    'send:1:35',
    'send:2:65',
    'true:0:95',
    'send:3:95',
  ]);

  await clock.tickAsync(minDelay);
  expect(Date.now()).to.equal(4 * minDelay + debounceDelay);

  expect(log).to.deep.equal([
    'send:0:5',
    'send:1:35',
    'send:2:65',
    'true:0:95',
    'send:3:95',
    'true:1:125',
  ]);

  await clock.tickAsync(minDelay);
  expect(Date.now()).to.equal(5 * minDelay + debounceDelay);

  expect(log).to.deep.equal([
    'send:0:5',
    'send:1:35',
    'send:2:65',
    'true:0:95',
    'send:3:95',
    'true:1:125',
    'true:2:155',
  ]);

  await clock.tickAsync(minDelay);
  expect(Date.now()).to.equal(6 * minDelay + debounceDelay);

  expect(log).to.deep.equal([
    'send:0:5',
    'send:1:35',
    'send:2:65',
    'true:0:95',
    'send:3:95',
    'true:1:125',
    'true:2:155',
    'true:3:185',
  ]);

  await clock.runAllAsync();
  await waitForAll();
});

test('maxConnections 1', async () => {
  const debounceDelay = 5;
  const maxConnections = 1;
  const requestTime = 90;

  createLoop({
    requestTime,
    debounceDelay,
    maxConnections,
  });

  send();
  await clock.runToLastAsync();

  expect(log).to.deep.equal(['send:0:5']);

  send();
  await clock.tickAsync(requestTime);

  expect(log).to.deep.equal(['send:0:5', 'true:0:95', 'send:1:95']);

  send();
  await clock.tickAsync(requestTime);

  expect(log).to.deep.equal([
    'send:0:5',
    'true:0:95',
    'send:1:95',
    'true:1:185',
    'send:2:185',
  ]);

  await clock.tickAsync(requestTime);

  expect(log).to.deep.equal([
    'send:0:5',
    'true:0:95',
    'send:1:95',
    'true:1:185',
    'send:2:185',
    'true:2:275',
  ]);

  await clock.runAllAsync();
  await waitForAll();
});

test('Adjust delay', async () => {
  const debounceDelay = 5;
  const maxConnections = 3;
  const requestTimes = [100, 200, 150];
  let i = 0;

  createLoop({
    get requestTime() {
      const t = requestTimes[i];
      i = (i + 1) % requestTimes.length;
      return t;
    },
    debounceDelay,
    maxConnections,
  });

  // reset
  i = 0;

  // 0
  send();
  await clock.runToLastAsync();

  // 1
  send();
  await clock.tickAsync(30);

  // 2
  send();
  await clock.tickAsync(30);

  // 3
  send();
  await clock.tickAsync(50);

  expect(log).to.deep.equal([
    'send:0:5',
    'send:1:35',
    'send:2:65',
    'true:0:105',
    'send:3:105',
  ]);

  // 4
  send();
  await clock.tickAsync(50);

  // 5
  send();
  await clock.tickAsync(50);

  // 6
  send();
  await clock.tickAsync(50);

  await clock.runAllAsync();
  expect(log).to.deep.equal([
    'send:0:5',
    'send:1:35',
    'send:2:65',
    'true:0:105',
    'send:3:105',
    'true:3:205',
    'send:4:205',
    'true:2:215',
    'true:1:235',
    'send:5:238',
    'send:6:279',
    'true:6:379',
    'true:5:388',
    'true:4:405',
  ]);
  await waitForAll();
});

for (const errorKind of [false, 'throw'] as const) {
  test(`error {errorKind: ${errorKind}}`, async () => {
    const debounceDelay = 5;
    const maxConnections = 3;
    const requestTime = 90;
    let requestCount = 0;

    createLoop({
      get invokeResult() {
        const shouldFail = requestCount > 4 && requestCount < 17;
        requestCount++;
        return shouldFail ? errorKind : true;
      },
      debounceDelay,
      requestTime,
      maxConnections,
    });

    // reset
    requestCount = 0;

    while (requestCount < 10) {
      send();
      await clock.tickAsync(30);
    }

    // 61685 is when the first success after a bunch of errors. Schedule a send
    // before this request comes back.
    await clock.tickAsync(61685 - 30 - Date.now());

    while (requestCount < 22) {
      send();
      await clock.tickAsync(30);
    }

    await clock.runAllAsync();

    expect(log).to.deep.equal([
      'send:0:5',
      'send:1:35',
      'send:2:65',
      'true:0:95',
      'send:3:95',
      'true:1:125',
      'send:4:125',
      'true:2:155',
      'send:5:155',
      'true:3:185',
      'send:6:185',
      'true:4:215',
      'send:7:215',
      'false-or-throw:5:245',
      'false-or-throw:6:275',
      'send:8:275',
      'false-or-throw:7:305',
      'false-or-throw:8:365',
      'send:9:395',
      'false-or-throw:9:485',
      'send:10:635',
      'false-or-throw:10:725',
      'send:11:1115',
      'false-or-throw:11:1205',
      'send:12:2075',
      'false-or-throw:12:2165',
      'send:13:3995',
      'false-or-throw:13:4085',
      'send:14:7835',
      'false-or-throw:14:7925',
      'send:15:15515',
      'false-or-throw:15:15605',
      'send:16:30875',
      'false-or-throw:16:30965',
      'send:17:61595',
      'true:17:61685', // first success
      'send:18:61685', // now we go back to 3 concurrent connections
      'send:19:61715',
      'send:20:61745',
      'true:18:61775',
      'send:21:61775',
      'true:19:61805',
      'true:20:61835',
      'true:21:61865',
    ]);
  });

  test(`error {errorKind: ${errorKind} start with error}`, async () => {
    // This tests that if the first few requests fail we recover correctly.
    const debounceDelay = 5;
    const maxConnections = 1;
    const requestTime = 50;
    let requestCount = 0;
    let minDelayMs = 80;

    createLoop({
      get invokeResult() {
        const shouldFail = requestCount < 5;
        requestCount++;
        return shouldFail ? errorKind : true;
      },
      debounceDelay,
      requestTime,
      maxConnections,
      get minDelayMs() {
        return minDelayMs;
      },
    });

    // reset
    requestCount = 0;

    while (requestCount < 5) {
      send();
      await clock.tickAsync(10);
    }

    while (requestCount < 8) {
      send();
      await clock.tickAsync(10);
    }

    minDelayMs = 40;

    while (requestCount < 10) {
      send();
      await clock.tickAsync(10);
    }

    await clock.runAllAsync();

    expect(log).to.deep.equal([
      'send:0:5',
      'false-or-throw:0:55',
      'send:1:85',
      'false-or-throw:1:135',
      'send:2:245',
      'false-or-throw:2:295',
      'send:3:565',
      'false-or-throw:3:615',
      'send:4:1205',
      'false-or-throw:4:1255',
      'send:5:2485',
      'true:5:2535',
      'send:6:2565',
      'true:6:2615',
      'send:7:2645',
      'true:7:2695',
      'send:8:2695',
      'true:8:2745',
      'send:9:2745',
      'true:9:2795',
      'send:10:2795',
      'true:10:2845',
    ]);
  });
}

test('watchdog timer', async () => {
  const debounceDelay = 10;
  const requestTime = 100;
  const watchdogTimer = 1000;
  createLoop({
    debounceDelay,
    watchdogTimer,
    requestTime,
  });

  await clock.tickAsync(watchdogTimer);

  expect(log).to.deep.equal([]);

  await clock.tickAsync(debounceDelay);

  expect(log).to.deep.equal(['send:0:1010']);

  await clock.tickAsync(requestTime);
  expect(log).to.deep.equal(['send:0:1010', 'true:0:1110']);

  await clock.tickAsync(watchdogTimer);

  expect(log).to.deep.equal(['send:0:1010', 'true:0:1110', 'send:1:2020']);

  await clock.tickAsync(requestTime);

  expect(log).to.deep.equal([
    'send:0:1010',
    'true:0:1110',
    'send:1:2020',
    'true:1:2120',
  ]);
});

test('watchdog timer again', async () => {
  const debounceDelay = 10;
  const requestTime = 100;
  const watchdogTimer = 1000;
  createLoop({
    debounceDelay,
    watchdogTimer,
    requestTime,
  });

  await clock.tickAsync(500);
  send();

  expect(log).to.deep.equal([]);

  await clock.tickAsync(debounceDelay);

  expect(log).to.deep.equal(['send:0:510']);

  await clock.tickAsync(requestTime);
  expect(log).to.deep.equal(['send:0:510', 'true:0:610']);

  await clock.tickAsync(watchdogTimer);

  expect(log).to.deep.equal(['send:0:510', 'true:0:610', 'send:1:1520']);

  await clock.tickAsync(requestTime);

  expect(log).to.deep.equal([
    'send:0:510',
    'true:0:610',
    'send:1:1520',
    'true:1:1620',
  ]);
});

test('mutate minDelayMs', async () => {
  let minDelayMs = 50;
  const log: number[] = [];
  loop = new ConnectionLoop({
    invokeSend() {
      log.push(Date.now());
      return promiseTrue;
    },
    debounceDelay: 0,
    get minDelayMs() {
      return minDelayMs;
    },
    maxDelayMs: 60_000,
    maxConnections: 3,
    watchdogTimer: null,
  });

  while (Date.now() < 200) {
    send();
    await clock.tickAsync(25);
  }

  minDelayMs = 500;

  while (Date.now() < 2000) {
    send();
    await clock.tickAsync(50);
  }

  minDelayMs = 20;

  while (Date.now() < 2400) {
    send();
    await clock.tickAsync(10);
  }

  expect(log).to.deep.equal([
    0, 50, 100, 150, 200, 250, 750, 1250, 1750, 2250, 2270, 2290, 2310, 2330,
    2350, 2370, 2390,
  ]);
});
