import {expect} from '@esm-bundle/chai';
import {SinonFakeTimers, useFakeTimers} from 'sinon';
import {
  ConnectionLoopDelegate,
  ConnectionLoop,
  DEBOUNCE_DELAY_MS,
  MAX_DELAY_MS,
  MIN_DELAY_MS,
} from './connection-loop';
import {sleep} from './sleep';

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
      log.push(`s${c}:${Date.now()}`);
      await sleep(requestTime);
      log.push(`${invokeResult !== true ? 'e' : 'f'}${c}:${Date.now()}`);
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

  expect(log).to.deep.equal(['s0:3', 'f0:203']);

  loop.send();
  await clock.runAllAsync();

  loop.send();
  await clock.runAllAsync();

  expect(log).to.deep.equal([
    's0:3',
    'f0:203',
    's1:206',
    'f1:406',
    's2:409',
    'f2:609',
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
  expect(log).to.deep.equal(['s0:50']);

  await clock.tickAsync(40);
  expect(log).to.deep.equal(['s0:50', 'f0:100']);

  await clock.runAllAsync();

  expect(log).to.deep.equal(['s0:50', 'f0:100', 's1:110', 'f1:160']);

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

  expect(log).to.deep.equal(['s0:5']);

  await clock.tickAsync(requestTime);
  expect(Date.now()).to.equal(debounceDelay + requestTime);

  expect(log).to.deep.equal(['s0:5', 'f0:55']);

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

  expect(log).to.deep.equal(['s0:5']);
  send();
  expect(log).to.deep.equal(['s0:5']);

  await clock.tickAsync(minDelay);
  expect(Date.now()).to.equal(debounceDelay + minDelay);

  expect(log).to.deep.equal(['s0:5', 's1:35']);

  send();
  await clock.tickAsync(minDelay);
  expect(Date.now()).to.equal(debounceDelay + 2 * minDelay);

  expect(log).to.deep.equal(['s0:5', 's1:35', 's2:65']);

  send();
  await clock.tickAsync(minDelay);
  expect(Date.now()).to.equal(debounceDelay + 3 * minDelay);

  expect(log).to.deep.equal(['s0:5', 's1:35', 's2:65', 'f0:95', 's3:95']);

  await clock.tickAsync(minDelay);
  expect(Date.now()).to.equal(4 * minDelay + debounceDelay);

  expect(log).to.deep.equal([
    's0:5',
    's1:35',
    's2:65',
    'f0:95',
    's3:95',
    'f1:125',
  ]);

  await clock.tickAsync(minDelay);
  expect(Date.now()).to.equal(5 * minDelay + debounceDelay);

  expect(log).to.deep.equal([
    's0:5',
    's1:35',
    's2:65',
    'f0:95',
    's3:95',
    'f1:125',
    'f2:155',
  ]);

  await clock.tickAsync(minDelay);
  expect(Date.now()).to.equal(6 * minDelay + debounceDelay);

  expect(log).to.deep.equal([
    's0:5',
    's1:35',
    's2:65',
    'f0:95',
    's3:95',
    'f1:125',
    'f2:155',
    'f3:185',
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

  expect(log).to.deep.equal(['s0:5']);

  send();
  await clock.tickAsync(requestTime);

  expect(log).to.deep.equal(['s0:5', 'f0:95', 's1:95']);

  send();
  await clock.tickAsync(requestTime);

  expect(log).to.deep.equal(['s0:5', 'f0:95', 's1:95', 'f1:185', 's2:185']);

  await clock.tickAsync(requestTime);

  expect(log).to.deep.equal([
    's0:5',
    'f0:95',
    's1:95',
    'f1:185',
    's2:185',
    'f2:275',
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

  expect(log).to.deep.equal(['s0:5', 's1:35', 's2:65', 'f0:105', 's3:105']);

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
    's0:5',
    's1:35',
    's2:65',
    'f0:105',
    's3:105',
    'f3:205',
    's4:205',
    'f2:215',
    'f1:235',
    's5:238',
    's6:279',
    'f6:379',
    'f5:388',
    'f4:405',
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
      's0:5',
      's1:35',
      's2:65',
      'f0:95',
      's3:95',
      'f1:125',
      's4:125',
      'f2:155',
      's5:155',
      'f3:185',
      's6:185',
      'f4:215',
      's7:215',
      'e5:245',
      'e6:275',
      's8:275',
      'e7:305',
      'e8:365',
      's9:395',
      'e9:485',
      's10:635',
      'e10:725',
      's11:1115',
      'e11:1205',
      's12:2075',
      'e12:2165',
      's13:3995',
      'e13:4085',
      's14:7835',
      'e14:7925',
      's15:15515',
      'e15:15605',
      's16:30875',
      'e16:30965',
      's17:61595',
      'f17:61685', // first success
      's18:61685', // now we go back to 3 concurrent connections
      's19:61715',
      's20:61745',
      'f18:61775',
      's21:61775',
      'f19:61805',
      'f20:61835',
      'f21:61865',
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
      's0:5',
      'e0:55',
      's1:85',
      'e1:135',
      's2:245',
      'e2:295',
      's3:565',
      'e3:615',
      's4:1205',
      'e4:1255',
      's5:2485',
      'f5:2535',
      's6:2565',
      'f6:2615',
      's7:2645',
      'f7:2695',
      's8:2695',
      'f8:2745',
      's9:2745',
      'f9:2795',
      's10:2795',
      'f10:2845',
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

  expect(log).to.deep.equal(['s0:1010']);

  await clock.tickAsync(requestTime);
  expect(log).to.deep.equal(['s0:1010', 'f0:1110']);

  await clock.tickAsync(watchdogTimer);

  expect(log).to.deep.equal(['s0:1010', 'f0:1110', 's1:2020']);

  await clock.tickAsync(requestTime);

  expect(log).to.deep.equal(['s0:1010', 'f0:1110', 's1:2020', 'f1:2120']);
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

  expect(log).to.deep.equal(['s0:510']);

  await clock.tickAsync(requestTime);
  expect(log).to.deep.equal(['s0:510', 'f0:610']);

  await clock.tickAsync(watchdogTimer);

  expect(log).to.deep.equal(['s0:510', 'f0:610', 's1:1520']);

  await clock.tickAsync(requestTime);

  expect(log).to.deep.equal(['s0:510', 'f0:610', 's1:1520', 'f1:1620']);
});

test('mutate minDelayMs', async () => {
  let minDelayMs = 50;
  const log: number[] = [];
  loop = new ConnectionLoop({
    async invokeSend() {
      log.push(Date.now());
      return true;
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
