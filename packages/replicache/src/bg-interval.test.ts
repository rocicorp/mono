import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import sinon, {type SinonFakeTimers, useFakeTimers} from 'sinon';
import {afterEach, beforeEach, expect, test} from 'vitest';
import {TestLogSink} from '../../shared/src/logging-test-utils.js';
import {initBgIntervalProcess} from './bg-interval.js';

let clock: SinonFakeTimers;
beforeEach(() => {
  clock = useFakeTimers();
});

afterEach(() => {
  clock.restore();
  sinon.restore();
});

test('initBgIntervalProcess starts interval that executes process with delayMs between each execution', async () => {
  let processCallCount = 0;
  const process = async () => {
    processCallCount++;
    await clock.tickAsync(50);
  };
  const controller = new AbortController();
  initBgIntervalProcess(
    'testProcess',
    process,
    () => 100,
    new LogContext(),
    controller.signal,
  );

  expect(processCallCount).to.equal(0);
  await clock.tickAsync(100);
  expect(processCallCount).to.equal(1);
  await clock.tickAsync(100);
  expect(processCallCount).to.equal(2);
  await clock.tickAsync(100);
  await clock.tickAsync(100);
  await clock.tickAsync(100);
  await clock.tickAsync(100);
  expect(processCallCount).to.equal(6);
});

test('initBgIntervalProcess starts interval that executes process with delayMs at 100 on even process call count and 50 on odd process call count', async () => {
  let processCallCount = 0;
  const process = async () => {
    processCallCount++;
    await clock.tickAsync(50);
  };
  const controller = new AbortController();
  initBgIntervalProcess(
    'testProcess',
    process,
    () => {
      if (processCallCount % 2 === 0) {
        return 100;
      }
      return 50;
    },
    new LogContext(),
    controller.signal,
  );

  expect(processCallCount).to.equal(0);
  await clock.tickAsync(100);
  expect(processCallCount).to.equal(1);
  await clock.tickAsync(50);
  expect(processCallCount).to.equal(2);
  await clock.tickAsync(100);
  expect(processCallCount).to.equal(3);
  await clock.tickAsync(50);
  expect(processCallCount).to.equal(4);
  await clock.tickAsync(50);
  expect(processCallCount).to.equal(4);
  await clock.tickAsync(50);
  expect(processCallCount).to.equal(5);
  await clock.tickAsync(100);
  expect(processCallCount).to.equal(6);
});

test('calling function returned by initBgIntervalProcess, stops interval', async () => {
  let processCallCount = 0;
  const process = () => {
    processCallCount++;
    return Promise.resolve();
  };
  const controller = new AbortController();
  initBgIntervalProcess(
    'testProcess',
    process,
    () => 100,
    new LogContext(),
    controller.signal,
  );

  expect(processCallCount).to.equal(0);
  await clock.tickAsync(100);
  expect(processCallCount).to.equal(1);
  controller.abort();
  await clock.tickAsync(100);
  expect(processCallCount).to.equal(1);
  await clock.tickAsync(400);
  expect(processCallCount).to.equal(1);
});

test('error thrown during process (before stop is called) is logged to error', async () => {
  const lc = new LogContext();
  const errorStub = sinon.stub(console, 'error');
  const process = () => Promise.reject('TestErrorBeforeStop');
  const controller = new AbortController();
  initBgIntervalProcess(
    'testProcess',
    process,
    () => 100,
    lc,
    controller.signal,
  );
  await clock.tickAsync(100);
  sinon.assert.calledOnceWithExactly(
    errorStub,
    'bgIntervalProcess=testProcess',
    'Error running.',
    'TestErrorBeforeStop',
  );
});

test('error thrown during process (after stop is called) is logged to debug', async () => {
  const testLogSink = new TestLogSink();
  const lc = new LogContext('debug', undefined, testLogSink);

  let processCallCount = 0;
  const processResolver = resolver();
  const process = () => {
    processCallCount++;
    return processResolver.promise;
  };
  const controller = new AbortController();
  initBgIntervalProcess(
    'testProcess',
    process,
    () => 100,
    lc,
    controller.signal,
  );
  expect(processCallCount).to.equal(0);
  await clock.tickAsync(100);
  expect(processCallCount).to.equal(1);
  controller.abort();
  processResolver.reject('TestErrorAfterStop');
  try {
    await processResolver.promise;
  } catch (e) {
    expect(e).to.equal('TestErrorAfterStop');
  }
  expect(testLogSink.messages).to.deep.equal([
    ['debug', {bgIntervalProcess: 'testProcess'}, ['Starting']],
    ['debug', {bgIntervalProcess: 'testProcess'}, ['Running']],
    [
      'debug',
      {bgIntervalProcess: 'testProcess'},
      ['Error running most likely due to close.', 'TestErrorAfterStop'],
    ],
    ['debug', {bgIntervalProcess: 'testProcess'}, ['Stopping']],
  ]);
});
