import {expect} from '@esm-bundle/chai';
import {SinonFakeTimers, useFakeTimers} from 'sinon';
import {sleep} from './sleep.js';
import {AbortError} from './abort-error.js';

let clock: SinonFakeTimers;
setup(() => {
  clock = useFakeTimers(0);
});

teardown(() => {
  clock.restore();
});

test('sleep', async () => {
  let callCount = 0;
  const p = (async () => {
    await sleep(100);
    callCount++;
  })();
  await clock.tickAsync(99);
  expect(Date.now()).to.equal(99);
  expect(callCount).to.equal(0);

  await clock.tickAsync(1);
  expect(callCount).to.equal(1);
  expect(Date.now()).to.equal(100);

  await clock.tickAsync(100);
  expect(callCount).to.equal(1);
  expect(Date.now()).to.equal(200);

  await p;
  expect(Date.now()).to.equal(200);
});

test('sleep abort', async () => {
  const controller = new AbortController();
  const p = sleep(100, controller.signal);
  controller.abort();
  let e;
  try {
    expect(Date.now()).to.equal(0);
    await p;
  } catch (err) {
    e = err;
  }
  expect(Date.now()).to.equal(0);

  expect(e).to.be.instanceof(AbortError);

  await clock.tickAsync(100);
  expect(Date.now()).to.equal(100);

  await clock.tickAsync(100);
  expect(Date.now()).to.equal(200);
});
