import {describe, expect, test} from '@jest/globals';
import {CountDownLatch, Signal} from './async.js';
import {sleep} from './sleep.js';

describe('CountDownLatch', () => {
  test('countDown and zero', async () => {
    const latch1 = new CountDownLatch();
    const latch2 = new CountDownLatch(2);

    const fn = new Promise<void>(async resolve => {
      await latch2.zero();
      latch1.countDown();
      resolve();
    });

    expect(latch1.value()).toBe(1);
    expect(latch2.value()).toBe(2);

    latch2.countDown();

    expect(latch1.value()).toBe(1);
    expect(latch2.value()).toBe(1);

    latch2.countDown();
    expect(latch2.value()).toBe(0);

    await fn;
    expect(latch1.value()).toBe(0);
  });

  test('countDown does not go below zero', () => {
    const latch = new CountDownLatch(3);
    expect(latch.value()).toBe(3);
    latch.countDown();
    expect(latch.value()).toBe(2);
    latch.countDown();
    expect(latch.value()).toBe(1);
    latch.countDown();
    expect(latch.value()).toBe(0);
    latch.countDown();
    expect(latch.value()).toBe(0);
  });
});

describe('Signal', () => {
  test('notify and notification', async () => {
    const signal1 = new Signal();
    const signal2 = new Signal();

    let waiting = true;

    const fn = new Promise<void>(async resolve => {
      await signal2.notification();
      waiting = false;
      signal1.notify();
      resolve();
    });

    await sleep(1);
    expect(waiting).toBe(true);

    signal2.notify();
    await signal1.notification();
    expect(waiting).toBe(false);

    expect(fn).resolves;
  });
});
