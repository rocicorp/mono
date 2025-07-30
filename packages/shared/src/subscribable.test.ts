import {expect, test} from 'vitest';
import {Subscribable} from './subscribable.ts';

test('subscribe adds listener and returns unsubscribe function', () => {
  const subscribable = new Subscribable<string>();
  const listener = (_arg: string) => {};

  const unsubscribe = subscribable.subscribe(listener);

  expect(subscribable.hasListeners()).toBe(true);
  expect(typeof unsubscribe).toBe('function');
});

test('unsubscribe function removes listener', () => {
  const subscribable = new Subscribable<string>();
  const listener = (_arg: string) => {};

  const unsubscribe = subscribable.subscribe(listener);
  expect(subscribable.hasListeners()).toBe(true);

  unsubscribe();
  expect(subscribable.hasListeners()).toBe(false);
});

test('notify calls all subscribed listeners with provided argument', () => {
  const subscribable = new Subscribable<number>();
  const results: number[] = [];

  const listener1 = (arg: number) => results.push(arg * 2);
  const listener2 = (arg: number) => results.push(arg * 3);

  subscribable.subscribe(listener1);
  subscribable.subscribe(listener2);

  subscribable.notify(5);

  expect(results).toEqual([10, 15]);
});

test('notify handles no listeners gracefully', () => {
  const subscribable = new Subscribable<string>();

  // Should not throw
  expect(() => subscribable.notify('test')).not.toThrow();
});

test('hasListeners returns correct state', () => {
  const subscribable = new Subscribable<boolean>();
  const listener = (_arg: boolean) => {};

  // Initially no listeners
  expect(subscribable.hasListeners()).toBe(false);

  // After subscribing
  const unsubscribe = subscribable.subscribe(listener);
  expect(subscribable.hasListeners()).toBe(true);

  // After unsubscribing
  unsubscribe();
  expect(subscribable.hasListeners()).toBe(false);
});

test('cleanup removes all listeners', () => {
  const subscribable = new Subscribable<string>();
  const listener1 = (_arg: string) => {};
  const listener2 = (_arg: string) => {};

  subscribable.subscribe(listener1);
  subscribable.subscribe(listener2);
  expect(subscribable.hasListeners()).toBe(true);

  subscribable.cleanup();
  expect(subscribable.hasListeners()).toBe(false);
});

test('multiple subscriptions of same listener are handled correctly', () => {
  const subscribable = new Subscribable<number>();
  let callCount = 0;
  const listener = (_arg: number) => {
    callCount++;
  };

  const unsubscribe1 = subscribable.subscribe(listener);
  const unsubscribe2 = subscribable.subscribe(listener);

  subscribable.notify(1);
  // Should only be called once since Set deduplicates
  expect(callCount).toBe(1);

  unsubscribe1();
  expect(subscribable.hasListeners()).toBe(false);

  // Second unsubscribe should be safe
  unsubscribe2();
  expect(subscribable.hasListeners()).toBe(false);
});

test('unsubscribing twice is safe', () => {
  const subscribable = new Subscribable<string>();
  const listener = (_arg: string) => {};

  const unsubscribe = subscribable.subscribe(listener);

  unsubscribe();
  expect(subscribable.hasListeners()).toBe(false);

  // Should not throw
  expect(() => unsubscribe()).not.toThrow();
  expect(subscribable.hasListeners()).toBe(false);
});

test('notifying after cleanup does not call listeners', () => {
  const subscribable = new Subscribable<string>();
  let called = false;
  const listener = (_arg: string) => {
    called = true;
  };

  subscribable.subscribe(listener);
  subscribable.cleanup();
  subscribable.notify('test');

  expect(called).toBe(false);
});

test('listeners are called in order they were added', () => {
  const subscribable = new Subscribable<number>();
  const results: string[] = [];

  const listener1 = (_arg: number) => results.push('first');
  const listener2 = (_arg: number) => results.push('second');
  const listener3 = (_arg: number) => results.push('third');

  subscribable.subscribe(listener1);
  subscribable.subscribe(listener2);
  subscribable.subscribe(listener3);

  subscribable.notify(1);

  expect(results).toEqual(['first', 'second', 'third']);
});

test('supports complex argument types', () => {
  interface TestData {
    id: number;
    name: string;
    active: boolean;
  }

  const subscribable = new Subscribable<TestData>();
  let receivedData: TestData | null = null;

  const listener = (data: TestData) => {
    receivedData = data;
  };
  subscribable.subscribe(listener);

  const testData: TestData = {id: 1, name: 'test', active: true};
  subscribable.notify(testData);

  expect(receivedData).toEqual(testData);
});
