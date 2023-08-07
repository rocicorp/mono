import {describe, test, expect} from '@jest/globals';
import {resolver} from '@rocicorp/resolver';
import {watch} from './watch.js';
import {fakeFirestore} from './test-helpers.js';

describe('watch', () => {
  test('receives snapshots', async () => {
    const {promise: wasUnsubscribeCalled, resolve: unsubscribe} =
      resolver<void>();
    const doc = {
      onSnapshot: (onNext: (snapshot: string) => void) => {
        setTimeout(() => {
          onNext('foo');
          onNext('bar');
          onNext('baz');
        }, 1);
        return unsubscribe;
      },
    };
    const received = [];
    for await (const snapshot of watch(doc)) {
      received.push(snapshot);
      if (received.length === 3) {
        break;
      }
    }
    await wasUnsubscribeCalled;
    expect(received).toEqual(['foo', 'bar', 'baz']);
  });

  test('unsubscribes after iteration', async () => {
    const {promise: wasUnsubscribeCalled, resolve: unsubscribe} =
      resolver<void>();
    const doc = {
      onSnapshot: (onNext: (snapshot: string) => void) => {
        setTimeout(() => {
          onNext('foo');
          onNext('bar');
          onNext('baz');
        }, 1);
        return unsubscribe;
      },
    };
    const received = [];
    let err: unknown;
    try {
      for await (const snapshot of watch(doc)) {
        received.push(snapshot);
        if (received.length === 3) {
          throw new Error('bonk');
        }
      }
    } catch (e) {
      err = e;
    }
    await wasUnsubscribeCalled;
    expect(received).toEqual(['foo', 'bar', 'baz']);
    expect(String(err)).toBe('Error: bonk');
  });

  test('unsubscribes after snapshot error', async () => {
    const {promise: wasUnsubscribeCalled, resolve: unsubscribe} =
      resolver<void>();
    const doc = {
      onSnapshot: (
        onNext: (snapshot: string) => void,
        onError: (err: Error) => void,
      ) => {
        setTimeout(() => {
          onNext('foo');
          onNext('bar');
          onError(new Error('bonk'));
        }, 1);
        return unsubscribe;
      },
    };
    const received = [];
    let err: unknown;
    try {
      for await (const snapshot of watch(doc)) {
        received.push(snapshot);
      }
    } catch (e) {
      err = e;
    }
    await wasUnsubscribeCalled;
    expect(received).toEqual(['foo', 'bar']);
    expect(String(err)).toBe('Error: bonk');
  });

  test('unsubscribes after timeout', async () => {
    const {promise: wasUnsubscribeCalled, resolve: unsubscribe} =
      resolver<void>();
    const doc = {
      onSnapshot: (onNext: (snapshot: string) => void) => {
        setTimeout(() => {
          onNext('foo');
        }, 1);
        setTimeout(() => {
          onNext('bar');
        }, 2);
        setTimeout(() => {
          onNext('baz');
        }, 10);
        return unsubscribe;
      },
    };
    const received = [];
    let err: unknown;
    try {
      for await (const snapshot of watch(doc, 5)) {
        received.push(snapshot);
      }
    } catch (e) {
      err = e;
    }

    await wasUnsubscribeCalled;
    expect(received).toEqual(['foo', 'bar']);
    expect(String(err)).toBe('Error: Timed out after 5 milliseconds');
  });

  test('with firebase mock document', async () => {
    const firebase = fakeFirestore();
    await firebase.doc('foo/bar').set({foo: 'bar'});

    let data;
    for await (const snapshot of watch(firebase.doc('foo/bar'))) {
      data = snapshot.data();
      break;
    }
    expect(data).toEqual({foo: 'bar'});
  });

  test('with firebase mock query', async () => {
    const firebase = fakeFirestore();
    await firebase.doc('foo/bar').set({foo: 'bar'});
    await firebase.doc('foo/baz').set({foo: 'baz'});

    let data;
    for await (const snapshot of watch(firebase.collection('foo'))) {
      data = snapshot.docs.map(doc => doc.data());
      break;
    }
    expect(data).toEqual([{foo: 'bar'}, {foo: 'baz'}]);
  });
});
