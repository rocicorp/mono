import {beforeEach, describe, expect, test, vi} from 'vitest';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import {type AbstractQuery} from '../../zql/src/query/query-impl.ts';
import type {ResultType} from '../../zql/src/query/typed-view.ts';
import {getAllViewsSizeForTesting, ViewStore} from './use-query.tsx';

function newMockQuery(
  query: string,
  singular = false,
): AbstractQuery<Schema, string> {
  const view = newView();
  return {
    hash() {
      return query;
    },
    materialize: vi.fn().mockImplementation(() => view),
    format: {singular},
  } as unknown as AbstractQuery<Schema, string>;
}

function newView() {
  return {
    listeners: new Set<() => void>(),
    addListener(cb: () => void) {
      this.listeners.add(cb);
    },
    destroy() {
      this.listeners.clear();
    },
    updateTTL() {},
  };
}

describe('ViewStore', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  describe('duplicate queries', () => {
    test('duplicate queries do not create duplicate views', () => {
      const viewStore = new ViewStore();

      const view1 = viewStore.getView(
        'client1',
        newMockQuery('query1'),
        true,
        'none',
      );
      const view2 = viewStore.getView(
        'client1',
        newMockQuery('query1'),
        true,
        'none',
      );

      expect(view1).toBe(view2);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });

    test('removing a duplicate query does not destroy the shared view', () => {
      const viewStore = new ViewStore();

      const view1 = viewStore.getView(
        'client1',
        newMockQuery('query1'),
        true,
        'none',
      );
      const view2 = viewStore.getView(
        'client1',
        newMockQuery('query1'),
        true,
        'none',
      );

      const cleanup1 = view1.subscribeReactInternals(() => {});
      view2.subscribeReactInternals(() => {});

      cleanup1();

      vi.advanceTimersByTime(100);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });

    test('Using the same query with different TTL should reuse views', () => {
      const viewStore = new ViewStore();

      const q1 = newMockQuery('query1');
      const view1 = viewStore.getView('client1', q1, true, '1s');

      const updateTTLSpy = vi.spyOn(view1, 'updateTTL');
      expect(q1.materialize).toHaveBeenCalledExactlyOnceWith('1s');

      const q2 = newMockQuery('query1');
      const view2 = viewStore.getView('client1', q2, true, '1m');
      expect(view1).toBe(view2);

      // Same query hash so only one view. Should have called updateTTL on the existing one.
      expect(q2.materialize).not.toHaveBeenCalled();
      expect(updateTTLSpy).toHaveBeenCalledExactlyOnceWith('1m');

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });

    test('Using the same query with same TTL but different representation', () => {
      const viewStore = new ViewStore();

      const q1 = newMockQuery('query1');
      const view1 = viewStore.getView('client1', q1, true, '60s');
      const updateTTLSpy = vi.spyOn(view1, 'updateTTL');
      expect(q1.materialize).toHaveBeenCalledTimes(1);

      const q2 = newMockQuery('query1');
      const view2 = viewStore.getView('client1', q2, true, '1m');
      expect(view1).toBe(view2);

      expect(updateTTLSpy).toHaveBeenCalledExactlyOnceWith('1m');

      const q3 = newMockQuery('query1');
      const view3 = viewStore.getView('client1', q3, true, 60_000);

      expect(view1).toBe(view3);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });
  });

  describe('destruction', () => {
    test('removing all duplicate queries destroys the shared view', () => {
      const viewStore = new ViewStore();

      const view1 = viewStore.getView(
        'client1',
        newMockQuery('query1'),
        true,
        'none',
      );
      const view2 = viewStore.getView(
        'client1',
        newMockQuery('query1'),
        true,
        'none',
      );

      const cleanup1 = view1.subscribeReactInternals(() => {});
      const cleanup2 = view2.subscribeReactInternals(() => {});

      cleanup1();
      cleanup2();

      vi.advanceTimersByTime(100);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });

    test('removing a unique query destroys the view', () => {
      const viewStore = new ViewStore();

      const view = viewStore.getView(
        'client1',
        newMockQuery('query1'),
        true,
        'none',
      );

      const cleanup = view.subscribeReactInternals(() => {});
      cleanup();

      vi.advanceTimersByTime(100);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });

    test('view destruction is delayed via setTimeout', () => {
      const viewStore = new ViewStore();

      const view = viewStore.getView(
        'client1',
        newMockQuery('query1'),
        true,
        'none',
      );

      const cleanup = view.subscribeReactInternals(() => {});
      cleanup();

      vi.advanceTimersByTime(5);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
      vi.advanceTimersByTime(10);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });

    test('subscribing to a view scheduled for cleanup prevents the cleanup', () => {
      const viewStore = new ViewStore();
      const view = viewStore.getView(
        'client1',
        newMockQuery('query1'),
        true,
        'none',
      );
      const cleanup = view.subscribeReactInternals(() => {});

      cleanup();

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
      vi.advanceTimersByTime(5);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);

      const view2 = viewStore.getView(
        'client1',
        newMockQuery('query1'),
        true,
        'none',
      );
      const cleanup2 = view.subscribeReactInternals(() => {});
      vi.advanceTimersByTime(100);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);

      expect(view2).toBe(view);

      cleanup2();
      vi.advanceTimersByTime(100);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });

    test('destroying the same underlying view twice is a no-op', () => {
      const viewStore = new ViewStore();
      const view = viewStore.getView(
        'client1',
        newMockQuery('query1'),
        true,
        'none',
      );
      const cleanup = view.subscribeReactInternals(() => {});

      cleanup();
      cleanup();

      vi.advanceTimersByTime(100);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });
  });

  describe('clients', () => {
    test('the same query for different clients results in different views', () => {
      const viewStore = new ViewStore();

      const view1 = viewStore.getView(
        'client1',
        newMockQuery('query1'),
        true,
        'none',
      );
      const view2 = viewStore.getView(
        'client2',
        newMockQuery('query1'),
        true,
        'none',
      );

      expect(view1).not.toBe(view2);
    });
  });

  describe('collapse multiple empty on data', () => {
    test('plural', () => {
      const viewStore = new ViewStore();
      const q = newMockQuery('query1');
      const {listeners} = q.materialize() as unknown as {
        listeners: Set<(data: unknown, resultType: ResultType) => void>;
      };
      const view = viewStore.getView('client1', q, true, 'none');

      const cleanup = view.subscribeReactInternals(() => {});

      listeners.forEach(cb => cb([], 'unknown'));

      const snapshot1 = view.getSnapshot();

      listeners.forEach(cb => cb([], 'unknown'));

      const snapshot2 = view.getSnapshot();

      expect(snapshot1).toBe(snapshot2);

      listeners.forEach(cb => cb([{a: 1}], 'unknown'));

      // TODO: Assert that data[0] is the same object as passed into the listener.
      expect(view.getSnapshot()).toEqual([[{a: 1}], {type: 'unknown'}]);

      listeners.forEach(cb => cb([], 'complete'));
      const snapshot3 = view.getSnapshot();
      expect(snapshot3).toEqual([[], {type: 'complete'}]);

      listeners.forEach(cb => cb([], 'complete'));
      const snapshot4 = view.getSnapshot();
      expect(snapshot3).toBe(snapshot4);

      cleanup();
    });

    test('singular', () => {
      const viewStore = new ViewStore();
      const q = newMockQuery('query1', true);
      const {listeners} = q.materialize() as unknown as {
        listeners: Set<(...args: unknown[]) => void>;
      };
      const view = viewStore.getView('client1', q, true, 'none');

      const cleanup = view.subscribeReactInternals(() => {});

      listeners.forEach(cb => cb(undefined, 'unknown'));
      const snapshot1 = view.getSnapshot();
      expect(snapshot1).toEqual([undefined, {type: 'unknown'}]);

      listeners.forEach(cb => cb(undefined, 'unknown'));
      const snapshot2 = view.getSnapshot();
      expect(snapshot1).toBe(snapshot2);

      listeners.forEach(cb => cb({a: 1}, 'unknown'));
      // TODO: Assert that data is the same object as passed into the listener.
      expect(view.getSnapshot()).toEqual([{a: 1}, {type: 'unknown'}]);

      listeners.forEach(cb => cb(undefined, 'complete'));
      const snapshot3 = view.getSnapshot();
      expect(snapshot3).toEqual([undefined, {type: 'complete'}]);

      listeners.forEach(cb => cb(undefined, 'complete'));
      const snapshot4 = view.getSnapshot();
      expect(snapshot3).toBe(snapshot4);

      cleanup();
    });
  });
});
