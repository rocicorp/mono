/* eslint-disable @typescript-eslint/no-explicit-any */
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {getAllViews, ViewStore} from './use-query.js';
import type {AdvancedQuery} from '../../zql/src/query/query-internal.js';

function newMockQuery(query: string): AdvancedQuery<any, any> {
  return {
    hash() {
      return query;
    },
    materialize() {
      return newView();
    },
    format: {singular: false},
  } as unknown as AdvancedQuery<any, any>;
}

function newView() {
  return {
    addListener(_cb: () => void) {},
    destroy() {},
  };
}

describe('ViewStore', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  describe('duplicate queries', () => {
    test('duplicate queries do not create duplicate views', () => {
      const viewStore = new ViewStore();

      const view1 = viewStore.getView('client1', newMockQuery('query1'), true);
      const view2 = viewStore.getView('client1', newMockQuery('query1'), true);

      expect(view1).toBe(view2);

      expect(viewStore[getAllViews]().size).toBe(1);
    });

    test('removing a duplicate query does not destroy the shared view', () => {
      const viewStore = new ViewStore();

      const view1 = viewStore.getView('client1', newMockQuery('query1'), true);
      const view2 = viewStore.getView('client1', newMockQuery('query1'), true);

      const cleanup1 = view1.subscribeReactInternals(() => {});
      view2.subscribeReactInternals(() => {});

      cleanup1();

      vi.advanceTimersByTime(100);

      expect(viewStore[getAllViews]().size).toBe(1);
    });
  });

  describe('destruction', () => {
    test('removing all duplicate queries destroys the shared view', () => {
      const viewStore = new ViewStore();

      const view1 = viewStore.getView('client1', newMockQuery('query1'), true);
      const view2 = viewStore.getView('client1', newMockQuery('query1'), true);

      const cleanup1 = view1.subscribeReactInternals(() => {});
      const cleanup2 = view2.subscribeReactInternals(() => {});

      cleanup1();
      cleanup2();

      vi.advanceTimersByTime(100);

      expect(viewStore[getAllViews]().size).toBe(0);
    });

    test('removing a unique query destroys the view', () => {
      const viewStore = new ViewStore();

      const view = viewStore.getView('client1', newMockQuery('query1'), true);

      const cleanup = view.subscribeReactInternals(() => {});
      cleanup();

      vi.advanceTimersByTime(100);
      expect(viewStore[getAllViews]().size).toBe(0);
    });

    test('view destruction is delayed via setTimeout', () => {
      const viewStore = new ViewStore();

      const view = viewStore.getView('client1', newMockQuery('query1'), true);

      const cleanup = view.subscribeReactInternals(() => {});
      cleanup();

      vi.advanceTimersByTime(5);
      expect(viewStore[getAllViews]().size).toBe(1);
      vi.advanceTimersByTime(10);

      expect(viewStore[getAllViews]().size).toBe(0);
    });

    test('subscribing to a view scheduled for cleanup prevents the cleanup', () => {
      const viewStore = new ViewStore();
      const view = viewStore.getView('client1', newMockQuery('query1'), true);
      const cleanup = view.subscribeReactInternals(() => {});

      cleanup();

      expect(viewStore[getAllViews]().size).toBe(1);
      vi.advanceTimersByTime(5);
      expect(viewStore[getAllViews]().size).toBe(1);

      const view2 = viewStore.getView('client1', newMockQuery('query1'), true);
      const cleanup2 = view.subscribeReactInternals(() => {});
      vi.advanceTimersByTime(100);

      expect(viewStore[getAllViews]().size).toBe(1);

      expect(view2).toBe(view);

      cleanup2();
      vi.advanceTimersByTime(100);
      expect(viewStore[getAllViews]().size).toBe(0);
    });

    test('destroying the same underlying view twice is a no-op', () => {
      const viewStore = new ViewStore();
      const view = viewStore.getView('client1', newMockQuery('query1'), true);
      const cleanup = view.subscribeReactInternals(() => {});

      cleanup();
      cleanup();

      vi.advanceTimersByTime(100);
      expect(viewStore[getAllViews]().size).toBe(0);
    });
  });

  describe('clients', () => {
    test('the same query for different clients results in different views', () => {
      const viewStore = new ViewStore();

      const view1 = viewStore.getView('client1', newMockQuery('query1'), true);
      const view2 = viewStore.getView('client2', newMockQuery('query1'), true);

      expect(view1).not.toBe(view2);
    });
  });
});
