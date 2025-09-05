import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {Suspense} from 'react';
import {createRoot, type Root} from 'react-dom/client';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import {type AbstractQuery} from '../../zql/src/query/query-impl.ts';
import type {ResultType} from '../../zql/src/query/typed-view.ts';
import {
  getAllViewsSizeForTesting,
  ViewStore,
  useSuspenseQuery,
} from './use-query.tsx';
import {ZeroProvider} from './zero-provider.tsx';
import type {Zero} from '../../zero-client/src/client/zero.ts';
import type {ErroredQuery} from '../../zero-protocol/src/custom-queries.ts';

function newMockQuery(
  query: string,
  singular = false,
): AbstractQuery<Schema, string> {
  const ret = {
    hash() {
      return query;
    },
    format: {singular},
  } as unknown as AbstractQuery<Schema, string>;
  return ret;
}

function newMockZero(clientID: string): Zero<Schema> {
  const view = newView();

  return {
    clientID,
    materialize: vi.fn().mockImplementation(() => view),
  } as unknown as Zero<Schema>;
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
        newMockZero('client1'),
        newMockQuery('query1'),
        true,
        'forever',
      );
      const view2 = viewStore.getView(
        newMockZero('client1'),
        newMockQuery('query1'),
        true,
        'forever',
      );

      expect(view1).toBe(view2);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });

    test('removing a duplicate query does not destroy the shared view', () => {
      const viewStore = new ViewStore();

      const view1 = viewStore.getView(
        newMockZero('client1'),
        newMockQuery('query1'),
        true,
        'forever',
      );
      const view2 = viewStore.getView(
        newMockZero('client1'),
        newMockQuery('query1'),
        true,
        'forever',
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
      const zero = newMockZero('client1');
      const materializeSpy = vi.spyOn(zero, 'materialize');
      const view1 = viewStore.getView(zero, q1, true, '1s');

      const updateTTLSpy = vi.spyOn(view1, 'updateTTL');
      expect(materializeSpy).toHaveBeenCalledTimes(1);
      expect(materializeSpy.mock.calls[0][0]).toBe(q1);
      expect(materializeSpy.mock.calls[0][1]).toEqual({ttl: '1s'});

      const q2 = newMockQuery('query1');
      const zeroClient2 = newMockZero('client1');
      const materializeSpy2 = vi.spyOn(zero, 'materialize');
      const view2 = viewStore.getView(zeroClient2, q2, true, '1m');
      expect(view1).toBe(view2);

      // Same query hash and client id so only one view. Should have called
      // updateTTL on the existing one.
      expect(materializeSpy2).not.toHaveBeenCalled();
      expect(updateTTLSpy).toHaveBeenCalledExactlyOnceWith('1m');

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });

    test('Using the same query with same TTL but different representation', () => {
      const viewStore = new ViewStore();

      const q1 = newMockQuery('query1');
      const zero = newMockZero('client1');
      const materializeSpy = vi.spyOn(zero, 'materialize');
      const view1 = viewStore.getView(zero, q1, true, '60s');
      const updateTTLSpy = vi.spyOn(view1, 'updateTTL');
      expect(materializeSpy).toHaveBeenCalledTimes(1);

      const q2 = newMockQuery('query1');
      const view2 = viewStore.getView(newMockZero('client1'), q2, true, '1m');
      expect(view1).toBe(view2);

      expect(updateTTLSpy).toHaveBeenCalledExactlyOnceWith('1m');

      const q3 = newMockQuery('query1');
      const view3 = viewStore.getView(newMockZero('client1'), q3, true, 60_000);

      expect(view1).toBe(view3);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });
  });

  describe('destruction', () => {
    test('removing all duplicate queries destroys the shared view', () => {
      const viewStore = new ViewStore();

      const view1 = viewStore.getView(
        newMockZero('client1'),
        newMockQuery('query1'),
        true,
        'forever',
      );
      const view2 = viewStore.getView(
        newMockZero('client1'),
        newMockQuery('query1'),
        true,
        'forever',
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
        newMockZero('client1'),
        newMockQuery('query1'),
        true,
        'forever',
      );

      const cleanup = view.subscribeReactInternals(() => {});
      cleanup();

      vi.advanceTimersByTime(100);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });

    test('view destruction is delayed via setTimeout', () => {
      const viewStore = new ViewStore();

      const view = viewStore.getView(
        newMockZero('client1'),
        newMockQuery('query1'),
        true,
        'forever',
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
        newMockZero('client1'),
        newMockQuery('query1'),
        true,
        'forever',
      );
      const cleanup = view.subscribeReactInternals(() => {});

      cleanup();

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
      vi.advanceTimersByTime(5);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);

      const view2 = viewStore.getView(
        newMockZero('client1'),
        newMockQuery('query1'),
        true,
        'forever',
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
        newMockZero('client1'),
        newMockQuery('query1'),
        true,
        'forever',
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
        newMockZero('client1'),
        newMockQuery('query1'),
        true,
        'forever',
      );
      const view2 = viewStore.getView(
        newMockZero('client2'),
        newMockQuery('query1'),
        true,
        'forever',
      );

      expect(view1).not.toBe(view2);
    });
  });

  describe('collapse multiple empty on data', () => {
    test('plural', () => {
      const viewStore = new ViewStore();
      const q = newMockQuery('query1');
      const zero = newMockZero('client1');
      const materializeSpy = vi.spyOn(zero, 'materialize');
      const view = viewStore.getView(zero, q, true, 'forever');

      expect(materializeSpy).toHaveBeenCalledTimes(1);
      const {listeners} = materializeSpy.mock.results[0].value as unknown as {
        listeners: Set<(...args: unknown[]) => void>;
      };

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
      const zero = newMockZero('client1');
      const materializeSpy = vi.spyOn(zero, 'materialize');
      const view = viewStore.getView(zero, q, true, 'forever');

      expect(materializeSpy).toHaveBeenCalledTimes(1);
      const {listeners} = materializeSpy.mock.results[0].value as unknown as {
        listeners: Set<(...args: unknown[]) => void>;
      };

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

describe('useSuspenseQuery', () => {
  let root: Root;
  let element: HTMLDivElement;
  let unique: number = 0;

  beforeEach(() => {
    vi.useRealTimers();
    element = document.createElement('div');
    document.body.appendChild(element);
    root = createRoot(element);
    unique++;
  });

  afterEach(() => {
    document.body.removeChild(element);
    root.unmount();
  });

  test('suspendsUntil complete', async () => {
    const q = newMockQuery('query' + unique);
    const zero = newMockZero('client' + unique);
    const materializeSpy = vi.spyOn(zero, 'materialize');

    function Comp() {
      const [data] = useSuspenseQuery(q, {suspendUntil: 'complete'});
      return <div>{JSON.stringify(data)}</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Suspense fallback={<>loading</>}>
          <Comp />
        </Suspense>
      </ZeroProvider>,
    );

    await expect.poll(() => element.textContent).toBe('loading');

    const view = materializeSpy.mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    view.listeners.forEach(cb => cb([{a: 1}], 'complete'));
    await expect.poll(() => element.textContent).toBe('[{"a":1}]');
  });

  test('suspendsUntil complete, already complete', async () => {
    const q = newMockQuery('query' + unique);
    const zero = newMockZero('client' + unique);
    const materializeSpy = vi.spyOn(zero, 'materialize');

    function Comp({label}: {label: string}) {
      const [data] = useSuspenseQuery(q, {suspendUntil: 'complete'});
      return <div>{`${label}:${JSON.stringify(data)}`}</div>;
    }

    root.render(
      <ZeroProvider zero={zero} key="1">
        <Suspense fallback={<>loading</>}>
          <Comp label="1" />
        </Suspense>
      </ZeroProvider>,
    );

    await expect.poll(() => element.textContent).toBe('loading');

    const view = materializeSpy.mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    view.listeners.forEach(cb => cb([{a: 1}], 'complete'));
    await expect.poll(() => element.textContent).toBe('1:[{"a":1}]');

    root.render(
      <ZeroProvider zero={zero} key="2">
        <Suspense fallback={<>loading</>}>
          <Comp label="2" />
        </Suspense>
      </ZeroProvider>,
    );

    await expect.poll(() => element.textContent).toBe('2:[{"a":1}]');
  });

  test('suspendsUntil partial, partial array before complete', async () => {
    const q = newMockQuery('query' + unique);
    const zero = newMockZero('client' + unique);
    const materializeSpy = vi.spyOn(zero, 'materialize');

    function Comp() {
      const [data] = useSuspenseQuery(q, {suspendUntil: 'partial'});
      return <div>{JSON.stringify(data)}</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Suspense fallback={<>loading</>}>
          <Comp />
        </Suspense>
      </ZeroProvider>,
    );

    await expect.poll(() => element.textContent).toBe('loading');

    const view = materializeSpy.mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    view.listeners.forEach(cb => cb([{a: 1}], 'unknown'));
    await expect.poll(() => element.textContent).toBe('[{"a":1}]');
  });

  test('suspendsUntil partial, already partial array before complete', async () => {
    const q = newMockQuery('query' + unique);
    const zero = newMockZero('client' + unique);
    const materializeSpy = vi.spyOn(zero, 'materialize');

    function Comp({label}: {label: string}) {
      const [data] = useSuspenseQuery(q, {suspendUntil: 'partial'});
      return <div>{`${label}:${JSON.stringify(data)}`}</div>;
    }

    root.render(
      <ZeroProvider zero={zero} key="1">
        <Suspense fallback={<>loading</>}>
          <Comp label="1" />
        </Suspense>
      </ZeroProvider>,
    );

    await expect.poll(() => element.textContent).toBe('loading');

    const view = materializeSpy.mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    view.listeners.forEach(cb => cb([{a: 1}], 'unknown'));
    await expect.poll(() => element.textContent).toBe('1:[{"a":1}]');

    root.render(
      <ZeroProvider zero={zero} key="2">
        <Suspense fallback={<>loading</>}>
          <Comp label="2" />
        </Suspense>
      </ZeroProvider>,
    );

    await expect.poll(() => element.textContent).toBe('2:[{"a":1}]');
  });

  test('suspendsUntil partial singular, defined value before complete', async () => {
    const q = newMockQuery('query' + unique, true);
    const zero = newMockZero('client' + unique);
    const materializeSpy = vi.spyOn(zero, 'materialize');

    function Comp() {
      const [data] = useSuspenseQuery(q, {suspendUntil: 'partial'});
      return <div>{JSON.stringify(data)}</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Suspense fallback={<>loading</>}>
          <Comp />
        </Suspense>
      </ZeroProvider>,
    );

    await expect.poll(() => element.textContent).toBe('loading');

    const view = materializeSpy.mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    view.listeners.forEach(cb => cb({a: 1}, 'unknown'));
    await expect.poll(() => element.textContent).toBe('{"a":1}');
  });

  test('suspendUntil partial, complete with empty array', async () => {
    const q = newMockQuery('query' + unique);
    const zero = newMockZero('client' + unique);
    const materializeSpy = vi.spyOn(zero, 'materialize');

    function Comp() {
      const [data] = useSuspenseQuery(q, {suspendUntil: 'partial'});
      return <div>{JSON.stringify(data)}</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Suspense fallback={<>loading</>}>
          <Comp />
        </Suspense>
      </ZeroProvider>,
    );

    await expect.poll(() => element.textContent).toBe('loading');

    const view = materializeSpy.mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    view.listeners.forEach(cb => cb([], 'complete'));
    await expect.poll(() => element.textContent).toBe('[]');
  });

  test('suspendUntil partial, complete with undefined', async () => {
    const q = newMockQuery('query' + unique, true);
    const zero = newMockZero('client' + unique);
    const materializeSpy = vi.spyOn(zero, 'materialize');

    function Comp() {
      const [data] = useSuspenseQuery(q, {suspendUntil: 'partial'});
      return (
        <div>
          {data === undefined ? 'singularUndefined' : JSON.stringify(data)}
        </div>
      );
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Suspense fallback={<>loading</>}>
          <Comp />
        </Suspense>
      </ZeroProvider>,
    );

    await expect.poll(() => element.textContent).toBe('loading');

    const view = materializeSpy.mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    view.listeners.forEach(cb => cb(undefined, 'complete'));
    await expect.poll(() => element.textContent).toBe('singularUndefined');
  });

  describe('error handling', () => {
    test('plural query returns error details when query fails', async () => {
      const q = newMockQuery('query' + unique);
      const zero = newMockZero('client' + unique);
      const materializeSpy = vi.spyOn(zero, 'materialize');

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'complete'});
        return (
          <div>
            {details.type === 'error'
              ? `Error: ${details.error?.queryName || 'Unknown error'}`
              : JSON.stringify(data)}
          </div>
        );
      }

      root.render(
        <ZeroProvider zero={zero}>
          <Suspense fallback={<>loading</>}>
            <Comp />
          </Suspense>
        </ZeroProvider>,
      );

      await expect.poll(() => element.textContent).toBe('loading');

      const view = materializeSpy.mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      const error: ErroredQuery = {
        error: 'app',
        id: 'test-error-1',
        name: 'Query failed',
        details: {reason: 'Invalid syntax'},
      };
      view.listeners.forEach(cb => cb([], 'error', error));
      await expect.poll(() => element.textContent).toBe('Error: Query failed');
    });

    test('singular query returns error details when query fails', async () => {
      const q = newMockQuery('query' + unique, true);
      const zero = newMockZero('client' + unique);
      const materializeSpy = vi.spyOn(zero, 'materialize');

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'complete'});
        return (
          <div>
            {details.type === 'error'
              ? `Error: ${details.error?.queryName || 'Unknown error'}`
              : JSON.stringify(data)}
          </div>
        );
      }

      root.render(
        <ZeroProvider zero={zero}>
          <Suspense fallback={<>loading</>}>
            <Comp />
          </Suspense>
        </ZeroProvider>,
      );

      await expect.poll(() => element.textContent).toBe('loading');

      const view = materializeSpy.mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      const error: ErroredQuery = {
        error: 'app',
        id: 'test-error-2',
        name: 'Query failed',
        details: {reason: 'Invalid syntax'},
      };
      view.listeners.forEach(cb => cb(undefined, 'error', error));
      await expect.poll(() => element.textContent).toBe('Error: Query failed');
    });

    test('query transitions from error to success state', async () => {
      const q = newMockQuery('query' + unique);
      const zero = newMockZero('client' + unique);
      const materializeSpy = vi.spyOn(zero, 'materialize');

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'partial'});
        return (
          <div>
            {details.type === 'error'
              ? `Error: ${details.error?.queryName}`
              : `Data: ${JSON.stringify(data)}, Type: ${details.type}`}
          </div>
        );
      }

      root.render(
        <ZeroProvider zero={zero}>
          <Suspense fallback={<>loading</>}>
            <Comp />
          </Suspense>
        </ZeroProvider>,
      );

      await expect.poll(() => element.textContent).toBe('loading');

      const view = materializeSpy.mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      // First emit error
      const error: ErroredQuery = {
        error: 'app',
        id: 'temp-failure',
        name: 'Temporary failure',
        details: {},
      };
      view.listeners.forEach(cb => cb([], 'error', error));
      await expect
        .poll(() => element.textContent)
        .toBe('Error: Temporary failure');

      // Then emit success
      view.listeners.forEach(cb => cb([{a: 1}], 'complete'));
      await expect
        .poll(() => element.textContent)
        .toBe('Data: [{"a":1}], Type: complete');
    });

    test('query can return partial data with error state', async () => {
      const q = newMockQuery('query' + unique);
      const zero = newMockZero('client' + unique);
      const materializeSpy = vi.spyOn(zero, 'materialize');

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'partial'});
        return (
          <div>
            Data: {JSON.stringify(data)}, Type: {details.type}, Error:{' '}
            {details.type === 'error' ? details.error?.queryName : 'none'}
          </div>
        );
      }

      root.render(
        <ZeroProvider zero={zero}>
          <Suspense fallback={<>loading</>}>
            <Comp />
          </Suspense>
        </ZeroProvider>,
      );

      await expect.poll(() => element.textContent).toBe('loading');

      const view = materializeSpy.mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      const error: ErroredQuery = {
        error: 'app',
        id: 'partial-failure',
        name: 'Partial failure',
        details: {message: 'Some items failed'},
      };
      view.listeners.forEach(cb => cb([{a: 1}], 'error', error));
      await expect
        .poll(() => element.textContent)
        .toBe('Data: [{"a":1}], Type: error, Error: Partial failure');
    });

    test('error state without suspense returns immediately', async () => {
      const q = newMockQuery('query' + unique);
      const zero = newMockZero('client' + unique);
      const materializeSpy = vi.spyOn(zero, 'materialize');

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'partial'});
        return (
          <div>
            {details.type === 'error'
              ? `Error state: ${details.error?.queryName}`
              : `Data: ${JSON.stringify(data)}`}
          </div>
        );
      }

      root.render(
        <ZeroProvider zero={zero}>
          <Suspense fallback={<>loading</>}>
            <Comp />
          </Suspense>
        </ZeroProvider>,
      );

      await expect.poll(() => element.textContent).toBe('loading');

      const view = materializeSpy.mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      // Emit error immediately
      const error: ErroredQuery = {
        error: 'zero',
        id: 'immediate-error',
        name: 'Immediate error',
        details: {},
      };
      view.listeners.forEach(cb => cb([], 'error', error));
      await expect
        .poll(() => element.textContent)
        .toBe('Error state: Immediate error');
    });

    test('HTTP error type is handled correctly', async () => {
      const q = newMockQuery('query' + unique);
      const zero = newMockZero('client' + unique);
      const materializeSpy = vi.spyOn(zero, 'materialize');

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'partial'});
        return (
          <div>
            {details.type === 'error' && details.error?.type === 'http'
              ? `HTTP Error: ${details.error.status}`
              : JSON.stringify(data)}
          </div>
        );
      }

      root.render(
        <ZeroProvider zero={zero}>
          <Suspense fallback={<>loading</>}>
            <Comp />
          </Suspense>
        </ZeroProvider>,
      );

      await expect.poll(() => element.textContent).toBe('loading');

      const view = materializeSpy.mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      const httpError: ErroredQuery = {
        error: 'http',
        status: 500,
        id: 'q1',
        name: 'q1',
        details: 'Internal Server Error',
      };
      view.listeners.forEach(cb => cb([], 'error', httpError));
      await expect.poll(() => element.textContent).toBe('HTTP Error: 500');
    });
  });
});
