import {Suspense, useState} from 'react';
import {createRoot, type Root} from 'react-dom/client';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  test,
  vi,
  type Mock,
} from 'vitest';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {queryInternalsTag, type QueryImpl} from './bindings.ts';
import {
  getAllViewsSizeForTesting,
  type MaybeSelectedQueryResult,
  type SelectedQueryResult,
  useQuery,
  useSuspenseQuery,
  ViewStore,
} from './use-query.tsx';
import {ZeroProvider} from './zero-provider.tsx';
import {
  createSchema,
  number,
  string,
  table,
  type CustomMutatorDefs,
  type ErroredQuery,
  type Query,
  type QueryResultDetails,
  type ReadonlyJSONValue,
  type ResultType,
  type Schema,
  type Zero,
} from './zero.ts';

function newMockQuery(query: string, singular = false): Query<string, Schema> {
  const ret = {
    [queryInternalsTag]: true,
    hash() {
      return query;
    },
    format: {singular},
  } as unknown as QueryImpl<string, Schema>;
  return ret;
}

function newMockZero<
  MD extends CustomMutatorDefs | undefined = undefined,
  C = unknown,
>(clientID: string): Zero<Schema, MD, C> {
  const view = newView();
  return {
    clientID,
    materialize: vi.fn().mockImplementation(() => view),
  } as unknown as Zero<Schema, MD, C>;
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

      const zero1 = newMockZero('client1');
      const view1 = viewStore.getView(
        zero1,
        newMockQuery('query1'),
        true,
        'forever',
      );

      const zero2 = newMockZero('client1');
      const view2 = viewStore.getView(
        zero2,
        newMockQuery('query1'),
        true,
        'forever',
      );

      expect(view1).toBe(view2);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });

    test('removing a duplicate query does not destroy the shared view', () => {
      const viewStore = new ViewStore();

      const zero1 = newMockZero('client1');
      const view1 = viewStore.getView(
        zero1,
        newMockQuery('query1'),
        true,
        'forever',
      );
      const zero2 = newMockZero('client1');
      const view2 = viewStore.getView(
        zero2,
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
      const view1 = viewStore.getView(zero, q1, true, '1s');

      const updateTTLSpy = vi.spyOn(view1, 'updateTTL');
      expect(zero.materialize).toHaveBeenCalledTimes(1);
      expect(vi.mocked(zero.materialize).mock.calls[0][0]).toBe(q1);
      expect(vi.mocked(zero.materialize).mock.calls[0][1]).toEqual({ttl: '1s'});

      const q2 = newMockQuery('query1');
      const zeroClient2 = newMockZero('client1');
      const view2 = viewStore.getView(zeroClient2, q2, true, '1m');
      expect(view1).toBe(view2);

      // Same query hash and client id so only one view. Should have called
      // updateTTL on the existing one.
      expect(zeroClient2.materialize).not.toHaveBeenCalled();
      expect(updateTTLSpy).toHaveBeenCalledExactlyOnceWith('1m');

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });

    test('Using the same query with same TTL but different representation', () => {
      const viewStore = new ViewStore();

      const q1 = newMockQuery('query1');
      const zero = newMockZero('client1');
      const view1 = viewStore.getView(zero, q1, true, '60s');
      const updateTTLSpy = vi.spyOn(view1, 'updateTTL');
      expect(zero.materialize).toHaveBeenCalledTimes(1);

      const q2 = newMockQuery('query1');
      const zeroClient2 = newMockZero('client1');
      const view2 = viewStore.getView(zeroClient2, q2, true, '1m');
      expect(view1).toBe(view2);

      expect(updateTTLSpy).toHaveBeenCalledExactlyOnceWith('1m');

      const q3 = newMockQuery('query1');
      const zeroClient3 = newMockZero('client1');
      const view3 = viewStore.getView(zeroClient3, q3, true, 60_000);

      expect(view1).toBe(view3);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });
  });

  describe('destruction', () => {
    test('removing all duplicate queries destroys the shared view', () => {
      const viewStore = new ViewStore();

      const zero1 = newMockZero('client1');
      const view1 = viewStore.getView(
        zero1,
        newMockQuery('query1'),
        true,
        'forever',
      );

      const zero2 = newMockZero('client1');
      const view2 = viewStore.getView(
        zero2,
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

      const zero = newMockZero('client1');
      const view = viewStore.getView(
        zero,
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

      const zero = newMockZero('client1');
      const view = viewStore.getView(
        zero,
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
      const zero1 = newMockZero('client1');
      const view = viewStore.getView(
        zero1,
        newMockQuery('query1'),
        true,
        'forever',
      );
      const cleanup = view.subscribeReactInternals(() => {});

      cleanup();

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
      vi.advanceTimersByTime(5);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);

      const zero2 = newMockZero('client1');
      const view2 = viewStore.getView(
        zero2,
        newMockQuery('query1'),
        true,
        'forever',
      );
      const cleanup2 = view2.subscribeReactInternals(() => {});
      vi.advanceTimersByTime(100);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);

      expect(view2).toBe(view);

      cleanup2();
      vi.advanceTimersByTime(100);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });

    test('destroying the same underlying view twice is a no-op', () => {
      const viewStore = new ViewStore();
      const zero = newMockZero('client1');
      const view = viewStore.getView(
        zero,
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

      const zero1 = newMockZero('client1');
      const view1 = viewStore.getView(
        zero1,
        newMockQuery('query1'),
        true,
        'forever',
      );

      const zero2 = newMockZero('client2');
      const view2 = viewStore.getView(
        zero2,
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
      const view = viewStore.getView(zero, q, true, 'forever');

      expect(zero.materialize).toHaveBeenCalledTimes(1);
      const {listeners} = vi.mocked(zero.materialize).mock.results[0]
        .value as unknown as {
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
      const view = viewStore.getView(zero, q, true, 'forever');

      expect(zero.materialize).toHaveBeenCalledTimes(1);
      const {listeners} = vi.mocked(zero.materialize).mock.results[0]
        .value as unknown as {
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

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    view.listeners.forEach(cb => cb([{a: 1}], 'complete'));
    await expect.poll(() => element.textContent).toBe('[{"a":1}]');
  });

  test('suspendsUntil complete, already complete', async () => {
    const q = newMockQuery('query' + unique);
    const zero = newMockZero('client' + unique);

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

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
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

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    view.listeners.forEach(cb => cb([{a: 1}], 'unknown'));
    await expect.poll(() => element.textContent).toBe('[{"a":1}]');
  });

  test('suspendsUntil partial, already partial array before complete', async () => {
    const q = newMockQuery('query' + unique);
    const zero = newMockZero('client' + unique);

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

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
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

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    view.listeners.forEach(cb => cb({a: 1}, 'unknown'));
    await expect.poll(() => element.textContent).toBe('{"a":1}');
  });

  test('suspendUntil partial, complete with empty array', async () => {
    const q = newMockQuery('query' + unique);
    const zero = newMockZero('client' + unique);

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

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    view.listeners.forEach(cb => cb([], 'complete'));
    await expect.poll(() => element.textContent).toBe('[]');
  });

  test('suspendUntil partial, complete with undefined', async () => {
    const q = newMockQuery('query' + unique, true);
    const zero = newMockZero('client' + unique);

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

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    view.listeners.forEach(cb => cb(undefined, 'complete'));
    await expect.poll(() => element.textContent).toBe('singularUndefined');
  });

  describe('error handling', () => {
    const getErroredQuery = (
      message: string,
      details?: ReadonlyJSONValue,
    ): ErroredQuery => ({
      error: 'app',
      id: 'test-error-1',
      name: 'testName1',
      message,
      ...(details ? {details} : {}),
    });

    test('plural query returns error details when query fails', async () => {
      const q = newMockQuery('query' + unique);
      const zero = newMockZero('client' + unique);

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'complete'});
        return (
          <div>
            {details.type === 'error'
              ? `Error: ${details.error?.message || 'Unknown error'}`
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

      const view = vi.mocked(zero.materialize).mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      const error = getErroredQuery('Query failed');
      view.listeners.forEach(cb => cb([], 'error', error));
      await expect.poll(() => element.textContent).toBe('Error: Query failed');
    });

    test('singular query returns error details when query fails', async () => {
      const q = newMockQuery('query' + unique, true);
      const zero = newMockZero('client' + unique);

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'complete'});
        return (
          <div>
            {details.type === 'error'
              ? `Error: ${details.error?.message || 'Unknown error'}`
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

      const view = vi.mocked(zero.materialize).mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      const error = getErroredQuery('Query failed', {reason: 'Invalid syntax'});
      view.listeners.forEach(cb => cb(undefined, 'error', error));
      await expect.poll(() => element.textContent).toBe('Error: Query failed');
    });

    test('query transitions from error to success state', async () => {
      const q = newMockQuery('query' + unique);
      const zero = newMockZero('client' + unique);

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'partial'});
        return (
          <div>
            {details.type === 'error'
              ? `Error: ${details.error?.message} ${JSON.stringify(details.error?.details)}`
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

      const view = vi.mocked(zero.materialize).mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      // First emit error
      const error = getErroredQuery('Temporary failure', {some: 'detail'});
      view.listeners.forEach(cb => cb([], 'error', error));
      await expect
        .poll(() => element.textContent)
        .toBe('Error: Temporary failure {"some":"detail"}');

      // Then emit success
      view.listeners.forEach(cb => cb([{a: 1}], 'complete'));
      await expect
        .poll(() => element.textContent)
        .toBe('Data: [{"a":1}], Type: complete');
    });

    test('query can return partial data with error state', async () => {
      const q = newMockQuery('query' + unique);
      const zero = newMockZero('client' + unique);

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'partial'});
        return (
          <div>
            Data: {JSON.stringify(data)}, Type: {details.type}, Error:{' '}
            {details.type === 'error' ? details.error?.message : 'none'}
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

      const view = vi.mocked(zero.materialize).mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      const error = getErroredQuery('Partial failure', {
        message: 'Some items failed',
      });
      view.listeners.forEach(cb => cb([{a: 1}], 'error', error));
      await expect
        .poll(() => element.textContent)
        .toBe('Data: [{"a":1}], Type: error, Error: Partial failure');
    });

    test('error state without suspense returns immediately', async () => {
      const q = newMockQuery('query' + unique);
      const zero = newMockZero('client' + unique);

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'partial'});
        return (
          <div>
            {details.type === 'error'
              ? `Error state: ${details.error?.message}`
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

      const view = vi.mocked(zero.materialize).mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      // Emit error immediately
      const error = getErroredQuery('Immediate error');
      view.listeners.forEach(cb => cb([], 'error', error));
      await expect
        .poll(() => element.textContent)
        .toBe('Error state: Immediate error');
    });

    test('parse error type is handled correctly', async () => {
      const q = newMockQuery('query' + unique);
      const zero = newMockZero('client' + unique);

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'partial'});
        return (
          <div>
            {details.type === 'error' && details.error?.type === 'parse'
              ? `Parse Error: ${details.error.message}`
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

      const view = vi.mocked(zero.materialize).mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      const parseError: ErroredQuery = {
        error: 'parse',
        id: 'q1',
        name: 'q1',
        message: 'Parse error',
        details: {message: 'Invalid syntax'},
      };
      view.listeners.forEach(cb => cb([], 'error', parseError));
      await expect
        .poll(() => element.textContent)
        .toBe('Parse Error: Parse error');
    });

    test('retry function retries the query after error', async () => {
      const q = newMockQuery('query' + unique);
      const zero = newMockZero('client' + unique);

      let retryFn: (() => void) | undefined;
      let refetchFn: (() => void) | undefined;

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'partial'});

        // Store retry function if available
        if (details.type === 'error' && details.retry) {
          retryFn = details.retry;
          refetchFn = details.refetch;
        }

        return (
          <div>
            {details.type === 'error'
              ? `Error: ${details.error?.message}`
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

      // First materialize call
      const firstView = vi.mocked(zero.materialize).mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
        destroy: Mock;
      };

      // Add destroy spy
      firstView.destroy = vi.fn(() => {
        firstView.listeners.clear();
      });

      // Emit error
      const error = getErroredQuery('Query failed', {message: 'Network error'});
      firstView.listeners.forEach(cb => cb([], 'error', error));
      await expect.poll(() => element.textContent).toBe('Error: Query failed');

      // Verify retry function is available
      expect(retryFn).toBeDefined();
      expect(refetchFn).toEqual(retryFn);

      // Call retry
      retryFn!();

      // Verify that the old view was destroyed
      expect(firstView.destroy).toHaveBeenCalledTimes(1);

      // Verify that materialize was called again
      expect(zero.materialize).toHaveBeenCalledTimes(2);

      // Second materialize call creates new view
      const secondView = vi.mocked(zero.materialize).mock.results[1].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      // Emit successful data on retry
      secondView.listeners.forEach(cb => cb([{a: 1, b: 2}], 'complete'));
      await expect
        .poll(() => element.textContent)
        .toBe('Data: [{"a":1,"b":2}], Type: complete');
    });

    test('retry function can be called multiple times', async () => {
      const q = newMockQuery('query' + unique, true);
      const zero = newMockZero('client' + unique);

      let retryFn: (() => void) | undefined;

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'partial'});

        // Store retry function if available
        if (details.type === 'error' && details.retry) {
          retryFn = details.retry;
        }

        return (
          <div>
            {details.type === 'error'
              ? `Error: ${details.error?.message} ${JSON.stringify(details.error?.details)}`
              : data !== undefined
                ? `Data: ${JSON.stringify(data)}`
                : 'No data'}
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

      // First materialize call
      const firstView = vi.mocked(zero.materialize).mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
        destroy: Mock;
      };
      firstView.destroy = vi.fn(() => {
        firstView.listeners.clear();
      });

      // First error
      const error1 = getErroredQuery('First failure', {
        message: 'Network error',
      });
      firstView.listeners.forEach(cb => cb(undefined, 'error', error1));
      await expect
        .poll(() => element.textContent)
        .toBe('Error: First failure {"message":"Network error"}');

      // First retry
      retryFn!();
      expect(firstView.destroy).toHaveBeenCalledTimes(1);
      expect(zero.materialize).toHaveBeenCalledTimes(2);

      // Second view also fails
      const secondView = vi.mocked(zero.materialize).mock.results[1].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
        destroy: Mock;
      };
      secondView.destroy = vi.fn(() => {
        secondView.listeners.clear();
      });

      const error2 = getErroredQuery('Second failure', {
        message: 'Service unavailable',
      });
      secondView.listeners.forEach(cb => cb(undefined, 'error', error2));
      await expect
        .poll(() => element.textContent)
        .toBe('Error: Second failure {"message":"Service unavailable"}');

      // Second retry
      retryFn!();
      expect(secondView.destroy).toHaveBeenCalledTimes(1);
      expect(zero.materialize).toHaveBeenCalledTimes(3);

      // Third view succeeds
      const thirdView = vi.mocked(zero.materialize).mock.results[2].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };
      thirdView.listeners.forEach(cb => cb({success: true}, 'complete'));
      await expect
        .poll(() => element.textContent)
        .toBe('Data: {"success":true}');
    });

    test('retry function is undefined when query is not in error state', async () => {
      const q = newMockQuery('query' + unique);
      const zero = newMockZero('client' + unique);

      let capturedDetails: QueryResultDetails | undefined;

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'partial'});
        capturedDetails = details;

        return (
          <div>
            Data: {JSON.stringify(data)}, Type: {details.type}
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

      const view = vi.mocked(zero.materialize).mock.results[0].value as {
        listeners: Set<
          (snap: unknown, resultType: ResultType, error?: ErroredQuery) => void
        >;
      };

      // Emit successful data (not error state)
      view.listeners.forEach(cb => cb([{a: 1}], 'complete'));
      await expect
        .poll(() => element.textContent)
        .toBe('Data: [{"a":1}], Type: complete');

      // Verify that retry is not available when not in error state
      expect(capturedDetails?.type).toBe('complete');
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      expect((capturedDetails as any).retry).toBeUndefined();
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      expect((capturedDetails as any).retry).toBeUndefined();
    });
  });

  describe('view management after fix', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    test('concurrent getView calls ideally share the same view', async () => {
      const viewStore = new ViewStore();
      const zero = newMockZero('client1');
      const query = newMockQuery('query1');

      // Simulate concurrent calls
      const promises = Array.from({length: 10}, () =>
        Promise.resolve().then(() =>
          viewStore.getView(zero, query, true, 'forever'),
        ),
      );

      const views = await Promise.all(promises);

      // Check if views are shared (ideal case)
      const uniqueViews = new Set(views);
      expect(uniqueViews.size).toBe(1);

      // Subscribe to all views
      const cleanups = views.map(v => v.subscribeReactInternals(() => {}));

      // Clean up all
      cleanups.forEach(cleanup => cleanup());
      vi.advanceTimersByTime(100);

      // Verify all views are eventually cleaned up
      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });

    test('rapid mount/unmount/remount reuses view when possible', () => {
      const viewStore = new ViewStore();
      const zero = newMockZero('client1');
      const query = newMockQuery('query1');

      const views = [];

      // Simulate React strict mode double-mounting
      for (let i = 0; i < 5; i++) {
        const view = viewStore.getView(zero, query, true, 'forever');
        views.push(view);
        const cleanup = view.subscribeReactInternals(() => {});

        // Immediate cleanup (unmount)
        cleanup();

        // Immediate remount before timeout
        const view2 = viewStore.getView(zero, query, true, 'forever');
        views.push(view2);
        const cleanup2 = view2.subscribeReactInternals(() => {});

        // In ideal case, should reuse the same view
        // There can be an edge case where we do not share the view.
        // If this test is able to trigger that we should change expectation
        // that ~99% of the time we share the view.
        expect(view).toBe(view2);

        cleanup2();
      }

      // Verify cleanup works regardless of whether views were shared
      vi.advanceTimersByTime(100);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });

    test('overlapping cleanup timers all resolve correctly', () => {
      const viewStore = new ViewStore();
      const zero = newMockZero('client1');
      const query = newMockQuery('query1');

      // Create multiple views that might or might not be shared
      const subscriptions = [];

      for (let i = 0; i < 3; i++) {
        const view = viewStore.getView(zero, query, true, 'forever');
        const cleanup = view.subscribeReactInternals(() => {});
        subscriptions.push({view, cleanup});
      }

      // Stagger the cleanups to create overlapping timers
      subscriptions[0].cleanup();
      vi.advanceTimersByTime(3);

      subscriptions[1].cleanup();
      vi.advanceTimersByTime(3);

      subscriptions[2].cleanup();
      vi.advanceTimersByTime(3);

      // Some timers still pending
      expect(getAllViewsSizeForTesting(viewStore)).toBeGreaterThan(0);

      vi.advanceTimersByTime(3);
      // Some timers still pending
      expect(getAllViewsSizeForTesting(viewStore)).toBeGreaterThan(0);

      vi.advanceTimersByTime(3);
      // Some timers still pending
      expect(getAllViewsSizeForTesting(viewStore)).toBeGreaterThan(0);

      // Advance past all cleanup timers
      vi.advanceTimersByTime(100);

      // All views should be cleaned up
      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });
  });
});

describe('select option', () => {
  let container: HTMLElement;
  let root: Root;
  let zero: Zero<Schema>;
  let unique: number = 0;

  beforeEach(() => {
    vi.useRealTimers();
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    zero = newMockZero('client-select');
    unique++;
  });

  afterEach(() => {
    root.unmount();
    document.body.removeChild(container);
    vi.resetAllMocks();
  });

  test('select with primitive return value only re-renders when value changes', async () => {
    const q = newMockQuery('query-select-primitive' + unique);
    let renderCount = 0;

    function Comp() {
      renderCount++;
      const [count] = useQuery(q, {
        select: (data: unknown[]) => data.length,
      });
      return <div>count:{count}</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Comp />
      </ZeroProvider>,
    );

    // Wait for initial render
    await expect.poll(() => container.textContent).toBe('count:0');
    const initialRenderCount = renderCount;

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    // Emit data with 2 items
    view.listeners.forEach(cb => cb([{id: 1}, {id: 2}], 'complete'));
    await expect.poll(() => container.textContent).toBe('count:2');
    expect(renderCount).toBe(initialRenderCount + 1);

    // Emit different data but same length (should NOT re-render)
    const renderCountBeforeSameLength = renderCount;
    view.listeners.forEach(cb => cb([{id: 3}, {id: 4}], 'complete'));

    // Give React time to potentially re-render
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(renderCount).toBe(renderCountBeforeSameLength);
    expect(container.textContent).toBe('count:2');

    // Emit data with different length (should re-render)
    view.listeners.forEach(cb => cb([{id: 1}], 'complete'));
    await expect.poll(() => container.textContent).toBe('count:1');
    expect(renderCount).toBe(renderCountBeforeSameLength + 1);
  });

  test('select with object return value uses deep equality', async () => {
    const q = newMockQuery('query-select-object' + unique);
    let renderCount = 0;

    function Comp() {
      renderCount++;
      const [summary] = useQuery(q, {
        select: (data: unknown[]) => {
          const typedData = data as Array<{id: number; name: string}>;
          return {
            count: typedData.length,
            firstId: typedData[0]?.id ?? null,
          };
        },
      });
      return (
        <div>
          count:{summary?.count ?? 0},first:{summary?.firstId ?? 'none'}
        </div>
      );
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Comp />
      </ZeroProvider>,
    );

    // Wait for initial render
    await expect.poll(() => container.textContent).toBe('count:0,first:none');
    const initialRenderCount = renderCount;

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    // Emit data
    view.listeners.forEach(cb =>
      cb([{id: 1, name: 'Alice'}, {id: 2, name: 'Bob'}], 'complete'),
    );
    await expect.poll(() => container.textContent).toBe('count:2,first:1');
    expect(renderCount).toBe(initialRenderCount + 1);

    // Emit different underlying data but same selected object shape
    // (count still 2, firstId still 1) - should NOT re-render
    const renderCountBeforeSameObject = renderCount;
    view.listeners.forEach(cb =>
      cb([{id: 1, name: 'Alice Updated'}, {id: 2, name: 'Bob Updated'}], 'complete'),
    );

    // Give React time to potentially re-render
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(renderCount).toBe(renderCountBeforeSameObject);
    expect(container.textContent).toBe('count:2,first:1');

    // Change count but keep firstId - should re-render
    view.listeners.forEach(cb =>
      cb([{id: 1, name: 'Alice'}, {id: 2, name: 'Bob'}, {id: 3, name: 'Charlie'}], 'complete'),
    );
    await expect.poll(() => container.textContent).toBe('count:3,first:1');
    expect(renderCount).toBe(renderCountBeforeSameObject + 1);

    // Change firstId but keep count - should re-render
    const renderCountBeforeFirstIdChange = renderCount;
    view.listeners.forEach(cb =>
      cb([{id: 5, name: 'Eve'}, {id: 6, name: 'Frank'}, {id: 7, name: 'Grace'}], 'complete'),
    );
    await expect.poll(() => container.textContent).toBe('count:3,first:5');
    expect(renderCount).toBe(renderCountBeforeFirstIdChange + 1);
  });

  test('select with array return value changes instance when content changes', async () => {
    const q = newMockQuery('query-select-array' + unique);
    const capturedArrays: Array<number[] | undefined> = [];
    let renderCount = 0;

    function Comp() {
      renderCount++;
      const [ids] = useQuery(q, {
        select: (data: unknown[]) =>
          (data as Array<{id: number; name: string}>).map(item => item.id),
      });
      capturedArrays.push(ids);
      return <div>ids:{JSON.stringify(ids)}</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Comp />
      </ZeroProvider>,
    );

    // Wait for initial render
    await expect.poll(() => container.textContent).toBe('ids:[]');
    const initialRenderCount = renderCount;
    expect(capturedArrays.length).toBe(initialRenderCount);

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    // Emit data with some items
    view.listeners.forEach(cb =>
      cb([{id: 1, name: 'Alice'}, {id: 2, name: 'Bob'}], 'complete'),
    );
    await expect.poll(() => container.textContent).toBe('ids:[1,2]');
    expect(renderCount).toBe(initialRenderCount + 1);

    // Capture the array reference after first data update
    const firstArrayRef = capturedArrays[capturedArrays.length - 1];
    expect(firstArrayRef).toEqual([1, 2]);

    // Emit same IDs but different names (should NOT re-render, same array content)
    const renderCountBefore = renderCount;
    view.listeners.forEach(cb =>
      cb([{id: 1, name: 'Alice Updated'}, {id: 2, name: 'Bob Updated'}], 'complete'),
    );

    // Give React time to potentially re-render
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(renderCount).toBe(renderCountBefore);
    expect(container.textContent).toBe('ids:[1,2]');

    // Verify array reference is still the same (no re-render occurred)
    expect(capturedArrays[capturedArrays.length - 1]).toBe(firstArrayRef);

    // Emit different IDs (should re-render, array content changed)
    view.listeners.forEach(cb =>
      cb([{id: 1, name: 'Alice'}, {id: 3, name: 'Charlie'}], 'complete'),
    );
    await expect.poll(() => container.textContent).toBe('ids:[1,3]');
    expect(renderCount).toBe(renderCountBefore + 1);

    // Verify array reference changed (new array instance for useEffect deps)
    const secondArrayRef = capturedArrays[capturedArrays.length - 1];
    expect(secondArrayRef).toEqual([1, 3]);
    expect(secondArrayRef).not.toBe(firstArrayRef);
  });

  test('select with .one() query transforms singular result', async () => {
    // Create a singular query (like .one())
    const q = newMockQuery('query-select-one' + unique, true);
    let renderCount = 0;

    function Comp() {
      renderCount++;
      const [name] = useQuery(q, {
        select: (data: unknown) => {
          const typedData = data as {id: number; name: string} | undefined;
          return typedData?.name ?? 'none';
        },
      });
      return <div>name:{name}</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Comp />
      </ZeroProvider>,
    );

    // Wait for initial render (undefined data → 'none')
    await expect.poll(() => container.textContent).toBe('name:none');
    const initialRenderCount = renderCount;

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    // Emit singular data (not an array, just an object)
    view.listeners.forEach(cb => cb({id: 1, name: 'Alice'}, 'complete'));
    await expect.poll(() => container.textContent).toBe('name:Alice');
    expect(renderCount).toBe(initialRenderCount + 1);

    // Emit different object with same name (should NOT re-render)
    const renderCountBeforeSameName = renderCount;
    view.listeners.forEach(cb => cb({id: 2, name: 'Alice'}, 'complete'));

    // Give React time to potentially re-render
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(renderCount).toBe(renderCountBeforeSameName);
    expect(container.textContent).toBe('name:Alice');

    // Emit object with different name (should re-render)
    view.listeners.forEach(cb => cb({id: 1, name: 'Bob'}, 'complete'));
    await expect.poll(() => container.textContent).toBe('name:Bob');
    expect(renderCount).toBe(renderCountBeforeSameName + 1);

    // Emit undefined (no match) (should re-render)
    const renderCountBeforeUndefined = renderCount;
    view.listeners.forEach(cb => cb(undefined, 'complete'));
    await expect.poll(() => container.textContent).toBe('name:none');
    expect(renderCount).toBe(renderCountBeforeUndefined + 1);
  });

  test('select receives undefined during loading state', async () => {
    const q = newMockQuery('query-select-loading' + unique);
    const selectCalls: Array<unknown[]> = [];

    function Comp() {
      const [ids] = useQuery(q, {
        select: (data: unknown[]) => {
          selectCalls.push(data);
          return (data as Array<{id: number}>).map(item => item.id);
        },
      });
      return <div>ids:{JSON.stringify(ids)}</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Comp />
      </ZeroProvider>,
    );

    // Wait for initial render (loading state with empty array)
    await expect.poll(() => container.textContent).toBe('ids:[]');

    // Select should have been called with empty array during initial loading
    expect(selectCalls.length).toBeGreaterThan(0);
    expect(selectCalls[0]).toEqual([]);

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    // Emit actual data
    view.listeners.forEach(cb => cb([{id: 1}, {id: 2}], 'complete'));
    await expect.poll(() => container.textContent).toBe('ids:[1,2]');

    // Select should have been called with the actual data
    expect(selectCalls[selectCalls.length - 1]).toEqual([{id: 1}, {id: 2}]);
  });

  test('status change causes re-render even if selected value unchanged', async () => {
    const q = newMockQuery('query-select-status' + unique);
    let renderCount = 0;
    let capturedDetails: QueryResultDetails | undefined;

    function Comp() {
      renderCount++;
      const [count, details] = useQuery(q, {
        select: (data: unknown[]) => data.length,
      });
      capturedDetails = details;
      return (
        <div>
          count:{count},status:{details.type}
        </div>
      );
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Comp />
      </ZeroProvider>,
    );

    // Wait for initial render (unknown status with empty array)
    await expect.poll(() => container.textContent).toBe('count:0,status:unknown');
    const initialRenderCount = renderCount;
    expect(capturedDetails?.type).toBe('unknown');

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    // Emit same data (empty array, count=0) but with 'complete' status
    // This should trigger a re-render because status changed, even though selected value is the same
    view.listeners.forEach(cb => cb([], 'complete'));
    await expect.poll(() => container.textContent).toBe('count:0,status:complete');
    expect(renderCount).toBe(initialRenderCount + 1);
    expect(capturedDetails?.type).toBe('complete');

    // Emit same data again with same 'complete' status (should NOT re-render)
    const renderCountBeforeSameStatus = renderCount;
    view.listeners.forEach(cb => cb([], 'complete'));

    // Give React time to potentially re-render
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(renderCount).toBe(renderCountBeforeSameStatus);
    expect(container.textContent).toBe('count:0,status:complete');
  });

  test('no select maintains existing behavior with reference equality', async () => {
    const q = newMockQuery('query-no-select-reference' + unique);
    let renderCount = 0;
    const capturedData: Array<unknown[]> = [];

    function Comp() {
      renderCount++;
      const [data] = useQuery(q);
      capturedData.push(data as unknown[]);
      return <div>count:{(data as unknown[]).length}</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Comp />
      </ZeroProvider>,
    );

    // Wait for initial render
    await expect.poll(() => container.textContent).toBe('count:0');
    const initialRenderCount = renderCount;

    const view = vi.mocked(zero.materialize).mock.results[0].value as {
      listeners: Set<(snap: unknown, resultType: ResultType) => void>;
    };

    // Emit data - should re-render since data changed
    view.listeners.forEach(cb => cb([{id: 1}, {id: 2}], 'complete'));
    await expect.poll(() => container.textContent).toBe('count:2');
    expect(renderCount).toBe(initialRenderCount + 1);

    // Without select, emitting deeply equal but different data DOES cause re-render
    // because useSyncExternalStore uses reference equality, and the ViewWrapper
    // creates a new snapshot on each onData call (even for deep-equal data).
    const renderCountBeforeNewData = renderCount;
    view.listeners.forEach(cb => cb([{id: 1}, {id: 2}], 'complete'));
    await expect.poll(() => renderCount).toBe(renderCountBeforeNewData + 1);

    // The data arrays should be different references (not deep-equal optimized)
    const prevData = capturedData[capturedData.length - 2];
    const currData = capturedData[capturedData.length - 1];
    expect(prevData).not.toBe(currData); // Different references
    expect(prevData).toEqual(currData); // But same content

    // Emit same data with same status again - still re-renders
    // (ViewWrapper creates a new snapshot each time onData is called)
    const renderCountBeforeThirdEmit = renderCount;
    view.listeners.forEach(cb => cb([{id: 1}, {id: 2}], 'complete'));
    await expect.poll(() => renderCount).toBe(renderCountBeforeThirdEmit + 1);
  });

  describe('type inference', () => {
    // Schema with typed queries for type tests
    const testSchema = createSchema({
      tables: [
        table('item').columns({id: number(), name: string()}).primaryKey('id'),
      ],
    });
    const pluralQuery = newQuery(testSchema, 'item');
    const singularQuery = pluralQuery.one();
    type Item = {readonly id: number; readonly name: string};

    // Type-only tests wrapped in component functions to avoid runtime execution
    test('select infers TSelected from select function return type', () => {
      function Types() {
        const [count, countDetails] = useQuery(pluralQuery, {
          select: (data: Item[]) => data.length,
        });
        expectTypeOf(count).toEqualTypeOf<number>();
        expectTypeOf([count, countDetails] as const).toEqualTypeOf<SelectedQueryResult<number>>();

        const [names, namesDetails] = useQuery(pluralQuery, {
          select: (data: Item[]) => data.map(item => item.name),
        });
        expectTypeOf(names).toEqualTypeOf<string[]>();
        expectTypeOf([names, namesDetails] as const).toEqualTypeOf<SelectedQueryResult<string[]>>();

        const [hasItems, hasItemsDetails] = useQuery(pluralQuery, {
          select: (data: Item[]) => data.length > 0,
        });
        expectTypeOf(hasItems).toEqualTypeOf<boolean>();
        expectTypeOf([hasItems, hasItemsDetails] as const).toEqualTypeOf<SelectedQueryResult<boolean>>();
      }
      void Types;
    });

    test('select with singular query receives Item | undefined', () => {
      function Types() {
        const [name, details] = useQuery(singularQuery, {
          select: (data: Item | undefined) => data?.name ?? 'unknown',
        });
        expectTypeOf(name).toEqualTypeOf<string>();
        expectTypeOf([name, details] as const).toEqualTypeOf<SelectedQueryResult<string>>();
      }
      void Types;
    });

    test('select with maybe query returns MaybeSelectedQueryResult', () => {
      function Types() {
        const maybeQuery = pluralQuery as typeof pluralQuery | null;
        const [count, details] = useQuery(maybeQuery, {
          select: (data: Item[]) => data.length,
        });
        expectTypeOf(count).toEqualTypeOf<number | undefined>();
        expectTypeOf([count, details] as const).toEqualTypeOf<MaybeSelectedQueryResult<number>>();
      }
      void Types;
    });

    test('without select returns original HumanReadable type', () => {
      function Types() {
        const [items] = useQuery(pluralQuery);
        expectTypeOf(items).toEqualTypeOf<Item[]>();

        const [item] = useQuery(singularQuery);
        expectTypeOf(item).toEqualTypeOf<Item | undefined>();
      }
      void Types;
    });
  });
});

describe('maybe queries', () => {
  let container: HTMLElement;
  let root: Root;
  let zero: Zero<Schema>;

  beforeEach(() => {
    vi.useRealTimers();
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    zero = newMockZero('client-maybe');
  });

  afterEach(() => {
    root.unmount();
    document.body.removeChild(container);
    vi.resetAllMocks();
  });

  // Shared schema and type for maybe query tests
  const testSchema = createSchema({
    tables: [
      table('item').columns({id: number(), name: string()}).primaryKey('id'),
    ],
  });
  const pluralQuery = newQuery(testSchema, 'item');
  const singularQuery = pluralQuery.one();
  type Item = {readonly id: number; readonly name: string};

  test('plural maybe query (truthy at runtime) returns typed data', async () => {
    let capturedDetails: QueryResultDetails | undefined;

    function Comp() {
      // Non-maybe query returns Item[] (no undefined)
      const [nonMaybeData] = useQuery(pluralQuery);
      expectTypeOf(nonMaybeData).toEqualTypeOf<Item[]>();

      // Maybe query returns Item[] | undefined
      const maybeQuery = pluralQuery as typeof pluralQuery | null;
      const [data, details] = useQuery(maybeQuery);
      capturedDetails = details;

      expectTypeOf(data).toEqualTypeOf<Item[] | undefined>();
      expectTypeOf(details).toEqualTypeOf<QueryResultDetails>();

      return <div>Has query</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Comp />
      </ZeroProvider>,
    );

    await vi.waitFor(() => {
      expect(capturedDetails).toBeDefined();
    });

    expect(zero.materialize).toHaveBeenCalled();
  });

  test('plural maybe query (falsy at runtime) returns undefined', async () => {
    let capturedData: unknown;
    let capturedDetails: QueryResultDetails | undefined;

    function Comp() {
      const maybeQuery = null as typeof pluralQuery | null;
      const [data, details] = useQuery(maybeQuery);
      capturedData = data;
      capturedDetails = details;

      // Type assertions: plural maybe query returns Item[] | undefined
      expectTypeOf(data).toEqualTypeOf<Item[] | undefined>();
      expectTypeOf(details).toEqualTypeOf<QueryResultDetails>();

      return <div>No query</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Comp />
      </ZeroProvider>,
    );

    await vi.waitFor(() => {
      expect(capturedDetails).toBeDefined();
    });

    expect(capturedData).toBe(undefined);
    expect(capturedDetails).toEqual({type: 'unknown'});
    expect(zero.materialize).not.toHaveBeenCalled();
  });

  test('singular maybe query (truthy at runtime) returns typed data', async () => {
    let capturedDetails: QueryResultDetails | undefined;

    function Comp() {
      // Non-maybe singular query returns Item | undefined (undefined for no match)
      const [nonMaybeData] = useQuery(singularQuery);
      expectTypeOf(nonMaybeData).toEqualTypeOf<Item | undefined>();

      // Maybe singular query also returns Item | undefined (same type)
      const maybeQuery = singularQuery as typeof singularQuery | null;
      const [data, details] = useQuery(maybeQuery);
      capturedDetails = details;

      expectTypeOf(data).toEqualTypeOf<Item | undefined>();
      expectTypeOf(details).toEqualTypeOf<QueryResultDetails>();

      return <div>Has query</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Comp />
      </ZeroProvider>,
    );

    await vi.waitFor(() => {
      expect(capturedDetails).toBeDefined();
    });

    expect(zero.materialize).toHaveBeenCalled();
  });

  test('singular maybe query (falsy at runtime) returns undefined', async () => {
    let capturedData: unknown;
    let capturedDetails: QueryResultDetails | undefined;

    function Comp() {
      const maybeQuery = null as typeof singularQuery | null;
      const [data, details] = useQuery(maybeQuery);
      capturedData = data;
      capturedDetails = details;

      // Type assertions: singular maybe query returns Item | undefined
      expectTypeOf(data).toEqualTypeOf<Item | undefined>();
      expectTypeOf(details).toEqualTypeOf<QueryResultDetails>();

      return <div>No query</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Comp />
      </ZeroProvider>,
    );

    await vi.waitFor(() => {
      expect(capturedDetails).toBeDefined();
    });

    expect(capturedData).toBe(undefined);
    expect(capturedDetails).toEqual({type: 'unknown'});
    expect(zero.materialize).not.toHaveBeenCalled();
  });

  // These tests verify that transitioning between truthy/falsy queries doesn't
  // cause React hooks order violations. Without the fix, React throws:
  // - "Rendered fewer hooks than expected" (truthy → falsy)
  // - "Rendered more hooks than during the previous render" (falsy → truthy)

  test('query transitioning from truthy to falsy maintains hooks order', async () => {
    let capturedData: Item[] | undefined;
    let setQueryEnabled!: (enabled: boolean) => void;

    function Comp() {
      const [enabled, setEnabled] = useState(true);
      setQueryEnabled = setEnabled;

      const maybeQuery = enabled ? pluralQuery : null;
      const [data] = useQuery(maybeQuery);
      capturedData = data;

      return <div>{enabled ? 'Has query' : 'No query'}</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Comp />
      </ZeroProvider>,
    );

    await vi.waitFor(() => {
      expect(container.textContent).toBe('Has query');
    });
    expect(zero.materialize).toHaveBeenCalled();

    // Transition to falsy - would throw "Rendered fewer hooks" without fix
    setQueryEnabled(false);

    await vi.waitFor(() => {
      expect(container.textContent).toBe('No query');
    });
    expect(capturedData).toBe(undefined);
  });

  test('query transitioning from falsy to truthy maintains hooks order', async () => {
    let capturedData: Item[] | undefined;
    let setQueryEnabled!: (enabled: boolean) => void;

    function Comp() {
      const [enabled, setEnabled] = useState(false);
      setQueryEnabled = setEnabled;

      const maybeQuery = enabled ? pluralQuery : null;
      const [data] = useQuery(maybeQuery);
      capturedData = data;

      return <div>{enabled ? 'Has query' : 'No query'}</div>;
    }

    root.render(
      <ZeroProvider zero={zero}>
        <Comp />
      </ZeroProvider>,
    );

    await vi.waitFor(() => {
      expect(container.textContent).toBe('No query');
    });
    expect(capturedData).toBe(undefined);
    expect(zero.materialize).not.toHaveBeenCalled();

    // Transition to truthy - would throw "Rendered more hooks" without fix
    setQueryEnabled(true);

    await vi.waitFor(() => {
      expect(container.textContent).toBe('Has query');
    });
    expect(zero.materialize).toHaveBeenCalled();
  });
});
