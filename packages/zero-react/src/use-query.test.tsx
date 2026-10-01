import {Suspense, useState, type ReactNode} from 'react';
import {createRoot, type Root} from 'react-dom/client';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  test,
  vi,
} from 'vitest';
import type {Format} from '../../zero-types/src/format.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import type {TTL} from '../../zql/src/query/ttl.ts';
import {queryInternalsTag, type QueryImpl} from './bindings.ts';
import {
  getAllViewsSizeForTesting,
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
  type Schema,
  type Zero,
} from './zero.ts';

function newMockQuery(query: string, singular = false): Query<string, Schema> {
  const ret = {
    [queryInternalsTag]: true,
    hash() {
      return query + singular;
    },
    format: {singular},
  } as unknown as QueryImpl<string, Schema>;
  return ret;
}

function newMockQueryWithFormat(
  query: string,
  format: Format,
): Query<string, Schema> {
  const ret = {
    [queryInternalsTag]: true,
    hash() {
      return query + JSON.stringify(format);
    },
    format,
  } as unknown as QueryImpl<string, Schema>;
  return ret;
}

type MockView = ReturnType<typeof newView>;

/** The views each mock Zero's `materialize` returned, in order. */
const materializedViews = new WeakMap<object, MockView[]>();

function newMockZero<
  MD extends CustomMutatorDefs | undefined = undefined,
  C = unknown,
>(clientID: string): Zero<Schema, MD, C> {
  const views: MockView[] = [];
  const zero = {
    clientID,
    materialize: vi.fn(() => {
      const view = newView();
      views.push(view);
      return view;
    }),
  };
  materializedViews.set(zero, views);
  return zero as unknown as Zero<Schema, MD, C>;
}

function newView() {
  const listeners = new Set<(...args: unknown[]) => void>();
  return {
    listeners,
    addListener(cb: (...args: unknown[]) => void) {
      listeners.add(cb);
    },
    destroy: vi.fn(() => {
      listeners.clear();
    }),
    updateTTL(_ttl: TTL) {},
    /** Calls the listeners, as the real view does when it flushes. */
    emit(...args: unknown[]) {
      for (const cb of listeners) {
        cb(...args);
      }
    },
  };
}

/** The view the last (or the `n`th) `zero.materialize` call returned. */
function materializedView(zero: object, n = -1): MockView {
  const view = materializedViews.get(zero)?.at(n);
  if (!view) {
    throw new Error(`materialize was not called ${n < 0 ? -n : n + 1} times`);
  }
  return view;
}

function getView(
  viewStore: ViewStore,
  {
    zero = newMockZero('client1'),
    query = newMockQuery('query1'),
    ttl = 'forever',
  }: {
    zero?: Zero<Schema> | undefined;
    query?: Query<string, Schema> | undefined;
    ttl?: TTL | undefined;
  } = {},
) {
  return viewStore.getView(zero, query, true, ttl);
}

/**
 * A view of `query1` in a new store, and a way to call the listeners of the
 * view it materialized.
 */
function newMaterializedView(singular = false) {
  const zero = newMockZero('client1');
  const view = getView(new ViewStore(), {
    zero,
    query: newMockQuery('query1', singular),
  });
  return {
    zero,
    view,
    emit: (...args: unknown[]) => materializedView(zero).emit(...args),
  };
}

/** Mounts a fresh React root for each test of the enclosing describe. */
function setupRoot() {
  let element: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    vi.useRealTimers();
    element = document.createElement('div');
    document.body.appendChild(element);
    root = createRoot(element);
  });

  afterEach(() => {
    root.unmount();
    element.remove();
  });

  return {
    render(zero: Zero<Schema>, children: ReactNode, key?: string) {
      root.render(
        <ZeroProvider zero={zero} key={key}>
          {children}
        </ZeroProvider>,
      );
    },
    /** The rendered text, for `expect.poll`. */
    text: () => element.textContent,
  };
}

describe('ViewStore', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  describe('duplicate queries', () => {
    // Each getView below uses a new Zero with the same client ID.

    test('duplicate queries do not create duplicate views', () => {
      const viewStore = new ViewStore();
      expect(getView(viewStore)).toBe(getView(viewStore));
      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });

    test('removing a duplicate query does not destroy the shared view', () => {
      const viewStore = new ViewStore();
      const cleanup1 = getView(viewStore).subscribeReactInternals(() => {});
      getView(viewStore).subscribeReactInternals(() => {});

      cleanup1();
      vi.advanceTimersByTime(100);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });

    test('Using the same query with different TTL should reuse views', () => {
      const viewStore = new ViewStore();
      const q1 = newMockQuery('query1');
      const zero = newMockZero('client1');
      const view1 = getView(viewStore, {zero, query: q1, ttl: '1s'});

      const updateTTLSpy = vi.spyOn(view1, 'updateTTL');
      expect(zero.materialize).toHaveBeenCalledExactlyOnceWith(q1, {ttl: '1s'});

      const zeroClient2 = newMockZero('client1');
      expect(getView(viewStore, {zero: zeroClient2, ttl: '1m'})).toBe(view1);

      // Same query hash and client id so only one view. Should have called
      // updateTTL on the existing one.
      expect(zeroClient2.materialize).not.toHaveBeenCalled();
      expect(updateTTLSpy).toHaveBeenCalledExactlyOnceWith('1m');

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });

    test('Using the same query with same TTL but different representation', () => {
      const viewStore = new ViewStore();
      const zero = newMockZero('client1');
      const view1 = getView(viewStore, {zero, ttl: '60s'});
      const updateTTLSpy = vi.spyOn(view1, 'updateTTL');
      expect(zero.materialize).toHaveBeenCalledTimes(1);

      expect(getView(viewStore, {ttl: '1m'})).toBe(view1);
      expect(updateTTLSpy).toHaveBeenCalledExactlyOnceWith('1m');

      expect(getView(viewStore, {ttl: 60_000})).toBe(view1);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });
  });

  describe('destruction', () => {
    test('removing all duplicate queries destroys the shared view', () => {
      const viewStore = new ViewStore();
      const cleanup1 = getView(viewStore).subscribeReactInternals(() => {});
      const cleanup2 = getView(viewStore).subscribeReactInternals(() => {});

      cleanup1();
      cleanup2();
      vi.advanceTimersByTime(100);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });

    test('removing a unique query destroys the view', () => {
      const viewStore = new ViewStore();
      const cleanup = getView(viewStore).subscribeReactInternals(() => {});
      cleanup();

      vi.advanceTimersByTime(100);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });

    test('view destruction is delayed via setTimeout', () => {
      const viewStore = new ViewStore();
      const cleanup = getView(viewStore).subscribeReactInternals(() => {});
      cleanup();

      vi.advanceTimersByTime(5);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
      vi.advanceTimersByTime(10);

      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });

    test('subscribing to a view scheduled for cleanup prevents the cleanup', () => {
      const viewStore = new ViewStore();
      const view = getView(viewStore);
      const cleanup = view.subscribeReactInternals(() => {});

      cleanup();

      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
      vi.advanceTimersByTime(5);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);

      const view2 = getView(viewStore);
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
      const cleanup = getView(viewStore).subscribeReactInternals(() => {});

      cleanup();
      cleanup();

      vi.advanceTimersByTime(100);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });
  });

  describe('clients', () => {
    test('the same query for different clients results in different views', () => {
      const viewStore = new ViewStore();
      expect(getView(viewStore)).not.toBe(
        getView(viewStore, {zero: newMockZero('client2')}),
      );
    });

    test('one client’s views are destroyed without disturbing another’s', () => {
      const viewStore = new ViewStore();
      const zero1 = newMockZero('client1');
      const view1 = getView(viewStore, {zero: zero1});
      const zero2 = newMockZero('client2');
      const view2 = getView(viewStore, {zero: zero2});
      expect(getAllViewsSizeForTesting(viewStore)).toBe(2);

      const cleanup1 = view1.subscribeReactInternals(() => {});
      const cleanup2 = view2.subscribeReactInternals(() => {});

      cleanup1();
      vi.advanceTimersByTime(100);

      // The other client keeps its view, and asking again returns that same
      // one rather than building a second.
      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
      expect(getView(viewStore, {zero: zero2})).toBe(view2);

      cleanup2();
      vi.advanceTimersByTime(100);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);

      // ...and the store still works afterwards, having dropped the per-client
      // entry it no longer needs.
      expect(getView(viewStore, {zero: zero1})).not.toBe(view1);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });
  });

  describe('ttl', () => {
    test('an unchanged ttl is not forwarded to the view', () => {
      const viewStore = new ViewStore();
      const zero = newMockZero('client1');

      getView(viewStore, {zero, ttl: 1000});
      // The wrapper materializes eagerly, so the underlying view is the one
      // to watch: `getView` calls the wrapper's `updateTTL` either way, and
      // what the guard changes is whether it forwards.
      const updateTTL = vi.spyOn(materializedView(zero), 'updateTTL');

      // Same ttl, as every re-render passes: nothing to tell the view, and
      // nothing to re-derive in the query manager.
      getView(viewStore, {zero, ttl: 1000});
      getView(viewStore, {zero, ttl: 1000});
      expect(updateTTL).not.toHaveBeenCalled();

      // A different ttl still propagates...
      getView(viewStore, {zero, ttl: 2000});
      expect(updateTTL).toHaveBeenCalledWith(2000);

      // ...including a change only in how the duration is spelled.
      updateTTL.mockClear();
      getView(viewStore, {zero, ttl: '2s'});
      expect(updateTTL).toHaveBeenCalledWith('2s');
    });
  });

  describe('singular vs plural', () => {
    test('the same query hash with different singular flag creates different views', () => {
      const viewStore = new ViewStore();
      const zero = newMockZero('client1');
      const view1 = getView(viewStore, {
        zero,
        query: newMockQuery('query1', false),
      });
      const view2 = getView(viewStore, {
        zero,
        query: newMockQuery('query1', true),
      });

      expect(view1).not.toBe(view2);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(2);
    });

    test('duplicate singular queries share a view', () => {
      const viewStore = new ViewStore();
      const view1 = getView(viewStore, {query: newMockQuery('query1', true)});
      const view2 = getView(viewStore, {query: newMockQuery('query1', true)});

      expect(view1).toBe(view2);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });

    test('queries that differ only in a nested relationship singular flag create different views', () => {
      // `related('owner', q => q.one())` and `related('owner', q => q.limit(1))`
      // produce the same AST (and therefore the same query hash) and the same
      // top-level `format.singular`. They differ only in the *nested*
      // `format.relationships.owner.singular`, so the cache key must fold the
      // whole format, not just the top-level singular flag.
      const viewStore = new ViewStore();
      const zero = newMockZero('client1');
      const withOwner = (singular: boolean) =>
        newMockQueryWithFormat('query1', {
          singular: false,
          relationships: {owner: {singular, relationships: {}}},
        });

      const view1 = getView(viewStore, {zero, query: withOwner(true)});
      const view2 = getView(viewStore, {zero, query: withOwner(false)});

      expect(view1).not.toBe(view2);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(2);
    });

    test('duplicate queries with matching nested formats share a view', () => {
      const viewStore = new ViewStore();
      const zero = newMockZero('client1');
      // A new but equal format object for each query.
      const withSingularOwner = () =>
        newMockQueryWithFormat('query1', {
          singular: false,
          relationships: {owner: {singular: true, relationships: {}}},
        });

      const view1 = getView(viewStore, {zero, query: withSingularOwner()});
      const view2 = getView(viewStore, {zero, query: withSingularOwner()});

      expect(view1).toBe(view2);
      expect(getAllViewsSizeForTesting(viewStore)).toBe(1);
    });
  });

  // The data is built by a function so that each call passes a new object:
  // equal data, not the same reference.
  const shapes = [
    {name: 'plural', singular: false, empty: () => [], row: () => [{a: 1}]},
    {
      name: 'singular',
      singular: true,
      empty: () => undefined,
      row: () => ({a: 1}),
    },
  ];

  describe('collapse multiple empty on data', () => {
    test.each(shapes)('$name', ({singular, empty, row}) => {
      const {zero, view, emit} = newMaterializedView(singular);
      expect(zero.materialize).toHaveBeenCalledTimes(1);
      const cleanup = view.subscribeReactInternals(() => {});

      emit(empty(), 'unknown');
      const snapshot1 = view.getSnapshot();
      expect(snapshot1).toEqual([empty(), {type: 'unknown'}]);
      emit(empty(), 'unknown');
      expect(view.getSnapshot()).toBe(snapshot1);

      emit(row(), 'unknown');
      // TODO: Assert that the data is the same object as passed into the listener.
      expect(view.getSnapshot()).toEqual([row(), {type: 'unknown'}]);

      emit(empty(), 'complete');
      const snapshot3 = view.getSnapshot();
      expect(snapshot3).toEqual([empty(), {type: 'complete'}]);
      emit(empty(), 'complete');
      expect(view.getSnapshot()).toBe(snapshot3);

      cleanup();
    });
  });

  describe('cached result type', () => {
    test.each(shapes)(
      '$name: empty cached snapshots are shared and stable',
      ({singular, empty, row}) => {
        const {view, emit} = newMaterializedView(singular);
        const cleanup = view.subscribeReactInternals(() => {});

        emit(empty(), 'cached');
        const snapshot1 = view.getSnapshot();
        expect(snapshot1).toEqual([empty(), {type: 'cached'}]);
        emit(empty(), 'cached');
        expect(view.getSnapshot()).toBe(snapshot1);

        emit(row(), 'cached');
        expect(view.getSnapshot()).toEqual([row(), {type: 'cached'}]);

        emit(row(), 'complete');
        expect(view.getSnapshot()).toEqual([row(), {type: 'complete'}]);

        cleanup();
      },
    );

    test('empty cached result satisfies nonEmpty but not complete', () => {
      const {view, emit} = newMaterializedView();
      const cleanup = view.subscribeReactInternals(() => {});

      // A server-confirmed empty result from a previous session is enough
      // for suspendUntil: 'partial' to render while offline.
      emit([], 'cached');
      expect(view.nonEmpty).toBe(true);
      expect(view.complete).toBe(false);

      cleanup();
    });

    test('a revoked empty cached result suspends again', async () => {
      const {view, emit} = newMaterializedView();
      const cleanup = view.subscribeReactInternals(() => {});

      emit([], 'cached');
      expect(view.nonEmpty).toBe(true);

      // The got key was evicted before this connection confirmed the query.
      emit([], 'unknown');
      expect(view.nonEmpty).toBe(false);
      let resolved = false;
      void view.waitForNonEmpty().then(() => {
        resolved = true;
      });
      await Promise.resolve();
      expect(resolved).toBe(false);

      emit([{a: 1}], 'unknown');
      expect(view.nonEmpty).toBe(true);
      await Promise.resolve();
      expect(resolved).toBe(true);

      cleanup();
    });

    test('cached does not satisfy complete-waiters', () => {
      const {view, emit} = newMaterializedView();
      const cleanup = view.subscribeReactInternals(() => {});

      emit([{a: 1}], 'cached');
      // 'cached' is last session's server-confirmed answer; only a
      // confirmation on THIS connection may report complete.
      expect(view.complete).toBe(false);

      emit([{a: 1}], 'complete');
      expect(view.complete).toBe(true);

      cleanup();
    });
  });

  describe('unchanged data on flush', () => {
    test('same data reference keeps snapshot identity and does not notify', () => {
      const {view, emit} = newMaterializedView();
      const notify = vi.fn();
      const cleanup = view.subscribeReactInternals(notify);

      const rows = [{a: 1}];
      emit(rows, 'unknown');
      const snapshot1 = view.getSnapshot();
      expect(snapshot1).toEqual([[{a: 1}], {type: 'unknown'}]);
      expect(notify).toHaveBeenCalledTimes(1);

      // Same data reference, resultType and error: the previous snapshot
      // tuple is kept (so useSyncExternalStore's Object.is bailout works) and
      // React is not notified.
      emit(rows, 'unknown');
      expect(view.getSnapshot()).toBe(snapshot1);
      expect(notify).toHaveBeenCalledTimes(1);

      // Same data reference but new resultType: new snapshot, notified.
      emit(rows, 'complete');
      const snapshot2 = view.getSnapshot();
      expect(snapshot2).not.toBe(snapshot1);
      expect(snapshot2).toEqual([[{a: 1}], {type: 'complete'}]);
      expect(notify).toHaveBeenCalledTimes(2);

      // New data reference: new snapshot, notified.
      emit([{a: 2}], 'complete');
      expect(view.getSnapshot()).toEqual([[{a: 2}], {type: 'complete'}]);
      expect(notify).toHaveBeenCalledTimes(3);

      cleanup();
    });

    test('same error reference keeps snapshot identity and does not notify', () => {
      const {view, emit} = newMaterializedView();
      const notify = vi.fn();
      const cleanup = view.subscribeReactInternals(notify);

      const error: ErroredQuery = {
        error: 'app',
        id: 'query1',
        name: 'query1',
        details: 'boom',
      };
      const rows: unknown[] = [];
      emit(rows, 'error', error);
      const snapshot1 = view.getSnapshot();
      expect(snapshot1[1].type).toBe('error');
      expect(notify).toHaveBeenCalledTimes(1);

      emit(rows, 'error', error);
      expect(view.getSnapshot()).toBe(snapshot1);
      expect(notify).toHaveBeenCalledTimes(1);

      // A different error object produces a new snapshot and notifies.
      emit(rows, 'error', {...error, details: 'boom again'});
      expect(view.getSnapshot()).not.toBe(snapshot1);
      expect(notify).toHaveBeenCalledTimes(2);

      cleanup();
    });

    // After unsubscribe → destroy → re-subscribe, the new view can deliver the
    // same data reference and resultType as before the destroy; an empty
    // .one() query redelivering (undefined, 'complete') is the realistic case.
    // The snapshot keeps its identity and React is not notified, but the
    // resolvers, reset by the destroy, must still resolve.
    test.each([
      {name: 'a row', row: {a: 1}},
      {name: 'no row', row: undefined},
    ])(
      'resolvers resolve after re-materialize with unchanged data: $name',
      async ({row}) => {
        const {view, emit} = newMaterializedView(true);
        const cleanup = view.subscribeReactInternals(() => {});
        emit(row, 'complete');
        expect(view.complete).toBe(true);
        const snapshot = view.getSnapshot();
        expect(snapshot).toEqual([row, {type: 'complete'}]);

        cleanup();
        vi.advanceTimersByTime(20);
        expect(view.complete).toBe(false);

        const notify = vi.fn();
        const cleanup2 = view.subscribeReactInternals(notify);
        emit(row, 'complete');
        expect(view.getSnapshot()).toBe(snapshot);
        expect(notify).not.toHaveBeenCalled();
        expect(view.complete).toBe(true);
        await expect(view.waitForComplete()).resolves.toBeUndefined();
        expect(view.nonEmpty).toBe(true);
        await expect(view.waitForNonEmpty()).resolves.toBeUndefined();

        cleanup2();
      },
    );
  });
});

describe('stable query identity', () => {
  const dom = setupRoot();

  /**
   * A named query as a call site sees it: the `CustomQuery` is a stable
   * module-level object, and calling it allocates a fresh request each render.
   */
  function newMockCustomQuery() {
    const built = newMockQuery('stable-query');
    const fn = vi.fn(() => built);
    const customQuery = {fn};
    const request = (args: ReadonlyJSONValue) =>
      ({
        'query': customQuery,
        args,
        '~': 'QueryRequest',
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      }) as any;
    return {fn, request};
  }

  function Comp({
    n,
    request,
  }: {
    n: number;
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    request: any;
  }) {
    useQuery(request);
    return <div>{n}</div>;
  }

  async function render(
    zero: Zero<Schema>,
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    request: any,
    n: number,
  ) {
    dom.render(zero, <Comp n={n} request={request} />);
    await expect.poll(dom.text).toBe(String(n));
  }

  test('a request with equal args is resolved once across re-renders', async () => {
    const {fn, request} = newMockCustomQuery();
    const zero = newMockZero('client-stable');

    await render(zero, request({id: 'a'}), 1);
    expect(fn).toHaveBeenCalledTimes(1);

    // A fresh request object each render, meaning the same thing: the query
    // definition -- argument validation and the builder chain -- is not run
    // again.
    await render(zero, request({id: 'a'}), 2);
    await render(zero, request({id: 'a'}), 3);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  test('changing the args resolves again', async () => {
    const {fn, request} = newMockCustomQuery();
    const zero = newMockZero('client-stable-args');

    await render(zero, request({id: 'a'}), 1);
    await render(zero, request({id: 'b'}), 2);
    expect(fn).toHaveBeenCalledTimes(2);

    // ...and the new args are then themselves stable.
    await render(zero, request({id: 'b'}), 3);
    expect(fn).toHaveBeenCalledTimes(2);
  });

  test('a query toggled off and back on reuses what it had', async () => {
    const {fn, request} = newMockCustomQuery();
    const zero = newMockZero('client-stable-toggle');

    await render(zero, request({id: 'a'}), 1);
    expect(fn).toHaveBeenCalledTimes(1);

    await render(zero, undefined, 2);
    await render(zero, request({id: 'a'}), 3);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  test('changing the Zero instance resolves again', async () => {
    const {fn, request} = newMockCustomQuery();

    await render(newMockZero('client-stable-z1'), request({id: 'a'}), 1);
    expect(fn).toHaveBeenCalledTimes(1);

    // A different Zero means a different context and a different view store
    // entry, so the cached resolution does not carry over.
    await render(newMockZero('client-stable-z2'), request({id: 'a'}), 2);
    expect(fn).toHaveBeenCalledTimes(2);
  });
});

describe('useSuspenseQuery', () => {
  const dom = setupRoot();
  let unique = 0;

  beforeEach(() => {
    unique++;
  });

  /** A query and a Zero instance that no other test uses. */
  function newQueryAndZero(singular = false) {
    return {
      q: newMockQuery('query' + unique, singular),
      zero: newMockZero('client' + unique),
    };
  }

  /** Renders `children` in a suspense boundary that shows `loading`. */
  function renderSuspense(
    zero: Zero<Schema>,
    children: ReactNode,
    key?: string,
  ) {
    dom.render(
      zero,
      <Suspense fallback={<>loading</>}>{children}</Suspense>,
      key,
    );
  }

  /** Renders the data, prefixed with `label:` when there is one. */
  function Data({
    query,
    suspendUntil,
    label,
  }: {
    query: Query<string, Schema>;
    suspendUntil: 'complete' | 'partial';
    label?: string | undefined;
  }) {
    const [data] = useSuspenseQuery(query, {suspendUntil});
    const text = String(JSON.stringify(data));
    return <div>{label === undefined ? text : `${label}:${text}`}</div>;
  }

  test.each([
    {
      name: 'suspendsUntil complete',
      suspendUntil: 'complete',
      data: [{a: 1}],
      resultType: 'complete',
      text: '[{"a":1}]',
    },
    {
      name: 'suspendsUntil partial, partial array before complete',
      suspendUntil: 'partial',
      data: [{a: 1}],
      resultType: 'unknown',
      text: '[{"a":1}]',
    },
    {
      name: 'suspendsUntil partial singular, defined value before complete',
      singular: true,
      suspendUntil: 'partial',
      data: {a: 1},
      resultType: 'unknown',
      text: '{"a":1}',
    },
    {
      name: 'suspendUntil partial, complete with empty array',
      suspendUntil: 'partial',
      data: [],
      resultType: 'complete',
      text: '[]',
    },
    {
      name: 'suspendUntil partial, complete with undefined',
      singular: true,
      suspendUntil: 'partial',
      data: undefined,
      resultType: 'complete',
      text: 'undefined',
    },
  ] as const)(
    '$name',
    async ({singular = false, suspendUntil, data, resultType, text}) => {
      const {q, zero} = newQueryAndZero(singular);
      renderSuspense(zero, <Data query={q} suspendUntil={suspendUntil} />);
      await expect.poll(dom.text).toBe('loading');

      materializedView(zero).emit(data, resultType);
      await expect.poll(dom.text).toBe(text);
    },
  );

  test.each([
    {
      name: 'suspendsUntil complete, already complete',
      suspendUntil: 'complete',
      resultType: 'complete',
    },
    {
      name: 'suspendsUntil partial, already partial array before complete',
      suspendUntil: 'partial',
      resultType: 'unknown',
    },
  ] as const)('$name', async ({suspendUntil, resultType}) => {
    const {q, zero} = newQueryAndZero();
    renderSuspense(
      zero,
      <Data query={q} suspendUntil={suspendUntil} label="1" />,
      '1',
    );
    await expect.poll(dom.text).toBe('loading');

    materializedView(zero).emit([{a: 1}], resultType);
    await expect.poll(dom.text).toBe('1:[{"a":1}]');

    // A new provider and component; the view already satisfies them.
    renderSuspense(
      zero,
      <Data query={q} suspendUntil={suspendUntil} label="2" />,
      '2',
    );
    await expect.poll(dom.text).toBe('2:[{"a":1}]');
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

    test.each([
      {name: 'plural', singular: false, empty: []},
      {name: 'singular', singular: true, empty: undefined},
    ])(
      '$name query returns error details when query fails',
      async ({singular, empty}) => {
        const {q, zero} = newQueryAndZero(singular);

        function Comp() {
          const [data, details] = useSuspenseQuery(q, {
            suspendUntil: 'complete',
          });
          return (
            <div>
              {details.type === 'error'
                ? `Error: ${details.error?.message || 'Unknown error'}`
                : JSON.stringify(data)}
            </div>
          );
        }

        renderSuspense(zero, <Comp />);
        await expect.poll(dom.text).toBe('loading');

        materializedView(zero).emit(
          empty,
          'error',
          getErroredQuery('Query failed', {reason: 'Invalid syntax'}),
        );
        await expect.poll(dom.text).toBe('Error: Query failed');
      },
    );

    test('query transitions from error to success state', async () => {
      const {q, zero} = newQueryAndZero();

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

      renderSuspense(zero, <Comp />);
      await expect.poll(dom.text).toBe('loading');
      const view = materializedView(zero);

      // First emit error
      view.emit(
        [],
        'error',
        getErroredQuery('Temporary failure', {some: 'detail'}),
      );
      await expect
        .poll(dom.text)
        .toBe('Error: Temporary failure {"some":"detail"}');

      // Then emit success
      view.emit([{a: 1}], 'complete');
      await expect.poll(dom.text).toBe('Data: [{"a":1}], Type: complete');
    });

    test('query can return partial data with error state', async () => {
      const {q, zero} = newQueryAndZero();

      function Comp() {
        const [data, details] = useSuspenseQuery(q, {suspendUntil: 'partial'});
        return (
          <div>
            Data: {JSON.stringify(data)}, Type: {details.type}, Error:{' '}
            {details.type === 'error' ? details.error?.message : 'none'}
          </div>
        );
      }

      renderSuspense(zero, <Comp />);
      await expect.poll(dom.text).toBe('loading');

      materializedView(zero).emit(
        [{a: 1}],
        'error',
        getErroredQuery('Partial failure', {message: 'Some items failed'}),
      );
      await expect
        .poll(dom.text)
        .toBe('Data: [{"a":1}], Type: error, Error: Partial failure');
    });

    test('error state without suspense returns immediately', async () => {
      const {q, zero} = newQueryAndZero();

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

      renderSuspense(zero, <Comp />);
      await expect.poll(dom.text).toBe('loading');

      // Emit error immediately
      materializedView(zero).emit(
        [],
        'error',
        getErroredQuery('Immediate error'),
      );
      await expect.poll(dom.text).toBe('Error state: Immediate error');
    });

    test('parse error type is handled correctly', async () => {
      const {q, zero} = newQueryAndZero();

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

      renderSuspense(zero, <Comp />);
      await expect.poll(dom.text).toBe('loading');

      materializedView(zero).emit([], 'error', {
        error: 'parse',
        id: 'q1',
        name: 'q1',
        message: 'Parse error',
        details: {message: 'Invalid syntax'},
      } satisfies ErroredQuery);
      await expect.poll(dom.text).toBe('Parse Error: Parse error');
    });

    test('retry function retries the query after error', async () => {
      const {q, zero} = newQueryAndZero();

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

      renderSuspense(zero, <Comp />);
      await expect.poll(dom.text).toBe('loading');

      const firstView = materializedView(zero, 0);
      firstView.emit(
        [],
        'error',
        getErroredQuery('Query failed', {message: 'Network error'}),
      );
      await expect.poll(dom.text).toBe('Error: Query failed');

      // Verify retry function is available
      expect(retryFn).toBeDefined();
      expect(refetchFn).toEqual(retryFn);

      // Retrying destroys the old view and materializes a new one.
      retryFn!();
      expect(firstView.destroy).toHaveBeenCalledTimes(1);
      expect(zero.materialize).toHaveBeenCalledTimes(2);

      // Emit successful data on retry
      materializedView(zero, 1).emit([{a: 1, b: 2}], 'complete');
      await expect.poll(dom.text).toBe('Data: [{"a":1,"b":2}], Type: complete');
    });

    test('retry function can be called multiple times', async () => {
      const {q, zero} = newQueryAndZero(true);

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

      renderSuspense(zero, <Comp />);
      await expect.poll(dom.text).toBe('loading');

      // The first two views fail; each retry destroys the failed view and
      // materializes a new one.
      for (const [i, [message, details]] of [
        ['First failure', 'Network error'],
        ['Second failure', 'Service unavailable'],
      ].entries()) {
        const view = materializedView(zero, i);
        view.emit(
          undefined,
          'error',
          getErroredQuery(message, {message: details}),
        );
        await expect
          .poll(dom.text)
          .toBe(`Error: ${message} {"message":"${details}"}`);

        retryFn!();
        expect(view.destroy).toHaveBeenCalledTimes(1);
        expect(zero.materialize).toHaveBeenCalledTimes(i + 2);
      }

      // Third view succeeds
      materializedView(zero, 2).emit({success: true}, 'complete');
      await expect.poll(dom.text).toBe('Data: {"success":true}');
    });

    test('retry function is undefined when query is not in error state', async () => {
      const {q, zero} = newQueryAndZero();

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

      renderSuspense(zero, <Comp />);
      await expect.poll(dom.text).toBe('loading');

      // Emit successful data (not error state)
      materializedView(zero).emit([{a: 1}], 'complete');
      await expect.poll(dom.text).toBe('Data: [{"a":1}], Type: complete');

      // Verify that retry is not available when not in error state
      expect(capturedDetails?.type).toBe('complete');
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
      const views = await Promise.all(
        Array.from({length: 10}, () =>
          Promise.resolve().then(() => getView(viewStore, {zero, query})),
        ),
      );

      // Check if views are shared (ideal case)
      expect(new Set(views).size).toBe(1);

      // Subscribe to all views, then clean up all
      const cleanups = views.map(v => v.subscribeReactInternals(() => {}));
      cleanups.forEach(cleanup => cleanup());
      vi.advanceTimersByTime(100);

      // Verify all views are eventually cleaned up
      expect(getAllViewsSizeForTesting(viewStore)).toBe(0);
    });

    test('rapid mount/unmount/remount reuses view when possible', () => {
      const viewStore = new ViewStore();
      const zero = newMockZero('client1');
      const query = newMockQuery('query1');

      // Simulate React strict mode double-mounting
      for (let i = 0; i < 5; i++) {
        const view = getView(viewStore, {zero, query});
        const cleanup = view.subscribeReactInternals(() => {});

        // Immediate cleanup (unmount)
        cleanup();

        // Immediate remount before timeout
        const view2 = getView(viewStore, {zero, query});
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
      const cleanups = Array.from({length: 3}, () =>
        getView(viewStore, {zero, query}).subscribeReactInternals(() => {}),
      );

      // Stagger the cleanups to create overlapping timers
      for (const cleanup of cleanups) {
        cleanup();
        vi.advanceTimersByTime(3);
      }

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

describe('maybe queries', () => {
  const dom = setupRoot();
  let zero: Zero<Schema>;

  beforeEach(() => {
    zero = newMockZero('client-maybe');
  });

  afterEach(() => {
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

    dom.render(zero, <Comp />);

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

    dom.render(zero, <Comp />);

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

    dom.render(zero, <Comp />);

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

    dom.render(zero, <Comp />);

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
  function Toggle({
    initiallyEnabled,
    onRender,
  }: {
    initiallyEnabled: boolean;
    onRender: (
      data: Item[] | undefined,
      setEnabled: (e: boolean) => void,
    ) => void;
  }) {
    const [enabled, setEnabled] = useState(initiallyEnabled);
    const [data] = useQuery(enabled ? pluralQuery : null);
    onRender(data, setEnabled);
    return <div>{enabled ? 'Has query' : 'No query'}</div>;
  }

  test('query transitioning from truthy to falsy maintains hooks order', async () => {
    let capturedData: Item[] | undefined;
    let setQueryEnabled!: (enabled: boolean) => void;

    dom.render(
      zero,
      <Toggle
        initiallyEnabled={true}
        onRender={(data, setEnabled) => {
          capturedData = data;
          setQueryEnabled = setEnabled;
        }}
      />,
    );
    await expect.poll(dom.text).toBe('Has query');
    expect(zero.materialize).toHaveBeenCalled();

    // Transition to falsy - would throw "Rendered fewer hooks" without fix
    setQueryEnabled(false);
    await expect.poll(dom.text).toBe('No query');
    expect(capturedData).toBe(undefined);
  });

  test('query transitioning from falsy to truthy maintains hooks order', async () => {
    let capturedData: Item[] | undefined;
    let setQueryEnabled!: (enabled: boolean) => void;

    dom.render(
      zero,
      <Toggle
        initiallyEnabled={false}
        onRender={(data, setEnabled) => {
          capturedData = data;
          setQueryEnabled = setEnabled;
        }}
      />,
    );
    await expect.poll(dom.text).toBe('No query');
    expect(capturedData).toBe(undefined);
    expect(zero.materialize).not.toHaveBeenCalled();

    // Transition to truthy - would throw "Rendered more hooks" without fix
    setQueryEnabled(true);
    await expect.poll(dom.text).toBe('Has query');
    expect(zero.materialize).toHaveBeenCalled();
  });
});
