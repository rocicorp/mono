import React, {memo, useSyncExternalStore} from 'react';
import {createRoot, type Root} from 'react-dom/client';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {queryInternalsTag, type QueryImpl} from './bindings.ts';
import {ViewStore} from './use-query.tsx';
import type {
  ErroredQuery,
  Query,
  ResultType,
  Schema,
  Zero,
} from './zero.ts';

// ─── Test helpers ───────────────────────────────────────────────────────────

function newMockQuery(query: string, singular = false): Query<string, Schema> {
  return {
    [queryInternalsTag]: true,
    hash: () => query,
    format: {singular},
  } as unknown as QueryImpl<string, Schema>;
}

type MockView = {
  listeners: Set<
    (data: unknown, resultType: ResultType, error?: ErroredQuery) => void
  >;
  addListener(
    cb: (data: unknown, resultType: ResultType, error?: ErroredQuery) => void,
  ): () => void;
  destroy(): void;
  updateTTL(): void;
};

function newView(): MockView {
  return {
    listeners: new Set(),
    addListener(cb) {
      this.listeners.add(cb);
      return () => { this.listeners.delete(cb); };
    },
    destroy() { this.listeners.clear(); },
    updateTTL() {},
  };
}

function newMockZero(clientID: string): Zero<Schema, undefined, unknown> {
  const view = newView();
  return {
    clientID,
    materialize: vi.fn().mockImplementation(() => view),
  } as unknown as Zero<Schema, undefined, unknown>;
}

function getListeners(zero: Zero<Schema, undefined, unknown>, index = 0) {
  const result = vi.mocked(zero.materialize).mock.results[index]?.value as MockView | undefined;
  if (!result) throw new Error('materialize was not called');
  return result.listeners;
}

function createView(viewStore: ViewStore, suffix: string) {
  const q = newMockQuery(`q-${suffix}`);
  const zero = newMockZero(`client-${suffix}`);
  const view = viewStore.getView(zero, q, true, 'forever');
  const cleanup = view.subscribeReactInternals(() => {});
  return {view, zero, q, cleanup};
}

// ─── Snapshot identity ──────────────────────────────────────────────────────

describe('Snapshot identity', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  test('getSnapshot returns same reference without data changes, new reference after', () => {
    const viewStore = new ViewStore();
    const {view, zero, cleanup} = createView(viewStore, 'identity');

    // Same ref before data
    expect(view.getSnapshot()).toBe(view.getSnapshot());

    // Push data
    getListeners(zero).forEach(cb => cb([{id: '1'}], 'unknown'));
    const withData = view.getSnapshot();

    // Same ref after data (no further changes)
    expect(view.getSnapshot()).toBe(withData);

    // New ref after new data
    getListeners(zero).forEach(cb => cb([{id: '1'}, {id: '2'}], 'unknown'));
    expect(view.getSnapshot()).not.toBe(withData);

    cleanup();
  });

  test('empty snapshots use sentinel objects (no spurious re-renders)', () => {
    const viewStore = new ViewStore();
    const {view, zero, cleanup} = createView(viewStore, 'sentinel');

    const empty1 = view.getSnapshot();
    getListeners(zero).forEach(cb => cb([], 'unknown'));
    const empty2 = view.getSnapshot();

    // Same sentinel reference for repeated empty data
    expect(empty1).toBe(empty2);

    // Singular empty sentinel
    const qSingular = newMockQuery('singular', true);
    const zeroSingular = newMockZero('client-singular');
    const singular = viewStore.getView(zeroSingular, qSingular, true, 'forever');
    const cleanupSingular = singular.subscribeReactInternals(() => {});

    const s1 = singular.getSnapshot();
    getListeners(zeroSingular).forEach(cb => cb(undefined, 'unknown'));
    expect(singular.getSnapshot()).toBe(s1);

    cleanup();
    cleanupSingular();
  });

  test('row identity preserved in snapshot: unchanged rows keep same reference', () => {
    const viewStore = new ViewStore();
    const {view, zero, cleanup} = createView(viewStore, 'row-identity');
    const listeners = getListeners(zero);

    const row1 = {id: '1', name: 'Alice'};
    const row2 = {id: '2', name: 'Bob'};
    listeners.forEach(cb => cb([row1, row2], 'unknown'));

    // Update only row2, keep row1 as same object
    const row2Updated = {id: '2', name: 'Bob Updated'};
    listeners.forEach(cb => cb([row1, row2Updated], 'unknown'));

    const data = view.getSnapshot()[0] as Array<{id: string; name: string}>;
    expect(data[0]).toBe(row1);
    expect(data[1]).toBe(row2Updated);

    cleanup();
  });
});

// ─── Data flash prevention ──────────────────────────────────────────────────

describe('No data flash (data→empty→data)', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  test('snapshot never goes empty between data updates', () => {
    const viewStore = new ViewStore();
    const {view, zero, cleanup} = createView(viewStore, 'flash');
    const listeners = getListeners(zero);

    const snapshots: unknown[] = [];
    view.subscribeReactInternals(() => {
      snapshots.push((view.getSnapshot()[0] as unknown[]).length);
    });

    listeners.forEach(cb => cb([{id: '1'}], 'unknown'));
    listeners.forEach(cb => cb([{id: '1'}, {id: '2'}], 'unknown'));

    // After first data, no snapshot should ever be empty
    let hadData = false;
    for (const len of snapshots) {
      if ((len as number) > 0) hadData = true;
      if (hadData) expect(len).toBeGreaterThan(0);
    }

    cleanup();
  });

  test('stale snapshot preserved after view destroy (no empty flash on remount)', () => {
    const viewStore = new ViewStore();
    const {view, zero, cleanup} = createView(viewStore, 'destroy-flash');

    getListeners(zero).forEach(cb => cb([{id: '1'}], 'complete'));
    expect((view.getSnapshot()[0] as unknown[]).length).toBe(1);

    cleanup();
    vi.advanceTimersByTime(15); // past 10ms cleanup timeout

    // Stale snapshot still has data, not empty
    expect((view.getSnapshot()[0] as unknown[]).length).toBe(1);
  });
});

// ─── React.memo render counting ─────────────────────────────────────────────

describe('React.memo child render counting', () => {
  let root: Root;
  let element: HTMLDivElement;
  let unique = 0;

  beforeEach(() => {
    vi.useRealTimers();
    element = document.createElement('div');
    document.body.appendChild(element);
    root = createRoot(element);
    unique++;
  });

  afterEach(() => {
    root.unmount();
    document.body.removeChild(element);
  });

  test('only the changed row child re-renders; unchanged rows skip', async () => {
    const viewStore = new ViewStore();
    const q = newMockQuery(`react-memo-${unique}`);
    const zero = newMockZero(`client-memo-${unique}`);
    const parentRenders = {current: 0};
    const childRenders: Record<string, number> = {};

    type Row = {id: string; name: string};

    const ChildRow = memo(function ChildRow({row}: {row: Row}) {
      childRenders[row.id] = (childRenders[row.id] ?? 0) + 1;
      return <div data-testid={`row-${row.id}`}>{row.name}</div>;
    });

    function Parent() {
      const viewRef = viewStore.getView(zero, q, true, 'forever');
      const [data] = useSyncExternalStore(
        viewRef.subscribeReactInternals,
        viewRef.getSnapshot,
        viewRef.getSnapshot,
      );
      parentRenders.current++;
      return (
        <div>{((data ?? []) as Row[]).map(row => <ChildRow key={row.id} row={row} />)}</div>
      );
    }

    root.render(<Parent />);
    await expect.poll(() => parentRenders.current).toBeGreaterThanOrEqual(1);

    // Push initial data
    const row1 = {id: '1', name: 'Alice'};
    const row2 = {id: '2', name: 'Bob'};
    getListeners(zero).forEach(cb => cb([row1, row2], 'unknown'));

    await expect.poll(() => element.querySelector('[data-testid="row-1"]')?.textContent).toBe('Alice');

    const rendersAfterData = parentRenders.current;
    const child1After = childRenders['1'] ?? 0;

    // Update only row2, keep row1 as same reference
    getListeners(zero).forEach(cb => cb([row1, {id: '2', name: 'Bob Updated'}], 'unknown'));

    await expect.poll(() => element.querySelector('[data-testid="row-2"]')?.textContent).toBe('Bob Updated');

    // Parent re-renders (new snapshot tuple)
    expect(parentRenders.current).toBeGreaterThan(rendersAfterData);
    // Unchanged row1 child does NOT re-render
    expect(childRenders['1']).toBe(child1After);
    // Changed row2 child DOES re-render
    expect(childRenders['2']).toBeGreaterThan(1);
  });
});
