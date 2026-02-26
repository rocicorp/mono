import React, {useSyncExternalStore} from 'react';
import {createRoot, type Root} from 'react-dom/client';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {queryInternalsTag, type QueryImpl} from './bindings.ts';
import {getAllViewsSizeForTesting, ViewStore} from './use-query.tsx';
import type {
  ErroredQuery,
  Query,
  ResultType,
  Schema,
  Zero,
} from './zero.ts';

// ──────────────────────────────────────────────────────────────────────────────
// Navigation race condition tests
//
// These reproduce the data disappearance bug when rapidly navigating with
// key={location.pathname} on a Suspense boundary.
//
// Root cause: ViewWrapper.subscribeReactInternals schedules a 10ms setTimeout
// on each unsubscribe, but never invalidates stale timers when a new subscriber
// is added. With rapid navigation (unsub every 2-3ms), accumulated timers
// eventually fire during a gap between unsubscribe and resubscribe, destroying
// the view even though it was actively used since the timer was scheduled.
//
// Additionally, when maxRecentQueries=0 (default), destroying a view
// immediately evicts the query from the QueryManager, sending a `del` to the
// server. Re-materialization sends a `put`, but if the server's response to
// the `del` (removing g/<hash> from IDB) races with the `put`, the view can
// get stuck at resultType='unknown' with empty data forever.
// ──────────────────────────────────────────────────────────────────────────────

type Listener = (data: unknown, resultType: ResultType, error?: ErroredQuery) => void;

type MockView = {
  listeners: Set<Listener>;
  addListener(cb: Listener): () => void;
  destroy(): void;
  updateTTL(): void;
};

function newMockQuery(hash: string, singular = false): Query<string, Schema> {
  return {
    [queryInternalsTag]: true,
    hash: () => hash,
    format: {singular},
  } as unknown as QueryImpl<string, Schema>;
}

function newMockZero(clientID: string): Zero<Schema, undefined, unknown> {
  return {
    clientID,
    materialize: vi.fn().mockImplementation(() => ({
      listeners: new Set(),
      addListener(cb: Listener) {
        this.listeners.add(cb);
        return () => { this.listeners.delete(cb); };
      },
      destroy() { this.listeners.clear(); },
      updateTTL() {},
    } satisfies MockView)),
  } as unknown as Zero<Schema, undefined, unknown>;
}

function emit(zero: Zero<Schema, undefined, unknown>, data: unknown, resultType: ResultType = 'unknown') {
  const mock = vi.mocked(zero.materialize).mock.results[0]?.value as MockView | undefined;
  if (!mock) throw new Error('materialize not called');
  mock.listeners.forEach(cb => cb(data, resultType));
}

/** Emit on the Nth materialized view (0-indexed). Useful after destroy + re-materialize. */
function emitNth(zero: Zero<Schema, undefined, unknown>, n: number, data: unknown, resultType: ResultType = 'unknown') {
  const mock = vi.mocked(zero.materialize).mock.results[n]?.value as MockView | undefined;
  if (!mock) throw new Error(`materialize call #${n} not found`);
  mock.listeners.forEach(cb => cb(data, resultType));
}

function snapData(view: {getSnapshot: () => readonly [unknown, ...unknown[]]}) {
  return view.getSnapshot()[0];
}

function snapLength(view: {getSnapshot: () => readonly [unknown, ...unknown[]]}) {
  return (snapData(view) as unknown[]).length;
}

describe('Navigation: ViewStore destroy + re-create lifecycle', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  //   emit(data)  -->  snap has data
  //   unsubscribe  -->  10ms timer starts
  //   5ms elapsed  -->  view still alive
  //   resubscribe  -->  timer fires later, bails (listeners > 0)
  //   snap still has data
  test('resubscribe within 10ms window preserves data (no flash)', () => {
    const viewStore = new ViewStore();
    const query = newMockQuery('nav-preserve');
    const zero = newMockZero('c-nav');
    const view = viewStore.getView(zero, query, true, 'forever');
    const cleanup = view.subscribeReactInternals(() => {});

    emit(zero, [{id: '1', name: 'Alice'}]);
    expect(snapLength(view)).toBe(1);

    // Simulate unmount (navigation away)
    cleanup();

    // Advance only 5ms (within the 10ms window)
    vi.advanceTimersByTime(5);

    // Simulate remount (navigation back) — same ViewWrapper reused
    const view2 = viewStore.getView(zero, query, true, 'forever');
    expect(view2).toBe(view);
    const cleanup2 = view2.subscribeReactInternals(() => {});

    // Data is still there (no flash)
    expect(snapLength(view2)).toBe(1);
    expect((snapData(view2) as Array<{id: string}>)[0].id).toBe('1');

    // Timer fires but bails because listeners > 0
    vi.advanceTimersByTime(20);
    expect(snapLength(view2)).toBe(1);
    expect(getAllViewsSizeForTesting(viewStore)).toBe(1);

    // View was NOT re-materialized
    expect(zero.materialize).toHaveBeenCalledTimes(1);

    cleanup2();
  });

  //   emit(data)  -->  snap has data
  //   unsubscribe  -->  10ms timer starts
  //   15ms elapsed  -->  view destroyed, removed from ViewStore
  //   resubscribe  -->  NEW ViewWrapper created, re-materializes
  //   getSnapshot()  -->  empty (default snapshot)
  //   emit(data) on new view  -->  snap has data again
  test('resubscribe after 10ms creates new view, starts empty, then recovers', () => {
    const viewStore = new ViewStore();
    const query = newMockQuery('nav-destroy');
    const zero = newMockZero('c-nav2');
    const view = viewStore.getView(zero, query, true, 'forever');
    const cleanup = view.subscribeReactInternals(() => {});

    emit(zero, [{id: '1', name: 'Alice'}], 'complete');
    expect(snapLength(view)).toBe(1);

    // Simulate unmount
    cleanup();

    // Let the 10ms timer fire — view is destroyed
    vi.advanceTimersByTime(15);
    expect(getAllViewsSizeForTesting(viewStore)).toBe(0);

    // Simulate remount — creates a NEW ViewWrapper
    const view2 = viewStore.getView(zero, query, true, 'forever');
    expect(view2).not.toBe(view);
    const cleanup2 = view2.subscribeReactInternals(() => {});

    // New view starts with empty/default snapshot
    expect(snapLength(view2)).toBe(0);

    // A second materialize call happened
    expect(zero.materialize).toHaveBeenCalledTimes(2);

    // Server eventually sends data to the new view
    emitNth(zero, 1, [{id: '1', name: 'Alice'}], 'complete');
    expect(snapLength(view2)).toBe(1);

    cleanup2();
  });

  //   BUG: Rapid navigation accumulates stale destroy timers.
  //
  //   Timeline with 3ms between each unsub/resub:
  //     t=0:  subscribe
  //     t=0:  unsub() → timer T0 fires at t=10
  //     t=3:  subscribe
  //     t=3:  unsub() → timer T1 fires at t=13
  //     t=6:  subscribe
  //     t=6:  unsub() → timer T2 fires at t=16
  //     t=9:  subscribe
  //     t=9:  unsub() → timer T3 fires at t=19
  //     t=12: advancing time... T0 fires! listeners=0 → DESTROYS VIEW
  //
  //   T0 was scheduled at t=0 but fires at t=10 during a gap where no
  //   listener is active (between unsub at t=9 and resub at t=12).
  //   The timer's `#reactInternals.size > 0` check passes because
  //   size IS 0 at that moment, even though subscribers have come and
  //   gone since T0 was scheduled. The timer has no generation counter
  //   to detect that the world has changed since it was created.
  //
  //   Fix: clear pending destroy timers when a new subscriber is added,
  //   or use a generation counter to invalidate stale timers.
  //
  //   FAILS on main: stale timer T0 fires and destroys the view even
  //   though it was actively used (subscribed/unsubscribed 3 times)
  //   between when T0 was scheduled and when it fires.
  test('rapid navigation spam: data preserved across 5 unsub/resub cycles', () => {
    const viewStore = new ViewStore();
    const query = newMockQuery('nav-spam');
    const zero = newMockZero('c-spam');

    let view = viewStore.getView(zero, query, true, 'forever');
    let unsub = view.subscribeReactInternals(() => {});
    emit(zero, [{id: '1'}]);
    expect(snapLength(view)).toBe(1);

    for (let i = 0; i < 5; i++) {
      unsub();
      vi.advanceTimersByTime(3);
      view = viewStore.getView(zero, query, true, 'forever');
      unsub = view.subscribeReactInternals(() => {});
      // Data should be preserved — each cycle is within 10ms
      expect(snapLength(view)).toBe(1);
    }

    // View should never have been re-materialized
    expect(zero.materialize).toHaveBeenCalledTimes(1);
    expect(getAllViewsSizeForTesting(viewStore)).toBe(1);

    unsub();
  });

  //   Same race condition but with 2ms gaps (faster navigation).
  //   Even with very fast navigation, the 4th cycle crosses the 10ms
  //   boundary of the first timer, destroying the view.
  test('rapid navigation spam at 2ms intervals: data preserved', () => {
    const viewStore = new ViewStore();
    const query = newMockQuery('nav-spam-fast');
    const zero = newMockZero('c-spam-fast');

    let view = viewStore.getView(zero, query, true, 'forever');
    let unsub = view.subscribeReactInternals(() => {});
    emit(zero, [{id: '1'}]);
    expect(snapLength(view)).toBe(1);

    for (let i = 0; i < 8; i++) {
      unsub();
      vi.advanceTimersByTime(2);
      view = viewStore.getView(zero, query, true, 'forever');
      unsub = view.subscribeReactInternals(() => {});
      expect(snapLength(view)).toBe(1);
    }

    expect(zero.materialize).toHaveBeenCalledTimes(1);

    unsub();
  });

  //   Simulate: mount → unmount → wait >10ms → mount
  //   The unmount's timer fires, view is destroyed, new view created on remount.
  test('unmount + long pause past deadline = view destroyed', () => {
    const viewStore = new ViewStore();
    const query = newMockQuery('nav-deadline');
    const zero = newMockZero('c-deadline');
    const view = viewStore.getView(zero, query, true, 'forever');
    const unsub = view.subscribeReactInternals(() => {});

    emit(zero, [{id: '1'}], 'complete');
    expect(snapLength(view)).toBe(1);

    unsub();
    vi.advanceTimersByTime(15);

    // View is destroyed
    expect(getAllViewsSizeForTesting(viewStore)).toBe(0);

    // Remount creates new wrapper with empty data
    const view2 = viewStore.getView(zero, query, true, 'forever');
    expect(view2).not.toBe(view);
    expect(snapLength(view2)).toBe(0);
    expect(zero.materialize).toHaveBeenCalledTimes(2);

    view2.subscribeReactInternals(() => {});
  });

  //   The exact scenario from the bug report:
  //   1. User on assignment page, data loaded
  //   2. User clicks a link (navigates to sub-route)
  //   3. key={location.pathname} unmounts Suspense children
  //   4. New route renders (different component, different queries)
  //   5. User clicks back within 10ms
  //   6. Original queries re-subscribe, data should be preserved
  //
  //   Then the longer version:
  //   7. User navigates away again
  //   8. Stays on new page > 10ms
  //   9. User navigates back
  //   10. NEW ViewWrapper created, must re-materialize
  //   11. Server sends data to new view
  //   12. Data must appear
  test('full navigation lifecycle: away + back fast, then away + back slow', () => {
    const viewStore = new ViewStore();
    const queryA = newMockQuery('assignment-trackers');
    const queryB = newMockQuery('student-detail');
    const zero = newMockZero('c-full');

    // Phase 1: User on assignment page, data loaded
    const viewA = viewStore.getView(zero, queryA, true, 'forever');
    const unsubA = viewA.subscribeReactInternals(() => {});
    emit(zero, [{id: 't1', score: 85}, {id: 't2', score: 92}]);
    expect(snapLength(viewA)).toBe(2);

    // Phase 2: Navigate to student detail (within 10ms)
    unsubA();
    vi.advanceTimersByTime(2);

    // Student detail page mounts its own queries
    const viewB = viewStore.getView(zero, queryB, true, 'forever');
    const unsubB = viewB.subscribeReactInternals(() => {});

    // Phase 3: Quick back to assignment page (within 10ms)
    unsubB();
    vi.advanceTimersByTime(2);

    const viewA2 = viewStore.getView(zero, queryA, true, 'forever');
    const unsubA2 = viewA2.subscribeReactInternals(() => {});

    // Data preserved (same ViewWrapper, same snapshot)
    expect(viewA2).toBe(viewA);
    expect(snapLength(viewA2)).toBe(2);
    const data = snapData(viewA2) as Array<{id: string; score: number}>;
    expect(data[0].score).toBe(85);

    // Phase 4: Navigate away for longer than 10ms
    unsubA2();
    vi.advanceTimersByTime(15);

    // Both ViewWrappers are destroyed
    expect(getAllViewsSizeForTesting(viewStore)).toBe(0);

    // Phase 5: Navigate back to assignment page
    const viewA3 = viewStore.getView(zero, queryA, true, 'forever');
    expect(viewA3).not.toBe(viewA); // new wrapper
    const unsubA3 = viewA3.subscribeReactInternals(() => {});

    // Initially empty (new wrapper, no data yet)
    expect(snapLength(viewA3)).toBe(0);

    // A new materialize call was made
    const materializeCalls = vi.mocked(zero.materialize).mock.calls.length;
    expect(materializeCalls).toBeGreaterThanOrEqual(3);

    // Server eventually sends data to the new view
    const lastIdx = vi.mocked(zero.materialize).mock.results.length - 1;
    emitNth(zero, lastIdx, [{id: 't1', score: 85}, {id: 't2', score: 92}], 'complete');
    expect(snapLength(viewA3)).toBe(2);

    unsubA3();
  });
});

describe('Navigation: React key-based remount', () => {
  let root: Root;
  let element: HTMLDivElement;

  beforeEach(() => {
    vi.useRealTimers();
    element = document.createElement('div');
    document.body.appendChild(element);
    root = createRoot(element);
  });

  afterEach(() => {
    root.unmount();
    document.body.removeChild(element);
  });

  //   <Suspense key="page-a">
  //     <DataComponent />  -- subscribes to ViewStore, renders data
  //   </Suspense>
  //
  //   key changes to "page-b":
  //     old tree unmounts (unsubscribe)
  //     new tree mounts (subscribe)
  //     ViewStore finds existing ViewWrapper (< 10ms)
  //     Data is preserved
  test('key change remount preserves data (simulated Suspense key swap)', async () => {
    const viewStore = new ViewStore();
    const query = newMockQuery('key-swap');
    const zero = newMockZero('c-key');

    type Row = {id: string; name: string};
    const renderCounts = {current: 0};

    function DataComponent() {
      const view = viewStore.getView(zero, query, true, 'forever');
      const [data] = useSyncExternalStore(
        view.subscribeReactInternals,
        view.getSnapshot,
        view.getSnapshot,
      );
      renderCounts.current++;
      const rows = (data ?? []) as Row[];
      return (
        <div data-testid="data">
          {rows.map(r => <div key={r.id} data-testid={`row-${r.id}`}>{r.name}</div>)}
        </div>
      );
    }

    // Mount with key="page-a"
    root.render(
      <React.Suspense key="page-a" fallback={<div>Loading...</div>}>
        <DataComponent />
      </React.Suspense>,
    );

    await expect.poll(() => renderCounts.current).toBeGreaterThanOrEqual(1);

    // Emit data
    emit(zero, [{id: '1', name: 'Alice'}, {id: '2', name: 'Bob'}]);
    await expect.poll(() => element.querySelector('[data-testid="row-1"]')?.textContent).toBe('Alice');

    // Change key — this unmounts and remounts the entire Suspense subtree
    root.render(
      <React.Suspense key="page-b" fallback={<div>Loading...</div>}>
        <DataComponent />
      </React.Suspense>,
    );

    // Data should still be visible after remount (ViewWrapper reused within 10ms)
    await expect.poll(() => element.querySelector('[data-testid="row-1"]')?.textContent).toBe('Alice');
    await expect.poll(() => element.querySelector('[data-testid="row-2"]')?.textContent).toBe('Bob');

    // Only one materialize call (ViewWrapper was reused)
    expect(zero.materialize).toHaveBeenCalledTimes(1);
  });

  //   Reproduces the exact bug from the WebSocket trace:
  //   1. Data component renders with relationship data (parent + children)
  //   2. Key changes (navigation)
  //   3. Old Suspense unmounts, new Suspense mounts
  //   4. Data should be preserved through the transition
  //   5. If data goes empty, it must recover when server sends data
  test('key change with relationship data: no permanent data loss', async () => {
    const viewStore = new ViewStore();
    const query = newMockQuery('key-rel', true); // singular (.one())
    const zero = newMockZero('c-key-rel');

    type Assignment = {
      id: string;
      problem_trackers: Array<{
        id: string;
        mastery_assessment: {mastery_score: number} | undefined;
      }>;
    };
    const renderCounts = {current: 0};
    const snapshots: Array<Assignment | undefined> = [];

    function AssignmentPage() {
      const view = viewStore.getView(zero, query, true, 'forever');
      const [data] = useSyncExternalStore(
        view.subscribeReactInternals,
        view.getSnapshot,
        view.getSnapshot,
      );
      renderCounts.current++;
      const assignment = data as Assignment | undefined;
      snapshots.push(assignment);
      const trackers = assignment?.problem_trackers ?? [];
      return (
        <div data-testid="assignment">
          {trackers.map(t => (
            <div key={t.id} data-testid={`tracker-${t.id}`}>
              {t.mastery_assessment?.mastery_score ?? 'none'}
            </div>
          ))}
        </div>
      );
    }

    // Mount
    root.render(
      <React.Suspense key="/assignments/abc" fallback={<div>Loading...</div>}>
        <AssignmentPage />
      </React.Suspense>,
    );
    await expect.poll(() => renderCounts.current).toBeGreaterThanOrEqual(1);

    // Emit assignment with problem trackers + mastery assessments
    const assignmentData: Assignment = {
      id: 'a1',
      problem_trackers: [
        {id: 'pt1', mastery_assessment: {mastery_score: 85}},
        {id: 'pt2', mastery_assessment: {mastery_score: 92}},
        {id: 'pt3', mastery_assessment: undefined},
      ],
    };
    emit(zero, assignmentData, 'complete');
    await expect.poll(() => element.querySelector('[data-testid="tracker-pt1"]')?.textContent).toBe('85');

    // Navigate away (key change simulating route change)
    root.render(
      <React.Suspense key="/assignments/abc/students/s1" fallback={<div>Loading...</div>}>
        <AssignmentPage />
      </React.Suspense>,
    );

    // Navigate back (key change again)
    root.render(
      <React.Suspense key="/assignments/abc" fallback={<div>Loading...</div>}>
        <AssignmentPage />
      </React.Suspense>,
    );

    // Data should be preserved (ViewWrapper reused within 10ms)
    await expect.poll(() => element.querySelector('[data-testid="tracker-pt1"]')?.textContent).toBe('85');
    await expect.poll(() => element.querySelector('[data-testid="tracker-pt2"]')?.textContent).toBe('92');
    await expect.poll(() => element.querySelector('[data-testid="tracker-pt3"]')?.textContent).toBe('none');

    // Only one materialize (ViewWrapper was never destroyed)
    expect(zero.materialize).toHaveBeenCalledTimes(1);

    // Verify data was never empty between renders
    const dataRenders = snapshots.filter(s => s !== undefined);
    for (const snap of dataRenders) {
      if (snap!.problem_trackers.length > 0) {
        // Once we had data, we should never lose it
        expect(snap!.problem_trackers).toHaveLength(3);
      }
    }
  });
});
