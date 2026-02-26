import React, {memo, useSyncExternalStore} from 'react';
import {createRoot, type Root} from 'react-dom/client';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {consume} from '../../zql/src/ivm/stream.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
import {schema} from '../../zql/src/query/test/test-schemas.ts';
import {queryInternalsTag, type QueryImpl} from './bindings.ts';
import {ViewStore} from './use-query.tsx';
import type {
  ErroredQuery,
  Query,
  ResultType,
  Schema,
  Zero,
} from './zero.ts';

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

describe('Snapshot identity', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  //   getSnapshot()  -->  ref1
  //   getSnapshot()  -->  ref1  (same, no data change)
  //   listener([row])
  //   getSnapshot()  -->  ref2  (new, data changed)
  //   getSnapshot()  -->  ref2  (same, no further change)
  test('getSnapshot returns same reference without data changes, new reference after', () => {
    const viewStore = new ViewStore();
    const {view, zero, cleanup} = createView(viewStore, 'identity');

    expect(view.getSnapshot()).toBe(view.getSnapshot());

    getListeners(zero).forEach(cb => cb([{id: '1'}], 'unknown'));
    const withData = view.getSnapshot();

    expect(view.getSnapshot()).toBe(withData);

    getListeners(zero).forEach(cb => cb([{id: '1'}, {id: '2'}], 'unknown'));
    expect(view.getSnapshot()).not.toBe(withData);

    cleanup();
  });

  //   getSnapshot()  -->  [[], {type:'unknown'}]  (sentinel A)
  //   listener([])
  //   getSnapshot()  -->  [[], {type:'unknown'}]  (sentinel A, same ref)
  test('empty snapshots use sentinel objects (no spurious re-renders)', () => {
    const viewStore = new ViewStore();
    const {view, zero, cleanup} = createView(viewStore, 'sentinel');

    const empty1 = view.getSnapshot();
    getListeners(zero).forEach(cb => cb([], 'unknown'));
    const empty2 = view.getSnapshot();

    expect(empty1).toBe(empty2);

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

  //   listener([row1, row2])  -->  snap1 = [row1, row2]
  //   listener([row1, row2'])  -->  snap2 = [row1, row2']
  //
  //   snap2[0]:  same ref as row1 (unchanged)
  //   snap2[1]:  row2' (new ref, changed)
  test('row identity preserved in snapshot: unchanged rows keep same reference', () => {
    const viewStore = new ViewStore();
    const {view, zero, cleanup} = createView(viewStore, 'row-identity');
    const listeners = getListeners(zero);

    const row1 = {id: '1', name: 'Alice'};
    const row2 = {id: '2', name: 'Bob'};
    listeners.forEach(cb => cb([row1, row2], 'unknown'));

    const row2Updated = {id: '2', name: 'Bob Updated'};
    listeners.forEach(cb => cb([row1, row2Updated], 'unknown'));

    const data = view.getSnapshot()[0] as Array<{id: string; name: string}>;
    expect(data[0]).toBe(row1);
    expect(data[1]).toBe(row2Updated);

    cleanup();
  });
});

describe('No data flash (data to empty to data)', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  //   listener([row1])         -->  snap = [row1]   (has data)
  //   listener([row1, row2])   -->  snap = [row1, row2]
  //
  //   At no point should snap become [] between these two updates.
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

    let hadData = false;
    for (const len of snapshots) {
      if ((len as number) > 0) hadData = true;
      if (hadData) expect(len).toBeGreaterThan(0);
    }

    cleanup();
  });

  //   listener([row])  -->  snap = [row]
  //   unsubscribe
  //   ... 15ms (past 10ms cleanup timeout) ...
  //   getSnapshot()  -->  snap = [row]  (stale data preserved, not empty)
  test('stale snapshot preserved after view destroy (no empty flash on remount)', () => {
    const viewStore = new ViewStore();
    const {view, zero, cleanup} = createView(viewStore, 'destroy-flash');

    getListeners(zero).forEach(cb => cb([{id: '1'}], 'complete'));
    expect((view.getSnapshot()[0] as unknown[]).length).toBe(1);

    cleanup();
    vi.advanceTimersByTime(15);

    expect((view.getSnapshot()[0] as unknown[]).length).toBe(1);
  });

  //   listener([row])   -->  snap = [row]
  //   listener([])       -->  snap = []     (legitimate empty)
  //   listener([row2])  -->  snap = [row2]
  //
  //   Verifies that transitioning through empty is visible (not masked)
  //   when the server genuinely returns empty then non-empty.
  test('legitimate empty transition is visible (not masked)', () => {
    const viewStore = new ViewStore();
    const {view, zero, cleanup} = createView(viewStore, 'legit-empty');
    const listeners = getListeners(zero);

    const lengths: number[] = [];
    view.subscribeReactInternals(() => {
      lengths.push((view.getSnapshot()[0] as unknown[]).length);
    });

    listeners.forEach(cb => cb([{id: '1'}], 'unknown'));
    listeners.forEach(cb => cb([], 'complete'));
    listeners.forEach(cb => cb([{id: '2'}], 'complete'));

    expect(lengths).toEqual([1, 0, 1]);

    cleanup();
  });
});

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

  //   Parent (useQuery)
  //     |
  //     +-- ChildRow(row1)  React.memo  <-- same ref, skip re-render
  //     +-- ChildRow(row2)  React.memo  <-- new ref, re-renders
  //
  //   listener([row1, row2])    -->  both children render
  //   listener([row1, row2'])   -->  only row2 child re-renders
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

    const row1 = {id: '1', name: 'Alice'};
    const row2 = {id: '2', name: 'Bob'};
    getListeners(zero).forEach(cb => cb([row1, row2], 'unknown'));

    await expect.poll(() => element.querySelector('[data-testid="row-1"]')?.textContent).toBe('Alice');

    const rendersAfterData = parentRenders.current;
    const child1After = childRenders['1'] ?? 0;

    getListeners(zero).forEach(cb => cb([row1, {id: '2', name: 'Bob Updated'}], 'unknown'));

    await expect.poll(() => element.querySelector('[data-testid="row-2"]')?.textContent).toBe('Bob Updated');

    expect(parentRenders.current).toBeGreaterThan(rendersAfterData);
    expect(childRenders['1']).toBe(child1After);
    expect(childRenders['2']).toBeGreaterThan(1);
  });
});

//   issue1 ─── owner:Alice        edit comment1       issue1' ─── owner:Alice (same ref)
//           ├── comment1            ──────────►               ├── comment1' (new ref)
//           └── comment2                                      └── comment2 (same ref)
//   issue2 ─── owner:Bob                               issue2 (same ref, unrelated)
//           └── comment3                                     └── comment3 (same ref)
//
//   <IssueRow issue={issue1}>  re-renders (descendant changed)
//   <IssueRow issue={issue2}>  skips (React.memo, same ref)
describe('End-to-end: real IVM pipeline to React.memo', () => {
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

  test('editing a comment only re-renders the parent issue, not unrelated issues', async () => {
    const queryDelegate = new QueryDelegateImpl();
    const userSource = queryDelegate.getSource('user');
    const issueSource = queryDelegate.getSource('issue');
    const commentSource = queryDelegate.getSource('comment');

    consume(userSource.push({type: 'add', row: {id: 'u1', name: 'Alice', metadata: null}}));
    consume(userSource.push({type: 'add', row: {id: 'u2', name: 'Bob', metadata: null}}));
    consume(issueSource.push({type: 'add', row: {id: 'i1', title: 'Bug', description: 'd1', closed: false, ownerId: 'u1', createdAt: 1}}));
    consume(issueSource.push({type: 'add', row: {id: 'i2', title: 'Feature', description: 'd2', closed: false, ownerId: 'u2', createdAt: 2}}));
    consume(commentSource.push({type: 'add', row: {id: 'c1', authorId: 'u1', issueId: 'i1', text: 'first', createdAt: 1}}));
    consume(commentSource.push({type: 'add', row: {id: 'c2', authorId: 'u2', issueId: 'i1', text: 'second', createdAt: 2}}));
    consume(commentSource.push({type: 'add', row: {id: 'c3', authorId: 'u2', issueId: 'i2', text: 'third', createdAt: 3}}));

    const issueQuery = newQuery(schema, 'issue')
      .related('owner')
      .related('comments');

    const view = queryDelegate.materialize(issueQuery);

    // Wire the real TypedView to useSyncExternalStore
    type IssueRow = {id: string; title: string; comments: Array<{id: string; text: string}>; owner: {name: string} | undefined};
    let snapshot: unknown[] = [];
    const subscribers = new Set<() => void>();
    view.addListener(data => {
      snapshot = data as unknown[];
      for (const cb of subscribers) cb();
    });
    const subscribe = (cb: () => void) => {
      subscribers.add(cb);
      return () => { subscribers.delete(cb); };
    };
    const getSnapshot = () => snapshot;

    const issueRenders: Record<string, number> = {};

    const IssueRowComponent = memo(function IssueRowComponent({issue}: {issue: IssueRow}) {
      issueRenders[issue.id] = (issueRenders[issue.id] ?? 0) + 1;
      const commentTexts = issue.comments.map(c => c.text).join(', ');
      return (
        <div data-testid={`issue-${issue.id}`}>
          {issue.title} by {issue.owner?.name}: [{commentTexts}]
        </div>
      );
    });

    function IssueList() {
      const issues = useSyncExternalStore(subscribe, getSnapshot, getSnapshot) as IssueRow[];
      return (
        <div>
          {issues.map(issue => (
            <IssueRowComponent key={issue.id} issue={issue} />
          ))}
        </div>
      );
    }

    root.render(<IssueList />);

    // Wait for hydration render
    await expect
      .poll(() => element.querySelector('[data-testid="issue-i1"]')?.textContent)
      .toContain('Bug');
    await expect
      .poll(() => element.querySelector('[data-testid="issue-i2"]')?.textContent)
      .toContain('Feature');

    const issue1RendersAfterHydration = issueRenders['i1'] ?? 0;
    const issue2RendersAfterHydration = issueRenders['i2'] ?? 0;
    expect(issue1RendersAfterHydration).toBeGreaterThanOrEqual(1);
    expect(issue2RendersAfterHydration).toBeGreaterThanOrEqual(1);

    // Edit comment1's text via the real source
    consume(commentSource.push({
      type: 'edit',
      oldRow: {id: 'c1', authorId: 'u1', issueId: 'i1', text: 'first', createdAt: 1},
      row: {id: 'c1', authorId: 'u1', issueId: 'i1', text: 'first-EDITED', createdAt: 1},
    }));
    queryDelegate.commit();

    // Wait for the edit to appear in the DOM
    await expect
      .poll(() => element.querySelector('[data-testid="issue-i1"]')?.textContent)
      .toContain('first-EDITED');

    // issue1's component MUST have re-rendered (its comment changed)
    expect(issueRenders['i1']).toBeGreaterThan(issue1RendersAfterHydration);

    // issue2's component must NOT have re-rendered (unrelated, React.memo skips)
    expect(issueRenders['i2']).toBe(issue2RendersAfterHydration);

    view.destroy();
  });
});
