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

function mockViewStore(suffix: string) {
  const viewStore = new ViewStore();
  const query = newMockQuery(`q-${suffix}`);
  const zero = newMockZero(`c-${suffix}`);
  const view = viewStore.getView(zero, query, true, 'forever');
  const cleanup = view.subscribeReactInternals(() => {});
  return {view, zero, cleanup};
}

function snapData(view: {getSnapshot: () => readonly [unknown, ...unknown[]]}) {
  return view.getSnapshot()[0];
}

function snapLength(view: {getSnapshot: () => readonly [unknown, ...unknown[]]}) {
  return (snapData(view) as unknown[]).length;
}

describe('Snapshot identity', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  //   getSnapshot()  -->  ref1
  //   getSnapshot()  -->  ref1  (same, no data change)
  //   emit([row])
  //   getSnapshot()  -->  ref2  (new, data changed)
  //   getSnapshot()  -->  ref2  (same, no further change)
  test('same reference without changes, new reference after data', () => {
    const {view, zero, cleanup} = mockViewStore('identity');

    expect(view.getSnapshot()).toBe(view.getSnapshot());

    emit(zero, [{id: '1'}]);
    const withData = view.getSnapshot();
    expect(view.getSnapshot()).toBe(withData);

    emit(zero, [{id: '1'}, {id: '2'}]);
    expect(view.getSnapshot()).not.toBe(withData);

    cleanup();
  });

  //   getSnapshot()  -->  [[], {type:'unknown'}]  (sentinel A)
  //   emit([])
  //   getSnapshot()  -->  [[], {type:'unknown'}]  (sentinel A, same ref)
  test('empty snapshots use sentinel objects (no spurious re-renders)', () => {
    const {view, zero, cleanup} = mockViewStore('sentinel');

    const empty1 = view.getSnapshot();
    emit(zero, []);
    expect(view.getSnapshot()).toBe(empty1);

    const qSingular = newMockQuery('singular', true);
    const zeroSingular = newMockZero('c-singular');
    const viewStore = new ViewStore();
    const singular = viewStore.getView(zeroSingular, qSingular, true, 'forever');
    const cleanupSingular = singular.subscribeReactInternals(() => {});

    const s1 = singular.getSnapshot();
    emit(zeroSingular, undefined);
    expect(singular.getSnapshot()).toBe(s1);

    cleanup();
    cleanupSingular();
  });

  //   emit([row1, row2])    -->  snap1
  //   emit([row1, row2'])   -->  snap2
  //
  //   snap2[0]:  same ref as row1 (unchanged)
  //   snap2[1]:  row2' (new ref, changed)
  test('unchanged rows keep same reference in snapshot', () => {
    const {view, zero, cleanup} = mockViewStore('row-id');

    const row1 = {id: '1', name: 'Alice'};
    emit(zero, [row1, {id: '2', name: 'Bob'}]);

    const row2Updated = {id: '2', name: 'Bob Updated'};
    emit(zero, [row1, row2Updated]);

    const data = snapData(view) as Array<{id: string; name: string}>;
    expect(data[0]).toBe(row1);
    expect(data[1]).toBe(row2Updated);

    cleanup();
  });
});

describe('No data flash (data to empty to data)', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  //   emit([row1])         -->  snap has data
  //   emit([row1, row2])   -->  snap still has data
  //   At no point should snap become [] between these two updates.
  test('snapshot never goes empty between data updates', () => {
    const {view, zero, cleanup} = mockViewStore('flash');

    const lengths: number[] = [];
    view.subscribeReactInternals(() => { lengths.push(snapLength(view)); });

    emit(zero, [{id: '1'}]);
    emit(zero, [{id: '1'}, {id: '2'}]);

    let hadData = false;
    for (const len of lengths) {
      if (len > 0) hadData = true;
      if (hadData) expect(len).toBeGreaterThan(0);
    }

    cleanup();
  });

  //   emit([row])  -->  snap has data
  //   unsubscribe + 15ms  -->  view destroyed
  //   getSnapshot()  -->  snap still has data (stale, not empty)
  test('stale snapshot preserved after view destroy', () => {
    const {view, zero, cleanup} = mockViewStore('destroy');

    emit(zero, [{id: '1'}], 'complete');
    expect(snapLength(view)).toBe(1);

    cleanup();
    vi.advanceTimersByTime(15);

    expect(snapLength(view)).toBe(1);
  });

  //   emit([row])   -->  [1]
  //   emit([])      -->  [0]  (legitimate empty)
  //   emit([row2])  -->  [1]
  test('legitimate empty transition is visible (not masked)', () => {
    const {view, zero, cleanup} = mockViewStore('legit-empty');

    const lengths: number[] = [];
    view.subscribeReactInternals(() => { lengths.push(snapLength(view)); });

    emit(zero, [{id: '1'}]);
    emit(zero, [], 'complete');
    emit(zero, [{id: '2'}], 'complete');

    expect(lengths).toEqual([1, 0, 1]);

    cleanup();
  });
});

describe('React.memo render counting', () => {
  let root: Root;
  let element: HTMLDivElement;
  const cleanups: Array<() => void> = [];

  beforeEach(() => {
    vi.useRealTimers();
    element = document.createElement('div');
    document.body.appendChild(element);
    root = createRoot(element);
  });

  afterEach(() => {
    for (const fn of cleanups) fn();
    cleanups.length = 0;
    root.unmount();
    document.body.removeChild(element);
  });

  //   Parent (useSyncExternalStore)
  //     +-- ChildRow(row1)  React.memo  <-- same ref, skip
  //     +-- ChildRow(row2)  React.memo  <-- new ref, re-renders
  test('mock view: only the changed row child re-renders', async () => {
    const {view: viewRef, zero} = mockViewStore('memo');

    const parentRenders = {current: 0};
    const childRenders: Record<string, number> = {};
    type Row = {id: string; name: string};

    const ChildRow = memo(function ChildRow({row}: {row: Row}) {
      childRenders[row.id] = (childRenders[row.id] ?? 0) + 1;
      return <div data-testid={`row-${row.id}`}>{row.name}</div>;
    });

    function Parent() {
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
    emit(zero, [row1, {id: '2', name: 'Bob'}]);

    await expect.poll(() => element.querySelector('[data-testid="row-1"]')?.textContent).toBe('Alice');
    const rendersAfterData = parentRenders.current;
    const child1After = childRenders['1'] ?? 0;

    emit(zero, [row1, {id: '2', name: 'Bob Updated'}]);

    await expect.poll(() => element.querySelector('[data-testid="row-2"]')?.textContent).toBe('Bob Updated');
    expect(parentRenders.current).toBeGreaterThan(rendersAfterData);
    expect(childRenders['1']).toBe(child1After);
    expect(childRenders['2']).toBeGreaterThan(1);
  });

  //   issue1 ─── owner:Alice        edit comment1       issue1' ─── owner:Alice (same ref)
  //           ├── comment1            ──────────►               ├── comment1' (new ref)
  //           └── comment2                                      └── comment2 (same ref)
  //   issue2 ─── owner:Bob                               issue2 (same ref, unrelated)
  //           └── comment3                                     └── comment3 (same ref)
  //
  //   <IssueRow issue={issue1}>  re-renders (descendant changed)
  //   <IssueRow issue={issue2}>  skips (React.memo, same ref)
  test('real IVM pipeline: editing a comment only re-renders the parent issue', async () => {
    const queryDelegate = new QueryDelegateImpl({callGot: true});
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

    const view = queryDelegate.materialize(
      newQuery(schema, 'issue').related('owner').related('comments'),
    );
    cleanups.push(() => view.destroy());

    type IssueRow = {id: string; title: string; comments: Array<{id: string; text: string}>; owner: {name: string} | undefined};
    let snapshot: unknown[] = [];
    const subscribers = new Set<() => void>();
    view.addListener(data => {
      snapshot = data as unknown[];
      for (const cb of subscribers) cb();
    });
    const subscribe = (cb: () => void) => { subscribers.add(cb); return () => { subscribers.delete(cb); }; };
    const getSnapshot = () => snapshot;

    const issueRenders: Record<string, number> = {};

    const IssueRowComponent = memo(function IssueRowComponent({issue}: {issue: IssueRow}) {
      issueRenders[issue.id] = (issueRenders[issue.id] ?? 0) + 1;
      const commentTexts = issue.comments.map(comment => comment.text).join(', ');
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
          {issues.map(issue => <IssueRowComponent key={issue.id} issue={issue} />)}
        </div>
      );
    }

    root.render(<IssueList />);

    await expect.poll(() => element.querySelector('[data-testid="issue-i1"]')?.textContent).toContain('Bug');
    await expect.poll(() => element.querySelector('[data-testid="issue-i2"]')?.textContent).toContain('Feature');

    const issue1After = issueRenders['i1'] ?? 0;
    const issue2After = issueRenders['i2'] ?? 0;
    expect(issue1After).toBeGreaterThanOrEqual(1);
    expect(issue2After).toBeGreaterThanOrEqual(1);

    consume(commentSource.push({
      type: 'edit',
      oldRow: {id: 'c1', authorId: 'u1', issueId: 'i1', text: 'first', createdAt: 1},
      row: {id: 'c1', authorId: 'u1', issueId: 'i1', text: 'first-EDITED', createdAt: 1},
    }));
    queryDelegate.commit();

    await expect.poll(() => element.querySelector('[data-testid="issue-i1"]')?.textContent).toContain('first-EDITED');

    expect(issueRenders['i1']).toBeGreaterThan(issue1After);
    expect(issueRenders['i2']).toBe(issue2After);
  });
});
