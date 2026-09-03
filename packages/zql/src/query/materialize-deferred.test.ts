import {expect, test, vi} from 'vitest';
import {must} from '../../../shared/src/must.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import {ArrayView} from '../ivm/array-view.ts';
import {makeSourceChangeAdd, type Source} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import type {MetricMap} from './metrics-delegate.ts';
import {newQuery} from './query-impl.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';
import {schema} from './test/test-schemas.ts';
import type {ResultType} from './typed-view.ts';

/**
 * A delegate whose pipelines are not ready until `markReady()` is called,
 * mirroring what ZeroContext does between construction and the replica being
 * loaded into the IVM sources.
 */
class DeferredDelegate extends QueryDelegateImpl {
  #ready = false;
  readonly #pending = new Set<() => void>();
  readonly metrics: string[] = [];
  getSourceCalls = 0;

  override get pipelinesReady(): boolean {
    return this.#ready;
  }

  override onPipelinesReady(cb: () => void): () => void {
    this.#pending.add(cb);
    return () => {
      this.#pending.delete(cb);
    };
  }

  get pendingCount(): number {
    return this.#pending.size;
  }

  override getSource(name: string): Source {
    this.getSourceCalls++;
    if (this.failingTables.has(name)) {
      throw new Error(`boom: ${name}`);
    }
    return super.getSource(name);
  }

  override addMetric<K extends keyof MetricMap>(
    metric: K,
    _value: number,
    ..._args: MetricMap[K]
  ): void {
    this.metrics.push(metric);
  }

  /** Tables for which getSource throws, to simulate a pipeline build failure. */
  readonly failingTables = new Set<string>();

  markReady(): void {
    this.#ready = true;
    const pending = [...this.#pending];
    this.#pending.clear();
    this.batchViewUpdates(() => {
      for (const attach of pending) {
        try {
          attach();
        } catch {
          // mirrors ZeroContext.markPipelinesReady: log and continue
        }
      }
    });
    this.commit();
  }
}

function newDelegate() {
  const delegate = new DeferredDelegate();
  // Load sources directly, bypassing getSource counting, as ZeroRep.init does.
  const issues = must(
    QueryDelegateImpl.prototype.getSource.call(delegate, 'issue'),
  );
  const comments = must(
    QueryDelegateImpl.prototype.getSource.call(delegate, 'comment'),
  );
  const addIssue = (id: string) =>
    consume(
      issues.push(
        makeSourceChangeAdd({
          id,
          title: `issue ${id}`,
          description: '',
          closed: false,
          ownerId: null,
          createdAt: 1,
        }),
      ),
    );
  const addComment = (id: string, issueId: string) =>
    consume(
      comments.push(
        makeSourceChangeAdd({
          id,
          authorId: 'u1',
          issueId,
          text: `comment ${id}`,
          createdAt: 1,
        }),
      ),
    );
  return {delegate, addIssue, addComment};
}

test('materialize before ready returns an empty, unknown view and registers the server query', () => {
  const {delegate} = newDelegate();
  const view = delegate.materialize(newQuery(schema, 'issue'));
  const listener = vi.fn();
  view.addListener(listener);

  expect(view.data).toEqual([]);
  expect(listener).toHaveBeenCalledTimes(1);
  expect(listener.mock.calls[0][1]).toBe('unknown');
  expect(delegate.addedServerQueries).toHaveLength(1);
  expect(delegate.getSourceCalls).toBe(0);
  expect(delegate.pendingCount).toBe(1);
  expect(delegate.metrics).not.toContain('query-materialization-client');

  view.destroy();
});

test('rows loaded while deferred appear once pipelines are ready', () => {
  const {delegate, addIssue} = newDelegate();
  const view = delegate.materialize(newQuery(schema, 'issue'));
  const listener = vi.fn();
  view.addListener(listener);

  addIssue('i1');
  addIssue('i2');
  expect(view.data).toEqual([]);
  expect(listener).toHaveBeenCalledTimes(1);

  delegate.markReady();

  expect(delegate.pendingCount).toBe(0);
  expect(delegate.getSourceCalls).toBeGreaterThan(0);
  expect(view.data).toMatchObject([{id: 'i1'}, {id: 'i2'}]);
  expect(listener).toHaveBeenCalledTimes(2);
  expect(listener.mock.calls[1][1]).toBe('unknown');
  expect(delegate.metrics).toContain('query-materialization-client');

  view.destroy();
});

test('destroy before ready never builds the pipeline', () => {
  const {delegate, addIssue} = newDelegate();
  const view = delegate.materialize(newQuery(schema, 'issue'));
  addIssue('i1');
  view.destroy();

  expect(delegate.pendingCount).toBe(0);
  delegate.markReady();
  expect(delegate.getSourceCalls).toBe(0);
});

test('complete is gated on both got and the pipeline being attached', async () => {
  const {delegate, addIssue} = newDelegate();
  const view = delegate.materialize(newQuery(schema, 'issue'));
  let resultType: ResultType = 'unknown';
  view.addListener((_data, type) => {
    resultType = type;
  });
  addIssue('i1');

  // got arrives before the sources are loaded
  delegate.callAllGotCallbacks();
  await Promise.resolve();
  expect(resultType).toBe('unknown');

  delegate.markReady();
  await vi.waitFor(() => expect(resultType).toBe('complete'));
  expect(view.data).toMatchObject([{id: 'i1'}]);

  view.destroy();
});

test('got after ready completes as before', async () => {
  const {delegate, addIssue} = newDelegate();
  const view = delegate.materialize(newQuery(schema, 'issue'));
  let resultType: ResultType = 'unknown';
  view.addListener((_data, type) => {
    resultType = type;
  });
  addIssue('i1');

  delegate.markReady();
  await Promise.resolve();
  expect(resultType).toBe('unknown');

  delegate.callAllGotCallbacks();
  await vi.waitFor(() => expect(resultType).toBe('complete'));

  view.destroy();
});

test('run({type: complete}) resolves with data once ready and got', async () => {
  const {delegate, addIssue} = newDelegate();
  addIssue('i1');
  const p = delegate.run(newQuery(schema, 'issue'), {type: 'complete'});

  delegate.callAllGotCallbacks();
  delegate.markReady();

  expect(await p).toMatchObject([{id: 'i1'}]);
});

test('updateTTL before ready is forwarded to the server query', () => {
  const {delegate} = newDelegate();
  const view = delegate.materialize(newQuery(schema, 'issue'));
  view.updateTTL(12_345);
  expect(delegate.addedServerQueries[0].ttl).toBe(12_345);
  view.destroy();
});

test('limit and related hydrate correctly after ready and stay incremental', () => {
  const {delegate, addIssue, addComment} = newDelegate();
  const view = delegate.materialize(
    newQuery(schema, 'issue').related('comments').limit(2),
  );

  addIssue('i1');
  addIssue('i2');
  addIssue('i3');
  addComment('c1', 'i1');
  addComment('c2', 'i1');
  addComment('c3', 'i3');
  expect(view.data).toEqual([]);

  delegate.markReady();
  expect(view.data).toMatchObject([
    {id: 'i1', comments: [{id: 'c1'}, {id: 'c2'}]},
    {id: 'i2', comments: []},
  ]);

  // The attached pipeline is live: a new first row pushes i2 out of the limit.
  addIssue('i0');
  addComment('c4', 'i0');
  delegate.commit();
  expect(view.data).toMatchObject([
    {id: 'i0', comments: [{id: 'c4'}]},
    {id: 'i1', comments: [{id: 'c1'}, {id: 'c2'}]},
  ]);

  view.destroy();
});

test('materialize after ready builds the pipeline immediately', () => {
  const {delegate, addIssue} = newDelegate();
  addIssue('i1');
  delegate.markReady();

  const view = delegate.materialize(newQuery(schema, 'issue'));
  expect(delegate.pendingCount).toBe(0);
  expect(delegate.getSourceCalls).toBeGreaterThan(0);
  expect(view.data).toMatchObject([{id: 'i1'}]);
  expect(delegate.metrics).toContain('query-materialization-client');

  view.destroy();
});

test('deferred pipelines attach in materialization order', () => {
  const {delegate, addIssue} = newDelegate();
  const order: string[] = [];
  const a = delegate.materialize(newQuery(schema, 'issue'));
  const b = delegate.materialize(newQuery(schema, 'issue').where('id', 'i1'));
  a.addListener(data => {
    if (data.length > 0) order.push('a');
  });
  b.addListener(data => {
    if (data.length > 0) order.push('b');
  });
  addIssue('i1');

  delegate.markReady();
  expect(order).toEqual(['a', 'b']);

  a.destroy();
  b.destroy();
});

test('end-to-end metric is recorded once the view is hydrated, not on got alone', () => {
  const {delegate, addIssue} = newDelegate();
  const view = delegate.materialize(newQuery(schema, 'issue'));
  addIssue('i1');

  delegate.callAllGotCallbacks();
  expect(delegate.metrics).not.toContain('query-materialization-end-to-end');

  delegate.markReady();
  expect(delegate.metrics).toContain('query-materialization-end-to-end');

  view.destroy();
});

test('a factory that reads the schema before ready gets a live pipeline', () => {
  const {delegate, addIssue} = newDelegate();
  let schemaTable: string | undefined;
  const view = delegate.materialize(
    newQuery(schema, 'issue'),
    (
      _q,
      input,
      format,
      onDestroy,
      onTransactionCommit,
      queryComplete,
      updateTTL,
    ) => {
      // Reads the schema at construction, as ArrayView did before deferral.
      schemaTable = input.getSchema().tableName;
      const v = new ArrayView(input, format, queryComplete, updateTTL);
      v.onDestroy = onDestroy;
      onTransactionCommit(() => v.flush());
      return v;
    },
  );
  expect(schemaTable).toBe('issue');
  expect(delegate.getSourceCalls).toBeGreaterThan(0);

  // The pipeline was built eagerly and is connected: rows pushed before ready
  // arrive through the normal push path.
  addIssue('i1');
  delegate.commit();
  expect(view.data).toMatchObject([{id: 'i1'}]);

  // markReady is a no-op for this view.
  delegate.markReady();
  expect(view.data).toMatchObject([{id: 'i1'}]);
  view.destroy();
});

test('a factory that never calls setOutput does not break the drain', () => {
  const {delegate, addIssue} = newDelegate();
  const view = delegate.materialize(
    newQuery(schema, 'issue'),
    (_q, _input, _f, onDestroy) => ({
      destroy: onDestroy,
    }),
  );
  addIssue('i1');
  expect(() => delegate.markReady()).not.toThrow();
  view.destroy();
});

test('a pipeline that fails to build marks its view as errored and does not block others', async () => {
  const {delegate, addIssue} = newDelegate();
  delegate.failingTables.add('comment');
  const bad = delegate.materialize(newQuery(schema, 'comment'));
  const good = delegate.materialize(newQuery(schema, 'issue'));
  let badType: ResultType = 'unknown';
  let badError: ErroredQuery | undefined;
  bad.addListener((_d, type, error) => {
    badType = type;
    badError = error;
  });
  addIssue('i1');

  delegate.markReady();

  expect(good.data).toMatchObject([{id: 'i1'}]);
  await vi.waitFor(() => expect(badType).toBe('error'));
  expect(badError).toMatchObject({error: 'app', message: 'boom: comment'});

  bad.destroy();
  good.destroy();
});
