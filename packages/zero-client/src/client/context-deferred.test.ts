import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {describe, expect, test, vi} from 'vitest';
import type {Hash} from '../../../replicache/src/hash.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {newQuery} from '../../../zql/src/query/query-impl.ts';
import {
  ZeroContext,
  type AddCustomQuery,
  type AddQuery,
  type FlushQueryChanges,
  type UpdateCustomQuery,
  type UpdateQuery,
} from './context.ts';
import {IVMSourceBranch} from './ivm-branch.ts';
import {ENTITIES_KEY_PREFIX} from './keys.ts';

const schema = createSchema({
  tables: [
    table('t1')
      .columns({
        id: string(),
        name: string(),
      })
      .primaryKey('id'),
  ],
});

type Got = (got: boolean | 'cached') => void;

function newContext() {
  let batchCalls = 0;
  // The got callbacks of the queries added so far.
  const gots: Got[] = [];
  const context = new ZeroContext(
    new LogContext('info'),
    new IVMSourceBranch(schema.tables),
    ((_ast: unknown, _ttl: unknown, got: Got) => {
      gots.push(got);
      return () => {};
    }) as unknown as AddQuery,
    (() => () => {}) as unknown as AddCustomQuery,
    (() => {}) as unknown as UpdateQuery,
    (() => {}) as unknown as UpdateCustomQuery,
    (() => {}) as unknown as FlushQueryChanges,
    applyViewUpdates => {
      batchCalls++;
      applyViewUpdates();
    },
    () => {},
    () => {},
  );
  return {context, batchCalls: () => batchCalls, gots};
}

const add = (id: string, name: string) => ({
  key: `${ENTITIES_KEY_PREFIX}t1/${id}`,
  op: 'add' as const,
  newValue: {id, name},
});

test('pipelines are ready once marked ready', () => {
  const {context} = newContext();
  expect(context.pipelinesReady).toBe(false);
  context.markPipelinesReady();
  expect(context.pipelinesReady).toBe(true);
  context.processChanges(undefined, 'h1' as Hash, [add('e1', 'one')]);
  const view = context.materialize(newQuery(schema, 't1'));
  expect(view.data).toMatchObject([{id: 'e1'}]);
  view.destroy();
});

test('views materialized while deferred hydrate when pipelines become ready', () => {
  const {context, batchCalls} = newContext();
  expect(context.pipelinesReady).toBe(false);

  const view = context.materialize(newQuery(schema, 't1'));
  const listener = vi.fn();
  view.addListener(listener);
  expect(view.data).toEqual([]);

  // The replica is loaded into the sources. No pipeline is connected, so the
  // view does not change yet.
  context.processChanges(undefined, 'h1' as Hash, [
    add('e1', 'one'),
    add('e2', 'two'),
  ]);
  expect(view.data).toEqual([]);
  expect(listener).toHaveBeenCalledTimes(1);

  const batchesBefore = batchCalls();
  context.markPipelinesReady();
  expect(context.pipelinesReady).toBe(true);
  expect(batchCalls()).toBe(batchesBefore + 1);
  expect(view.data).toMatchObject([{id: 'e1'}, {id: 'e2'}]);
  expect(listener).toHaveBeenCalledTimes(2);

  // The attached pipeline receives later changes incrementally.
  context.processChanges('h1' as Hash, 'h2' as Hash, [add('e3', 'three')]);
  expect(view.data).toMatchObject([{id: 'e1'}, {id: 'e2'}, {id: 'e3'}]);
  expect(listener).toHaveBeenCalledTimes(3);

  view.destroy();
});

test('markPipelinesReady is idempotent and materialize after it is immediate', () => {
  const {context, batchCalls} = newContext();
  context.processChanges(undefined, 'h1' as Hash, [add('e1', 'one')]);
  context.markPipelinesReady();
  const batches = batchCalls();
  context.markPipelinesReady();
  expect(batchCalls()).toBe(batches);

  const view = context.materialize(newQuery(schema, 't1'));
  expect(view.data).toMatchObject([{id: 'e1'}]);
  view.destroy();
});

test('a view destroyed while deferred is not hydrated', () => {
  const {context} = newContext();
  const view = context.materialize(newQuery(schema, 't1'));
  const listener = vi.fn();
  view.addListener(listener);
  view.destroy();

  context.processChanges(undefined, 'h1' as Hash, [add('e1', 'one')]);
  context.markPipelinesReady();
  expect(listener).toHaveBeenCalledTimes(1);
  expect(view.data).toEqual([]);
});

test('a query for an unknown table throws from materialize, as before deferral', () => {
  const {context} = newContext();
  const otherSchema = createSchema({
    tables: [table('t2').columns({id: string()}).primaryKey('id')],
  });

  expect(() => context.materialize(newQuery(otherSchema, 't2'))).toThrow();
});

test('a deferred pipeline that fails at attach is logged and does not strand the others', async () => {
  const {context} = newContext();
  const view = context.materialize(newQuery(schema, 't1'));
  let type = 'unknown';
  view.addListener((_d, t) => {
    type = t;
  });

  // A diff for a table the schema does not know poisons the source branch, so
  // rebuilding the pipeline at attach time fails.
  expect(() =>
    context.processChanges(undefined, 'h1' as Hash, [
      {
        key: `${ENTITIES_KEY_PREFIX}nosuch/e1`,
        op: 'add',
        newValue: {id: 'e1'},
      },
    ]),
  ).toThrow();

  expect(() => context.markPipelinesReady()).not.toThrow();
  await vi.waitFor(() => expect(type).toBe('error'));

  view.destroy();
});

describe('hydratePendingPipelines', () => {
  // Hydrates one pipeline per slice and parks at every yield until `step()`.
  function sliced(context: ZeroContext) {
    let resume: (() => void) | undefined;
    let finished = false;
    let settled = resolver<void>();
    const done = context
      .hydratePendingPipelines(
        0,
        () =>
          new Promise<void>(resolve => {
            resume = resolve;
            settled.resolve();
          }),
      )
      .finally(() => {
        finished = true;
        settled.resolve();
      });
    // Resumes the loop and waits until it parks again or finishes, however
    // many microtasks that takes.
    const step = async () => {
      const r = resume;
      resume = undefined;
      settled = resolver<void>();
      r?.();
      await settled.promise;
    };
    return {done, step, parked: () => resume !== undefined && !finished};
  }

  function loaded() {
    const c = newContext();
    c.context.processChanges(undefined, 'h1' as Hash, [
      add('e1', 'one'),
      add('e2', 'two'),
    ]);
    return c;
  }

  test('yields between pipelines and releases every view in one batch', async () => {
    const {context, batchCalls} = loaded();
    const views = [1, 2, 3].map(() =>
      context.materialize(newQuery(schema, 't1')),
    );
    const calls: string[] = [];
    // Each listener also reads the last view: once any view is notified all
    // of them show their rows, including ones not notified yet.
    views.forEach((v, i) =>
      v.addListener(d =>
        calls.push(`${i}:${d.length}/${views[2].data.length}`),
      ),
    );
    calls.length = 0;

    const {done, step, parked} = sliced(context);
    // First slice ran synchronously: one pipeline hydrated, nothing exposed.
    expect(parked()).toBe(true);
    expect(context.pipelinesReady).toBe(false);
    expect(views.map(v => v.data.length)).toEqual([0, 0, 0]);

    // A listener added now sees the committed (empty) snapshot, not the rows
    // already pushed into the first view.
    const late = vi.fn();
    views[0].addListener(late);
    expect(late).toHaveBeenLastCalledWith([], 'unknown', undefined);

    await step();
    expect(parked()).toBe(true);
    expect(calls).toEqual([]);

    const batchesBefore = batchCalls();
    await step();
    await done;
    expect(context.pipelinesReady).toBe(true);
    expect(calls).toEqual(['0:2/2', '1:2/2', '2:2/2']);
    expect(late).toHaveBeenCalledTimes(2);
    // The last slice and the release.
    expect(batchCalls()).toBe(batchesBefore + 2);
    for (const v of views) {
      v.destroy();
    }
  });

  test('a query materialized while hydrating joins the same release', async () => {
    const {context} = loaded();
    const first = context.materialize(newQuery(schema, 't1'));
    const second = context.materialize(newQuery(schema, 't1').limit(1));
    const calls: string[] = [];
    first.addListener(d => calls.push(`first:${d.length}`));
    second.addListener(d => calls.push(`second:${d.length}`));

    const {done, step} = sliced(context);
    const joined = context.materialize(newQuery(schema, 't1').limit(2));
    joined.addListener(d => calls.push(`joined:${d.length}`));
    expect(joined.data).toEqual([]);
    calls.length = 0;

    await step();
    await step();
    await done;
    expect(calls).toEqual(['first:2', 'second:1', 'joined:2']);

    // After the release materialize is immediate again.
    expect(context.materialize(newQuery(schema, 't1')).data).toHaveLength(2);
  });

  test('complete is not reported before the release', async () => {
    const {context, gots} = newContext();
    // No rows: hydration leaves the views clean, so only the deferred
    // release keeps 'complete' from firing as soon as the first one attaches.
    const a = context.materialize(newQuery(schema, 't1'));
    const b = context.materialize(newQuery(schema, 't1').limit(1));
    const calls: string[] = [];
    a.addListener((_, type) => calls.push(`a:${type}`));
    b.addListener((_, type) => calls.push(`b:${type}`));
    gots.forEach(got => got(true));
    calls.length = 0;

    const {done, step} = sliced(context);
    // Completion is delivered through a promise; give an early one every
    // chance to show up.
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(calls).toEqual([]);
    await step();
    await done;
    await vi.waitFor(() => expect(calls).toEqual(['a:complete', 'b:complete']));
  });

  test('cached is reported only once every view is released, empty views included', async () => {
    const {context, gots} = newContext();
    // No rows, so hydration leaves both views clean.
    const a = context.materialize(newQuery(schema, 't1'));
    const b = context.materialize(newQuery(schema, 't1').limit(1));
    gots.forEach(got => got('cached'));

    const calls: string[] = [];
    a.addListener((_, type) => {
      if (type !== 'cached') {
        return;
      }
      // What b says at the moment a reports cached.
      b.addListener((_, bType) => calls.push(`b seen from a:${bType}`))();
      calls.push('a:cached');
      throw new Error('a listener that throws');
    });
    b.addListener((_, type) => calls.push(`b:${type}`));
    calls.length = 0;

    const {done, step} = sliced(context);
    expect(calls).toEqual([]);
    await step();
    await done;
    expect(calls).toEqual(['b seen from a:cached', 'a:cached', 'b:cached']);
  });

  test('a view destroyed while hydrating is skipped', async () => {
    const {context} = loaded();
    const hydrated = context.materialize(newQuery(schema, 't1'));
    const pending = context.materialize(newQuery(schema, 't1').limit(1));
    const hydratedListener = vi.fn();
    hydrated.addListener(hydratedListener);

    const {done, step} = sliced(context);
    hydrated.destroy();
    pending.destroy();
    await step();
    await done;
    expect(context.pipelinesReady).toBe(true);
    expect(hydratedListener).toHaveBeenCalledTimes(1);
  });

  test('processChanges while hydrating is rejected', async () => {
    const {context} = loaded();
    context.materialize(newQuery(schema, 't1'));
    context.materialize(newQuery(schema, 't1').limit(1));
    const {done, step} = sliced(context);
    expect(() =>
      context.processChanges('h1' as Hash, 'h2' as Hash, [add('e3', 'three')]),
    ).toThrow('while pipelines are being hydrated');
    await step();
    await done;
  });

  test('nothing pending resolves without a batch', async () => {
    const {context, batchCalls} = newContext();
    await context.hydratePendingPipelines();
    expect(context.pipelinesReady).toBe(true);
    expect(batchCalls()).toBe(0);
  });
});
