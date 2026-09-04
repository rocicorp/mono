import {LogContext} from '@rocicorp/logger';
import {expect, test, vi} from 'vitest';
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

function newContext() {
  let batchCalls = 0;
  const context = new ZeroContext(
    new LogContext('info'),
    new IVMSourceBranch(schema.tables),
    (() => () => {}) as unknown as AddQuery,
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
  return {context, batchCalls: () => batchCalls};
}

const add = (id: string, name: string) => ({
  key: `${ENTITIES_KEY_PREFIX}t1/${id}`,
  op: 'add' as const,
  newValue: {id, name},
});

test('pipelines are ready by default', () => {
  const {context} = newContext();
  expect(context.pipelinesReady).toBe(true);
  context.processChanges(undefined, 'h1' as Hash, [add('e1', 'one')]);
  const view = context.materialize(newQuery(schema, 't1'));
  expect(view.data).toMatchObject([{id: 'e1'}]);
  view.destroy();
});

test('views materialized while deferred hydrate when pipelines become ready', () => {
  const {context, batchCalls} = newContext();
  context.deferPipelines();
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
  context.deferPipelines();
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
  context.deferPipelines();
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
  context.deferPipelines();
  const otherSchema = createSchema({
    tables: [table('t2').columns({id: string()}).primaryKey('id')],
  });

  expect(() => context.materialize(newQuery(otherSchema, 't2'))).toThrow();
});

test('a deferred pipeline that fails at attach is logged and does not strand the others', async () => {
  const {context} = newContext();
  context.deferPipelines();
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
