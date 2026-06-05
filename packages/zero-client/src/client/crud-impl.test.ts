import {expect, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import * as crudImpl from './crud-impl.ts';
import {IVMSourceBranch} from './ivm-branch.ts';
import {aggregateRowKey} from './keys.ts';
import type {WriteTransaction} from './replicache-types.ts';

/**
 * Optimistic aggregate deltas (chunk 10): when a child row is locally
 * inserted/deleted, the CRUD executor bumps the synced aggregate's Replicache
 * row by ±1 so the count updates immediately. Replicache's rebase then
 * reconciles it against the server's authoritative value (covered by the
 * rebase tests in custom.test.ts — the aggregate row is just another row).
 */

const schema = createSchema({
  tables: [
    table('issue').columns({id: string()}).primaryKey('id'),
    table('comment')
      .columns({
        id: string(),
        issueId: string(),
        points: number().optional(),
      })
      .primaryKey('id'),
  ],
});

const AGG = 'aggregate:q1:commentCount';
const SUM_AGG = 'aggregate:q2:pointsSum';

function makeTx() {
  const map = new Map<string, unknown>();
  const tx = {
    get: (k: string) => Promise.resolve(map.get(k)),
    has: (k: string) => Promise.resolve(map.has(k)),
    set: (k: string, v: unknown) => {
      map.set(k, v);
      return Promise.resolve();
    },
    del: (k: string) => {
      map.delete(k);
      return Promise.resolve();
    },
  };
  return {map, tx: tx as unknown as WriteTransaction};
}

function branchWithCommentCount() {
  const branch = new IVMSourceBranch(schema.tables);
  // Mirror what the builder registers for `issue.related('comments', c=>c.count())`.
  branch.getOrCreateAggregateSource(
    AGG,
    {issueId: {type: 'string'}, value: {type: 'number'}},
    ['issueId'],
    {table: 'comment', childField: ['issueId'], fn: 'count', field: undefined},
  );
  return branch;
}

function branchWithPointsSum() {
  const branch = new IVMSourceBranch(schema.tables);
  // Mirror `issue.related('comments', c => c.sum('points'))`.
  branch.getOrCreateAggregateSource(
    SUM_AGG,
    {issueId: {type: 'string'}, value: {type: 'number'}},
    ['issueId'],
    {table: 'comment', childField: ['issueId'], fn: 'sum', field: 'points'},
  );
  return branch;
}

test('optimistic count: insert bumps +1, delete bumps -1', async () => {
  const branch = branchWithCommentCount();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(AGG, {issueId: 'i1'});
  // Server base: issue i1 currently has 2 comments.
  map.set(aggKey, {issueId: 'i1', value: 2, ['_0_version']: '01'});

  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i1'},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({issueId: 'i1', value: 3});

  await crudImpl.delete(
    tx,
    {op: 'delete', tableName: 'comment', primaryKey: ['id'], value: {id: 'c1'}},
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({issueId: 'i1', value: 2});
});

test('optimistic count: no row for the parent (not in result) → skipped', async () => {
  const branch = branchWithCommentCount();
  const {map, tx} = makeTx();

  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'iX'},
    },
    schema,
    branch,
  );
  // No aggregate row exists for iX, so nothing is created (count not displayed).
  expect(map.has(aggregateRowKey(AGG, {issueId: 'iX'}))).toBe(false);
});

test('optimistic count: tables with no registered aggregate are untouched', async () => {
  const branch = branchWithCommentCount();
  const {map, tx} = makeTx();

  await crudImpl.insert(
    tx,
    {op: 'insert', tableName: 'issue', primaryKey: ['id'], value: {id: 'i1'}},
    schema,
    branch,
  );
  // Inserting an issue writes only the issue row — no aggregate side effects.
  expect([...map.keys()]).toEqual(['e/issue/i1']);
});

test('optimistic count: deltas apply with no ivmBranch only via Replicache (none here)', async () => {
  // Legacy CRUD passes ivmBranch=undefined; without the registry there are no
  // optimistic aggregate writes (acceptable — legacy CRUD is deprecated).
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(AGG, {issueId: 'i1'});
  map.set(aggKey, {issueId: 'i1', value: 2, ['_0_version']: '01'});

  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i1'},
    },
    schema,
    undefined,
  );
  expect(map.get(aggKey)).toMatchObject({value: 2}); // unchanged
});

test('optimistic sum: insert adds the field, delete subtracts it', async () => {
  const branch = branchWithPointsSum();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(SUM_AGG, {issueId: 'i1'});
  // Server base: points sum for i1 = 10.
  map.set(aggKey, {issueId: 'i1', value: 10, ['_0_version']: '01'});

  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i1', points: 4},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({value: 14});

  await crudImpl.delete(
    tx,
    {
      op: 'delete',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1'},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({value: 10});
});

test('optimistic sum: a field edit adjusts by the difference', async () => {
  const branch = branchWithPointsSum();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(SUM_AGG, {issueId: 'i1'});
  map.set(aggKey, {issueId: 'i1', value: 10, ['_0_version']: '01'});

  // Insert points=4 (sum 14), then edit it to 9 (sum 19).
  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i1', points: 4},
    },
    schema,
    branch,
  );
  await crudImpl.update(
    tx,
    {
      op: 'update',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', points: 9},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({value: 19});
});

test('optimistic sum: null fields contribute 0', async () => {
  const branch = branchWithPointsSum();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(SUM_AGG, {issueId: 'i1'});
  map.set(aggKey, {issueId: 'i1', value: 10, ['_0_version']: '01'});

  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i1', points: null},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({value: 10}); // unchanged
});

const AVG_AGG = 'aggregate:q3:pointsAvg';

function branchWithPointsAvg() {
  const branch = new IVMSourceBranch(schema.tables);
  // Mirror `issue.related('comments', c => c.avg('points'))`.
  branch.getOrCreateAggregateSource(
    AVG_AGG,
    {
      issueId: {type: 'string'},
      value: {type: 'number'},
      sum: {type: 'number'},
      count: {type: 'number'},
    },
    ['issueId'],
    {table: 'comment', childField: ['issueId'], fn: 'avg', field: 'points'},
  );
  return branch;
}

test('optimistic avg: adjusts sum + count and recomputes the ratio', async () => {
  const branch = branchWithPointsAvg();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(AVG_AGG, {issueId: 'i1'});
  // Server base: avg 10 over sum 30 / count 3.
  map.set(aggKey, {
    issueId: 'i1',
    value: 10,
    sum: 30,
    count: 3,
    ['_0_version']: '01',
  });

  // Insert points=6 → sum 36 / count 4 → avg 9.
  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i1', points: 6},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({value: 9, sum: 36, count: 4});

  // Delete it → back to sum 30 / count 3 → avg 10.
  await crudImpl.delete(
    tx,
    {op: 'delete', tableName: 'comment', primaryKey: ['id'], value: {id: 'c1'}},
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({value: 10, sum: 30, count: 3});
});

test('optimistic avg: a null field contributes nothing', async () => {
  const branch = branchWithPointsAvg();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(AVG_AGG, {issueId: 'i1'});
  map.set(aggKey, {
    issueId: 'i1',
    value: 5,
    sum: 5,
    count: 1,
    ['_0_version']: '01',
  });

  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i1', points: null},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({value: 5, sum: 5, count: 1}); // unchanged
});

test('optimistic avg: deleting the last contributor → null', async () => {
  const branch = branchWithPointsAvg();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(AVG_AGG, {issueId: 'i1'});
  // Empty group: avg null, sum 0, count 0.
  map.set(aggKey, {
    issueId: 'i1',
    value: null,
    sum: 0,
    count: 0,
    ['_0_version']: '01',
  });

  // Add one contributor → avg 7.
  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i1', points: 7},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({value: 7, sum: 7, count: 1});

  // Remove it → empty again → null (count drives the empty case correctly).
  await crudImpl.delete(
    tx,
    {op: 'delete', tableName: 'comment', primaryKey: ['id'], value: {id: 'c1'}},
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({value: null, sum: 0, count: 0});
});
