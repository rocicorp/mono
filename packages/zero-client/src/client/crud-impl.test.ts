import {expect, test} from 'vitest';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  json,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {groupKey} from '../../../zql/src/ivm/group-by.ts';
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
        // Nullable, as a foreign key often is: a comment may have no issue.
        issueId: string().optional(),
        points: number().optional(),
        meta: json().optional(),
      })
      .primaryKey('id'),
  ],
});

const AGG = 'aggregate:q1:commentCount';
const SUM_AGG = 'aggregate:q2:pointsSum';
const GROUP_SUM = 'aggregate:q3:#group';

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
    {issueId: {type: 'string'}, _0_value: {type: 'number'}},
    ['issueId'],
    {
      table: 'comment',
      childField: ['issueId'],
      keyField: ['issueId'],
      fn: 'count',
      field: undefined,
      predicate: undefined,
    },
  );
  return branch;
}

function branchWithGroupedPointsSum() {
  const branch = new IVMSourceBranch(schema.tables);
  // Mirror `comment.groupBy('issueId').sum('points')`.
  branch.getOrCreateAggregateSource(
    GROUP_SUM,
    {
      issueId: {type: 'string', optional: true},
      _0_key: {type: 'string'},
      _0_value: {type: 'number', optional: true},
      _0_sum: {type: 'number'},
      _0_count: {type: 'number'},
      _0_rows: {type: 'number'},
    },
    ['_0_key'],
    {
      table: 'comment',
      childField: ['issueId'],
      keyField: ['issueId'],
      fn: 'sum',
      field: 'points',
      predicate: undefined,
      grouped: true,
    },
  );
  return branch;
}

function branchWithPointsSum() {
  const branch = new IVMSourceBranch(schema.tables);
  // Mirror `issue.related('comments', c => c.sum('points'))`.
  branch.getOrCreateAggregateSource(
    SUM_AGG,
    {
      issueId: {type: 'string'},
      _0_value: {type: 'number'},
      _0_sum: {type: 'number'},
      _0_count: {type: 'number'},
    },
    ['issueId'],
    {
      table: 'comment',
      childField: ['issueId'],
      keyField: ['issueId'],
      fn: 'sum',
      field: 'points',
      predicate: undefined,
    },
  );
  return branch;
}

test('optimistic count: insert bumps +1, delete bumps -1', async () => {
  const branch = branchWithCommentCount();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(AGG, {issueId: 'i1'});
  // Server base: issue i1 currently has 2 comments.
  map.set(aggKey, {issueId: 'i1', _0_value: 2, ['_0_version']: '01'});

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
  expect(map.get(aggKey)).toMatchObject({issueId: 'i1', _0_value: 3});

  await crudImpl.delete(
    tx,
    {op: 'delete', tableName: 'comment', primaryKey: ['id'], value: {id: 'c1'}},
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({issueId: 'i1', _0_value: 2});
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

test('optimistic count: moving a child to another parent moves the count', async () => {
  const branch = branchWithCommentCount();
  const {map, tx} = makeTx();
  const fromKey = aggregateRowKey(AGG, {issueId: 'i1'});
  const toKey = aggregateRowKey(AGG, {issueId: 'i2'});
  map.set(fromKey, {issueId: 'i1', _0_value: 1, ['_0_version']: '01'});
  map.set(toKey, {issueId: 'i2', _0_value: 5, ['_0_version']: '01'});
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
  expect(map.get(fromKey)).toMatchObject({issueId: 'i1', _0_value: 2});

  await crudImpl.update(
    tx,
    {
      op: 'update',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i2'},
    },
    schema,
    branch,
  );
  expect(map.get(fromKey)).toMatchObject({issueId: 'i1', _0_value: 1});
  expect(map.get(toKey)).toMatchObject({issueId: 'i2', _0_value: 6});
});

test('optimistic count: a child with a null correlation value has no parent', async () => {
  const branch = branchWithCommentCount();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(AGG, {issueId: 'i1'});
  map.set(aggKey, {issueId: 'i1', _0_value: 1, ['_0_version']: '01'});

  // Inserting an orphan is not an error and moves no aggregate.
  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c0', issueId: null},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({issueId: 'i1', _0_value: 1});

  // Detaching a child from its parent decrements that parent.
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
  expect(map.get(aggKey)).toMatchObject({issueId: 'i1', _0_value: 2});
  await crudImpl.update(
    tx,
    {
      op: 'update',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: null},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({issueId: 'i1', _0_value: 1});
});

test('optimistic count: the aggregate row is keyed by its server column names', async () => {
  // A correlation column with a server name of its own: the server streams the
  // aggregate rows keyed by `issue_id`, while the child row says `issueId`.
  const branch = new IVMSourceBranch(schema.tables);
  branch.getOrCreateAggregateSource(
    AGG,
    {issue_id: {type: 'string'}, _0_value: {type: 'number'}},
    ['issue_id'],
    {
      table: 'comment',
      childField: ['issueId'],
      keyField: ['issue_id'],
      fn: 'count',
      field: undefined,
      predicate: undefined,
    },
  );
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(AGG, {issue_id: 'i1'});
  map.set(aggKey, {issue_id: 'i1', _0_value: 2, ['_0_version']: '01'});

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
  expect(map.get(aggKey)).toMatchObject({issue_id: 'i1', _0_value: 3});
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
  map.set(aggKey, {issueId: 'i1', _0_value: 2, ['_0_version']: '01'});

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
  expect(map.get(aggKey)).toMatchObject({_0_value: 2}); // unchanged
});

test('optimistic sum: insert adds the field, delete subtracts it', async () => {
  const branch = branchWithPointsSum();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(SUM_AGG, {issueId: 'i1'});
  // Server base: points sum for i1 = 10.
  map.set(aggKey, {
    issueId: 'i1',
    _0_value: 10,
    _0_sum: 10,
    _0_count: 2,
    ['_0_version']: '01',
  });

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
  expect(map.get(aggKey)).toMatchObject({_0_value: 14});

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
  expect(map.get(aggKey)).toMatchObject({_0_value: 10});
});

test('optimistic sum: a field edit adjusts by the difference', async () => {
  const branch = branchWithPointsSum();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(SUM_AGG, {issueId: 'i1'});
  map.set(aggKey, {
    issueId: 'i1',
    _0_value: 10,
    _0_sum: 10,
    _0_count: 2,
    ['_0_version']: '01',
  });

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
  expect(map.get(aggKey)).toMatchObject({_0_value: 19});
});

test('optimistic sum: null fields contribute 0', async () => {
  const branch = branchWithPointsSum();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(SUM_AGG, {issueId: 'i1'});
  map.set(aggKey, {
    issueId: 'i1',
    _0_value: 10,
    _0_sum: 10,
    _0_count: 2,
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
  expect(map.get(aggKey)).toMatchObject({_0_value: 10}); // unchanged
});

test('optimistic sum: null over no contributors, 0 over a 0', async () => {
  const branch = branchWithPointsSum();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(SUM_AGG, {issueId: 'i1'});
  // Empty group: sum null.
  map.set(aggKey, {
    issueId: 'i1',
    _0_value: null,
    _0_sum: 0,
    _0_count: 0,
    ['_0_version']: '01',
  });
  const insert = (id: string, points: number) =>
    crudImpl.insert(
      tx,
      {
        op: 'insert',
        tableName: 'comment',
        primaryKey: ['id'],
        value: {id, issueId: 'i1', points},
      },
      schema,
      branch,
    );

  await insert('c1', 5);
  expect(map.get(aggKey)).toMatchObject({_0_value: 5, _0_sum: 5, _0_count: 1});

  // Removing the last contributor leaves SQL's null, not 0.
  await crudImpl.delete(
    tx,
    {op: 'delete', tableName: 'comment', primaryKey: ['id'], value: {id: 'c1'}},
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({
    _0_value: null,
    _0_sum: 0,
    _0_count: 0,
  });

  // A first contributor of 0 makes it 0, not null.
  await insert('c2', 0);
  expect(map.get(aggKey)).toMatchObject({_0_value: 0, _0_sum: 0, _0_count: 1});
});

const AVG_AGG = 'aggregate:q3:pointsAvg';

function branchWithPointsAvg() {
  const branch = new IVMSourceBranch(schema.tables);
  // Mirror `issue.related('comments', c => c.avg('points'))`.
  branch.getOrCreateAggregateSource(
    AVG_AGG,
    {
      issueId: {type: 'string'},
      _0_value: {type: 'number'},
      _0_sum: {type: 'number'},
      _0_count: {type: 'number'},
    },
    ['issueId'],
    {
      table: 'comment',
      childField: ['issueId'],
      keyField: ['issueId'],
      fn: 'avg',
      field: 'points',
      predicate: undefined,
    },
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
    _0_value: 10,
    _0_sum: 30,
    _0_count: 3,
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
  expect(map.get(aggKey)).toMatchObject({_0_value: 9, _0_sum: 36, _0_count: 4});

  // Delete it → back to sum 30 / count 3 → avg 10.
  await crudImpl.delete(
    tx,
    {op: 'delete', tableName: 'comment', primaryKey: ['id'], value: {id: 'c1'}},
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({
    _0_value: 10,
    _0_sum: 30,
    _0_count: 3,
  });
});

test('optimistic avg: a null field contributes nothing', async () => {
  const branch = branchWithPointsAvg();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(AVG_AGG, {issueId: 'i1'});
  map.set(aggKey, {
    issueId: 'i1',
    _0_value: 5,
    _0_sum: 5,
    _0_count: 1,
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
  expect(map.get(aggKey)).toMatchObject({_0_value: 5, _0_sum: 5, _0_count: 1}); // unchanged
});

test('optimistic avg: deleting the last contributor → null', async () => {
  const branch = branchWithPointsAvg();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(AVG_AGG, {issueId: 'i1'});
  // Empty group: avg null, sum 0, count 0.
  map.set(aggKey, {
    issueId: 'i1',
    _0_value: null,
    _0_sum: 0,
    _0_count: 0,
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
  expect(map.get(aggKey)).toMatchObject({_0_value: 7, _0_sum: 7, _0_count: 1});

  // Remove it → empty again → null (count drives the empty case correctly).
  await crudImpl.delete(
    tx,
    {op: 'delete', tableName: 'comment', primaryKey: ['id'], value: {id: 'c1'}},
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({
    _0_value: null,
    _0_sum: 0,
    _0_count: 0,
  });
});

// --- where-filtered optimistic aggregates ---------------------------------
//
// For a `where`-filtered aggregate the builder compiles the `where` to a
// per-row predicate and registers it (see builder.ts). A child contributes to
// the delta only when it matches; an update that moves a child in or out of the
// filtered set deltas by ±1 (count) / ±field (sum) accordingly. Here we mirror
// `c => c.where('points', '>=', 5).count()` etc. with the predicate directly.

const COUNT_WHERE_AGG = 'aggregate:q4:bigCommentCount';
const SUM_WHERE_AGG = 'aggregate:q5:bigPointsSum';

const pointsAtLeast5 = (row: Row): boolean =>
  typeof row.points === 'number' && row.points >= 5;

function branchWithBigCommentCount() {
  const branch = new IVMSourceBranch(schema.tables);
  branch.getOrCreateAggregateSource(
    COUNT_WHERE_AGG,
    {issueId: {type: 'string'}, _0_value: {type: 'number'}},
    ['issueId'],
    {
      table: 'comment',
      childField: ['issueId'],
      keyField: ['issueId'],
      fn: 'count',
      field: undefined,
      predicate: pointsAtLeast5,
    },
  );
  return branch;
}

function branchWithBigPointsSum() {
  const branch = new IVMSourceBranch(schema.tables);
  branch.getOrCreateAggregateSource(
    SUM_WHERE_AGG,
    {
      issueId: {type: 'string'},
      _0_value: {type: 'number'},
      _0_sum: {type: 'number'},
      _0_count: {type: 'number'},
    },
    ['issueId'],
    {
      table: 'comment',
      childField: ['issueId'],
      keyField: ['issueId'],
      fn: 'sum',
      field: 'points',
      predicate: pointsAtLeast5,
    },
  );
  return branch;
}

test('optimistic count + where: only matching children move the count', async () => {
  const branch = branchWithBigCommentCount();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(COUNT_WHERE_AGG, {issueId: 'i1'});
  map.set(aggKey, {issueId: 'i1', _0_value: 2, ['_0_version']: '01'});

  // A non-matching child (points < 5) leaves the count alone.
  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i1', points: 1},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({_0_value: 2}); // unchanged

  // A matching child (points >= 5) bumps it.
  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c2', issueId: 'i1', points: 9},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({_0_value: 3});

  // Deleting the matching child takes it back down...
  await crudImpl.delete(
    tx,
    {op: 'delete', tableName: 'comment', primaryKey: ['id'], value: {id: 'c2'}},
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({_0_value: 2});

  // ...while deleting the non-matching child does nothing.
  await crudImpl.delete(
    tx,
    {op: 'delete', tableName: 'comment', primaryKey: ['id'], value: {id: 'c1'}},
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({_0_value: 2});
});

test('optimistic count + where: an update moves a child in or out of the set', async () => {
  const branch = branchWithBigCommentCount();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(COUNT_WHERE_AGG, {issueId: 'i1'});
  map.set(aggKey, {issueId: 'i1', _0_value: 0, ['_0_version']: '01'});

  // Insert a non-matching child (points 1): count stays 0, but the row is
  // persisted so we can edit it.
  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i1', points: 1},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({_0_value: 0});

  // Edit it into the set (points 1 → 8): +1.
  await crudImpl.update(
    tx,
    {
      op: 'update',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', points: 8},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({_0_value: 1});

  // Edit it back out (points 8 → 2): -1.
  await crudImpl.update(
    tx,
    {
      op: 'update',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', points: 2},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({_0_value: 0});
});

test('optimistic sum + where: only matching children contribute the field', async () => {
  const branch = branchWithBigPointsSum();
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(SUM_WHERE_AGG, {issueId: 'i1'});
  map.set(aggKey, {
    issueId: 'i1',
    _0_value: 20,
    _0_sum: 20,
    _0_count: 2,
    ['_0_version']: '01',
  });

  // Non-matching child (points 3) doesn't touch the sum.
  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i1', points: 3},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({_0_value: 20}); // unchanged

  // Matching child (points 8) adds 8.
  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c2', issueId: 'i1', points: 8},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({_0_value: 28});

  // Editing a matching child's field adjusts by the in-set difference
  // (8 → 12 ⇒ +4): 28 → 32.
  await crudImpl.update(
    tx,
    {
      op: 'update',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c2', points: 12},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({_0_value: 32});
});

test('optimistic sum over a JSON path: only the number leaf moves the sum', async () => {
  const JSON_AGG = 'aggregate:q3:metaPoints';
  const branch = new IVMSourceBranch(schema.tables);
  // Mirror `issue.related('comments', c => c.sum(({json}) => json('meta', 'p')))`.
  branch.getOrCreateAggregateSource(
    JSON_AGG,
    {
      issueId: {type: 'string'},
      _0_value: {type: 'number'},
      _0_sum: {type: 'number'},
      _0_count: {type: 'number'},
    },
    ['issueId'],
    {
      table: 'comment',
      childField: ['issueId'],
      keyField: ['issueId'],
      fn: 'sum',
      field: 'meta',
      path: ['p'],
      predicate: undefined,
    },
  );
  const {map, tx} = makeTx();
  const aggKey = aggregateRowKey(JSON_AGG, {issueId: 'i1'});
  map.set(aggKey, {
    issueId: 'i1',
    _0_value: null,
    _0_sum: 0,
    _0_count: 0,
    ['_0_version']: '01',
  });
  const insert = (id: string, meta: unknown) =>
    crudImpl.insert(
      tx,
      {
        op: 'insert',
        tableName: 'comment',
        primaryKey: ['id'],
        value: {id, issueId: 'i1', meta} as Row,
      },
      schema,
      branch,
    );

  await insert('c1', {p: 4});
  expect(map.get(aggKey)).toMatchObject({_0_value: 4, _0_count: 1});
  // A string leaf, another key and a missing column contribute nothing.
  await insert('c2', {p: '5'});
  await insert('c3', {q: 5});
  await insert('c4', null);
  expect(map.get(aggKey)).toMatchObject({_0_value: 4, _0_count: 1});

  // An edit elsewhere in the document leaves the sum; a leaf edit moves it.
  await crudImpl.update(
    tx,
    {
      op: 'update',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', meta: {p: 4, note: 'x'}},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({_0_value: 4, _0_count: 1});
  await crudImpl.update(
    tx,
    {
      op: 'update',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c2', meta: {p: 6}},
    },
    schema,
    branch,
  );
  expect(map.get(aggKey)).toMatchObject({_0_value: 10, _0_count: 2});
});

test('optimistic grouped sum: a group comes with its first row and goes with its last', async () => {
  const branch = branchWithGroupedPointsSum();
  const {map, tx} = makeTx();
  const key = aggregateRowKey(GROUP_SUM, {_0_key: groupKey(['i1'])});
  const insert = (id: string, points: number | null) =>
    crudImpl.insert(
      tx,
      {
        op: 'insert',
        tableName: 'comment',
        primaryKey: ['id'],
        value: {id, issueId: 'i1', points},
      },
      schema,
      branch,
    );
  const remove = (id: string) =>
    crudImpl.delete(
      tx,
      {op: 'delete', tableName: 'comment', primaryKey: ['id'], value: {id}},
      schema,
      branch,
    );
  const group = (
    value: number | null,
    sum: number,
    count: number,
    rows: number,
  ) => ({
    issueId: 'i1',
    _0_key: groupKey(['i1']),
    _0_value: value,
    _0_sum: sum,
    _0_count: count,
    _0_rows: rows,
  });

  await insert('c1', 5);
  expect(map.get(key)).toEqual(group(5, 5, 1, 1));
  // A null value moves only the row count, which keeps the group.
  await insert('c2', null);
  expect(map.get(key)).toEqual(group(5, 5, 1, 2));
  await remove('c1');
  expect(map.get(key)).toEqual(group(null, 0, 0, 1));
  await remove('c2');
  expect(map.has(key)).toBe(false);
});

test('optimistic grouped sum: a group the client lacks only comes with a new row', async () => {
  const branch = branchWithGroupedPointsSum();
  const {map, tx} = makeTx();
  const key = aggregateRowKey(GROUP_SUM, {_0_key: groupKey(['i1'])});
  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i1', points: 5},
    },
    schema,
    branch,
  );
  // c1 is on the client but its group isn't (say the query hasn't synced):
  // editing its value adds nothing, since only the server knows the sum.
  map.delete(key);
  await crudImpl.update(
    tx,
    {
      op: 'update',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', points: 7},
    },
    schema,
    branch,
  );
  expect(map.has(key)).toBe(false);
});

test('optimistic grouped sum: a row stored without its row count is never removed', async () => {
  const branch = branchWithGroupedPointsSum();
  const {map, tx} = makeTx();
  const key = aggregateRowKey(GROUP_SUM, {_0_key: groupKey(['i1'])});
  await crudImpl.insert(
    tx,
    {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: 'c1', issueId: 'i1', points: 5},
    },
    schema,
    branch,
  );
  // As a client stored the group before rows carried `_0_rows`.
  const {['_0_rows']: _, ...stored} = map.get(key) as Record<string, unknown>;
  map.set(key, stored);

  await crudImpl.delete(
    tx,
    {op: 'delete', tableName: 'comment', primaryKey: ['id'], value: {id: 'c1'}},
    schema,
    branch,
  );
  // Its sum moves, but without a row count it can't tell it's empty.
  expect(map.get(key)).toEqual({
    issueId: 'i1',
    _0_key: groupKey(['i1']),
    _0_value: null,
    _0_sum: 0,
    _0_count: 0,
  });
});
