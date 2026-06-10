import {expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import {buildPipeline} from '../../zql/src/builder/builder.ts';
import {TestBuilderDelegate} from '../../zql/src/builder/test-builder-delegate.ts';
import {Catch} from '../../zql/src/ivm/catch.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import {Database} from './db.ts';
import {TableSource} from './table-source.ts';

const lc = createSilentLogContext();

/**
 * Repro for the production crash:
 *
 *   {"component":"view-syncer","errorMsg":"Bound should be set",
 *    "stack":"Error: Bound should be set
 *      at assert (.../shared/src/asserts.js)
 *      at #pushEditChange (.../zql/src/ivm/take.js)
 *      at Take.push (.../zql/src/ivm/take.js)
 *      at FilterEnd.push (.../zql/src/ivm/filter-operators.js)
 *      at maybeSplitAndPushEditChange (.../maybe-split-and-push-edit-change.js)
 *      at filterPush (.../zql/src/ivm/filter-push.js)"}
 *
 * Root cause: the `Take` operator trusts that the order/membership the SQLite
 * source produces on *fetch* matches what the JS layer produces on *push*:
 *   - On FETCH `TableSource` filters with a SQL `WHERE` clause evaluated by
 *     SQLite (table-source.ts `#requestToSQL`).
 *   - On PUSH `TableSource` filters with the JS `predicate`
 *     (memory-source.ts `genPush` -> `filterPush(change, ..., predicate)`).
 *
 * When those disagree about whether a row matches, the Take is hydrated
 * believing its (only) partition is empty (`size 0`, `bound undefined`) yet
 * later receives an EDIT for a row the JS predicate considers present. The
 * first line of `#pushEditChange` is `assert(takeState.bound, ...)`, so it
 * throws "Bound should be set".
 *
 * We trigger the disagreement with a column-affinity mismatch: the Zero schema
 * types `x` as a `number`, but the replicated SQLite column has TEXT affinity.
 * `WHERE x < 10` is then a TEXT comparison in SQLite ("5" < "10" is false) but
 * a NUMERIC comparison in JS (5 < 10 is true).
 *
 * This is the same class of bug as an ORDER BY whose SQLite order disagrees
 * with `makeComparator` (e.g. a `COLLATE NOCASE` column): any SQLite-vs-JS
 * ordering/membership divergence corrupts the Take's bound/size bookkeeping.
 */

// zero-cache builds the WHERE clause as a JS filter graph (FilterStart .. ->
// FilterEnd -> Take), which is why the production stack has a `FilterEnd.push`
// frame. `applyFiltersAnyway` forces that same topology here so the repro's
// stack matches the report frame-for-frame.
class FilterGraphDelegate extends TestBuilderDelegate {
  readonly applyFiltersAnyway = true;
}

// SELECT * FROM t WHERE x < 10 ORDER BY id LIMIT 1
const ast: AST = {
  table: 't',
  where: {
    type: 'simple',
    op: '<',
    left: {type: 'column', name: 'x'},
    right: {type: 'literal', value: 10},
  },
  orderBy: [['id', 'asc']],
  limit: 1,
};

test('Take: SQLite WHERE (fetch) vs JS predicate (push) divergence throws "Bound should be set"', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  // `x` is declared TEXT -> TEXT affinity. Numbers inserted into it are stored
  // as text ("5"), and comparisons on it are textual in SQLite...
  db.exec(/* sql */ `CREATE TABLE t (id TEXT, x TEXT, PRIMARY KEY (id));`);

  // ...but the Zero schema says `x` is a number.
  const source = new TableSource(
    lc,
    testLogConfig,
    db,
    't',
    {id: {type: 'string'}, x: {type: 'number'}},
    ['id'],
  );

  // Seed one row, x = 5. Stored as TEXT "5" because of the column's affinity.
  consume(source.push(makeSourceChangeAdd({id: 'a', x: 5})));

  const delegate = new FilterGraphDelegate({t: source});
  const out = new Catch(buildPipeline(ast, delegate, 'q1'));

  // Hydrate. SQLite evaluates `x < 10` textually: "5" < "10" is FALSE, so the
  // Take sees zero rows -> takeState becomes {size: 0, bound: undefined}.
  expect(out.fetch()).toEqual([]);

  // Now edit x: 5 -> 6. The JS push-predicate compares numbers, so both 5 < 10
  // and 6 < 10 are true and the EDIT is forwarded into the (empty) Take, which
  // asserts that its bound is set.
  expect(() =>
    consume(
      source.push(makeSourceChangeEdit({id: 'a', x: 6}, {id: 'a', x: 5})),
    ),
  ).toThrow('Bound should be set');
});

/**
 * Control: with a column whose SQLite storage matches the Zero `number` type,
 * fetch and push agree, the Take hydrates with the row present, and the same
 * edit is handled normally (no crash).
 */
test('Take: control with matching affinity does not crash', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  // No declared type -> values keep their numeric storage class.
  db.exec(/* sql */ `CREATE TABLE t (id, x, PRIMARY KEY (id));`);

  const source = new TableSource(
    lc,
    testLogConfig,
    db,
    't',
    {id: {type: 'string'}, x: {type: 'number'}},
    ['id'],
  );

  consume(source.push(makeSourceChangeAdd({id: 'a', x: 5})));

  const delegate = new FilterGraphDelegate({t: source});
  const out = new Catch(buildPipeline(ast, delegate, 'q1'));

  // SQLite numeric 5 < 10 is true, so the row is present after hydration.
  expect(out.fetch()).toMatchObject([{row: {id: 'a', x: 5}}]);

  expect(() =>
    consume(
      source.push(makeSourceChangeEdit({id: 'a', x: 6}, {id: 'a', x: 5})),
    ),
  ).not.toThrow();
});
