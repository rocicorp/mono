import {describe, expect, test} from 'vitest';
import type {SchemaValue} from '../../zero-schema/src/table-schema.ts';
import {format} from './internal/sql.ts';
import {normalizeWhitespace} from './internal/statement-cache.ts';
import {
  buildSelectQuery,
  type NoSubqueryCondition,
  type SelectRequest,
} from './query-builder.ts';
import {SelectQueryCache} from './select-query-cache.ts';

const columns: Record<string, SchemaValue> = {
  id: {type: 'string'},
  n: {type: 'number'},
  s: {type: 'string', optional: true},
  j: {type: 'json', optional: true},
};
const order = [
  ['n', 'desc'],
  ['s', 'asc'],
  ['j', 'asc'],
  ['id', 'asc'],
] as const;
const filters: NoSubqueryCondition = {
  type: 'simple',
  left: {type: 'column', name: 'n'},
  op: '>',
  right: {type: 'literal', value: 3},
};

function expected(req: SelectRequest) {
  const {text, values} = format(
    buildSelectQuery(
      'items',
      columns,
      req.constraint,
      filters,
      order,
      req.reverse,
      req.start,
      req.multiConstraints,
      req.filter,
    ),
  );
  return {text: normalizeWhitespace(text), values};
}

describe('SelectQueryCache', () => {
  test('compiles each shape of request once', () => {
    const cache = new SelectQueryCache('items', columns, filters, order);
    const reqs: SelectRequest[] = [
      {constraint: {id: 'a'}},
      {constraint: {id: 'b'}},
      {start: {basis: 'after', row: {id: 'a', n: 1, s: 'x'}}},
      {start: {basis: 'after', row: {id: 'b', n: 2, s: 'y', j: null}}},
      {constraint: {id: 'c'}},
    ];
    for (const req of reqs) {
      expect(cache.get(req)).toEqual(expected(req));
    }
    expect(cache.size).toBe(2);
  });

  test('distinguishes every difference in the SQL', () => {
    const cache = new SelectQueryCache('items', columns, filters, order);
    const fetchFilter: NoSubqueryCondition = {
      type: 'simple',
      left: {type: 'column', name: 'id'},
      op: '=',
      right: {type: 'literal', value: 'x'},
    };
    const start = (row: Record<string, unknown>, basis: 'at' | 'after') => ({
      start: {basis, row: {id: 'a', n: 1, s: 'x', ...row}},
    });
    const reqs: SelectRequest[] = [
      {},
      {reverse: true},
      {filter: fetchFilter},
      // The same condition as another object (not deduplicated).
      {filter: {...fetchFilter}},
      {constraint: {id: 'a'}},
      {constraint: {n: 1}},
      {constraint: {id: 'a', n: 1}},
      {constraint: {n: 1, id: 'a'}},
      {multiConstraints: [[{id: 'a'}]]},
      {multiConstraints: [[{id: 'a'}, {id: 'b'}]]},
      {multiConstraints: [[{n: 1}]]},
      {multiConstraints: [[], [{id: 'a'}]]},
      {multiConstraints: [[{id: 'a'}], []]},
      start({}, 'at'),
      start({}, 'after'),
      start({s: null}, 'after'),
      start({n: null}, 'after'),
      {reverse: true, ...start({}, 'after')},
    ];
    for (const req of reqs) {
      expect(cache.get(req)).toEqual(expected(req));
    }
    expect(cache.size).toBe(reqs.length);

    // A NULL JSON value is bound (as 'null'), so it is the same shape.
    const req = start({j: null}, 'after');
    expect(cache.get(req)).toEqual(expected(req));
    expect(cache.size).toBe(reqs.length);
  });

  test('is bounded', () => {
    const cache = new SelectQueryCache('items', columns, filters, order, 2);
    const reqs: SelectRequest[] = [
      {constraint: {id: 'a'}},
      {constraint: {n: 1}},
      {constraint: {s: 'x'}},
      {constraint: {id: 'b'}},
    ];
    for (const req of reqs) {
      expect(cache.get(req)).toEqual(expected(req));
      expect(cache.size).toBeLessThanOrEqual(2);
    }
  });
});
