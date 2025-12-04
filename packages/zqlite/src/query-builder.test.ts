import {describe, expect, test} from 'vitest';
import {formatNamed} from './internal/sql.ts';
import {
  buildSelectQuery,
  constraintsToSQL,
  orderByToSQL,
} from './query-builder.ts';

describe('constraintsToSQL', () => {
  const columns = {
    id: {type: 'string'},
    a: {type: 'number'},
    b: {type: 'number'},
    active: {type: 'boolean'},
    data: {type: 'json'},
  } as const;

  test('returns empty array for undefined constraint', () => {
    expect(constraintsToSQL(undefined, columns)).toEqual([]);
  });

  test('single constraint produces c_{col} named placeholder', () => {
    const result = constraintsToSQL({a: 42}, columns);
    expect(result).toHaveLength(1);
    const formatted = formatNamed(result[0]);
    expect(formatted.text).toBe('"a" = :c_a');
    expect(formatted.values).toEqual({c_a: 42});
  });

  test('multiple constraints produce sorted keys with c_ prefixes', () => {
    const result = constraintsToSQL({b: 2, a: 1}, columns);
    expect(result).toHaveLength(2);
    // Keys are sorted, so 'a' comes before 'b'
    expect(formatNamed(result[0]).text).toBe('"a" = :c_a');
    expect(formatNamed(result[1]).text).toBe('"b" = :c_b');
    expect(formatNamed(result[0]).values).toEqual({c_a: 1});
    expect(formatNamed(result[1]).values).toEqual({c_b: 2});
  });

  test('boolean constraint is converted to 0/1', () => {
    const result = constraintsToSQL({active: true}, columns);
    const formatted = formatNamed(result[0]);
    expect(formatted.text).toBe('"active" = :c_active');
    expect(formatted.values).toEqual({c_active: 1});
  });

  test('json constraint is stringified', () => {
    const result = constraintsToSQL({data: {foo: 'bar'}}, columns);
    const formatted = formatNamed(result[0]);
    expect(formatted.text).toBe('"data" = :c_data');
    expect(formatted.values).toEqual({c_data: '{"foo":"bar"}'});
  });
});

describe('orderByToSQL', () => {
  test('simple ascending order', () => {
    const result = formatNamed(orderByToSQL([['id', 'asc']], false));
    expect(result.text).toBe('ORDER BY "id" asc');
  });

  test('simple descending order', () => {
    const result = formatNamed(orderByToSQL([['id', 'desc']], false));
    expect(result.text).toBe('ORDER BY "id" desc');
  });

  test('compound order', () => {
    const result = formatNamed(
      orderByToSQL(
        [
          ['a', 'asc'],
          ['b', 'desc'],
        ],
        false,
      ),
    );
    expect(result.text).toBe('ORDER BY "a" asc, "b" desc');
  });

  test('reverse flips directions', () => {
    const result = formatNamed(
      orderByToSQL(
        [
          ['a', 'asc'],
          ['b', 'desc'],
        ],
        true,
      ),
    );
    expect(result.text).toBe('ORDER BY "a" desc, "b" asc');
  });
});

describe('buildSelectQuery', () => {
  const columns = {
    id: {type: 'string'},
    a: {type: 'number'},
    b: {type: 'number'},
  } as const;

  test('basic select with order', () => {
    const query = buildSelectQuery(
      'mytable',
      columns,
      undefined, // constraint
      undefined, // filters
      [['id', 'asc']],
      false, // reverse
      undefined, // start
    );
    const result = formatNamed(query);
    expect(result.text).toBe(
      'SELECT "id","a","b" FROM "mytable" ORDER BY "id" asc',
    );
    expect(result.values).toEqual({});
  });

  test('select with constraint uses c_ prefix', () => {
    const query = buildSelectQuery(
      'mytable',
      columns,
      {a: 42},
      undefined,
      [['id', 'asc']],
      false,
      undefined,
    );
    const result = formatNamed(query);
    expect(result.text).toBe(
      'SELECT "id","a","b" FROM "mytable" WHERE "a" = :c_a ORDER BY "id" asc',
    );
    expect(result.values).toEqual({c_a: 42});
  });

  test('select with filters uses f_ prefix', () => {
    const query = buildSelectQuery(
      'mytable',
      columns,
      undefined,
      {
        type: 'simple',
        left: {type: 'column', name: 'b'},
        op: '>',
        right: {type: 'literal', value: 10},
      },
      [['id', 'asc']],
      false,
      undefined,
    );
    const result = formatNamed(query);
    expect(result.text).toBe(
      'SELECT "id","a","b" FROM "mytable" WHERE "b" > :f_0 ORDER BY "id" asc',
    );
    expect(result.values).toEqual({f_0: 10});
  });

  test('select with start uses s_ prefix', () => {
    const query = buildSelectQuery(
      'mytable',
      columns,
      undefined,
      undefined,
      [['id', 'asc']],
      false,
      {row: {id: 'abc', a: 1, b: 2}, basis: 'after'},
    );
    const result = formatNamed(query);
    expect(result.text).toBe(
      'SELECT "id","a","b" FROM "mytable" WHERE (((:s_id IS NULL OR "id" > :s_id))) ORDER BY "id" asc',
    );
    expect(result.values).toEqual({s_id: 'abc'});
  });

  test('select with start basis=at includes exact match', () => {
    const query = buildSelectQuery(
      'mytable',
      columns,
      undefined,
      undefined,
      [['id', 'asc']],
      false,
      {row: {id: 'xyz', a: 1, b: 2}, basis: 'at'},
    );
    const result = formatNamed(query);
    // 'at' includes an OR clause for exact match
    expect(result.text).toBe(
      'SELECT "id","a","b" FROM "mytable" WHERE (((:s_id IS NULL OR "id" > :s_id)) OR ("id" IS :s_id)) ORDER BY "id" asc',
    );
    expect(result.values).toEqual({s_id: 'xyz'});
  });

  test('select with compound order and start', () => {
    const query = buildSelectQuery(
      'mytable',
      columns,
      undefined,
      undefined,
      [
        ['a', 'asc'],
        ['b', 'desc'],
        ['id', 'asc'],
      ],
      false,
      {row: {id: '1', a: 10, b: 20}, basis: 'after'},
    );
    const result = formatNamed(query);
    // Complex start constraints with multiple OR clauses
    expect(result.text).toMatchInlineSnapshot(
      `"SELECT "id","a","b" FROM "mytable" WHERE (((:s_a IS NULL OR "a" > :s_a)) OR ("a" IS :s_a AND ("b" IS NULL OR "b" < :s_b)) OR ("a" IS :s_a AND "b" IS :s_b AND (:s_id IS NULL OR "id" > :s_id))) ORDER BY "a" asc, "b" desc, "id" asc"`,
    );
    expect(result.values).toEqual({s_a: 10, s_b: 20, s_id: '1'});
  });

  test('select with constraint, filters, and start combined', () => {
    const query = buildSelectQuery(
      'mytable',
      columns,
      {a: 5},
      {
        type: 'simple',
        left: {type: 'column', name: 'b'},
        op: '<',
        right: {type: 'literal', value: 100},
      },
      [['id', 'asc']],
      false,
      {row: {id: 'start', a: 5, b: 50}, basis: 'after'},
    );
    const result = formatNamed(query);
    expect(result.text).toBe(
      'SELECT "id","a","b" FROM "mytable" WHERE "a" = :c_a AND (((:s_id IS NULL OR "id" > :s_id))) AND "b" < :f_0 ORDER BY "id" asc',
    );
    expect(result.values).toEqual({c_a: 5, s_id: 'start', f_0: 100});
  });

  test('reverse direction changes comparison operators in start', () => {
    const query = buildSelectQuery(
      'mytable',
      columns,
      undefined,
      undefined,
      [['id', 'asc']],
      true, // reverse
      {row: {id: 'abc', a: 1, b: 2}, basis: 'after'},
    );
    const result = formatNamed(query);
    // With reverse=true and asc order, comparison flips to < instead of >
    expect(result.text).toBe(
      'SELECT "id","a","b" FROM "mytable" WHERE ((("id" IS NULL OR "id" < :s_id))) ORDER BY "id" desc',
    );
    expect(result.values).toEqual({s_id: 'abc'});
  });
});
