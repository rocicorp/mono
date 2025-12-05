import {describe, expect, test} from 'vitest';
import {compile, format, formatNamed, named, sql} from './sql.ts';

test('can do empty slots', () => {
  const str = compile(sql`INSERT INTO foo (id, name) VALUES (?, ?)`);
  expect(str).toMatchInlineSnapshot(
    `"INSERT INTO foo (id, name) VALUES (?, ?)"`,
  );
});

test('quotes identifiers as advertised', () => {
  const str = compile(sql`SELECT * FROM ${sql.ident('foo', 'bar')}`);
  expect(str).toMatchInlineSnapshot(`"SELECT * FROM "foo"."bar""`);
});

test('escapes identifiers as advertised', () => {
  const str = compile(sql`SELECT * FROM ${sql.ident('foo"bar')}`);
  expect(str).toMatchInlineSnapshot(`"SELECT * FROM "foo""bar""`);
});

describe('named()', () => {
  test('creates NamedValue wrapper', () => {
    const result = named('myParam', 42);
    expect(result).toHaveProperty('name', 'myParam');
    expect(result).toHaveProperty('value', 42);
  });

  test('preserves various value types', () => {
    expect(named('str', 'hello').value).toBe('hello');
    expect(named('num', 123.45).value).toBe(123.45);
    expect(named('bool', true).value).toBe(true);
    expect(named('nil', null).value).toBe(null);
    expect(named('obj', {a: 1}).value).toEqual({a: 1});
    expect(named('arr', [1, 2, 3]).value).toEqual([1, 2, 3]);
  });
});

describe('formatNamed()', () => {
  test('simple query with named value produces :name placeholder', () => {
    const query = sql`SELECT * FROM users WHERE id = ${named('userId', 42)}`;
    const result = formatNamed(query);
    expect(result.text).toBe('SELECT * FROM users WHERE id = :userId');
    expect(result.values).toEqual({userId: 42});
  });

  test('multiple named values in same query', () => {
    const query = sql`SELECT * FROM users WHERE id = ${named('id', 1)} AND status = ${named('status', 'active')}`;
    const result = formatNamed(query);
    expect(result.text).toBe(
      'SELECT * FROM users WHERE id = :id AND status = :status',
    );
    expect(result.values).toEqual({id: 1, status: 'active'});
  });

  test('mixed named and unnamed values', () => {
    const query = sql`SELECT * FROM users WHERE id = ${named('id', 1)} AND age > ${25}`;
    const result = formatNamed(query);
    expect(result.text).toBe(
      'SELECT * FROM users WHERE id = :id AND age > :_p1',
    );
    expect(result.values).toEqual({id: 1, _p1: 25});
  });

  test('same named placeholder appearing multiple times shares value', () => {
    const value = named('x', 10);
    const query = sql`SELECT * FROM t WHERE a > ${value} AND b < ${value}`;
    const result = formatNamed(query);
    expect(result.text).toBe('SELECT * FROM t WHERE a > :x AND b < :x');
    // Both occurrences map to same key
    expect(result.values).toEqual({x: 10});
  });
});

describe('format() backwards compatibility', () => {
  test('NamedValue passed to format() is unwrapped', () => {
    const query = sql`SELECT * FROM t WHERE a = ${named('myName', 42)}`;
    const result = format(query);
    // format() uses positional placeholders
    expect(result.text).toBe('SELECT * FROM t WHERE a = ?');
    // Value is unwrapped from NamedValue
    expect(result.values).toEqual([42]);
  });
});
