import {expect, test} from 'vitest';
import {compile, sql} from './sql.js';

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
