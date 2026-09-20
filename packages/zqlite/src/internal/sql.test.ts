import {escapeSQLiteIdentifier} from '@databases/escape-identifier';
import type {FormatConfig} from '@databases/sql';
import {expect, test} from 'vitest';
import {compile, format, sql} from './sql.ts';

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

// `format` walks the SQL items itself instead of using `@databases/sql`'s
// generic `formatStandard`, so it has to keep producing exactly what
// `formatStandard` produces for every shape the query builder emits.
test('matches the generic @databases/sql formatter', () => {
  const generic: FormatConfig = {
    escapeIdentifier: str => escapeSQLiteIdentifier(str),
    formatValue: value => ({placeholder: '?', value}),
  };

  for (const query of [
    sql`SELECT 1`,
    sql`SELECT ${sql.join(
      ['a', 'b'].map(c => sql.ident(c)),
      sql`,`,
    )} FROM ${sql.ident('foo')} WHERE ${sql.ident('a')} = ${'x'} AND ${sql.ident(
      'b',
    )} IS ${null} ORDER BY ${sql.ident('a')} ${sql.__dangerous__rawValue(
      'desc',
    )}`,
    sql`SELECT * FROM ${sql.ident('sch"ema', 'ta ble')} WHERE ${sql.ident(
      'n',
    )} IN (${sql.join(
      [1, 2, 3].map(v => sql`${v}`),
      sql`,`,
    )})`,
    sql`
      SELECT ${sql.ident('a')}
        FROM ${sql.ident('foo')}
        WHERE ${sql.ident('a')} = ${1}
        ORDER BY ${sql.ident('a')}`,
    sql`   `,
  ]) {
    expect(format(query)).toEqual(query.format(generic));
  }
});
