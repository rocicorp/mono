import {expect, test} from 'vitest';
import {
  mapPostgresToLite,
  mapPostgresToLiteColumn,
  mapPostgresToLiteDefault,
  UnsupportedColumnDefaultError,
} from './pg-to-lite.ts';
import * as PostgresTypeClass from './postgres-type-class-enum.ts';
import {type ColumnSpec} from './specs.ts';

test('postgres to lite table spec', () => {
  expect(
    mapPostgresToLite({
      schema: 'public',
      name: 'issue',
      columns: {
        a: {
          pos: 1,
          dataType: 'varchar',
          characterMaximumLength: null,
          notNull: false,
          dflt: null,
          elemPgTypeClass: null,
        },
        b: {
          pos: 2,
          dataType: 'varchar',
          characterMaximumLength: 180,
          notNull: true,
          dflt: null,
          elemPgTypeClass: null,
        },
        int: {
          pos: 3,
          dataType: 'int8',
          characterMaximumLength: null,
          notNull: false,
          dflt: '2147483648',
        },
        bigint: {
          pos: 4,
          dataType: 'int8',
          characterMaximumLength: null,
          notNull: false,
          dflt: "'9007199254740992'::bigint",
        },
        text: {
          pos: 5,
          dataType: 'text',
          characterMaximumLength: null,
          notNull: false,
          dflt: "'foo'::string",
        },
        bool1: {
          pos: 6,
          dataType: 'bool',
          characterMaximumLength: null,
          notNull: false,
          dflt: 'true',
        },
        bool2: {
          pos: 7,
          dataType: 'bool',
          characterMaximumLength: null,
          notNull: false,
          dflt: 'false',
        },
        enomz: {
          pos: 8,
          dataType: 'my_type',
          pgTypeClass: PostgresTypeClass.Enum,
          characterMaximumLength: null,
          notNull: false,
          dflt: 'false',
        },
      },
    }),
  ).toEqual({
    name: 'issue',
    columns: {
      ['_0_version']: {
        characterMaximumLength: null,
        dataType: 'text',
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 9007199254740991,
      },
      a: {
        characterMaximumLength: null,
        dataType: 'varchar',
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 1,
      },
      b: {
        characterMaximumLength: null,
        dataType: 'varchar|NOT_NULL',
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 2,
      },
      bigint: {
        characterMaximumLength: null,
        dataType: 'int8',
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 4,
      },
      bool1: {
        characterMaximumLength: null,
        dataType: 'bool',
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 6,
      },
      bool2: {
        characterMaximumLength: null,
        dataType: 'bool',
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 7,
      },
      enomz: {
        characterMaximumLength: null,
        dataType: 'my_type|TEXT_ENUM',
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 8,
      },
      int: {
        characterMaximumLength: null,
        dataType: 'int8',
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 3,
      },
      text: {
        characterMaximumLength: null,
        dataType: 'text',
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 5,
      },
    },
  });

  // Non-public schema
  expect(
    mapPostgresToLite({
      schema: 'zero',
      name: 'foo',
      columns: {
        a: {
          pos: 1,
          dataType: 'varchar',
          characterMaximumLength: null,
          notNull: true,
          dflt: null,
          elemPgTypeClass: null,
        },
      },
      primaryKey: ['a'],
    }),
  ).toEqual({
    name: 'zero.foo',
    columns: {
      ['_0_version']: {
        characterMaximumLength: null,
        dataType: 'text',
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 9007199254740991,
      },
      a: {
        characterMaximumLength: null,
        dataType: 'varchar|NOT_NULL',
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 1,
      },
    },
  });

  // Default version
  expect(
    mapPostgresToLite(
      {
        schema: 'public',
        name: 'foo',
        columns: {
          a: {
            pos: 1,
            dataType: 'varchar',
            characterMaximumLength: null,
            notNull: true,
            dflt: null,
            elemPgTypeClass: null,
          },
        },
        primaryKey: ['a'],
      },
      '136',
    ),
  ).toEqual({
    name: 'foo',
    columns: {
      ['_0_version']: {
        characterMaximumLength: null,
        dataType: 'text',
        dflt: "'136'",
        elemPgTypeClass: null,
        notNull: false,
        pos: 9007199254740991,
      },
      a: {
        characterMaximumLength: null,
        dataType: 'varchar|NOT_NULL',
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 1,
      },
    },
  });
});

test.each([
  [
    {
      pos: 3,
      dataType: 'int8',
      characterMaximumLength: null,
      notNull: true,
      dflt: '2147483648',
      elemPgTypeClass: null,
    },
    {
      pos: 3,
      dataType: 'int8|NOT_NULL',
      characterMaximumLength: null,
      notNull: false,
      dflt: '2147483648',
      elemPgTypeClass: null,
    },
  ],
  [
    {
      pos: 4,
      dataType: 'int8',
      characterMaximumLength: null,
      notNull: false,
      dflt: "'9007199254740992'::bigint",
      elemPgTypeClass: null,
    },
    {
      pos: 4,
      dataType: 'int8',
      characterMaximumLength: null,
      notNull: false,
      dflt: "'9007199254740992'",
      elemPgTypeClass: null,
    },
  ],
  [
    {
      pos: 5,
      dataType: 'text',
      characterMaximumLength: null,
      notNull: false,
      dflt: "'foo'::string",
      elemPgTypeClass: null,
    },
    {
      pos: 5,
      dataType: 'text',
      characterMaximumLength: null,
      notNull: false,
      dflt: "'foo'",
      elemPgTypeClass: null,
    },
  ],
  [
    {
      pos: 6,
      dataType: 'bool',
      characterMaximumLength: null,
      notNull: false,
      dflt: 'true',
      elemPgTypeClass: null,
    },
    {
      pos: 6,
      dataType: 'bool',
      characterMaximumLength: null,
      notNull: false,
      dflt: '1',
      elemPgTypeClass: null,
    },
  ],
  [
    {
      pos: 7,
      dataType: 'bool',
      characterMaximumLength: null,
      notNull: false,
      dflt: 'false',
      elemPgTypeClass: null,
    },
    {
      pos: 7,
      dataType: 'bool',
      characterMaximumLength: null,
      notNull: false,
      dflt: '0',
      elemPgTypeClass: null,
    },
  ],
] satisfies [ColumnSpec, ColumnSpec][])(
  'postgres to lite column %s',
  (pg, lite) => {
    expect(mapPostgresToLiteColumn('foo', {name: 'bar', spec: pg})).toEqual(
      lite,
    );
  },
);

test.each([
  ['(id + 2)'],
  ['generate(id)'],
  ['current_timestamp'],
  ['CURRENT_TIMESTAMP'],
  ['Current_Time'],
  ['current_DATE'],
])('unsupported column default %s', value => {
  expect(() =>
    mapPostgresToLiteDefault('foo', 'bar', 'boolean', value),
  ).toThrow(UnsupportedColumnDefaultError);
});

test.each([
  ['123', '123', 'int4'],
  ['true', '1', 'boolean'],
  ['false', '0', 'boolean'],
  ["'12345678901234567890'::bigint", "'12345678901234567890'", 'int8'],
])('supported column default %s', (input, output, dataType) => {
  expect(mapPostgresToLiteDefault('foo', 'bar', dataType, input)).toEqual(
    output,
  );
});
