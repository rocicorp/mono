import {expect, test} from 'vitest';
import {
  mapPostgresToLite,
  mapPostgresToLiteColumn,
  mapPostgresToLiteDefault,
  mapPostgresToLiteIndex,
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
  [
    {
      pos: 8,
      dataType: 'int4[]',
      characterMaximumLength: null,
      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Base,
    },
    {
      pos: 8,
      dataType: 'int4[]|TEXT_ARRAY',
      characterMaximumLength: null,
      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Base,
    },
  ],
  [
    {
      pos: 9,
      dataType: 'my_enum[]',
      characterMaximumLength: null,
      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Enum,
    },
    {
      pos: 9,
      dataType: 'my_enum[]|TEXT_ENUM|TEXT_ARRAY',
      characterMaximumLength: null,
      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Enum,
    },
  ],
  [
    {
      pos: 10,
      dataType: 'int4[][]',
      characterMaximumLength: null,
      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Base,
    },
    {
      pos: 10,
      dataType: 'int4[][]|TEXT_ARRAY',
      characterMaximumLength: null,
      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Base,
    },
  ],
  [
    {
      pos: 11,
      dataType: 'my_enum[][]',
      characterMaximumLength: null,
      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Enum,
    },
    {
      pos: 11,
      dataType: 'my_enum[][]|TEXT_ENUM|TEXT_ARRAY',
      characterMaximumLength: null,
      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Enum,
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
  // Expressions with parentheses
  ['(id + 2)'],
  ['generate(id)'],
  ['now()'],

  // Time-related keywords/functions
  ['current_timestamp'],
  ['CURRENT_TIMESTAMP'],
  ['Current_Time'],
  ['current_DATE'],
  ['LOCALTIME'],
  ['LOCALTIMESTAMP'],

  // Session variables
  ['CURRENT_USER'],
  ['SESSION_USER'],
  ['CURRENT_SCHEMA'],

  // Non-empty PG array constructors (need backfill)
  ["ARRAY['a', 'b']::text[]"],
  ['ARRAY[1,2,3]::integer[]'],

  // Bare type casts (without quotes)
  ['1::integer'],
  ['0::smallint'],

  // Boolean expressions
  ['true AND false'],
  ['NOT true'],

  // Other PG-specific syntax
  ['uuid_generate_v4()'],
  ['gen_random_uuid()'],

  // Bare quoted strings without type cast (need explicit ::type)
  ["'foo'"],
  ["'hello world'"],
])('unsupported column default %s', value => {
  expect(() => mapPostgresToLiteDefault('foo', 'bar', value)).toThrow(
    UnsupportedColumnDefaultError,
  );
});

test.each([
  // Integers
  ['123', '123'],
  ['0', '0'],
  ['-456', '-456'],
  ['2147483648', '2147483648'],

  // Decimals
  ['123.456', '123.456'],
  ['-0.5', '-0.5'],

  // Booleans (converted to 1/0)
  ['true', '1'],
  ['false', '0'],

  // Quoted strings with type casts
  ["'12345678901234567890'::bigint", "'12345678901234567890'"],
  ["'foo'::text", "'foo'"],
  ["'hello world'::varchar", "'hello world'"],
  ["''::text", "''"], // empty string
  ["'it''s'::text", "'it''s'"], // escaped quote

  // Empty arrays → JSON empty array
  ['ARRAY[]::text[]', "'[]'"],
  ['ARRAY[]::integer[]', "'[]'"],
  ['ARRAY[ ]::text[]', "'[]'"], // with whitespace
  ["'{}'::text[]", "'[]'"], // PG literal syntax
  ["'{}'::integer[]", "'[]'"],
])('supported column default %s', (input, output) => {
  expect(mapPostgresToLiteDefault('foo', 'bar', input)).toEqual(output);
});

test('mapPostgresToLiteIndex appends missing primary key columns to non-unique indexes', () => {
  const baseIndex = {
    schema: 'public',
    tableName: 'issues',
    name: 'issues_created_idx',
    unique: false,
    columns: {created: 'DESC' as const},
  };

  // 1. Missing PK 'id' is appended inheriting trailing direction 'DESC'
  expect(mapPostgresToLiteIndex(baseIndex, ['id'])).toEqual({
    tableName: 'issues',
    name: 'issues_created_idx',
    unique: false,
    columns: {created: 'DESC', id: 'DESC'},
  });

  // 2. Trailing direction ASC is inherited
  const ascIndex = {
    ...baseIndex,
    columns: {created: 'ASC' as const},
  };
  expect(mapPostgresToLiteIndex(ascIndex, ['id'])).toEqual({
    tableName: 'issues',
    name: 'issues_created_idx',
    unique: false,
    columns: {created: 'ASC', id: 'ASC'},
  });

  // 3. Multi-column PK columns appended in order
  expect(mapPostgresToLiteIndex(baseIndex, ['orgId', 'id'])).toEqual({
    tableName: 'issues',
    name: 'issues_created_idx',
    unique: false,
    columns: {created: 'DESC', orgId: 'DESC', id: 'DESC'},
  });

  // 4. If index already includes some PK columns, only missing PK columns are appended
  const partialPKIndex = {
    schema: 'public',
    tableName: 'issues',
    name: 'issues_org_created_idx',
    unique: false,
    columns: {orgId: 'ASC' as const, created: 'DESC' as const},
  };
  expect(mapPostgresToLiteIndex(partialPKIndex, ['orgId', 'id'])).toEqual({
    tableName: 'issues',
    name: 'issues_org_created_idx',
    unique: false,
    columns: {orgId: 'ASC', created: 'DESC', id: 'DESC'},
  });

  // 5. Unique indexes do NOT get PK appended (must preserve uniqueness semantics)
  const uniqueIndex = {
    ...baseIndex,
    unique: true,
  };
  expect(mapPostgresToLiteIndex(uniqueIndex, ['id'])).toEqual({
    tableName: 'issues',
    name: 'issues_created_idx',
    unique: true,
    columns: {created: 'DESC'},
  });

  // 6. When no primaryKey is supplied, columns are unchanged
  expect(mapPostgresToLiteIndex(baseIndex)).toEqual({
    tableName: 'issues',
    name: 'issues_created_idx',
    unique: false,
    columns: {created: 'DESC'},
  });
});
