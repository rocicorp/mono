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
          metadata: {
            upstreamType: 'varchar',
            isNotNull: false,
            isEnum: false,
            isArray: false,
            characterMaxLength: null,
          },

          notNull: false,
          dflt: null,
          elemPgTypeClass: null,
        },
        b: {
          pos: 2,
          metadata: {
            upstreamType: 'varchar',
            isNotNull: false,
            isEnum: false,
            isArray: false,
            characterMaxLength: 180,
          },

          notNull: true,
          dflt: null,
          elemPgTypeClass: null,
        },
        int: {
          pos: 3,
          metadata: {
            upstreamType: 'int8',
            isNotNull: false,
            isEnum: false,
            isArray: false,
            characterMaxLength: null,
          },

          notNull: false,
          dflt: '2147483648',
        },
        bigint: {
          pos: 4,
          metadata: {
            upstreamType: 'int8',
            isNotNull: false,
            isEnum: false,
            isArray: false,
            characterMaxLength: null,
          },

          notNull: false,
          dflt: "'9007199254740992'::bigint",
        },
        text: {
          pos: 5,
          metadata: {
            upstreamType: 'text',
            isNotNull: false,
            isEnum: false,
            isArray: false,
            characterMaxLength: null,
          },

          notNull: false,
          dflt: "'foo'::string",
        },
        bool1: {
          pos: 6,
          metadata: {
            upstreamType: 'bool',
            isNotNull: false,
            isEnum: false,
            isArray: false,
            characterMaxLength: null,
          },

          notNull: false,
          dflt: 'true',
        },
        bool2: {
          pos: 7,
          metadata: {
            upstreamType: 'bool',
            isNotNull: false,
            isEnum: false,
            isArray: false,
            characterMaxLength: null,
          },

          notNull: false,
          dflt: 'false',
        },
        enomz: {
          pos: 8,
          metadata: {
            upstreamType: 'my_type',
            isNotNull: false,
            isEnum: false,
            isArray: false,
            characterMaxLength: null,
          },
          pgTypeClass: PostgresTypeClass.Enum,
          notNull: false,
          dflt: 'false',
        },
      },
    }),
  ).toEqual({
    name: 'issue',
    columns: {
      ['_0_version']: {
        metadata: {
          upstreamType: 'text',
          isNotNull: false,
          isEnum: false,
          isArray: false,
          characterMaxLength: null,
        },
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 9007199254740991,
      },
      a: {
        metadata: {
          upstreamType: 'varchar',
          isNotNull: false,
          isEnum: false,
          isArray: false,
          characterMaxLength: null,
        },
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 1,
      },
      b: {
        metadata: {
          upstreamType: 'varchar',
          isNotNull: true,
          isEnum: false,
          isArray: false,
          characterMaxLength: null,
        },
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 2,
      },
      bigint: {
        metadata: {
          upstreamType: 'int8',
          isNotNull: false,
          isEnum: false,
          isArray: false,
          characterMaxLength: null,
        },
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 4,
      },
      bool1: {
        metadata: {
          upstreamType: 'bool',
          isNotNull: false,
          isEnum: false,
          isArray: false,
          characterMaxLength: null,
        },
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 6,
      },
      bool2: {
        metadata: {
          upstreamType: 'bool',
          isNotNull: false,
          isEnum: false,
          isArray: false,
          characterMaxLength: null,
        },
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 7,
      },
      enomz: {
        metadata: {
          upstreamType: 'my_type',
          isNotNull: false,
          isEnum: true,
          isArray: false,
          characterMaxLength: null,
        },
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 8,
      },
      int: {
        metadata: {
          upstreamType: 'int8',
          isNotNull: false,
          isEnum: false,
          isArray: false,
          characterMaxLength: null,
        },
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 3,
      },
      text: {
        metadata: {
          upstreamType: 'text',
          isNotNull: false,
          isEnum: false,
          isArray: false,
          characterMaxLength: null,
        },
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
          metadata: {
            upstreamType: 'varchar',
            isNotNull: false,
            isEnum: false,
            isArray: false,
            characterMaxLength: null,
          },

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
        metadata: {
          upstreamType: 'text',
          isNotNull: false,
          isEnum: false,
          isArray: false,
          characterMaxLength: null,
        },
        dflt: null,
        elemPgTypeClass: null,
        notNull: false,
        pos: 9007199254740991,
      },
      a: {
        metadata: {
          upstreamType: 'varchar',
          isNotNull: true,
          isEnum: false,
          isArray: false,
          characterMaxLength: null,
        },
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
            metadata: {
              upstreamType: 'varchar',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },

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
        metadata: {
          upstreamType: 'text',
          isNotNull: false,
          isEnum: false,
          isArray: false,
          characterMaxLength: null,
        },
        dflt: "'136'",
        elemPgTypeClass: null,
        notNull: false,
        pos: 9007199254740991,
      },
      a: {
        metadata: {
          upstreamType: 'varchar',
          isNotNull: true,
          isEnum: false,
          isArray: false,
          characterMaxLength: null,
        },
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
      metadata: {
        upstreamType: 'int8',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },

      notNull: true,
      dflt: '2147483648',
      elemPgTypeClass: null,
    },
    {
      pos: 3,
      metadata: {
        upstreamType: 'int8',
        isNotNull: true,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: '2147483648',
      elemPgTypeClass: null,
    },
  ],
  [
    {
      pos: 4,
      metadata: {
        upstreamType: 'int8',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: "'9007199254740992'::bigint",
      elemPgTypeClass: null,
    },
    {
      pos: 4,
      metadata: {
        upstreamType: 'int8',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: "'9007199254740992'",
      elemPgTypeClass: null,
    },
  ],
  [
    {
      pos: 5,
      metadata: {
        upstreamType: 'text',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: "'foo'::string",
      elemPgTypeClass: null,
    },
    {
      pos: 5,
      metadata: {
        upstreamType: 'text',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: "'foo'",
      elemPgTypeClass: null,
    },
  ],
  [
    {
      pos: 6,
      metadata: {
        upstreamType: 'bool',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: 'true',
      elemPgTypeClass: null,
    },
    {
      pos: 6,
      metadata: {
        upstreamType: 'bool',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: '1',
      elemPgTypeClass: null,
    },
  ],
  [
    {
      pos: 7,
      metadata: {
        upstreamType: 'bool',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: 'false',
      elemPgTypeClass: null,
    },
    {
      pos: 7,
      metadata: {
        upstreamType: 'bool',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: '0',
      elemPgTypeClass: null,
    },
  ],
  [
    {
      pos: 8,
      metadata: {
        upstreamType: 'int4[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Base,
    },
    {
      pos: 8,
      metadata: {
        upstreamType: 'int4[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Base,
    },
  ],
  [
    {
      pos: 9,
      metadata: {
        upstreamType: 'my_enum[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Enum,
    },
    {
      pos: 9,
      metadata: {
        upstreamType: 'my_enum[]',
        isNotNull: false,
        isEnum: true,
        isArray: true,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Enum,
    },
  ],
  [
    {
      pos: 10,
      metadata: {
        upstreamType: 'int4[][]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Base,
    },
    {
      pos: 10,
      metadata: {
        upstreamType: 'int4[][]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Base,
    },
  ],
  [
    {
      pos: 11,
      metadata: {
        upstreamType: 'my_enum[][]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },

      notNull: false,
      dflt: null,
      elemPgTypeClass: PostgresTypeClass.Enum,
    },
    {
      pos: 11,
      metadata: {
        upstreamType: 'my_enum[][]',
        isNotNull: false,
        isEnum: true,
        isArray: true,
        characterMaxLength: null,
      },

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
  ['(id + 2)'],
  ['generate(id)'],
  ['current_timestamp'],
  ['CURRENT_TIMESTAMP'],
  ['Current_Time'],
  ['current_DATE'],
])('unsupported column default %s', value => {
  expect(() =>
    mapPostgresToLiteDefault(
      'foo',
      'bar',
      {
        upstreamType: 'boolean',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      value,
    ),
  ).toThrow(UnsupportedColumnDefaultError);
});

test.each([
  [
    '123',
    '123',
    {
      upstreamType: 'int4',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
    },
  ],
  [
    'true',
    '1',
    {
      upstreamType: 'boolean',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
    },
  ],
  [
    'false',
    '0',
    {
      upstreamType: 'boolean',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
    },
  ],
  [
    "'12345678901234567890'::bigint",
    "'12345678901234567890'",
    {
      upstreamType: 'int8',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
    },
  ],
])('supported column default %s', (input, output, metadata) => {
  expect(mapPostgresToLiteDefault('foo', 'bar', metadata, input)).toEqual(
    output,
  );
});
