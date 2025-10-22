import {describe, expect, test} from 'vitest';
import type {LiteTableSpec} from '../db/specs.ts';
import {
  dataTypeToZqlValueType,
  JSON_PARSED,
  JSON_STRINGIFIED,
  liteRow,
  liteValue,
  type JSONFormat,
} from './lite.ts';
import type {RowValue} from './row-key.ts';

describe('types/lite', () => {
  test.each([
    [
      {foo: 'bar'},
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {
            metadata: {
              upstreamType: 'string',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 1,
            elemPgTypeClass: null,
          },
        },
      },
      JSON_PARSED,
    ],
    [
      {foo: 'bar', baz: 2n},
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {
            metadata: {
              upstreamType: 'string',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 1,
            elemPgTypeClass: null,
          },
          baz: {
            metadata: {
              upstreamType: 'int',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 2,
            elemPgTypeClass: null,
          },
        },
      },
      JSON_PARSED,
    ],
    [
      {foo: 'bar', baz: 2n, boo: 3},
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {
            metadata: {
              upstreamType: 'string',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 1,
            elemPgTypeClass: null,
          },
          baz: {
            metadata: {
              upstreamType: 'int',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 2,
            elemPgTypeClass: null,
          },
          boo: {
            metadata: {
              upstreamType: 'int',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 3,
            elemPgTypeClass: null,
          },
        },
      },
      JSON_PARSED,
    ],
    [
      {foo: 'bar', baz: 2n, boo: 3, zoo: null},
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {
            metadata: {
              upstreamType: 'string',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 1,
            elemPgTypeClass: null,
          },
          baz: {
            metadata: {
              upstreamType: 'int',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 2,
            elemPgTypeClass: null,
          },
          boo: {
            metadata: {
              upstreamType: 'int',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 3,
            elemPgTypeClass: null,
          },
          zoo: {
            metadata: {
              upstreamType: 'int',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 4,
            elemPgTypeClass: null,
          },
        },
      },
      JSON_PARSED,
    ],
    [
      {foo: true},
      {foo: 1},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {
            metadata: {
              upstreamType: 'bool',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 1,
            elemPgTypeClass: null,
          },
        },
      },
      JSON_PARSED,
    ],
    [
      {foo: 'bar', b: false},
      {foo: 'bar', b: 0},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {
            metadata: {
              upstreamType: 'string',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 1,
            elemPgTypeClass: null,
          },
          b: {
            metadata: {
              upstreamType: 'boolean',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 2,
            elemPgTypeClass: null,
          },
        },
      },
      JSON_PARSED,
    ],
    [
      {foo: 'bar', b: true, baz: 2n},
      {foo: 'bar', b: 1, baz: 2n},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {
            metadata: {
              upstreamType: 'string',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 1,
            elemPgTypeClass: null,
          },
          b: {
            metadata: {
              upstreamType: 'boolean',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 2,
            elemPgTypeClass: null,
          },
          baz: {
            metadata: {
              upstreamType: 'int',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 3,
            elemPgTypeClass: null,
          },
        },
      },
      JSON_PARSED,
    ],
    [
      {b: true, foo: 'bar', baz: 2n, boo: 3},
      {b: 1, foo: 'bar', baz: 2n, boo: 3},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {
            metadata: {
              upstreamType: 'string',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 1,
            elemPgTypeClass: null,
          },
          b: {
            metadata: {
              upstreamType: 'boolean',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 2,
            elemPgTypeClass: null,
          },
          boo: {
            metadata: {
              upstreamType: 'int',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 3,
            elemPgTypeClass: null,
          },
          baz: {
            metadata: {
              upstreamType: 'int',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 4,
            elemPgTypeClass: null,
          },
        },
      },
      JSON_PARSED,
    ],
    [
      {foo: 'bar', baz: 2n, boo: 3, zoo: null, b: false},
      {foo: 'bar', baz: 2n, boo: 3, zoo: null, b: 0},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {
            metadata: {
              upstreamType: 'string',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 1,
            elemPgTypeClass: null,
          },
          b: {
            metadata: {
              upstreamType: 'boolean',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 2,
            elemPgTypeClass: null,
          },
          boo: {
            metadata: {
              upstreamType: 'int',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 3,
            elemPgTypeClass: null,
          },
          baz: {
            metadata: {
              upstreamType: 'int',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 4,
            elemPgTypeClass: null,
          },
          zoo: {
            metadata: {
              upstreamType: 'float',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 5,
            elemPgTypeClass: null,
          },
        },
      },
      JSON_PARSED,
    ],
    [
      {
        foo: 'bar',
        bar: 1,
        baz: true,
        boo: {key: 'val'},
      },
      {
        foo: '"bar"',
        bar: '1',
        baz: 'true',
        boo: '{"key":"val"}',
      },
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {
            metadata: {
              upstreamType: 'json',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 1,
            elemPgTypeClass: null,
          },
          bar: {
            metadata: {
              upstreamType: 'jsonb',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 2,
            elemPgTypeClass: null,
          },
          baz: {
            metadata: {
              upstreamType: 'json',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 3,
            elemPgTypeClass: null,
          },
          boo: {
            metadata: {
              upstreamType: 'jsonb',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 4,
            elemPgTypeClass: null,
          },
        },
      },
      JSON_PARSED,
    ],
    [
      {
        foo: '"bar"',
        bar: '1',
        baz: 'true',
        boo: '{"key":"val"}',
      },
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {
            metadata: {
              upstreamType: 'json',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 1,
            elemPgTypeClass: null,
          },
          bar: {
            metadata: {
              upstreamType: 'jsonb',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 2,
            elemPgTypeClass: null,
          },
          baz: {
            metadata: {
              upstreamType: 'json',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 3,
            elemPgTypeClass: null,
          },
          boo: {
            metadata: {
              upstreamType: 'jsonb',
              isNotNull: false,
              isEnum: false,
              isArray: false,
              characterMaxLength: null,
            },
            pos: 4,
            elemPgTypeClass: null,
          },
        },
      },
      JSON_STRINGIFIED,
    ],
  ] satisfies [RowValue, RowValue | undefined, LiteTableSpec, JSONFormat][])(
    'liteRow: %s',
    (input, output, table, jsonFormat) => {
      const {row: lite, numCols} = liteRow(input, table, jsonFormat);
      if (output) {
        expect(lite).toEqual(output);
      } else {
        expect(lite).toBe(input); // toBe => identity (i.e. no copy)
      }
      expect(numCols).toBe(Object.keys(input).length);
    },
  );

  test.each([
    [
      {
        upstreamType: 'int',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      1,
      1,
    ],
    [
      {
        upstreamType: 'string',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      'two',
      'two',
    ],
    [
      {
        upstreamType: 'string',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      null,
      null,
    ],
    [
      {
        upstreamType: 'int',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      12313214123432n,
      12313214123432n,
    ],
    [
      {
        upstreamType: 'float',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      123.456,
      123.456,
    ],
    [
      {
        upstreamType: 'bool',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      true,
      1,
    ],
    [
      {
        upstreamType: 'boolean',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      false,
      0,
    ],

    [
      {
        upstreamType: 'bytea',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      Buffer.from('hello world'),
      Buffer.from('hello world'),
    ],
    [
      {
        upstreamType: 'json',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      {custom: {json: 'object'}},
      '{"custom":{"json":"object"}}',
    ],
    [
      {
        upstreamType: 'jsonb',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      [1, 2],
      '[1,2]',
    ],
    [
      {
        upstreamType: 'json',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      ['two', 'three'],
      '["two","three"]',
    ],
    [
      {
        upstreamType: 'json',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      [null, null],
      '[null,null]',
    ],
    [
      {
        upstreamType: 'int[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },
      [12313214123432n, 12313214123432n],
      '[12313214123432,12313214123432]',
    ],
    [
      {
        upstreamType: 'float[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },
      [123.456, 987.654],
      '[123.456,987.654]',
    ],
    [
      {
        upstreamType: 'bool[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },
      [true, false],
      '[true,false]',
    ],
    [
      {
        upstreamType: 'json',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      [{custom: {json: 'object'}}, {another: {json: 'object'}}],
      '[{"custom":{"json":"object"}},{"another":{"json":"object"}}]',
    ],

    // Multi-dimensional array
    [
      {
        upstreamType: 'json[][]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },
      [
        [{custom: {json: 'object'}}, {another: {json: 'object'}}],
        [{custom: {foo: 'bar'}}, {another: {boo: 'far'}}],
      ],
      '[[{"custom":{"json":"object"}},{"another":{"json":"object"}}],[{"custom":{"foo":"bar"}},{"another":{"boo":"far"}}]]',
    ],
  ])('liteValue: $upstreamType', (metadata, input, output) => {
    expect(liteValue(input, metadata, JSON_PARSED)).toEqual(output);
  });
});

describe('dataTypeToZqlValueType', () => {
  test.each([
    [
      {
        upstreamType: 'int',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      'number',
    ],
    [
      {
        upstreamType: 'text',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      'string',
    ],
    [
      {
        upstreamType: 'float',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      'number',
    ],
    [
      {
        upstreamType: 'bool',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      'boolean',
    ],
    [
      {
        upstreamType: 'boolean',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      'boolean',
    ],
    [
      {
        upstreamType: 'json',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      },
      'json',
    ],
    [
      {
        upstreamType: 'int[]',
        isNotNull: true,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },
      'json',
    ],
    [
      {
        upstreamType: 'float[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },
      'json',
    ],
    [
      {
        upstreamType: 'bool[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },
      'json',
    ],
    [
      {
        upstreamType: 'json[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },
      'json',
    ],
    [
      {
        upstreamType: 'f[]',
        isNotNull: false,
        isEnum: true,
        isArray: true,
        characterMaxLength: null,
      },
      'json',
    ],
    [
      {
        upstreamType: 'b[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },
      'json',
    ],
    [
      {
        upstreamType: 'int[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },
      'json',
    ],
    [
      {
        upstreamType: 'float[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },
      'json',
    ],
    [
      {
        upstreamType: 'bool[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },
      'json',
    ],
    [
      {
        upstreamType: 'json[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      },
      'json',
    ],
  ])('dataTypeToZqlValueType: $upstreamType => %s', (metadata, zqlType) => {
    expect(dataTypeToZqlValueType(metadata)).toBe(zqlType);
  });
});
