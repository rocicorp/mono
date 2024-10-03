import {expect, test} from 'vitest';
import {mapPostgresToLite} from './lite.js';

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
          notNull: true,
          dflt: null,
        },
        b: {
          pos: 2,
          dataType: 'varchar',
          characterMaximumLength: 180,
          notNull: true,
          dflt: null,
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
      },
      primaryKey: ['b', 'a'],
    }),
  ).toEqual({
    schema: '',
    name: 'issue',
    columns: {
      ['_0_version']: {
        characterMaximumLength: null,
        dataType: 'TEXT',
        dflt: null,
        notNull: true,
        pos: 9007199254740991,
      },
      a: {
        characterMaximumLength: null,
        dataType: 'TEXT',
        dflt: null,
        notNull: true,
        pos: 1,
      },
      b: {
        characterMaximumLength: null,
        dataType: 'TEXT',
        dflt: null,
        notNull: true,
        pos: 2,
      },
      bigint: {
        characterMaximumLength: null,
        dataType: 'INTEGER',
        dflt: "'9007199254740992'",
        notNull: false,
        pos: 4,
      },
      bool1: {
        characterMaximumLength: null,
        dataType: 'BOOL',
        dflt: '1',
        notNull: false,
        pos: 6,
      },
      bool2: {
        characterMaximumLength: null,
        dataType: 'BOOL',
        dflt: '0',
        notNull: false,
        pos: 7,
      },
      int: {
        characterMaximumLength: null,
        dataType: 'INTEGER',
        dflt: '2147483648',
        notNull: false,
        pos: 3,
      },
      text: {
        characterMaximumLength: null,
        dataType: 'TEXT',
        dflt: "'foo'",
        notNull: false,
        pos: 5,
      },
    },
    primaryKey: ['b', 'a'],
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
        },
      },
      primaryKey: ['a'],
    }),
  ).toEqual({
    schema: '',
    name: 'zero.foo',
    columns: {
      ['_0_version']: {
        characterMaximumLength: null,
        dataType: 'TEXT',
        dflt: null,
        notNull: true,
        pos: 9007199254740991,
      },
      a: {
        characterMaximumLength: null,
        dataType: 'TEXT',
        dflt: null,
        notNull: true,
        pos: 1,
      },
    },
    primaryKey: ['a'],
  });
});
