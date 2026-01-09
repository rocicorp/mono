import {describe, expect, test} from 'vitest';
import {string, table} from '../../zero-schema/src/builder/table-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {initZero} from './init-zero.ts';

const issueTable = table('issue')
  .columns({
    id: string(),
    title: string(),
    creatorID: string(),
  })
  .primaryKey('id');

const userTable = table('user')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const schema = createSchema({
  tables: [issueTable, userTable],
});

type AuthData = {userID: string; role: 'admin' | 'user'};

describe('initZero', () => {
  test('returns typed utilities', () => {
    const result = initZero<typeof schema, AuthData | undefined>({schema});

    expect(result.builder).toBeDefined();
    expect(result.defineMutator).toBeDefined();
    expect(result.defineMutators).toBeDefined();
    expect(result.defineQuery).toBeDefined();
    expect(result.defineQueries).toBeDefined();
  });

  test('builder creates typed queries', () => {
    const {builder} = initZero<typeof schema>({schema});

    // Should be able to access issue table
    const issueQuery = builder.issue;
    expect(issueQuery).toBeDefined();

    // Should be able to access user table
    const userQuery = builder.user;
    expect(userQuery).toBeDefined();
  });

  test('defineMutators returns function', () => {
    const {defineMutators, defineMutator} = initZero<typeof schema>({schema});

    // Just verify the functions exist and are callable
    expect(typeof defineMutators).toBe('function');
    expect(typeof defineMutator).toBe('function');
  });

  test('defineQueries returns function', () => {
    const {defineQueries, defineQuery} = initZero<typeof schema>({schema});

    // Just verify the functions exist and are callable
    expect(typeof defineQueries).toBe('function');
    expect(typeof defineQuery).toBe('function');
  });

  test('can use builder to create where queries', () => {
    const {builder} = initZero<typeof schema>({schema});

    const query = builder.issue.where('id', '123');
    expect(query).toBeDefined();
  });
});
