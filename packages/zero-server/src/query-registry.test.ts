import type {StandardSchemaV1} from '@standard-schema/spec';
import {describe, expect, test} from 'vitest';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {defineQuery} from '../../zql/src/query/define-query.ts';
import {createBuilder} from '../../zql/src/query/named.ts';
import {QueryRegistry} from './query-registry.ts';

const schema = createSchema({
  tables: [
    table('user')
      .columns({
        id: string(),
        name: string(),
        age: number().optional(),
      })
      .primaryKey('id'),
  ],
  relationships: [],
});

const builder = createBuilder(schema);

describe('QueryRegistry', () => {
  test('should find and call query without validator', () => {
    const queries = {
      getUser: defineQuery(({args}: {args: {id: string}}) =>
        builder.user.where('id', args.id).one(),
      ),
    };

    const registry = new QueryRegistry(queries);
    const queryFn = registry.mustGet('getUser');
    const query = queryFn({id: 'user-1'});

    expect(query).toBeDefined();
  });

  test('should throw error for non-existent query', () => {
    const queries = {
      getUser: defineQuery(({args}: {args: {id: string}}) =>
        builder.user.where('id', args.id).one(),
      ),
    };

    const registry = new QueryRegistry(queries);

    expect(() => registry.mustGet('nonExistent')).toThrow(
      "Cannot find query 'nonExistent'",
    );
  });

  test('should validate args when validator is present', () => {
    const validator: StandardSchemaV1<{id: string}, {id: string}> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: (data: unknown) => {
          if (
            typeof data === 'object' &&
            data !== null &&
            'id' in data &&
            typeof data.id === 'string' &&
            data.id.length > 0
          ) {
            return {value: data as {id: string}};
          }
          return {issues: [{message: 'Invalid id: must be non-empty string'}]};
        },
      },
    };

    const queries = {
      getUser: defineQuery(validator, ({args}: {args: {id: string}}) =>
        builder.user.where('id', args.id).one(),
      ),
    };

    const registry = new QueryRegistry(queries);
    const queryFn = registry.mustGet('getUser');

    // Valid args should work
    const query = queryFn({id: 'user-1'});
    expect(query).toBeDefined();

    // Invalid args should throw
    expect(() => queryFn({id: ''})).toThrow(
      'Validation failed for query getUser: Invalid id: must be non-empty string',
    );
    expect(() => queryFn({id: 123 as unknown as string})).toThrow(
      'Validation failed for query getUser: Invalid id: must be non-empty string',
    );
    expect(() => queryFn({} as unknown as {id: string})).toThrow(
      'Validation failed for query getUser: Invalid id: must be non-empty string',
    );
  });

  test('should validate args with refining validator', () => {
    // Validator that refines the type (e.g., non-empty string)
    const validator: StandardSchemaV1<{id: string}, {id: string}> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: (data: unknown) => {
          if (
            typeof data === 'object' &&
            data !== null &&
            'id' in data &&
            typeof data.id === 'string' &&
            data.id.startsWith('user-')
          ) {
            return {value: data as {id: string}};
          }
          return {
            issues: [{message: 'Invalid id: must start with "user-"'}],
          };
        },
      },
    };

    const queries = {
      getUser: defineQuery(validator, ({args}: {args: {id: string}}) =>
        builder.user.where('id', args.id).one(),
      ),
    };

    const registry = new QueryRegistry(queries);
    const queryFn = registry.mustGet('getUser');

    // Should accept valid id
    const query = queryFn({id: 'user-123'});
    expect(query).toBeDefined();

    // Should reject invalid id
    expect(() => queryFn({id: 'admin-123'})).toThrow(
      'Validation failed for query getUser: Invalid id: must start with "user-"',
    );
  });

  test('should reject async validators', () => {
    const validator: StandardSchemaV1<{id: string}, {id: string}> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: () => Promise.resolve({value: {id: 'test'}}),
      },
    };

    const queries = {
      getUser: defineQuery(validator, ({args}: {args: {id: string}}) =>
        builder.user.where('id', args.id).one(),
      ),
    };

    const registry = new QueryRegistry(queries);
    const queryFn = registry.mustGet('getUser');

    expect(() => queryFn({id: 'test'})).toThrow(
      'Async validators are not supported. Query name getUser',
    );
  });

  test('should handle nested query definitions', () => {
    const queries = {
      user: {
        get: defineQuery(({args}: {args: {id: string}}) =>
          builder.user.where('id', args.id).one(),
        ),
        list: defineQuery(({args: _args}: {args: undefined}) => builder.user),
      },
    };

    const registry = new QueryRegistry(queries);

    const getFn = registry.mustGet('user.get');
    expect(getFn({id: 'user-1'})).toBeDefined();

    const listFn = registry.mustGet('user.list');
    expect(listFn()).toBeDefined();
  });

  test('should pass context to query function', () => {
    type Context = {userId: string; role: string};
    let capturedContext: Context | undefined;

    const queries = {
      getUser: defineQuery(
        ({args, ctx}: {args: {id: string}; ctx: Context}) => {
          capturedContext = ctx;
          return builder.user.where('id', args.id).one();
        },
      ),
    };

    const registry = new QueryRegistry(queries);
    const context: Context = {userId: 'admin', role: 'admin'};
    const queryFn = registry.mustGet<Context>('getUser', context);

    queryFn({id: 'user-1'});
    expect(capturedContext).toEqual(context);
  });

  test('should validate undefined args', () => {
    const validator: StandardSchemaV1<undefined, undefined> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: (data: unknown) => {
          if (data === undefined) {
            return {value: undefined};
          }
          return {issues: [{message: 'Must be undefined'}]};
        },
      },
    };

    const queries = {
      listUsers: defineQuery(
        validator,
        ({args: _args}: {args: undefined}) => builder.user,
      ),
    };

    const registry = new QueryRegistry(queries);
    const queryFn = registry.mustGet('listUsers');

    // Valid undefined should work
    expect(queryFn()).toBeDefined();
    expect(queryFn(undefined)).toBeDefined();

    // Non-undefined should fail
    expect(() => queryFn({something: 'value'})).toThrow(
      'Validation failed for query listUsers: Must be undefined',
    );
  });

  test('should handle multiple validation errors', () => {
    const validator: StandardSchemaV1<
      {id: string; name: string},
      {id: string; name: string}
    > = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: () => ({
          issues: [
            {message: 'ID is required'},
            {message: 'Name is required'},
            {message: 'Invalid format'},
          ],
        }),
      },
    };

    const queries = {
      createUser: defineQuery(
        validator,
        ({args}: {args: {id: string; name: string}}) =>
          builder.user.where('id', args.id).one(),
      ),
    };

    const registry = new QueryRegistry(queries);
    const queryFn = registry.mustGet('createUser');

    expect(() => queryFn({id: '', name: ''})).toThrow(
      'Validation failed for query createUser: ID is required, Name is required, Invalid format',
    );
  });

  test('should handle deeply nested query definitions', () => {
    const queries = {
      api: {
        v1: {
          users: {
            get: defineQuery(({args}: {args: {id: string}}) =>
              builder.user.where('id', args.id).one(),
            ),
            list: defineQuery(
              ({args: _args}: {args: undefined}) => builder.user,
            ),
          },
          admin: {
            getUser: defineQuery(({args}: {args: {id: string}}) =>
              builder.user.where('id', args.id).one(),
            ),
          },
        },
        v2: {
          users: {
            get: defineQuery(({args}: {args: {id: string}}) =>
              builder.user.where('id', args.id).one(),
            ),
          },
        },
      },
    };

    const registry = new QueryRegistry(queries);

    // Test all nested paths
    expect(registry.mustGet('api.v1.users.get')({id: 'user-1'})).toBeDefined();
    expect(registry.mustGet('api.v1.users.list')()).toBeDefined();
    expect(
      registry.mustGet('api.v1.admin.getUser')({id: 'user-1'}),
    ).toBeDefined();
    expect(registry.mustGet('api.v2.users.get')({id: 'user-1'})).toBeDefined();
  });

  test('should handle empty query definitions', () => {
    const queries = {};
    const registry = new QueryRegistry(queries);

    expect(() => registry.mustGet('anyQuery')).toThrow(
      "Cannot find query 'anyQuery'",
    );
  });

  test('should handle query with complex nested args', () => {
    type ComplexArgs = {
      user: {id: string; metadata: {role: string; permissions: string[]}};
      filters: {age?: number; active: boolean};
    };

    const queries = {
      complexQuery: defineQuery(({args}: {args: ComplexArgs}) =>
        builder.user.where('id', args.user.id),
      ),
    };

    const registry = new QueryRegistry(queries);
    const queryFn = registry.mustGet('complexQuery');

    const complexArgs: ComplexArgs = {
      user: {
        id: 'user-1',
        metadata: {role: 'admin', permissions: ['read', 'write']},
      },
      filters: {age: 25, active: true},
    };

    expect(queryFn(complexArgs)).toBeDefined();
  });

  test('should pass different contexts to different queries', () => {
    type Context = {userId: string};
    const capturedContexts: Context[] = [];

    const queries = {
      query1: defineQuery(({args, ctx}: {args: {id: string}; ctx: Context}) => {
        capturedContexts.push(ctx);
        return builder.user.where('id', args.id).one();
      }),
      query2: defineQuery(({args, ctx}: {args: {id: string}; ctx: Context}) => {
        capturedContexts.push(ctx);
        return builder.user.where('id', args.id).one();
      }),
    };

    const registry = new QueryRegistry(queries);

    const context1: Context = {userId: 'user-1'};
    const context2: Context = {userId: 'user-2'};

    const queryFn1 = registry.mustGet<Context>('query1', context1);
    const queryFn2 = registry.mustGet<Context>('query2', context2);

    queryFn1({id: 'test-1'});
    queryFn2({id: 'test-2'});

    expect(capturedContexts).toEqual([context1, context2]);
  });

  test('should handle query names with special characters', () => {
    const queries = {
      'user-query': defineQuery(({args}: {args: {id: string}}) =>
        builder.user.where('id', args.id).one(),
      ),
      'user_query': defineQuery(({args}: {args: {id: string}}) =>
        builder.user.where('id', args.id).one(),
      ),
    };

    const registry = new QueryRegistry(queries);

    expect(registry.mustGet('user-query')({id: 'user-1'})).toBeDefined();
    expect(registry.mustGet('user_query')({id: 'user-1'})).toBeDefined();
  });

  test('should handle validator with complex error structure', () => {
    const validator: StandardSchemaV1<{id: string}, {id: string}> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: () => ({
          issues: [
            {message: 'Error 1', path: ['id']},
            {message: 'Error 2', path: ['name']},
            {message: 'Global error'},
          ],
        }),
      },
    };

    const queries = {
      testQuery: defineQuery(validator, ({args}: {args: {id: string}}) =>
        builder.user.where('id', args.id).one(),
      ),
    };

    const registry = new QueryRegistry(queries);
    const queryFn = registry.mustGet('testQuery');

    expect(() => queryFn({id: ''})).toThrow(
      'Validation failed for query testQuery: Error 1, Error 2, Global error',
    );
  });
});
