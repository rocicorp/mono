// oxlint-disable require-await
import type {StandardSchemaV1} from '@standard-schema/spec';
import {describe, expect, test} from 'vitest';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {defineMutator} from '../../zero-types/src/define-mutator.ts';
import {MutatorRegistry} from './mutator-registry.ts';

describe('MutatorRegistry', () => {
  test('should find and call mutator without validator', async () => {
    let called = false;
    const mutators = {
      createUser: defineMutator(async ({args}: {args: {id: string}}) => {
        called = true;
        expect(args.id).toBe('user-1');
      }),
    };

    const registry = new MutatorRegistry(mutators);
    const mutatorFn = registry.mustGet('createUser');

    const mockTx = {} as never;
    await mutatorFn(mockTx, {id: 'user-1'});

    expect(called).toBe(true);
  });

  test('should throw error for non-existent mutator', () => {
    const mutators = {
      createUser: defineMutator(async () => {
        // no-op
      }),
    };

    const registry = new MutatorRegistry(mutators);

    expect(() => registry.mustGet('nonExistent')).toThrow(
      "Cannot find mutator 'nonExistent'",
    );
  });

  test('should validate args when validator is present', async () => {
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

    let receivedArgs: {id: string} | undefined;
    const mutators = {
      createUser: defineMutator(
        validator,
        async ({args}: {args: {id: string}}) => {
          receivedArgs = args;
        },
      ),
    };

    const registry = new MutatorRegistry(mutators);
    const mutatorFn = registry.mustGet('createUser');

    const mockTx = {} as never;

    // Valid args should work
    await mutatorFn(mockTx, {id: 'user-1'});
    expect(receivedArgs).toEqual({id: 'user-1'});

    // Invalid args should throw
    await expect(() => mutatorFn(mockTx, {id: ''})).rejects.toThrow(
      'Validation failed for mutator createUser: Invalid id: must be non-empty string',
    );
    await expect(() => mutatorFn(mockTx, {id: 123 as never})).rejects.toThrow(
      'Validation failed for mutator createUser: Invalid id: must be non-empty string',
    );
    await expect(() => mutatorFn(mockTx, {} as never)).rejects.toThrow(
      'Validation failed for mutator createUser: Invalid id: must be non-empty string',
    );
  });

  test('should validate args with refining validator', async () => {
    // Validator that refines the type (e.g., starts with prefix)
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

    let receivedArgs: {id: string} | undefined;
    const mutators = {
      createUser: defineMutator(
        validator,
        async ({args}: {args: {id: string}}) => {
          receivedArgs = args;
        },
      ),
    };

    const registry = new MutatorRegistry(mutators);
    const mutatorFn = registry.mustGet('createUser');

    const mockTx = {} as never;

    // Should accept valid id
    await mutatorFn(mockTx, {id: 'user-123'});
    expect(receivedArgs).toEqual({id: 'user-123'});

    // Should reject invalid id
    await expect(() => mutatorFn(mockTx, {id: 'admin-123'})).rejects.toThrow(
      'Validation failed for mutator createUser: Invalid id: must start with "user-"',
    );
  });

  test('should reject async validators', async () => {
    const validator: StandardSchemaV1<{id: string}, {id: string}> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: () => Promise.resolve({value: {id: 'test'}}),
      },
    };

    const mutators = {
      createUser: defineMutator(validator, async () => {
        // no-op
      }),
    };

    const registry = new MutatorRegistry(mutators);
    const mutatorFn = registry.mustGet('createUser');

    const mockTx = {} as never;

    // Should throw error about async validators
    await expect(() => mutatorFn(mockTx, {id: 'user-1'})).rejects.toThrow(
      'Async validators are not supported',
    );
  });

  test('should support nested mutator definitions', async () => {
    let createCalled = false;
    let updateCalled = false;

    const mutators = {
      user: {
        create: defineMutator(async ({args}: {args: {id: string}}) => {
          createCalled = true;
          expect(args.id).toBe('user-1');
        }),
        update: defineMutator(
          async ({args}: {args: {id: string; name: string}}) => {
            updateCalled = true;
            expect(args.id).toBe('user-1');
            expect(args.name).toBe('Alice');
          },
        ),
      },
    };

    const registry = new MutatorRegistry(mutators);

    const mockTx = {} as never;

    const createFn = registry.mustGet('user.create');
    await createFn(mockTx, {id: 'user-1'});
    expect(createCalled).toBe(true);

    const updateFn = registry.mustGet('user.update');
    await updateFn(mockTx, {id: 'user-1', name: 'Alice'});
    expect(updateCalled).toBe(true);
  });

  test('should support deeply nested mutator definitions', async () => {
    let called = false;

    const mutators = {
      api: {
        v1: {
          user: {
            create: defineMutator(async ({args}: {args: {id: string}}) => {
              called = true;
              expect(args.id).toBe('user-1');
            }),
          },
        },
      },
    };

    const registry = new MutatorRegistry(mutators);
    const mutatorFn = registry.mustGet('api.v1.user.create');

    const mockTx = {} as never;
    await mutatorFn(mockTx, {id: 'user-1'});

    expect(called).toBe(true);
  });

  test('should pass context to mutator', async () => {
    type Context = {userId: string; role: string};
    let receivedContext: Context | undefined;

    const mutators = {
      updateProfile: defineMutator(
        async ({args, ctx}: {args: {name: string}; ctx: Context}) => {
          receivedContext = ctx;
          expect(args.name).toBe('Alice');
        },
      ),
    };

    const registry = new MutatorRegistry<never, typeof mutators, Context>(
      mutators,
    );
    const context: Context = {userId: 'user-1', role: 'admin'};
    const mutatorFn = registry.mustGet('updateProfile', context);

    const mockTx = {} as never;
    await mutatorFn(mockTx, {name: 'Alice'});

    expect(receivedContext).toEqual({userId: 'user-1', role: 'admin'});
  });

  test('should pass transaction to mutator', async () => {
    let receivedTx: unknown;

    const mutators = {
      createUser: defineMutator(async ({tx}) => {
        receivedTx = tx;
      }),
    };

    const registry = new MutatorRegistry(mutators);
    const mutatorFn = registry.mustGet('createUser');

    const mockTx = {id: 'mock-tx-123'} as never;
    await mutatorFn(mockTx, {id: 'user-1'});

    expect(receivedTx).toEqual({id: 'mock-tx-123'});
  });

  test('should handle mutator that returns void', async () => {
    const mutators = {
      createUser: defineMutator(async ({args}: {args: {id: string}}) => {
        // Just do something, no return
        expect(args.id).toBe('user-1');
      }),
    };

    const registry = new MutatorRegistry(mutators);
    const mutatorFn = registry.mustGet('createUser');

    const mockTx = {} as never;
    const result = await mutatorFn(mockTx, {id: 'user-1'});

    expect(result).toBeUndefined();
  });

  test('should propagate errors from mutator', async () => {
    const mutators = {
      failingMutator: defineMutator(async () => {
        throw new Error('Mutator failed');
      }),
    };

    const registry = new MutatorRegistry(mutators);
    const mutatorFn = registry.mustGet('failingMutator');

    const mockTx = {} as never;
    await expect(() => mutatorFn(mockTx, {id: 'user-1'})).rejects.toThrow(
      'Mutator failed',
    );
  });

  test('should handle undefined args', async () => {
    let receivedArgs: ReadonlyJSONValue | undefined;

    const mutators = {
      noArgsMutator: defineMutator(async ({args}) => {
        receivedArgs = args;
      }),
    };

    const registry = new MutatorRegistry(mutators);
    const mutatorFn = registry.mustGet('noArgsMutator');

    const mockTx = {} as never;
    // Pass null instead of undefined as the latter is not a valid ReadonlyJSONValue
    await mutatorFn(mockTx, null as never);

    expect(receivedArgs).toBeNull();
  });

  test('should validate null args with validator', async () => {
    const validator: StandardSchemaV1<null, null> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: (data: unknown) => {
          if (data === null) {
            return {value: null};
          }
          return {issues: [{message: 'Args must be null'}]};
        },
      },
    };

    let called = false;
    const mutators = {
      noArgsMutator: defineMutator(validator, async ({args}) => {
        called = true;
        expect(args).toBeNull();
      }),
    };

    const registry = new MutatorRegistry(mutators);
    const mutatorFn = registry.mustGet('noArgsMutator');

    const mockTx = {} as never;

    // Valid (null) should work
    await mutatorFn(mockTx, null as never);
    expect(called).toBe(true);

    // Invalid (not null) should throw
    await expect(() =>
      mutatorFn(mockTx, {some: 'value'} as never),
    ).rejects.toThrow(
      'Validation failed for mutator noArgsMutator: Args must be null',
    );
  });

  test('should handle complex argument types', async () => {
    type ComplexArgs = {
      user: {
        id: string;
        profile: {
          name: string;
          age: number;
        };
      };
      tags: string[];
    };

    let receivedArgs: ComplexArgs | undefined;

    const mutators = {
      complexMutator: defineMutator(async ({args}: {args: ComplexArgs}) => {
        receivedArgs = args;
      }),
    };

    const registry = new MutatorRegistry(mutators);
    const mutatorFn = registry.mustGet('complexMutator');

    const mockTx = {} as never;
    const complexArgs: ComplexArgs = {
      user: {
        id: 'user-1',
        profile: {
          name: 'Alice',
          age: 30,
        },
      },
      tags: ['admin', 'active'],
    };

    await mutatorFn(mockTx, complexArgs);

    expect(receivedArgs).toEqual(complexArgs);
  });

  test('should support multiple mutators in flat structure', async () => {
    const callOrder: string[] = [];

    const mutators = {
      mutator1: defineMutator(async () => {
        callOrder.push('mutator1');
      }),
      mutator2: defineMutator(async () => {
        callOrder.push('mutator2');
      }),
      mutator3: defineMutator(async () => {
        callOrder.push('mutator3');
      }),
    };

    const registry = new MutatorRegistry(mutators);
    const mockTx = {} as never;

    await registry.mustGet('mutator1')(mockTx, {id: 'test'});
    await registry.mustGet('mutator2')(mockTx, {id: 'test'});
    await registry.mustGet('mutator3')(mockTx, {id: 'test'});

    expect(callOrder).toEqual(['mutator1', 'mutator2', 'mutator3']);
  });

  test('should not confuse similar mutator names', async () => {
    let userCalled = false;
    let usersCalled = false;

    const mutators = {
      user: defineMutator(async () => {
        userCalled = true;
      }),
      users: defineMutator(async () => {
        usersCalled = true;
      }),
    };

    const registry = new MutatorRegistry(mutators);
    const mockTx = {} as never;

    await registry.mustGet('user')(mockTx, {id: 'user-1'});
    expect(userCalled).toBe(true);
    expect(usersCalled).toBe(false);

    userCalled = false;
    await registry.mustGet('users')(mockTx, {ids: ['user-1', 'user-2']});
    expect(userCalled).toBe(false);
    expect(usersCalled).toBe(true);
  });

  test('should ignore non-mutator properties in nested objects', async () => {
    let called = false;

    // Create a properly typed mutators object with only mutator definitions
    const mutators = {
      user: {
        create: defineMutator(async () => {
          called = true;
        }),
      },
    };

    const registry = new MutatorRegistry(mutators);
    const mockTx = {} as never;

    await registry.mustGet('user.create')(mockTx, {id: 'user-1'});
    expect(called).toBe(true);

    // Non-mutator properties would not be registered
    expect(() => registry.mustGet('user.someProperty')).toThrow(
      "Cannot find mutator 'user.someProperty'",
    );
  });
});
