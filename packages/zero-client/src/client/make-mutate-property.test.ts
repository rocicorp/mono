// oxlint-disable no-explicit-any
import {describe, expect, test, vi} from 'vitest';
import type {CustomMutatorDefs, MutatorResult} from './custom.ts';
import {addCustomMutatorsProperties} from './make-mutate-property.ts';
import type {MutatorProxy} from './mutator-proxy.ts';

describe('makeMutateProperty', () => {
  function createMockMutatorProxy(): MutatorProxy {
    return {
      wrapCustomMutator: vi.fn((_name: string, fn: () => MutatorResult) => fn),
    } as unknown as MutatorProxy;
  }

  function createMockMutatorResult(): MutatorResult {
    return {
      client: Promise.resolve({type: 'success'}),
      server: Promise.resolve({type: 'success'}),
    } as MutatorResult;
  }

  test('handles flat CustomMutatorDefs', () => {
    const mutatorProxy = createMockMutatorProxy();
    const mutateObject = {};
    const mockRepMutate = {
      createUser: vi.fn(createMockMutatorResult),
      updateUser: vi.fn(createMockMutatorResult),
    };

    const mutators = {
      createUser: vi.fn(),
      updateUser: vi.fn(),
    } as CustomMutatorDefs;

    addCustomMutatorsProperties(
      mutators,
      mutatorProxy,
      mutateObject,
      mockRepMutate,
    );

    expect(mutateObject).toHaveProperty('createUser');
    expect(mutateObject).toHaveProperty('updateUser');
    expect(mutatorProxy.wrapCustomMutator).toHaveBeenCalledTimes(2);
    expect(mutatorProxy.wrapCustomMutator).toHaveBeenCalledWith(
      'createUser',
      mockRepMutate.createUser,
    );
    expect(mutatorProxy.wrapCustomMutator).toHaveBeenCalledWith(
      'updateUser',
      mockRepMutate.updateUser,
    );
  });

  test('handles nested CustomMutatorDefs', () => {
    const mutatorProxy = createMockMutatorProxy();
    const mutateObject = {};
    const mockRepMutate = {
      'user|create': vi.fn(createMockMutatorResult),
      'user|update': vi.fn(createMockMutatorResult),
      'post|create': vi.fn(createMockMutatorResult),
    };

    const mutators = {
      user: {
        create: vi.fn(),
        update: vi.fn(),
      },
      post: {
        create: vi.fn(),
      },
    } as CustomMutatorDefs;

    addCustomMutatorsProperties(
      mutators,
      mutatorProxy,
      mutateObject,
      mockRepMutate,
    );

    expect(mutateObject).toHaveProperty('user');
    expect(mutateObject).toHaveProperty('post');

    expect((mutateObject as any).user).toHaveProperty('create');

    expect((mutateObject as any).user).toHaveProperty('update');

    expect((mutateObject as any).post).toHaveProperty('create');

    expect(mutatorProxy.wrapCustomMutator).toHaveBeenCalledTimes(3);
    expect(mutatorProxy.wrapCustomMutator).toHaveBeenCalledWith(
      'user|create',
      mockRepMutate['user|create'],
    );
    expect(mutatorProxy.wrapCustomMutator).toHaveBeenCalledWith(
      'user|update',
      mockRepMutate['user|update'],
    );
    expect(mutatorProxy.wrapCustomMutator).toHaveBeenCalledWith(
      'post|create',
      mockRepMutate['post|create'],
    );
  });

  test('handles deeply nested CustomMutatorDefs', () => {
    const mutatorProxy = createMockMutatorProxy();
    const mutateObject = {};
    const mockRepMutate = {
      'api|user|profile|update': vi.fn(createMockMutatorResult),
      'api|user|profile|delete': vi.fn(createMockMutatorResult),
    };

    const mutators = {
      api: {
        user: {
          profile: {
            update: vi.fn(),
            delete: vi.fn(),
          },
        },
      },
    } as CustomMutatorDefs;

    addCustomMutatorsProperties(
      mutators,
      mutatorProxy,
      mutateObject,
      mockRepMutate,
    );

    expect((mutateObject as any).api.user.profile).toHaveProperty('update');

    expect((mutateObject as any).api.user.profile).toHaveProperty('delete');

    expect(mutatorProxy.wrapCustomMutator).toHaveBeenCalledTimes(2);
    expect(mutatorProxy.wrapCustomMutator).toHaveBeenCalledWith(
      'api|user|profile|update',
      mockRepMutate['api|user|profile|update'],
    );
    expect(mutatorProxy.wrapCustomMutator).toHaveBeenCalledWith(
      'api|user|profile|delete',
      mockRepMutate['api|user|profile|delete'],
    );
  });

  test('handles mixed flat and nested CustomMutatorDefs', () => {
    const mutatorProxy = createMockMutatorProxy();
    const mutateObject = {};
    const mockRepMutate = {
      'simpleAction': vi.fn(createMockMutatorResult),
      'user|create': vi.fn(createMockMutatorResult),
    };

    const mutators = {
      simpleAction: vi.fn(),
      user: {
        create: vi.fn(),
      },
    } as CustomMutatorDefs;

    addCustomMutatorsProperties(
      mutators,
      mutatorProxy,
      mutateObject,
      mockRepMutate,
    );

    expect(mutateObject).toHaveProperty('simpleAction');
    expect(mutateObject).toHaveProperty('user');

    expect((mutateObject as any).user).toHaveProperty('create');

    expect(mutatorProxy.wrapCustomMutator).toHaveBeenCalledTimes(2);
  });

  test('preserves existing properties in mutateObject', () => {
    const mutatorProxy = createMockMutatorProxy();
    const existingFn = vi.fn();
    const mutateObject = {
      existing: existingFn,
    };
    const mockRepMutate = {
      newMutator: vi.fn(createMockMutatorResult),
    };

    const mutators = {
      newMutator: vi.fn(),
    } as CustomMutatorDefs;

    addCustomMutatorsProperties(
      mutators,
      mutatorProxy,
      mutateObject,
      mockRepMutate,
    );

    expect(mutateObject).toHaveProperty('existing');
    expect(mutateObject).toHaveProperty('newMutator');

    expect((mutateObject as any).existing).toBe(existingFn);
  });

  test('reuses existing nested objects in mutateObject', () => {
    const mutatorProxy = createMockMutatorProxy();
    const existingUserObject = {existingProp: vi.fn()};
    const mutateObject = {
      user: existingUserObject,
    };
    const mockRepMutate = {
      'user|create': vi.fn(createMockMutatorResult),
    };

    const mutators = {
      user: {
        create: vi.fn(),
      },
    } as CustomMutatorDefs;

    addCustomMutatorsProperties(
      mutators,
      mutatorProxy,
      mutateObject,
      mockRepMutate,
    );

    expect((mutateObject as any).user).toBe(existingUserObject);

    expect((mutateObject as any).user).toHaveProperty('existingProp');

    expect((mutateObject as any).user).toHaveProperty('create');
  });

  test('handles CustomMutatorDefs', () => {
    const mutatorProxy = createMockMutatorProxy();
    const mutateObject = {};
    const mockRepMutate = {
      customAction: vi.fn(createMockMutatorResult),
    };

    const mockCustomMutator = vi.fn();

    const mutators = {
      customAction: mockCustomMutator,
    } as CustomMutatorDefs;

    addCustomMutatorsProperties(
      mutators,
      mutatorProxy,
      mutateObject,
      mockRepMutate,
    );

    expect(mutateObject).toHaveProperty('customAction');
    expect(mutatorProxy.wrapCustomMutator).toHaveBeenCalledWith(
      'customAction',
      mockRepMutate.customAction,
    );
  });

  test('wrapped mutators are callable', () => {
    const mutatorProxy = createMockMutatorProxy();
    const mutateObject: Record<string, unknown> = {};
    const mockFn = vi.fn(createMockMutatorResult);
    const mockRepMutate = {
      testMutator: mockFn,
    };

    const mutators = {
      testMutator: vi.fn(),
    } as CustomMutatorDefs;

    addCustomMutatorsProperties(
      mutators,
      mutatorProxy,
      mutateObject,
      mockRepMutate,
    );

    const wrappedMutator = mutateObject.testMutator as () => MutatorResult;
    const result = wrappedMutator();

    expect(result).toEqual({
      client: expect.any(Promise),
      server: expect.any(Promise),
    });
    expect(mockFn).toHaveBeenCalled();
  });

  test('handles empty CustomMutatorDefs', () => {
    const mutatorProxy = createMockMutatorProxy();
    const mutateObject = {};
    const mockRepMutate = {};

    const mutators = {} as CustomMutatorDefs;

    addCustomMutatorsProperties(
      mutators,
      mutatorProxy,
      mutateObject,
      mockRepMutate,
    );

    expect(Object.keys(mutateObject)).toHaveLength(0);
    expect(mutatorProxy.wrapCustomMutator).not.toHaveBeenCalled();
  });

  test('throws when repMutate does not contain expected key', () => {
    const mutatorProxy = createMockMutatorProxy();
    const mutateObject = {};
    const mockRepMutate = {}; // Missing the expected key

    const mutators = {
      missingMutator: vi.fn(),
    } as CustomMutatorDefs;

    expect(() => {
      addCustomMutatorsProperties(
        mutators,
        mutatorProxy,
        mutateObject,
        mockRepMutate,
      );
    }).toThrow('Unexpected undefined value');
  });

  test('constructs correct keys for nested mutators using pipe separator', () => {
    const mutatorProxy = createMockMutatorProxy();
    const mutateObject = {};
    const mockRepMutate = {
      'a|b|c': vi.fn(createMockMutatorResult),
    };

    const mockMutatorFn = vi.fn();

    const mutators = {
      a: {
        b: {
          c: mockMutatorFn,
        },
      },
    } as CustomMutatorDefs;

    addCustomMutatorsProperties(
      mutators,
      mutatorProxy,
      mutateObject,
      mockRepMutate,
    );

    expect(mutatorProxy.wrapCustomMutator).toHaveBeenCalledWith(
      'a|b|c',
      mockRepMutate['a|b|c'],
    );
  });
});
