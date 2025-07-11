import {expect, test, describe, vi} from 'vitest';
import {createSchema} from './builder/schema-builder.ts';
import {boolean, string, number, table} from './builder/table-builder.ts';
import {addDefaultToOptionalFields} from './add-defaults.ts';
import type {Location} from '../../zql/src/mutate/custom.ts';

const schema = createSchema({
  tables: [
    table('user')
      .columns({
        id: string(),
        name: string(),
        // Column with no defaults
        email: string().nullable(),
        // Column with client defaults and server db
        status: string().default({
          insert: {
            client: () => 'active',
            server: 'db',
          },
          update: {
            client: () => 'updated',
            server: 'db',
          },
        }),
        // Column with both client and server defaults
        version: number().default({
          insert: {
            client: () => 1,
            server: () => 0,
          },
          update: {
            client: () => 2,
            server: () => 10,
          },
        }),
        // Column with only update defaults
        updatedAt: string().default({
          update: {
            client: () => 'update-time',
            server: 'db',
          },
        }),
        // Nullable column with defaults
        isActive: boolean()
          .nullable()
          .default({
            insert: {
              client: () => true,
              server: () => false,
            },
          }),
      })
      .primaryKey('id'),
  ],
});

const userSchema = schema.tables.user;

// Schema with mock functions for testing execution
const mockInsertFn = vi.fn(() => 'mock-insert-value');
const mockUpdateFn = vi.fn(() => 'mock-update-value');
const mockInsertFn2 = vi.fn(() => 42);
const mockUpdateFn2 = vi.fn(() => 99);

const mockSchema = createSchema({
  tables: [
    table('mock')
      .columns({
        id: string(),
        testCol: string().default({
          insert: {
            client: mockInsertFn,
            server: () => 'server-insert',
          },
          update: {
            client: mockUpdateFn,
            server: () => 'server-update',
          },
        }),
        anotherCol: number().default({
          insert: {
            client: mockInsertFn2,
            server: () => 0,
          },
          update: {
            client: mockUpdateFn2,
            server: () => 10,
          },
        }),
      })
      .primaryKey('id'),
  ],
});

const mockTableSchema = mockSchema.tables.mock;

describe('addDefaultToOptionalFields', () => {
  describe('client location', () => {
    describe('insert operation', () => {
      test('applies client insert defaults for undefined values', () => {
        const input = {
          id: 'user1',
          name: 'John',
          status: undefined,
          // status, version, isActive should get client insert defaults
          // email, updatedAt should be null (no insert defaults)
        };

        const result = addDefaultToOptionalFields({
          schema: userSchema,
          value: input,
          operation: 'insert',
          location: 'client',
        });

        expect(result).toEqual({
          id: 'user1',
          name: 'John',
          email: null, // no default
          status: 'active', // client insert default applied
          version: 1, // client insert default applied
          updatedAt: null, // no insert default
          isActive: true, // client insert default applied
        });
      });

      test('preserves explicitly set values including null', () => {
        const input = {
          id: 'user1',
          name: 'John',
          email: 'john@example.com',
          status: 'custom-status',
          version: 5,
          updatedAt: 'custom-time',
          isActive: null, // explicitly null
        };

        const result = addDefaultToOptionalFields({
          schema: userSchema,
          value: input,
          operation: 'insert',
          location: 'client',
        });

        expect(result).toEqual({
          id: 'user1',
          name: 'John',
          email: 'john@example.com', // preserved
          status: 'custom-status', // preserved
          version: 5, // preserved
          updatedAt: 'custom-time', // preserved
          isActive: null, // preserved as null
        });
      });

      test('handles mix of undefined and set values', () => {
        const input = {
          id: 'user1',
          name: 'John',
          email: null, // explicitly null
          status: 'inactive', // explicitly set
          // version, updatedAt, isActive should get defaults or remain undefined
        };

        const result = addDefaultToOptionalFields({
          schema: userSchema,
          value: input,
          operation: 'insert',
          location: 'client',
        });

        expect(result).toEqual({
          id: 'user1',
          name: 'John',
          email: null, // preserved as null
          status: 'inactive', // preserved
          version: 1, // client insert default applied
          updatedAt: null, // no insert default
          isActive: true, // client insert default applied
        });
      });
    });

    describe('update operation', () => {
      test('applies client update defaults for undefined values', () => {
        const input = {
          id: 'user1',
          name: 'John Updated',
          // status, version, updatedAt should get client update defaults
          // email, isActive should remain undefined (no update defaults)
        };

        const result = addDefaultToOptionalFields({
          schema: userSchema,
          value: input,
          operation: 'update',
          location: 'client',
        });

        expect(result).toEqual({
          id: 'user1',
          name: 'John Updated',
          email: null, // no update default
          status: 'updated', // client update default
          version: 2, // client update default
          updatedAt: 'update-time', // client update default
          isActive: null, // no update default
        });
      });

      test('preserves explicitly set values in update', () => {
        const input = {
          id: 'user1',
          name: 'John Updated',
          email: 'newemail@example.com',
          status: 'custom-updated',
          version: 99,
          updatedAt: 'custom-update-time',
          isActive: false,
        };

        const result = addDefaultToOptionalFields({
          schema: userSchema,
          value: input,
          operation: 'update',
          location: 'client',
        });

        expect(result).toEqual({
          id: 'user1',
          name: 'John Updated',
          email: 'newemail@example.com', // preserved
          status: 'custom-updated', // preserved
          version: 99, // preserved
          updatedAt: 'custom-update-time', // preserved
          isActive: false, // preserved
        });
      });
    });
  });

  describe('server location', () => {
    describe('insert operation', () => {
      test('applies server insert defaults and omits undefined values', () => {
        const input = {
          id: 'user1',
          name: 'John',
          // version, isActive should get server defaults
          // status should be omitted (db default)
          // email, updatedAt should be omitted (undefined, no defaults)
        };

        const result = addDefaultToOptionalFields({
          schema: userSchema,
          value: input,
          operation: 'insert',
          location: 'server',
        });

        expect(result).toEqual({
          id: 'user1',
          name: 'John',
          version: 0, // server insert default applied
          isActive: false, // server insert default applied
          // email, status, updatedAt are omitted (undefined, no server function defaults)
        });
      });

      test('preserves explicitly set values and omits undefined', () => {
        const input = {
          id: 'user1',
          name: 'John',
          email: 'john@example.com',
          status: 'custom',
          version: 99,
        };

        const result = addDefaultToOptionalFields({
          schema: userSchema,
          value: input,
          operation: 'insert',
          location: 'server',
        });

        expect(result).toEqual({
          id: 'user1',
          name: 'John',
          email: 'john@example.com', // preserved
          status: 'custom', // preserved
          version: 99, // preserved
          isActive: false, // server default applied
          // updatedAt omitted (undefined, no insert default)
        });
      });

      test('handles null values correctly on server', () => {
        const input = {
          id: 'user1',
          name: 'John',
          email: null,
          status: null,
          isActive: null,
        };

        const result = addDefaultToOptionalFields({
          schema: userSchema,
          value: input,
          operation: 'insert',
          location: 'server',
        });

        expect(result).toEqual({
          id: 'user1',
          name: 'John',
          email: null, // preserved as null
          status: null, // preserved as null
          version: 0, // server default applied
          isActive: null, // preserved as null
          // updatedAt omitted
        });
      });

      test('handles db defaults correctly (omits them)', () => {
        const input = {
          id: 'user1',
          name: 'John',
        };

        const result = addDefaultToOptionalFields({
          schema: userSchema,
          value: input,
          operation: 'insert',
          location: 'server',
        });

        // status has 'db' default, so it should be omitted
        // version and isActive have function defaults, so they should be applied
        expect(result).toEqual({
          id: 'user1',
          name: 'John',
          version: 0, // function default applied
          isActive: false, // function default applied
          // status omitted (db default)
          // email, updatedAt omitted (undefined, no defaults)
        });
      });
    });

    describe('update operation', () => {
      test('applies server update defaults and omits undefined values', () => {
        const input = {
          id: 'user1',
          name: 'John Updated',
        };

        const result = addDefaultToOptionalFields({
          schema: userSchema,
          value: input,
          operation: 'update',
          location: 'server',
        });

        expect(result).toEqual({
          id: 'user1',
          name: 'John Updated',
          version: 10, // server update default applied
          // status, updatedAt omitted (db defaults)
          // email, isActive omitted (no update defaults)
        });
      });

      test('preserves set values and applies defaults', () => {
        const input = {
          id: 'user1',
          name: 'John Updated',
          email: 'updated@example.com',
          status: 'manually-updated',
        };

        const result = addDefaultToOptionalFields({
          schema: userSchema,
          value: input,
          operation: 'update',
          location: 'server',
        });

        expect(result).toEqual({
          id: 'user1',
          name: 'John Updated',
          email: 'updated@example.com', // preserved
          status: 'manually-updated', // preserved
          version: 10, // server update default applied
          // updatedAt omitted (db default)
          // isActive omitted (no update default)
        });
      });
    });
  });

  describe('edge cases', () => {
    test('works with empty input object', () => {
      const input = {};

      const clientInsertResult = addDefaultToOptionalFields({
        schema: userSchema,
        value: input,
        operation: 'insert',
        location: 'client',
      });

      const serverInsertResult = addDefaultToOptionalFields({
        schema: userSchema,
        value: input,
        operation: 'insert',
        location: 'server',
      });

      expect(clientInsertResult).toEqual({
        id: null,
        name: null,
        email: null,
        status: 'active', // client insert default
        version: 1, // client insert default
        updatedAt: null, // no insert default
        isActive: true, // client insert default
      });

      expect(serverInsertResult).toEqual({
        version: 0, // server insert default
        isActive: false, // server insert default
        // all others omitted
      });
    });

    test('preserves falsy but defined values', () => {
      const input = {
        id: '',
        name: '',
        email: '',
        status: '',
        version: 0,
        updatedAt: '',
        isActive: false,
      };

      const result = addDefaultToOptionalFields({
        schema: userSchema,
        value: input,
        operation: 'insert',
        location: 'client',
      });

      expect(result).toEqual({
        id: '',
        name: '',
        email: '', // preserved (not undefined)
        status: '', // preserved (not undefined)
        version: 0, // preserved (not undefined)
        updatedAt: '', // preserved (not undefined)
        isActive: false, // preserved (not undefined)
      });
    });

    test('works with invalid location parameter', () => {
      const input = {
        id: 'user1',
        name: 'John',
      };

      const result = addDefaultToOptionalFields({
        schema: userSchema,
        value: input,
        operation: 'insert',
        location: 'invalid' as Location,
      });

      // Should include all fields but not apply any defaults
      expect(result).toEqual({
        id: 'user1',
        name: 'John',
        email: null,
        status: null,
        version: null,
        updatedAt: null,
        isActive: null,
      });
    });

    test('handles all combinations of operations and locations', () => {
      const input = {id: 'user1', name: 'John'};

      // Client + Insert
      const clientInsert = addDefaultToOptionalFields({
        schema: userSchema,
        value: input,
        operation: 'insert',
        location: 'client',
      });
      expect(clientInsert.status).toBe('active');
      expect(clientInsert.version).toBe(1);
      expect(clientInsert.isActive).toBe(true);
      expect(clientInsert.updatedAt).toBe(null);

      // Client + Update
      const clientUpdate = addDefaultToOptionalFields({
        schema: userSchema,
        value: input,
        operation: 'update',
        location: 'client',
      });
      expect(clientUpdate.status).toBe('updated');
      expect(clientUpdate.version).toBe(2);
      expect(clientUpdate.updatedAt).toBe('update-time');
      expect(clientUpdate.isActive).toBe(null);

      // Server + Insert
      const serverInsert = addDefaultToOptionalFields({
        schema: userSchema,
        value: input,
        operation: 'insert',
        location: 'server',
      });
      expect(serverInsert.version).toBe(0);
      expect(serverInsert.isActive).toBe(false);
      expect(serverInsert).not.toHaveProperty('status'); // db default, omitted
      expect(serverInsert).not.toHaveProperty('updatedAt'); // no insert default

      // Server + Update
      const serverUpdate = addDefaultToOptionalFields({
        schema: userSchema,
        value: input,
        operation: 'update',
        location: 'server',
      });
      expect(serverUpdate.version).toBe(10);
      expect(serverUpdate).not.toHaveProperty('status'); // db default, omitted
      expect(serverUpdate).not.toHaveProperty('updatedAt'); // db default, omitted
      expect(serverUpdate).not.toHaveProperty('isActive'); // no update default
    });
  });

  describe('additional edge cases', () => {
    test('ensures immutability of input object', () => {
      const input = {id: 'u1', name: 'John'};
      const originalInput = {...input};

      addDefaultToOptionalFields({
        schema: userSchema,
        value: input,
        operation: 'insert',
        location: 'client',
      });

      // Input object should not be mutated
      expect(input).toEqual(originalInput);
    });

    test('ignores extraneous keys not in schema', () => {
      const input: {
        id: string;
        name: string;
        foo: string;
        extraProperty: number;
      } = {
        id: 'u1',
        name: 'John',
        foo: 'bar',
        extraProperty: 123,
      };

      const result = addDefaultToOptionalFields({
        schema: userSchema,
        value: input,
        operation: 'insert',
        location: 'client',
      });

      // Result should not contain properties not in schema
      expect(result).not.toHaveProperty('foo');
      expect(result).not.toHaveProperty('extraProperty');

      // Should only contain schema columns
      expect(Object.keys(result)).toEqual(
        expect.arrayContaining([
          'id',
          'name',
          'email',
          'status',
          'version',
          'updatedAt',
          'isActive',
        ]),
      );
    });

    test('default functions only run when needed', () => {
      // Reset mock function call counts
      mockInsertFn.mockClear();
      mockUpdateFn.mockClear();
      mockInsertFn2.mockClear();
      mockUpdateFn2.mockClear();

      const input = {id: 'mock1'};

      // Test insert operation - only insert functions should be called
      addDefaultToOptionalFields({
        schema: mockTableSchema,
        value: input,
        operation: 'insert',
        location: 'client',
      });

      expect(mockInsertFn).toHaveBeenCalledTimes(1);
      expect(mockInsertFn2).toHaveBeenCalledTimes(1);
      expect(mockUpdateFn).not.toHaveBeenCalled();
      expect(mockUpdateFn2).not.toHaveBeenCalled();

      // Reset and test update operation - only update functions should be called
      mockInsertFn.mockClear();
      mockUpdateFn.mockClear();
      mockInsertFn2.mockClear();
      mockUpdateFn2.mockClear();

      addDefaultToOptionalFields({
        schema: mockTableSchema,
        value: input,
        operation: 'update',
        location: 'client',
      });

      expect(mockUpdateFn).toHaveBeenCalledTimes(1);
      expect(mockUpdateFn2).toHaveBeenCalledTimes(1);
      expect(mockInsertFn).not.toHaveBeenCalled();
      expect(mockInsertFn2).not.toHaveBeenCalled();
    });

    test('default functions not called when values are explicitly provided', () => {
      mockInsertFn.mockClear();
      mockInsertFn2.mockClear();

      const input = {
        id: 'mock1',
        testCol: 'explicit-value',
        anotherCol: 999,
      };

      addDefaultToOptionalFields({
        schema: mockTableSchema,
        value: input,
        operation: 'insert',
        location: 'client',
      });

      // No default functions should be called since values were provided
      expect(mockInsertFn).not.toHaveBeenCalled();
      expect(mockInsertFn2).not.toHaveBeenCalled();
    });
  });
});
