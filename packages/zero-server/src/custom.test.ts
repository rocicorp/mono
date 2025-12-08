import {expect, expectTypeOf, test} from 'vitest';
import type {CustomMutatorDefs as CustomMutatorClientDefs} from '../../zero-client/src/client/custom.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import type {DBTransaction} from '../../zql/src/mutate/custom.ts';
import type {CustomMutatorDefs} from './custom.ts';
import {makeSchemaCRUD} from './custom.ts';
import {schema} from './test/schema.ts';

test('server mutator type is compatible with client mutator type', () => {
  expectTypeOf<
    CustomMutatorDefs<unknown>
  >().toExtend<CustomMutatorClientDefs>();
});

test('makeSchemaCRUD shares table CRUD across transactions but isolates bindings', () => {
  const mockServerSchema: ServerSchema = {
    basic: {
      id: {type: 'text', isEnum: false, isArray: false},
      a: {type: 'integer', isEnum: false, isArray: false},
      b: {type: 'text', isEnum: false, isArray: false},
      c: {type: 'boolean', isEnum: false, isArray: false},
    },
  };

  const crudProvider = makeSchemaCRUD(schema);

  // Create mock transactions
  const queries1: unknown[][] = [];
  const queries2: unknown[][] = [];

  const mockTx1: DBTransaction<unknown> = {
    wrappedTransaction: null,
    query: (...args: unknown[]) => {
      queries1.push(args);
      return Promise.resolve([]);
    },
    runQuery: () => Promise.reject(new Error('not implemented')),
  };

  const mockTx2: DBTransaction<unknown> = {
    wrappedTransaction: null,
    query: (...args: unknown[]) => {
      queries2.push(args);
      return Promise.resolve([]);
    },
    runQuery: () => Promise.reject(new Error('not implemented')),
  };

  // Get CRUD for two different transactions
  const crud1 = crudProvider(mockTx1, mockServerSchema);
  const crud2 = crudProvider(mockTx2, mockServerSchema);

  // Access the same table from both
  const tableCrud1 = crud1.basic;
  const tableCrud2 = crud2.basic;

  // They should be different objects (different bound methods)
  expect(tableCrud1).not.toBe(tableCrud2);

  // But calling insert on each should use the correct transaction
  void tableCrud1.insert({id: '1', a: 1, b: 'one'});
  void tableCrud2.insert({id: '2', a: 2, b: 'two'});

  // Verify the queries went to the right transactions (values are parameterized)
  expect(queries1).toHaveLength(1);
  expect(queries2).toHaveLength(1);
  expect(queries1[0]?.[1]).toContain('1'); // The values array
  expect(queries2[0]?.[1]).toContain('2');
});

test('makeSchemaCRUD caches bound methods per transaction', () => {
  const mockServerSchema: ServerSchema = {
    basic: {
      id: {type: 'text', isEnum: false, isArray: false},
      a: {type: 'integer', isEnum: false, isArray: false},
      b: {type: 'text', isEnum: false, isArray: false},
      c: {type: 'boolean', isEnum: false, isArray: false},
    },
  };

  const crudProvider = makeSchemaCRUD(schema);

  const mockTx: DBTransaction<unknown> = {
    wrappedTransaction: null,
    query: () => Promise.resolve([]),
    runQuery: () => Promise.reject(new Error('not implemented')),
  };

  const crud = crudProvider(mockTx, mockServerSchema);

  // Access the same table twice
  const tableCrud1 = crud.basic;
  const tableCrud2 = crud.basic;

  // Should be the same cached object
  expect(tableCrud1).toBe(tableCrud2);
});
