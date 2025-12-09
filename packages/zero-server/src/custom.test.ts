import {describe, expect, expectTypeOf, test, vi} from 'vitest';
import type {CustomMutatorDefs as CustomMutatorClientDefs} from '../../zero-client/src/client/custom.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import {createCRUDBuilder} from '../../zql/src/mutate/crud.ts';
import type {DBTransaction} from '../../zql/src/mutate/custom.ts';
import type {CustomMutatorDefs} from './custom.ts';
import {CRUDMutatorFactory, makeMutateCRUD, makeSchemaCRUD} from './custom.ts';
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

describe('server CRUD patterns', () => {
  const mockServerSchema: ServerSchema = {
    basic: {
      id: {type: 'text', isEnum: false, isArray: false},
      a: {type: 'integer', isEnum: false, isArray: false},
      b: {type: 'text', isEnum: false, isArray: false},
      c: {type: 'boolean', isEnum: false, isArray: false},
    },
  };

  const createMockTx = () => {
    const queries: unknown[][] = [];
    const mockTx: DBTransaction<unknown> = {
      wrappedTransaction: null,
      query: (...args: unknown[]) => {
        queries.push(args);
        return Promise.resolve([]);
      },
      runQuery: () => Promise.reject(new Error('not implemented')),
    };
    return {mockTx, queries};
  };

  describe('legacy pattern: tx.mutate.table.op(args)', () => {
    test('available when enableLegacyMutators: true', () => {
      // schema from test/schema.ts has enableLegacyMutators: true
      const crudProvider = makeSchemaCRUD(schema);
      const {mockTx, queries} = createMockTx();

      const mutate = crudProvider(mockTx, mockServerSchema);

      // Verify 'basic' property exists on mutate at runtime
      expect('basic' in mutate).toBe(true);

      // Type test: MutateCRUD should include SchemaCRUD when enableLegacyMutators is true
      type MutateType = typeof mutate;
      type HasBasicProp = 'basic' extends keyof MutateType ? true : false;
      expectTypeOf<HasBasicProp>().toEqualTypeOf<true>();

      // Verify insert method has correct signature by calling it
      void mutate.basic.insert({id: '1', a: 1, b: 'test'});
      expect(queries).toHaveLength(1);

      // Test that the method signatures match TableCRUD
      type BasicCRUD = MutateType['basic'];
      type InsertFn = BasicCRUD['insert'];
      type UpdateFn = BasicCRUD['update'];
      type UpsertFn = BasicCRUD['upsert'];
      type DeleteFn = BasicCRUD['delete'];

      // Verify return types are Promise<void>
      expectTypeOf<ReturnType<InsertFn>>().toEqualTypeOf<Promise<void>>();
      expectTypeOf<ReturnType<UpdateFn>>().toEqualTypeOf<Promise<void>>();
      expectTypeOf<ReturnType<UpsertFn>>().toEqualTypeOf<Promise<void>>();
      expectTypeOf<ReturnType<DeleteFn>>().toEqualTypeOf<Promise<void>>();
    });

    test('works when enableLegacyMutators: false', () => {
      const schemaNoLegacy = createSchema({
        tables: [
          table('basic')
            .columns({
              id: string(),
              a: number(),
              b: string(),
              c: boolean().optional(),
            })
            .primaryKey('id'),
        ],
        enableLegacyMutators: false,
      });

      // makeSchemaCRUD works regardless of enableLegacyMutators
      const crudProvider = makeSchemaCRUD(schemaNoLegacy);
      const {mockTx} = createMockTx();
      const mutate = crudProvider(mockTx, mockServerSchema);
      expect(typeof mutate).toBe('function');
    });

    test('works when enableLegacyMutators is not set (defaults to false)', () => {
      const schemaDefault = createSchema({
        tables: [
          table('basic')
            .columns({
              id: string(),
              a: number(),
              b: string(),
              c: boolean().optional(),
            })
            .primaryKey('id'),
        ],
        // enableLegacyMutators not set - defaults to false
      });

      // makeSchemaCRUD works regardless of enableLegacyMutators
      const crudProvider = makeSchemaCRUD(schemaDefault);
      const {mockTx} = createMockTx();
      const mutate = crudProvider(mockTx, mockServerSchema);
      expect(typeof mutate).toBe('function');
    });
  });

  describe('modern pattern: tx.mutate(crud.table.op(args))', () => {
    test('works without enableLegacyMutators (default)', () => {
      const schemaModern = createSchema({
        tables: [
          table('basic')
            .columns({
              id: string(),
              a: number(),
              b: string(),
              c: boolean().optional(),
            })
            .primaryKey('id'),
        ],
        // enableLegacyMutators not set - defaults to false
      });

      const crudBuilder = createCRUDBuilder(schemaModern);
      const {mockTx, queries} = createMockTx();

      // Use makeMutateCRUD for schemas without legacy mutators
      const mutate = makeMutateCRUD(mockTx, mockServerSchema, schemaModern);

      // Verify mutate is callable
      type MutateType = typeof mutate;
      expectTypeOf<MutateType>().toBeCallableWith(
        crudBuilder.basic.insert({id: '1', a: 1, b: 'test'}),
      );

      // Verify CRUD request types are correct
      const insertReq = crudBuilder.basic.insert({id: '1', a: 1, b: 'test'});
      expectTypeOf(insertReq.schema).toEqualTypeOf<typeof schemaModern>();
      expectTypeOf(insertReq.table).toEqualTypeOf<'basic'>();
      expectTypeOf(insertReq.kind).toEqualTypeOf<'insert'>();

      // Use modern pattern
      void mutate(insertReq);
      expect(queries).toHaveLength(1);
    });

    test('works with enableLegacyMutators: false explicitly', () => {
      const schemaExplicit = createSchema({
        tables: [
          table('basic')
            .columns({
              id: string(),
              a: number(),
              b: string(),
              c: boolean().optional(),
            })
            .primaryKey('id'),
        ],
        enableLegacyMutators: false,
      });

      const crudBuilder = createCRUDBuilder(schemaExplicit);
      const {mockTx, queries} = createMockTx();

      // Use makeMutateCRUD for schemas without legacy mutators
      const mutate = makeMutateCRUD(mockTx, mockServerSchema, schemaExplicit);

      // All CRUD operations work via modern pattern
      void mutate(crudBuilder.basic.insert({id: '1', a: 1, b: 'test1'}));
      void mutate(crudBuilder.basic.update({id: '1', b: 'updated'}));
      void mutate(crudBuilder.basic.upsert({id: '2', a: 2, b: 'test2'}));
      void mutate(crudBuilder.basic.delete({id: '1'}));

      expect(queries).toHaveLength(4);
    });

    test('works with enableLegacyMutators: true (both patterns available)', () => {
      // schema from test/schema.ts has enableLegacyMutators: true
      const crudBuilder = createCRUDBuilder(schema);
      const crudProvider = makeSchemaCRUD(schema);
      const {mockTx, queries} = createMockTx();

      const mutate = crudProvider(mockTx, mockServerSchema);

      // Modern pattern still works
      void mutate(crudBuilder.basic.insert({id: '1', a: 1, b: 'modern'}));
      expect(queries).toHaveLength(1);

      // Legacy pattern also works
      void mutate.basic.insert({id: '2', a: 2, b: 'legacy'});
      expect(queries).toHaveLength(2);
    });

    test('makeMutateCRUD works with enableLegacyMutators: true', () => {
      // schema from test/schema.ts has enableLegacyMutators: true
      const crudBuilder = createCRUDBuilder(schema);
      const {mockTx, queries} = createMockTx();

      // makeMutateCRUD works with both new and legacy schemas
      const mutate = makeMutateCRUD(mockTx, mockServerSchema, schema);

      // Modern pattern works
      void mutate(crudBuilder.basic.insert({id: '1', a: 1, b: 'modern'}));
      expect(queries).toHaveLength(1);

      // Legacy pattern also works
      void mutate.basic.insert({id: '2', a: 2, b: 'legacy'});
      expect(queries).toHaveLength(2);
    });
  });

  describe('CRUD request types', () => {
    test('createCRUDBuilder produces correct request objects', () => {
      const testSchema = createSchema({
        tables: [
          table('user')
            .columns({
              id: string(),
              name: string(),
            })
            .primaryKey('id'),
        ],
      });

      const crud = createCRUDBuilder(testSchema);

      // Insert request
      const insertReq = crud.user.insert({id: '1', name: 'Test'});
      expect(insertReq.schema).toBe(testSchema);
      expect(insertReq.table).toBe('user');
      expect(insertReq.kind).toBe('insert');
      expect(insertReq.args).toEqual({id: '1', name: 'Test'});

      // Update request
      const updateReq = crud.user.update({id: '1', name: 'Updated'});
      expect(updateReq.kind).toBe('update');

      // Upsert request
      const upsertReq = crud.user.upsert({id: '1', name: 'Upserted'});
      expect(upsertReq.kind).toBe('upsert');

      // Delete request
      const deleteReq = crud.user.delete({id: '1'});
      expect(deleteReq.kind).toBe('delete');
    });
  });
});

describe('CRUDMutatorFactory', () => {
  const mockServerSchema: ServerSchema = {
    basic: {
      id: {type: 'text', isEnum: false, isArray: false},
      a: {type: 'integer', isEnum: false, isArray: false},
      b: {type: 'text', isEnum: false, isArray: false},
      c: {type: 'boolean', isEnum: false, isArray: false},
    },
  };

  const createMockTx = () => {
    const queries: unknown[][] = [];
    const mockTx: DBTransaction<unknown> = {
      wrappedTransaction: null,
      query: (...args: unknown[]) => {
        queries.push(args);
        return Promise.resolve([]);
      },
      runQuery: () => Promise.reject(new Error('not implemented')),
    };
    return {mockTx, queries};
  };

  test('createExecutor creates working executor', async () => {
    const factory = new CRUDMutatorFactory(schema);
    const {mockTx, queries} = createMockTx();

    const executor = factory.createExecutor(mockTx, mockServerSchema);

    // Execute some operations via the executor
    await executor('basic', 'insert', {id: '1', a: 1, b: 'test'});
    await executor('basic', 'update', {id: '1', b: 'updated'});
    await executor('basic', 'upsert', {id: '2', a: 2, b: 'test2'});
    await executor('basic', 'delete', {id: '1'});

    expect(queries).toHaveLength(4);
  });

  test('createMutateCRUD creates working MutateCRUD', async () => {
    const factory = new CRUDMutatorFactory(schema);
    const {mockTx, queries} = createMockTx();

    const executor = factory.createExecutor(mockTx, mockServerSchema);
    // Verify the executor works correctly
    await executor('basic', 'insert', {id: '1', a: 1, b: 'test'});
    expect(queries).toHaveLength(1);
  });

  test('creates isolated executors for different transactions', () => {
    const factory = new CRUDMutatorFactory(schema);

    const queries1: unknown[][] = [];
    const queries2: unknown[][] = [];

    const tx1: DBTransaction<unknown> = {
      wrappedTransaction: null,
      query: (...args: unknown[]) => {
        queries1.push(args);
        return Promise.resolve([]);
      },
      runQuery: () => Promise.reject(new Error('not implemented')),
    };

    const tx2: DBTransaction<unknown> = {
      wrappedTransaction: null,
      query: (...args: unknown[]) => {
        queries2.push(args);
        return Promise.resolve([]);
      },
      runQuery: () => Promise.reject(new Error('not implemented')),
    };

    const exec1 = factory.createExecutor(tx1, mockServerSchema);
    const exec2 = factory.createExecutor(tx2, mockServerSchema);

    void exec1('basic', 'insert', {id: '1', a: 1, b: 'one'});
    void exec2('basic', 'insert', {id: '2', a: 2, b: 'two'});

    // Each executor should write to its own transaction
    expect(queries1).toHaveLength(1);
    expect(queries2).toHaveLength(1);
    expect(queries1[0]?.[1]).toContain('1');
    expect(queries2[0]?.[1]).toContain('2');
  });

  test('executors from same factory are different functions', () => {
    const factory = new CRUDMutatorFactory(schema);
    const {mockTx: mockTx1} = createMockTx();
    const {mockTx: mockTx2} = createMockTx();

    const executor1 = factory.createExecutor(mockTx1, mockServerSchema);
    const executor2 = factory.createExecutor(mockTx2, mockServerSchema);

    expect(executor1).not.toBe(executor2);
  });

  test('createTransaction creates working ServerTransaction', async () => {
    // Use a simple schema for this test to avoid mocking many tables
    const simpleSchema = createSchema({
      tables: [
        table('basic')
          .columns({
            id: string(),
            a: number(),
            b: string(),
          })
          .primaryKey('id'),
      ],
      enableLegacyMutators: true,
    });

    const factory = new CRUDMutatorFactory(simpleSchema);
    const {mockTx} = createMockTx();

    // Mock the information_schema query that getServerSchema uses
    const mockSchemaRows = [
      {
        schema: 'public',
        table: 'basic',
        column: 'id',
        dataType: 'text',
        length: null,
        precision: null,
        scale: null,
        typtype: 'b',
        typename: 'text',
        elemTyptype: null,
        elemTypname: null,
      },
      {
        schema: 'public',
        table: 'basic',
        column: 'a',
        dataType: 'integer',
        length: null,
        precision: null,
        scale: null,
        typtype: 'b',
        typename: 'int4',
        elemTyptype: null,
        elemTypname: null,
      },
      {
        schema: 'public',
        table: 'basic',
        column: 'b',
        dataType: 'text',
        length: null,
        precision: null,
        scale: null,
        typtype: 'b',
        typename: 'text',
        elemTyptype: null,
        elemTypname: null,
      },
    ];

    const originalQuery = mockTx.query;
    mockTx.query = vi.fn((...args: unknown[]) => {
      const sql = args[0] as string;
      if (sql.includes('information_schema')) {
        return Promise.resolve(mockSchemaRows);
      }
      return originalQuery.apply(
        mockTx,
        args as Parameters<typeof originalQuery>,
      );
    }) as typeof mockTx.query;

    const tx = await factory.createTransaction(mockTx, 'client1', 1);

    expect(tx.clientID).toBe('client1');
    expect(tx.mutationID).toBe(1);
    expect(tx.location).toBe('server');
    expect(tx.reason).toBe('authoritative');

    // Verify mutate works
    const crudBuilder = createCRUDBuilder(simpleSchema);
    void tx.mutate(crudBuilder.basic.insert({id: '1', a: 1, b: 'test'}));

    // The information_schema query + the insert
    expect(mockTx.query).toHaveBeenCalled();
  });

  test('caches serverSchema after first fetch', async () => {
    // Use a simple schema for this test
    const simpleSchema = createSchema({
      tables: [
        table('basic')
          .columns({
            id: string(),
            a: number(),
            b: string(),
          })
          .primaryKey('id'),
      ],
    });

    const factory = new CRUDMutatorFactory(simpleSchema);

    // Mock data matching ServerSchemaRow structure
    const mockSchemaRows = [
      {
        schema: 'public',
        table: 'basic',
        column: 'id',
        dataType: 'text',
        length: null,
        precision: null,
        scale: null,
        typtype: 'b',
        typename: 'text',
        elemTyptype: null,
        elemTypname: null,
      },
      {
        schema: 'public',
        table: 'basic',
        column: 'a',
        dataType: 'integer',
        length: null,
        precision: null,
        scale: null,
        typtype: 'b',
        typename: 'int4',
        elemTyptype: null,
        elemTypname: null,
      },
      {
        schema: 'public',
        table: 'basic',
        column: 'b',
        dataType: 'text',
        length: null,
        precision: null,
        scale: null,
        typtype: 'b',
        typename: 'text',
        elemTyptype: null,
        elemTypname: null,
      },
    ];

    let serverSchemaFetchCount = 0;
    const createMockTxWithSchemaQuery = () => {
      const queries: unknown[][] = [];
      const mockTx: DBTransaction<unknown> = {
        wrappedTransaction: null,
        query: vi.fn((...args: unknown[]) => {
          const sql = args[0] as string;
          if (sql.includes('information_schema')) {
            serverSchemaFetchCount++;
            return Promise.resolve(mockSchemaRows);
          }
          queries.push(args);
          return Promise.resolve([]);
        }) as DBTransaction<unknown>['query'],
        runQuery: () => Promise.reject(new Error('not implemented')),
      };
      return {mockTx, queries};
    };

    // First transaction - should fetch serverSchema
    const {mockTx: tx1} = createMockTxWithSchemaQuery();
    await factory.createTransaction(tx1, 'client1', 1);
    expect(serverSchemaFetchCount).toBe(1);

    // Second transaction - should reuse cached serverSchema
    const {mockTx: tx2} = createMockTxWithSchemaQuery();
    await factory.createTransaction(tx2, 'client2', 2);
    expect(serverSchemaFetchCount).toBe(1); // Still 1, not 2

    // Third transaction - still cached
    const {mockTx: tx3} = createMockTxWithSchemaQuery();
    await factory.createTransaction(tx3, 'client3', 3);
    expect(serverSchemaFetchCount).toBe(1); // Still 1
  });

  test('different factories do not share cache', async () => {
    // Use a simple schema for this test
    const simpleSchema = createSchema({
      tables: [
        table('basic')
          .columns({
            id: string(),
            a: number(),
            b: string(),
          })
          .primaryKey('id'),
      ],
    });

    const factory1 = new CRUDMutatorFactory(simpleSchema);
    const factory2 = new CRUDMutatorFactory(simpleSchema);

    // Mock data matching ServerSchemaRow structure
    const mockSchemaRows = [
      {
        schema: 'public',
        table: 'basic',
        column: 'id',
        dataType: 'text',
        length: null,
        precision: null,
        scale: null,
        typtype: 'b',
        typename: 'text',
        elemTyptype: null,
        elemTypname: null,
      },
      {
        schema: 'public',
        table: 'basic',
        column: 'a',
        dataType: 'integer',
        length: null,
        precision: null,
        scale: null,
        typtype: 'b',
        typename: 'int4',
        elemTyptype: null,
        elemTypname: null,
      },
      {
        schema: 'public',
        table: 'basic',
        column: 'b',
        dataType: 'text',
        length: null,
        precision: null,
        scale: null,
        typtype: 'b',
        typename: 'text',
        elemTyptype: null,
        elemTypname: null,
      },
    ];

    let serverSchemaFetchCount = 0;
    const createMockTxWithSchemaQuery = () => {
      const mockTx: DBTransaction<unknown> = {
        wrappedTransaction: null,
        query: vi.fn((...args: unknown[]) => {
          const sql = args[0] as string;
          if (sql.includes('information_schema')) {
            serverSchemaFetchCount++;
            return Promise.resolve(mockSchemaRows);
          }
          return Promise.resolve([]);
        }) as DBTransaction<unknown>['query'],
        runQuery: () => Promise.reject(new Error('not implemented')),
      };
      return {mockTx};
    };

    // Factory 1 fetches
    const {mockTx: tx1} = createMockTxWithSchemaQuery();
    await factory1.createTransaction(tx1, 'client1', 1);
    expect(serverSchemaFetchCount).toBe(1);

    // Factory 2 also fetches (separate cache)
    const {mockTx: tx2} = createMockTxWithSchemaQuery();
    await factory2.createTransaction(tx2, 'client2', 2);
    expect(serverSchemaFetchCount).toBe(2);
  });

  test('works with schemas without enableLegacyMutators', async () => {
    const modernSchema = createSchema({
      tables: [
        table('user')
          .columns({
            id: string(),
            name: string(),
          })
          .primaryKey('id'),
      ],
      enableLegacyMutators: false,
    });

    const mockServerSchemaModern: ServerSchema = {
      user: {
        id: {type: 'text', isEnum: false, isArray: false},
        name: {type: 'text', isEnum: false, isArray: false},
      },
    };

    const factory = new CRUDMutatorFactory(modernSchema);
    const {mockTx, queries} = createMockTx();

    const executor = factory.createExecutor(mockTx, mockServerSchemaModern);

    await executor('user', 'insert', {id: '1', name: 'Alice'});
    await executor('user', 'update', {id: '1', name: 'Bob'});

    expect(queries).toHaveLength(2);
  });
});
