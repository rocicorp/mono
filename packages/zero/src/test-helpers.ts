import {
    createBuilder,
    type HumanReadable,
    type Query,
    type RunOptions,
    type Schema,
    type TableSchema,
  } from './zero.ts';
  
  import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
  import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
  import type {
    ClientTransaction,
    SchemaCRUD,
    SchemaQuery,
    TableCRUD,
  } from './pg.ts';
  
  export type MemoryZero<TSchema extends Schema> = {
    run<TTable extends keyof TSchema['tables'] & string, TReturn>(
      query: Query<TSchema, TTable, TReturn>,
      runOptions?: RunOptions,
    ): Promise<HumanReadable<TReturn>>;
    query: SchemaQuery<TSchema>;
    mutate: SchemaCRUD<TSchema>;
    transaction(
      cb: (tx: ClientTransaction<TSchema>) => Promise<void>,
    ): Promise<void>;
    reset(): Promise<void>;
  };
  
  export class MemoryZeroImpl<TSchema extends Schema>
    implements MemoryZero<TSchema>
  {
    #sources: Record<string, MemorySource>;
    #pendingTxSources: Record<string, MemorySource> | undefined;
    #schemaCRUD: SchemaCRUD<TSchema>;
    #schemaQuery: SchemaQuery<TSchema>;
    #delegate: QueryDelegateImpl;
    #schema: TSchema;
  
    constructor(schema: TSchema) {
      this.#schema = schema;
      this.#schemaQuery = createBuilder(schema);
      this.#sources = createSources(schema);
      this.#schemaCRUD = createSchemaCRUD(this.#sources);
      this.#delegate = new QueryDelegateImpl({sources: this.#sources});
    }
  
    async reset() {
      if (this.#pendingTxSources) {
        throw new Error('Cannot reset while there is a pending transaction');
      }
      this.#sources = createSources(this.#schema);
      this.#schemaCRUD = createSchemaCRUD(this.#sources);
      this.#delegate = new QueryDelegateImpl({sources: this.#sources});
    }
  
    run<TTable extends keyof TSchema['tables'] & string, TReturn>(
      query: Query<TSchema, TTable, TReturn>,
      runOptions?: RunOptions,
    ) {
      return this.#delegate.run(query, runOptions);
    }
  
    get mutate() {
      return this.#schemaCRUD;
    }
    get query() {
      return this.#schemaQuery;
    }
  
    async transaction(
      cb: (tx: ClientTransaction<TSchema>) => Promise<void>,
    ): Promise<void> {
      if (this.#pendingTxSources) {
        throw new Error('Cannot reset while there is a pending transaction');
      }
      this.#pendingTxSources = forkSources(this.#sources);
      const transaction = new TransactionImpl(
        this.#schema,
        this.#pendingTxSources,
      );
      try {
        await cb(transaction);
        this.#sources = this.#pendingTxSources;
        this.#delegate = new QueryDelegateImpl({sources: this.#sources});
      } catch (error) {
        throw error;
      } finally {
        this.#pendingTxSources = undefined;
      }
    }
  }
  
  function createSources<TSchema extends Schema>(schema: TSchema) {
    const sources = {} as Record<string, MemorySource>;
    for (const [key, tableSchema] of Object.entries(schema.tables)) {
      sources[key] = new MemorySource(
        tableSchema.name,
        tableSchema.columns,
        tableSchema.primaryKey,
      );
    }
    return sources;
  }
  
  function forkSources(
    sources: Record<string, MemorySource>,
  ): Record<string, MemorySource> {
    const forked = {} as Record<string, MemorySource>;
    for (const [key, source] of Object.entries(sources)) {
      forked[key] = source.fork();
    }
    return forked;
  }
  
  function createSchemaCRUD<TSchema extends Schema>(
    sources: Record<string, MemorySource>,
  ) {
    const schemaCRUD: Record<string, TableCRUD<TableSchema>> = {};
    for (const key of Object.keys(sources)) {
      schemaCRUD[key] = makeTableCrud(sources[key]);
    }
    return schemaCRUD as SchemaCRUD<TSchema>;
  }
  
  function makeTableCrud<TSchema extends Schema>(
    source: MemorySource,
  ): SchemaCRUD<TSchema>[keyof TSchema['tables']] {
    return {
      insert: async value => {
        source.push({
          type: 'add',
          row: value,
        });
      },
      upsert: async value => {
        const oldRow = source.data.get(value);
        if (oldRow) {
          source.push({
            type: 'edit',
            oldRow,
            row: {...oldRow, ...value},
          });
        } else {
          source.push({
            type: 'add',
            row: value,
          });
        }
      },
      update: async value => {
        const oldRow = source.data.get(value);
        if (!oldRow) {
          throw new Error('Row not found');
        }
        source.push({
          type: 'edit',
          oldRow,
          row: {...oldRow, ...value},
        });
      },
      delete: async value => {
        const oldRow = source.data.get(value);
        if (!oldRow) {
          throw new Error('Row not found');
        }
        source.push({
          type: 'remove',
          row: oldRow,
        });
      },
    };
  }
  
  class TransactionImpl<TSchema extends Schema>
    implements ClientTransaction<TSchema>
  {
    readonly location = 'client';
    readonly mutate: SchemaCRUD<TSchema>;
    readonly query: SchemaQuery<TSchema>;
    readonly reason = 'optimistic';
    readonly clientID = '';
    readonly mutationID = 0;
    #delegate: QueryDelegateImpl;
  
    // Sources should already be forked
    constructor(schema: TSchema, sources: Record<string, MemorySource>) {
      this.#delegate = new QueryDelegateImpl({sources});
      this.mutate = createSchemaCRUD(sources);
      this.query = createBuilder(schema);
    }
  
    run<TTable extends keyof TSchema['tables'] & string, TReturn>(
      query: Query<TSchema, TTable, TReturn>,
      options?: RunOptions,
    ): Promise<HumanReadable<TReturn>> {
      return this.#delegate.run(query, options);
    }
  }
  