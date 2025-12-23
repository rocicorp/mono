import {
  createBuilder,
  type ConditionalSchemaQuery,
  type HumanReadable,
  type Query,
  type RunOptions,
  type Schema,
  type SchemaQuery,
} from './zero.ts';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import type {ClientTransaction} from './server.ts';
import type {SchemaCRUD, CRUDKind} from '../../zql/src/mutate/crud.ts';
import {makeTransactionMutate} from '../../zql/src/mutate/crud.ts';
import {createRunnableBuilder} from '../../zql/src/query/create-builder.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import type {SourceChange} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import {must} from '../../shared/src/must.ts';

export class MemoryZQL<TSchema extends Schema> {
  #sources: Record<string, MemorySource>;
  #pendingTxSources: Record<string, MemorySource> | undefined;
  #schemaCRUD: SchemaCRUD<TSchema>;
  #schemaQuery: SchemaQuery<TSchema>;
  #delegate: QueryDelegateImpl;
  #schema: TSchema;

  constructor(schema: TSchema, sources?: Record<string, MemorySource>) {
    this.#schema = schema;
    this.#schemaQuery = createBuilder(schema);
    this.#sources = sources ?? makeSources(schema);
    this.#schemaCRUD = makeSchemaCRUD(this.#schema, this.#sources);
    this.#delegate = new QueryDelegateImpl({sources: this.#sources});
  }

  reset() {
    if (this.#pendingTxSources) {
      throw new Error('Cannot reset while there is a pending transaction');
    }
    this.#sources = makeSources(this.#schema);
    this.#schemaCRUD = makeSchemaCRUD(this.#schema, this.#sources);
    this.#delegate = new QueryDelegateImpl({sources: this.#sources});
  }

  /**
   * Hydrates memory2 with initial data from raw rows.
   * For each table, applies 'add' pushes and consumes them to commit.
   */
  hydrate(raw: Map<keyof TSchema['tables'], Row[]>): void {
    for (const [table, rows] of raw) {
      const source = must(this.#sources[table as string]);
      for (const row of rows) {
        consume(
          source.push({
            type: 'add',
            row,
          }),
        );
      }
    }
  }

  push(table: string, change: SourceChange): void {
    const source = must(this.#sources[table]);
    consume(source.push(change));
  }

  fork(): MemoryZQL<TSchema> {
    return new MemoryZQL(this.#schema, forkSources(this.#sources));
  }

  run<TTable extends keyof TSchema['tables'] & string, TReturn>(
    query: Query<TTable, TSchema, TReturn>,
    runOptions?: RunOptions,
  ) {
    return this.#delegate.run(query, runOptions);
  }

  get delegate() {
    return this.#delegate;
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
      throw new Error('Can only run one transaction at a time');
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
    } finally {
      this.#pendingTxSources = undefined;
    }
  }
}

function forkSources(sources: Record<string, MemorySource>) {
  return Object.fromEntries(
    Object.entries(sources).map(([key, source]) => [key, source.fork()]),
  ) as Record<string, MemorySource>;
}

function makeSources<TSchema extends Schema>(schema: TSchema) {
  return Object.fromEntries(
    Object.entries(schema.tables).map(([key, tableSchema]) => [
      key,
      new MemorySource(
        tableSchema.name,
        tableSchema.columns,
        tableSchema.primaryKey,
      ),
    ]),
  ) as Record<string, MemorySource>;
}

function makeSchemaCRUD<TSchema extends Schema>(
  schema: TSchema,
  sources: Record<string, MemorySource>,
): SchemaCRUD<TSchema> {
  const executor = (table: string, kind: CRUDKind, args: unknown) => {
    const source = must(sources[table]);
    const row = args as Row;
    switch (kind) {
      case 'insert': {
        consume(
          source.push({
            type: 'add',
            row,
          }),
        );
        return Promise.resolve();
      }
      case 'upsert': {
        const oldRow = source.data.get(row);
        if (oldRow) {
          consume(
            source.push({
              type: 'edit',
              oldRow,
              row: {...oldRow, ...row},
            }),
          );
        } else {
          consume(
            source.push({
              type: 'add',
              row,
            }),
          );
        }
        return Promise.resolve();
      }
      case 'update': {
        const oldRow = must(source.data.get(row));
        consume(
          source.push({
            type: 'edit',
            oldRow,
            row: {...oldRow, ...row},
          }),
        );
        return Promise.resolve();
      }
      case 'delete': {
        const oldRow = must(source.data.get(row));
        consume(
          source.push({
            type: 'remove',
            row: oldRow,
          }),
        );
        return Promise.resolve();
      }
    }
  };

  return makeTransactionMutate(schema, executor);
}

class TransactionImpl<TSchema extends Schema>
  implements ClientTransaction<TSchema>
{
  readonly location = 'client';
  readonly mutate: SchemaCRUD<TSchema>;
  readonly query: ConditionalSchemaQuery<TSchema>;
  readonly reason = 'optimistic';
  readonly clientID = '';
  readonly mutationID = 0;
  #delegate: QueryDelegateImpl;

  constructor(schema: TSchema, sources: Record<string, MemorySource>) {
    this.#delegate = new QueryDelegateImpl({sources});
    this.mutate = makeSchemaCRUD(schema, sources);
    this.query = createRunnableBuilder(this.#delegate, schema);
  }

  run<TTable extends keyof TSchema['tables'] & string, TReturn>(
    query: Query<TTable, TSchema, TReturn>,
    options?: RunOptions,
  ): Promise<HumanReadable<TReturn>> {
    return this.#delegate.run(query, options);
  }
}
