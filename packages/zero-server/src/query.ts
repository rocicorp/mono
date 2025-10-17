import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {ServerSchema} from '../../zero-schema/src/server-schema.ts';
import {defaultFormat} from '../../zero-types/src/format.ts';
import type {DBTransaction, SchemaQuery} from '../../zql/src/mutate/custom.ts';
import type {Query} from '../../zql/src/query/query.ts';
import {ZPGQuery} from './zpg-query.ts';

class SchemaQueryHandler<TSchema extends Schema, TContext> {
  readonly #dbTransaction: DBTransaction<unknown>;
  readonly #schema: TSchema;
  readonly #serverSchema: ServerSchema;

  constructor(
    dbTransaction: DBTransaction<unknown>,
    schema: TSchema,
    serverSchema: ServerSchema,
  ) {
    this.#dbTransaction = dbTransaction;
    this.#schema = schema;
    this.#serverSchema = serverSchema;
  }

  get(
    target: Record<
      string,
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      Omit<Query<TSchema, any, any, TContext>, 'materialize' | 'preload'>
    >,
    prop: string,
  ) {
    if (prop in target) {
      return target[prop];
    }

    if (!(prop in this.#schema.tables)) {
      throw new Error(`Table ${prop} does not exist in schema`);
    }

    const q = new ZPGQuery<TSchema, typeof prop, unknown, TContext>(
      this.#schema,
      this.#serverSchema,
      prop,
      this.#dbTransaction,
      {table: prop},
      defaultFormat,
    );
    target[prop] = q;
    return q;
  }
}

export function makeSchemaQuery<S extends Schema, TContext>(
  schema: S,
): (
  dbTransaction: DBTransaction<unknown>,
  serverSchema: ServerSchema,
) => SchemaQuery<S, TContext> {
  return (dbTransaction: DBTransaction<unknown>, serverSchema: ServerSchema) =>
    new Proxy(
      {},
      new SchemaQueryHandler<S, TContext>(dbTransaction, schema, serverSchema),
    ) as SchemaQuery<S, TContext>;
}
