import type {ServerSchema} from '../../zero-schema/src/server-schema.ts';
import {defaultFormat} from '../../zero-types/src/format.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {DBTransaction, SchemaQuery} from '../../zql/src/mutate/custom.ts';
import type {Query} from '../../zql/src/query/query.ts';
import {ZPGQuery} from './zpg-query.ts';

export function makeSchemaQuery<S extends Schema, TContext>(
  schema: S,
): (
  dbTransaction: DBTransaction<unknown>,
  serverSchema: ServerSchema,
) => SchemaQuery<S, TContext> {
  class SchemaQueryHandler {
    readonly #dbTransaction: DBTransaction<unknown>;
    readonly #serverSchema: ServerSchema;
    constructor(
      dbTransaction: DBTransaction<unknown>,
      serverSchema: ServerSchema,
    ) {
      this.#dbTransaction = dbTransaction;
      this.#serverSchema = serverSchema;
    }

    get(
      target: Record<
        string,
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
        Omit<Query<S, string, any>, 'materialize' | 'preload'>
      >,
      prop: string,
    ) {
      if (prop in target) {
        return target[prop];
      }

      if (!(prop in schema.tables)) {
        throw new Error(`Table ${prop} does not exist in schema`);
      }

      const q = new ZPGQuery(
        schema,
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

  return (dbTransaction: DBTransaction<unknown>, serverSchema: ServerSchema) =>
    new Proxy(
      {},
      new SchemaQueryHandler(dbTransaction, serverSchema),
    ) as SchemaQuery<S, TContext>;
}
