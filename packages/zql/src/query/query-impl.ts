import {type AST, type System} from '../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {defaultFormat} from '../ivm/default-format.ts';
import type {Format} from '../ivm/view.ts';
import {AbstractQuery} from './abstract-query.ts';
import type {CustomQueryID} from './named.ts';
import {type PullRow, type QueryBuilder, type ToZQL} from './query-builder.ts';

export function newQuery<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
>(schema: TSchema, table: TTable): QueryBuilder<TTable, TSchema> {
  return new QueryImpl(schema, table, {table}, defaultFormat, undefined);
}

export class QueryImpl<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn = PullRow<TTable, TSchema>,
  >
  extends AbstractQuery<TTable, TSchema, TReturn>
  implements
    QueryBuilder<TTable, TSchema, TReturn>,
    ToZQL<TTable, TSchema, TReturn, unknown>
{
  constructor(
    schema: TSchema,
    tableName: TTable,
    ast: AST = {table: tableName},
    format: Format = defaultFormat,
    system: System = 'client',
    customQueryID?: CustomQueryID,
    currentJunction?: string,
  ) {
    super(
      schema,
      tableName,
      ast,
      format,
      system,
      customQueryID,
      currentJunction,
      (tableName, ast, format, customQueryID, currentJunction) =>
        new QueryImpl(
          schema,
          tableName,
          ast,
          format,
          system,
          customQueryID,
          currentJunction,
        ),
    );
  }
}
