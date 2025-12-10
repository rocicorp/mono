import type {AST, System} from '../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {defaultFormat} from '../ivm/default-format.ts';
import type {Format} from '../ivm/view.ts';
import type {CustomQueryID} from './named.ts';
import {QueryImpl} from './query-impl.ts';
import type {PullRow} from './query.ts';

export function newStaticQuery<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn = PullRow<TTable, TSchema>,
>(schema: TSchema, tableName: TTable): StaticQuery<TTable, TSchema, TReturn> {
  return new StaticQuery<TTable, TSchema, TReturn>(
    schema,
    tableName,
    {table: tableName},
    defaultFormat,
  );
}

/**
 * A query that cannot be run.
 * Only serves to generate ASTs.
 */
export class StaticQuery<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn = PullRow<TTable, TSchema>,
> extends QueryImpl<TTable, TSchema, TReturn> {
  constructor(
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
    system: System = 'permissions',
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
        new StaticQuery(
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
