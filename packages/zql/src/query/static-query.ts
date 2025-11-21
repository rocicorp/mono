import {assert} from '../../../shared/src/asserts.ts';
import type {AST, System} from '../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {defaultFormat} from '../ivm/default-format.ts';
import type {Format} from '../ivm/view.ts';
import {ExpressionBuilder} from './expression.ts';
import type {CustomQueryID} from './named.ts';
import {AbstractQuery} from './query-impl.ts';
import type {PullRow, Query} from './query.ts';

export function staticQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
>(schema: TSchema, tableName: TTable): Query<TSchema, TTable, TReturn> {
  return new StaticQuery<TSchema, TTable, TReturn>(
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
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> extends AbstractQuery<TSchema, TTable, TReturn> {
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
      (tableName, ast, format, _customQueryID, _currentJunction) =>
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

  expressionBuilder() {
    return new ExpressionBuilder(this._exists);
  }
}

export function asStaticQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(q: Query<TSchema, TTable, TReturn>): StaticQuery<TSchema, TTable, TReturn> {
  assert(q instanceof StaticQuery);
  return q;
}
