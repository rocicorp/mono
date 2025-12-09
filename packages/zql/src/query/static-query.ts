import {assert} from '../../../shared/src/asserts.ts';
import type {AST, System} from '../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {defaultFormat} from '../ivm/default-format.ts';
import type {Format} from '../ivm/view.ts';
import {AbstractQuery} from './abstract-query.ts';
import {ExpressionBuilder} from './expression.ts';
import type {CustomQueryID} from './named.ts';
import type {PullRow, Query} from './query.ts';

// oxlint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyStaticQuery = StaticQuery<string, Schema, any>;

export function staticQuery<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn = PullRow<TTable, TSchema>,
>(schema: TSchema, tableName: TTable): Query<TTable, TSchema, TReturn> {
  return new StaticQuery<TTable, TSchema, TReturn>(
    schema,
    tableName,
    {table: tableName},
    defaultFormat,
  );
}

/**
 * A query builder that cannot be run.
 * Only serves to generate ASTs.
 */
export class StaticQuery<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn = PullRow<TTable, TSchema>,
> extends AbstractQuery<TTable, TSchema, TReturn> {
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
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn,
>(q: Query<TTable, TSchema, TReturn>): StaticQuery<TTable, TSchema, TReturn> {
  assert(q instanceof StaticQuery);
  return q;
}
