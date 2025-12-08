import {assert} from '../../../shared/src/asserts.ts';
import type {AST, System} from '../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {defaultFormat} from '../ivm/default-format.ts';
import type {Format} from '../ivm/view.ts';
import {AbstractQuery} from './abstract-query.ts';
import {ExpressionBuilder} from './expression.ts';
import type {CustomQueryID} from './named.ts';
import type {PullRow, QueryBuilder} from './query-builder.ts';

// oxlint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyStaticQueryBuilder = StaticQueryBuilder<string, Schema, any>;

export function staticQueryBuilder<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn = PullRow<TTable, TSchema>,
>(schema: TSchema, tableName: TTable): QueryBuilder<TTable, TSchema, TReturn> {
  return new StaticQueryBuilder<TTable, TSchema, TReturn>(
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
export class StaticQueryBuilder<
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
        new StaticQueryBuilder(
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

export function asStaticQueryBuilder<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn,
>(
  q: QueryBuilder<TTable, TSchema, TReturn>,
): StaticQueryBuilder<TTable, TSchema, TReturn> {
  assert(q instanceof StaticQueryBuilder);
  return q;
}
