import type {Schema} from '../../../zero-types/src/schema.ts';
import {defaultFormat} from '../ivm/default-format.ts';
import {newQueryImpl, type QueryImpl} from './query-impl.ts';
import type {PullRow, Query} from './query.ts';

export function newStaticQuery<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn = PullRow<TTable, TSchema>,
>(schema: TSchema, tableName: TTable): Query<TTable, TSchema, TReturn> {
  return newQueryImpl(
    schema,
    tableName,
    {table: tableName},
    defaultFormat,
    'permissions',
  );
}

export function newExpressionBuilder<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
>(schema: TSchema, tableName: TTable) {
  const q = newStaticQuery(schema, tableName);
  return (q as QueryImpl<TTable, TSchema>).expressionBuilder();
}
