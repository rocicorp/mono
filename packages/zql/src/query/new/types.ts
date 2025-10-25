import type {Schema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import type {PullRow, Query} from '../query.ts';

/**
 * Function type for root query functions that take context and args.
 */
export type Func<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TArgs,
> = (options: {
  ctx: TContext;
  args: TArgs;
}) => Query<TSchema, TTable, TReturn, TContext>;

/**
 * Function type for chaining one query to another.
 */
type ChainQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn1,
  TReturn2,
  TContext,
> = (
  q: Query<TSchema, TTable, TReturn1, TContext>,
) => Query<TSchema, TTable, TReturn2, TContext>;

export type AnyChainQuery = ChainQuery<
  Schema,
  string,
  PullRow<string, Schema>,
  PullRow<string, Schema>,
  unknown
>;
