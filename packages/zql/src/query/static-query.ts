import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {Format} from '../ivm/view.ts';
import {ExpressionBuilder} from './expression.ts';
import {AbstractQuery, defaultFormat, newQuerySymbol} from './query-impl.ts';
import type {HumanReadable, PullRow, Query} from './query.ts';
import type {TypedView} from '../ivm/typed-view.ts';

export function staticQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
>(schema: TSchema, tableName: TTable): Query<TSchema, TTable> {
  return new StaticQuery<TSchema, TTable>(
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
  expressionBuilder() {
    return new ExpressionBuilder(this._exists);
  }

  constructor(
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
    currentJunction?: string | undefined,
  ) {
    super(schema, tableName, ast, format, 'permissions', currentJunction);
  }

  protected [newQuerySymbol]<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
    currentJunction: string | undefined,
  ): StaticQuery<TSchema, TTable, TReturn> {
    return new StaticQuery(schema, tableName, ast, format, currentJunction);
  }

  get ast() {
    return this._completeAst();
  }

  materialize(): TypedView<HumanReadable<TReturn>> {
    throw new Error('StaticQuery cannot be materialized');
  }

  run(): Promise<HumanReadable<TReturn>> {
    return Promise.reject(new Error('StaticQuery cannot be run'));
  }

  preload(): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    throw new Error('StaticQuery cannot be preloaded');
  }
}
