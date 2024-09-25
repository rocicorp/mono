import {
  DefaultQueryResultRow,
  Query,
  QueryType,
  Smash,
} from 'zql/src/zql/query/query.js';
import {Schema} from 'zql/src/zql/query/schema.js';
import {AbstractQuery} from 'zql/src/zql/query/query-impl.js';
import {Format} from 'zql/src/zql/ivm/array-view.js';
import {AST} from 'zql/src/zql/ast/ast.js';
import {TypedView} from 'zql/src/zql/query/typed-view.js';

export class ConfigQuery<
  TSchema extends Schema,
  TReturn extends QueryType = DefaultQueryResultRow<TSchema>,
> extends AbstractQuery<TSchema, TReturn> {
  constructor(
    schema: TSchema,
    ast?: AST | undefined,
    format?: Format | undefined,
  ) {
    super(schema, ast, format);
  }

  protected _newQuery<TSchema extends Schema, TReturn extends QueryType>(
    schema: TSchema,
    ast: AST,
    format: Format | undefined,
  ): Query<TSchema, TReturn> {
    return new ConfigQuery(schema, ast, format);
  }

  materialize(): TypedView<Smash<TReturn>> {
    throw new Error('ConfigQuery cannot be materialized');
  }

  preload(): {
    cleanup: () => void;
  } {
    throw new Error('ConfigQuery cannot be preloaded');
  }
}
