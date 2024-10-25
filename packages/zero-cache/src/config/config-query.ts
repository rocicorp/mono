import type {AST} from '../../../zero-protocol/src/ast.js';
import type {Format} from '../../../zql/src/zql/ivm/view.js';
import type {NormalizedTableSchema} from '../../../zql/src/zql/query/normalize-table-schema.js';
import {AbstractQuery} from '../../../zql/src/zql/query/query-impl.js';
import type {
  DefaultQueryResultRow,
  Query,
  QueryType,
  Smash,
} from '../../../zql/src/zql/query/query.js';
import type {TableSchema} from '../../../zql/src/zql/query/schema.js';
import type {TypedView} from '../../../zql/src/zql/query/typed-view.js';

export class ConfigQuery<
  TTableSchema extends TableSchema,
  TReturn extends QueryType = DefaultQueryResultRow<TTableSchema>,
> extends AbstractQuery<TTableSchema, TReturn> {
  constructor(
    schema: NormalizedTableSchema,
    ast?: AST | undefined,
    format?: Format | undefined,
  ) {
    super(schema, ast, format);
  }

  get ast() {
    return this._completeAst();
  }

  protected _newQuery<TSchema extends TableSchema, TReturn extends QueryType>(
    schema: NormalizedTableSchema,
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
    complete: Promise<void>;
  } {
    throw new Error('ConfigQuery cannot be preloaded');
  }
}
