import type {AST} from '../../../zero-protocol/src/ast.js';
import type {NormalizedTableSchema} from '../../../zero-schema/src/normalize-table-schema.js';
import type {TableSchema} from '../../../zero-schema/src/table-schema.js';
import type {Format} from '../../../zql/src/ivm/view.js';
import {AbstractQuery} from '../../../zql/src/query/query-impl.js';
import type {
  DefaultQueryResultRow,
  Query,
  QueryType,
  Smash,
} from '../../../zql/src/query/query.js';
import type {TypedView} from '../../../zql/src/query/typed-view.js';

export class ConfigQuery<
  TTableSchema extends TableSchema,
  TReturn extends QueryType = DefaultQueryResultRow<TTableSchema>,
> extends AbstractQuery<TTableSchema, TReturn> {
  constructor(
    schema: NormalizedTableSchema,
    ast: AST = {table: schema.tableName},
    format?: Format | undefined,
  ) {
    super(schema, ast, format);
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

  run(): Smash<TReturn> {
    throw new Error('ConfigQuery cannot be run');
  }

  preload(): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    throw new Error('ConfigQuery cannot be preloaded');
  }
}
