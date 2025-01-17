import type {AST} from '../../../zero-protocol/src/ast.js';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.js';
import type {Format} from '../ivm/view.js';
import {ExpressionBuilder} from './expression.js';
import {AbstractQuery} from './query-impl.js';
import type {HumanReadable, PullRow, Query} from './query.js';
import type {TypedView} from './typed-view.js';

export function authQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
>(schema: TSchema, tableName: TTable): Query<TSchema, TTable> {
  return new AuthQuery<TSchema, TTable>(schema, tableName);
}

export class AuthQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> extends AbstractQuery<TSchema, TTable, TReturn> {
  constructor(
    schema: TSchema,
    tableName: TTable,
    ast: AST = {table: tableName},
    format?: Format | undefined,
  ) {
    super(schema, tableName, ast, format);
  }

  expressionBuilder() {
    return new ExpressionBuilder(this._exists);
  }

  protected readonly _system = 'permissions';

  protected _newQuery<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format | undefined,
  ): Query<TSchema, TTable, TReturn> {
    return new AuthQuery(schema, tableName, ast, format);
  }

  get ast() {
    return this._completeAst();
  }

  materialize(): TypedView<HumanReadable<TReturn>> {
    throw new Error('AuthQuery cannot be materialized');
  }

  run(): HumanReadable<TReturn> {
    throw new Error('AuthQuery cannot be run');
  }

  preload(): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    throw new Error('AuthQuery cannot be preloaded');
  }
}
