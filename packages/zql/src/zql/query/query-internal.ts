import type {Format, ViewFactory} from '../ivm/view.js';
import type {DefaultQueryResultRow, Query, QueryType, Smash} from './query.js';
import type {TableSchema} from './schema.js';
import type {TypedView} from './typed-view.js';

export interface AdvancedQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType = DefaultQueryResultRow<TSchema>,
> extends Query<TSchema, TReturn> {
  materialize(): TypedView<Smash<TReturn>>;
  materialize<T>(factory: ViewFactory<TSchema, TReturn, T>): T;
  get format(): Format;
  hash(): string;
}
