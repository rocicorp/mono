import type {Schema} from '../../../zero-schema/src/builder/schema-builder.js';
import type {Format, ViewFactory} from '../ivm/view.js';
import type {HumanReadable, PullRow, Query} from './query.js';
import type {TypedView} from './typed-view.js';

export interface AdvancedQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> extends Query<TSchema, TTable, TReturn> {
  materialize(): TypedView<HumanReadable<TReturn>>;
  materialize<T>(factory: ViewFactory<TSchema, TTable, TReturn, T>): T;
  get format(): Format;
  hash(): string;
}
