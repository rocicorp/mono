import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {Format, ViewFactory} from '../ivm/view.ts';
import type {HumanReadable, PullRow, Query} from './query.ts';
import type {TTL} from './ttl.ts';
import type {TypedView} from './typed-view.ts';

export interface AdvancedQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> extends Query<TSchema, TTable, TReturn> {
  materialize(ttl?: TTL): TypedView<HumanReadable<TReturn>>;
  materialize<T>(
    factory: ViewFactory<TSchema, TTable, TReturn, T>,
    ttl?: TTL,
  ): T;
  get format(): Format;
  hash(): string;
  updateTTL(ttl: TTL): void;
}
