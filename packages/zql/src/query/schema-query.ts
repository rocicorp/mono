import type {Schema} from '../../../zero-types/src/schema.ts';
import type {Query} from './query.ts';

export type SchemaQuery<S extends Schema> = {
  // Note: Using Query<S, K> without explicit third type parameter is
  // intentional. When the default PullRow<K, S> is left implicit, TypeScript
  // defers its evaluation, which prevents "type exceeds maximum length" errors
  // in deeply chained .related() calls (e.g., 90+ calls in stress tests).
  readonly [K in keyof S['tables'] & string]: Query<S, K>;
};
