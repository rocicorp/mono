import type {Schema} from '../../../zero-types/src/schema.ts';
import type {QueryDefinitions} from './query-definitions.ts';

export function defineQueries<S extends Schema, Context, MD>(
  mutators: MD,
): QueryDefinitions<S, Context> {
  void mutators;
  // oxlint-disable-next-line no-explicit-any
  return {} as any;
}
