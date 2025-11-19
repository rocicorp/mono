import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import {
  isQueryDefinition,
  type QueryDefinition,
  wrapCustomQuery,
} from '../../zql/src/query/define-query.ts';
import type {QueryDefinitions} from '../../zql/src/query/query-definitions.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';

// oxlint-disable no-explicit-any
type AnyQueryDefinition<S extends Schema> = QueryDefinition<
  S,
  any,
  any,
  any,
  any,
  any
>;
// oxlint-enable no-explicit-any

export class QueryRegistry<
  S extends Schema,
  // oxlint-disable-next-line no-explicit-any
  QD extends QueryDefinitions<S, any>,
> {
  readonly #map: Map<string, AnyQueryDefinition<S>>;

  constructor(queries: QD) {
    this.#map = buildMap(queries);
  }

  mustGet<Context>(
    name: string,
    context?: Context,
  ): (args?: ReadonlyJSONValue) => AnyQuery {
    const f = this.#map.get(name);
    if (!f) {
      throw new Error(`Cannot find query '${name}'`);
    }

    return wrapCustomQuery(name, f, {context});
  }
}

function buildMap<
  S extends Schema,
  QD extends QueryDefinitions<S, Context>,
  Context,
>(queries: QD): Map<string, AnyQueryDefinition<S>> {
  const map = new Map<string, AnyQueryDefinition<S>>();

  function recurse(obj: unknown, prefix: string): void {
    if (typeof obj === 'object' && obj !== null) {
      for (const [key, value] of Object.entries(obj)) {
        const name = prefix ? `${prefix}.${key}` : key;
        if (isQueryDefinition(value)) {
          map.set(name, value);
        } else {
          recurse(value, name);
        }
      }
    }
  }

  recurse(queries, '');

  return map;
}
