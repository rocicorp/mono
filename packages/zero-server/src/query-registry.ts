import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import {
  isDefinedQueryFunction,
  type DefinedQueryFunction,
} from '../../zql/src/query/define-query.ts';
import type {QueryDefinitions} from '../../zql/src/query/query-definitions.ts';

type AnyDefinedQueryFunction<S extends Schema> = DefinedQueryFunction<
  S,
  keyof S['tables'] & string,
  // oxlint-disable-next-line no-explicit-any
  any,
  // oxlint-disable-next-line no-explicit-any
  any,
  ReadonlyJSONValue | undefined,
  ReadonlyJSONValue | undefined
>;

export class QueryRegistry<
  S extends Schema,
  // oxlint-disable-next-line no-explicit-any
  QD extends QueryDefinitions<S, any>,
> {
  readonly #map: Map<string, AnyDefinedQueryFunction<S>>;

  constructor(queries: QD) {
    this.#map = buildMap(queries);
  }

  mustGet(name: string): AnyDefinedQueryFunction<S> {
    const current = this.#map.get(name);
    if (!current) {
      throw new Error(`Cannot find query '${name}'`);
    }
    return current;
  }
}

function buildMap<
  S extends Schema,
  QD extends QueryDefinitions<S, Context>,
  Context,
>(queries: QD): Map<string, AnyDefinedQueryFunction<S>> {
  const map = new Map<string, AnyDefinedQueryFunction<S>>();
  debugger;

  function recurse(obj: unknown, prefix: string): void {
    if (typeof obj === 'object' && obj !== null) {
      for (const [key, value] of Object.entries(obj)) {
        const name = prefix ? `${prefix}.${key}` : key;
        if (isDefinedQueryFunction(value)) {
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
