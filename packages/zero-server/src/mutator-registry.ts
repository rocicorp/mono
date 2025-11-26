import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {
  isMutatorDefinition,
  type MutatorDefinition,
} from '../../zero-types/src/mutator.ts';
import type {MutatorDefinitions} from '../../zero-types/src/mutator-registry.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {Transaction} from '../../zql/src/mutate/custom.ts';
import {validateInput} from '../../zql/src/query/validate-input.ts';

// oxlint-disable no-explicit-any
type AnyMutatorDefinition<S extends Schema> = MutatorDefinition<
  S,
  any,
  any,
  any,
  any
>;
// oxlint-enable no-explicit-any

export class MutatorRegistry<
  S extends Schema,
  MD extends MutatorDefinitions<S, Context>,
  Context,
> {
  readonly #map: Map<string, AnyMutatorDefinition<S>>;

  constructor(mutators: MD) {
    this.#map = buildMap<S, MD, Context>(mutators);
  }

  mustGet<TDBTransaction extends Transaction<S>, Context>(
    name: string,
    context?: Context,
  ): (tx: TDBTransaction, args: ReadonlyJSONValue) => Promise<void> {
    const f = this.#map.get(name);
    if (!f) {
      throw new Error(`Cannot find mutator '${name}'`);
    }

    // oxlint-disable-next-line require-await
    return async (tx: TDBTransaction, args: ReadonlyJSONValue | undefined) => {
      const v = validateInput(name, args, f.validator, 'mutator');
      return f({args: v, ctx: context, tx});
    };
  }
}

function buildMap<
  S extends Schema,
  MD extends MutatorDefinitions<S, Context>,
  Context,
>(mutators: MD): Map<string, AnyMutatorDefinition<S>> {
  const map = new Map<string, AnyMutatorDefinition<S>>();

  function recurse(obj: unknown, prefix: string): void {
    if (typeof obj === 'object' && obj !== null) {
      for (const [key, value] of Object.entries(obj)) {
        const name = prefix ? `${prefix}.${key}` : key;
        if (isMutatorDefinition(value)) {
          map.set(name, value as unknown as AnyMutatorDefinition<S>);
        } else {
          recurse(value, name);
        }
      }
    }
  }

  recurse(mutators, '');

  return map;
}
