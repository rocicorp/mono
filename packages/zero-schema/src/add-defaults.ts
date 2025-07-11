import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {Location} from '../../zql/src/mutate/custom.ts';
import type {TableSchema} from './table-schema.ts';

/**
 * Adds defaults to optional fields.
 *
 * On the client, we override any undefined values with either the client default
 * or null.
 *
 * On the server, we override any undefined values with either the server default
 * or omit the field.
 */
export function addDefaultToOptionalFields<
  TSchema extends TableSchema,
  TInput extends Partial<
    Record<keyof TSchema['columns'], ReadonlyJSONValue | undefined>
  >,
  TReturn extends Partial<Record<keyof TSchema['columns'], ReadonlyJSONValue>>,
>({
  schema,
  value,
  operation,
  location,
}: {
  schema: TSchema;
  value: TInput;
  operation: 'insert' | 'update';
  location: Location;
}): TReturn {
  const rv: Record<string, unknown> = {};

  for (const [name, columnSchema] of Object.entries(schema.columns)) {
    let newValue = value[name];

    // only apply overrides if the column value was not explicitly defined
    if (value[name] === undefined) {
      const defaultFn =
        location === 'client'
          ? operation === 'insert'
            ? columnSchema?.defaultConfig?.insert?.client
            : operation === 'update'
              ? columnSchema?.defaultConfig?.update?.client
              : undefined
          : location === 'server'
            ? operation === 'insert'
              ? columnSchema?.defaultConfig?.insert?.server
              : operation === 'update'
                ? columnSchema?.defaultConfig?.update?.server
                : undefined
            : undefined;

      if (typeof defaultFn === 'function') {
        newValue = defaultFn();
      }
    }

    if (newValue === undefined) {
      if (location === 'server') {
        continue;
      }
      newValue = null;
    }

    rv[name] = newValue;
  }

  return rv as TReturn;
}
