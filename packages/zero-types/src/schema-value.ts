import type {JSONValue} from '../../shared/src/json.ts';

/**
 * The allowed value types in Zero schema definitions.
 */
export type ValueType = 'string' | 'number' | 'boolean' | 'null' | 'json';

/**
 * Schema value definition with optional custom type support.
 */
export type SchemaValue<T = unknown> =
  | {
      type: ValueType;
      serverName?: string | undefined;
      optional?: boolean | undefined;
    }
  | SchemaValueWithCustomType<T>;

export type SchemaValueWithCustomType<T> = {
  type: ValueType;
  serverName?: string | undefined;
  optional?: boolean | undefined;
  customType: T;
};

export type TypeNameToTypeMap = {
  string: string;
  number: number;
  boolean: boolean;
  null: null;
  json: JSONValue;
};

export type ColumnTypeName<T extends SchemaValue | ValueType> =
  T extends SchemaValue ? T['type'] : T;

/**
 * Given a schema value, return the TypeScript type.
 *
 * This allows us to create the correct return type for a
 * query that has a selection.
 */
export type SchemaValueToTSType<T extends SchemaValue | ValueType> =
  T extends ValueType
    ? TypeNameToTypeMap[T]
    : T extends {
          optional: true;
        }
      ?
          | (T extends SchemaValueWithCustomType<infer V>
              ? V
              : TypeNameToTypeMap[ColumnTypeName<T>])
          | null
      : T extends SchemaValueWithCustomType<infer V>
        ? V
        : TypeNameToTypeMap[ColumnTypeName<T>];
