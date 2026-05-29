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

/**
 * A bidirectional codec for a column.
 *
 * `Encoded` is the value as stored, synced, and processed by Zero: it must be
 * one of the JSON-native {@linkcode ValueType}s so that it can be compared,
 * sorted, persisted to IndexedDB, and sent over the wire.
 *
 * `Decoded` is the value the application sees and provides. It can be any
 * JavaScript value (e.g. `Date`, `Temporal.Instant`, a branded id, ...).
 *
 * `decode` runs when reading query results; `encode` runs when writing
 * (insert/update) and when comparing values in `where` clauses. The codec must
 * be a stable bijection so that the encoded value preserves identity and
 * ordering.
 *
 * `null`/`undefined` are never passed to `decode`/`encode`; they pass through
 * unchanged.
 */
export type Codec<Encoded, Decoded> = {
  decode: (value: Encoded) => Decoded;
  encode: (value: Decoded) => Encoded;
};

/**
 * A {@linkcode SchemaValue} that carries a runtime {@linkcode Codec}. The
 * user-facing TypeScript type (via {@linkcode SchemaValueToTSType}) is the
 * `Decoded` type, exactly as for {@linkcode SchemaValueWithCustomType}, while
 * the `Encoded` type is what is stored/synced.
 */
export type SchemaValueWithCodec<Decoded = unknown, Encoded = unknown> = {
  type: ValueType;
  serverName?: string | undefined;
  optional?: boolean | undefined;
  customType: Decoded;
  codec: Codec<Encoded, Decoded>;
};

/**
 * Returns the runtime codec attached to a column, or `undefined` if the column
 * has no codec.
 */
export function getCodec(
  value: SchemaValue,
): Codec<unknown, unknown> | undefined {
  return (value as Partial<SchemaValueWithCodec>).codec;
}

export type TypeNameToTypeMap = {
  string: string;
  number: number;
  boolean: boolean;
  null: null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  json: any;
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
