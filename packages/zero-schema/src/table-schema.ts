import type {ValueType} from '../../zero-protocol/src/client-schema.ts';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';

export type {ValueType} from '../../zero-protocol/src/client-schema.ts';

export type DefaultValueFunction<T> = () => T;

export type BaseSchemaValue = {
  type: ValueType;
  serverName?: string | undefined;
  nullable?: boolean | undefined;
};

export type SchemaValueWithCustomType<T = unknown> = BaseSchemaValue & {
  customType: T;
};

export type DefaultConfig<T> = {
  insert?: {
    client: DefaultValueFunction<T>;
    server: DefaultValueFunction<T> | 'db';
  };
  update?: {
    client: DefaultValueFunction<T>;
    server: DefaultValueFunction<T> | 'db';
  };
};

/**
 * `related` calls need to know what the available relationships are.
 * The `schema` type encodes this information.
 */
export type SchemaValue<T = unknown> = (
  | BaseSchemaValue
  | EnumSchemaValue<T>
  | SchemaValueWithCustomType<T>
) &
  (Record<string, never> | SchemaValueWithDefaults<T>);

export type SchemaValueWithDefaults<T> = {
  defaultConfig?: DefaultConfig<T> | undefined;
};

export type EnumSchemaValue<T> = Omit<SchemaValueWithCustomType<T>, 'type'> & {
  kind: 'enum';
  type: 'string';
};

export type TableSchema = {
  readonly name: string;
  readonly serverName?: string | undefined;
  readonly columns: Record<string, SchemaValue>;
  readonly primaryKey: PrimaryKey;
};

export type RelationshipsSchema = {
  readonly [name: string]: Relationship;
};

export type TypeNameToTypeMap = {
  string: string;
  number: number;
  boolean: boolean;
  null: null;

  // In schema-v2, the user will be able to specify the TS type that
  // the JSON should match and `any`` will no
  // longer be used here.
  // ReadOnlyJSONValue is not used as it causes
  // infinite depth errors to pop up for users of our APIs.

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  json: any;
};

export type ColumnTypeName<T extends SchemaValue | ValueType> =
  T extends SchemaValue ? T['type'] : T;

type SchemaValueWithTypeOverride<T extends SchemaValue | ValueType> =
  T extends SchemaValueWithCustomType<infer V>
    ? V
    : TypeNameToTypeMap[ColumnTypeName<T>];

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
          nullable: true;
        }
      ? SchemaValueWithTypeOverride<T> | null
      : SchemaValueWithTypeOverride<T>;

type Connection = {
  readonly sourceField: readonly string[];
  readonly destField: readonly string[];
  readonly destSchema: string;
  readonly cardinality: Cardinality;
};

export type Cardinality = 'one' | 'many';

export type Relationship =
  | readonly [Connection]
  | readonly [Connection, Connection];
// | readonly [Connection, Connection, Connection];

export type LastInTuple<T extends Relationship> = T extends readonly [infer L]
  ? L
  : T extends readonly [unknown, infer L]
    ? L
    : T extends readonly [unknown, unknown, infer L]
      ? L
      : never;

export type AtLeastOne<T> = readonly [T, ...T[]];

export function atLeastOne<T>(arr: readonly T[]): AtLeastOne<T> {
  if (arr.length === 0) {
    throw new Error('Expected at least one element');
  }
  return arr as AtLeastOne<T>;
}

export function isOneHop(r: Relationship): r is readonly [Connection] {
  return r.length === 1;
}

export function isTwoHop(
  r: Relationship,
): r is readonly [Connection, Connection] {
  return r.length === 2;
}

export type Opaque<BaseType, BrandType = unknown> = BaseType & {
  readonly [base]: BaseType;
  readonly [brand]: BrandType;
};

declare const base: unique symbol;
declare const brand: unique symbol;

export type IsOpaque<T> = T extends {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly [brand]: any;
}
  ? true
  : false;

export type ExpandRecursiveSkipOpaque<T> =
  IsOpaque<T> extends true
    ? T
    : T extends object
      ? T extends infer O
        ? {[K in keyof O]: ExpandRecursiveSkipOpaque<O[K]>}
        : never
      : T;
