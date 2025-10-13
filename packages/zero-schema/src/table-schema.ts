import type {
  ColumnTypeName,
  SchemaValue,
  SchemaValueToTSType,
  SchemaValueWithCustomType,
  TypeNameToTypeMap,
  ValueType,
} from '../../zero-types/src/schema-value.ts';
import type {
  Cardinality,
  LastInTuple,
  Relationship,
  RelationshipsSchema,
  TableSchema,
} from '../../zero-types/src/schema.ts';

export type {
  Cardinality,
  ColumnTypeName,
  LastInTuple,
  Relationship,
  RelationshipsSchema,
  SchemaValue,
  SchemaValueToTSType,
  SchemaValueWithCustomType,
  TableSchema,
  TypeNameToTypeMap,
  ValueType,
};

export type AtLeastOne<T> = readonly [T, ...T[]];

export function atLeastOne<T>(arr: readonly T[]): AtLeastOne<T> {
  if (arr.length === 0) {
    throw new Error('Expected at least one element');
  }
  return arr as AtLeastOne<T>;
}

export type Opaque<BaseType, BrandType = unknown> = BaseType & {
  readonly [base]: BaseType;
  readonly [brand]: BrandType;
};

declare const base: unique symbol;
declare const brand: unique symbol;

export type IsOpaque<T> = T extends {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
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
