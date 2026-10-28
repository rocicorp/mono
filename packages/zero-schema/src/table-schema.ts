import type {
  SchemaValue,
  SchemaValueWithCustomType,
  ValueType,
} from '../../zero-types/src/schema-value.ts';
import type {Relationship, TableSchema} from '../../zero-types/src/schema.ts';

export type {
  Relationship,
  SchemaValue,
  SchemaValueWithCustomType,
  TableSchema,
  ValueType,
};

export type Opaque<BaseType, BrandType = unknown> = BaseType & {
  readonly [base]: BaseType;
  readonly [brand]: BrandType;
};

declare const base: unique symbol;
declare const brand: unique symbol;
