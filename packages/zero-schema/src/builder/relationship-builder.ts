/* oxlint-disable @typescript-eslint/no-explicit-any */
import type {Relationship, TableSchema} from '../table-schema.ts';
import type {TableBuilderWithColumns} from './table-builder.ts';

type ConnectArg<TSourceField, TDestField, TDest extends TableSchema> = {
  readonly sourceField: TSourceField;
  readonly destField: TDestField;
  readonly destSchema: TableBuilderWithColumns<TDest>;
};

type ManyConnection<TSourceField, TDestField, TDest extends TableSchema> = {
  readonly sourceField: TSourceField;
  readonly destField: TDestField;
  readonly destSchema: TDest['name'];
  readonly cardinality: 'many';
};

type OneConnection<TSourceField, TDestField, TDest extends TableSchema> = {
  readonly sourceField: TSourceField;
  readonly destField: TDestField;
  readonly destSchema: TDest['name'];
  readonly cardinality: 'one';
};

type Prev = [-1, 0, 1, 2, 3, 4, 5, 6];

export type PreviousSchema<
  TSource extends TableSchema,
  K extends number,
  TDests extends TableSchema[],
> = K extends 0 ? TSource : TDests[Prev[K]];

export type Relationships = {
  name: string; // table name
  relationships: Record<string, Relationship>; // relationships for that table
};

// Overloaded types for better inference
type ManyConnector<TSource extends TableSchema> = {
  // Single direct relationship
  <TDest extends TableSchema>(
    arg: ConnectArg<
      readonly (keyof TSource['columns'] & string)[],
      readonly (keyof TDest['columns'] & string)[],
      TDest
    >,
  ): [
    ManyConnection<
      readonly (keyof TSource['columns'] & string)[],
      readonly (keyof TDest['columns'] & string)[],
      TDest
    >,
  ];

  // Junction relationship (two hops)
  <TJunction extends TableSchema, TDest extends TableSchema>(
    firstHop: ConnectArg<
      readonly (keyof TSource['columns'] & string)[],
      readonly (keyof TJunction['columns'] & string)[],
      TJunction
    >,
    secondHop: ConnectArg<
      readonly (keyof TJunction['columns'] & string)[],
      readonly (keyof TDest['columns'] & string)[],
      TDest
    >,
  ): [
    ManyConnection<
      readonly (keyof TSource['columns'] & string)[],
      readonly (keyof TJunction['columns'] & string)[],
      TJunction
    >,
    ManyConnection<
      readonly (keyof TJunction['columns'] & string)[],
      readonly (keyof TDest['columns'] & string)[],
      TDest
    >,
  ];
};

type OneConnector<TSource extends TableSchema> = {
  // Single direct relationship
  <TDest extends TableSchema>(
    arg: ConnectArg<
      readonly (keyof TSource['columns'] & string)[],
      readonly (keyof TDest['columns'] & string)[],
      TDest
    >,
  ): [
    OneConnection<
      readonly (keyof TSource['columns'] & string)[],
      readonly (keyof TDest['columns'] & string)[],
      TDest
    >,
  ];

  // Two-hop relationship (e.g., invoice_line -> invoice -> customer)
  <TIntermediate extends TableSchema, TDest extends TableSchema>(
    firstHop: ConnectArg<
      readonly (keyof TSource['columns'] & string)[],
      readonly (keyof TIntermediate['columns'] & string)[],
      TIntermediate
    >,
    secondHop: ConnectArg<
      readonly (keyof TIntermediate['columns'] & string)[],
      readonly (keyof TDest['columns'] & string)[],
      TDest
    >,
  ): [
    OneConnection<
      readonly (keyof TSource['columns'] & string)[],
      readonly (keyof TIntermediate['columns'] & string)[],
      TIntermediate
    >,
    OneConnection<
      readonly (keyof TIntermediate['columns'] & string)[],
      readonly (keyof TDest['columns'] & string)[],
      TDest
    >,
  ];
};

export function relationships<
  TSource extends TableSchema,
  TRelationships extends Record<string, Relationship>,
>(
  table: TableBuilderWithColumns<TSource>,
  cb: (connects: {
    many: ManyConnector<TSource>;
    one: OneConnector<TSource>;
  }) => TRelationships,
): {name: TSource['name']; relationships: TRelationships} {
  const relationships = cb({many, one} as any);

  return {
    name: table.schema.name,
    relationships,
  };
}

function many(
  ...args: readonly ConnectArg<any, any, TableSchema>[]
): ManyConnection<any, any, any>[] {
  return args.map(arg => ({
    sourceField: arg.sourceField,
    destField: arg.destField,
    destSchema: arg.destSchema.schema.name,
    cardinality: 'many',
  }));
}

function one(
  ...args: readonly ConnectArg<any, any, TableSchema>[]
): OneConnection<any, any, any>[] {
  return args.map(arg => ({
    sourceField: arg.sourceField,
    destField: arg.destField,
    destSchema: arg.destSchema.schema.name,
    cardinality: 'one',
  }));
}
