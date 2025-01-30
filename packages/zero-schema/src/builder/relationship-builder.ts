/* eslint-disable @typescript-eslint/no-explicit-any */
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

export function relationships<
  TSource extends TableSchema,
  TRelationships extends Record<string, Relationship>,
>(
  table: TableBuilderWithColumns<TSource>,
  cb: (connects: {
    many: <
      TDests extends TableSchema[],
      TSourceFields extends {
        [K in keyof TDests]: (keyof PreviousSchema<
          TSource,
          K & number,
          TDests
        >['columns'] &
          string)[];
      },
      TDestFields extends {
        [K in keyof TDests]: (keyof TDests[K]['columns'] & string)[];
      },
    >(
      ...args: {
        [K in keyof TDests]: ConnectArg<
          TSourceFields[K],
          TDestFields[K],
          TDests[K]
        >;
      }
    ) => {
      [K in keyof TDests]: ManyConnection<
        TSourceFields[K],
        TDestFields[K],
        TDests[K]
      >;
    };
    one: <
      TDests extends TableSchema[],
      TSourceFields extends {
        [K in keyof TDests]: (keyof PreviousSchema<
          TSource,
          K & number,
          TDests
        >['columns'] &
          string)[];
      },
      TDestFields extends {
        [K in keyof TDests]: (keyof TDests[K]['columns'] & string)[];
      },
    >(
      ...args: {
        [K in keyof TDests]: ConnectArg<
          TSourceFields[K],
          TDestFields[K],
          TDests[K]
        >;
      }
    ) => {
      [K in keyof TDests]: OneConnection<
        TSourceFields[K],
        TDestFields[K],
        TDests[K]
      >;
    };
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
