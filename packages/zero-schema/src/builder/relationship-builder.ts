/* eslint-disable @typescript-eslint/no-explicit-any */
import type {Relationship, TableSchema} from '../table-schema.js';
import type {TableBuilderWithColumns} from './table-builder.js';

type ConnectArg<TSourceField, TDestField, TDest extends TableSchema> = {
  sourceField: TSourceField;
  destField: TDestField;
  destSchema: TableBuilderWithColumns<TDest>;
};

type ConnectResult<TSourceField, TDestField, TDest extends TableSchema> = {
  sourceField: TSourceField;
  destField: TDestField;
  destSchema: TDest['name'];
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
  cb: (
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
      [K in keyof TDests]: ConnectResult<
        TSourceFields[K],
        TDestFields[K],
        TDests[K]
      >;
    },
  ) => TRelationships,
): {name: TSource['name']; relationships: TRelationships} {
  const relationships = cb(many as any);

  return {
    name: table.schema.name,
    relationships,
  };
}

function many(
  ...args: readonly ConnectArg<any, any, TableSchema>[]
): ConnectResult<any, any, any>[] {
  return args.map(arg => ({
    sourceField: arg.sourceField,
    destField: arg.destField,
    destSchema: arg.destSchema.schema.name,
  }));
}

// class RelationshipBuilder<TShape extends Relationship> {
//   readonly #shape: TShape;
//   constructor(shape: TShape) {
//     this.#shape = shape;
//   }

//   many() {}

//   one() {}

//   build() {
//     return this.#shape;
//   }
// }
