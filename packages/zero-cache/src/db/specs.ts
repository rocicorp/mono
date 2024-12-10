import type {DeepReadonly} from '../../../shared/src/json.js';
import * as v from '../../../shared/src/valita.js';

// Values of the `typtype` column in https://www.postgresql.org/docs/17/catalog-pg-type.html#CATALOG-PG-TYPE
export enum PostgresTypeClass {
  Base = 'b',
  Composite = 'c',
  Domain = 'd',
  Enum = 'e',
  Pseudo = 'p',
  Range = 'r',
  Multirange = 'm',
}

export const pgTypeClassSchema = v.union(
  v.literal(PostgresTypeClass.Base),
  v.literal(PostgresTypeClass.Composite),
  v.literal(PostgresTypeClass.Domain),
  v.literal(PostgresTypeClass.Enum),
  v.literal(PostgresTypeClass.Pseudo),
  v.literal(PostgresTypeClass.Range),
  v.literal(PostgresTypeClass.Multirange),
);

export const columnSpec = v.object({
  pos: v.number(),
  dataType: v.string(),
  pgTypeClass: pgTypeClassSchema.optional(),
  characterMaximumLength: v.number().nullable().optional(),
  notNull: v.boolean().nullable().optional(),
  dflt: v.string().nullable().optional(),
});

export type ColumnSpec = Readonly<v.Infer<typeof columnSpec>>;

const publishedColumnSpec = columnSpec.extend({
  typeOID: v.number(),
});

export const liteTableSpec = v.object({
  name: v.string(),
  columns: v.record(columnSpec),
  primaryKey: v.array(v.string()),
});

export const tableSpec = liteTableSpec.extend({
  schema: v.string(),
});

export const publishedTableSpec = tableSpec.extend({
  oid: v.number(),
  columns: v.record(publishedColumnSpec),
  publications: v.record(v.object({rowFilter: v.string().nullable()})),
});

export type LiteTableSpec = Readonly<v.Infer<typeof liteTableSpec>>;

export type TableSpec = Readonly<v.Infer<typeof tableSpec>>;

export type PublishedTableSpec = Readonly<v.Infer<typeof publishedTableSpec>>;

export const directionSchema = v.union(v.literal('ASC'), v.literal('DESC'));

export const liteIndexSpec = v.object({
  name: v.string(),
  tableName: v.string(),
  unique: v.boolean(),
  columns: v.record(directionSchema),
});

export type MutableLiteIndexSpec = v.Infer<typeof liteIndexSpec>;

export type LiteIndexSpec = Readonly<MutableLiteIndexSpec>;

export const indexSpec = liteIndexSpec.extend({
  schema: v.string(),
});

export type IndexSpec = DeepReadonly<v.Infer<typeof indexSpec>>;
