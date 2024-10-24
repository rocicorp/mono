import type {DeepReadonly} from '../../../shared/src/json.js';
import * as v from '../../../shared/src/valita.js';

export const columnSpec = v.object({
  pos: v.number(),
  dataType: v.string(),
  characterMaximumLength: v.number().nullable().optional(),
  notNull: v.boolean().nullable().optional(),
  dflt: v.string().nullable().optional(),
});

export type ColumnSpec = Readonly<v.Infer<typeof columnSpec>>;

export const liteTableSpec = v.object({
  name: v.string(),
  columns: v.record(columnSpec),
  primaryKey: v.array(v.string()),
});

export const tableSpec = liteTableSpec.extend({
  schema: v.string(),
});

export const filteredTableSpec = tableSpec.extend({
  publications: v.record(v.object({rowFilter: v.string().nullable()})),
});

export type LiteTableSpec = Readonly<v.Infer<typeof liteTableSpec>>;

export type TableSpec = Readonly<v.Infer<typeof tableSpec>>;

export type FilteredTableSpec = Readonly<v.Infer<typeof filteredTableSpec>>;

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
