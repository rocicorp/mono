import type {DeepReadonly} from '../../../shared/src/json.js';
import * as v from '../../../shared/src/valita.js';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';
import * as PostgresReplicaIdentity from './postgres-replica-identity-enum.js';
import * as PostgresTypeClass from './postgres-type-class-enum.js';

export const pgTypeClassSchema = v.union(
  v.literal(PostgresTypeClass.Base),
  v.literal(PostgresTypeClass.Composite),
  v.literal(PostgresTypeClass.Domain),
  v.literal(PostgresTypeClass.Enum),
  v.literal(PostgresTypeClass.Pseudo),
  v.literal(PostgresTypeClass.Range),
  v.literal(PostgresTypeClass.Multirange),
);

export const pgReplicaIdentitySchema = v.union(
  v.literal(PostgresReplicaIdentity.Default),
  v.literal(PostgresReplicaIdentity.Nothing),
  v.literal(PostgresReplicaIdentity.Full),
  v.literal(PostgresReplicaIdentity.Index),
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
  primaryKey: v.array(v.string()).optional(),
});

export const tableSpec = liteTableSpec.extend({
  schema: v.string(),
});

export const publishedTableSpec = tableSpec.extend({
  oid: v.number(),
  columns: v.record(publishedColumnSpec),
  replicaIdentity: pgReplicaIdentitySchema.optional(),
  publications: v.record(v.object({rowFilter: v.string().nullable()})),
});

export type LiteTableSpec = Readonly<v.Infer<typeof liteTableSpec>>;

export type LiteTableSpecWithKeys = Omit<LiteTableSpec, 'primaryKey'> & {
  /**
   * The key selected to act as the "primary key". Primary keys
   * are not explicitly set on the replica, but an appropriate
   * unique index is required.
   */
  primaryKey: PrimaryKey; // note: required

  /**
   * The union of all columns that are part of any unique index.
   * This is guaranteed to include any combination of columns that
   * can serve as a key.
   */
  unionKey: PrimaryKey;
};

export type LiteAndZqlSpec = {
  tableSpec: LiteTableSpecWithKeys;
  zqlSpec: Record<string, SchemaValue>;
};

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

export const publishedIndexSpec = indexSpec.extend({
  isReplicaIdentity: v.boolean().optional(),
  isImmediate: v.boolean().optional(),
});

export type PublishedIndexSpec = DeepReadonly<
  v.Infer<typeof publishedIndexSpec>
>;
