import type {DeepReadonly} from '../../../shared/src/json.ts';
import * as v from '../../../shared/src/valita.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import * as PostgresReplicaIdentity from './postgres-replica-identity-enum.ts';
import * as PostgresTypeClass from './postgres-type-class-enum.ts';

export const pgTypeClassSchema = v.literalUnion(
  PostgresTypeClass.Base,
  PostgresTypeClass.Composite,
  PostgresTypeClass.Domain,
  PostgresTypeClass.Enum,
  PostgresTypeClass.Pseudo,
  PostgresTypeClass.Range,
  PostgresTypeClass.Multirange,
);

const pgReplicaIdentitySchema = v.literalUnion(
  PostgresReplicaIdentity.Default,
  PostgresReplicaIdentity.Nothing,
  PostgresReplicaIdentity.Full,
  PostgresReplicaIdentity.Index,
);

export const columnSpec = v.object({
  pos: v.number(),
  dataType: v.string(),
  pgTypeClass: pgTypeClassSchema.optional(),

  // If the column is an array, this will be the type of the
  // elements in the array. If the column is not an array,
  // this will be null.
  elemPgTypeClass: pgTypeClassSchema.nullable().optional(),

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
  // Always present for new instances (e.g. from DDL triggers), but
  // may from `initialSchema` object stored in the `replicas` table.
  schemaOID: v.number().optional(),
  columns: v.record(publishedColumnSpec),
  replicaIdentity: pgReplicaIdentitySchema.optional(),
  publications: v.record(v.object({rowFilter: v.string().nullable()})),
});

export type MutableLiteTableSpec = v.Infer<typeof liteTableSpec>;

export type LiteTableSpec = Readonly<MutableLiteTableSpec>;

export type LiteTableSpecWithKeys = Omit<LiteTableSpec, 'primaryKey'> & {
  /**
   * All keys associated with a unique index.  Includes indexes with
   * nullable columns.
   */
  uniqueKeys: PrimaryKey[];

  /**
   * The key selected to act as the "primary key". Primary keys
   * are not explicitly set on the replica, but an appropriate
   * unique index is required.
   */
  primaryKey: PrimaryKey; // note: required

  /**
   * All keys associated with a unique index over non-null
   * columns, i.e. suitable as a primary key.
   */
  allPotentialPrimaryKeys: PrimaryKey[];
};

export type LiteAndZqlSpec = {
  tableSpec: LiteTableSpecWithKeys;
  zqlSpec: Record<string, SchemaValue>;
};

export type TableSpec = Readonly<v.Infer<typeof tableSpec>>;

export type PublishedTableSpec = Readonly<v.Infer<typeof publishedTableSpec>>;

export const directionSchema = v.literalUnion('ASC', 'DESC');

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
  isPrimaryKey: v.boolean().optional(),
  isImmediate: v.boolean().optional(),
});

export type PublishedIndexSpec = DeepReadonly<
  v.Infer<typeof publishedIndexSpec>
>;
