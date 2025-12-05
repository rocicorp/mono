import type {SchemaValue} from './schema-value.ts';

/**
 * Primary key definition - a readonly array with at least one string element.
 * First element is the primary key field, additional elements form composite keys.
 */
export type PrimaryKey = readonly [string, ...string[]];

export type TableSchema = {
  readonly name: string;
  readonly serverName?: string | undefined;
  readonly columns: Record<string, SchemaValue>;
  readonly primaryKey: PrimaryKey;
};

export type RelationshipsSchema = {
  readonly [name: string]: Relationship;
};

export type Cardinality = 'one' | 'many';

type Connection = {
  readonly sourceField: readonly string[];
  readonly destField: readonly string[];
  readonly destSchema: string;
  readonly cardinality: Cardinality;
};

export type Relationship =
  | readonly [Connection]
  | readonly [Connection, Connection];

export type LastInTuple<T extends Relationship> = T extends readonly [infer L]
  ? L
  : T extends readonly [unknown, infer L]
    ? L
    : T extends readonly [unknown, unknown, infer L]
      ? L
      : never;

/**
 * Top-level schema definition for Zero applications.
 * Contains table definitions, relationships, and feature flags.
 */
export type Schema = {
  readonly tables: {readonly [table: string]: TableSchema};
  readonly relationships: {readonly [table: string]: RelationshipsSchema};
  /**
   * Enables legacy query support.
   * When this is true, old-style queries that do not require server side implementations will be enabled.
   * What we currently call "custom queries" will become "queries" and
   * the only option for reading data.
   * The default is false.
   */
  readonly enableLegacyQueries?: boolean | undefined;
  /**
   * Enables legacy mutator support.
   * When this is true, old-style mutations that do not require server side implementations will be enabled.
   * What we currently call "custom mutations" will become "mutations" and
   * the only option for writing data.
   * The default is false.
   */
  readonly enableLegacyMutators?: boolean | undefined;
};
