import type postgres from 'postgres';

/**
 * PostgreSQL type class from pg_type.typtype
 * b=base, c=composite, d=domain, e=enum, p=pseudo, r=range, m=multirange
 */
export type PgTypeClass = 'b' | 'c' | 'd' | 'e' | 'p' | 'r' | 'm';

/**
 * Introspected column metadata
 */
export interface IntrospectedColumn {
  name: string;
  position: number;
  dataType: string; // SQL standard type (e.g., 'character varying')
  udtName: string; // PostgreSQL type name (e.g., 'varchar', 'int4')
  isNullable: boolean;
  characterMaxLength: number | null;
  numericPrecision: number | null;
  numericScale: number | null;
  defaultValue: string | null;
  pgTypeClass: PgTypeClass;
  isArray: boolean;
  arrayElementTypeClass: PgTypeClass | null;
}

/**
 * Introspected table metadata
 */
export interface IntrospectedTable {
  schema: string;
  name: string;
  columns: IntrospectedColumn[];
  primaryKey: string[]; // Column names in order
}

/**
 * Introspected enum type
 */
export interface IntrospectedEnum {
  schema: string;
  name: string;
  values: string[]; // Enum labels in sort order
}

/**
 * Foreign key ON DELETE/UPDATE action
 */
export type ForeignKeyAction =
  | 'NO ACTION'
  | 'RESTRICT'
  | 'CASCADE'
  | 'SET NULL'
  | 'SET DEFAULT';

/**
 * Introspected foreign key constraint
 */
export interface IntrospectedForeignKey {
  constraintName: string;
  sourceSchema: string;
  sourceTable: string;
  sourceColumns: string[]; // In constraint order
  targetSchema: string;
  targetTable: string;
  targetColumns: string[]; // In constraint order
  onDelete: ForeignKeyAction;
  onUpdate: ForeignKeyAction;
}

/**
 * Introspected unique constraint (for relationship inference)
 */
export interface IntrospectedUniqueConstraint {
  constraintName: string;
  schema: string;
  tableName: string;
  columns: string[];
  isPrimaryKey: boolean;
}

/**
 * Complete introspected schema
 */
export interface IntrospectedSchema {
  schemaName: string;
  tables: IntrospectedTable[];
  enums: IntrospectedEnum[];
  foreignKeys: IntrospectedForeignKey[];
  uniqueConstraints: IntrospectedUniqueConstraint[];
}

/**
 * Introspection options
 */
export interface IntrospectOptions {
  /** PostgreSQL connection string */
  connectionString?: string | undefined;
  /** Existing postgres.js client */
  client?: postgres.Sql | undefined;
  /** Schema to introspect (default: 'public') */
  schema?: string | undefined;
  /** Tables to include (default: all) */
  includeTables?: string[] | undefined;
  /** Tables to exclude (default: none) */
  excludeTables?: string[] | undefined;
}
