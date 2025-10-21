/**
 * Column metadata table for storing upstream PostgreSQL schema information.
 *
 * Previously, upstream type metadata was embedded in SQLite column type strings
 * using pipe-delimited notation (e.g., "int8|NOT_NULL|TEXT_ENUM"). This caused
 * issues with SQLite type affinity and made schema inspection difficult.
 *
 * This table stores that metadata separately, allowing SQLite columns to use
 * plain type names while preserving all necessary upstream type information.
 */

import {Database} from '../../../../zqlite/src/db.ts';
import type {LiteTableSpec} from '../../db/specs.ts';
import {
  upstreamDataType,
  nullableUpstream,
  isEnum as checkIsEnum,
  isArray as checkIsArray,
} from '../../types/lite.ts';

/**
 * Structured column metadata, replacing the old pipe-delimited string format.
 */
export interface ColumnMetadata {
  /** PostgreSQL type name, e.g., 'int8', 'varchar', 'text[]', 'user_role' */
  upstreamType: string;
  /** True if column has NOT NULL constraint */
  isNotNull: boolean;
  /** True if column is a PostgreSQL enum type */
  isEnum: boolean;
  /** True if column is an array type (includes [] in upstreamType) */
  isArray: boolean;
  /** Maximum character length for varchar/char types */
  characterMaxLength?: number | null;
}

/**
 * Creates the _zero.column_metadata table.
 *
 * Columns:
 * - table_name: The name of the table this column belongs to
 * - column_name: The name of the column
 * - upstream_type: The PostgreSQL type name (e.g., 'int8', 'varchar', 'my_enum')
 * - is_not_null: 1 if column has NOT NULL constraint, 0 otherwise
 * - is_enum: 1 if column is a PostgreSQL enum type, 0 otherwise
 * - is_array: 1 if column is an array type, 0 otherwise
 * - character_max_length: Maximum length for character types (nullable)
 */
export const CREATE_COLUMN_METADATA_TABLE = `
  CREATE TABLE "_zero.column_metadata" (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    upstream_type TEXT NOT NULL,
    is_not_null INTEGER NOT NULL,
    is_enum INTEGER NOT NULL,
    is_array INTEGER NOT NULL,
    character_max_length INTEGER,
    PRIMARY KEY (table_name, column_name)
  );
  CREATE INDEX "idx_column_metadata_table" ON "_zero.column_metadata"(table_name);
`;

/**
 * Inserts metadata for a single column.
 */
export function insertColumnMetadata(
  db: Database,
  tableName: string,
  columnName: string,
  metadata: ColumnMetadata,
): void {
  db.prepare(
    `
    INSERT INTO "_zero.column_metadata"
      (table_name, column_name, upstream_type, is_not_null, is_enum, is_array, character_max_length)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    `,
  ).run(
    tableName,
    columnName,
    metadata.upstreamType,
    metadata.isNotNull ? 1 : 0,
    metadata.isEnum ? 1 : 0,
    metadata.isArray ? 1 : 0,
    metadata.characterMaxLength ?? null,
  );
}

/**
 * Updates metadata for a column (type change or rename).
 */
export function updateColumnMetadata(
  db: Database,
  tableName: string,
  oldColumnName: string,
  newColumnName: string,
  metadata: ColumnMetadata,
): void {
  db.prepare(
    `
    UPDATE "_zero.column_metadata"
    SET column_name = ?,
        upstream_type = ?,
        is_not_null = ?,
        is_enum = ?,
        is_array = ?,
        character_max_length = ?
    WHERE table_name = ? AND column_name = ?
    `,
  ).run(
    newColumnName,
    metadata.upstreamType,
    metadata.isNotNull ? 1 : 0,
    metadata.isEnum ? 1 : 0,
    metadata.isArray ? 1 : 0,
    metadata.characterMaxLength ?? null,
    tableName,
    oldColumnName,
  );
}

/**
 * Deletes metadata for a single column.
 */
export function deleteColumnMetadata(
  db: Database,
  tableName: string,
  columnName: string,
): void {
  db.prepare(
    `
    DELETE FROM "_zero.column_metadata"
    WHERE table_name = ? AND column_name = ?
    `,
  ).run(tableName, columnName);
}

/**
 * Deletes all metadata for a table.
 */
export function deleteTableMetadata(db: Database, tableName: string): void {
  db.prepare(
    `
    DELETE FROM "_zero.column_metadata"
    WHERE table_name = ?
    `,
  ).run(tableName);
}

/**
 * Renames a table in the metadata (updates all column entries).
 */
export function renameTableMetadata(
  db: Database,
  oldTableName: string,
  newTableName: string,
): void {
  db.prepare(
    `
    UPDATE "_zero.column_metadata"
    SET table_name = ?
    WHERE table_name = ?
    `,
  ).run(newTableName, oldTableName);
}

/**
 * Converts pipe-delimited LiteTypeString to structured ColumnMetadata.
 * This is a compatibility helper for the migration period.
 */
export function liteTypeStringToMetadata(
  liteTypeString: string,
  characterMaxLength?: number | null,
): ColumnMetadata {
  const baseType = upstreamDataType(liteTypeString);
  const isArrayType = checkIsArray(liteTypeString);

  // Reconstruct the full upstream type including array notation
  // For new-style arrays like 'text[]', upstreamDataType returns 'text[]'
  // For old-style arrays like 'int4|NOT_NULL[]', upstreamDataType returns 'int4', so we append '[]'
  const fullUpstreamType =
    isArrayType && !baseType.includes('[]') ? `${baseType}[]` : baseType;

  return {
    upstreamType: fullUpstreamType,
    isNotNull: !nullableUpstream(liteTypeString),
    isEnum: checkIsEnum(liteTypeString),
    isArray: isArrayType,
    characterMaxLength: characterMaxLength ?? null,
  };
}

/**
 * Populates metadata table from existing tables that use pipe notation.
 * This is used during migration v6 to backfill the metadata table.
 */
export function populateColumnMetadataFromExistingTables(
  db: Database,
  tables: LiteTableSpec[],
): void {
  for (const table of tables) {
    for (const [columnName, columnSpec] of Object.entries(table.columns)) {
      const metadata = liteTypeStringToMetadata(
        columnSpec.dataType,
        columnSpec.characterMaximumLength,
      );
      insertColumnMetadata(db, table.name, columnName, metadata);
    }
  }
}

/**
 * Checks if the metadata table exists.
 */
export function hasColumnMetadataTable(db: Database): boolean {
  const result = db
    .prepare(
      `
    SELECT 1 FROM sqlite_master
    WHERE type = 'table' AND name = '_zero.column_metadata'
    `,
    )
    .get();
  return result !== undefined;
}
