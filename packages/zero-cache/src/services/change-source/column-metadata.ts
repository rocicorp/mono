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

import type {Database, Statement} from '../../../../zqlite/src/db.ts';
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
  isNotNull: boolean;
  isEnum: boolean;
  isArray: boolean;
  /** Maximum character length for varchar/char types */
  characterMaxLength?: number | null;
}

type ColumnMetadataRow = {
  upstream_type: string;
  is_not_null: number;
  is_enum: number;
  is_array: number;
  character_max_length: number | null;
};

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
`;

/**
 * Efficient column metadata store that prepares all statements upfront.
 * Use this class to avoid re-preparing statements on every operation.
 */
export class ColumnMetadataStore {
  readonly #insertStmt: Statement;
  readonly #updateStmt: Statement;
  readonly #deleteColumnStmt: Statement;
  readonly #deleteTableStmt: Statement;
  readonly #renameTableStmt: Statement;
  readonly #getColumnStmt: Statement;
  readonly #getTableStmt: Statement;
  readonly #hasTableStmt: Statement;

  constructor(db: Database) {
    this.#insertStmt = db.prepare(`
      INSERT INTO "_zero.column_metadata"
        (table_name, column_name, upstream_type, is_not_null, is_enum, is_array, character_max_length)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);

    this.#updateStmt = db.prepare(`
      UPDATE "_zero.column_metadata"
      SET column_name = ?,
          upstream_type = ?,
          is_not_null = ?,
          is_enum = ?,
          is_array = ?,
          character_max_length = ?
      WHERE table_name = ? AND column_name = ?
    `);

    this.#deleteColumnStmt = db.prepare(`
      DELETE FROM "_zero.column_metadata"
      WHERE table_name = ? AND column_name = ?
    `);

    this.#deleteTableStmt = db.prepare(`
      DELETE FROM "_zero.column_metadata"
      WHERE table_name = ?
    `);

    this.#renameTableStmt = db.prepare(`
      UPDATE "_zero.column_metadata"
      SET table_name = ?
      WHERE table_name = ?
    `);

    this.#getColumnStmt = db.prepare(`
      SELECT upstream_type, is_not_null, is_enum, is_array, character_max_length
      FROM "_zero.column_metadata"
      WHERE table_name = ? AND column_name = ?
    `);

    this.#getTableStmt = db.prepare(`
      SELECT column_name, upstream_type, is_not_null, is_enum, is_array, character_max_length
      FROM "_zero.column_metadata"
      WHERE table_name = ?
      ORDER BY column_name
    `);

    this.#hasTableStmt = db.prepare(`
      SELECT 1 FROM sqlite_master
      WHERE type = 'table' AND name = '_zero.column_metadata'
    `);
  }

  insert(
    tableName: string,
    columnName: string,
    metadata: ColumnMetadata,
  ): void {
    this.#insertStmt.run(
      tableName,
      columnName,
      metadata.upstreamType,
      metadata.isNotNull ? 1 : 0,
      metadata.isEnum ? 1 : 0,
      metadata.isArray ? 1 : 0,
      metadata.characterMaxLength ?? null,
    );
  }

  update(
    tableName: string,
    oldColumnName: string,
    newColumnName: string,
    metadata: ColumnMetadata,
  ): void {
    this.#updateStmt.run(
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

  deleteColumn(tableName: string, columnName: string): void {
    this.#deleteColumnStmt.run(tableName, columnName);
  }

  deleteTable(tableName: string): void {
    this.#deleteTableStmt.run(tableName);
  }

  renameTable(oldTableName: string, newTableName: string): void {
    this.#renameTableStmt.run(newTableName, oldTableName);
  }

  getColumn(tableName: string, columnName: string): ColumnMetadata | undefined {
    const row = this.#getColumnStmt.get(tableName, columnName) as
      | ColumnMetadataRow
      | undefined;

    if (!row) {
      return undefined;
    }

    return {
      upstreamType: row.upstream_type,
      isNotNull: row.is_not_null !== 0,
      isEnum: row.is_enum !== 0,
      isArray: row.is_array !== 0,
      characterMaxLength: row.character_max_length,
    };
  }

  getTable(tableName: string): Map<string, ColumnMetadata> {
    const rows = this.#getTableStmt.all(tableName) as Array<
      ColumnMetadataRow & {column_name: string}
    >;

    const metadata = new Map<string, ColumnMetadata>();
    for (const row of rows) {
      metadata.set(row.column_name, {
        upstreamType: row.upstream_type,
        isNotNull: row.is_not_null !== 0,
        isEnum: row.is_enum !== 0,
        isArray: row.is_array !== 0,
        characterMaxLength: row.character_max_length,
      });
    }

    return metadata;
  }

  hasTable(): boolean {
    const result = this.#hasTableStmt.get();
    return result !== undefined;
  }

  /**
   * Populates metadata table from existing tables that use pipe notation.
   * This is used during migration v6 to backfill the metadata table.
   */
  populateFromExistingTables(tables: LiteTableSpec[]): void {
    for (const table of tables) {
      for (const [columnName, columnSpec] of Object.entries(table.columns)) {
        const metadata = liteTypeStringToMetadata(
          columnSpec.dataType,
          columnSpec.characterMaximumLength,
        );
        this.insert(table.name, columnName, metadata);
      }
    }
  }
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
 * Converts structured ColumnMetadata back to pipe-delimited LiteTypeString.
 * This is a compatibility helper for the migration period.
 */
export function metadataToLiteTypeString(metadata: ColumnMetadata): string {
  const {upstreamType, isNotNull, isEnum} = metadata;

  let typeString = upstreamType;
  if (isNotNull) {
    typeString += '|NOT_NULL';
  }
  if (isEnum) {
    typeString += '|TEXT_ENUM';
  }
  return typeString;
}
