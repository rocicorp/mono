import {metadataToLiteTypeString} from '../services/change-source/column-metadata.ts';
import {id, idList} from '../types/sql.ts';
import type {ColumnSpec, LiteIndexSpec, LiteTableSpec} from './specs.ts';

export function liteColumnDef(spec: ColumnSpec) {
  // Dual-write: Generate pipe-delimited type string for SQLite schema (backward compatibility)
  // This ensures old services can still read from the SQLite schema without the metadata table
  const typeString = metadataToLiteTypeString(spec.metadata);

  let def = id(typeString);

  if (spec.metadata.characterMaxLength) {
    def += `(${spec.metadata.characterMaxLength})`;
  }
  if (spec.notNull) {
    def += ' NOT NULL';
  }
  if (spec.dflt) {
    def += ` DEFAULT ${spec.dflt}`;
  }
  return def;
}

/**
 * Constructs a `CREATE TABLE` statement for a {@link TableSpec}.
 */
export function createLiteTableStatement(spec: LiteTableSpec): string {
  const defs = Object.entries(spec.columns)
    .sort(([_a, {pos: a}], [_b, {pos: b}]) => a - b)
    .map(([name, columnSpec]) => `${id(name)} ${liteColumnDef(columnSpec)}`);
  if (spec.primaryKey) {
    defs.push(`PRIMARY KEY (${idList(spec.primaryKey)})`);
  }

  const createStmt = `CREATE TABLE ${id(spec.name)} (`;
  return [createStmt, defs.join(',\n'), ');'].join('\n');
}

export function createLiteIndexStatement(index: LiteIndexSpec): string {
  const columns = Object.entries(index.columns)
    .map(([name, dir]) => `${id(name)} ${dir}`)
    .join(',');
  const unique = index.unique ? 'UNIQUE' : '';
  return `CREATE ${unique} INDEX ${id(index.name)} ON ${id(
    index.tableName,
  )} (${columns});`;
}
