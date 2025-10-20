import {TEXT_ARRAY_ATTRIBUTE, upstreamDataType} from '../types/lite.ts';
import {id, idList} from '../types/sql.ts';
import type {
  ColumnSpec,
  LiteIndexSpec,
  LiteTableSpec,
  TableSpec,
} from './specs.ts';

export function columnDef(spec: ColumnSpec, forPostgres: boolean) {
  // Remove legacy |TEXT_ARRAY attribute for backwards compatibility
  const typeWithAttrs = spec.dataType.replace(TEXT_ARRAY_ATTRIBUTE, '');

  let def: string;
  if (spec.elemPgTypeClass !== null) {
    // Arrays: PostgreSQL wants "type"[], SQLite wants "type[]" (with attributes inside quotes)
    // upstreamDataType strips attributes (|...) but NOT the [] suffix, so we strip it here
    const baseType = upstreamDataType(typeWithAttrs).replace(/\[\]$/, '');
    // New data has [] suffix, but legacy data might not (it had |TEXT_ARRAY instead)
    const needsBrackets = !typeWithAttrs.endsWith('[]');
    def = forPostgres
      ? `${id(baseType)}[]`
      : id(needsBrackets ? typeWithAttrs + '[]' : typeWithAttrs);
  } else {
    def = id(typeWithAttrs);
  }

  if (spec.characterMaximumLength) {
    def += `(${spec.characterMaximumLength})`;
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
export function createTableStatement(spec: TableSpec | LiteTableSpec): string {
  const forPostgres = 'schema' in spec;
  const defs = Object.entries(spec.columns)
    .sort(([_a, {pos: a}], [_b, {pos: b}]) => a - b)
    .map(
      ([name, columnSpec]) =>
        `${id(name)} ${columnDef(columnSpec, forPostgres)}`,
    );
  if (spec.primaryKey) {
    defs.push(`PRIMARY KEY (${idList(spec.primaryKey)})`);
  }

  const createStmt =
    'schema' in spec
      ? `CREATE TABLE ${id(spec.schema)}.${id(spec.name)} (`
      : `CREATE TABLE ${id(spec.name)} (`;
  return [createStmt, defs.join(',\n'), ');'].join('\n');
}

export function createIndexStatement(index: LiteIndexSpec): string {
  const columns = Object.entries(index.columns)
    .map(([name, dir]) => `${id(name)} ${dir}`)
    .join(',');
  const unique = index.unique ? 'UNIQUE' : '';
  return `CREATE ${unique} INDEX ${id(index.name)} ON ${id(
    index.tableName,
  )} (${columns});`;
}
