import {id, idList} from '../types/sql.js';
import type {
  ColumnSpec,
  LiteIndexSpec,
  LiteTableSpec,
  TableSpec,
} from './specs.js';

export function columnDef(spec: ColumnSpec) {
  const parts = [id(spec.dataType)];
  if (spec.characterMaximumLength) {
    parts.push(`(${spec.characterMaximumLength})`);
  }
  if (spec.notNull) {
    parts.push(' NOT NULL');
  }
  if (spec.dflt) {
    parts.push(` DEFAULT ${spec.dflt}`);
  }
  return parts.join('');
}

/**
 * Constructs a `CREATE TABLE` statement for a {@link TableSpec}.
 */
export function createTableStatement(spec: TableSpec | LiteTableSpec): string {
  const defs = Object.entries(spec.columns)
    .sort(([_a, {pos: a}], [_b, {pos: b}]) => a - b)
    .map(([name, columnSpec]) => `${id(name)} ${columnDef(columnSpec)}`);
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
