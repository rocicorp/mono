import {id, idList} from '../types/sql.js';
import type {ColumnSpec, LiteIndexSpec, TableSpec} from './specs.js';

/**
 * Constructs a `CREATE TABLE` statement for a {@link TableSpec}.
 */
export function createTableStatement(spec: TableSpec): string {
  function colDef(name: string, colSpec: ColumnSpec): string {
    const parts = [`${id(name)} ${colSpec.dataType}`];
    if (colSpec.characterMaximumLength) {
      parts.push(`(${colSpec.characterMaximumLength})`);
    }
    if (colSpec.notNull) {
      parts.push(' NOT NULL');
    }
    // Note: DEFAULT expressions are ignored for CREATE TABLE statements,
    // as in that case, row values always come from the replication stream.
    return parts.join('');
  }

  const defs = Object.entries(spec.columns)
    .sort(([_a, {pos: a}], [_b, {pos: b}]) => a - b)
    .map(([name, col]) => colDef(name, col));
  if (spec.primaryKey) {
    defs.push(`PRIMARY KEY (${idList(spec.primaryKey)})`);
  }

  const createStmt = spec.schema.length
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
  )} (${columns})`;
}
