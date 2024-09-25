import type {ColumnSpec, TableSpec} from 'zero-cache/src/types/specs.js';
import {id, idList} from 'zero-cache/src/types/sql.js';

/**
 * Constructs a `CREATE TABLE` statement for a {@link TableSpec}.
 */
export function createTableStatement(spec: TableSpec): string {
  function colDef(name: string, colSpec: ColumnSpec): string {
    const parts = [`${id(name)} ${colSpec.dataType}`];
    if (colSpec.characterMaximumLength !== null) {
      parts.push(`(${colSpec.characterMaximumLength})`);
    }
    if (colSpec.notNull) {
      parts.push(' NOT NULL');
    }
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
