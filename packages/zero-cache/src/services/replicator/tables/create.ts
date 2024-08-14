import {id, idList} from '../../../types/sql.js';
import type {ColumnSpec, TableSpec} from './specs.js';

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

  const defs = Object.entries(spec.columns).map(([name, col]) =>
    colDef(name, col),
  );
  if (spec.primaryKey) {
    defs.push(`PRIMARY KEY (${idList(spec.primaryKey)})`);
  }

  const createStmt = spec.schema.length
    ? `CREATE TABLE ${id(spec.schema)}.${id(spec.name)} (`
    : `CREATE TABLE ${id(spec.name)} (`;
  return [createStmt, defs.join(',\n'), ');'].join('\n');
}
