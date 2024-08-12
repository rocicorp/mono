import {id, idList} from '../../../types/sql.js';
import type {ColumnSpec, TableSpec} from './specs.js';

/**
 * Constructs a `CREATE TABLE` statement for a {@link TableSpec}.
 *
 * For replication purposes, the `notNull` (i.e. `NOT NULL`) constraint is ignored.
 * Although logical replication notifies the subscriber of changes to the table
 * _structure_, it does not notify the subscriber of changes to table constraints
 * (other than primary keys). As such, it is unsafe to initialize a replicated table
 * with a constraint that might be removed without our knowledge.
 *
 * Instead, it is sufficient to assume that the data in the logical replication stream
 * satisfies the constraints of the upstream table.
 */
export function createTableStatementIgnoringNotNullConstraint(
  spec: TableSpec,
): string {
  function colDef(name: string, colSpec: ColumnSpec): string {
    const parts = [`${id(name)} ${colSpec.dataType}`];
    if (colSpec.characterMaximumLength !== null) {
      parts.push(`(${colSpec.characterMaximumLength})`);
    }
    if (colSpec.columnDefault) {
      parts.push(` DEFAULT ${colSpec.columnDefault}`);
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
