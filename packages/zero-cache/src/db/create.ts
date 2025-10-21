import {id, idList} from '../types/sql.ts';
import type {ColumnSpec, LiteIndexSpec, LiteTableSpec} from './specs.ts';

export function liteColumnDef(spec: ColumnSpec) {
  // Remove legacy |TEXT_ARRAY attribute for backwards compatibility
  // Legacy formats: "text|TEXT_ARRAY", "text[]|TEXT_ARRAY", "text|TEXT_ARRAY[]", "text[]|TEXT_ARRAY[]"
  const hadTextArray = spec.dataType.includes('|TEXT_ARRAY');
  const typeWithAttrs = spec.dataType.replace(/\|TEXT_ARRAY(\[\])?/, '');

  // Only add brackets if we had |TEXT_ARRAY and there are no brackets after removing it
  // This handles the legacy "text|TEXT_ARRAY" -> "text" -> "text[]" case
  const needsBrackets = hadTextArray && !typeWithAttrs.includes('[]');
  const finalType = needsBrackets ? typeWithAttrs + '[]' : typeWithAttrs;

  let def = id(finalType);

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
