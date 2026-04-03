import {must} from '../../../../shared/src/must.ts';
import {
  difference,
  equals,
  intersection,
} from '../../../../shared/src/set-utils.ts';
import type {AST, Condition} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import {ErrorOrigin} from '../../../../zero-protocol/src/error-origin.ts';
import {ProtocolError} from '../../../../zero-protocol/src/error.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../../db/specs.ts';
import {appSchema, upstreamSchema, type ShardID} from '../../types/shards.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../replicator/schema/constants.ts';

export function checkClientSchema(
  shardID: ShardID,
  clientSchema: ClientSchema,
  tableSpecs: Map<string, LiteAndZqlSpec>,
  fullTables: Map<string, LiteTableSpec>,
) {
  if (fullTables.size === 0) {
    throw new ProtocolError({
      kind: 'Internal',
      message:
        `No tables have been synced from upstream. ` +
        `Please check that the ZERO_UPSTREAM_DB has been properly set.`,
      origin: ErrorOrigin.ZeroCache,
    });
  }
  const errors: string[] = [];
  const clientTables = new Set(Object.keys(clientSchema.tables));
  const missingTables = difference(clientTables, tableSpecs);
  for (const missing of [...missingTables].sort()) {
    if (fullTables.has(missing)) {
      errors.push(
        `The "${missing}" table is missing a primary key or non-null ` +
          `unique index and thus cannot be synced to the client`,
      );
    } else {
      const app = appSchema(shardID) + '.';
      const shard = upstreamSchema(shardID) + '.';
      const syncedTables = [...tableSpecs.keys()]
        .filter(t => !t.startsWith(app) && !t.startsWith(shard))
        .sort()
        .map(t => `"${t}"`)
        .join(',');
      const schemaTip =
        missing.includes('.') && !syncedTables.includes('.')
          ? ` Note that zero does not sync tables from non-public schemas ` +
            `by default. Make sure you have defined a custom ` +
            `ZERO_APP_PUBLICATION to sync tables from non-public schemas.`
          : '';
      errors.push(
        `The "${missing}" table does not exist or is not ` +
          `one of the replicated tables: ${syncedTables}.${schemaTip}`,
      );
    }
  }
  const tables = intersection(tableSpecs, clientTables);
  for (const table of [...tables].sort()) {
    const clientSpec = clientSchema.tables[table];
    const serverSpec = must(tableSpecs.get(table)); // guaranteed by intersection
    const fullSpec = must(fullTables.get(table));

    const clientColumns = new Set(Object.keys(clientSpec.columns));
    const syncedColumns = new Set(Object.keys(serverSpec.zqlSpec));
    const missingColumns = difference(clientColumns, syncedColumns);
    for (const missing of [...missingColumns].sort()) {
      if (fullSpec.columns[missing]) {
        errors.push(
          `The "${table}"."${missing}" column cannot be synced because it ` +
            `is of an unsupported data type "${fullSpec.columns[missing].dataType}"`,
        );
      } else {
        const columns = [...syncedColumns]
          .filter(c => c !== ZERO_VERSION_COLUMN_NAME)
          .sort()
          .map(c => `"${c}"`)
          .join(',');

        errors.push(
          `The "${table}"."${missing}" column does not exist ` +
            `or is not one of the replicated columns: ${columns}.`,
        );
      }
    }
    const columns = intersection(clientColumns, syncedColumns);
    for (const column of columns) {
      const clientType = clientSpec.columns[column].type;
      const serverType = serverSpec.zqlSpec[column].type;
      if (clientSpec.columns[column].type !== serverSpec.zqlSpec[column].type) {
        errors.push(
          `The "${table}"."${column}" column's upstream type "${serverType}" ` +
            `does not match the client type "${clientType}"`,
        );
      }
    }
    if (!clientSpec.primaryKey) {
      errors.push(
        `The "${table}" table's client schema does not specify a primary key.`,
      );
    } else {
      const clientPrimaryKey = new Set(clientSpec.primaryKey);
      if (
        !serverSpec.tableSpec.allPotentialPrimaryKeys.some(key =>
          equals(clientPrimaryKey, new Set(key)),
        )
      ) {
        errors.push(
          `The "${table}" table's primaryKey <${clientSpec.primaryKey.join(',')}> ` +
            `is not associated with a non-null unique index.`,
        );
      }
    }
  }
  if (errors.length) {
    throw new ProtocolError({
      kind: 'SchemaVersionNotSupported',
      message: errors.join('\n'),
      origin: ErrorOrigin.ZeroCache,
    });
  }
}

/**
 * Validates that a transformed AST only references tables and columns
 * that exist in the replica's tableSpecs. Returns a list of validation
 * error strings (empty if valid).
 */
export function checkTransformedAST(
  ast: AST,
  tableSpecs: ReadonlyMap<string, LiteAndZqlSpec>,
): string[] {
  const errors: string[] = [];
  checkASTRecursive(ast, tableSpecs, errors);
  return errors;
}

function syncedTablesList(
  tableSpecs: ReadonlyMap<string, LiteAndZqlSpec>,
): string {
  return [...tableSpecs.keys()]
    .filter(t => !t.includes('.'))
    .sort()
    .map(t => `"${t}"`)
    .join(',');
}

function syncedColumnsList(
  tableSpecs: ReadonlyMap<string, LiteAndZqlSpec>,
  table: string,
): string {
  const spec = tableSpecs.get(table);
  if (!spec) {
    return '';
  }
  return Object.keys(spec.zqlSpec)
    .filter(c => c !== ZERO_VERSION_COLUMN_NAME)
    .sort()
    .map(c => `"${c}"`)
    .join(',');
}

function checkASTRecursive(
  ast: AST,
  tableSpecs: ReadonlyMap<string, LiteAndZqlSpec>,
  errors: string[],
): void {
  const tableSpec = tableSpecs.get(ast.table);
  if (!tableSpec) {
    errors.push(
      `The "${ast.table}" table does not exist or is not ` +
        `one of the replicated tables: ${syncedTablesList(tableSpecs)}.`,
    );
    // Can't validate columns if table doesn't exist.
    return;
  }

  const syncedColumns = new Set(Object.keys(tableSpec.zqlSpec));

  const checkColumn = (column: string) => {
    if (!syncedColumns.has(column)) {
      errors.push(
        `The "${ast.table}"."${column}" column does not exist ` +
          `or is not one of the replicated columns: ${syncedColumnsList(tableSpecs, ast.table)}.`,
      );
    }
  };

  // Validate columns in where conditions.
  if (ast.where) {
    checkConditionColumns(
      ast.table,
      ast.where,
      tableSpecs,
      errors,
      checkColumn,
    );
  }

  // Validate columns in orderBy.
  if (ast.orderBy) {
    for (const [column] of ast.orderBy) {
      checkColumn(column);
    }
  }

  // Validate columns in start.row.
  if (ast.start) {
    for (const column of Object.keys(ast.start.row)) {
      checkColumn(column);
    }
  }

  // Validate related subqueries.
  if (ast.related) {
    for (const related of ast.related) {
      // Validate correlation parent fields (belong to this table).
      for (const column of related.correlation.parentField) {
        checkColumn(column);
      }
      // Validate correlation child fields and the subquery itself.
      const childSpec = tableSpecs.get(related.subquery.table);
      if (childSpec) {
        const childColumns = new Set(Object.keys(childSpec.zqlSpec));
        for (const column of related.correlation.childField) {
          if (!childColumns.has(column)) {
            errors.push(
              `The "${related.subquery.table}"."${column}" column does not exist ` +
                `or is not one of the replicated columns: ${syncedColumnsList(tableSpecs, related.subquery.table)}.`,
            );
          }
        }
      }
      // Recursively validate the subquery AST.
      checkASTRecursive(related.subquery, tableSpecs, errors);
    }
  }
}

function checkConditionColumns(
  table: string,
  condition: Condition,
  tableSpecs: ReadonlyMap<string, LiteAndZqlSpec>,
  errors: string[],
  checkColumn: (column: string) => void,
): void {
  switch (condition.type) {
    case 'simple':
      if (condition.left.type === 'column') {
        checkColumn(condition.left.name);
      }
      break;
    case 'and':
    case 'or':
      for (const sub of condition.conditions) {
        checkConditionColumns(table, sub, tableSpecs, errors, checkColumn);
      }
      break;
    case 'correlatedSubquery':
      // Recursively validate the correlated subquery's AST.
      checkASTRecursive(condition.related.subquery, tableSpecs, errors);
      break;
  }
}
