import {must} from '../../../../shared/src/must.ts';
import {difference, intersection} from '../../../../shared/src/set-utils.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../../db/specs.ts';
import {ErrorForClient} from '../../types/error-for-client.ts';

export function checkClientSchema(
  clientSchema: ClientSchema,
  tableSpecs: Map<string, LiteAndZqlSpec>,
  fullTables: Map<string, LiteTableSpec>,
) {
  if (fullTables.size === 0) {
    throw new ErrorForClient({
      kind: 'Internal',
      message:
        `No tables have been synced from upstream. ` +
        `Please check that the ZERO_UPSTREAM_DB has been properly set.`,
    });
  }
  const errors: string[] = [];
  const clientTables = new Set(Object.keys(clientSchema.tables));
  const missingTables = difference(clientTables, tableSpecs);
  for (const missing of [...missingTables].sort()) {
    if (fullTables.has(missing)) {
      errors.push(
        `The "${missing}" table is missing a primary key or non-null unique index and thus cannot be synced to the client`,
      );
    } else {
      errors.push(
        `The "${missing}" table does not exist or is not being replicated from the upstream database.`,
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
          `The "${table}"."${missing}" column cannot be synced because it is of an unsupported data type "${fullSpec.columns[missing].dataType}"`,
        );
      } else {
        errors.push(
          `The "${table}"."${missing}" column does not exist or is not being replicated from the upstream database`,
        );
      }
    }
    const columns = intersection(clientColumns, syncedColumns);
    for (const column of [...columns]) {
      const clientType = clientSpec.columns[column].type;
      const serverType = serverSpec.zqlSpec[column].type;
      if (clientSpec.columns[column].type !== serverSpec.zqlSpec[column].type) {
        errors.push(
          `The "${table}"."${column}" column's upstream type "${serverType}" does not match the client type "${clientType}"`,
        );
      }
    }
  }
  if (errors.length) {
    throw new ErrorForClient({
      kind: 'SchemaVersionNotSupported',
      message: errors.join('\n'),
    });
  }
}
