import {must} from '../../../../shared/src/must.ts';
import {difference, intersection} from '../../../../shared/src/set-utils.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../../db/specs.ts';
import {ErrorForClient} from '../../types/error-for-client.ts';
import {appSchema, upstreamSchema, type ShardID} from '../../types/shards.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../replicator/schema/replication-state.ts';

export function checkClientSchema(
  shardID: ShardID,
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
    for (const column of [...columns]) {
      const clientType = clientSpec.columns[column].type;
      const serverType = serverSpec.zqlSpec[column].type;
      if (clientSpec.columns[column].type !== serverSpec.zqlSpec[column].type) {
        // 'timestamp' and 'date' schema types were introduced in
        //  0.18, prior to this these types were typed as 'number'.
        // Accept number for at least one release for backwards compat.
        if (
          !(
            (serverType === 'timestamp' || serverType === 'date') &&
            clientType === 'number'
          )
        ) {
          errors.push(
            `The "${table}"."${column}" column's upstream type "${serverType}" ` +
              `does not match the client type "${clientType}"`,
          );
        }
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
