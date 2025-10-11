import {NameMapper} from '../../zero-types/src/name-mapper.ts';
import type {TableSchema} from './table-schema.ts';

export {NameMapper};

export function clientToServer(
  tables: Record<string, TableSchema>,
): NameMapper {
  return createMapperFrom('client', tables);
}

export function serverToClient(
  tables: Record<string, TableSchema>,
): NameMapper {
  return createMapperFrom('server', tables);
}

function createMapperFrom(
  src: 'client' | 'server',
  tables: Record<string, TableSchema>,
): NameMapper {
  const mapping = new Map(
    Object.entries(tables).map(
      ([tableName, {serverName: serverTableName, columns}]) => {
        let allColumnsSame = true;
        const names: Record<string, string> = {};
        for (const [name, {serverName}] of Object.entries(columns)) {
          if (serverName && serverName !== name) {
            allColumnsSame = false;
          }
          if (src === 'client') {
            names[name] = serverName ?? name;
          } else {
            names[serverName ?? name] = name;
          }
        }
        return [
          src === 'client' ? tableName : (serverTableName ?? tableName),
          {
            tableName:
              src === 'client' ? (serverTableName ?? tableName) : tableName,
            columns: names,
            allColumnsSame,
          },
        ];
      },
    ),
  );
  return new NameMapper(mapping);
}

/**
 * Returns an "identity" NameMapper that simply serves the purpose
 * of validating that all table and column names conform to the
 * specified `tablesToColumns` map.
 */
export function validator(tablesToColumns: Map<string, string[]>): NameMapper {
  const identity = new Map(
    [...tablesToColumns.entries()].map(([tableName, columns]) => [
      tableName,
      {
        tableName,
        columns: Object.fromEntries(columns.map(c => [c, c])),
        allColumnsSame: true,
      },
    ]),
  );
  return new NameMapper(identity);
}
