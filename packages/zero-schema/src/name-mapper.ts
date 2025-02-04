import type {JSONValue} from '../../shared/src/json.ts';
import type {Row, Value} from '../../zero-protocol/src/data.ts';
import type {TableSchema} from './table-schema.ts';

type ColumnNames = {[src: string]: string};

type DestNames = {
  tableName: string;
  columns: ColumnNames;
  allColumnsSame: boolean;
};

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
          src === 'client' ? tableName : serverTableName ?? tableName,
          {
            tableName:
              src === 'client' ? serverTableName ?? tableName : tableName,
            columns: names,
            allColumnsSame,
          },
        ];
      },
    ),
  );
  return new NameMapper(mapping);
}

export class NameMapper {
  readonly #tables = new Map<string, DestNames>();

  constructor(tables: Map<string, DestNames>) {
    this.#tables = tables;
  }

  #getTable(src: string, ctx?: JSONValue | undefined): DestNames {
    const table = this.#tables.get(src);
    if (!table) {
      throw new Error(
        `unknown table "${src}" ${!ctx ? '' : `in ${JSON.stringify(ctx)}`}`,
      );
    }
    return table;
  }

  tableName(src: string, context?: JSONValue): string {
    return this.#getTable(src, context).tableName;
  }

  columnName(table: string, src: string, ctx?: JSONValue): string {
    const dst = this.#getTable(table, ctx).columns[src];
    if (!dst) {
      throw new Error(
        `unknown column "${src}" of "${table}" table ${
          !ctx ? '' : `in ${JSON.stringify(ctx)}`
        }`,
      );
    }
    return dst;
  }

  row<R extends Row>(table: string, row: R): R {
    const dest = this.#getTable(table);
    const {allColumnsSame, columns} = dest;
    if (allColumnsSame) {
      return row;
    }
    const clientRow: Record<string, Value> = {};
    for (const col in row) {
      // Note: columns with unknown names simply pass through.
      clientRow[columns[col] ?? col] = row[col];
    }
    return clientRow as R;
  }

  columns(
    table: string,
    cols: readonly string[] | undefined,
  ): readonly string[] | undefined {
    const dest = this.#getTable(table);
    const {allColumnsSame, columns} = dest;

    // Note: Columns not defined in the schema simply pass through.
    return cols === undefined || allColumnsSame
      ? cols
      : cols.map(col => columns[col] ?? col);
  }
}
