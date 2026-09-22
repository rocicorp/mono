import type {JSONValue, ReadonlyJSONValue} from '../../shared/src/json.ts';

// Value type from zero-protocol (JSONValue/ReadonlyJSONValue | undefined)
// Defined here to avoid circular dependency with zero-protocol
export type Value = JSONValue | ReadonlyJSONValue | undefined;

export type ColumnNames = {[src: string]: string};

export type DestNames = {
  tableName: string;
  columns: ColumnNames;
  allColumnsSame: boolean;
};

export class NameMapper {
  readonly #tables = new Map<string, DestNames>();

  constructor(tables: Map<string, DestNames>) {
    this.#tables = tables;
  }

  #getTable(src: string): DestNames {
    const table = this.#tables.get(src);
    if (!table) {
      // log-leak-ignore -- a table name is schema, not customer data
      throw new Error(`unknown table "${src}"`);
    }
    return table;
  }

  tableName(src: string): string {
    return this.#getTable(src).tableName;
  }

  tableNameIfKnown(src: string): string | undefined {
    return this.#tables.get(src)?.tableName;
  }

  columnName(table: string, src: string): string {
    const dst = this.#getTable(table).columns[src];
    if (!dst) {
      // log-leak-ignore -- table and column names are schema, not customer data
      throw new Error(`unknown column "${src}" of "${table}" table`);
    }
    return dst;
  }

  row<V extends Value>(
    table: string,
    row: Readonly<Record<string, V>>,
  ): Readonly<Record<string, V>> {
    const dest = this.#getTable(table);
    const {allColumnsSame, columns} = dest;
    if (allColumnsSame) {
      return row;
    }
    const clientRow: Record<string, V> = {};
    for (const col in row) {
      // Note: columns with unknown names simply pass through.
      clientRow[columns[col] ?? col] = row[col];
    }
    return clientRow;
  }

  columns<Columns extends readonly string[] | undefined>(
    table: string,
    cols: Columns,
  ): Columns {
    const dest = this.#getTable(table);
    const {allColumnsSame, columns} = dest;

    // Note: Columns not defined in the schema simply pass through.
    return cols === undefined || allColumnsSame
      ? cols
      : (cols.map(col => columns[col] ?? col) as unknown as Columns);
  }
}
