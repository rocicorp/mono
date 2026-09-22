import type {JSONValue, ReadonlyJSONValue} from '../../shared/src/json.ts';

// Value type from zero-protocol (JSONValue/ReadonlyJSONValue | undefined)
// Defined here to avoid circular dependency with zero-protocol
export type Value = JSONValue | ReadonlyJSONValue | undefined;

export type ColumnNames = {[src: string]: string};

export type DestNames = {
  tableName: string;
  columns: ColumnNames;
  allColumnsSame: boolean;
  /**
   * The source names of the table's `json` columns, when the mapper was built
   * with column type information. Absent for mappers built from names alone
   * (e.g. the identity `validator`), which then skip the json-column check.
   */
  jsonColumns?: ReadonlySet<string> | undefined;
};

export class NameMapper {
  readonly #tables = new Map<string, DestNames>();

  constructor(tables: Map<string, DestNames>) {
    this.#tables = tables;
  }

  #getTable(src: string, ctx?: JSONValue): DestNames {
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

  tableNameIfKnown(src: string): string | undefined {
    return this.#tables.get(src)?.tableName;
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

  /**
   * Maps the column wrapped by a JSON path reference. A JSON path is only
   * valid on a `json` column — on any other column the SQLite replica's
   * `json_type()`/`json_extract()` would throw at fetch time and take the
   * whole client connection down — so a mapper that knows column types
   * rejects it here, at the query boundary.
   */
  jsonColumnName(table: string, src: string, ctx?: JSONValue): string {
    const dest = this.#getTable(table, ctx);
    if (dest.jsonColumns && !dest.jsonColumns.has(src)) {
      throw new Error(
        `column "${src}" of "${table}" table is not a json column ${
          !ctx ? '' : `in ${JSON.stringify(ctx)}`
        }`,
      );
    }
    return this.columnName(table, src, ctx);
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
