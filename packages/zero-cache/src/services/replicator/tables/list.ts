import type {Database} from 'better-sqlite3';
import {assert} from 'shared/src/asserts.js';
import {TableSpec} from './specs.js';

type ColumnInfo = {
  table: string;
  name: string;
  type: string;
  notNull: number;
  default: string | null;
  keyPos: number;
};

export function listTables(db: Database): TableSpec[] {
  const columns = db
    .prepare(
      `
      SELECT 
        m.name as "table", 
        p.name as name, 
        p.type as type, 
        p."notnull" as "notNull",
        p.dflt_value as "default",
        p.pk as keyPos 
      FROM sqlite_master as m 
      LEFT JOIN pragma_table_info(m.name) as p 
      WHERE m.type = 'table'
      `,
    )
    .all() as ColumnInfo[];

  // For convenience when building the table spec. The returned TableSpec type is readonly.
  type Writeable<T> = {-readonly [P in keyof T]: Writeable<T[P]>};

  const tables: Writeable<TableSpec>[] = [];
  let table: Writeable<TableSpec> | undefined;

  columns.forEach(col => {
    if (col.table !== table?.name) {
      // New table
      table = {
        schema: '',
        name: col.table,
        columns: {},
        primaryKey: [],
      };
      tables.push(table);
    }

    table.columns[col.name] = {
      dataType: col.type,
      characterMaximumLength: null,
      columnDefault: col.default,
      notNull: col.notNull !== 0,
    };
    if (col.keyPos) {
      while (table.primaryKey.length < col.keyPos) {
        table.primaryKey.push('');
      }
      table.primaryKey[col.keyPos - 1] = col.name;
    }
  });

  // Sanity check that the primary keys are filled in.
  Object.values(tables).forEach(table => {
    assert(
      table.primaryKey.indexOf('') < 0,
      `Invalid primary key for ${JSON.stringify(table)}`,
    );
  });

  return tables;
}
