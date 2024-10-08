import {assert} from '../../../shared/src/asserts.js';
import type {Database} from '../../../zqlite/src/db.js';
import type {IndexSpec, TableSpec} from '../types/specs.js';

type ColumnInfo = {
  table: string;
  name: string;
  type: string;
  notNull: number;
  dflt: string | null;
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
        p.dflt_value as "dflt",
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
      pos: Object.keys(table.columns).length + 1,
      dataType: col.type,
      characterMaximumLength: null,
      notNull: col.notNull !== 0,
      dflt: col.dflt,
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

export function listIndices(db: Database): IndexSpec[] {
  const indices = db
    .prepare(
      `SELECT name as indexName, tbl_name as tableName FROM sqlite_master WHERE type = 'index' AND tbl_name NOT LIKE '_zero.%'`,
    )
    .all() as {
    indexName: string;
    tableName: string;
  }[];
  const ret: IndexSpec[] = [];
  for (const indexDef of indices) {
    const uniqueAndOrigin = db
      .prepare(
        `SELECT "unique", origin FROM pragma_index_list(?) WHERE name = ?`,
      )
      .get(indexDef.tableName, indexDef.indexName) as {
      unique: number;
      origin: string;
    };
    if (uniqueAndOrigin.origin === 'pk') {
      continue;
    }
    const columns = db
      .prepare(`SELECT name FROM pragma_index_info(?) ORDER BY seqno ASC`)
      .all(indexDef.indexName) as {
      name: string;
    }[];

    ret.push({
      schemaName: '',
      tableName: indexDef.tableName,
      name: indexDef.indexName,
      columns: columns.map(col => col.name),
      unique: uniqueAndOrigin.unique !== 0,
    });
  }

  return ret;
}
