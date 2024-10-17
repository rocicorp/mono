import {assert} from '../../../shared/src/asserts.js';
import {must} from '../../../shared/src/must.js';
import type {Database} from '../../../zqlite/src/db.js';
import type {
  LiteIndexSpec,
  LiteTableSpec,
  MutableLiteIndexSpec,
} from './specs.js';

type ColumnInfo = {
  table: string;
  name: string;
  type: string;
  notNull: number;
  dflt: string | null;
  keyPos: number;
};

export function listTables(db: Database): LiteTableSpec[] {
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

  const tables: LiteTableSpec[] = [];
  let table: LiteTableSpec | undefined;

  columns.forEach(col => {
    if (col.table !== table?.name) {
      // New table
      table = {
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

export function listIndexes(db: Database): LiteIndexSpec[] {
  const indexes = db
    .prepare(
      `SELECT 
         idx.name as indexName, 
         idx.tbl_name as tableName, 
         info."unique" as "unique",
         col.name as column,
         CASE WHEN col.desc = 0 THEN 'ASC' ELSE 'DESC' END as dir
      FROM sqlite_master as idx
       JOIN pragma_index_list(idx.tbl_name) AS info ON info.name = idx.name
       JOIN pragma_index_xinfo(idx.name) as col
       WHERE idx.type = 'index' AND 
             info.origin != 'pk' AND
             col.key = 1 AND
             idx.tbl_name NOT LIKE '_zero.%'
       ORDER BY idx.name, col.seqno ASC`,
    )
    .all() as {
    indexName: string;
    tableName: string;
    unique: number;
    column: string;
    dir: 'ASC' | 'DESC';
  }[];

  const ret: MutableLiteIndexSpec[] = [];
  for (const {indexName: name, tableName, unique, column, dir} of indexes) {
    if (ret.at(-1)?.name === name) {
      // Aggregate multiple column names into the array.
      must(ret.at(-1)).columns[column] = dir;
    } else {
      ret.push({
        tableName,
        name,
        columns: {[column]: dir},
        unique: unique !== 0,
      });
    }
  }

  return ret;
}
