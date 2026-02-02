import type {LogContext} from '@rocicorp/logger';
import {must} from '../../../shared/src/must.ts';
import {difference} from '../../../shared/src/set-utils.ts';
import * as v from '../../../shared/src/valita.ts';
import {primaryKeySchema} from '../../../zero-protocol/src/primary-key.ts';
import type {Database} from '../../../zqlite/src/db.ts';
import {
  ColumnMetadataStore,
  metadataToLiteTypeString,
} from '../services/replicator/schema/column-metadata.ts';
import {
  isArray,
  isEnum,
  liteTypeToZqlValueType,
  mapLiteDataTypeToZqlSchemaValue,
  nullableUpstream,
} from '../types/lite.ts';
import * as PostgresTypeClass from './postgres-type-class-enum.ts';
import type {
  LiteAndZqlSpec,
  LiteIndexSpec,
  LiteTableSpec,
  MutableLiteIndexSpec,
  MutableLiteTableSpec,
} from './specs.ts';

type ColumnInfo = {
  table: string;
  name: string;
  type: string;
  notNull: number;
  dflt: string | null;
  keyPos: number;
};

export type LiteTableSpecWithReplicationStatus = LiteTableSpec & {
  readonly backfilling?: string[];
};

type MutableLiteTableSpecWithReplicationStatus = MutableLiteTableSpec & {
  backfilling: string[];
};

export function listTables(
  db: Database,
  useColumnMetadata = true,
): LiteTableSpecWithReplicationStatus[] {
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
      AND m.name NOT LIKE 'sqlite_%'
      AND m.name NOT LIKE '_zero.%'
      AND m.name NOT LIKE '_litestream_%'
      `,
    )
    .all() as ColumnInfo[];

  const tables: LiteTableSpec[] = [];
  let table: MutableLiteTableSpecWithReplicationStatus | undefined;

  columns.forEach(col => {
    if (col.table !== table?.name) {
      // New table
      table = {
        name: col.table,
        columns: {},
        backfilling: [],
      };
      tables.push(table);
    }

    // Try to read from metadata table first, fall back to SQLite column type
    let dataType: string;
    let elemPgTypeClass:
      | typeof PostgresTypeClass.Base
      | typeof PostgresTypeClass.Enum
      | null;

    const metadata = useColumnMetadata
      ? ColumnMetadataStore.getInstance(db)?.getColumn(col.table, col.name)
      : undefined;
    if (metadata) {
      // Read from metadata table and convert to pipe notation
      dataType = metadataToLiteTypeString(metadata);
      elemPgTypeClass = metadata.isArray
        ? metadata.isEnum
          ? PostgresTypeClass.Enum
          : PostgresTypeClass.Base
        : null;
    } else {
      // Fall back to reading from SQLite column type (pipe notation)
      dataType = col.type;
      elemPgTypeClass = isArray(col.type)
        ? isEnum(col.type)
          ? PostgresTypeClass.Enum
          : PostgresTypeClass.Base
        : null;
    }

    table.columns[col.name] = {
      pos: Object.keys(table.columns).length + 1,
      dataType,
      characterMaximumLength: null,
      notNull: col.notNull !== 0,
      dflt: col.dflt,
      elemPgTypeClass,
    };
    if (metadata?.isBackfilling) {
      table.backfilling.push(col.name);
    }
    if (col.keyPos) {
      table.primaryKey ??= [];
      while (table.primaryKey.length < col.keyPos) {
        table.primaryKey.push('');
      }
      table.primaryKey[col.keyPos - 1] = col.name;
    }
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

export type ZqlSpecOptions = {
  /**
   * Controls whether to include backfilling columns in the computed
   * LiteAndZqlSpec. In general, backfilling columns should be include
   * in "replication" logic (copying data from upstream to the replica),
   * and excluded from "sync" logic (sending data from the replica to
   * clients).
   */
  includeBackfillingColumns: boolean;
};

/**
 * Computes a TableSpec "view" of the replicated data that is
 * suitable for processing / consumption for the client. This
 * includes:
 * * excluding tables without a PRIMARY KEY or UNIQUE INDEX
 * * excluding columns with types that are not supported by ZQL
 * * choosing columns to use as the primary key amongst those
 *   in unique indexes
 *
 * @param tableSpecs an optional map to reset and populate
 * @param fullTables an optional map to receive the full table specs,
 *        which may include tables and columns that are not synced to
 *        the client because they lack a primary key or are of unsupported
 *        data types.
 */
export function computeZqlSpecs(
  lc: LogContext,
  replica: Database,
  opts: ZqlSpecOptions,
  tableSpecs: Map<string, LiteAndZqlSpec> = new Map(),
  fullTables?: Map<string, LiteTableSpec>,
): Map<string, LiteAndZqlSpec> {
  return computeZqlSpecsFromLiteSpecs(
    listTables(replica),
    listIndexes(replica),
    opts,
    tableSpecs,
    fullTables,
    lc,
  );
}

export function computeZqlSpecsFromLiteSpecs(
  tables: LiteTableSpecWithReplicationStatus[],
  indexes: LiteIndexSpec[],
  {includeBackfillingColumns}: ZqlSpecOptions,
  tableSpecs: Map<string, LiteAndZqlSpec> = new Map(),
  fullTables?: Map<string, LiteTableSpec>,
  lc?: LogContext,
): Map<string, LiteAndZqlSpec> {
  tableSpecs.clear();
  fullTables?.clear();

  const uniqueIndexColumns = new Map<string, string[][]>();
  for (const {tableName, columns} of indexes.filter(idx => idx.unique)) {
    if (!uniqueIndexColumns.has(tableName)) {
      uniqueIndexColumns.set(tableName, []);
    }
    uniqueIndexColumns.get(tableName)?.push(Object.keys(columns));
  }

  tables.forEach(fullTable => {
    fullTables?.set(fullTable.name, fullTable);

    const backfilling = new Set(fullTable.backfilling);
    // Only include columns that:
    // - have a defined ZQL Value
    // - aren't backfilling if `includeBackfillingColumns` is false
    const visibleColumns = Object.entries(fullTable.columns).filter(
      ([col, {dataType}]) =>
        liteTypeToZqlValueType(dataType) &&
        (includeBackfillingColumns || !backfilling.has(col)),
    );
    const notNullColumns = new Set(
      visibleColumns
        .filter(
          ([col, {dataType}]) =>
            !nullableUpstream(dataType) || fullTable.primaryKey?.includes(col),
        )
        .map(([col]) => col),
    );

    const uniqueKeys = uniqueIndexColumns.get(fullTable.name) ?? [];
    // Examine all column combinations that can serve as a primary key,
    // i.e. excluding indexes over nullable or unsynced columns.
    const keys = uniqueKeys.filter(
      key => difference(new Set(key), notNullColumns).size === 0,
    );
    if (keys.length === 0) {
      // Only include tables with a row key.
      //
      // Note that this will automatically exclude tables that are being
      // backfilled (when includeBackfillingColumns is `false`) since candidate
      // keys only include visible columns.
      lc?.debug?.(
        `not syncing table ${fullTable.name} because it ` +
          (backfilling.size ? 'is being backfilled' : 'has no primary key'),
      );
      return;
    }
    // Pick the "best" (i.e. shortest) key for default IVM operations.
    const primaryKey = keys.sort(keyCmp)[0];

    const tableSpec = {
      ...fullTable,
      columns: Object.fromEntries(visibleColumns),
      // normalize (sort) keys to minimize creating new objects.
      // See row-key.ts: normalizedKeyOrder()
      primaryKey: v.parse(primaryKey.sort(), primaryKeySchema),
      uniqueKeys: uniqueKeys.map(key => v.parse(key.sort(), primaryKeySchema)),
      allPotentialPrimaryKeys: keys.map(key =>
        v.parse(key.sort(), primaryKeySchema),
      ),
    };

    tableSpecs.set(tableSpec.name, {
      tableSpec,
      zqlSpec: Object.fromEntries(
        Object.entries(tableSpec.columns).map(([name, {dataType}]) => [
          name,
          mapLiteDataTypeToZqlSchemaValue(dataType),
        ]),
      ),
    });
  });
  return tableSpecs;
}

export function mustGetTableSpec(
  tableSpecs: Map<string, LiteAndZqlSpec>,
  tableName: string,
): LiteAndZqlSpec {
  const tableSpec = tableSpecs.get(tableName);
  if (!tableSpec) {
    throw new Error(
      `table '${tableName}' is not one of: ${[...tableSpecs.keys()]
        .filter(t => !t.includes('.') && !t.startsWith('_litestream_'))
        .sort()}. ` +
        `Check the spelling and ensure that the table has a primary key.`,
    );
  }
  return tableSpec;
}

// Deterministic comparator for favoring shorter row keys.
function keyCmp(a: string[], b: string[]) {
  if (a.length !== b.length) {
    return a.length - b.length; // Fewer columns are better.
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] < b[i]) {
      return -1;
    }
    if (a[i] > b[i]) {
      return 1;
    }
  }
  return 0;
}
