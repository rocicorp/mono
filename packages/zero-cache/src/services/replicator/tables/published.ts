import type postgres from 'postgres';
import {assert} from 'shared/src/asserts.js';
import {equals} from 'shared/src/set-utils.js';
import * as v from 'shared/src/valita.js';
import type {FilteredTableSpec, IndexSpec, MutableIndexSpec} from './specs.js';

const publishedColumnsSchema = v.array(
  v.object({
    schema: v.string(),
    table: v.string(),
    pos: v.number(),
    name: v.string(),
    type: v.string(),
    typeID: v.number(),
    maxLen: v.number(),
    arrayDims: v.number(),
    keyPos: v.number().nullable(),
    notNull: v.boolean(),
    rowFilter: v.string().nullable(),
    pubname: v.string(),
  }),
);

const publicationSchema = v.object({
  pubname: v.string(),
  pubinsert: v.boolean(),
  pubupdate: v.boolean(),
  pubdelete: v.boolean(),
  pubtruncate: v.boolean(),
});

const publicationsResultSchema = v.array(publicationSchema);

export type Publication = v.Infer<typeof publicationSchema>;

export type PublicationInfo = {
  readonly publications: Publication[];
  readonly tables: FilteredTableSpec[];
  readonly indices: IndexSpec[];
};

/** The publication prefix used for tables replicated to zero. */
export const ZERO_PUB_PREFIX = 'zero_';

/**
 * Retrieves all tables and columns published under any PUBLICATION
 * whose name starts with the specified `pubPrefix` (e.g. "zero_").
 */
export async function getPublicationInfo(
  sql: postgres.Sql,
  pubPrefix = ZERO_PUB_PREFIX,
): Promise<PublicationInfo> {
  const result = await sql.unsafe(`
  SELECT ${Object.keys(publicationSchema.shape).join(',')} FROM pg_publication
    WHERE STARTS_WITH(pubname, '${pubPrefix}')
    ORDER BY pubname;

  SELECT 
    nspname AS schema, 
    pc.relname AS table, 
    attnum AS pos, 
    attname AS name, 
    pt.typname AS type, 
    atttypid AS "typeID", 
    atttypmod AS "maxLen", 
    attndims "arrayDims", 
    ARRAY_POSITION(conkey, attnum) AS "keyPos",
    attnotnull as "notNull",
    pb.rowfilter as "rowFilter",
    pb.pubname
  FROM pg_attribute
  JOIN pg_class pc ON pc.oid = attrelid
  JOIN pg_namespace pns ON pns.oid = relnamespace
  JOIN pg_type pt ON atttypid = pt.oid
  JOIN pg_publication_tables as pb ON 
    pb.schemaname = nspname AND 
    pb.tablename = pc.relname AND
    attname = ANY(pb.attnames)
  LEFT JOIN pg_constraint pk ON pk.contype = 'p' AND pk.connamespace = relnamespace AND pk.conrelid = attrelid
  LEFT JOIN pg_attrdef pd ON pd.adrelid = attrelid AND pd.adnum = attnum
  WHERE STARTS_WITH(pb.pubname, '${pubPrefix}')
  ORDER BY nspname, pc.relname, pb.pubname, attnum;
  `); // Sort by [schema, table, publication, column] to process tables in multiple publications consecutively.

  const publications = v.parse(result[0], publicationsResultSchema);
  const columns = v.parse(result[1], publishedColumnsSchema);

  // For convenience when building the table spec. The returned TableSpec type is readonly.
  type Writeable<T> = {-readonly [P in keyof T]: Writeable<T[P]>};

  const tables: Writeable<FilteredTableSpec>[] = [];
  let table: Writeable<FilteredTableSpec> | undefined;
  let pubname: string | undefined;

  // Check the new table against the last added table (columns are processed in <table, publication> order):
  // 1. to ensure that a table is always published with the same set of columns
  // 2. to collect all filter conditions for which a row may be published
  function addOrCoalesce(t: Writeable<FilteredTableSpec>) {
    const last = tables.at(-1);
    if (t.schema !== last?.schema || t.name !== last?.name) {
      tables.push(t);
      return;
    }
    const lastColumns = new Set(Object.keys(last.columns));
    const nextColumns = new Set(Object.keys(t.columns));
    if (!equals(lastColumns, nextColumns)) {
      throw new Error(
        `Table ${t.name} is exported with different columns: [${[
          ...lastColumns,
        ]}] vs [${[...nextColumns]}]`,
      );
    }
    if (last.filterConditions.length === 0 || t.filterConditions.length === 0) {
      last.filterConditions.splice(0); // unconditional
    } else {
      last.filterConditions.push(...t.filterConditions); // OR all conditions
    }
  }

  columns.forEach(col => {
    if (
      col.schema !== table?.schema ||
      col.table !== table?.name ||
      col.pubname !== pubname
    ) {
      if (table) {
        addOrCoalesce(table);
      }
      // New table
      pubname = col.pubname;
      table = {
        schema: col.schema,
        name: col.table,
        columns: {},
        primaryKey: [],
        filterConditions: col.rowFilter ? [col.rowFilter] : [],
      };
    }

    // https://stackoverflow.com/a/52376230
    const maxLen =
      col.maxLen < 0
        ? null
        : col.typeID === 1043 || col.typeID === 1042
        ? col.maxLen - 4
        : col.maxLen;

    table.columns[col.name] = {
      dataType: col.arrayDims
        ? `${col.type.substring(1)}${'[]'.repeat(col.arrayDims)}`
        : col.type,
      characterMaximumLength: maxLen,
      notNull: col.notNull,
    };
    if (col.keyPos) {
      while (table.primaryKey.length < col.keyPos) {
        table.primaryKey.push('');
      }
      table.primaryKey[col.keyPos - 1] = col.name;
    }
  });

  if (table) {
    addOrCoalesce(table);
  }

  // Sanity check that the primary keys are filled in.
  Object.values(tables).forEach(table => {
    assert(
      table.primaryKey.indexOf('') < 0,
      `Invalid primary key for ${JSON.stringify(table)}`,
    );
  });

  // now go find all the indices for each table
  const indexDefinitions = await sql`SELECT
      pg_indexes.indexname,
      pg_attribute.attname as col,
      pg_constraint.contype as idx_type
    FROM pg_indexes
    JOIN pg_namespace ON pg_indexes.schemaname = pg_namespace.nspname
    JOIN pg_class ON
      pg_class.relname = pg_indexes.indexname
      AND pg_class.relnamespace = pg_namespace.oid
    JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid
    LEFT JOIN pg_constraint ON pg_constraint.conindid = pg_class.oid
    WHERE
      (pg_indexes.schemaname, pg_indexes.tablename) IN (${tables.map(t => [
        t.schema,
        t.name,
      ])})
      AND pg_constraint.contype is distinct from 'p'
      AND pg_constraint.contype is distinct from 'f'
    ORDER BY
      pg_indexes.schemaname,
      pg_indexes.tablename,
      pg_indexes.indexname,
      pg_attribute.attnum ASC;`;

  const indices: IndexSpec[] = [];
  let index: MutableIndexSpec | undefined;
  indexDefinitions.forEach(row => {
    if (
      row.schemaname !== index?.schemaName ||
      row.tablename !== index?.tableName ||
      row.indexname !== index?.name
    ) {
      if (index) {
        indices.push(index);
      }
      index = {
        schemaName: row.schemaname,
        tableName: row.schemaname,
        name: row.indexname,
        unique: row.idx_type === 'u',
        columns: [],
      };
    }
    index!.columns.push(row.col);
  });

  return {
    publications,
    tables,
    indices,
  };
}
