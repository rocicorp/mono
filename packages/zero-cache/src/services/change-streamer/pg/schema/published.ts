import {literal} from 'pg-format';
import type postgres from 'postgres';
import {equals} from '../../../../../../shared/src/set-utils.js';
import * as v from '../../../../../../shared/src/valita.js';
import type {FilteredTableSpec, IndexSpec} from '../../../../db/specs.js';

type PublishedTableQueryResult = {
  tables: FilteredTableSpec[];
};

export function publishedTableQuery(publications: string[]) {
  return `
WITH published_columns AS (SELECT 
  nspname AS "schema", 
  pc.relname AS "name", 
  attnum AS "pos", 
  attname AS "col", 
  pt.typname AS "type", 
  atttypid AS "typeID", 
  NULLIF(atttypmod, -1) AS "maxLen", 
  attndims "arrayDims", 
  attnotnull AS "notNull",
  pg_get_expr(pd.adbin, pd.adrelid) as "dflt",
  NULLIF(ARRAY_POSITION(conkey, attnum), -1) AS "keyPos", 
  pb.rowfilter as "rowFilter",
  pb.pubname as "publication"
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
WHERE pb.pubname IN (${literal(publications)})
ORDER BY nspname, pc.relname),

tables AS (SELECT json_build_object(
  'schema', "schema", 
  'name', "name", 
  'columns', json_object_agg(
    DISTINCT
    col,
    jsonb_build_object(
      'pos', "pos",
      'dataType', CASE WHEN "arrayDims" = 0 
                       THEN "type" 
                       ELSE substring("type" from 2) || repeat('[]', "arrayDims") END,
      -- https://stackoverflow.com/a/52376230
      'characterMaximumLength', CASE WHEN "typeID" = 1043 OR "typeID" = 1042 
                                     THEN "maxLen" - 4 
                                     ELSE "maxLen" END,
      'notNull', "notNull",
      'dflt', "dflt"
    )
  ),
  'primaryKey', ARRAY( SELECT json_object_keys(
    json_strip_nulls(
      json_object_agg(
        DISTINCT "col", "keyPos" ORDER BY "keyPos"
      )
    )
  )),
  'publications', json_object_agg(
    DISTINCT 
    "publication", 
    jsonb_build_object('rowFilter', "rowFilter")
  )
) AS "table" FROM published_columns GROUP BY "schema", "name")

SELECT COALESCE(json_agg("table"), '[]'::json) as "tables" FROM tables
  `;
}

type IndexDefinitionsQueryResult = {
  indexes: IndexSpec[];
};

export function indexDefinitionsQuery(publications: string[]) {
  // Note: pg_attribute contains column names for tables and for indexes.
  // However, the latter does not get updated when a column in a table is
  // renamed.
  //
  // https://www.postgresql.org/message-id/5860814f-c91d-4ab0-b771-ded90d7b9c55%40www.fastmail.com
  //
  // To address this, the pg_attribute rows are looked up for the index's
  // table rather than the index itself, using the pg_index.indkey array
  // to determine the set and order of columns to include.
  //
  // Note: The first bit of indoption is 1 for DESC and 0 for ASC:
  // https://github.com/postgres/postgres/blob/4e1fad37872e49a711adad5d9870516e5c71a375/src/include/catalog/pg_index.h#L89
  return `
  WITH indexed_columns AS (SELECT
      pg_indexes.schemaname as "schema",
      pg_indexes.tablename as "tableName",
      pg_indexes.indexname as "name",
      index_column.name as "col",
      CASE WHEN pg_index.indoption[index_column.pos-1] & 1 = 1 THEN 'DESC' ELSE 'ASC' END as "dir",
      pg_index.indisunique as "unique"
    FROM pg_indexes
    JOIN pg_namespace ON pg_indexes.schemaname = pg_namespace.nspname
    JOIN pg_class pc ON
      pc.relname = pg_indexes.indexname
      AND pc.relnamespace = pg_namespace.oid
    JOIN pg_publication_tables as pb ON 
      pb.schemaname = pg_indexes.schemaname AND 
      pb.tablename = pg_indexes.tablename
    JOIN pg_index ON pg_index.indexrelid = pc.oid
    JOIN LATERAL (
      SELECT pg_attribute.attname as name, col.index_pos as pos
        FROM UNNEST(pg_index.indkey) WITH ORDINALITY as col(table_pos, index_pos)
        JOIN pg_attribute ON attrelid = pg_index.indrelid AND attnum = col.table_pos
    ) AS index_column ON true
    LEFT JOIN pg_constraint ON pg_constraint.conindid = pc.oid
    WHERE pb.pubname IN (${literal(publications)})
      AND pg_constraint.contype is distinct from 'p'
      AND pg_constraint.contype is distinct from 'f'
    ORDER BY
      pg_indexes.schemaname,
      pg_indexes.tablename,
      pg_indexes.indexname,
      index_column.pos ASC),
  
    indexes AS (SELECT json_build_object(
      'schema', "schema",
      'tableName', "tableName",
      'name', "name",
      'unique', "unique",
      'columns', json_object_agg(DISTINCT "col", "dir")
    ) AS index FROM indexed_columns 
      GROUP BY "schema", "tableName", "name", "unique")

    SELECT COALESCE(json_agg("index"), '[]'::json) as "indexes" FROM indexes
  `;
}

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

/**
 * Retrieves published tables and columns. By default, includes all
 * publications that start with "zero_" or "_zero_", but this can be
 * overridden by specifying a specific set of `publications`.
 */
export async function getPublicationInfo(
  sql: postgres.Sql,
  publications: string[],
): Promise<PublicationInfo> {
  const result = await sql.unsafe(`
  SELECT 
    schemaname AS "schema",
    tablename AS "table", 
    json_object_agg(pubname, attnames) AS "publications"
    FROM pg_publication_tables pb
    WHERE pb.pubname IN (${literal(publications)})
    GROUP BY schemaname, tablename;

  SELECT ${Object.keys(publicationSchema.shape).join(
    ',',
  )} FROM pg_publication pb
    WHERE pb.pubname IN (${literal(publications)})
    ORDER BY pubname;

  ${publishedTableQuery(publications)};

  ${indexDefinitionsQuery(publications)};
`);

  // The first query is used to check that tables in multiple publications
  // always publish the same set of columns.
  const publishedColumns = result[0] as {
    schema: string;
    table: string;
    publications: Record<string, string[]>;
  }[];
  for (const {table, publications} of publishedColumns) {
    let expected: Set<string>;
    Object.entries(publications).forEach(([_, columns], i) => {
      const cols = new Set(columns);
      if (i === 0) {
        expected = cols;
      } else if (!equals(expected, cols)) {
        throw new Error(
          `Table ${table} is exported with different columns: [${[
            ...expected,
          ]}] vs [${[...cols]}]`,
        );
      }
    });
  }

  return {
    publications: v.parse(result[1], publicationsResultSchema),
    tables: (result[2] as PublishedTableQueryResult[])[0].tables,
    indices: (result[3] as IndexDefinitionsQueryResult[])[0].indexes,
  };
}
