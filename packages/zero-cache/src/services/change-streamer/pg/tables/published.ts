import type postgres from 'postgres';
import {equals} from 'shared/src/set-utils.js';
import * as v from 'shared/src/valita.js';
import type {FilteredTableSpec, IndexSpec} from 'zero-cache/src/types/specs.js';

/** The publication prefix used for tables replicated to zero. */
export const ZERO_PUB_PREFIX = 'zero_';

function publishedTableQuery(pubPrefix = ZERO_PUB_PREFIX) {
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
WHERE STARTS_WITH(pb.pubname, '${pubPrefix}')
ORDER BY nspname, pc.relname)

SELECT json_build_object(
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
      'notNull', "notNull"
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
) AS "table" FROM published_columns GROUP BY "schema", "name";
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
 * Retrieves all tables and columns published under any PUBLICATION
 * whose name starts with the specified `pubPrefix` (e.g. "zero_").
 */
export async function getPublicationInfo(
  sql: postgres.Sql,
  pubPrefix = ZERO_PUB_PREFIX,
): Promise<PublicationInfo> {
  const result = await sql.unsafe(`
  SELECT 
    schemaname AS "schema",
    tablename AS "table", 
    json_object_agg(pubname, attnames) AS "publications"
    FROM pg_publication_tables 
    WHERE STARTS_WITH(pubname, '${pubPrefix}')
    GROUP BY schemaname, tablename;

  SELECT ${Object.keys(publicationSchema.shape).join(',')} FROM pg_publication
    WHERE STARTS_WITH(pubname, '${pubPrefix}')
    ORDER BY pubname;

  ${publishedTableQuery(pubPrefix)};
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

  const publications = v.parse(result[1], publicationsResultSchema);
  const tables = (result[2] as {table: FilteredTableSpec}[]).map(
    ({table}) => table,
  );

  if (tables.length === 0) {
    return {
      publications,
      tables,
      indices: [],
    };
  }

  const indexDefinitionsQuery = sql<{index: IndexSpec}[]>`
  WITH indexed_columns AS (SELECT
      pg_indexes.schemaname as "schemaName",
      pg_indexes.tablename as "tableName",
      pg_indexes.indexname as "name",
      pg_attribute.attname as "col",
      pg_index.indisunique as "unique"
    FROM pg_indexes
    JOIN pg_namespace ON pg_indexes.schemaname = pg_namespace.nspname
    JOIN pg_class ON
      pg_class.relname = pg_indexes.indexname
      AND pg_class.relnamespace = pg_namespace.oid
    JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid
    LEFT JOIN pg_constraint ON pg_constraint.conindid = pg_class.oid
    JOIN pg_index ON pg_index.indexrelid = pg_class.oid
    WHERE
      (pg_indexes.schemaname, pg_indexes.tablename) IN ${sql(
        tables.map(t => sql([t.schema, t.name])),
      )}
      AND pg_constraint.contype is distinct from 'p'
      AND pg_constraint.contype is distinct from 'f'
    ORDER BY
      pg_indexes.schemaname,
      pg_indexes.tablename,
      pg_indexes.indexname,
      pg_attribute.attnum ASC)
  
    SELECT json_build_object(
      'schemaName', "schemaName",
      'tableName', "tableName",
      'name', "name",
      'unique', "unique",
      'columns', json_agg("col")
    ) AS index FROM indexed_columns 
      GROUP BY "schemaName", "tableName", "name", "unique";
  `;

  const indexDefinitions = await indexDefinitionsQuery;
  const indices = indexDefinitions.map(r => r.index);

  return {
    publications,
    tables,
    indices,
  };
}
