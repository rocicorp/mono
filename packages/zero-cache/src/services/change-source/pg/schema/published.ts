import {literal} from 'pg-format';
import type postgres from 'postgres';
import {equals} from '../../../../../../shared/src/set-utils.ts';
import * as v from '../../../../../../shared/src/valita.ts';
import {computeZqlSpecsFromLiteSpecs} from '../../../../db/lite-tables.ts';
import {
  mapPostgresToLite,
  mapPostgresToLiteIndex,
} from '../../../../db/pg-to-lite.ts';
import {publishedIndexSpec, publishedTableSpec} from '../../../../db/specs.ts';
import {liteTableName} from '../../../../types/names.ts';

export function publishedTableQuery(publications: readonly string[]) {
  // Notes:
  // * There's a bug in PG15 in which generated columns are incorrectly
  //   included in pg_publication_tables.attnames, (even though the generated
  //   column values are not be included in the replication stream).
  //   The WHERE condition `attgenerated = ''` fixes this by explicitly excluding
  //   generated columns from the list.
  return /*sql*/ `
WITH published_columns AS (SELECT 
  pc.oid::int8 AS "oid",
  nspname AS "schema",
  pc.relnamespace::int8 AS "schemaOID" ,
  pc.relname AS "name", 
  pc.relreplident AS "replicaIdentity",
  attnum AS "pos", 
  attname AS "col", 
  pt.typname AS "type", 
  atttypid::int8 AS "typeOID", 
  pt.typtype,
  elem_pt.typtype AS "elemTyptype",
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
LEFT JOIN pg_type elem_pt ON elem_pt.oid = pt.typelem
JOIN pg_publication_tables as pb ON 
  pb.schemaname = nspname AND 
  pb.tablename = pc.relname AND
  attname = ANY(pb.attnames)
LEFT JOIN pg_constraint pk ON pk.contype = 'p' AND pk.connamespace = relnamespace AND pk.conrelid = attrelid
LEFT JOIN pg_attrdef pd ON pd.adrelid = attrelid AND pd.adnum = attnum
WHERE pb.pubname IN (${literal(publications)}) AND 
      (current_setting('server_version_num')::int >= 160000 OR attgenerated = '')
ORDER BY nspname, pc.relname),

tables AS (SELECT json_build_object(
  'oid', "oid",
  'schema', "schema", 
  'schemaOID', "schemaOID",
  'name', "name", 
  'replicaIdentity', "replicaIdentity",
  'columns', json_object_agg(
    DISTINCT
    col,
    jsonb_build_object(
      'pos', "pos",
      'dataType', CASE WHEN "arrayDims" = 0 
                       THEN "type" 
                       ELSE substring("type" from 2) || repeat('[]', "arrayDims") END,
      'pgTypeClass', "typtype",
      'elemPgTypeClass', "elemTyptype",
      'typeOID', "typeOID",
      -- https://stackoverflow.com/a/52376230
      'characterMaximumLength', CASE WHEN "typeOID" = 1043 OR "typeOID" = 1042 
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
) AS "table" FROM published_columns GROUP BY "schema", "schemaOID", "name", "oid", "replicaIdentity")

SELECT COALESCE(json_agg("table"), '[]'::json) as "tables" FROM tables
  `;
}

export function indexDefinitionsQuery(publications: readonly string[]) {
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
  // Notes:
  // * The first bit of indoption is 1 for DESC and 0 for ASC:
  //   https://github.com/postgres/postgres/blob/4e1fad37872e49a711adad5d9870516e5c71a375/src/include/catalog/pg_index.h#L89
  // * pg_index.indkey is an int2vector which is 0-based instead of 1-based.
  // * The additional check for attgenerated is required for the aforementioned
  //   (in publishedTableQuery) bug in PG15 in which generated columns are
  //   incorrectly included in pg_publication_tables.attnames
  return /*sql*/ `
  WITH indexed_columns AS (SELECT
      pg_indexes.schemaname as "schema",
      pg_indexes.tablename as "tableName",
      pg_indexes.indexname as "name",
      index_column.name as "col",
      CASE WHEN pg_index.indoption[index_column.pos-1] & 1 = 1 THEN 'DESC' ELSE 'ASC' END as "dir",
      pg_index.indisunique as "unique",
      pg_index.indisprimary as "isPrimaryKey",
      pg_index.indisreplident as "isReplicaIdentity",
      pg_index.indimmediate as "isImmediate"
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
      SELECT array_agg(attname) as attnames, array_agg(attgenerated != '') as generated FROM pg_attribute
        WHERE attrelid = pg_index.indrelid
          AND attnum = ANY( (pg_index.indkey::smallint[] )[:pg_index.indnkeyatts - 1] )
    ) as indexed ON true
    JOIN LATERAL (
      SELECT pg_attribute.attname as name, col.index_pos as pos
        FROM UNNEST( (pg_index.indkey::smallint[])[:pg_index.indnkeyatts - 1] ) 
          WITH ORDINALITY as col(table_pos, index_pos)
        JOIN pg_attribute ON attrelid = pg_index.indrelid AND attnum = col.table_pos
    ) AS index_column ON true
    LEFT JOIN pg_constraint ON pg_constraint.conindid = pc.oid
    WHERE pb.pubname IN (${literal(publications)})
      AND pg_index.indexprs IS NULL
      AND pg_index.indpred IS NULL
      AND (pg_constraint.contype IS NULL OR pg_constraint.contype IN ('p', 'u'))
      AND indexed.attnames <@ pb.attnames
      AND (current_setting('server_version_num')::int >= 160000 OR false = ALL(indexed.generated))
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
      'isPrimaryKey', "isPrimaryKey",
      'isReplicaIdentity', "isReplicaIdentity",
      'isImmediate', "isImmediate",
      'columns', json_object_agg("col", "dir")
    ) AS index FROM indexed_columns 
      GROUP BY "schema", "tableName", "name", "unique", 
         "isPrimaryKey", "isReplicaIdentity", "isImmediate")

    SELECT COALESCE(json_agg("index"), '[]'::json) as "indexes" FROM indexes
  `;
}

export const publishedSchema = v
  .object({
    tables: v.array(publishedTableSpec),
    indexes: v.array(publishedIndexSpec),
  })
  .map(({tables, indexes}) => {
    const zqlSpecs = computeZqlSpecsFromLiteSpecs(
      tables.map(t => mapPostgresToLite(t)),
      indexes.map(mapPostgresToLiteIndex),
    );
    return {
      indexes,

      // Denormalize the schema such that each `table` includes the
      // `replicaIdentityColumns` corresponding to the table's
      // replica identity and associated primary key or index.
      tables: tables.map(table => {
        const replicaIdentityColumns: string[] = [];
        switch (table.replicaIdentity) {
          case 'd':
            replicaIdentityColumns.push(...(table.primaryKey ?? []));
            break;
          case 'i':
            replicaIdentityColumns.push(
              ...Object.keys(
                indexes.find(
                  ind =>
                    ind.schema === table.schema &&
                    ind.tableName === table.name &&
                    ind.isReplicaIdentity,
                )?.columns ?? {},
              ),
            );
            break;
          case 'f': {
            // For the key columns of replica identity "full", use the columns
            // that the replicator/change-processor will end up using
            // (in #getKey()) as the row key.
            const zqlTable = zqlSpecs.get(liteTableName(table));
            // Note: There zql spec will be absent if the table is not synced,
            //       e.g. if it has no suitable unique indexes.
            if (zqlTable) {
              replicaIdentityColumns.push(...zqlTable.tableSpec.primaryKey);
            }
            break;
          }
        }
        return {
          ...table,
          replicaIdentityColumns,
        };
      }),
    };
  });

export type PublishedSchema = v.Infer<typeof publishedSchema>;

export type PublishedTableWithReplicaIdentity =
  PublishedSchema['tables'][number];

const publicationSchema = v.object({
  pubname: v.string(),
  pubinsert: v.boolean(),
  pubupdate: v.boolean(),
  pubdelete: v.boolean(),
  pubtruncate: v.boolean(),
});

const publicationsResultSchema = v.array(publicationSchema);

export type PublicationInfo = PublishedSchema & {
  publications: v.Infer<typeof publicationsResultSchema>;
};

/**
 * Retrieves published tables and columns.
 */
export async function getPublicationInfo(
  sql: postgres.Sql,
  publications: string[],
): Promise<PublicationInfo> {
  const result = await sql.unsafe(/*sql*/ `
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
    ...v.parse(
      {
        ...result[2][0], // tables
        ...result[3][0], // indexes
      },
      publishedSchema,
    ),
  };
}
