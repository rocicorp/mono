import type postgres from 'postgres';
import {assert} from 'shared/src/asserts.js';
import * as v from 'shared/src/valita.js';
import type {TableSpec} from './specs.js';

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
    default: v.string().nullable(),
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
  publications: Publication[];
  tables: TableSpec[];
};

/**
 * Retrieves all tables and columns published under any PUBLICATION
 * whose name starts with the specified `pubPrefix` (e.g. "zero_").
 */
export async function getPublicationInfo(
  sql: postgres.Sql,
  pubPrefix: string,
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
    pg_get_expr(pd.adbin, pd.adrelid) as default,
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
  ORDER BY nspname, pc.relname, attnum;
  `);

  const publications = v.parse(result[0], publicationsResultSchema);
  const columns = v.parse(result[1], publishedColumnsSchema);
  const tables: TableSpec[] = [];
  let table: TableSpec | undefined;

  columns.forEach(col => {
    if (col.schema !== table?.schema || col.table !== table?.name) {
      // New table
      table = {
        schema: col.schema,
        name: col.table,
        columns: {},
        primaryKey: [],
      };
      tables.push(table);
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
      columnDefault: col.default,
      notNull: col.notNull,
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

  return {
    publications,
    tables,
  };
}
