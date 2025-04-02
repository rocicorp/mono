import type {ServerSchema} from '../../z2s/src/schema.ts';
import {formatPg, sql} from '../../z2s/src/sql.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {DBTransaction} from '../../zql/src/mutate/custom.ts';

export type ServerSchemaRow = {
  schema: string;
  table: string;
  column: string;
  type: string;
  enum: string;
  enumtype: string;
};

export async function getServerSchema<S extends Schema>(
  dbTransaction: DBTransaction<unknown>,
  schema: S,
): Promise<ServerSchema> {
  const schemaTablePairs: [string, string][] = Object.values(schema.tables).map(
    ({name, serverName}) => {
      let schemaTablePair: [string, string] = ['public', serverName ?? name];
      if (serverName) {
        const firstPeriod = serverName.indexOf('.');
        if (firstPeriod > -1) {
          schemaTablePair = [
            serverName.substring(0, firstPeriod),
            serverName.substring(firstPeriod + 1, serverName.length),
          ];
        }
      }
      return schemaTablePair;
    },
  );

  if (schemaTablePairs.length === 0) {
    return {}; // No pairs to query for
  }

  // Cast all inputs to text and all outputs to text to avoid
  // any conversions customer's DBTransaction impl has on other types.
  const inClause = sql.join(
    schemaTablePairs.map(
      ([schema, table]) => sql`(${schema}::text, ${table}::text)`,
    ),
    ',',
  );
  const query = sql`
      SELECT
          c.table_schema::text AS schema,
          c.table_name::text AS table,
          c.column_name::text AS column,
          c.data_type::text AS type,
          (t.typtype = 'e')::text AS enum
      FROM
          information_schema.columns c
      JOIN
          pg_catalog.pg_type t ON c.udt_name = t.typname
      JOIN
          pg_catalog.pg_namespace n ON t.typnamespace = n.oid
      WHERE
          (c.table_schema, c.table_name) IN (${inClause})
    `;
  const {text, values} = formatPg(query);
  const results: Iterable<ServerSchemaRow> = (await dbTransaction.query(
    text,
    values,
  )) as Iterable<ServerSchemaRow>;

  const serverSchema: ServerSchema = {};

  for (const row of results) {
    const tableName =
      row.schema === 'public' ? row.table : `${row.schema}.${row.table}`;
    let tableSchema = serverSchema[tableName];
    if (!tableSchema) {
      tableSchema = {};
      serverSchema[tableName] = tableSchema;
    }
    const isEnum = row.enum.toLowerCase().startsWith('t');
    tableSchema[row.column] = {
      type: isEnum ? row.enumtype : row.type,
      isEnum,
    };
  }

  // Validate that schema.ts is a valid subset of serverSchema and
  // types are compatible.

  return serverSchema;
}
