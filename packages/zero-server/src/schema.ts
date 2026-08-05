import {assert} from '../../shared/src/asserts.ts';
import type {Enum} from '../../shared/src/enum.ts';
import {must} from '../../shared/src/must.ts';
import {formatPg, sql} from '../../z2s/src/sql.ts';
import * as PostgresTypeClass from '../../zero-cache/src/db/postgres-type-class-enum.ts';
import {dataTypeToZqlValueType} from '../../zero-cache/src/types/pg-data-type.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import type {DBTransaction} from '../../zql/src/mutate/custom.ts';

type PostgresTypeClass = Enum<typeof PostgresTypeClass>;

type ServerSchemaRow = {
  schema: string;
  table: string;
  column: string;
  dataType: string;
  length: string | null;
  precision: string | null;
  scale: string | null;
  typtype: PostgresTypeClass;
  typename: string;
  elemTyptype: PostgresTypeClass | null;
  elemTypname: string | null;
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

  const {text, values} = serverSchemaQuery(schemaTablePairs);
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
    const isArray = row.elemTyptype !== null;
    const isEnum = (row.elemTyptype ?? row.typtype) === PostgresTypeClass.Enum;
    let type = row.dataType.toLowerCase();
    if (isArray) {
      type = must(
        row.elemTypname,
        `Array column "${row.column}" in table "${tableName}" is missing element type name`,
      );
    } else if (isEnum) {
      type = row.typename;
    } else if (
      type === 'user-defined' &&
      dataTypeToZqlValueType(row.typename, false, false) !== undefined
    ) {
      type = row.typename;
    }
    if (
      (type === 'bpchar' ||
        type === 'character' ||
        type === 'character varying' ||
        type === 'varchar') &&
      row.length
    ) {
      type = `${type}(${row.length})`;
    }
    if (
      (type === 'numeric' || type === 'decimal') &&
      row.precision !== null &&
      row.scale !== null
    ) {
      type = `${type}(${row.precision}, ${row.scale})`;
    }
    tableSchema[row.column] = {
      type,
      isEnum,
      isArray,
    };
  }

  const errors = checkSchemasAreCompatible(schema, serverSchema);
  assert(errors.length === 0, () => makeSchemaIncompatibleErrorMessage(errors));

  return serverSchema;
}

/**
 * The query behind {@linkcode getServerSchema}, exported so that it can be
 * diffed against the `information_schema` query it replaced.
 */
export function serverSchemaQuery(
  schemaTablePairs: readonly (readonly [string, string])[],
): {text: string; values: unknown[]} {
  // Cast all inputs to text and all outputs to text to avoid
  // any conversions customer's DBTransaction impl has on other types.
  const inClause = sql.join(
    schemaTablePairs.map(
      ([schema, table]) => sql`(${schema}::text, ${table}::text)`,
    ),
    ',',
  );
  // Read pg_catalog directly instead of information_schema.columns. The view
  // derives every column it returns through the information_schema._pg_*
  // functions and a per column privilege check, which is several times slower
  // than the query below. Server processes run this on the first transaction
  // they serve, so serverless deployments pay for it on every cold start.
  //
  // The expressions below reproduce the parts of the view's definition that we
  // select, down to the int4 wire types of length, precision and scale: the
  // _pg_char_max_length / _pg_numeric_precision / _pg_numeric_scale arithmetic,
  // the single level domain unwrap that data_type and udt_name perform (`ut`),
  // and the view's own relkind, attnum and privilege filters. pg_type is joined
  // by OID rather than by udt_name, which additionally avoids duplicate rows
  // when two schemas declare a type with the same name. The types those
  // functions special case are named through regtype, which the parser resolves
  // while planning.
  const query = sql`
      SELECT
          n.nspname::text AS schema,
          cl.relname::text AS table,
          a.attname::text AS column,
          CASE
              WHEN ut.typelem <> 0 AND ut.typlen = -1 THEN 'ARRAY'
              WHEN nut.nspname = 'pg_catalog' THEN pg_catalog.format_type(ut.oid, NULL)
              ELSE 'USER-DEFINED'
          END::text AS "dataType",
          CASE
              WHEN tm.typmod = -1 THEN NULL
              WHEN ut.oid IN ('pg_catalog.bpchar'::regtype, 'pg_catalog.varchar'::regtype) THEN tm.typmod - 4
              WHEN ut.oid IN ('pg_catalog.bit'::regtype, 'pg_catalog.varbit'::regtype) THEN tm.typmod
          END AS length,
          CASE ut.oid
              WHEN 'pg_catalog.int2'::regtype THEN 16
              WHEN 'pg_catalog.int4'::regtype THEN 32
              WHEN 'pg_catalog.int8'::regtype THEN 64
              WHEN 'pg_catalog.float4'::regtype THEN 24
              WHEN 'pg_catalog.float8'::regtype THEN 53
              WHEN 'pg_catalog.numeric'::regtype THEN CASE WHEN tm.typmod = -1 THEN NULL ELSE ((tm.typmod - 4) >> 16) & 65535 END
          END AS precision,
          CASE
              WHEN ut.oid IN ('pg_catalog.int2'::regtype, 'pg_catalog.int4'::regtype, 'pg_catalog.int8'::regtype) THEN 0
              WHEN ut.oid = 'pg_catalog.numeric'::regtype AND tm.typmod <> -1 THEN (tm.typmod - 4) & 65535
          END AS scale,
          ut.typtype::text AS typtype,
          ut.typname::text AS typename,
          CASE WHEN ut.typelem <> 0 THEN et.typtype::text ELSE NULL END AS "elemTyptype",
          CASE WHEN ut.typelem <> 0 THEN et.typname::text ELSE NULL END AS "elemTypname"
      FROM
          pg_catalog.pg_attribute a
      JOIN
          pg_catalog.pg_class cl ON cl.oid = a.attrelid
      JOIN
          pg_catalog.pg_namespace n ON n.oid = cl.relnamespace
      JOIN
          pg_catalog.pg_type t ON t.oid = a.atttypid
      JOIN
          pg_catalog.pg_type ut ON ut.oid = CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE t.oid END
      JOIN
          pg_catalog.pg_namespace nut ON nut.oid = ut.typnamespace
      LEFT JOIN
          pg_catalog.pg_type et ON et.oid = ut.typelem
      CROSS JOIN LATERAL
          (SELECT CASE WHEN t.typtype = 'd' THEN t.typtypmod ELSE a.atttypmod END AS typmod) tm
      WHERE
          a.attnum > 0
          AND NOT a.attisdropped
          AND cl.relkind IN ('r', 'v', 'f', 'p')
          AND NOT pg_catalog.pg_is_other_temp_schema(n.oid)
          AND (pg_catalog.pg_has_role(cl.relowner, 'USAGE')
               OR pg_catalog.has_column_privilege(cl.oid, a.attnum, 'SELECT, INSERT, UPDATE, REFERENCES'))
          AND (n.nspname, cl.relname) IN (${inClause})
    `;
  return formatPg(query);
}

function makeSchemaIncompatibleErrorMessage(
  errors: SchemaIncompatibilityError[],
) {
  if (errors.length === 0) {
    return 'No schema incompatibilities found.';
  }

  const messages: string[] = [];

  for (const error of errors) {
    switch (error.type) {
      case 'missingTable':
        messages.push(
          `Table "${error.table}" is defined in your zero schema but does not exist in the database.`,
        );
        break;
      case 'missingColumn':
        messages.push(
          `Column "${error.column}" in table "${error.table}" is defined in your zero schema but does not exist in the database.`,
        );
        break;
      case 'typeError':
        messages.push(
          `Type mismatch for column "${error.column}" in table "${error.table}": ${error.requiredType === undefined ? `${error.pgType} is currently unsupported in Zero. Please file a bug at https://bugs.rocicorp.dev/` : `${error.pgType} should be mapped to ${error.requiredType} in Zero not ${error.declaredType}.`}`,
        );
        break;
    }
  }

  return [
    'Schema incompatibility detected between your zero schema definition and the database:',
    '',
    ...messages.map(msg => `  - ${msg}`),
    '',
    'Please update your schema definition to match the database or migrate your database to match the schema.',
  ].join('\n');
}

export type SchemaIncompatibilityError =
  | {
      type: 'typeError';
      table: string;
      column: string;
      pgType: string;
      declaredType: string;
      requiredType: string | undefined;
    }
  | {
      type: 'missingColumn';
      table: string;
      column: string;
    }
  | {
      type: 'missingTable';
      table: string;
    };

export function checkSchemasAreCompatible(
  schema: Schema,
  serverSchema: ServerSchema,
): SchemaIncompatibilityError[] {
  const errors: SchemaIncompatibilityError[] = [];
  // Check that all tables in schema exist in serverSchema
  for (const table of Object.values(schema.tables)) {
    const serverTableName = table.serverName ?? table.name;

    if (!serverSchema[serverTableName]) {
      errors.push({
        type: 'missingTable',
        table: serverTableName,
      });
      continue;
    }

    // Check that all columns in the table exist in serverSchema
    for (const [columnName, column] of Object.entries(table.columns)) {
      const serverColumnName = column.serverName ?? columnName;

      if (!serverSchema[serverTableName][serverColumnName]) {
        errors.push({
          type: 'missingColumn',
          table: serverTableName,
          column: serverColumnName,
        });
        continue;
      }

      // Check type compatibility
      const serverColumn = serverSchema[serverTableName][serverColumnName];
      const declaredType = column.type;
      const pgType = serverColumn.type;
      const requiredType = dataTypeToZqlValueType(
        pgType,
        serverColumn.isEnum,
        serverColumn.isArray,
      );
      if (requiredType !== declaredType) {
        errors.push({
          type: 'typeError',
          table: serverTableName,
          column: serverColumnName,
          pgType,
          declaredType,
          requiredType,
        });
      }
    }
  }

  return errors;
}
