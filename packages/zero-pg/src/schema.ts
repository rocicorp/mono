import {assert} from '../../shared/src/asserts.ts';
import type {ServerSchema} from '../../z2s/src/schema.ts';
import {formatPg, sql} from '../../z2s/src/sql.ts';
import {dataTypeToZqlValueType} from '../../zero-cache/src/types/pg.ts';
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
          (t.typtype = 'e')::text AS enum,
          t.typname::text AS enumtype
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

  const errors = checkSchemasAreCompatible(schema, serverSchema);
  assert(errors.length === 0, () => makeSchemaIncompatibleErrorMessage(errors));

  return serverSchema;
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
      const requiredType = dataTypeToZqlValueType(pgType, serverColumn.isEnum);
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
