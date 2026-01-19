import postgres from 'postgres';
import type {
  ForeignKeyAction,
  IntrospectedColumn,
  IntrospectedEnum,
  IntrospectedForeignKey,
  IntrospectedSchema,
  IntrospectedTable,
  IntrospectedUniqueConstraint,
  IntrospectOptions,
  PgTypeClass,
} from './types.ts';
import {
  COLUMNS_QUERY,
  ENUMS_QUERY,
  FOREIGN_KEYS_QUERY,
  PRIMARY_KEYS_QUERY,
  UNIQUE_CONSTRAINTS_QUERY,
} from './queries.ts';

/**
 * Introspects a PostgreSQL database schema and returns structured metadata.
 *
 * @param options - Introspection options including connection and filtering
 * @returns The introspected schema with tables, columns, enums, foreign keys, etc.
 */
export async function introspect(
  options: IntrospectOptions,
): Promise<IntrospectedSchema> {
  const {
    connectionString,
    client,
    schema = 'public',
    includeTables,
    excludeTables = [],
  } = options;

  if (!connectionString && !client) {
    throw new Error(
      'Either connectionString or client must be provided to introspect',
    );
  }

  // Create or use provided client
  const sql = client ?? postgres(connectionString!);
  const shouldClose = !client;

  try {
    // Execute all queries in parallel
    const [
      columnsResult,
      primaryKeysResult,
      foreignKeysResult,
      enumsResult,
      uniqueResult,
    ] = await Promise.all([
      sql.unsafe(COLUMNS_QUERY, [schema]),
      sql.unsafe(PRIMARY_KEYS_QUERY, [schema]),
      sql.unsafe(FOREIGN_KEYS_QUERY, [schema]),
      sql.unsafe(ENUMS_QUERY, [schema]),
      sql.unsafe(UNIQUE_CONSTRAINTS_QUERY, [schema]),
    ]);

    // Process results into IR
    const tables = processColumns(
      columnsResult,
      primaryKeysResult,
      includeTables,
      excludeTables,
    );
    const enums = processEnums(enumsResult);
    const foreignKeys = processForeignKeys(
      foreignKeysResult,
      includeTables,
      excludeTables,
    );
    const uniqueConstraints = processUniqueConstraints(
      uniqueResult,
      includeTables,
      excludeTables,
    );

    return {
      schemaName: schema,
      tables,
      enums,
      foreignKeys,
      uniqueConstraints,
    };
  } finally {
    if (shouldClose) {
      await sql.end();
    }
  }
}

function processColumns(
  columnsResult: postgres.RowList<postgres.Row[]>,
  primaryKeysResult: postgres.RowList<postgres.Row[]>,
  includeTables: string[] | undefined,
  excludeTables: string[],
): IntrospectedTable[] {
  // Build primary key lookup: tableName -> columns[]
  const primaryKeyMap = new Map<string, string[]>();
  for (const row of primaryKeysResult) {
    const key = `${row.table_schema}.${row.table_name}`;
    if (!primaryKeyMap.has(key)) {
      primaryKeyMap.set(key, []);
    }
    primaryKeyMap.get(key)!.push(row.column_name as string);
  }

  // Group columns by table
  const tableMap = new Map<string, IntrospectedTable>();

  for (const row of columnsResult) {
    const tableName = row.table_name as string;

    // Apply include/exclude filters
    if (includeTables && !includeTables.includes(tableName)) continue;
    if (excludeTables.includes(tableName)) continue;

    const key = `${row.table_schema}.${tableName}`;

    if (!tableMap.has(key)) {
      tableMap.set(key, {
        schema: row.table_schema as string,
        name: tableName,
        columns: [],
        primaryKey: primaryKeyMap.get(key) ?? [],
      });
    }

    const column: IntrospectedColumn = {
      name: row.column_name as string,
      position: row.ordinal_position as number,
      dataType: row.data_type as string,
      udtName: row.udt_name as string,
      isNullable: row.is_nullable === 'YES',
      characterMaxLength: row.character_maximum_length as number | null,
      numericPrecision: row.numeric_precision as number | null,
      numericScale: row.numeric_scale as number | null,
      defaultValue: row.column_default as string | null,
      pgTypeClass: row.pg_type_class as PgTypeClass,
      isArray: row.is_array as boolean,
      arrayElementTypeClass: row.elem_type_class as PgTypeClass | null,
    };

    tableMap.get(key)!.columns.push(column);
  }

  return Array.from(tableMap.values());
}

function processEnums(
  enumsResult: postgres.RowList<postgres.Row[]>,
): IntrospectedEnum[] {
  return enumsResult.map(row => ({
    schema: row.schema_name as string,
    name: row.enum_name as string,
    values: row.enum_values as string[],
  }));
}

function processForeignKeys(
  foreignKeysResult: postgres.RowList<postgres.Row[]>,
  includeTables: string[] | undefined,
  excludeTables: string[],
): IntrospectedForeignKey[] {
  // Group by constraint name
  const fkMap = new Map<string, IntrospectedForeignKey>();

  for (const row of foreignKeysResult) {
    const sourceTable = row.source_table as string;
    const targetTable = row.target_table as string;

    // Apply filters
    if (includeTables && !includeTables.includes(sourceTable)) continue;
    if (excludeTables.includes(sourceTable)) continue;
    if (includeTables && !includeTables.includes(targetTable)) continue;
    if (excludeTables.includes(targetTable)) continue;

    const key = `${row.source_schema}.${row.constraint_name}`;

    if (!fkMap.has(key)) {
      fkMap.set(key, {
        constraintName: row.constraint_name as string,
        sourceSchema: row.source_schema as string,
        sourceTable,
        sourceColumns: [],
        targetSchema: row.target_schema as string,
        targetTable,
        targetColumns: [],
        onDelete: row.on_delete as ForeignKeyAction,
        onUpdate: row.on_update as ForeignKeyAction,
      });
    }

    const fk = fkMap.get(key)!;
    fk.sourceColumns.push(row.source_column as string);
    fk.targetColumns.push(row.target_column as string);
  }

  return Array.from(fkMap.values());
}

function processUniqueConstraints(
  uniqueResult: postgres.RowList<postgres.Row[]>,
  includeTables: string[] | undefined,
  excludeTables: string[],
): IntrospectedUniqueConstraint[] {
  return uniqueResult
    .filter(row => {
      const tableName = row.table_name as string;
      if (includeTables && !includeTables.includes(tableName)) return false;
      if (excludeTables.includes(tableName)) return false;
      return true;
    })
    .map(row => ({
      constraintName: row.constraint_name as string,
      schema: row.table_schema as string,
      tableName: row.table_name as string,
      columns: row.columns as string[],
      isPrimaryKey: row.is_primary_key as boolean,
    }));
}
