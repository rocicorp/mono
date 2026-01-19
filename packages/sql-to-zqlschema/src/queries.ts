/**
 * Query to get all tables and columns in a schema.
 * Joins information_schema.columns with pg_type for type class info.
 * Excludes _zero* tables (Zero internal tables).
 */
export const COLUMNS_QUERY = `
SELECT
  c.table_schema,
  c.table_name,
  c.column_name,
  c.ordinal_position,
  c.data_type,
  c.udt_name,
  c.is_nullable,
  c.character_maximum_length,
  c.numeric_precision,
  c.numeric_scale,
  c.column_default,
  pt.typtype AS pg_type_class,
  pt.typelem != 0 AS is_array,
  elem_pt.typtype AS elem_type_class
FROM information_schema.columns c
JOIN pg_catalog.pg_type pt ON pt.typname = c.udt_name
JOIN pg_catalog.pg_namespace pn ON pn.oid = pt.typnamespace
LEFT JOIN pg_catalog.pg_type elem_pt ON elem_pt.oid = pt.typelem
WHERE c.table_schema = $1
  AND c.table_name NOT LIKE '\\_zero%' ESCAPE '\\'
  AND pn.nspname = c.udt_schema
ORDER BY c.table_name, c.ordinal_position
`;

/**
 * Query to get primary keys for all tables in a schema.
 * Returns columns in ordinal_position order.
 */
export const PRIMARY_KEYS_QUERY = `
SELECT
  tc.table_schema,
  tc.table_name,
  kcu.column_name,
  kcu.ordinal_position
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
  AND tc.table_name = kcu.table_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = $1
ORDER BY tc.table_name, kcu.ordinal_position
`;

/**
 * Query to get foreign keys with ON DELETE/UPDATE actions.
 * Handles composite foreign keys by ordering by ordinal_position.
 */
export const FOREIGN_KEYS_QUERY = `
SELECT
  tc.constraint_name,
  tc.table_schema AS source_schema,
  tc.table_name AS source_table,
  kcu.column_name AS source_column,
  kcu.ordinal_position,
  ccu.table_schema AS target_schema,
  ccu.table_name AS target_table,
  ccu.column_name AS target_column,
  rc.delete_rule AS on_delete,
  rc.update_rule AS on_update
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
JOIN information_schema.referential_constraints rc
  ON tc.constraint_name = rc.constraint_name
  AND tc.table_schema = rc.constraint_schema
JOIN information_schema.constraint_column_usage ccu
  ON rc.unique_constraint_name = ccu.constraint_name
  AND rc.unique_constraint_schema = ccu.constraint_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = $1
ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
`;

/**
 * Query to get custom enum types and their values.
 * Returns values in enum sort order.
 */
export const ENUMS_QUERY = `
SELECT
  n.nspname AS schema_name,
  t.typname AS enum_name,
  array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
FROM pg_catalog.pg_type t
JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid
JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
WHERE n.nspname = $1
GROUP BY n.nspname, t.typname
ORDER BY t.typname
`;

/**
 * Query to get unique constraints (including primary keys).
 * Used for relationship inference to determine one-to-one relationships.
 */
export const UNIQUE_CONSTRAINTS_QUERY = `
SELECT
  tc.constraint_name,
  tc.table_schema,
  tc.table_name,
  tc.constraint_type = 'PRIMARY KEY' AS is_primary_key,
  array_agg(kcu.column_name ORDER BY kcu.ordinal_position) AS columns
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
  AND tc.table_schema = $1
GROUP BY tc.constraint_name, tc.table_schema, tc.table_name, tc.constraint_type
ORDER BY tc.table_name, tc.constraint_name
`;
