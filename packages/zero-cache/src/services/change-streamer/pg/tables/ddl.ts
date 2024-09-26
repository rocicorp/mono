import {jsonObjectSchema} from 'shared/src/json-schema.js';
import * as v from 'shared/src/valita.js';
import {
  indexDefinitionsQuery,
  publishedTableQuery,
  ZERO_PUB_PREFIX,
} from './published.js';

const createOrAlterTagSchema = v.union(
  v.literal('CREATE TABLE'),
  v.literal('ALTER TABLE'),
  v.literal('CREATE INDEX'),
);

const dropTagSchema = v.union(v.literal('DROP TABLE'), v.literal('DROP INDEX'));

const createOrAlterSchema = v.object({
  tag: createOrAlterTagSchema,
  query: v.string(),
  table: v.array(jsonObjectSchema), // FilteredTableSpec[]
  indexes: v.array(jsonObjectSchema), // IndexSpec[]
  renamedColumns: v.array(v.number()), // 1-indexed column pos
});

const dropSchema = v.object({
  tag: dropTagSchema,
  query: v.string(),
  // TODO: Define schema and collect data from sql_drop() events.
});

export const ddlMessageSchema = v.object({
  type: v.literal('ddl'),
  msg: v.union(createOrAlterSchema, dropSchema),
});

export type DdlMessage = v.Infer<typeof ddlMessageSchema>;

export function replicateCreateOrAlterEvent(pubPrefix: string) {
  return `
CREATE OR REPLACE FUNCTION zero.replicate_create_or_alter_event()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
  tag text;
  query text;
  tables record;
  indexes record;
  renamed_columns json;
  replication_message text;
BEGIN 
  ${publishedTableQuery(
    pubPrefix,
    `JOIN pg_event_trigger_ddl_commands() ddl ON ddl.objid = pc.oid`,
  )} INTO tables;

  ${indexDefinitionsQuery(
    pubPrefix,
    `JOIN pg_event_trigger_ddl_commands() ddl on ddl.objid = pc.oid`,
  )} INTO indexes;
  
  IF json_array_length(tables.tables)   = 0 AND 
     json_array_length(indexes.indexes) = 0
  THEN
    -- not a table published by "pubPrefix"
    RETURN;
  END IF;

  SELECT COALESCE(json_agg(objsubid), '[]'::json)
    FROM pg_event_trigger_ddl_commands()
    WHERE object_type = 'table column'
    INTO renamed_columns;

  SELECT current_query() INTO query;

  SELECT command_tag FROM pg_event_trigger_ddl_commands() INTO tag;

  IF LENGTH(tag) = 0
  THEN
    -- should be impossible
    RAISE EXCEPTION 'missing command_tag from pg_event_trigger_ddl_commands() for %', query;
  END IF;

  SELECT json_build_object(
    'type', 'ddl',
    'msg', json_build_object(
      'tag', tag,
      'query', query,
      'tables', tables.tables,
      'indexes', indexes.indexes,
      'renamedColumns', renamed_columns
    )
  ) INTO replication_message;

  PERFORM pg_logical_emit_message(true, 'zero', replication_message);
END $$
  `;
}

export function createEventTriggerStatements(pubPrefix = ZERO_PUB_PREFIX) {
  return [
    replicateCreateOrAlterEvent(pubPrefix),
    `DROP EVENT TRIGGER IF EXISTS zero_replicate_create_or_alter`,
    `CREATE EVENT TRIGGER zero_replicate_create_or_alter
      ON ddl_command_end
      WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'CREATE INDEX')
      EXECUTE PROCEDURE zero.replicate_create_or_alter_event()`,
  ].join(';\n');
}
