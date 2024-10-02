import {jsonObjectSchema} from 'shared/src/json-schema.js';
import * as v from 'shared/src/valita.js';
import type {FilteredTableSpec, IndexSpec} from 'zero-cache/src/types/specs.js';
import {indexDefinitionsQuery, publishedTableQuery} from './published.js';

// Sent in the 'version' tag of "ddl" event messages. This is used to ensure
// that the message constructed in the upstream Trigger function is compatible
// with the code processing it in the zero-cache.
//
// Increment this when changing the format of the contents of the "ddl" events.
// This will allow old / incompatible code to detect the change and abort.
export const PROTOCOL_VERSION = 1;

const triggerEvent = v.object({
  context: v.object({query: v.string()}).rest(v.string()),
});

const createOrAlterTableTagSchema = v.union(
  v.literal('CREATE TABLE'),
  v.literal('ALTER TABLE'),
);

const createIndexTagSchema = v.literal('CREATE INDEX');

const createOrAlterTableEventSchema = triggerEvent.extend({
  tag: createOrAlterTableTagSchema,
  table: jsonObjectSchema.map(t => t as FilteredTableSpec), // TODO: Define FilteredTableSpec schema.
  indexes: v.array(jsonObjectSchema.map(t => t as IndexSpec)), // TODO: Define IndexSpec schema.
});

export type CreateOrAlterTableEvent = v.Infer<
  typeof createOrAlterTableEventSchema
>;

const createIndexEventSchema = triggerEvent.extend({
  tag: createIndexTagSchema,
  index: jsonObjectSchema.map(t => t as IndexSpec), // TODO: Define IndexSpec schema.
});

export type CreateIndexEvent = v.Infer<typeof createIndexEventSchema>;

const dropTableTagSchema = v.literal('DROP TABLE');
const dropIndexTagSchema = v.literal('DROP INDEX');

const identifier = v.object({
  schema: v.string(),
  // `object_identity` field, as defined in
  // https://www.postgresql.org/docs/current/functions-event-triggers.html#PG-EVENT-TRIGGER-SQL-DROP-FUNCTIONS
  objectIdentity: v.string(),
});

const dropTableEventSchema = triggerEvent.extend({
  tag: dropTableTagSchema,
  tables: v.array(identifier),
});

const dropIndexEventSchema = triggerEvent.extend({
  tag: dropIndexTagSchema,
  indexes: v.array(identifier),
});

export const ddlEventSchema = v.object({
  type: v.literal('ddl'),
  version: v.literal(PROTOCOL_VERSION),
  event: v.union(
    createOrAlterTableEventSchema,
    createIndexEventSchema,
    dropTableEventSchema,
    dropIndexEventSchema,
  ),
});

export type DdlEvent = v.Infer<typeof ddlEventSchema>;

export const errorEventSchema = v.object({
  type: v.literal('error'),
  event: triggerEvent.extend({message: v.string()}).rest(v.string()),
});

export type ErrorEvent = v.Infer<typeof errorEventSchema>;

const COMMON_TRIGGER_FUNCTIONS = `
CREATE OR REPLACE FUNCTION zero.get_trigger_context()
RETURNS record
AS $$
DECLARE
  result record;
BEGIN
  SELECT current_query() AS "query" into result;
  RETURN result;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION zero.emit_error(message TEXT)
RETURNS void
AS $$
DECLARE
  event text;
BEGIN
  SELECT json_build_object(
    'type', 'error',
    'event', json_build_object(
      'context', zero.get_trigger_context(),
      'message', message
    )
  ) into event;

  PERFORM pg_logical_emit_message(true, 'zero', event);
END
$$ LANGUAGE plpgsql;
`;

export function replicateCreateOrAlterTable() {
  return `
CREATE OR REPLACE FUNCTION zero.replicate_create_or_alter_table()
RETURNS event_trigger
AS $$
DECLARE
  tag text;
  tables record;
  indexes record;
  event text;
BEGIN 
  SELECT command_tag FROM pg_event_trigger_ddl_commands() INTO tag;
  IF LENGTH(tag) = 0  -- should be impossible
  THEN
    RAISE EXCEPTION 'missing command_tag from pg_event_trigger_ddl_commands() for %', query;
  END IF;

  ${publishedTableQuery(
    undefined,
    `JOIN pg_event_trigger_ddl_commands() ddl ON ddl.objid = pc.oid`,
  )} INTO tables;

  IF json_array_length(tables.tables) = 0
  THEN RETURN; END IF;  -- not a table published by "pubPrefix"

  IF json_array_length(tables.tables) > 1
  THEN
    PERFORM zero.emit_error(
      FORMAT('unexpected number of tables for "%s": %s',
        tag, json_array_length(tables.tables))
    );
    RETURN;
  END IF;

  ${indexDefinitionsQuery(
    undefined,
    `JOIN pg_event_trigger_ddl_commands() ddl on ddl.objid = pg_index.indrelid`,
  )} INTO indexes;
  
  SELECT json_build_object(
    'type', 'ddl',
    'version', ${PROTOCOL_VERSION},
    'event', json_build_object(
      'context', zero.get_trigger_context(),
      'tag', tag,
      'table', tables.tables -> 0,
      'indexes', indexes.indexes
    )
  ) INTO event;

  PERFORM pg_logical_emit_message(true, 'zero', event);
END 
$$ LANGUAGE plpgsql;
  `;
}

export function replicateCreateIndex() {
  return `
CREATE OR REPLACE FUNCTION zero.replicate_create_index()
RETURNS event_trigger
AS $$
DECLARE
  indexes record;
  event text;
BEGIN 
  ${indexDefinitionsQuery(
    undefined,
    `JOIN pg_event_trigger_ddl_commands() ddl on ddl.objid = pc.oid`,
  )} INTO indexes;

  IF json_array_length(indexes.indexes) = 0
  THEN RETURN; END IF;  -- not an index published by "pubPrefix"

  IF json_array_length(indexes.indexes) > 1
  THEN
    PERFORM zero.emit_error(
      FORMAT('unexpected number of indexes for "CREATE INDEX": %s', 
        json_array_length(indexes.indexes))
    );
    RETURN;
  END IF;
  
  SELECT json_build_object(
    'type', 'ddl',
    'version', ${PROTOCOL_VERSION},
    'event', json_build_object(
      'context', zero.get_trigger_context(),
      'tag', 'CREATE INDEX',
      'index', indexes.indexes -> 0
    )
  ) INTO event;

  PERFORM pg_logical_emit_message(true, 'zero', event);
END
$$ LANGUAGE plpgsql;
  `;
}

const DROP_EVENT_TRIGGERS = `
CREATE OR REPLACE FUNCTION zero.replicate_drop_event(tag TEXT, type TEXT, dropped TEXT)
RETURNS void
AS $$
DECLARE
  event text;
BEGIN
  SELECT json_build_object(
    'type', 'ddl',
    'version', ${PROTOCOL_VERSION},
    'event', json_build_object(
      'context', zero.get_trigger_context(),
      'tag', tag,
      dropped, json_agg(json_build_object(
        'schema', schema_name,
        'objectIdentity', object_identity
      ))
    )
  ) FROM pg_event_trigger_dropped_objects() 
    WHERE object_type = type
    INTO event;

  PERFORM pg_logical_emit_message(true, 'zero', event);
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION zero.replicate_drop_table()
RETURNS event_trigger
AS $$
BEGIN
  PERFORM zero.replicate_drop_event('DROP TABLE', 'table', 'tables');
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION zero.replicate_drop_index()
RETURNS event_trigger
AS $$
BEGIN
  PERFORM zero.replicate_drop_event('DROP INDEX', 'index', 'indexes');
END
$$ LANGUAGE plpgsql;
`;

export function createEventTriggerStatements() {
  return [
    COMMON_TRIGGER_FUNCTIONS,

    replicateCreateOrAlterTable(),
    `DROP EVENT TRIGGER IF EXISTS zero_replicate_create_or_alter_table`,
    `CREATE EVENT TRIGGER zero_replicate_create_or_alter_table
      ON ddl_command_end
      WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE')
      EXECUTE PROCEDURE zero.replicate_create_or_alter_table()`,

    replicateCreateIndex(),
    `DROP EVENT TRIGGER IF EXISTS zero_replicate_create_index`,
    `CREATE EVENT TRIGGER zero_replicate_create_index
      ON ddl_command_end
      WHEN TAG IN ('CREATE INDEX')
      EXECUTE PROCEDURE zero.replicate_create_index()`,

    DROP_EVENT_TRIGGERS,
    `DROP EVENT TRIGGER IF EXISTS zero_replicate_drop_table`,
    `CREATE EVENT TRIGGER zero_replicate_drop_table
      ON sql_drop
      WHEN TAG IN ('DROP TABLE')
      EXECUTE PROCEDURE zero.replicate_drop_table()`,

    `DROP EVENT TRIGGER IF EXISTS zero_replicate_drop_index`,
    `CREATE EVENT TRIGGER zero_replicate_drop_index
        ON sql_drop
        WHEN TAG IN ('DROP INDEX')
        EXECUTE PROCEDURE zero.replicate_drop_index()`,
  ].join(';\n');
}
