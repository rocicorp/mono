import {ident as id, literal as lit} from 'pg-format';
import * as v from '../../../../../../shared/src/valita.js';
import {filteredTableSpec, indexSpec} from '../../../../db/specs.js';
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
  table: filteredTableSpec,
  indexes: v.array(indexSpec),
});

export type CreateOrAlterTableEvent = v.Infer<
  typeof createOrAlterTableEventSchema
>;

const createIndexEventSchema = triggerEvent.extend({
  tag: createIndexTagSchema,
  index: indexSpec,
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

const alterPublicationTagSchema = v.literal('ALTER PUBLICATION');

const alterPublicationEventSchema = triggerEvent.extend({
  tag: alterPublicationTagSchema,
  publication: v.string(),
  tables: v.array(filteredTableSpec),
  indexes: v.array(indexSpec),
});

export const ddlEventSchema = v.object({
  type: v.literal('ddl'),
  version: v.literal(PROTOCOL_VERSION),
  event: v.union(
    createOrAlterTableEventSchema,
    createIndexEventSchema,
    dropTableEventSchema,
    dropIndexEventSchema,
    alterPublicationEventSchema,
  ),
});

export type DdlEvent = v.Infer<typeof ddlEventSchema>;

export const errorEventSchema = v.object({
  type: v.literal('error'),
  event: triggerEvent.extend({message: v.string()}).rest(v.string()),
});

export type ErrorEvent = v.Infer<typeof errorEventSchema>;

const GLOBAL_TRIGGER_FUNCTIONS = `
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

CREATE OR REPLACE FUNCTION zero.emit_error(shardID TEXT, message TEXT)
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

  PERFORM pg_logical_emit_message(true, 'zero/' || shardID, event);
END
$$ LANGUAGE plpgsql;
`;

function append(shardID: string) {
  return (name: string) => id(name + '_' + shardID);
}

export function replicateCreateOrAlterTable(
  shardID: string,
  publications: string[],
) {
  const sharded = append(shardID);
  return `
CREATE OR REPLACE FUNCTION zero.${sharded('replicate_create_or_alter_table')}()
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
    publications,
    `JOIN pg_event_trigger_ddl_commands() ddl ON ddl.objid = pc.oid`,
  )} INTO tables;

  IF json_array_length(tables.tables) = 0
  THEN RETURN; END IF;  -- not a table published by "pubPrefix"

  IF json_array_length(tables.tables) > 1
  THEN
    PERFORM zero.emit_error(
      ${lit(shardID)},
      FORMAT('unexpected number of tables for "%s": %s',
        tag, json_array_length(tables.tables))
    );
    RETURN;
  END IF;

  ${indexDefinitionsQuery(
    publications,
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

  PERFORM pg_logical_emit_message(true, ${lit('zero/' + shardID)}, event);
END 
$$ LANGUAGE plpgsql;
  `;
}

export function replicateCreateIndex(shardID: string, publications: string[]) {
  const sharded = append(shardID);
  return `
CREATE OR REPLACE FUNCTION zero.${sharded('replicate_create_index')}()
RETURNS event_trigger
AS $$
DECLARE
  indexes record;
  event text;
BEGIN 
  ${indexDefinitionsQuery(
    publications,
    `JOIN pg_event_trigger_ddl_commands() ddl on ddl.objid = pc.oid`,
  )} INTO indexes;

  IF json_array_length(indexes.indexes) = 0
  THEN RETURN; END IF;  -- not an index published by "pubPrefix"

  IF json_array_length(indexes.indexes) > 1
  THEN
    PERFORM zero.emit_error(
      ${lit(shardID)},
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

  PERFORM pg_logical_emit_message(true, ${lit('zero/' + shardID)}, event);
END
$$ LANGUAGE plpgsql;
  `;
}

export function replicateAlterPublication(
  shardID: string,
  publications: string[],
) {
  const sharded = append(shardID);
  return `
CREATE OR REPLACE FUNCTION zero.${sharded('replicate_alter_publication')}()
RETURNS event_trigger
AS $$
DECLARE
  r record;
  pub text;
BEGIN 
  SELECT objid, object_type, object_identity FROM pg_event_trigger_ddl_commands() INTO r;

  IF r.objid IS NULL THEN RETURN; END IF;  -- e.g. DROP alteration

  IF r.object_type = 'publication relation' THEN
    SELECT pub.pubname FROM pg_publication_rel AS rel
      JOIN pg_publication AS pub ON pub.oid = rel.prpubid
      WHERE rel.oid = r.objid INTO pub;

  ELSIF r.object_type = 'publication namespace' THEN
    SELECT pub.pubname FROM pg_publication_namespace AS ns
      JOIN pg_publication AS pub ON pub.oid = ns.pnpubid
      WHERE ns.oid = r.objid INTO pub;
  END IF;

  IF pub IN (${lit(publications)})
  THEN
    PERFORM zero.${sharded('emit_all_publications')}(pub);
  ELSE
    RAISE NOTICE ${lit('zero(' + shardID + ') ignoring %')}, r.object_identity;
  END IF;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION zero.${sharded('replicate_alter_publication_drop')}()
RETURNS event_trigger
AS $$
DECLARE
  object_id text;
  pub text;
BEGIN 
  SELECT object_identity FROM pg_event_trigger_dropped_objects() INTO object_id;

  SELECT SPLIT_PART(object_id, ' in publication ', 2) INTO pub;

  IF pub IN (${lit(publications)})
  THEN
    PERFORM zero.${sharded('emit_all_publications')}(pub);
  END IF;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION zero.${sharded('emit_all_publications')}(pub TEXT)
RETURNS void
AS $$
DECLARE
  tables record;
  indexes record;
  event text;
BEGIN
  ${publishedTableQuery(publications)} INTO tables;

  ${indexDefinitionsQuery(publications)} INTO indexes;
  
  SELECT json_build_object(
    'type', 'ddl',
    'version', ${PROTOCOL_VERSION},
    'event', json_build_object(
      'context', zero.get_trigger_context(),
      'tag', 'ALTER PUBLICATION',
      'publication', pub,
      'tables', tables.tables,
      'indexes', indexes.indexes
    )
  ) INTO event;

  PERFORM pg_logical_emit_message(true, ${lit('zero/' + shardID)}, event);
END 
$$ LANGUAGE plpgsql;
  `;
}

export function replicateDropEvents(shardID: string) {
  const sharded = append(shardID);
  return `
CREATE OR REPLACE FUNCTION zero.${sharded('replicate_drop_event')}(
  tag TEXT, type TEXT, dropped TEXT)
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

  PERFORM pg_logical_emit_message(true, ${lit('zero/' + shardID)}, event);
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION zero.${sharded('replicate_drop_table')}()
RETURNS event_trigger
AS $$
BEGIN
  PERFORM zero.${sharded('replicate_drop_event')}(
    'DROP TABLE', 'table', 'tables');
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION zero.${sharded('replicate_drop_index')}()
RETURNS event_trigger
AS $$
BEGIN
  PERFORM zero.${sharded('replicate_drop_event')}(
    'DROP INDEX', 'index', 'indexes');
END
$$ LANGUAGE plpgsql;
`;
}

export function createEventTriggerStatements(
  shardID: string,
  publications: string[],
) {
  const sharded = append(shardID);
  return [
    GLOBAL_TRIGGER_FUNCTIONS,

    replicateCreateOrAlterTable(shardID, publications),
    `DROP EVENT TRIGGER IF EXISTS ${sharded(
      'zero_replicate_create_or_alter_table',
    )}`,
    `CREATE EVENT TRIGGER ${sharded('zero_replicate_create_or_alter_table')}
      ON ddl_command_end
      WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE')
      EXECUTE PROCEDURE zero.${sharded('replicate_create_or_alter_table')}()`,

    replicateCreateIndex(shardID, publications),
    `DROP EVENT TRIGGER IF EXISTS ${sharded('zero_replicate_create_index')}`,
    `CREATE EVENT TRIGGER ${sharded('zero_replicate_create_index')}
      ON ddl_command_end
      WHEN TAG IN ('CREATE INDEX')
      EXECUTE PROCEDURE zero.${sharded('replicate_create_index')}()`,

    replicateAlterPublication(shardID, publications),
    `DROP EVENT TRIGGER IF EXISTS ${sharded(
      'zero_replicate_alter_publication',
    )}`,
    `CREATE EVENT TRIGGER ${sharded('zero_replicate_alter_publication')}
      ON ddl_command_end
      WHEN TAG IN ('ALTER PUBLICATION')
      EXECUTE PROCEDURE zero.${sharded('replicate_alter_publication')}()`,

    `DROP EVENT TRIGGER IF EXISTS ${sharded(
      'zero_replicate_alter_publication_drop',
    )}`,
    `CREATE EVENT TRIGGER ${sharded('zero_replicate_alter_publication_drop')}
      ON sql_drop
      WHEN TAG IN ('ALTER PUBLICATION')
      EXECUTE PROCEDURE zero.${sharded('replicate_alter_publication_drop')}()`,

    replicateDropEvents(shardID),
    `DROP EVENT TRIGGER IF EXISTS ${sharded('zero_replicate_drop_table')}`,
    `CREATE EVENT TRIGGER ${sharded('zero_replicate_drop_table')}
      ON sql_drop
      WHEN TAG IN ('DROP TABLE')
      EXECUTE PROCEDURE zero.${sharded('replicate_drop_table')}()`,

    `DROP EVENT TRIGGER IF EXISTS ${sharded('zero_replicate_drop_index')}`,
    `CREATE EVENT TRIGGER ${sharded('zero_replicate_drop_index')}
        ON sql_drop
        WHEN TAG IN ('DROP INDEX')
        EXECUTE PROCEDURE zero.${sharded('replicate_drop_index')}()`,
  ].join(';\n');
}
