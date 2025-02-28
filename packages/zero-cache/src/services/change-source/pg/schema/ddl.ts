import {literal as lit} from 'pg-format';
import * as v from '../../../../../../shared/src/valita.ts';
import {upstreamSchema, type ShardConfig} from '../../../../types/shards.ts';
import {id} from '../../../../types/sql.ts';
import {
  indexDefinitionsQuery,
  publishedSchema,
  publishedTableQuery,
} from './published.ts';

// Sent in the 'version' tag of "ddlStart" and "ddlUpdate" event messages.
// This is used to ensure that the message constructed in the upstream
// Trigger function is compatible with the code processing it in the zero-cache.
//
// Increment this when changing the format of the contents of the "ddl" events.
// This will allow old / incompatible code to detect the change and abort.
export const PROTOCOL_VERSION = 1;

const triggerEvent = v.object({
  context: v.object({query: v.string()}).rest(v.string()),
});

// All DDL events contain a snapshot of the current tables and indexes that
// are published / relevant to the shard.
export const ddlEventSchema = triggerEvent.extend({
  version: v.literal(PROTOCOL_VERSION),
  schema: publishedSchema,
});

// The `ddlStart` message is computed before every DDL event, regardless of
// whether the subsequent event affects the shard. Downstream processing should
// capture the contained schema information in order to determine the schema
// changes necessary to apply a subsequent `ddlUpdate` message. Note that a
// `ddlUpdate` message may not follow, as updates determined to be irrelevant
// to the shard will not result in a message. However, all `ddlUpdate` messages
// are guaranteed to be preceded by a `ddlStart` message.
export const ddlStartEventSchema = ddlEventSchema.extend({
  type: v.literal('ddlStart'),
});

export type DdlStartEvent = v.Infer<typeof ddlStartEventSchema>;

/**
 * An tableEvent indicates the table that was created or altered. Note that
 * this may result in changes to indexes.
 *
 * Note that a table alteration can only consistent of a single change, whether
 * that be the name of the table, or an aspect of a single column. In particular,
 * if the update contains a new column and removes an old column, that must
 * necessarily mean that the old column was renamed.
 *
 * tableEvents are only emitted for tables published to the shard.
 */
const tableEvent = v.object({
  tag: v.union(v.literal('CREATE TABLE'), v.literal('ALTER TABLE')),
  table: v.object({schema: v.string(), name: v.string()}),
});

/**
 * An indexEvent indicates an index that was manually created.
 *
 * indexEvents are only emitted for (indexes of) tables published to the shard.
 */
const indexEvent = v.object({
  tag: v.literal('CREATE INDEX'),
  index: v.object({schema: v.string(), name: v.string()}),
});

/**
 * A drop event indicates the dropping of table(s) or index(es). Note
 * that a `DROP TABLE` event can result in the dropping of both tables
 * and their indexes.
 *
 * Drop events are emitted to all shards. It is up to the downstream
 * processor to determine (from the preceding {@link DdlStartEvent})
 * whether the dropped objects are relevant to the shard.
 */
const dropEvent = v.object({
  tag: v.union(v.literal('DROP TABLE'), v.literal('DROP INDEX')),
});

/**
 * A publication event indicates a change in the visibility of tables
 * and/or columns. This can mean any number of columns added or
 * removed. However, it will never represent table or column renames
 * (as those are only possible with `ALTER TABLE` statements).
 *
 * Publication events are not emitted if the altered publication is
 * known and not included by the shard. However, due to the way
 * Postgres Triggers work, the publication is not always known, and
 * so it is possible for a shard to receive irrelevant publication
 * events.
 */
const publicationEvent = v.object({
  tag: v.literal('ALTER PUBLICATION'),
});

/**
 * The {@link DdlUpdateEvent} contains an updated schema resulting from
 * a particular ddl event. The event type provides information
 * (i.e. constraints) on the difference from the schema of the preceding
 * {@link DdlStartEvent}.
 *
 * Note that in almost all cases (the exception being `CREATE` events),
 * it is possible that there is no relevant difference between the
 * ddl-start schema and the ddl-update schema, as many aspects of the
 * schema (e.g. column constraints) are not relevant to downstream
 * replication.
 */
export const ddlUpdateEventSchema = ddlEventSchema.extend({
  type: v.literal('ddlUpdate'),
  event: v.union(tableEvent, indexEvent, dropEvent, publicationEvent),
});

export type DdlUpdateEvent = v.Infer<typeof ddlUpdateEventSchema>;

export const replicationEventSchema = v.union(
  ddlStartEventSchema,
  ddlUpdateEventSchema,
);

export type ReplicationEvent = v.Infer<typeof replicationEventSchema>;

// Creates a function that appends `_{shard-num}` to the input and
// quotes the result to be a valid identifier.
function append(shardNum: number) {
  return (name: string) => id(name + '_' + String(shardNum));
}

/**
 * Event trigger functions contain the core logic that are invoked by triggers.
 *
 * Note that although many of these functions can theoretically be parameterized and
 * shared across shards, it is advantageous to keep the functions in each shard
 * isolated from each other in order to avoid the complexity of shared-function
 * versioning.
 *
 * In a sense, shards (and their triggers and functions) should be thought of as
 * execution environments that can be updated at different schedules. If per-shard
 * triggers called into shared functions, we would have to consider versioning the
 * functions when changing their behavior, backwards compatibility, removal of
 * unused versions, etc. (not unlike versioning of npm packages).
 *
 * Instead, we opt for the simplicity and isolation of having each shard
 * completely own (and maintain) the entirety of its trigger/function stack.
 */
function createEventFunctionStatements(shard: ShardConfig) {
  const {appID, shardNum, publications} = shard;
  const schema = id(upstreamSchema(shard)); // e.g. "{APP_ID}_{SHARD_ID}"
  return `
CREATE SCHEMA IF NOT EXISTS ${schema};

CREATE OR REPLACE FUNCTION ${schema}.get_trigger_context()
RETURNS record AS $$
DECLARE
  result record;
BEGIN
  SELECT current_query() AS "query" into result;
  RETURN result;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${schema}.notice_ignore(object_id TEXT)
RETURNS void AS $$
BEGIN
  RAISE NOTICE 'zero(%) ignoring %', ${lit(shardNum)}, object_id;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${schema}.schema_specs()
RETURNS TEXT AS $$
DECLARE
  tables record;
  indexes record;
BEGIN
  ${publishedTableQuery(publications)} INTO tables;
  ${indexDefinitionsQuery(publications)} INTO indexes;
  RETURN json_build_object(
    'tables', tables.tables,
    'indexes', indexes.indexes
  );
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${schema}.emit_ddl_start()
RETURNS event_trigger AS $$
DECLARE
  schema_specs TEXT;
  message TEXT;
BEGIN
  SELECT ${schema}.schema_specs() INTO schema_specs;

  SELECT json_build_object(
    'type', 'ddlStart',
    'version', ${PROTOCOL_VERSION},
    'schema', schema_specs::json,
    'context', ${schema}.get_trigger_context()
  ) INTO message;

  PERFORM pg_logical_emit_message(true, ${lit(
    `${appID}/${shardNum}`,
  )}, message);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${schema}.emit_ddl_end(tag TEXT)
RETURNS void AS $$
DECLARE
  publications TEXT[];
  cmd RECORD;
  target RECORD;
  pub RECORD;
  in_shard RECORD;
  schema_specs TEXT;
  message TEXT;
  event TEXT;
BEGIN
  publications := ARRAY[${lit(publications)}];

  SELECT objid, object_type, object_identity 
    FROM pg_event_trigger_ddl_commands() 
    WHERE object_type IN (
      'table',
      'table column',
      'index',
      'publication relation',
      'publication namespace')
    LIMIT 1 INTO cmd;

  -- Filter DDL updates that are not relevant to the shard (i.e. publications) when possible.

  IF cmd.object_type = 'table' OR cmd.object_type = 'table column' THEN
    SELECT ns.nspname as "schema", c.relname as "name" FROM pg_class AS c
      JOIN pg_namespace As ns ON c.relnamespace = ns.oid
      JOIN pg_publication_tables AS pb ON pb.schemaname = ns.nspname AND pb.tablename = c.relname
      WHERE c.oid = cmd.objid AND pb.pubname = ANY (publications)
      INTO target;
    IF target IS NULL THEN
      PERFORM ${schema}.notice_ignore(cmd.object_identity);
      RETURN;
    END IF;

    cmd.object_type := 'table';  -- normalize the 'table column' target to 'table'

  ELSIF cmd.object_type = 'index' THEN
    SELECT ns.nspname as "schema", c.relname as "name" FROM pg_class AS c
      JOIN pg_namespace As ns ON c.relnamespace = ns.oid
      JOIN pg_indexes as ind ON ind.schemaname = ns.nspname AND ind.indexname = c.relname
      JOIN pg_publication_tables AS pb ON pb.schemaname = ns.nspname AND pb.tablename = ind.tablename
      WHERE c.oid = cmd.objid AND pb.pubname = ANY (publications)
      INTO target;
    IF target IS NULL THEN
      PERFORM ${schema}.notice_ignore(cmd.object_identity);
      RETURN;
    END IF;

  ELSIF cmd.object_type = 'publication relation' THEN
    SELECT pb.pubname FROM pg_publication_rel AS rel
      JOIN pg_publication AS pb ON pb.oid = rel.prpubid
      WHERE rel.oid = cmd.objid AND pb.pubname = ANY (publications) 
      INTO pub;
    IF pub IS NULL THEN
      PERFORM ${schema}.notice_ignore(cmd.object_identity);
      RETURN;
    END IF;

  ELSIF cmd.object_type = 'publication namespace' THEN
    SELECT pb.pubname FROM pg_publication_namespace AS ns
      JOIN pg_publication AS pb ON pb.oid = ns.pnpubid
      WHERE ns.oid = cmd.objid AND pb.pubname = ANY (publications) 
      INTO pub;
    IF pub IS NULL THEN
      PERFORM ${schema}.notice_ignore(cmd.object_identity);
      RETURN;
    END IF;
  END IF;

  -- Construct and emit the DdlUpdateEvent message.

  IF target IS NOT NULL THEN
    SELECT json_build_object('tag', tag, cmd.object_type, target) INTO event;
  ELSIF tag LIKE 'CREATE %' THEN
    PERFORM ${schema}.notice_ignore('noop ' || tag);
    RETURN;
  ELSE
    SELECT json_build_object('tag', tag) INTO event;
  END IF;
  
  SELECT ${schema}.schema_specs() INTO schema_specs;

  SELECT json_build_object(
    'type', 'ddlUpdate',
    'version', ${PROTOCOL_VERSION},
    'schema', schema_specs::json,
    'event', event::json,
    'context', ${schema}.get_trigger_context()
  ) INTO message;

  PERFORM pg_logical_emit_message(true, ${lit(
    `${appID}/${shardNum}`,
  )}, message);
END
$$ LANGUAGE plpgsql;
`;
}

// Exported for testing.
export const TAGS = [
  'CREATE TABLE',
  'ALTER TABLE',
  'CREATE INDEX',
  'DROP TABLE',
  'DROP INDEX',
  'ALTER PUBLICATION',
] as const;

export function createEventTriggerStatements(shard: ShardConfig) {
  // Unlike functions, which are namespaced in shard-specific schemas,
  // EVENT TRIGGER names are in the global namespace and thus must include
  // the appID and shardNum.
  const {appID, shardNum} = shard;
  const sharded = append(shardNum);
  const schema = id(upstreamSchema(shard));

  const triggers = [
    dropEventTriggerStatements(shard.appID, shard.shardNum),
    createEventFunctionStatements(shard),
  ];

  // A single ddl_command_start trigger covering all relevant tags.
  triggers.push(`
CREATE EVENT TRIGGER ${sharded(`${appID}_ddl_start`)}
  ON ddl_command_start
  WHEN TAG IN (${lit(TAGS)})
  EXECUTE PROCEDURE ${schema}.emit_ddl_start();
`);

  // A per-tag ddl_command_end trigger that dispatches to ${schema}.emit_ddl_end(tag)
  for (const tag of TAGS) {
    const tagID = tag.toLowerCase().replace(' ', '_');
    triggers.push(`
CREATE OR REPLACE FUNCTION ${schema}.emit_${tagID}() 
RETURNS event_trigger AS $$
BEGIN
  PERFORM ${schema}.emit_ddl_end(${lit(tag)});
END
$$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER ${sharded(`${appID}_${tagID}`)}
  ON ddl_command_end
  WHEN TAG IN (${lit(tag)})
  EXECUTE PROCEDURE ${schema}.emit_${tagID}();
`);
  }
  return triggers.join('');
}

export function dropEventTriggerStatements(
  appID: string,
  shardID: string | number,
) {
  const stmts: string[] = [];
  // A single ddl_command_start trigger covering all relevant tags.
  stmts.push(`
    DROP EVENT TRIGGER IF EXISTS ${id(`${appID}_ddl_start_${shardID}`)};
  `);

  // A per-tag ddl_command_end trigger that dispatches to ${schema}.emit_ddl_end(tag)
  for (const tag of TAGS) {
    const tagID = tag.toLowerCase().replace(' ', '_');
    stmts.push(`
      DROP EVENT TRIGGER IF EXISTS ${id(`${appID}_${tagID}_${shardID}`)};
    `);
  }
  return stmts.join('');
}
