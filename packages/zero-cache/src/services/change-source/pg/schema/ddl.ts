import {literal as lit} from 'pg-format';
import {assert} from '../../../../../../shared/src/asserts.ts';
import * as v from '../../../../../../shared/src/valita.ts';
import {upstreamSchema, type ShardConfig} from '../../../../types/shards.ts';
import {id} from '../../../../types/sql.ts';
import {publishedSchema, publishedSchemaQuery} from './published.ts';

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
  event: v.object({tag: v.string()}),
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
});

export type DdlUpdateEvent = v.Infer<typeof ddlUpdateEventSchema>;

/**
 * The `schemaSnapshot` message is a snapshot of a schema taken in response to
 * a `COMMENT ON PUBLICATION` command, which is a hook recognized by zero
 * to manually emit schema snapshots to support detection of schema changes
 * from `ALTER PUBLICATION` commands on supabase, which does not fire event
 * triggers for them (https://github.com/supabase/supautils/issues/123).
 *
 * The hook is exercised by bookmarking the publication change with
 * `COMMENT ON PUBLICATION` statements within e.g.
 *
 * ```sql
 * BEGIN;
 * COMMENT ON PUBLICATION my_publication IS 'whatever';
 * ALTER PUBLICATION my_publication ...;
 * COMMENT ON PUBLICATION my_publication IS 'whatever';
 * COMMIT;
 * ```
 *
 * The `change-source` will perform the diff between a `schemaSnapshot`
 * events and its preceding `schemaSnapshot` (or `ddlUpdate`) within the
 * transaction.
 *
 * In the case where event trigger support is missing, this results in
 * diffing the `schemaSnapshot`s before and after the `ALTER PUBLICATION`
 * statement, thus effecting the same logic that would have been exercised
 * between the `ddlStart` and `ddlEvent` events fired by a database with
 * fully functional event triggers.
 *
 * Note that if the same transaction is run on a database that *does*
 * support event triggers on `ALTER PUBLICATION` statements, the sequence
 * of emitted messages will be:
 *
 * * `schemaSnapshot`
 * * `ddlStart`
 * * `ddlUpdate`
 * * `schemaSnapshot`
 *
 * Since `schemaSnapshot` messages are diffed with the preceding
 * `schemaSnapshot` or `ddlUpdate` event (if any), there will be no schema
 * difference between the `ddlUpdate` and the second `schemaSnapshot`, and
 * thus the extra `COMMENT` statements will effectively be no-ops.
 */
export const schemaSnapshotEventSchema = ddlEventSchema.extend({
  type: v.literal('schemaSnapshot'),
});

export type SchemaSnapshotEvent = v.Infer<typeof schemaSnapshotEventSchema>;

export const replicationEventSchema = v.union(
  ddlStartEventSchema,
  ddlUpdateEventSchema,
  schemaSnapshotEventSchema,
);

export type ReplicationEvent = v.Infer<typeof replicationEventSchema>;

// Creates a function that appends `_{shard-num}` to the input and
// quotes the result to be a valid identifier.
function append(shardNum: number) {
  return (name: string) => id(name + '_' + String(shardNum));
}

// pg_advisory_xact_lock key for serializing ddl statements in order to
// produce correct schema change diffs.
const DDL_SERIALIZATION_LOCK = 0x3c6b8468f1bac0b0n;

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
  return /*sql*/ `
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


CREATE OR REPLACE FUNCTION ${schema}.notice_ignore(tag TEXT, target record)
RETURNS void AS $$
BEGIN
  RAISE NOTICE 'zero(%) ignoring % %', ${lit(shardNum)}, tag, row_to_json(target);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${schema}.schema_specs()
RETURNS TEXT 
STABLE
AS $$
  ${publishedSchemaQuery(publications)}
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION ${schema}.emit_ddl_start()
RETURNS event_trigger AS $$
DECLARE
  schema_specs TEXT;
  message TEXT;
BEGIN
  -- serialize DDL statements to compute correct schema change diffs
  PERFORM pg_advisory_xact_lock(${DDL_SERIALIZATION_LOCK});

  SELECT ${schema}.schema_specs() INTO schema_specs;

  SELECT json_build_object(
    'type', 'ddlStart',
    'version', ${PROTOCOL_VERSION},
    'schema', schema_specs::json,
    'event', json_build_object('tag', TG_TAG),
    'context', ${schema}.get_trigger_context()
  ) INTO message;

  PERFORM pg_logical_emit_message(true, ${lit(
    `${appID}/${shardNum}`,
  )}, message);
END
$$ LANGUAGE plpgsql;

-- Delete legacy function (and dependent legacy triggers).
DROP FUNCTION IF EXISTS ${schema}.emit_ddl_end(text) CASCADE;

CREATE OR REPLACE FUNCTION ${schema}.emit_ddl_end()
RETURNS event_trigger AS $$
DECLARE
  publications TEXT[];
  target RECORD;
  relevant RECORD;
  schema_specs TEXT;
  message TEXT;
  event TEXT;
  event_type TEXT;
  event_prefix TEXT;
BEGIN
  publications := ARRAY[${lit(publications)}];

  SELECT objid, object_type, object_identity 
    FROM pg_event_trigger_ddl_commands() 
    LIMIT 1 INTO target;

  -- Filter DDL updates that are not relevant to the shard (i.e. publications) when possible.
  SELECT true INTO relevant;

  -- Note: ALTER TABLE statements may *remove* the table from the set of published
  --       tables, and there is no way to determine if the table "used to be" in the
  --       set. Thus, all ALTER TABLE statements must produce a ddl update, similar to
  --       any DROP * statement.
  IF (target.object_type = 'table' AND TG_TAG != 'ALTER TABLE') 
     OR target.object_type = 'table column' THEN
    SELECT ns.nspname AS "schema", c.relname AS "name" FROM pg_class AS c
      JOIN pg_namespace AS ns ON c.relnamespace = ns.oid
      JOIN pg_publication_tables AS pb ON pb.schemaname = ns.nspname AND pb.tablename = c.relname
      WHERE c.oid = target.objid AND pb.pubname = ANY (publications)
      INTO relevant;

  ELSIF target.object_type = 'index' THEN
    SELECT ns.nspname AS "schema", c.relname AS "name" FROM pg_class AS c
      JOIN pg_namespace AS ns ON c.relnamespace = ns.oid
      JOIN pg_indexes as ind ON ind.schemaname = ns.nspname AND ind.indexname = c.relname
      JOIN pg_publication_tables AS pb ON pb.schemaname = ns.nspname AND pb.tablename = ind.tablename
      WHERE c.oid = target.objid AND pb.pubname = ANY (publications)
      INTO relevant;

  ELSIF target.object_type = 'publication relation' THEN
    SELECT pb.pubname FROM pg_publication_rel AS rel
      JOIN pg_publication AS pb ON pb.oid = rel.prpubid
      WHERE rel.oid = target.objid AND pb.pubname = ANY (publications) 
      INTO relevant;

  ELSIF target.object_type = 'publication namespace' THEN
    SELECT pb.pubname FROM pg_publication_namespace AS ns
      JOIN pg_publication AS pb ON pb.oid = ns.pnpubid
      WHERE ns.oid = target.objid AND pb.pubname = ANY (publications) 
      INTO relevant;

  ELSIF target.object_type = 'schema' THEN
    SELECT ns.nspname AS "schema", c.relname AS "name" FROM pg_class AS c
      JOIN pg_namespace AS ns ON c.relnamespace = ns.oid
      JOIN pg_publication_tables AS pb ON pb.schemaname = ns.nspname AND pb.tablename = c.relname
      WHERE ns.oid = target.objid AND pb.pubname = ANY (publications)
      INTO relevant;

  ELSIF target.object_type = 'publication' THEN
    SELECT 1 WHERE target.object_identity = ANY (publications)
      INTO relevant;

  -- no-op CREATE IF NOT EXIST statements
  ELSIF TG_TAG LIKE 'CREATE %' AND target.object_type IS NULL THEN
    relevant := NULL;
  END IF;

  IF relevant IS NULL THEN
    PERFORM ${schema}.notice_ignore(TG_TAG, target);
    RETURN;
  END IF;

  IF TG_TAG = 'COMMENT' THEN
    -- Only make schemaSnapshots for COMMENT ON PUBLICATION
    IF target.object_type != 'publication' THEN
      PERFORM ${schema}.notice_ignore(TG_TAG, target);
      RETURN;
    END IF;
    event_type := 'schemaSnapshot';
    event_prefix := '/ddl';
  ELSE
    event_type := 'ddlUpdate';
    event_prefix := '';  -- TODO: Use '/ddl' for both when rollback safe
  END IF;

  RAISE INFO 'Creating % for % %', event_type, TG_TAG, row_to_json(target);

  SELECT ${schema}.schema_specs() INTO schema_specs;

  SELECT json_build_object(
    'type', event_type,
    'version', ${PROTOCOL_VERSION},
    'schema', schema_specs::json,
    'event', json_build_object('tag', TG_TAG),
    'context', ${schema}.get_trigger_context()
  ) INTO message;

  PERFORM pg_logical_emit_message(true, ${lit(
    `${appID}/${shardNum}`,
  )} || event_prefix, message);
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
  'ALTER SCHEMA',
] as const;

export function createEventTriggerStatements(shard: ShardConfig) {
  // Better to assert here than get a cryptic syntax error from Postgres.
  assert(shard.publications.length, `shard publications must be non-empty`);

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
  triggers.push(/*sql*/ `
CREATE EVENT TRIGGER ${sharded(`${appID}_ddl_start`)}
  ON ddl_command_start
  WHEN TAG IN (${lit(TAGS)})
  EXECUTE PROCEDURE ${schema}.emit_ddl_start();

CREATE EVENT TRIGGER ${sharded(`${appID}_ddl_end`)}
  ON ddl_command_end
  WHEN TAG IN (${lit([...TAGS, 'COMMENT'])})
  EXECUTE PROCEDURE ${schema}.emit_ddl_end();
`);

  // Drop legacy functions / triggers.
  for (const tag of [...TAGS, 'COMMENT']) {
    const tagID = tag.toLowerCase().replace(' ', '_');
    triggers.push(`DROP FUNCTION IF EXISTS ${schema}.emit_${tagID}() CASCADE;`);
  }
  return triggers.join('');
}

// Exported for testing.
export function dropEventTriggerStatements(
  appID: string,
  shardID: string | number,
) {
  return /*sql*/ `
    DROP EVENT TRIGGER IF EXISTS ${id(`${appID}_ddl_start_${shardID}`)};
    DROP EVENT TRIGGER IF EXISTS ${id(`${appID}_ddl_end_${shardID}`)};
  `;
}
