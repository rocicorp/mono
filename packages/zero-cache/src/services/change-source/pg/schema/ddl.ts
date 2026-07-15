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

// In protocol v2 (planned, not yet emitted), "ddlStart" events that are not
// associated with a schema change will be context-only, omitting the
// `schema` snapshot entirely. This avoids bloating the WAL with (large)
// redundant schema snapshots for DDL commands that do not affect the
// published schema, e.g. the CREATE/ALTER/DROP TABLE sub-commands executed
// by REFRESH MATERIALIZED VIEW CONCURRENTLY.
//
// This release already accepts v2 events so that the (subsequent) release
// that upgrades the triggers to emit them remains rollback safe with respect
// to this one.
const versionSchema = v.literalUnion(PROTOCOL_VERSION, 2);

const triggerEvent = v.object({
  context: v.object({query: v.string()}).rest(v.string()),
});

export const ddlEventSchema = triggerEvent.extend({
  version: versionSchema,
  event: v.object({tag: v.string()}),
  // Maps the OID of each published table to the `attnum`s of published
  // columns that were created in the (upstream) transaction that emitted
  // the event. Such columns are guaranteed to have the column default in
  // all pre-existing rows, and can thus be replicated without backfill if
  // the default value itself is replicable. Tables whose publication
  // entries (e.g. column lists) were modified in the same transaction are
  // excluded, since a newly *published* (as opposed to newly *created*)
  // column may hold arbitrary values in existing rows.
  //
  // The field is absent in messages from older versions of the upstream
  // functions (in which case backfill decisions fall back to the command
  // tag heuristic), and `null` when there are no such columns.
  newColumns: v.record(v.array(v.number())).nullable().optional(),
});

/**
 * A {@link DdlStartEvent} message is emitted before every DDL event,
 * containing the command `tag`.
 *
 * In most cases, the `DdlStartEvent` itself will not be associated with a
 * schema change, in which case the event is context-only (in protocol v2,
 * planned, both `previousSchema` and `schema` will be absent; in v1,
 * `schema` is set and `previousSchema` is `null`). The message is still
 * emitted to provide the command `tag` context in case an immediately
 * following `DdlStartEvent` tag is emitted with a schema change (which can
 * happen when another event trigger results in a nested ddl statement).
 *
 * In such cases, the `previousSchema` and `schema` fields of the latter event
 * are used to determine the necessary schema change operations (as they are
 * with `ddlUpdate` and `schemaSnapshot` events), and the `tag` of the
 * preceding start event indicates the command that precipitated the schema
 * change (e.g. a CREATE vs ALTER) to determine whether a backfill is
 * necessary.
 */
export const ddlStartEventSchema = ddlEventSchema.extend({
  type: v.literal('ddlStart'),
  // Set (along with `previousSchema`) only if the ddlStart event itself is
  // associated with a schema change. Absent in context-only (protocol v2,
  // planned) events. v1 events always contain the current `schema`.
  schema: publishedSchema.optional(),
  // For ddlStart messages, previousSchema is `null` (v1) or absent
  // (protocol v2, planned) if there was no change in schema detected.
  previousSchema: publishedSchema.nullable().optional(),
  // For backwards compatibility with previous versions of the trigger,
  // default an absent `event` field with a semantic equivalent. This
  // field override can be removed in a version that is rollback safe
  // with 1.4.0.
  event: v.object({tag: v.string()}).optional(() => ({tag: 'UNKNOWN'})),
});

export type DdlStartEvent = v.Infer<typeof ddlStartEventSchema>;

/**
 * A {@link DdlUpdateEvent} is emitted if there was a change in the schema.
 * It always contains `previousSchema` and (current) `schema` fields, leaving
 * it to the receiver to compute the necessary schema change operations.
 */
export const ddlUpdateEventSchema = ddlEventSchema.extend({
  type: v.literal('ddlUpdate'),
  // ddlUpdate messages are only emitted if the schema changed. `schema`
  // contains the current (i.e. post-change) snapshot.
  schema: publishedSchema,
  // The `previousSchema` contains the schema before the change.
  //
  // In 1.5.0 it is always set, and can be made non-optional when
  // rollback safe.
  previousSchema: publishedSchema.optional(),
});

export type DdlUpdateEvent = v.Infer<typeof ddlUpdateEventSchema>;

/**
 * The `schemaSnapshot` message is a snapshot of a schema taken in response to
 * a `COMMENT ON PUBLICATION` command, which is a hook recognized by zero
 * to manually emit `previousSchema` and `schema` snapshots when a difference
 * is detected. This is a workaround provided to support detection of schema
 * changes from `ALTER PUBLICATION` commands on supabase, which does not fire
 * event triggers for them (https://github.com/supabase/supautils/issues/123).
 *
 * The hook is exercised by trailing the publication change with a
 * `COMMENT ON PUBLICATION` statement, e.g.
 *
 * ```sql
 * BEGIN;
 * ALTER PUBLICATION my_publication ...;
 * COMMENT ON PUBLICATION my_publication IS 'whatever';
 * COMMIT;
 * ```
 *
 * Note that it is fine to invoke `COMMENT ON PUBLICATION` statements
 * on a database that *does* support event triggers on
 * `ALTER PUBLICATION` statements, as it will simply be a no-op.
 */
export const schemaSnapshotEventSchema = ddlEventSchema.extend({
  type: v.literal('schemaSnapshot'),
  schema: publishedSchema,
  previousSchema: publishedSchema.optional(),
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
export function createEventFunctionStatements(shard: ShardConfig) {
  const {appID, shardNum, publications} = shard;
  const schema = id(upstreamSchema(shard)); // e.g. "{APP_ID}_{SHARD_ID}"
  return /*sql*/ `
CREATE SCHEMA IF NOT EXISTS ${schema};

-- The SECURITY DEFINER entrypoints below call helper functions in this schema.
-- PUBLIC must not be able to add an overload that could be selected while the
-- entrypoints are running with their owner's privileges. Explicit grants to
-- trusted administration roles are left unchanged.
REVOKE CREATE ON SCHEMA ${schema} FROM PUBLIC;

CREATE OR REPLACE FUNCTION ${schema}.get_trigger_context()
RETURNS record AS $$
DECLARE
  result record;
BEGIN
  SELECT COALESCE(current_query(), 'current_query() returned NULL') AS "query" into result;
  RETURN result;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${schema}.notice_ignore(reason TEXT, tag TEXT, target record)
RETURNS void AS $$
BEGIN
  RAISE NOTICE '${appID}_${shardNum} ignoring % % %', reason, tag, 
    COALESCE(row_to_json(target)::text, '');
END
$$ LANGUAGE plpgsql;


-- Note: DROP and CREATE to upgrade from v20 to v21 because the
-- return type has changed. This can be simplified to CREATE OR REPLACE
-- once 1.5.0 is rollback safe.
DROP FUNCTION IF EXISTS ${schema}.schema_specs();
CREATE FUNCTION ${schema}.schema_specs()
RETURNS JSON 
STABLE
AS $$
  ${publishedSchemaQuery(publications)}
$$ LANGUAGE sql;


-- Stores the most recent published schema
CREATE TABLE IF NOT EXISTS ${schema}."publishedSchema" (
  current JSON,
  exists BOOL PRIMARY KEY DEFAULT true CHECK (exists)
);

INSERT INTO ${schema}."publishedSchema" (current) VALUES (${schema}.schema_specs())
  ON CONFLICT (exists) DO 
  UPDATE SET current = excluded.current;


CREATE OR REPLACE FUNCTION ${schema}.update_schemas(event_type text, tag text, target record)
RETURNS void AS $$
DECLARE
  prev_schema_specs JSON;
  schema_specs JSON;
  message TEXT;
BEGIN
  SELECT current FROM ${schema}."publishedSchema" INTO prev_schema_specs;
  SELECT ${schema}.schema_specs() INTO schema_specs;
  
  IF prev_schema_specs::text != schema_specs::text THEN
    UPDATE ${schema}."publishedSchema" SET current = schema_specs;
  ELSIF event_type = 'ddlStart' THEN
    -- ddlStart events are always be emitted to allow the zero-cache
    -- to track the context of the current command tag in the face of
    -- nested event triggers (e.g. start->start->end->end).
    prev_schema_specs = NULL;
  ELSIF event_type = 'ddlUpdate' THEN
    -- TODO: fold 'schemaSnapshot' into this condition too (i.e. make it "ELSE")
    -- when 1.5.0 is rollback safe. Until then, noop schemaSnapshots are sent
    -- for compatibility with 1.0.0 ~ 1.4.0.
    PERFORM ${schema}.notice_ignore('noop', tag, target);
    RETURN;
  END IF;

  SELECT json_build_object(
    'type', event_type,
    'version', ${PROTOCOL_VERSION},
    'previousSchema', prev_schema_specs,
    'schema', schema_specs,
    'event', json_build_object('tag', tag),
    'context', ${schema}.get_trigger_context()
  ) INTO message;

  PERFORM pg_logical_emit_message(true, '${appID}/${shardNum}/ddl', message);

  RAISE NOTICE 'Emitted ${appID}_${shardNum} % for % %', event_type, tag,
    COALESCE(row_to_json(target)::text, '');
END
$$ LANGUAGE plpgsql;


-- Hook/workaround to manually trigger replication of schema changes on DBs 
-- that do not support/allow event triggers.
CREATE OR REPLACE FUNCTION ${schema}.update_schemas()
RETURNS void AS $$
BEGIN
  PERFORM ${schema}.update_schemas('schemaSnapshot', 'MANUAL', NULL);
END
$$ LANGUAGE plpgsql;


-- These are the only privileged entrypoints in the DDL trigger stack. Event
-- triggers are database-wide, so a role can invoke them indirectly without
-- having access to this schema. Everything reached from these functions runs
-- with the existing function owner's privileges: keep internal calls schema
-- qualified, keep pg_catalog before pg_temp, and never introduce
-- caller-controlled dynamic SQL anywhere in this call chain.
CREATE OR REPLACE FUNCTION ${schema}.emit_ddl_start()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
  schema_specs JSON;
  message TEXT;
BEGIN
  -- serialize DDL statements to compute correct schema change diffs
  PERFORM pg_advisory_xact_lock(${DDL_SERIALIZATION_LOCK});
  PERFORM ${schema}.update_schemas('ddlStart', TG_TAG, NULL);
END
$$;


CREATE OR REPLACE FUNCTION ${schema}.emit_ddl_end()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
  publications TEXT[];
  target RECORD;
  relevant RECORD;
  schema_specs JSON;
  message TEXT;
  event TEXT;
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
    PERFORM ${schema}.notice_ignore('irrelevant', TG_TAG, target);
    RETURN;
  END IF;

  IF TG_TAG = 'COMMENT' THEN
    -- Only make schemaSnapshots for COMMENT ON PUBLICATION
    IF target.object_type != 'publication' THEN
      PERFORM ${schema}.notice_ignore('irrelevant', TG_TAG, target);
      RETURN;
    END IF;
    PERFORM ${schema}.update_schemas('schemaSnapshot', TG_TAG, target);
  ELSE
    PERFORM ${schema}.update_schemas('ddlUpdate', TG_TAG, target);
  END IF;

END
$$;


-- Event triggers retain their function references by OID and do not require
-- the role issuing DDL to call these entrypoints directly. PostgreSQL grants
-- EXECUTE to PUBLIC on new functions. That did not cross a privilege boundary
-- while these functions were SECURITY INVOKER, but is unnecessary once they
-- run as their owner.
REVOKE ALL ON FUNCTION ${schema}.emit_ddl_start() FROM PUBLIC;
REVOKE ALL ON FUNCTION ${schema}.emit_ddl_end() FROM PUBLIC;
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
    /*sql*/ `
CREATE EVENT TRIGGER ${sharded(`${appID}_ddl_start`)}
  ON ddl_command_start
  WHEN TAG IN (${lit(TAGS)})
  EXECUTE PROCEDURE ${schema}.emit_ddl_start();

CREATE EVENT TRIGGER ${sharded(`${appID}_ddl_end`)}
  ON ddl_command_end
  WHEN TAG IN (${lit([...TAGS, 'COMMENT'])})
  EXECUTE PROCEDURE ${schema}.emit_ddl_end();
`,
  ];

  // Drop legacy functions / triggers.
  triggers.push(
    `DROP FUNCTION IF EXISTS ${schema}.emit_ddl_end(text) CASCADE;`,
    `DROP FUNCTION IF EXISTS ${schema}.notice_ignore(text, record);`,
  );
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
