import {literal as lit} from 'pg-format';
import {assert} from '../../../../../../shared/src/asserts.ts';
import * as v from '../../../../../../shared/src/valita.ts';
import {upstreamSchema, type ShardConfig} from '../../../../types/shards.ts';
import {id} from '../../../../types/sql.ts';
import {jsonValueSchema} from '../../protocol/current/json.ts';
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
  // the event. Such columns are guaranteed to hold their initial value
  // (i.e. the default at creation time, or NULL) in all pre-existing rows,
  // and can thus be replicated without backfill if that value is known and
  // replicable (see `missingValues`).
  //
  // A newly *published* (as opposed to newly *created*) column may hold
  // arbitrary values in existing rows. Columns of tables whose publication
  // entries (e.g. column lists) were modified in the same transaction are
  // thus only reported if they provably did not exist when the transaction
  // started its first DDL command, and no rows of the table were written
  // since (see `xactSnapshotSetting()`).
  //
  // The field is absent in messages from older versions of the upstream
  // functions (in which case backfill decisions fall back to the command
  // tag heuristic), and `null` when there are no such columns.
  newColumns: v.record(v.array(v.number())).nullable().optional(),
  // Maps the OID of each published table to the "missing value"
  // (i.e. `pg_attribute.attmissingval`) of each column (keyed by `attnum`)
  // in `newColumns` that has one. This is the value that all pre-existing
  // rows contain for a column that was added with Postgres' fast "default
  // for all rows" optimization, and it is unaffected by later changes to
  // the column default (which only apply to subsequently created rows).
  //
  // A column can thus be replicated without backfill iff its current
  // default is a replicable expression that evaluates to its missing
  // value (see `defaultValueMatches()`), as replicating the default is
  // then guaranteed to reproduce the contents of pre-existing rows.
  // A `null` value indicates that all pre-existing rows are NULL, i.e.
  // that the column was added without a default, and that nothing else
  // (identity or generation, a domain default, or a table rewrite) filled
  // in values. This is only reported when proven by the transaction's
  // snapshot (see `xactSnapshotSetting()`).
  //
  // Columns without an entry (e.g. added with a volatile default) must be
  // backfilled.
  //
  // Like `newColumns`, the field is absent in messages from older
  // versions of the upstream functions, and `null` when there are no such
  // columns.
  missingValues: v.record(v.record(jsonValueSchema)).nullable().optional(),
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
 * The name of the transaction-local setting in which the first DDL command
 * of a transaction records, for each published table, its `relnatts`, the
 * transaction's row write counters (inserted, updated, deleted), and its
 * relfilenode as `{[oid]: [relnatts, inserted, updated, deleted, filenode]}`.
 *
 * A published column whose `attnum` exceeds the recorded `relnatts` was
 * created after the snapshot. If the counters are unchanged, no row of the
 * table was written since, and if the relfilenode is unchanged, the table
 * was not rewritten (which is not reflected in the counters). All
 * pre-existing rows are then guaranteed to hold the column's initial value,
 * even if the column was published in the same transaction (e.g. by
 * `ALTER PUBLICATION ... ADD TABLE t (..., col)`), which would otherwise
 * require a backfill.
 *
 * Note that the counters (`pg_stat_get_xact_tuples_*()`) are not strictly
 * scoped to the current transaction (they include stats not yet flushed
 * from previous transactions), but they are not flushed while a transaction
 * is in progress, so an unchanged value implies that no rows were written.
 */
export function xactSnapshotSetting({appID, shardNum}: ShardConfig) {
  // Custom setting names must start with a letter or underscore, whereas
  // appIDs may start with a digit.
  return `_${appID}_${shardNum}.xact_snapshot`;
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
export function createEventFunctionStatements(
  shard: ShardConfig,
  includePartialIndexes = true,
) {
  const {appID, shardNum, publications} = shard;
  const schema = id(upstreamSchema(shard)); // e.g. "{APP_ID}_{SHARD_ID}"
  const snapshotSetting = lit(xactSnapshotSetting(shard));
  return /*sql*/ `
CREATE SCHEMA IF NOT EXISTS ${schema};

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
  ${publishedSchemaQuery(publications, includePartialIndexes)}
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
  new_columns JSON;
  missing_values JSON;
  xact_snapshot JSON;
  publications_changed BOOL;
  message TEXT;
BEGIN
  SELECT current FROM ${schema}."publishedSchema" INTO prev_schema_specs;
  SELECT ${schema}.schema_specs() INTO schema_specs;

  IF prev_schema_specs::text != schema_specs::text THEN
    UPDATE ${schema}."publishedSchema" SET current = schema_specs;

    -- Report the published columns that were created in the current
    -- transaction (i.e. pg_attribute rows inserted by this transaction),
    -- along with the "missing value" (attmissingval) of each column that
    -- has one. The missing value is what all pre-existing rows contain
    -- for a column added with a non-volatile default, and is unaffected
    -- by later changes to the column default; the zero-cache can thus
    -- replicate such a column without backfill iff its current default
    -- evaluates to its missing value.
    --
    -- A newly *published* column may be a pre-existing column with
    -- arbitrary values in existing rows, which requires a backfill. If the
    -- publications were changed in the same transaction (e.g.
    -- ALTER PUBLICATION ... SET TABLE with a column list, or
    -- ADD TABLES IN SCHEMA), columns are thus only reported if the
    -- transaction's snapshot (recorded by its first DDL command) proves that
    -- the column did not exist at the time, and that no rows of the table
    -- have been written since.
    xact_snapshot := NULLIF(current_setting(${snapshotSetting}, true), '')::json;

    SELECT EXISTS (
      SELECT 1 FROM pg_publication pub
        WHERE pub.pubname IN (${lit(publications)})
          AND pub.xmin = pg_current_xact_id()::xid
    ) OR EXISTS (
      SELECT 1 FROM pg_publication_namespace ns
        JOIN pg_publication pub ON pub.oid = ns.pnpubid
        WHERE pub.pubname IN (${lit(publications)})
          AND ns.xmin = pg_current_xact_id()::xid
    ) INTO publications_changed;

    WITH new_cols AS (
      SELECT DISTINCT pc.oid AS rel_oid, attnum, atthasmissing,
                      attidentity, attgenerated, typtype,
                      COALESCE(unchanged_since_snapshot, false) AS proven
        FROM pg_attribute
        JOIN pg_type pt ON pt.oid = atttypid
        JOIN pg_class pc ON pc.oid = attrelid
        JOIN pg_namespace pns ON pns.oid = pc.relnamespace
        JOIN pg_publication_tables pb ON
          pb.schemaname = pns.nspname AND
          pb.tablename = pc.relname AND
          attname = ANY(pb.attnames)
        LEFT JOIN LATERAL (
          SELECT xact_snapshot -> (pc.oid::text) AS snap
        ) snapshot ON true
        LEFT JOIN LATERAL (
          -- The column was created after the snapshot, and the table has
          -- neither been written to nor rewritten (e.g. by a volatile
          -- default, a stored generated column, or ALTER COLUMN ... TYPE)
          -- since.
          SELECT pc.relkind = 'r'
            AND attnum > (snap ->> 0)::int
            AND pg_stat_get_xact_tuples_inserted(pc.oid) = (snap ->> 1)::int8
            AND pg_stat_get_xact_tuples_updated(pc.oid) = (snap ->> 2)::int8
            AND pg_stat_get_xact_tuples_deleted(pc.oid) = (snap ->> 3)::int8
            AND pg_relation_filenode(pc.oid) = (snap ->> 4)::oid
            AS unchanged_since_snapshot
        ) unchanged ON true
        WHERE pb.pubname IN (${lit(publications)})
          AND attnum > 0
          AND NOT attisdropped
          AND pg_attribute.xmin = pg_current_xact_id()::xid
          AND (
            NOT publications_changed AND NOT EXISTS (
              SELECT 1 FROM pg_publication_rel rel
                JOIN pg_publication pub ON pub.oid = rel.prpubid
                WHERE rel.prrelid = pc.oid
                  AND pub.pubname IN (${lit(publications)})
                  AND rel.xmin = pg_current_xact_id()::xid
            )
            OR unchanged_since_snapshot
          )
    )
    SELECT
      (SELECT json_object_agg(rel_oid::int8, attnums) FROM (
        SELECT rel_oid, json_agg(attnum) AS attnums
          FROM new_cols GROUP BY rel_oid
      ) attnums_by_table),
      (SELECT json_object_agg(rel_oid::int8, vals) FROM (
        SELECT n.rel_oid,
               json_object_agg(
                 n.attnum,
                 CASE WHEN n.atthasmissing
                   THEN array_to_json(a.attmissingval)->0
                   ELSE NULL
                 END
               ) AS vals
          FROM new_cols n
          JOIN pg_attribute a ON a.attrelid = n.rel_oid AND a.attnum = n.attnum
          WHERE n.atthasmissing OR (
            -- Columns added without a (non-null) default hold NULL in all
            -- pre-existing rows, unless they are filled by other means,
            -- i.e. as identity or generated columns, by a domain default, or
            -- by a table rewrite (e.g. from a volatile default that has
            -- since been dropped). The latter leaves no trace in the column
            -- definition, so this requires the snapshot to prove that the
            -- table was not rewritten.
            n.proven AND
            NOT n.atthasmissing AND
            n.attidentity = '' AND
            n.attgenerated = '' AND
            n.typtype != 'd'
          )
          GROUP BY n.rel_oid
      ) vals_by_table)
      INTO new_columns, missing_values;
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
    'newColumns', new_columns,
    'missingValues', missing_values,
    'event', json_build_object('tag', tag),
    'context', ${schema}.get_trigger_context()
  ) INTO message;

  PERFORM pg_logical_emit_message(true, '${appID}/${shardNum}/ddl', message);

  RAISE NOTICE 'Emitted ${appID}_${shardNum} % for % %', event_type, tag,
    COALESCE(row_to_json(target)::text, '');
END
$$ LANGUAGE plpgsql;


-- Hook/workaround to manually trigger replication of schema changes on DBs
-- that do not support/allow event triggers. This should be invoked in the
-- same transaction as the schema change statement(s); among other things,
-- this allows columns added with replicable defaults (e.g. constants) to
-- be replicated without a backfill.
--
-- Note that it must be invoked *before* any subsequent DML on the altered
-- tables: since this hook emits the schema change at the point of the
-- call (rather than at each DDL statement, as event triggers do), row
-- changes between a column's creation and the call reference a column
-- that the replica does not yet know about, and fail replication.
CREATE OR REPLACE FUNCTION ${schema}.update_schemas()
RETURNS void AS $$
BEGIN
  PERFORM ${schema}.update_schemas('schemaSnapshot', 'MANUAL', NULL);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${schema}.emit_ddl_start()
RETURNS event_trigger AS $$
DECLARE
  schema_specs JSON;
  message TEXT;
BEGIN
  -- serialize DDL statements to compute correct schema change diffs
  PERFORM pg_advisory_xact_lock(${DDL_SERIALIZATION_LOCK});

  -- Record the columns and row write counters of published tables at the
  -- first DDL command of the transaction. This is used to determine whether
  -- columns published later in the transaction were newly created (and
  -- thus hold their initial value in all rows). The row write counters are
  -- only maintained if track_counts is enabled.
  IF current_setting('track_counts')::bool AND
     COALESCE(current_setting(${snapshotSetting}, true), '') = '' THEN
    PERFORM set_config(${snapshotSetting}, COALESCE((
      SELECT json_object_agg(oid::int8, json_build_array(
        relnatts,
        pg_stat_get_xact_tuples_inserted(oid),
        pg_stat_get_xact_tuples_updated(oid),
        pg_stat_get_xact_tuples_deleted(oid),
        pg_relation_filenode(oid)
      ))::text FROM (
        SELECT DISTINCT pc.oid, pc.relnatts FROM pg_class pc
          JOIN pg_namespace pns ON pns.oid = pc.relnamespace
          JOIN pg_publication_tables pb ON
            pb.schemaname = pns.nspname AND pb.tablename = pc.relname
          WHERE pb.pubname IN (${lit(publications)}) AND pc.relkind = 'r'
      ) published
    ), '{}'), true);
  END IF;

  PERFORM ${schema}.update_schemas('ddlStart', TG_TAG, NULL);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${schema}.emit_ddl_end()
RETURNS event_trigger AS $$
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
