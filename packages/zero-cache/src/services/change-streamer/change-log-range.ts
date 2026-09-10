import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import type {SchemaChange} from '../change-source/protocol/current/data.ts';
import {schemaChangeTags} from '../change-source/protocol/current/schema-change-tags.ts';
import {
  CHANGE_LOG_STREAM_TABLE,
  FOLD_TAGS,
} from '../replicator/change-log-db.ts';

/**
 * Bounds the initializer's advisory comparison by relevant schema changes.
 * Subscriber backfill recovery runs through catchup and has no such cap.
 */
export const MAX_FOLD_SCAN_ROWS = 10_000;

const quoted = (tags: readonly string[]) =>
  tags.map(tag => `'${tag}'`).join(', ');

/**
 * The complete partial-index predicate must appear verbatim: SQLite does not
 * infer that the schema-only IN list implies the index's larger IN list.
 * The second predicate removes backfill announcements from this schema fold.
 */
export const READ_SCHEMA_CHANGES_SQL = /*sql*/ `
  SELECT "change" FROM "${CHANGE_LOG_STREAM_TABLE}"
    WHERE "watermark" > ? AND "watermark" <= ?
      AND "tag" IN (${quoted(FOLD_TAGS)})
      AND "tag" IN (${quoted(schemaChangeTags)})
    ORDER BY "watermark", "pos"
    LIMIT ${MAX_FOLD_SCAN_ROWS + 1}
`;

/** Returns undefined only when the advisory fold has too many schema changes. */
export function readSchemaChanges(
  db: Database,
  after: string,
  through: string,
): SchemaChange[] | undefined {
  const rows = db
    .prepare(READ_SCHEMA_CHANGES_SQL)
    .all<{change: string}>(after, through);
  return rows.length > MAX_FOLD_SCAN_ROWS
    ? undefined
    : rows.map(({change}) => BigIntJSON.parse(change) as SchemaChange);
}
