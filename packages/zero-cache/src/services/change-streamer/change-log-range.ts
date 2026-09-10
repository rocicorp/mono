/**
 * Tag-filtered reads of a watermark range out of the SQLite change log.
 *
 * Two callers need the same thing over a `(after, through]` interval and
 * differ only in which tags they want: the change-log initializer folds the
 * *schema changes* forward over a replica-derived cookie set, and the
 * change-streamer's subscribe path reads the *backfill run announcements* to
 * decide whether a declaring subscriber will follow a run from catchup alone.
 *
 * Both are bounded the same way, and for the same reason: the interval is the
 * distance a subscriber (or a replica) has fallen behind the head, which is
 * not bounded by anything, and walking it is latency on a connection that is
 * trying to start.
 */

import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import type {
  BackfillStarted,
  Identifier,
  SchemaChange,
} from '../change-source/protocol/current/data.ts';
import {
  backfillControlTags,
  schemaChangeTags,
} from '../change-source/protocol/current/schema-change-tags.ts';
import {CHANGE_LOG_STREAM_TABLE} from '../replicator/change-log-db.ts';

/**
 * The most rows a range read will scan before declining.
 *
 * The change log is a buffer of every change, so a range is dominated by
 * ordinary data changes; the tags being looked for are a handful at most.
 * Rather than let a stalled reader turn a range read into a full scan of the
 * retention window, a range above this cap is declined and the caller falls
 * back to whatever it does without the answer — a re-seed for the
 * initializer, a restart-from-zero for a backfill.
 */
export const MAX_FOLD_SCAN_ROWS = 10_000;

const quoted = (tags: readonly string[]) =>
  tags.map(tag => `'${tag}'`).join(', ');

const SCHEMA_CHANGE_TAG_LIST = quoted(schemaChangeTags);
const BACKFILL_CONTROL_TAG_LIST = quoted(backfillControlTags);

/**
 * The log's changes with one of `tagList`'s tags over `(after, through]`, in
 * stream order, or `undefined` if the interval is too large to scan.
 *
 * The tag is stored beside the verbatim change so this scan never parses the
 * payloads of ordinary data changes, and a partial index over the same tags
 * (`CHANGE_LOG_STREAM_FOLD_TAG_INDEX`) keeps it off their rows entirely.
 *
 * The row count is still checked first so a stalled reader cannot make the
 * range scan unbounded. The count is itself capped, at one row past the cap it
 * is deciding: a bare `count(*)` over the interval is `O(interval)`, so the
 * unbounded interval this exists to decline would still be walked in full
 * before being declined — which is the latency the cap is here to avoid, not a
 * cheaper form of it. The `LIMIT` makes the decision
 * `O(MAX_FOLD_SCAN_ROWS)`, and the comparison is unaffected: the subquery
 * returns the true count whenever it is within the cap, and `cap + 1` whenever
 * it is not.
 */
function readTaggedChanges<T>(
  db: Database,
  after: string,
  through: string,
  tagList: string,
): T[] | undefined {
  const {rows} = db
    .prepare(/*sql*/ `
      SELECT count(*) AS "rows" FROM (
        SELECT 1 FROM "${CHANGE_LOG_STREAM_TABLE}"
          WHERE "watermark" > ? AND "watermark" <= ?
          LIMIT ${MAX_FOLD_SCAN_ROWS + 1}
      )
    `)
    .get<{rows: number}>(after, through);
  if (rows > MAX_FOLD_SCAN_ROWS) {
    return undefined;
  }
  return db
    .prepare(/*sql*/ `
      SELECT "change" FROM "${CHANGE_LOG_STREAM_TABLE}"
        WHERE "watermark" > ? AND "watermark" <= ?
          AND "tag" IN (${tagList})
        ORDER BY "watermark", "pos"
    `)
    .all<{change: string}>(after, through)
    .map(({change}) => BigIntJSON.parse(change) as T);
}

/** The log's schema changes over `(after, through]`, in stream order. */
export function readSchemaChanges(
  db: Database,
  after: string,
  through: string,
): SchemaChange[] | undefined {
  return readTaggedChanges<SchemaChange>(
    db,
    after,
    through,
    SCHEMA_CHANGE_TAG_LIST,
  );
}

/** The log's backfill run announcements over `(after, through]`. */
export function readBackfillAnnouncements(
  db: Database,
  after: string,
  through: string,
): BackfillStarted[] | undefined {
  return readTaggedChanges<BackfillStarted>(
    db,
    after,
    through,
    BACKFILL_CONTROL_TAG_LIST,
  );
}

/**
 * Folds the table renames and drops in a change sequence into a mapping from
 * the identity a table had at the start of the interval to the identity it has
 * at the end, or `null` if it was dropped.
 *
 * This is the *identity* half of the cookie fold, applied to a subscriber's
 * declaration rather than to a cookie set: the declaration names the table the
 * subscriber knew at its watermark, and the change source knows the current
 * one. `backfill-completed` deliberately does not drop anything here -- a
 * completion the subscriber has not followed is not its completion.
 */
export function foldIdentities(
  changes: Iterable<SchemaChange>,
): Map<string, Identifier | null> {
  const key = ({schema, name}: Identifier) => `${schema}.${name}`;
  // Seeded lazily: a table nobody renamed or dropped maps to itself, which
  // the caller resolves with `?? the declared identity`.
  const current = new Map<string, Identifier | null>();
  // The reverse index, so that a second rename of the same table follows the
  // first rather than starting over.
  const origin = new Map<string, string>();

  const move = (from: Identifier, to: Identifier | null) => {
    const chained = origin.get(key(from));
    if (chained === undefined && current.has(key(from))) {
      // This name was already renamed (or dropped) away earlier in the
      // interval, so a table now carrying it is a different one, created
      // after the interval began. No declaration can be referring to it --
      // a declaration names a table the subscriber knew at its watermark --
      // so it is not tracked.
      return;
    }
    const start = chained ?? key(from);
    current.set(start, to);
    origin.delete(key(from));
    if (to !== null) {
      origin.set(key(to), start);
    }
  };

  for (const change of changes) {
    if (change.tag === 'rename-table') {
      move(change.old, change.new);
    } else if (change.tag === 'drop-table') {
      move(change.id, null);
    }
  }
  return current;
}
