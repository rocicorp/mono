/**
 * Where a stream connection's initialization parameters come from.
 *
 * A change-streamer starts a stream from three things read at one position: the
 * watermark to resume at, the {@link BackfillRequest}s to hand the change
 * source, and the cookie set those requests were derived from. Postgres has
 * always supplied all three (`Storer.getStartStreamInitializationParameters()`).
 * The point of this workstream is that a restored replica can supply them
 * instead, which is what lets the `cdc` schema be decommissioned.
 *
 * This is the two-source stage. Both are computed on every stream connection,
 * compared, and Postgres's is used — so the replica-derived path runs in
 * production, at full rate, before anything depends on it. The flip is
 * `initFromPgChangeLog: false`, not a rewrite.
 *
 * The shape is {@link UpstreamAcker}'s, deliberately: two stores, one output, a
 * flag per store, and the Postgres flag carrying the marker that retires it.
 * That class solved the identical problem for upstream ACKs earlier in this
 * workstream.
 *
 * ### Why the comparison is a fold and not an equality
 *
 * The replica is a subscriber downstream of the change-streamer's write loop,
 * so its state version trails the log's head by however long the replicator is
 * behind. A raw difference between the two cookie sets is therefore the normal
 * case and says nothing. What is checked instead is that the replica's set,
 * carried forward over exactly the schema changes the log holds in the interval,
 * equals Postgres's set at Postgres's watermark. The log holds precisely those
 * transactions, schema changes are rare, and the check is exact rather than
 * approximate — so it also happens to be the strongest available test of the
 * premise that three interpreters of one fold agree.
 */

import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {must} from '../../../../shared/src/must.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {SchemaChange} from '../change-source/protocol/current/data.ts';
import {schemaChangeTags} from '../change-source/protocol/current/schema-change-tags.ts';
import type {BackfillRequest} from '../change-source/protocol/current/upstream.ts';
import {foldCookies, type CookieSet} from '../replicator/change-log-cookies.ts';
import {CHANGE_LOG_STREAM_TABLE} from '../replicator/change-log-db.ts';
import {
  readBackfillRequests,
  readReplicaCookies,
} from '../replicator/schema/backfilling.ts';
import {getReplicationState} from '../replicator/schema/replication-state.ts';
import {SQLITE_CHANGE_LOG_BOUNDARY_SQL} from './sqlite-change-log-reader.ts';

/** Everything one stream connection starts from, read at one position. */
export type InitializationParameters = {
  readonly lastWatermark: string;
  readonly backfillRequests: BackfillRequest[];
  readonly cookies: CookieSet;
};

/**
 * - `equal` — the two stores were at the same watermark and held the same
 *   cookies. Nothing had to be folded, which is the strongest of the four.
 * - `equal-after-fold` — the replica trailed, and carrying its set forward over
 *   the log's schema changes in the interval reproduced Postgres's exactly.
 *   The expected steady-state result.
 * - `divergent` — the two disagree at the same position. Alert; investigate
 *   individually.
 * - `inconclusive` — the log does not span the interval, so the fold cannot be
 *   performed. Routine on a cold log, and not a finding.
 * - `error` — the comparison itself failed: the replica could not be read, or
 *   the fold threw. Not one of the four classifications, because it is a
 *   statement about this process rather than about the two stores. Kept
 *   separate rather than folded into `inconclusive` so that a change-streamer
 *   that has silently stopped being able to read its replica is visible before
 *   that read becomes the only source.
 */
export type InitComparisonResult =
  | 'equal'
  | 'equal-after-fold'
  | 'divergent'
  | 'inconclusive'
  | 'error';

export type ChangeLogInitializerSources = {
  /** `Storer.getStartStreamInitializationParameters()`. */
  readonly pgChangeLog: () => Promise<InitializationParameters>;
  /** Typically {@link replicaInitializationSource}. */
  readonly replica: () => InitializationParameters;
  /**
   * The change log, for the fold. `undefined` before the stream loop's first
   * reconcile has created it, and again once the writer has failed soft and
   * deleted it — both of which make the comparison `inconclusive` rather than
   * an error, since neither says anything about the two stores.
   */
  readonly changeLog: () => Database | undefined;
};

type Opts = {
  /**
   * Read the resume point from the Postgres change log. It is authoritative
   * whenever it is read.
   */
  // TODO: set false when retiring PG
  readonly initFromPgChangeLog: boolean;
  /**
   * Read it from the replica as well. Until the flag above goes false this is
   * computed and compared but not used, which is the whole of slice C4.
   */
  readonly initFromReplica: boolean;
};

/**
 * The most change-log rows one comparison will scan.
 *
 * The interval is `(replicaWatermark, pgWatermark]`, so it is normally a
 * handful of transactions — but a replicator that has been stalled can leave it
 * arbitrarily large, and this runs on the stream loop between reconciliation and
 * `startStream`, where latency delays the resumption of replication. Over the
 * cap the comparison declines rather than pays: a chronically lagging replicator
 * shows up as `inconclusive`, which is charted, and not as a stall.
 */
const MAX_FOLD_SCAN_ROWS = 100_000;

export class ChangeLogInitializer {
  readonly #lc: LogContext;
  readonly #initFromPgChangeLog: boolean;
  readonly #initFromReplica: boolean;
  readonly #sources: ChangeLogInitializerSources;

  readonly #resultCounter = getOrCreateCounter(
    'replica',
    'sqlite_change_log.init_compare_result',
    'Comparisons of the replica-derived stream initialization parameters ' +
      'against the Postgres-derived ones, by classification.',
  );

  /**
   * Set by {@link initialize} when both sources ran, consumed by
   * {@link compare}. The two are separate calls because the fold reads the
   * change log and the change log is only in agreement with the resume
   * watermark *after* it has been reconciled against it — before that it can
   * hold phantom transactions, another replica's history, or nothing at all,
   * none of which would be a divergence between the two stores.
   */
  #pending:
    | {pg: InitializationParameters; replica: InitializationParameters}
    | undefined;

  constructor(
    lc: LogContext,
    {initFromPgChangeLog, initFromReplica}: Opts,
    sources: ChangeLogInitializerSources,
  ) {
    assert(
      initFromPgChangeLog || initFromReplica,
      `At least one of initFromPgChangeLog or initFromReplica must be true`,
    );
    this.#lc = lc.withContext('component', 'change-log-initializer');
    this.#initFromPgChangeLog = initFromPgChangeLog;
    this.#initFromReplica = initFromReplica;
    this.#sources = sources;
  }

  /**
   * Reads every enabled source and returns the authoritative one. When both are
   * enabled the replica's is held for {@link compare}, which the caller runs
   * once the log has been reconciled against the returned watermark.
   */
  async initialize(): Promise<InitializationParameters> {
    const pg = this.#initFromPgChangeLog
      ? await this.#sources.pgChangeLog()
      : undefined;
    const replica = this.#initFromReplica
      ? this.#readReplica(pg === undefined)
      : undefined;
    this.#pending = pg && replica ? {pg, replica} : undefined;
    // `must` rather than an `assert` on the flags: the constructor already
    // rejects the case where neither is enabled.
    return pg ?? must(replica, 'no change-log initialization source');
  }

  /**
   * Compares the two derivations of the last {@link initialize}, reports the
   * classification, and discards them. A no-op unless both sources are enabled,
   * so the caller does not have to know which stage of the rollout it is in.
   *
   * Must run after the change log has been reconciled against the watermark
   * {@link initialize} returned. Nothing acts on the result: Postgres remains
   * authoritative until C5.
   */
  compare(): InitComparisonResult | undefined {
    const pending = this.#pending;
    this.#pending = undefined;
    if (pending === undefined) {
      return undefined;
    }
    const {pg, replica} = pending;
    let result: InitComparisonResult;
    try {
      result = this.#classify(pg, replica);
    } catch (e) {
      this.#lc.warn?.('failed to compare change-log initialization sources', e);
      result = 'error';
    }
    this.#resultCounter.add(1, {result});
    this.#lc.debug?.('compared change-log initialization sources', {
      changeLogInitCompare: {
        result,
        pgWatermark: pg.lastWatermark,
        replicaWatermark: replica.lastWatermark,
      },
    });
    return result;
  }

  #readReplica(required: boolean): InitializationParameters | undefined {
    try {
      return this.#sources.replica();
    } catch (e) {
      if (required) {
        throw e;
      }
      // Observational until the flip, so a replica that cannot be read must not
      // stop replication -- but it must not be quiet either, since the flip
      // makes this read the only source.
      this.#lc.warn?.(
        'failed to derive change-log initialization from the replica',
        e,
      );
      this.#resultCounter.add(1, {result: 'error'});
      return undefined;
    }
  }

  #classify(
    pg: InitializationParameters,
    replica: InitializationParameters,
  ): InitComparisonResult {
    if (replica.lastWatermark > pg.lastWatermark) {
      // Transactions reach subscribers before the storer's commit, so a replica
      // read taken while one is in flight can be a transaction ahead. The fold
      // only runs forward, so there is nothing to be said either way.
      return 'inconclusive';
    }
    if (replica.lastWatermark === pg.lastWatermark) {
      return this.#report(pg, replica, replica.cookies, 'equal');
    }

    const db = this.#sources.changeLog();
    if (db === undefined || !spansInterval(db, replica.lastWatermark)) {
      return 'inconclusive';
    }
    const changes = readSchemaChanges(
      db,
      replica.lastWatermark,
      pg.lastWatermark,
    );
    if (changes === undefined) {
      return 'inconclusive'; // over MAX_FOLD_SCAN_ROWS
    }
    return this.#report(
      pg,
      replica,
      foldCookies(replica.cookies, changes),
      'equal-after-fold',
      changes.length,
    );
  }

  #report(
    pg: InitializationParameters,
    replica: InitializationParameters,
    derived: CookieSet,
    whenEqual: InitComparisonResult,
    foldedChanges = 0,
  ): InitComparisonResult {
    const expected = canonicalizeCookies(pg.cookies);
    const actual = canonicalizeCookies(derived);
    if (expected === actual) {
      return whenEqual;
    }
    // Both sets are normally empty and are at most a handful of rows, so they
    // are logged whole: a divergence is investigated individually (`P§:10`)
    // and the two renderings are the whole of what there is to look at.
    this.#lc.error?.(
      'the replica-derived change-log cookies do not match Postgres',
      {
        changeLogInitCompare: {
          pgWatermark: pg.lastWatermark,
          replicaWatermark: replica.lastWatermark,
          foldedChanges,
          expected,
          actual,
        },
      },
    );
    return 'divergent';
  }
}

/**
 * Reads the replica's initialization parameters in one snapshot, which is
 * invariant 15 on the replica side: the state version and the cookies are only
 * meaningful together.
 */
export function readReplicaInitializationParameters(
  db: Database,
): InitializationParameters {
  const runner = new StatementRunner(db);
  runner.begin(); // deferred, i.e. one read snapshot for the three statements
  try {
    return {
      lastWatermark: getReplicationState(runner).stateVersion,
      backfillRequests: readBackfillRequests(db),
      cookies: readReplicaCookies(db),
    };
  } finally {
    if (db.inTransaction) {
      runner.rollback();
    }
  }
}

/**
 * The replica source for {@link ChangeLogInitializer}, opening and closing the
 * file per stream connection rather than holding a handle on a database another
 * process owns.
 *
 * The handle is read-only, which is invariant 19 stated in the API: the
 * replica's cookie tables are written by the change-processor and by nothing
 * else. Note that a read-only SQLite connection still needs to attach the
 * WAL's `-shm`, so this can fail if it opens at a moment when no writer holds
 * the replica; that surfaces as the `error` classification, which is the point
 * of running the path under observation before anything depends on it.
 */
export function replicaInitializationSource(
  lc: LogContext,
  replicaFile: string,
): () => InitializationParameters {
  return () => {
    const db = new Database(
      lc.withContext('component', 'change-log-initializer'),
      replicaFile,
      {readonly: true},
    );
    try {
      return readReplicaInitializationParameters(db);
    } finally {
      db.close();
    }
  };
}

/**
 * Whether the log holds the complete interval above `fromWatermark`, using the
 * same predicate catchup uses to decide `too-old`: the log both reaches back
 * that far and contains that transaction's `commit` row, so the next row after
 * it is the first change of the interval rather than the middle of a
 * transaction the log only partly holds.
 */
function spansInterval(db: Database, fromWatermark: string): boolean {
  if (!streamTableExists(db)) {
    return false;
  }
  const row = db
    .prepare(/*sql*/ `
      SELECT
        (SELECT min("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}")
          AS "minWatermark",
        coalesce((${SQLITE_CHANGE_LOG_BOUNDARY_SQL}), 0) AS "boundaryExists"
    `)
    .get<{minWatermark: string | null; boundaryExists: number}>({
      fromWatermark,
    });
  return (
    row.minWatermark !== null &&
    fromWatermark >= row.minWatermark &&
    row.boundaryExists === 1
  );
}

const SCHEMA_CHANGE_TAG_LIST = schemaChangeTags
  .map(tag => `'${tag}'`)
  .join(', ');

/**
 * The log's schema changes over `(after, through]`, in stream order, or
 * `undefined` if the interval is too large to scan (see
 * {@link MAX_FOLD_SCAN_ROWS}).
 *
 * The tag is stored beside the verbatim change so this scan never parses the
 * payloads of ordinary data changes. The row count is still checked first so a
 * stalled replicator cannot make the range scan unbounded.
 *
 * The count is itself capped, at one row past the cap it is deciding. A bare
 * `count(*)` over the interval is `O(interval)`, so the unbounded interval this
 * exists to decline would still be walked in full before being declined —
 * which is the latency the cap is here to avoid, not a cheaper form of it. The
 * `LIMIT` makes the decision `O(MAX_FOLD_SCAN_ROWS)`, and the comparison is
 * unaffected: the subquery returns the true count whenever it is within the
 * cap, and `cap + 1` whenever it is not.
 */
function readSchemaChanges(
  db: Database,
  after: string,
  through: string,
): SchemaChange[] | undefined {
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
          AND "tag" IN (${SCHEMA_CHANGE_TAG_LIST})
        ORDER BY "watermark", "pos"
    `)
    .all<{change: string}>(after, through)
    .map(({change}) => BigIntJSON.parse(change) as SchemaChange);
}

function streamTableExists(db: Database): boolean {
  return (
    db
      .prepare(/*sql*/ `
        SELECT 1 FROM "sqlite_master" WHERE "type" = 'table' AND "name" = ?
      `)
      .get<{1: number} | undefined>(CHANGE_LOG_STREAM_TABLE) !== undefined
  );
}

/**
 * A canonical rendering of a whole cookie set: rows sorted by primary key,
 * object keys sorted at every depth.
 *
 * Both the equality check and the `divergent` log line fall out of this one
 * value, and neither can be defeated by a difference that is not a difference.
 * Postgres stores the cookie documents as `jsonb`, which does not preserve key
 * order, and the two readers order their rows in SQL in two engines under two
 * collations -- `storer.ts` pins `COLLATE "C"` for exactly that reason. Sorting
 * again here means an ordering or key-order regression can never be reported as
 * a divergence, which is a class of false alert that would cost more than it
 * caught.
 */
function canonicalizeCookies(cookies: CookieSet): string {
  return BigIntJSON.stringify({
    tableMetadata: byKey(cookies.tableMetadata, c => [c.schema, c.table]).map(
      c => [c.schema, c.table, sortKeys(c.metadata)],
    ),
    backfilling: byKey(cookies.backfilling, c => [
      c.schema,
      c.table,
      c.column,
    ]).map(c => [c.schema, c.table, c.column, sortKeys(c.backfill)]),
  });
}

function byKey<T>(rows: readonly T[], key: (row: T) => string[]): T[] {
  return rows
    .map(row => [JSON.stringify(key(row)), row] as const)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([, row]) => row);
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortKeys);
  }
  if (value === null || typeof value !== 'object') {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => [k, sortKeys(v)]),
  );
}
