import {stringify} from '../../../../shared/src/bigint-json.ts';
import {getOrInsert} from '../../../../shared/src/map.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';

/**
 * The default maximum number of entries retained by a {@link SnapshotRowCache}.
 *
 * Each entry is a raw SQLite row (or the list of rows conflicting with
 * a row on its unique keys) read while computing a snapshot diff. A
 * replicated transaction of N rows produces up to 2N entries (the new value
 * and the previous value(s) of each row), so the cache should be large enough
 * to hold the largest transactions that are commonly replicated in order for
 * all of the client groups on a worker to share the reads.
 */
export const DEFAULT_MAX_SNAPSHOT_ROW_CACHE_ENTRIES = 50_000;

/**
 * Serializes a lookup key value (i.e. a SQLite-bound parameter). Numbers and
 * bigints serialize identically, as row keys parsed from the change log are
 * JSON numbers whereas the values read from SQLite are bigints; both address
 * the same row. Strings are prefixed to distinguish them from numbers.
 */
function serialize(v: unknown): string {
  switch (typeof v) {
    case 'string':
      return 's' + v;
    case 'number':
    case 'bigint':
      return String(v);
    case 'boolean':
      return v ? 'true' : 'false';
    case 'object':
      return v === null ? 'null' : 'o' + stringify(v);
    default:
      return 'u' + String(v);
  }
}

const HIT = {result: 'hit'} as const;
const MISS = {result: 'miss'} as const;

/**
 * A worker-wide, bounded cache of the rows that a `Snapshotter` reads from
 * replica snapshots when computing the diff between two versions of the
 * replica.
 *
 * Every client group (i.e. view-syncer) on a sync worker computes its own
 * diff from the shared `_zero.changeLog2`, and each change in the diff costs
 * two SQLite reads: the new value of the row (from the `curr` snapshot) and
 * the previous value(s) that it replaces (from the `prev` snapshot). The
 * reads for a given change are identical for every client group that
 * traverses it, so a replicated transaction of N rows costs N × 2 × (client
 * groups) reads per worker, most of which are redundant. This cache lets the
 * client groups on a worker share those reads.
 *
 * ### Keys
 *
 * Entries are keyed by the exact SQL statement and arguments of the read
 * (which encodes the table, the selected columns and the lookup keys), plus
 * a `tag` supplied by the caller that pins the result to a version of the
 * replica:
 *
 * - The new value of a row is the row as of the `stateVersion` recorded in
 *   its change log entry (its `_0_version` is asserted to equal that
 *   version), and is tagged with that `stateVersion`. It is thus shareable
 *   by all client groups regardless of the snapshots they are advancing
 *   between.
 * - Previous values are read from the `prev` snapshot at its version, and
 *   are tagged with that version (and, when the result can depend on the
 *   changes applied to the snapshot before the read, the `curr` version
 *   as well). See `Diff` in `snapshotter.ts` for the details.
 *
 * Because every key includes the version(s) that the result depends on,
 * entries never go stale and are never invalidated; they are only evicted.
 *
 * ### Eviction
 *
 * The cache is bounded by entry count and evicts in insertion (FIFO) order
 * rather than by recency. Diffs are traversed in change log order by every
 * client group, so the entries that will not be needed again are those that
 * were inserted the earliest (all groups have moved past them), while the
 * most recently inserted entries are the ones that the trailing groups have
 * yet to reach. Refreshing an entry on a hit would evict exactly the wrong
 * entries when a trailing group catches up.
 *
 * ### Sharing
 *
 * Cached values are shared by reference across client groups and must
 * therefore never be mutated by callers.
 */
export class SnapshotRowCache {
  readonly #maxEntries: number;
  readonly #entries = new Map<string, unknown>();
  // Interns the SQL text of each distinct read so that entry keys
  // do not repeat the (long) column list of every statement.
  readonly #sqlIDs = new Map<string, number>();
  readonly #reads = getOrCreateCounter(
    'sync',
    'ivm.snapshot-row-reads',
    'Row reads performed while computing snapshot diffs for IVM, labeled by whether the read was served by the worker-wide snapshot row cache.',
  );
  #hits = 0;
  #misses = 0;

  constructor(maxEntries = DEFAULT_MAX_SNAPSHOT_ROW_CACHE_ENTRIES) {
    this.#maxEntries = Math.max(0, maxEntries);
  }

  get size(): number {
    return this.#entries.size;
  }

  get maxEntries(): number {
    return this.#maxEntries;
  }

  stats(): {hits: number; misses: number; size: number} {
    return {hits: this.#hits, misses: this.#misses, size: this.#entries.size};
  }

  /**
   * Returns the cached result of the read identified by `tag`, `sql` and
   * `args`, or performs the read via `read()`, caching and returning its
   * result. `undefined` results (i.e. a missing row) are not cached.
   */
  getOrRead<T>(tag: string, sql: string, args: unknown[], read: () => T): T {
    const key = this.#key(tag, sql, args);
    const cached = this.#entries.get(key);
    if (cached !== undefined) {
      this.#hits++;
      this.#reads.add(1, HIT);
      return cached as T;
    }
    this.#misses++;
    this.#reads.add(1, MISS);
    const value = read();
    if (value !== undefined && this.#maxEntries > 0) {
      this.#entries.set(key, value);
      // Map iteration order is insertion order, so the first key is the oldest.
      for (const oldest of this.#entries.keys()) {
        if (this.#entries.size <= this.#maxEntries) {
          break;
        }
        this.#entries.delete(oldest);
      }
    }
    return value;
  }

  clear(): void {
    this.#entries.clear();
  }

  #key(tag: string, sql: string, args: unknown[]): string {
    const sqlID = getOrInsert(this.#sqlIDs, sql, this.#sqlIDs.size);
    let key = `${tag}\0${sqlID}`;
    // Include each value's length so separators inside string values cannot
    // make different argument arrays produce the same key.
    for (const arg of args) {
      const serialized = serialize(arg);
      key += `\0${serialized.length}:${serialized}`;
    }
    return key;
  }
}
