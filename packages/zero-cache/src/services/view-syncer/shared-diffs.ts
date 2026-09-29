import type {LogContext} from '@rocicorp/logger';
import {stringify} from '../../../../shared/src/bigint-json.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import {estimateBytes} from '../../../../zqlite/src/pending-delta.ts';
import {computeZqlSpecs} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../../db/specs.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {AppID} from '../../types/shards.ts';
import type {SnapshotRowCache} from './snapshot-row-cache.ts';
import {ResetPipelinesSignal, Snapshotter, type Change} from './snapshotter.ts';

/**
 * The changes between two consecutive versions that the producer of a
 * {@link SharedDiffs} advanced through, as a {@link SnapshotDiff} would
 * produce them for a client group that observes every table.
 */
export type Segment = {
  readonly from: string;
  readonly to: string;
  readonly changes: readonly Change[];
  readonly bytes: number;
  /** The number of leases holding the segment. */
  refs: number;
  /** Removed from the ring. Its bytes are released once `refs` is 0. */
  evicted: boolean;
};

/**
 * The segments that cover a client group's advancement. They stay counted
 * against the budget until {@link release} is called, which may be called
 * more than once.
 */
export interface SegmentLease {
  readonly segments: readonly Segment[];
  /** The number of changes in the segments. */
  readonly changes: number;
  release(): void;
}

export type SharedDiffsOptions = {
  /**
   * The maximum estimated bytes of the segments that are either in the ring
   * or held by a lease.
   */
  readonly maxBytes: number;
  /** The most changes a segment may have; a larger advance is a gap. */
  readonly maxSegmentChanges?: number | undefined;
  /**
   * The most estimated bytes a segment may have; a larger advance is a gap.
   * Defaults to a quarter of {@link maxBytes}.
   */
  readonly maxSegmentBytes?: number | undefined;
  /** The most segments kept in the ring. */
  readonly maxSegments?: number | undefined;
  /** How often to log what was shared, in milliseconds. 0 turns it off. */
  readonly logIntervalMs?: number | undefined;
  /**
   * How long the producer may go without a client group advancing before it
   * closes its snapshots, in milliseconds. A snapshot that is held open
   * keeps the replicator from reusing the WAL files after it. 0 keeps them.
   */
  readonly idleMs?: number | undefined;
};

const DEFAULT_MAX_SEGMENT_CHANGES = 10_000;
const DEFAULT_MAX_SEGMENTS = 512;
const DEFAULT_IDLE_MS = 10_000;

/** Why an advancement did not use segments. */
type Miss =
  | 'empty' // there were no changes to share
  | 'specs' // the client group computed different table specs
  | 'behind' // the segments it needs have left the ring
  | 'gap' // an advance of the producer was not kept as a segment
  | 'unaligned'; // its version is not one the producer advanced to

/** Why an advance of the producer was not kept as a segment. */
type Gap =
  | 'size' // too many changes or bytes for one segment
  | 'budget' // the segments held by leases fill the budget
  | 'reset' // a schema change, truncate or permissions change
  | 'error';

/**
 * Computes the diff between two versions of the replica once for all the
 * client groups of a sync worker, rather than once per client group.
 *
 * Each client group's {@link Snapshotter} advances at its own pace: whenever
 * it is notified of a new version and is not busy hydrating, to whatever the
 * head of the replica is at that moment. Its diff is computed from the change
 * log: the entries between its two versions, the new value of each row and
 * the rows it replaces, converted to ZQL values. That work is the same for
 * every client group that advances over the same versions.
 *
 * A `SharedDiffs` is a producer with its own snapshots. When a client group
 * advances, the producer advances to head too, if it is behind, and keeps the
 * changes it advanced through as a segment. A client group whose previous and
 * new versions are both versions the producer advanced to replays the
 * segments in between, in order, instead of computing its own diff. Pace
 * stays per client group; only the work is shared. Replaying segments one
 * after another reaches the same state as the client group's own diff, which
 * keeps just the last change of each row: a row that changed in several
 * segments is applied once per segment.
 *
 * Segments are built as the producer advances, since the change log keeps one
 * entry per row and so cannot reproduce an older segment later.
 *
 * Memory is bounded by {@link SharedDiffsOptions.maxBytes}, which counts a
 * segment from when it is built until it has left the ring and no lease holds
 * it. An advance that does not fit, is too large for one segment, or
 * contains a table-wide change is not kept (a gap), and client groups whose
 * advancement spans it compute their own diffs as before.
 */
export class SharedDiffs {
  readonly #lc: LogContext;
  readonly #newSnapshotter: () => Snapshotter;
  #snapshotter: Snapshotter;
  readonly #maxBytes: number;
  readonly #maxSegmentChanges: number;
  readonly #maxSegmentBytes: number;
  readonly #maxSegments: number;
  readonly #logIntervalMs: number;
  readonly #specs = new Map<string, LiteAndZqlSpec>();
  readonly #allTableNames = new Set<string>();
  #fingerprint = '';
  /** The ring of segments, by `from` version, oldest first. */
  readonly #segments = new Map<string, Segment>();
  /** The version each client group is at, i.e. where it advances from. */
  readonly #consumers = new Map<object, string>();
  #liveBytes = 0;

  readonly #advances = getOrCreateCounter(
    'sync',
    'ivm.shared-diffs.advances',
    'Number of client group advancements that replayed shared diff segments ' +
      '(result=shared) or computed their own diff (result=own, with a reason)',
  );
  readonly #built = getOrCreateCounter(
    'sync',
    'ivm.shared-diffs.segments',
    'Number of advances of the shared diff producer kept as a segment ' +
      '(result=kept) or not (result=gap, with a reason)',
  );
  #stats = newStats();
  #lastLogMs = Date.now();
  #lastAcquireMs = Date.now();
  readonly #idleTimer: ReturnType<typeof setInterval> | undefined;

  constructor(
    lc: LogContext,
    dbFile: string,
    appID: AppID,
    options: SharedDiffsOptions,
    rowCache?: SnapshotRowCache,
  ) {
    this.#lc = lc;
    this.#newSnapshotter = () =>
      new Snapshotter(lc, dbFile, appID, undefined, rowCache);
    this.#snapshotter = this.#newSnapshotter();
    this.#maxBytes = options.maxBytes;
    this.#maxSegmentChanges =
      options.maxSegmentChanges ?? DEFAULT_MAX_SEGMENT_CHANGES;
    this.#maxSegmentBytes = options.maxSegmentBytes ?? options.maxBytes / 4;
    this.#maxSegments = options.maxSegments ?? DEFAULT_MAX_SEGMENTS;
    this.#logIntervalMs = options.logIntervalMs ?? 0;
    const idleMs = options.idleMs ?? DEFAULT_IDLE_MS;
    if (idleMs > 0) {
      this.#idleTimer = setInterval(() => this.#closeIfIdle(idleMs), idleMs);
      this.#idleTimer.unref?.();
    }
  }

  /**
   * Closes the producer's snapshots if no client group has advanced for
   * `idleMs`. It starts again at head when one does, and client groups
   * behind that version compute their own diffs until they reach it.
   */
  #closeIfIdle(idleMs: number) {
    if (
      this.#snapshotter.initialized() &&
      Date.now() - this.#lastAcquireMs >= idleMs
    ) {
      this.#lc.debug?.('closing the snapshots of an idle diff producer');
      this.#snapshotter.destroy();
      this.#snapshotter = this.#newSnapshotter();
    }
  }

  /**
   * Starts the producer at the current version of the replica, if it has
   * not started. Otherwise it starts when a client group first advances.
   */
  start(): this {
    this.#lastAcquireMs = Date.now();
    this.#catchUp('');
    return this;
  }

  /**
   * Records the version a client group is at, from which its next
   * advancement starts. Segments that start before the version of every
   * client group are dropped.
   */
  track(consumer: object, version: string) {
    this.#consumers.set(consumer, version);
  }

  /** Stops tracking a client group that no longer advances. */
  untrack(consumer: object) {
    this.#consumers.delete(consumer);
  }

  /** The estimated bytes of the segments in the ring or held by leases. */
  get liveBytes(): number {
    return this.#liveBytes;
  }

  /**
   * Returns the segments that take a client group from version `from` to
   * version `to`, or `undefined` if they do not cover exactly that range.
   * `specsFingerprint` is the {@link specsFingerprint} of the table specs the
   * client group converts rows with, which must be the producer's.
   */
  acquire(
    from: string,
    to: string,
    specsFingerprint: string,
  ): SegmentLease | undefined {
    this.#lastAcquireMs = Date.now();
    const lease = this.#acquire(from, to, specsFingerprint);
    this.#maybeLogStats();
    return lease;
  }

  #acquire(
    from: string,
    to: string,
    specsFingerprint: string,
  ): SegmentLease | undefined {
    // Before the check for an empty range, so that the producer starts at a
    // version where client groups are rather than after the next change.
    this.#catchUp(to);
    if (from >= to) {
      return this.#miss('empty');
    }
    if (specsFingerprint !== this.#fingerprint) {
      return this.#miss('specs');
    }
    const segments: Segment[] = [];
    let changes = 0;
    let version = from;
    while (version < to) {
      const segment = this.#segments.get(version);
      if (segment === undefined) {
        const [oldest] = this.#segments.values();
        return this.#miss(
          segments.length === 0 && (oldest === undefined || from < oldest.from)
            ? 'behind'
            : 'gap',
        );
      }
      segments.push(segment);
      changes += segment.changes.length;
      version = segment.to;
    }
    if (version !== to) {
      return this.#miss('unaligned');
    }
    for (const segment of segments) {
      segment.refs++;
    }
    this.#advances.add(1, {result: 'shared'});
    this.#stats.shared++;
    let released = false;
    return {
      segments,
      changes,
      release: () => {
        if (released) {
          return;
        }
        released = true;
        for (const segment of segments) {
          segment.refs--;
          if (segment.refs === 0 && segment.evicted) {
            this.#liveBytes -= segment.bytes;
          }
        }
      },
    };
  }

  #miss(reason: Miss): undefined {
    this.#advances.add(1, {result: 'own', reason});
    this.#stats.own[reason]++;
    return undefined;
  }

  /** Advances the producer to head if it has not reached `version`. */
  #catchUp(version: string) {
    if (!this.#snapshotter.initialized()) {
      this.#snapshotter.init();
      this.#computeSpecs();
    }
    if (this.#snapshotter.current().version < version) {
      this.#advance();
    }
  }

  #computeSpecs() {
    const {db} = this.#snapshotter.current();
    const fullTables = new Map<string, LiteTableSpec>();
    computeZqlSpecs(
      this.#lc,
      db.db,
      {includeBackfillingColumns: false},
      this.#specs,
      fullTables,
    );
    this.#allTableNames.clear();
    for (const table of fullTables.keys()) {
      this.#allTableNames.add(table);
    }
    this.#fingerprint = specsFingerprint(this.#specs);
  }

  #advance() {
    const diff = this.#snapshotter.advance(
      this.#specs,
      this.#allTableNames,
      undefined,
      'none', // The producer never writes to its snapshots.
    );
    const from = diff.prev.version;
    const to = diff.curr.version;
    if (diff.changes > this.#maxSegmentChanges) {
      this.#gap('size', from, to);
      return;
    }
    const changes: Change[] = [];
    let bytes = 0;
    try {
      for (const change of diff) {
        bytes += changeBytes(change);
        if (bytes > this.#maxSegmentBytes) {
          this.#gap('size', from, to);
          return;
        }
        changes.push(change);
      }
    } catch (e) {
      if (e instanceof ResetPipelinesSignal) {
        // Every client group resets, from its own diff. Later segments have
        // to be converted with the specs of the new schema.
        this.#gap('reset', from, to);
        this.#computeSpecs();
        return;
      }
      this.#lc.warn?.(`could not build a diff segment ${from} => ${to}`, e);
      this.#gap('error', from, to);
      return;
    }
    this.#trim();
    this.#evictFor(bytes);
    if (this.#liveBytes + bytes > this.#maxBytes) {
      this.#gap('budget', from, to);
      return;
    }
    this.#segments.set(from, {
      from,
      to,
      changes,
      bytes,
      refs: 0,
      evicted: false,
    });
    this.#liveBytes += bytes;
    this.#built.add(1, {result: 'kept'});
    this.#stats.kept++;
  }

  #gap(reason: Gap, from: string, to: string) {
    this.#lc.debug?.(`no diff segment for ${from} => ${to}: ${reason}`);
    this.#built.add(1, {result: 'gap', reason});
    this.#stats.gaps[reason]++;
  }

  /**
   * Removes the oldest segments from the ring until there is room for
   * `bytes` more, or none are left. A removed segment that a lease holds
   * stays counted until the lease is released.
   */
  #evictFor(bytes: number) {
    for (const segment of this.#segments.values()) {
      if (
        this.#liveBytes + bytes <= this.#maxBytes &&
        this.#segments.size < this.#maxSegments
      ) {
        return;
      }
      this.#evict(segment);
    }
  }

  /**
   * Removes the segments that no client group can replay: those that start
   * before the version of the client group that is furthest behind (all of
   * them, if there are no client groups).
   */
  #trim() {
    let oldest: string | undefined;
    for (const version of this.#consumers.values()) {
      if (oldest === undefined || version < oldest) {
        oldest = version;
      }
    }
    for (const segment of this.#segments.values()) {
      if (oldest !== undefined && segment.from >= oldest) {
        return;
      }
      this.#evict(segment);
    }
  }

  #evict(segment: Segment) {
    this.#segments.delete(segment.from);
    segment.evicted = true;
    if (segment.refs === 0) {
      this.#liveBytes -= segment.bytes;
    }
  }

  #maybeLogStats() {
    if (this.#logIntervalMs <= 0) {
      return;
    }
    const now = Date.now();
    if (now - this.#lastLogMs < this.#logIntervalMs) {
      return;
    }
    this.#lastLogMs = now;
    const stats = this.#stats;
    this.#stats = newStats();
    this.#lc.info?.(
      `shared diffs: ${stats.shared} advancements shared, own: ` +
        `${stringify(stats.own)}; segments kept ${stats.kept}, gaps: ` +
        `${stringify(stats.gaps)}; ${this.#segments.size} segments for ` +
        `${this.#consumers.size} client groups, ` +
        `${(this.#liveBytes / 1024 ** 2).toFixed(1)} MB held`,
    );
  }

  destroy() {
    clearInterval(this.#idleTimer);
    this.#segments.clear();
    this.#snapshotter.destroy();
  }
}

function newStats() {
  return {
    shared: 0,
    own: {empty: 0, specs: 0, behind: 0, gap: 0, unaligned: 0} satisfies Record<
      Miss,
      number
    >,
    kept: 0,
    gaps: {size: 0, budget: 0, reset: 0, error: 0} satisfies Record<
      Gap,
      number
    >,
  };
}

function changeBytes({prevValues, nextValue, rowKey}: Change): number {
  let bytes = 64 + estimateBytes(rowKey as Row);
  for (const row of prevValues) {
    bytes += estimateBytes(row);
  }
  if (nextValue !== null) {
    bytes += estimateBytes(nextValue);
  }
  return bytes;
}

/**
 * Identifies the table specs that a client group converts replicated rows
 * with. A client group may only replay segments built with the same specs.
 */
export function specsFingerprint(
  specs: ReadonlyMap<string, LiteAndZqlSpec>,
): string {
  return stringify(
    [...specs].toSorted(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
  );
}
