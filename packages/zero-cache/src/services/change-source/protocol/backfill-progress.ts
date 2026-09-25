import {deepEqual} from '../../../../../shared/src/json.ts';
import type {
  BackfillProgressMark,
  BackfillRequest,
  Identifier,
} from './current.ts';

/**
 * Whether two progress marks are on the same timeline, and are thus comparable.
 */
function sameTimeline(a: BackfillProgressMark, b: BackfillProgressMark) {
  return a.timeline === b.timeline;
}

/** Returns the earlier of two marks on the same timeline. */
function earlier(
  a: BackfillProgressMark,
  b: BackfillProgressMark,
): BackfillProgressMark {
  return b.progressMark < a.progressMark ? b : a;
}

/** Returns the later of two marks on the same timeline. */
function later(
  a: BackfillProgressMark,
  b: BackfillProgressMark,
): BackfillProgressMark {
  return b.progressMark > a.progressMark ? b : a;
}

/**
 * The result of applying a backfill message to a column's tracked progress.
 * If `accept` is true, `progress` is the column's new progress.
 */
export type AcceptResult =
  | {accept: true; progress: BackfillProgressMark | undefined}
  | {accept: false};

/**
 * Applies the continuity policy of `backfill` (and `backfill-completed`)
 * messages to a column's `own` tracked progress, where `undefined` means that
 * the column has not received any backfill data (i.e. it needs a backfill
 * from scratch).
 *
 * * Messages without `progressMarks` are from pre-v8 replication-managers
 *   and are accepted unconditionally (i.e. the legacy behavior), with no
 *   change to the tracked progress.
 * * A message without a `previous` mark starts a backfill from scratch (or on
 *   a new timeline), and is always accepted.
 * * Otherwise, the message is only accepted if it is on the same timeline as
 *   `own`, and continues from a position at or before `own` (i.e. it does not
 *   create a gap).
 *
 * `current` is unset for `backfill-completed` messages, which do not advance
 * the progress.
 */
export function acceptBackfill(
  own: BackfillProgressMark | undefined,
  marks:
    | {
        previous?: BackfillProgressMark | undefined;
        current?: BackfillProgressMark;
      }
    | undefined,
): AcceptResult {
  if (marks === undefined) {
    return {accept: true, progress: own}; // legacy (pre-v8)
  }
  const {previous, current} = marks;
  // Starting from scratch is always accepted, as there can be no gap.
  if (previous === undefined) {
    return {accept: true, progress: current};
  }
  if (
    own === undefined ||
    !sameTimeline(own, previous) ||
    previous.progressMark > own.progressMark
  ) {
    return {accept: false};
  }
  return {
    accept: true,
    progress:
      current === undefined
        ? own
        : sameTimeline(own, current)
          ? later(own, current)
          : current,
  };
}

/**
 * Returns whether a stream whose progress (for a column) is at `stream`
 * will deliver all of the data needed by a subscriber whose progress (for the
 * same column) is at `subscriber`. A `stream` progress of `undefined` indicates
 * a backfill that will start from scratch, which covers all subscribers.
 */
export function covers(
  stream: BackfillProgressMark | undefined,
  subscriber: BackfillProgressMark | undefined,
): boolean {
  return (
    stream === undefined ||
    (subscriber !== undefined &&
      sameTimeline(stream, subscriber) &&
      stream.progressMark <= subscriber.progressMark)
  );
}

/**
 * Returns the position from which a backfill for the `req` must start
 * in order to satisfy the progress of all of its columns, i.e. the earliest
 * progress mark, or `undefined` (i.e. from scratch) if any column has no
 * progress or if the columns are on different timelines.
 */
export function resumePoint(
  req: BackfillRequest,
): BackfillProgressMark | undefined {
  let point: BackfillProgressMark | undefined;
  for (const {progress} of Object.values(req.columns)) {
    if (progress === undefined) {
      return undefined;
    }
    if (point === undefined) {
      point = progress;
    } else if (!sameTimeline(point, progress)) {
      return undefined;
    } else {
      point = earlier(point, progress);
    }
  }
  return point;
}

/**
 * Returns a copy of the `req` with the progress of every column set to
 * the request's {@link resumePoint}, i.e. the position from which the
 * backfill of the request will actually start.
 */
export function withResumePoint(req: BackfillRequest): BackfillRequest {
  const progress = resumePoint(req);
  return {
    table: req.table,
    columns: Object.fromEntries(
      Object.entries(req.columns).map(([col, {id}]) => [
        col,
        progress === undefined ? {id} : {id, progress},
      ]),
    ),
  };
}

/**
 * Merges two BackfillRequests for the same table into one that satisfies
 * both. Both requests are expected to reflect the same (current) upstream
 * schema. For columns in both requests, the earlier progress mark is chosen,
 * or no progress mark (i.e. from scratch) if they are on different timelines.
 * If a column's backfill IDs differ (which is unexpected), the ID of `req1`
 * is chosen, and the column is backfilled from scratch.
 */
export function getTableSuperset(
  req1: BackfillRequest,
  req2: BackfillRequest,
): BackfillRequest {
  const columns = {...req1.columns};
  for (const [name, col] of Object.entries(req2.columns)) {
    const exists = columns[name];
    if (!exists) {
      columns[name] = col;
      continue;
    }
    const {id, progress} = exists;
    if (
      progress === undefined ||
      col.progress === undefined ||
      !sameTimeline(progress, col.progress) ||
      !deepEqual(id, col.id)
    ) {
      // Different timelines (or IDs). Start from scratch.
      columns[name] = {id};
    } else {
      columns[name] = {id, progress: earlier(progress, col.progress)};
    }
  }
  return {
    table: {
      ...req1.table,
      metadata: req1.table.metadata ?? req2.table.metadata,
    },
    columns,
  };
}

function tableKey({schema, name}: Identifier) {
  return JSON.stringify([schema, name]);
}

/**
 * Merges the BackfillRequests of multiple parties (e.g. subscribers) into
 * one request per table, using {@link getTableSuperset}.
 */
export function getSuperset(
  ...requestSets: Iterable<BackfillRequest>[]
): BackfillRequest[] {
  const merged = new Map<string, BackfillRequest>();
  for (const requests of requestSets) {
    for (const req of requests) {
      const key = tableKey(req.table);
      const existing = merged.get(key);
      merged.set(key, existing ? getTableSuperset(existing, req) : req);
    }
  }
  return [...merged.values()];
}
