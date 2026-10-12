import {MIN_PUSH_CUTOFF_MS} from './push-cutoff.ts';

/**
 * The weight of the latest advancement in a query's cost per change. The
 * cost of a change depends on the row (one joined to many others costs more
 * than one that is filtered out), so it varies from one advancement to the
 * next, and workloads shift.
 */
const LATEST_WEIGHT = 0.3;

/**
 * Learns what it costs to push a change of each table through each query of
 * a client group, and estimates from it what pushing a batch of changes
 * would cost.
 *
 * The estimates only decide whether a query skips pushing and starts an
 * advancement on rebuild (see {@link shouldStartOnRebuild}). Nothing depends
 * on their accuracy: a query that starts on push when it should not have is
 * cut off (see `PushCutoff`), and one that starts on rebuild costs its
 * hydration time, as it would have at the cutoff.
 */
export class PushCostEstimator {
  /** Milliseconds per change, by query ID and table. */
  readonly #costs = new Map<string, Map<string, number>>();

  /**
   * Records what an advancement cost a query.
   *
   * @param tables The tables the query reads.
   * @param timeMsByTable The time spent pushing the changes of each table
   *        to the query. Absent for a table whose changes cost it nothing,
   *        e.g. because the rows did not match its filters.
   * @param changesByTable The entries of each table that the advancement
   *        read from the change log, whether or not they reached the query,
   *        or changed anything at all (a row that is inserted and deleted
   *        between two snapshots is an entry that changes nothing). These
   *        are the entries that {@link estimate} is given the count of.
   */
  record(
    queryID: string,
    tables: Iterable<string>,
    timeMsByTable: ReadonlyMap<string, number> | undefined,
    changesByTable: ReadonlyMap<string, number>,
  ): void {
    let costs = this.#costs.get(queryID);
    for (const table of tables) {
      const changes = changesByTable.get(table);
      if (!changes) {
        continue;
      }
      if (costs === undefined) {
        costs = new Map();
        this.#costs.set(queryID, costs);
      }
      const latest = (timeMsByTable?.get(table) ?? 0) / changes;
      const previous = costs.get(table);
      const cost =
        previous === undefined
          ? latest
          : previous + LATEST_WEIGHT * (latest - previous);
      costs.set(table, cost);
    }
  }

  /**
   * The estimated time to push `changes` to the query, or `undefined` if it
   * has never been pushed any change. A table that the query has never been
   * pushed a change of is estimated to cost nothing.
   *
   * @param tables The tables the query reads.
   * @param changes The number of changes to push, by table.
   */
  estimate(
    queryID: string,
    tables: Iterable<string>,
    changes: ReadonlyMap<string, number>,
  ): number | undefined {
    const costs = this.#costs.get(queryID);
    if (costs === undefined) {
      return undefined;
    }
    let total = 0;
    for (const table of tables) {
      total += (changes.get(table) ?? 0) * (costs.get(table) ?? 0);
    }
    return total;
  }

  /** Forgets what was learned about a query. */
  forget(queryID: string): void {
    this.#costs.delete(queryID);
  }
}

/**
 * Learns what it costs a client group to read a batch of changes, apart from
 * pushing them through its queries, and estimates from it what reading the
 * next one would cost.
 *
 * Reading has three parts, which do not cost the same entries (see
 * {@link ReadParts}): walking the change log, parsing the entries that
 * the walk returns, and looking up the rows of the entries of the tables
 * that are read. What each costs is learned per entry that it was done for.
 * A cost learned per entry of some other count is only right for a batch
 * with the same mix of entries: charged to the entries that are pushed, for
 * instance, the parsing of a batch that is mostly of tables that no query
 * reads makes the next batch that is not look many times as expensive as it
 * is.
 *
 * No query can be cut off for what this costs, as it is not a cost of any
 * one query. It is what pushing costs all of them together, on top of what
 * it costs each (see {@link shouldRebuildInsteadOfReading}).
 */
export class ReadCostEstimator {
  #walkMsPerEntry: number | undefined;
  #parseMsPerEntry: number | undefined;
  #lookupMsPerEntry: number | undefined;

  /**
   * Records what reading a batch cost. Only for a batch that was read to
   * its end.
   *
   * @param timeMs The time spent on each part of reading the batch.
   * @param entries The entries that each part was done for.
   */
  record(timeMs: ReadParts, entries: ReadParts): void {
    this.#walkMsPerEntry = learn(
      this.#walkMsPerEntry,
      timeMs.walk,
      entries.walk,
    );
    this.#parseMsPerEntry = learn(
      this.#parseMsPerEntry,
      timeMs.parse,
      entries.parse,
    );
    this.#lookupMsPerEntry = learn(
      this.#lookupMsPerEntry,
      timeMs.lookup,
      entries.lookup,
    );
  }

  /**
   * The estimated time to read a batch, or `undefined` if none has been
   * read yet.
   *
   * @param entries The entries that each part of reading the batch would be
   *        done for.
   */
  estimate(entries: ReadParts): number | undefined {
    if (this.#walkMsPerEntry === undefined) {
      return undefined;
    }
    return (
      entries.walk * this.#walkMsPerEntry +
      entries.parse * (this.#parseMsPerEntry ?? 0) +
      entries.lookup * (this.#lookupMsPerEntry ?? 0)
    );
  }
}

/**
 * A number for each part of reading a batch of changes: the entries of the
 * change log that the part is done for, or the time that it takes.
 */
export type ReadParts = {
  /**
   * Walking the change log, which is done for every entry since the
   * previous snapshot, whether or not the entry is of a table that is read:
   * the entries are in the order they were committed in, not grouped by
   * table.
   */
  readonly walk: number;
  /**
   * Parsing, which is done for every entry that the walk returns. That is
   * all of them, unless the walk is told which tables are read.
   */
  readonly parse: number;
  /**
   * Looking up the row in both snapshots, which is done for every entry of
   * a table that is read. This includes the entries that then turn out to
   * change nothing (e.g. a row inserted and deleted since the previous
   * snapshot).
   */
  readonly lookup: number;
};

function learn(
  msPerEntry: number | undefined,
  timeMs: number,
  entries: number,
): number | undefined {
  return entries > 0 ? decay(msPerEntry, timeMs / entries) : msPerEntry;
}

function decay(previous: number | undefined, latest: number): number {
  return previous === undefined
    ? latest
    : previous + LATEST_WEIGHT * (latest - previous);
}

/**
 * Whether the queries that are left to push a batch to should all be
 * hydrated again instead, without the batch being read: when reading it and
 * pushing it to them is estimated to cost at least `margin` times what
 * hydrating them does.
 *
 * This is {@link shouldStartOnRebuild} for the queries together, with what
 * reading the batch costs added to what pushing it costs each of them. A
 * query that few of the batch's changes reach is cheap to push to, but the
 * whole batch still has to be walked to find those changes.
 *
 * @param estimatedReadMs `undefined` if there is no estimate, in which case
 *        the batch is read.
 * @param queries What pushing the batch is estimated to cost each query
 *        (nothing, if it has no estimate), and what hydrating it took.
 */
export function shouldRebuildInsteadOfReading(
  estimatedReadMs: number | undefined,
  queries: Iterable<{
    readonly estimatedPushMs: number | undefined;
    readonly hydrationTimeMs: number;
  }>,
  margin: number,
): boolean {
  if (estimatedReadMs === undefined) {
    return false;
  }
  let pushMs = estimatedReadMs;
  let hydrationMs = 0;
  for (const query of queries) {
    pushMs += query.estimatedPushMs ?? 0;
    hydrationMs += Math.max(query.hydrationTimeMs, MIN_PUSH_CUTOFF_MS);
  }
  return hydrationMs > 0 && pushMs >= margin * hydrationMs;
}

/**
 * Whether a query should start an advancement on rebuild, i.e. be hydrated
 * again without being pushed any of the advancement's changes: when pushing
 * them is estimated to cost at least `margin` times what hydrating it does.
 *
 * The margin is for the imprecision of the estimate. A query without one
 * starts on push.
 */
export function shouldStartOnRebuild(
  estimatedPushMs: number | undefined,
  hydrationTimeMs: number,
  margin: number,
): boolean {
  return (
    estimatedPushMs !== undefined &&
    estimatedPushMs >= margin * Math.max(hydrationTimeMs, MIN_PUSH_CUTOFF_MS)
  );
}
