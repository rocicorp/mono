/** What an advancement has pushed to a query so far. */
export type QueryPushProgress = {
  readonly queryID: string;
  /** The time spent processing pushes to the query in this advancement. */
  readonly pushTimeMs: number;
  /** The number of changes pushed to the query in this advancement. */
  readonly changes: number;
  /** The query's hydration time, i.e. what rebuilding it is expected to cost. */
  readonly hydrationTimeMs: number;
};

/** Whether to stop pushing an advancement's changes to a query. */
export type ShouldCutOffPush = (progress: QueryPushProgress) => boolean;

/**
 * The least a query's hydration is taken to cost when deciding whether to
 * stop pushing to it. Small queries hydrate in well under a millisecond,
 * where one push can take longer than the hydration did on timer noise alone,
 * and hydrating again saves too little to be worth its fixed costs.
 */
export const MIN_PUSH_CUTOFF_MS = 2;

/**
 * Whether to stop pushing an advancement's changes to a query and hydrate it
 * again instead: once the pushes have taken as long as the hydration is
 * expected to.
 *
 * This is the ski-rental strategy (rent until the rent paid equals the
 * purchase price, then buy). Whatever the actual costs turn out to be, the
 * query costs at most about twice the cheaper of pushing every change and
 * hydrating from the start, plus the one change that crossed the line. It
 * needs no estimate of what the remaining changes will cost.
 */
export function shouldCutOffPush({
  pushTimeMs,
  hydrationTimeMs,
}: Pick<QueryPushProgress, 'pushTimeMs' | 'hydrationTimeMs'>): boolean {
  return pushTimeMs >= Math.max(hydrationTimeMs, MIN_PUSH_CUTOFF_MS);
}

/**
 * Bounds the cost of one advancement query by query: each query is pushed
 * changes until {@link ShouldCutOffPush} says to stop, and is then hydrated
 * again once the advancement is done.
 *
 * An advancement with a cutoff is not subject to the client group's
 * advancement time limit, which resets every pipeline of the client group.
 * The two are alternatives and are never applied together.
 */
export class PushCutoff {
  readonly #shouldCutOff: ShouldCutOffPush;
  /** The queries pushed to since the last {@link takeCutOff}. */
  readonly #pushed = new Set<string>();

  constructor(shouldCutOff: ShouldCutOffPush = shouldCutOffPush) {
    this.#shouldCutOff = shouldCutOff;
  }

  /** Called for each push to a query. */
  recordPush(queryID: string): void {
    this.#pushed.add(queryID);
  }

  /**
   * Called between changes, when no push is in progress. Returns the queries
   * to stop pushing to, of those pushed to since the last call.
   *
   * @param progressOf The progress of a query, or `undefined` if it is no
   *        longer being pushed to.
   */
  takeCutOff(
    progressOf: (queryID: string) => QueryPushProgress | undefined,
  ): QueryPushProgress[] {
    if (this.#pushed.size === 0) {
      return [];
    }
    const cutOff: QueryPushProgress[] = [];
    for (const queryID of this.#pushed) {
      const progress = progressOf(queryID);
      if (progress !== undefined && this.#shouldCutOff(progress)) {
        cutOff.push(progress);
      }
    }
    this.#pushed.clear();
    return cutOff;
  }
}
