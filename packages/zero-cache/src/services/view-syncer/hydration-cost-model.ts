const HYDRATION_OBSERVATION_DECAY = 0.75;
const MAX_HYDRATION_WALL_MULTIPLIER = 20;
/**
 * The number of in-flight hydration passes beyond which a reset does not get
 * any more expensive. Without a limit, a process full of hydrating client
 * groups would price resets so high that advancement is never abandoned, no
 * matter how pathological.
 */
const MAX_PRICED_ACTIVE_HYDRATIONS = 4;

/**
 * A full hydration pass that has been counted as in flight by a
 * {@link HydrationCostModel}.
 */
export type HydrationPass = {
  /**
   * Reports that the pass completed, having spent `processTimeMs` hydrating
   * queries. The wall time is measured by the model.
   */
  observe(processTimeMs: number): void;

  /** Must be called exactly once, whether or not the pass completed. */
  end(): void;
};

/**
 * Estimates full pipeline-reset cost from hydration observations shared by all
 * client groups in a syncer process.
 *
 * The estimate is
 * `processTime * wallMultiplier * (1 + min(activeHydrations, 4))`:
 *
 * - `wallMultiplier` is the wall time a pass takes per unit of hydration
 *   process time when it has the process to itself (query transform round
 *   trips, CVR flushes, pokes, etc.). Passes are weighted by their process
 *   time, so the fixed overheads that dominate small passes do not set the
 *   price for large ones.
 * - `1 + activeHydrations` counts the full hydration passes (one per client
 *   group) that are in flight right now. Each observation is divided by the
 *   concurrency it ran under so that this contention is not also learned by
 *   `wallMultiplier`.
 *
 * The active-hydrations term is deliberate backpressure rather than a cost
 * estimate: sharing the thread slows advancement down just as much as it slows
 * hydration, but only the reset side of the comparison is scaled (advancement
 * is measured in process time). This biases client groups towards finishing
 * their advancement while others are hydrating, which keeps a wave of resets
 * from feeding itself. It is capped so that the bias stays bounded.
 */
export class HydrationCostModel {
  readonly #now: () => number;
  // Exponentially decayed sums; their ratio is the wall multiplier.
  #wallMs = 0;
  #processMs = 0;
  #activeHydrations = 0;
  // Integral of #activeHydrations over time, up to #concurrencyUpdatedAt.
  #concurrencyMs = 0;
  #concurrencyUpdatedAt: number;

  constructor(now: () => number = () => performance.now()) {
    this.#now = now;
    this.#concurrencyUpdatedAt = now();
  }

  beginHydration(): HydrationPass {
    const startedAt = this.#setActiveHydrations(this.#activeHydrations + 1);
    const concurrencyMsAtStart = this.#concurrencyMs;
    let ended = false;
    return {
      observe: processTimeMs => {
        const now = this.#setActiveHydrations(this.#activeHydrations);
        this.#observe(
          now - startedAt,
          this.#concurrencyMs - concurrencyMsAtStart,
          processTimeMs,
        );
      },
      end: () => {
        if (ended) {
          throw new Error('Hydration pass already ended');
        }
        ended = true;
        this.#setActiveHydrations(this.#activeHydrations - 1);
      },
    };
  }

  /** Brings the concurrency integral up to date. Returns the current time. */
  #setActiveHydrations(activeHydrations: number): number {
    const now = this.#now();
    this.#concurrencyMs +=
      this.#activeHydrations * (now - this.#concurrencyUpdatedAt);
    this.#concurrencyUpdatedAt = now;
    this.#activeHydrations = activeHydrations;
    return now;
  }

  #observe(
    wallTimeMs: number,
    concurrencyMs: number,
    processTimeMs: number,
  ): void {
    if (processTimeMs <= 0 || !(wallTimeMs > 0) || !(concurrencyMs > 0)) {
      return;
    }

    // The pass itself is active throughout, so the average is at least 1.
    const averageConcurrency = Math.max(1, concurrencyMs / wallTimeMs);
    const uncontendedWallTimeMs = Math.max(
      processTimeMs,
      Math.min(
        processTimeMs * MAX_HYDRATION_WALL_MULTIPLIER,
        wallTimeMs / averageConcurrency,
      ),
    );
    this.#wallMs =
      this.#wallMs * HYDRATION_OBSERVATION_DECAY + uncontendedWallTimeMs;
    this.#processMs =
      this.#processMs * HYDRATION_OBSERVATION_DECAY + processTimeMs;
  }

  estimate(processTimeMs: number): number {
    const wallMultiplier =
      this.#processMs > 0 ? this.#wallMs / this.#processMs : 1;
    return (
      processTimeMs *
      wallMultiplier *
      (1 + Math.min(this.#activeHydrations, MAX_PRICED_ACTIVE_HYDRATIONS))
    );
  }
}
