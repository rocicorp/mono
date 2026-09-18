const HYDRATION_WALL_MULTIPLIER_ALPHA = 0.25;
const MAX_HYDRATION_WALL_MULTIPLIER = 20;

/**
 * Estimates full pipeline-reset cost from hydration observations shared by all
 * client groups in a syncer process.
 */
export class HydrationCostModel {
  #wallMultiplier = 1;
  #activeHydrations = 0;

  beginHydration(): void {
    this.#activeHydrations++;
  }

  endHydration(): void {
    if (this.#activeHydrations <= 0) {
      throw new Error('Cannot end hydration when none is active');
    }
    this.#activeHydrations--;
  }

  observe(wallTimeMs: number, processTimeMs: number): void {
    if (processTimeMs <= 0 || !Number.isFinite(wallTimeMs)) {
      return;
    }

    const observedMultiplier = Math.max(
      1,
      Math.min(MAX_HYDRATION_WALL_MULTIPLIER, wallTimeMs / processTimeMs),
    );
    this.#wallMultiplier =
      this.#wallMultiplier * (1 - HYDRATION_WALL_MULTIPLIER_ALPHA) +
      observedMultiplier * HYDRATION_WALL_MULTIPLIER_ALPHA;
  }

  estimate(processTimeMs: number): number {
    return processTimeMs * this.#wallMultiplier * (1 + this.#activeHydrations);
  }
}
