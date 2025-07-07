/**
 * Utility for sampling metrics to reduce cardinality and volume.
 *
 * High-frequency operations can overwhelm metric backends with too many data points.
 * Sampling allows us to capture representative data while reducing costs.
 */

export class MetricSampler {
  private counters = new Map<string, number>();

  constructor(private sampleRate: number = 100) {
    if (sampleRate < 1) {
      throw new Error('Sample rate must be at least 1');
    }
  }

  /**
   * Determines if a metric should be recorded based on sampling rate.
   *
   * @param key - Unique identifier for the metric type
   * @returns true if this sample should be recorded
   */
  shouldSample(key: string): boolean {
    const count = this.counters.get(key) || 0;
    const newCount = count + 1;
    this.counters.set(key, newCount);

    return newCount % this.sampleRate === 0;
  }

  /**
   * Resets the counter for a specific metric key.
   * Useful for periodic resets to prevent counter overflow.
   */
  resetCounter(key: string): void {
    this.counters.delete(key);
  }

  /**
   * Resets all counters.
   */
  resetAllCounters(): void {
    this.counters.clear();
  }
}

// Default samplers for different types of metrics
export const hydrationSampler = new MetricSampler(10); // Sample 1 in 10 hydrations
export const changeAdvanceSampler = new MetricSampler(5); // Sample 1 in 5 change advances
