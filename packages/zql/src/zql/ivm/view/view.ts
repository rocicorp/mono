export interface View<T> {
  on(fn: (value: T, version: number) => void): () => void;
  /**
   * If there are 0 listeners left after removing the given listener,
   * the signal is destroyed.
   *
   * To opt out of this behavior, pass `autoCleanup: false`
   */
  off(
    fn: (value: T, version: number) => void,
    options?: {autoCleanup?: boolean},
  ): void;
  get value(): T;
  get hydrated(): boolean;
  pullHistoricalData(): void;
}
