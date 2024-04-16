export interface Service {
  readonly id: string;

  /**
   * `run` is called once by the Service Runner to run the service.
   * The returned Promise resolves when the service stops, either because
   * {@link Service.stop()} was called, or because the service
   * has completed its work. If the Promise rejects with an error, the
   * Service Runner will restart it with exponential backoff.
   */
  run(): Promise<void>;

  /**
   * Called to signal the service to stop. This is generally only used
   * in tests.
   */
  stop(): Promise<void>;
}
