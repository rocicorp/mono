/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
export interface Service {
  readonly id: string;

  /**
   * `run` is called once by the Service Runner to run the service.
   * The returned Promise resolves when the service stops, either because
   * {@link stop()} was called, or because the service
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

export interface ActivityBasedService extends Service {
  /**
   * Requests that service continue running if not already shutting down.
   * This is applicable to services whose life cycle is tied to external
   * activity and shutdown after a period of inactivity.
   *
   * @return `true` if the service will continue running for its
   *         configured keepalive interval, or `false` if it has
   *         already shut down or begun the shutdown process.
   */
  keepalive(): boolean;
}

export interface SingletonService extends Service {
  /**
   * `drain` is called when the process receives the `SIGTERM` signal
   * to initiate graceful shutdown. The process will wait until the
   * return Promise resolves, after which {@link stop()} will be called.
   */
  drain?(): Promise<void>;
}

export interface RefCountedService extends Service {
  ref(): void;
  unref(): void;
  hasRefs(): boolean;
}
