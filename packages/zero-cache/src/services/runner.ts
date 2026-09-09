import type {LogContext} from '@rocicorp/logger';
import type {Service} from './service.ts';

/**
 * Manages the creation and lifecycle of objects that implement
 * {@link Service}.
 */
export class ServiceRunner<S extends Service> {
  readonly #lc: LogContext;
  readonly #instances = new Map<string, S>();
  readonly #create: (id: string) => S;
  readonly #isValid: (existing: S) => boolean;

  constructor(
    lc: LogContext,
    factory: (id: string) => S,
    isValid: (existing: S) => boolean = () => true,
  ) {
    this.#lc = lc;
    this.#create = factory;
    this.#isValid = isValid;
  }

  /**
   * Creates and runs the Service with the given `id`, returning
   * an existing one if it is still running a valid.
   */
  getService(id: string): S {
    const existing = this.#instances.get(id);
    if (existing && this.#isValid(existing)) {
      return existing;
    }
    const service = this.#create(id);
    this.#instances.set(id, service);
    void service
      .run()
      .catch(e =>
        this.#lc.error?.(
          `Error running ${service.constructor?.name} ${service.id}`,
          e,
        ),
      )
      .finally(() => {
        // Only remove the instance that just finished. If it was already
        // replaced (e.g. because it became invalid and a new instance was
        // created under the same id while it was still shutting down), the
        // replacement must stay tracked; otherwise the next getService()
        // would spawn a duplicate and the untracked one would never be
        // stopped by this runner.
        if (this.#instances.get(id) === service) {
          this.#instances.delete(id);
        }
      });
    return service;
  }

  get size() {
    return this.#instances.size;
  }

  getServices(): Iterable<S> {
    return this.#instances.values();
  }
}
