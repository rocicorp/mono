/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {LogContext} from '@rocicorp/logger';
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
        this.#instances.delete(id);
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
