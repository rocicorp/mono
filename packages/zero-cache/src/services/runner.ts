import {LogContext} from '@rocicorp/logger';
import type {Service} from './service.js';

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
      .catch(e => {
        this.#lc.error?.(
          `Error running ${service.constructor?.name} ${service.id}`,
          e,
        );
        this.#lc.info?.(e.toString());
      })
      .finally(() => {
        this.#instances.delete(id);
      });
    return service;
  }
}

/**
 * Runs the specified services, exiting on SIGTERM, or logging an error and
 * exiting the process if any of them fail.
 */
export async function runOrExit(
  lc: LogContext,
  ...services: Service[]
): Promise<void> {
  for (const signal of ['SIGINT', 'SIGQUIT', 'SIGTERM'] as const) {
    process.once(signal, () => {
      for (const svc of services) {
        lc.info?.(`exiting ${svc.constructor.name} (${svc.id}) for ${signal}`);
        void svc.stop();
      }
    });
  }

  try {
    // Exit if any of the services stop.
    const svc = await Promise.race(
      services.map(svc => svc.run().then(() => svc)),
    );
    lc.info?.(`exiting because ${svc.constructor.name} (${svc.id}) stopped`);
    process.exit(0);
  } catch (e) {
    lc.error?.(`exiting on error`, e);
    process.exit(-1);
  }
}
