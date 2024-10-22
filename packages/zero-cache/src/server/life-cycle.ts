import {LogContext} from '@rocicorp/logger';
import type {EventEmitter} from 'stream';
import type {SingletonService} from '../services/service.js';
import type {Worker} from '../types/processes.js';

/**
 * * `user-facing` workers serve external requests and are the first to
 *   receive a `SIGTERM` or `SIGINT` signal for graceful shutdown.
 *
 * * `supporting` workers support `user-facing` workers and are sent
 *   the `SIGTERM` signal only after all `user-facing` workers have
 *   exited.
 *
 * For other kill signals, such as `SIGQUIT`, all workers
 * are stopped without draining. Additionally, if any worker exits
 * unexpectedly, all workers sent an immediate `SIGQUIT` signal.
 */
export type WorkerType = 'user-facing' | 'supporting';

export const GRACEFUL_SHUTDOWN = ['SIGTERM', 'SIGINT'] as const;
export const FORCEFUL_SHUTDOWN = ['SIGQUIT'] as const;

/**
 * Handles termination signals and coordination of graceful shutdown.
 */
export class Terminator {
  readonly #lc: LogContext;
  readonly #userFacing = new Set<Worker>();
  readonly #all = new Set<Worker>();
  readonly #exit: (code: number) => never;

  #drainStart = 0;

  constructor(
    lc: LogContext,
    // testing hooks
    proc: EventEmitter = process,
    exit = (code: number) => process.exit(code),
  ) {
    this.#lc = lc.withContext('component', 'terminator');

    // Propagate `SIGTERM` and `SIGINT` to all user-facing workers,
    // initiating a graceful shutdown. The terminator process will
    // exit once all user-facing workers have exited ...
    for (const signal of GRACEFUL_SHUTDOWN) {
      proc.on(signal, () => {
        this.#drainStart = Date.now();
        if (this.#userFacing.size) {
          kill(this.#userFacing, signal);
        } else {
          exit(0);
        }
      });
    }

    // ... which will result in sending `SIGTERM` to the remaining workers.
    proc.on('exit', code =>
      kill(this.#all, code === 0 ? GRACEFUL_SHUTDOWN[0] : FORCEFUL_SHUTDOWN[0]),
    );

    // For other (catchable) kill signals, exit with a non-zero error code
    // to send a `SIGQUIT` to all workers. For this signal, workers are
    // stopped immediately without draining. See `runUntilKilled()`.
    for (const signal of FORCEFUL_SHUTDOWN) {
      proc.on(signal, () => exit(-1));
    }

    this.#exit = exit;
  }

  addWorker(worker: Worker, type: WorkerType): Worker {
    if (type === 'user-facing') {
      this.#userFacing.add(worker);
    }
    this.#all.add(worker);

    worker.on('error', err => this.logErrorAndExit(err));
    worker.on('close', (code, signal) =>
      this.#onExit(code, signal, worker, type),
    );
    return worker;
  }

  logErrorAndExit(err: unknown): never {
    this.#lc.error?.(`shutting down for error`, err);
    this.#exit(-1);
  }

  #onExit(code: number, sig: NodeJS.Signals, worker: Worker, type: WorkerType) {
    if (sig) {
      this.#lc.error?.(`shutting down because ${type} worker killed (${sig})`);
      return this.#exit(-1);
    }
    if (code !== 0) {
      this.#lc.error?.(`shutting down because ${type} worker exited (${code})`);
      return this.#exit(code);
    }
    if (type === 'supporting') {
      // The replication-manager has no user-facing workers.
      // In this case, code === 0 shutdowns are not errors.
      const log = code === 0 && this.#userFacing.size === 0 ? 'info' : 'error';
      this.#lc[log]?.(
        `shutting down because supporting worker exited with code ${code}`,
      );
      return this.#exit(log === 'error' ? -1 : code);
    }
    if (this.#drainStart === 0) {
      this.#lc.error?.(
        `shutting down because user-facing worker exited before SIGTERM`,
      );
      return this.#exit(-1);
    }

    // user-facing worker finished draining.
    this.#userFacing.delete(worker);
    this.#all.delete(worker);

    if (this.#userFacing.size === 0) {
      this.#lc.info?.(
        `all user-facing workers drained (${Date.now() - this.#drainStart} ms)`,
      );
      return this.#exit(0);
    }
  }
}

function kill(workers: Iterable<Worker>, signal: NodeJS.Signals) {
  for (const worker of workers) {
    worker.kill(signal);
  }
}

/**
 * Runs the specified services, stopping them on `SIGTERM` or `SIGINT` with
 * an optional {@link SingletonService.drain drain()}, or stopping them
 * without draining for `SIGQUIT`.
 *
 * @returns a Promise that resolves/rejects when any of the services stops/throws.
 */

export async function runUntilKilled(
  lc: LogContext,
  parent: Worker,
  ...services: SingletonService[]
): Promise<void> {
  for (const signal of [...GRACEFUL_SHUTDOWN, ...FORCEFUL_SHUTDOWN]) {
    parent.once(signal, () => {
      const GRACEFUL_SIGNALS = GRACEFUL_SHUTDOWN as readonly NodeJS.Signals[];

      services.forEach(async svc => {
        if (GRACEFUL_SIGNALS.includes(signal) && svc.drain) {
          lc.info?.(`draining ${svc.constructor.name} ${svc.id} (${signal})`);
          await svc.drain();
        }
        lc.info?.(`stopping ${svc.constructor.name} ${svc.id} (${signal})`);
        await svc.stop();
      });
    });
  }

  try {
    // Run all services and resolve when any of them stops.
    const svc = await Promise.race(
      services.map(svc => svc.run().then(() => svc)),
    );
    lc.info?.(`${svc.constructor.name} (${svc.id}) stopped`);
  } catch (e) {
    lc.error?.(`exiting on error`, e);
    throw e;
  }
}

export async function exitAfter(run: () => Promise<void>) {
  try {
    await run();
    process.exit(0);
  } catch (e) {
    console.error(e);
    process.exit(-1);
  }
}
