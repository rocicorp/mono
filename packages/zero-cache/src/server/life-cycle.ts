import {LogContext} from '@rocicorp/logger';
import {pid} from 'process';
import type {EventEmitter} from 'stream';
import {HttpService, type Options} from '../services/http-service.js';
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
  readonly #exitImpl: (code: number) => never;

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
      proc.on(signal, () => this.#startDrain(signal));
    }

    // ... which will result in sending `SIGTERM` to the remaining workers.
    proc.on('exit', code =>
      this.#kill(
        this.#all,
        code === 0 ? GRACEFUL_SHUTDOWN[0] : FORCEFUL_SHUTDOWN[0],
      ),
    );

    // For other (catchable) kill signals, exit with a non-zero error code
    // to send a `SIGQUIT` to all workers. For this signal, workers are
    // stopped immediately without draining. See `runUntilKilled()`.
    for (const signal of FORCEFUL_SHUTDOWN) {
      proc.on(signal, () => exit(-1));
    }

    this.#exitImpl = exit;
  }

  #exit(code: number) {
    this.#lc.info?.('exiting with code', code);
    void this.#lc.flush().finally(() => this.#exitImpl(code));
  }

  #startDrain(signal: 'SIGTERM' | 'SIGINT' = 'SIGTERM') {
    this.#lc.info?.(`initiating drain (${signal})`);
    this.#drainStart = Date.now();
    if (this.#userFacing.size) {
      this.#kill(this.#userFacing, signal);
    } else {
      this.#kill(this.#all, signal);
    }
  }

  addWorker(worker: Worker, type: WorkerType): Worker {
    if (type === 'user-facing') {
      this.#userFacing.add(worker);
    }
    this.#all.add(worker);

    worker.on(
      'error',
      err => this.#lc.error?.(`error from worker ${worker.pid}`, err),
    );
    worker.on('close', (code, signal) =>
      this.#onExit(code, signal, null, type, worker),
    );
    return worker;
  }

  logErrorAndExit(err: unknown) {
    // only accessible by the main (i.e. user-facing) process.
    this.#onExit(-1, null, err, 'user-facing', undefined);
  }

  #onExit(
    code: number,
    sig: NodeJS.Signals | null,
    err: unknown | null,
    type: WorkerType,
    worker: Worker | undefined,
  ) {
    // Remove the worker from maps to avoid attempting to send more signals to it.
    if (worker) {
      this.#userFacing.delete(worker);
      this.#all.delete(worker);
    }

    const pid = worker?.pid ?? process.pid;

    if (type === 'supporting') {
      // The replication-manager has no user-facing workers.
      // In this case, code === 0 shutdowns are not errors.
      const log = code === 0 && this.#userFacing.size === 0 ? 'info' : 'error';
      this.#lc[log]?.(
        `${type} worker ${pid} exited with code (${code})`,
        err ?? '',
      );
      return this.#exit(log === 'error' ? -1 : code);
    }

    const log = this.#drainStart === 0 ? 'error' : 'warn';
    if (sig) {
      this.#lc[log]?.(`${type} worker ${pid} killed with (${sig})`, err ?? '');
    } else if (code !== 0) {
      this.#lc[log]?.(
        `${type} worker ${pid} exited with code (${code})`,
        err ?? '',
      );
    } else {
      this.#lc.info?.(`${type} worker ${pid} exited with code (${code})`);
    }

    // Exit only if not draining. If a user-facing worker exits unexpectedly
    // during a drain, log a warning but let other user-facing workers drain.
    if (log === 'error') {
      this.#exit(code || -1);
    }

    // user-facing worker finished draining.
    if (this.#userFacing.size === 0) {
      this.#lc.info?.(
        `all user-facing workers drained (${Date.now() - this.#drainStart} ms)`,
      );
      return this.#exit(0);
    }
    return undefined;
  }

  #kill(workers: Iterable<Worker>, signal: NodeJS.Signals) {
    for (const worker of workers) {
      try {
        worker.kill(signal);
      } catch (e) {
        this.#lc.error?.(e);
      }
    }
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
  parent: Worker | NodeJS.Process,
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
    console.info(`pid ${pid} exiting normally`);
    process.exit(0);
  } catch (e) {
    console.error(`pid ${pid} exiting with error`, e);
    process.exit(-1);
  }
}

const DEFAULT_STOP_INTERVAL_MS = 15_000;

/**
 * The HeartbeatMonitor listens on a dedicated port to monitor the cadence
 * of "heartbeat" requests (e.g. health checks) that signal that the server
 * should continue processing requests. When a configurable `stopInterval`
 * elapses without receiving these heartbeats, the monitor initiates a
 * graceful shutdown of the server. This works with common load balancing
 * frameworks such as AWS Elastic Load Balancing.
 *
 * The HeartbeatMonitor is **opt-in** in that it only kicks in after it
 * starts receiving health checks on that port.
 */
export class HeartbeatMonitor extends HttpService {
  readonly #stopInterval: number;

  #timer: NodeJS.Timeout | undefined;
  #lastHeartbeat = 0;

  constructor(
    lc: LogContext,
    opts: Options,
    stopInterval = DEFAULT_STOP_INTERVAL_MS,
  ) {
    super('heartbeat-monitor', lc, opts, fastify => {
      fastify.get('/', (_req, res) => {
        this.#onHeartbeat();
        return res.send('OK');
      });
    });
    this.#stopInterval = stopInterval;
  }

  #onHeartbeat() {
    this.#lastHeartbeat = Date.now();
    if (this.#timer === undefined) {
      this._lc.info?.(
        `starting heartbeat monitor at ${
          this.#stopInterval / 1000
        } second interval`,
      );
      this.#timer = setInterval(this.#onStopInterval, this.#stopInterval);
    }
  }

  #onStopInterval = () => {
    const timeSinceLastHeartbeat = Date.now() - this.#lastHeartbeat;
    if (timeSinceLastHeartbeat >= this.#stopInterval) {
      this._lc.info?.(
        `last heartbeat received ${
          timeSinceLastHeartbeat / 1000
        } seconds ago. draining.`,
      );
      process.kill(process.pid, GRACEFUL_SHUTDOWN[0]);
    }
  };

  async stop() {
    clearTimeout(this.#timer);
    await super.stop();
  }
}
