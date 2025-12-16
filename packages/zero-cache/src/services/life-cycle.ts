import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {IncomingHttpHeaders} from 'node:http';
import {pid} from 'node:process';
import type {EventEmitter} from 'stream';
import {
  singleProcessMode,
  type Subprocess,
  type Worker,
} from '../types/processes.ts';
import {RunningState} from './running-state.ts';
import type {SingletonService} from './service.ts';

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
 * Handles readiness, termination signals, and coordination of graceful
 * shutdown.
 */
export class ProcessManager {
  readonly #lc: LogContext;
  readonly #userFacing = new Set<Subprocess>();
  readonly #all = new Set<Subprocess>();
  readonly #exitImpl: (code: number) => never;
  readonly #start = Date.now();
  readonly #ready: Promise<void>[] = [];

  #runningState = new RunningState('process-manager');
  #drainStart = 0;

  constructor(lc: LogContext, proc: EventEmitter) {
    this.#lc = lc.withContext('component', 'process-manager');

    // Propagate `SIGTERM` and `SIGINT` to all user-facing workers,
    // initiating a graceful shutdown. The parent process will
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
      proc.on(signal, () => this.#exit(-1));
    }

    this.#exitImpl = (code: number) => {
      if (singleProcessMode()) {
        return proc.emit('exit', code) as never; // For unit / integration tests.
      }
      process.exit(code);
    };
  }

  done() {
    return this.#runningState.stopped();
  }

  #exit(code: number) {
    this.#lc.info?.('exiting with code', code);
    this.#runningState.stop(this.#lc);
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

  addSubprocess(proc: Subprocess, type: WorkerType, name: string) {
    if (type === 'user-facing') {
      this.#userFacing.add(proc);
    }
    this.#all.add(proc);

    proc.on('error', err =>
      this.#lc.error?.(`error from ${name} ${proc.pid}`, err),
    );
    proc.on('close', (code, signal) =>
      this.#onExit(code, signal, null, type, name, proc),
    );
  }

  readonly #initializing = new Map<number, string>();
  #nextID = 0;

  addWorker(worker: Worker, type: WorkerType, name: string): Worker {
    this.addSubprocess(worker, type, name);

    const id = ++this.#nextID;
    this.#initializing.set(id, name);
    const {promise, resolve} = resolver();
    this.#ready.push(promise);

    worker.onceMessageType('ready', () => {
      this.#lc.debug?.(`${name} ready (${Date.now() - this.#start} ms)`);
      this.#initializing.delete(id);
      resolve();
    });

    return worker;
  }

  initializing(): string[] {
    return [...this.#initializing.values()];
  }

  async allWorkersReady() {
    await Promise.all(this.#ready);
  }

  logErrorAndExit(err: unknown, name: string) {
    // only accessible by the main (i.e. user-facing) process.
    this.#onExit(-1, null, err, 'user-facing', name, undefined);
  }

  #onExit(
    code: number,
    sig: NodeJS.Signals | null,
    err: unknown | null,
    type: WorkerType,
    name: string,
    worker: Subprocess | undefined,
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
      // Non-zero exits are warnings (not errors) since they're often transient issues.
      const log = code === 0 && this.#userFacing.size === 0 ? 'info' : 'warn';
      this.#lc[log]?.(`${name} (${pid}) exited with code (${code})`, err ?? '');
      return this.#exit(log === 'info' ? code : -1);
    }

    const log = this.#drainStart === 0 ? 'error' : 'warn';
    if (sig) {
      this.#lc[log]?.(`${name} (${pid}) killed with (${sig})`, err ?? '');
    } else if (code !== 0) {
      this.#lc[log]?.(`${name} (${pid}) exited with code (${code})`, err ?? '');
    } else {
      this.#lc.info?.(`${name} (${pid}) exited with code (${code})`);
    }

    // user-facing workers exited or finished draining.
    if (this.#userFacing.size === 0) {
      this.#lc.info?.(
        this.#drainStart
          ? `all user-facing workers drained (${
              Date.now() - this.#drainStart
            } ms)`
          : `all user-facing workers exited`,
      );
      return this.#exit(0);
    }

    // Exit only if not draining. If a user-facing worker exits unexpectedly
    // during a drain, log a warning but let other user-facing workers drain.
    if (log === 'error') {
      return this.#exit(code || -1);
    }

    return undefined;
  }

  #kill(workers: Iterable<Subprocess>, signal: NodeJS.Signals) {
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
  parent: EventEmitter,
  ...services: SingletonService[]
): Promise<void> {
  if (services.length === 0) {
    return;
  }
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
    // oxlint-disable-next-line no-console
    console.info(`pid ${pid} exiting normally`);
    process.exit(0);
  } catch (e) {
    // oxlint-disable-next-line no-console
    console.error(`pid ${pid} exiting with error`, e);
    process.exit(-1);
  }
}

const DEFAULT_STOP_INTERVAL_MS = 20_000;

/**
 * The HeartbeatMonitor monitors the cadence heartbeats (e.g. "/keepalive"
 * health checks made to HttpServices) that signal that the server
 * should continue processing requests. When a configurable `stopInterval`
 * elapses without receiving these heartbeats, the monitor initiates a
 * graceful shutdown of the server. This works with common load balancing
 * frameworks such as AWS Elastic Load Balancing.
 *
 * The HeartbeatMonitor is **opt-in** in that it only kicks in after it
 * starts receiving keepalives.
 */
export class HeartbeatMonitor {
  readonly #stopInterval: number;

  #lc: LogContext;
  #checkIntervalTimer: NodeJS.Timeout | undefined;
  #checkImmediateTimer: NodeJS.Immediate | undefined;
  #lastHeartbeat = 0;

  constructor(lc: LogContext, stopInterval = DEFAULT_STOP_INTERVAL_MS) {
    this.#lc = lc;
    this.#stopInterval = stopInterval;
  }

  onHeartbeat(reqHeaders: IncomingHttpHeaders) {
    this.#lastHeartbeat = Date.now();
    if (this.#checkIntervalTimer === undefined) {
      this.#lc.info?.(
        `starting heartbeat monitor at ${
          this.#stopInterval / 1000
        } second interval`,
        reqHeaders,
      );
      // e.g. check every 5 seconds to see if it's been over 20 seconds
      //      since the last heartbeat.
      this.#checkIntervalTimer = setInterval(
        this.#checkStopInterval,
        this.#stopInterval / 4,
      );
    }
  }

  #checkStopInterval = () => {
    // In the Node.js event loop, timers like setInterval and setTimeout
    // run *before* I/O events coming from network sockets or file reads/writes.
    // When this process gets starved of CPU resources for long periods of time,
    // for example when other processes are monopolizing all available cores,
    // pathological behavior can emerge:
    // - keepalive network request comes in, but is queued in Node internals waiting
    //   for time on the event loop
    // - CPU is starved/monopolized by other processes for longer than the time
    //   configured via this.#stopInterval
    // - When CPU becomes available and the event loop wakes up, this stop interval
    //   check is run *before* the keepalive request is processed. The value of
    //   this.#lastHeartbeat is now very stale, and erroneously triggers a shutdown
    //   even though keepalive requests were about to be processed and update
    //   this.#lastHeartbeat. Downtime ensues.
    //
    // To avoid this, we push the check out to a phase of the event loop *after*
    // I/O events are processed, using setImmediate():
    // https://nodejs.org/en/learn/asynchronous-work/event-loop-timers-and-nexttick#setimmediate-vs-settimeout
    //
    // This ensures we see a value for this.#lastHeartbeat that reflects
    // any keepalive requests that came in during the current event loop turn.
    this.#checkImmediateTimer = setImmediate(() => {
      this.#checkImmediateTimer = undefined;
      const timeSinceLastHeartbeat = Date.now() - this.#lastHeartbeat;
      if (timeSinceLastHeartbeat >= this.#stopInterval) {
        this.#lc.info?.(
          `last heartbeat received ${
            timeSinceLastHeartbeat / 1000
          } seconds ago. draining.`,
        );
        process.kill(process.pid, GRACEFUL_SHUTDOWN[0]);
      }
    });
  };

  stop() {
    clearTimeout(this.#checkIntervalTimer);
    if (this.#checkImmediateTimer) {
      clearImmediate(this.#checkImmediateTimer);
    }
  }
}
