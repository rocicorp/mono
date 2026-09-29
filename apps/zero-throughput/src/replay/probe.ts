import {performance} from 'node:perf_hooks';
import type {ClientSchema} from '../../../../packages/zero-protocol/src/client-schema.ts';
import type {Recorder} from './recorder.ts';
import {SyncClient} from './sync-client.ts';
import type {WorkloadGroup, WorkloadQuery} from './workload.ts';

export type HydrationProbeOptions = {
  readonly group: WorkloadGroup;
  readonly clientSchema: ClientSchema;
  readonly cacheURL: string;
  readonly protocolVersion: number;
  readonly auth: string;
  readonly prepareQuery: (query: WorkloadQuery) => WorkloadQuery;
  readonly intervalMs: number;
  readonly timeoutMs: number;
  readonly clientGroupPrefix: string;
  readonly pingIntervalMs: number;
  readonly maxHeaderLength: number;
  readonly recorder: Recorder;
};

/**
 * A fixed yardstick for hydration: every `intervalMs`, a brand-new client
 * group for the same user connects, and the probe records how long until
 * every one of its session queries is hydrated. Unlike organic sessions,
 * whose users and library sizes vary, successive probes do identical work,
 * so their times track how busy the server is. At most one probe runs at a
 * time; a tick that finds one still running is skipped.
 */
export class HydrationProbe {
  readonly #o: HydrationProbeOptions;
  #timer: NodeJS.Timeout | undefined;
  #running: Promise<void> | undefined;
  #count = 0;

  constructor(options: HydrationProbeOptions) {
    this.#o = options;
  }

  start(): void {
    this.#tick();
    this.#timer = setInterval(() => this.#tick(), this.#o.intervalMs);
  }

  async stop(): Promise<void> {
    clearInterval(this.#timer);
    await this.#running;
  }

  #tick(): void {
    if (this.#running === undefined) {
      this.#running = this.#probe().finally(() => {
        this.#running = undefined;
      });
    }
  }

  #probe(): Promise<void> {
    const o = this.#o;
    const clientGroupID = `${o.clientGroupPrefix}-probe-${this.#count++}`;
    const queries = o.group.sessionQueries.map(q => ({
      ...o.prepareQuery(q),
      kind: 'session' as const,
    }));
    return new Promise<void>(resolve => {
      let done = false;
      const startedAt = performance.now();
      const finish = (failure: string | undefined) => {
        if (done) {
          return;
        }
        done = true;
        clearTimeout(timeout);
        if (failure === undefined) {
          o.recorder.probeHydrated(performance.now() - startedAt);
        } else {
          o.recorder.probeFailed(failure);
        }
        client.close();
        resolve();
      };
      const client: SyncClient = new SyncClient({
        cacheURL: o.cacheURL,
        protocolVersion: o.protocolVersion,
        auth: o.auth,
        clientSchema: o.clientSchema,
        device: {
          clientGroupID,
          clientID: `${clientGroupID}-c`,
          userID: o.group.userID,
          cookie: null,
          gotHashes: new Set(),
        },
        pingIntervalMs: o.pingIntervalMs,
        maxHeaderLength: o.maxHeaderLength,
        listener: {
          onConnected: () => undefined,
          onFirstPoke: () => undefined,
          onHydrated: () => {
            if (client.pendingQueries === 0) {
              finish(undefined);
            }
          },
          onQueryError: ({name, message}) => {
            o.recorder.queryError(name, message);
            if (client.pendingQueries === 0) {
              finish(undefined);
            }
          },
          onPoke: () => undefined,
          onRow: () => undefined,
          onPong: () => undefined,
          onServerError: (kind, message) =>
            o.recorder.serverError(kind, message),
          onClose: ({code, reason}) =>
            finish(`closed before hydrating (${code} ${reason})`),
        },
      });
      const timeout = setTimeout(
        finish,
        o.timeoutMs,
        `not hydrated after ${o.timeoutMs} ms`,
      );
      client.connect(queries);
    });
  }
}
