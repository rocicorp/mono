import type {ClientSchema} from '../../../../packages/zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../packages/zero-protocol/src/data.ts';
import type {Recorder} from './recorder.ts';
import {SyncClient, type DeviceState, type ReplayQuery} from './sync-client.ts';
import type {ScreenQuery, WorkloadGroup, WorkloadQuery} from './workload.ts';

export type SessionDriverOptions = {
  readonly groups: readonly WorkloadGroup[];
  readonly screenQueries: readonly ScreenQuery[];
  readonly clientSchema: ClientSchema;
  readonly cacheURLs: readonly string[];
  readonly protocolVersion: number;
  readonly authForUser: (userID: string) => string;
  /**
   * Devices that sessions are drawn from. Device `i` replays group
   * `i % groups.length`; a device that has synced before starts a returning
   * session.
   */
  readonly devices: number;
  readonly clientGroupPrefix: string;
  readonly meanSessionMs: number;
  readonly screenQueriesPerMinute: number;
  readonly meanScreenDwellMs: number;
  readonly maxScreenQueriesPerSession: number;
  readonly reconnectDelayMs: number;
  readonly pingIntervalMs: number;
  readonly maxHeaderLength: number;
  readonly random: () => number;
  /**
   * Applied to each query as a session registers it, e.g. to move
   * clock-derived arguments to the current time.
   */
  readonly prepareQuery: (query: WorkloadQuery) => WorkloadQuery;
  readonly recorder: Recorder;
  readonly onRow: (tableName: string, row: Row, caughtUpAtMs: number) => void;
  readonly log: (message: string) => void;
};

type Device = {
  readonly index: number;
  readonly group: WorkloadGroup;
  readonly state: DeviceState;
  busy: boolean;
};

type Session = {
  readonly device: Device;
  readonly client: SyncClient;
  readonly screens: Map<string, ReplayQuery>;
  readonly timers: Set<NodeJS.Timeout>;
};

/**
 * Keeps a target number of replayed sessions open. Each session lasts an
 * exponentially distributed time, then closes and is replaced by a session
 * on another idle device, so connects arrive at `concurrency / meanSession`.
 * While open, a session visits screen queries as a Poisson process.
 */
export class SessionDriver {
  readonly #options: SessionDriverOptions;
  readonly #devices: Device[];
  readonly #sessions = new Set<Session>();
  readonly #screenCumulativeWeights: number[];
  #target = 0;
  #running = false;
  readonly #startTimers = new Set<NodeJS.Timeout>();

  constructor(options: SessionDriverOptions) {
    if (options.groups.length === 0) {
      throw new Error('SessionDriver needs at least one workload group');
    }
    this.#options = options;
    this.#devices = Array.from({length: options.devices}, (_, index) => {
      const group = options.groups[index % options.groups.length];
      const clientGroupID = `${options.clientGroupPrefix}-${index}`;
      return {
        index,
        group,
        busy: false,
        state: {
          clientGroupID,
          clientID: `${clientGroupID}-c`,
          userID: group.userID,
          cookie: null,
          gotHashes: new Set<string>(),
        },
      };
    });
    let total = 0;
    this.#screenCumulativeWeights = options.screenQueries.map(q => {
      total += q.weight;
      return total;
    });
  }

  get activeSessions(): number {
    return this.#sessions.size;
  }

  get pendingQueries(): number {
    let n = 0;
    for (const s of this.#sessions) {
      n += s.client.pendingQueries;
    }
    return n;
  }

  /** User IDs of the open sessions, for writers that target active users. */
  activeUserIDs(): string[] {
    return Array.from(this.#sessions, s => s.device.group.userID);
  }

  /**
   * Opens sessions up to `target`, spreading new ones over `rampMs`, or
   * closes the excess immediately.
   */
  setConcurrency(target: number, rampMs: number): void {
    this.#running = true;
    this.#target = target;
    const missing = target - this.#sessions.size - this.#startTimers.size;
    for (let i = 0; i < missing; i++) {
      const timer = setTimeout(
        () => {
          this.#startTimers.delete(timer);
          this.#fill();
        },
        missing <= 1 ? 0 : (rampMs * i) / missing,
      );
      this.#startTimers.add(timer);
    }
    let excess = this.#sessions.size - target;
    for (const session of this.#sessions) {
      if (excess-- <= 0) {
        break;
      }
      this.#end(session);
    }
  }

  async stop(): Promise<void> {
    this.#running = false;
    for (const timer of this.#startTimers) {
      clearTimeout(timer);
    }
    this.#startTimers.clear();
    for (const session of this.#sessions) {
      this.#end(session);
    }
    const deadline = Date.now() + 5_000;
    while (this.#sessions.size > 0 && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 50));
    }
  }

  #fill(): void {
    if (
      !this.#running ||
      this.#sessions.size + this.#startTimers.size >= this.#target
    ) {
      return;
    }
    const device = this.#pickIdleDevice();
    if (device === undefined) {
      this.#options.log('no idle device; raise --devices');
      return;
    }
    this.#start(device);
  }

  #pickIdleDevice(): Device | undefined {
    const idle = this.#devices.filter(d => !d.busy);
    if (idle.length === 0) {
      return undefined;
    }
    return idle[Math.floor(this.#options.random() * idle.length)];
  }

  #start(device: Device): void {
    const o = this.#options;
    const recorder = o.recorder;
    device.busy = true;
    const returning = device.state.cookie !== null;
    const timers = new Set<NodeJS.Timeout>();
    const screens = new Map<string, ReplayQuery>();
    const client: SyncClient = new SyncClient({
      cacheURL: o.cacheURLs[device.index % o.cacheURLs.length],
      protocolVersion: o.protocolVersion,
      auth: o.authForUser(device.group.userID),
      clientSchema: o.clientSchema,
      device: device.state,
      pingIntervalMs: o.pingIntervalMs,
      maxHeaderLength: o.maxHeaderLength,
      listener: {
        onConnected: ({connectMs}) => recorder.connected(connectMs),
        onFirstPoke: ({ms}) => recorder.firstPoke(ms),
        onHydrated: ({name, kind, ms}) =>
          recorder.hydrated(name, kind, ms, returning),
        onQueryError: ({name, message}) => recorder.queryError(name, message),
        onPoke: ({rows, bytes}) => recorder.poke(rows, bytes),
        onRow: o.onRow,
        onPong: rtt => recorder.pong(rtt),
        onServerError: (kind, message) => recorder.serverError(kind, message),
        onClose: ({code, reason, expected}) => {
          for (const timer of timers) {
            clearTimeout(timer);
          }
          this.#sessions.delete(session);
          device.busy = false;
          recorder.sessionEnded(expected, `${code} ${reason}`);
          if (!this.#running) {
            return;
          }
          const delay = expected
            ? o.random() * 1_000
            : o.reconnectDelayMs * (0.5 + o.random());
          const timer = setTimeout(() => {
            this.#startTimers.delete(timer);
            this.#fill();
          }, delay);
          this.#startTimers.add(timer);
        },
      },
    });
    const session: Session = {device, client, screens, timers};
    this.#sessions.add(session);
    recorder.sessionStarted();
    client.connect(
      device.group.sessionQueries.map(q => ({
        ...o.prepareQuery(q),
        kind: 'session',
      })),
    );

    this.#after(session, exponential(o.random, o.meanSessionMs), () =>
      this.#end(session),
    );
    this.#scheduleScreenVisit(session);
  }

  #scheduleScreenVisit(session: Session): void {
    const o = this.#options;
    if (o.screenQueriesPerMinute <= 0 || o.screenQueries.length === 0) {
      return;
    }
    this.#after(
      session,
      exponential(o.random, 60_000 / o.screenQueriesPerMinute),
      () => {
        this.#visitScreen(session);
        this.#scheduleScreenVisit(session);
      },
    );
  }

  #visitScreen(session: Session): void {
    const o = this.#options;
    if (session.screens.size >= o.maxScreenQueriesPerSession) {
      return;
    }
    const picked = o.prepareQuery(this.#pickScreenQuery());
    const key = JSON.stringify([picked.name, picked.args]);
    if (session.screens.has(key)) {
      return;
    }
    const query: ReplayQuery = {
      name: picked.name,
      args: picked.args,
      ttlMs: picked.ttlMs,
      kind: 'screen',
    };
    session.screens.set(key, query);
    session.client.addQuery(query);
    this.#after(session, exponential(o.random, o.meanScreenDwellMs), () => {
      session.screens.delete(key);
      session.client.removeQuery(query);
    });
  }

  #pickScreenQuery(): ScreenQuery {
    const weights = this.#screenCumulativeWeights;
    const target = this.#options.random() * (weights.at(-1) ?? 0);
    let lo = 0;
    let hi = weights.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (weights[mid] <= target) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return this.#options.screenQueries[lo];
  }

  #after(session: Session, ms: number, fn: () => void): void {
    const timer = setTimeout(() => {
      session.timers.delete(timer);
      fn();
    }, ms);
    session.timers.add(timer);
  }

  #end(session: Session): void {
    for (const timer of session.timers) {
      clearTimeout(timer);
    }
    session.timers.clear();
    session.client.close();
  }
}

export function exponential(random: () => number, mean: number): number {
  return -Math.log(1 - random()) * mean;
}

/** A small seeded generator (mulberry32) so runs are repeatable. */
export function seededRandom(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
