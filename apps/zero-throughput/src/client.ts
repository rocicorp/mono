import {Zero, type ConnectionState, type ResultType} from '@rocicorp/zero';
import WebSocket from 'ws';
import type {BenchmarkConfig} from './config.ts';
import {buildProfileQuery} from './profile-queries.ts';
import {schema} from './schema.ts';
import {nowMs, withTimeout} from './util.ts';

export type ClientQueryStats = {
  readonly clientID: string;
  readonly queryIndex: number;
  readonly queryName: string;
  readonly initialSyncMs: number | undefined;
  readonly updates: number;
  readonly observedSeq: number;
  readonly observedRows: number;
  readonly latencySamplesMs: readonly number[];
  readonly lastResultType: ResultType;
};

export type ClientStats = {
  readonly userID: string;
  readonly connected: boolean;
  readonly connectionState: ConnectionState;
  readonly queries: readonly ClientQueryStats[];
};

type MaterializedView = {
  addListener(
    listener: (rows: readonly unknown[], resultType: ResultType) => void,
  ): () => void;
  destroy(): void;
};

type Signal = {
  readonly seq: number;
  readonly writtenAt: number;
};

export class SyntheticClient {
  readonly userID: string;
  readonly #zero: Zero<typeof schema>;
  readonly #queryStates: QueryState[] = [];
  readonly #createdAtMs = nowMs();
  #connectionState: ConnectionState;
  #unsubscribeConnection: (() => void) | undefined;

  constructor(userID: string, config: BenchmarkConfig) {
    this.userID = userID;
    installWebSocketPolyfill();
    this.#zero = new Zero({
      schema,
      cacheURL: config.cacheURL,
      userID,
      storageKey: config.runID,
      kvStore: 'mem',
      logLevel: 'error',
      queryChangeThrottleMs: 0,
    });
    this.#connectionState = this.#zero.connection.state.current;
    this.#unsubscribeConnection = this.#zero.connection.state.subscribe(
      state => {
        this.#connectionState = state;
      },
    );

    for (let i = 0; i < config.queriesPerUser; i++) {
      this.#registerProfileQuery(config, i);
    }
  }

  #registerProfileQuery(config: BenchmarkConfig, queryIndex: number): void {
    const {name, query} = buildProfileQuery(
      this.#zero.query,
      config.profile,
      queryIndex,
      config.rowsPerQuery,
    );
    const view = this.#zero.materialize(query);
    this.#registerView(name, queryIndex, view);
  }

  #registerView(
    queryName: string,
    queryIndex: number,
    view: MaterializedView,
  ): void {
    const state = new QueryState(
      this.userID,
      queryIndex,
      queryName,
      this.#createdAtMs,
    );
    const unsubscribe = view.addListener((rows, resultType) => {
      state.observe(rows, resultType);
    });
    state.setCleanup(() => {
      unsubscribe();
      view.destroy();
    });
    this.#queryStates.push(state);
  }

  async waitForInitialSync(timeoutMs: number): Promise<void> {
    await withTimeout(
      Promise.all(this.#queryStates.map(state => state.initialSync)),
      timeoutMs,
      `${this.userID} did not complete initial sync within ${timeoutMs}ms`,
    );
  }

  stats(): ClientStats {
    return {
      userID: this.userID,
      connected: this.#connectionState.name === 'connected',
      connectionState: this.#connectionState,
      queries: this.#queryStates.map(state => state.stats()),
    };
  }

  minObservedSeq(): number {
    return Math.min(...this.#queryStates.map(state => state.observedSeq));
  }

  latencySamplesMs(): number[] {
    return this.#queryStates.flatMap(state => state.latencySamplesMs);
  }

  async close(): Promise<void> {
    this.#unsubscribeConnection?.();
    this.#unsubscribeConnection = undefined;
    for (const state of this.#queryStates) {
      state.cleanup();
    }
    await this.#zero.close();
  }
}

export async function startSyntheticClients(
  config: BenchmarkConfig,
): Promise<SyntheticClient[]> {
  const clients: SyntheticClient[] = [];
  for (let i = 0; i < config.users; i++) {
    clients.push(new SyntheticClient(`throughput-user-${i}`, config));
  }
  await Promise.all(
    clients.map(client => client.waitForInitialSync(config.warmupMs)),
  );
  return clients;
}

class QueryState {
  readonly #clientID: string;
  readonly #queryIndex: number;
  readonly #queryName: string;
  readonly #createdAtMs: number;
  readonly #initialSyncPromise: Promise<void>;
  #resolveInitialSync: () => void = () => undefined;
  #cleanup: (() => void) | undefined;
  #initialSyncMs: number | undefined;
  #updates = 0;
  #observedSeq = 0;
  #observedRows = 0;
  #lastResultType: ResultType = 'unknown';
  readonly #latencySamplesMs: number[] = [];

  constructor(
    clientID: string,
    queryIndex: number,
    queryName: string,
    createdAtMs: number,
  ) {
    this.#clientID = clientID;
    this.#queryIndex = queryIndex;
    this.#queryName = queryName;
    this.#createdAtMs = createdAtMs;
    this.#initialSyncPromise = new Promise(resolve => {
      this.#resolveInitialSync = resolve;
    });
  }

  get initialSync(): Promise<void> {
    return this.#initialSyncPromise;
  }

  get observedSeq(): number {
    return this.#observedSeq;
  }

  get latencySamplesMs(): readonly number[] {
    return this.#latencySamplesMs;
  }

  setCleanup(cleanup: () => void): void {
    this.#cleanup = cleanup;
  }

  cleanup(): void {
    this.#cleanup?.();
    this.#cleanup = undefined;
  }

  observe(rows: readonly unknown[], resultType: ResultType): void {
    this.#updates++;
    this.#lastResultType = resultType;
    if (resultType === 'complete' && this.#initialSyncMs === undefined) {
      this.#initialSyncMs = nowMs() - this.#createdAtMs;
      this.#resolveInitialSync();
    }

    const receivedAtMs = nowMs();
    const previousObservedSeq = this.#observedSeq;
    let maxSeq = this.#observedSeq;
    const newWrittenAtBySeq = new Map<number, number>();
    for (const signal of collectSignals(rows)) {
      if (signal.seq > previousObservedSeq) {
        const writtenAt = newWrittenAtBySeq.get(signal.seq);
        if (writtenAt === undefined || signal.writtenAt < writtenAt) {
          newWrittenAtBySeq.set(signal.seq, signal.writtenAt);
        }
      }
      maxSeq = Math.max(maxSeq, signal.seq);
    }
    for (const writtenAt of newWrittenAtBySeq.values()) {
      this.#latencySamplesMs.push(Math.max(0, receivedAtMs - writtenAt));
    }
    this.#observedSeq = maxSeq;
    this.#observedRows += rows.length;
  }

  stats(): ClientQueryStats {
    return {
      clientID: this.#clientID,
      queryIndex: this.#queryIndex,
      queryName: this.#queryName,
      initialSyncMs: this.#initialSyncMs,
      updates: this.#updates,
      observedSeq: this.#observedSeq,
      observedRows: this.#observedRows,
      latencySamplesMs: this.#latencySamplesMs,
      lastResultType: this.#lastResultType,
    };
  }
}

function collectSignals(value: unknown): Signal[] {
  const signals: Signal[] = [];
  collectSignalsInto(value, signals, new Set());
  return signals;
}

function collectSignalsInto(
  value: unknown,
  signals: Signal[],
  seen: Set<object>,
): void {
  if (value === null || value === undefined) {
    return;
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      collectSignalsInto(item, signals, seen);
    }
    return;
  }
  if (typeof value !== 'object') {
    return;
  }
  if (seen.has(value)) {
    return;
  }
  seen.add(value);

  const record = value as Record<string, unknown>;
  const seq = record.seq;
  const writtenAt = record.writtenAt;
  if (
    typeof seq === 'number' &&
    Number.isFinite(seq) &&
    typeof writtenAt === 'number' &&
    Number.isFinite(writtenAt)
  ) {
    signals.push({seq, writtenAt});
  }

  for (const child of Object.values(record)) {
    collectSignalsInto(child, signals, seen);
  }
}

function installWebSocketPolyfill(): void {
  const globalWithWebSocket = globalThis as typeof globalThis & {
    WebSocket?: typeof globalThis.WebSocket | undefined;
  };
  globalWithWebSocket.WebSocket ??=
    WebSocket as unknown as typeof globalThis.WebSocket;
}
