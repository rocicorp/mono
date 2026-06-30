import {Zero, type ConnectionState, type ResultType} from '@rocicorp/zero';
import WebSocket from 'ws';
import type {BenchmarkConfig} from './config.ts';
import {schema, type ThroughputEvent} from './schema.ts';
import {nowMs, withTimeout} from './util.ts';

export type ClientQueryStats = {
  readonly clientID: string;
  readonly queryIndex: number;
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
      const eventQuery = this.#zero.query.event;
      if (eventQuery === undefined) {
        throw new Error('Zero query builder did not expose event table');
      }
      const query = eventQuery
        .where('bucket', 0)
        .orderBy('seq', 'desc')
        .limit(config.rowsPerQuery);
      const state = new QueryState(this.userID, i, this.#createdAtMs);
      const view = this.#zero.materialize(query);
      const unsubscribe = view.addListener((rows, resultType) => {
        state.observe(rows as readonly ThroughputEvent[], resultType);
      });
      state.setCleanup(() => {
        unsubscribe();
        view.destroy();
      });
      this.#queryStates.push(state);
    }
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

  constructor(clientID: string, queryIndex: number, createdAtMs: number) {
    this.#clientID = clientID;
    this.#queryIndex = queryIndex;
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

  observe(rows: readonly ThroughputEvent[], resultType: ResultType): void {
    this.#updates++;
    this.#lastResultType = resultType;
    if (resultType === 'complete' && this.#initialSyncMs === undefined) {
      this.#initialSyncMs = nowMs() - this.#createdAtMs;
      this.#resolveInitialSync();
    }

    const receivedAtMs = nowMs();
    let maxSeq = this.#observedSeq;
    for (const row of rows) {
      if (row.seq > this.#observedSeq) {
        this.#latencySamplesMs.push(Math.max(0, receivedAtMs - row.writtenAt));
      }
      maxSeq = Math.max(maxSeq, row.seq);
    }
    this.#observedSeq = maxSeq;
    this.#observedRows += rows.length;
  }

  stats(): ClientQueryStats {
    return {
      clientID: this.#clientID,
      queryIndex: this.#queryIndex,
      initialSyncMs: this.#initialSyncMs,
      updates: this.#updates,
      observedSeq: this.#observedSeq,
      observedRows: this.#observedRows,
      latencySamplesMs: this.#latencySamplesMs,
      lastResultType: this.#lastResultType,
    };
  }
}

function installWebSocketPolyfill(): void {
  const globalWithWebSocket = globalThis as typeof globalThis & {
    WebSocket?: typeof globalThis.WebSocket | undefined;
  };
  globalWithWebSocket.WebSocket ??=
    WebSocket as unknown as typeof globalThis.WebSocket;
}
