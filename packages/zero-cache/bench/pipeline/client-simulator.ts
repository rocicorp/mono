import {resolver, type Resolver} from '@rocicorp/resolver';
import WebSocket from 'ws';
import {Queue} from '../../../shared/src/queue.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../zero-protocol/src/client-schema.ts';
import {
  encodeSecProtocols,
  type InitConnectionMessage,
} from '../../../zero-protocol/src/connect.ts';
import {PROTOCOL_VERSION} from '../../../zero-protocol/src/protocol-version.ts';

export interface ClientStats {
  readonly clientID: string;
  readonly clientGroupID: string;
  readonly viewSyncerIndex: number;
  readonly connected: boolean;
  readonly initialHydrationDurationMs: number | null;
  readonly pokesReceived: number;
  readonly lastPokeTimestamp: number | null;
  readonly lastPokeCookie: string | null;
  readonly lastPokeWatermark: string | null;
  readonly errors: readonly string[];
}

export class SimulatedClient {
  readonly clientID: string;
  readonly clientGroupID: string;
  readonly viewSyncerIndex: number;
  readonly #port: number;
  readonly #queries: readonly AST[];
  readonly #clientSchema: ClientSchema;

  #ws: WebSocket | null = null;
  #connected = false;
  #closing = false;
  #connectTime = 0;
  #initialHydrationDurationMs: number | null = null;
  #pokesReceived = 0;
  #lastPokeTimestamp: number | null = null;
  #lastPokeCookie: string | null = null;
  #lastPokeWatermark: string | null = null;
  readonly #errors: string[] = [];

  readonly #messageQueue = new Queue<unknown>();
  readonly #initialHydrationResolver = resolver<void>();
  #nextPokeResolvers: Resolver<void>[] = [];

  constructor(options: {
    clientID: string;
    clientGroupID: string;
    viewSyncerIndex: number;
    port: number;
    queries: readonly AST[];
    clientSchema: ClientSchema;
  }) {
    this.clientID = options.clientID;
    this.clientGroupID = options.clientGroupID;
    this.viewSyncerIndex = options.viewSyncerIndex;
    this.#port = options.port;
    this.#queries = options.queries;
    this.#clientSchema = options.clientSchema;
  }

  async connect(maxRetries = 30, retryDelayMs = 150): Promise<void> {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        await this.#attemptConnect();
        return;
      } catch (err) {
        if (attempt === maxRetries - 1) {
          throw err;
        }
        await sleep(retryDelayMs);
      }
    }
  }

  #attemptConnect(): Promise<void> {
    const wsid = Math.floor(Math.random() * 1_000_000).toString(36);
    this.#connectTime = performance.now();

    const connectResolver = resolver<void>();

    const url =
      `ws://localhost:${this.#port}/zero/sync/v${PROTOCOL_VERSION}/connect` +
      `?clientGroupID=${encodeURIComponent(this.clientGroupID)}` +
      `&clientID=${encodeURIComponent(this.clientID)}` +
      `&wsid=${wsid}` +
      `&schemaVersion=1` +
      `&baseCookie=` +
      `&ts=${Date.now()}` +
      `&lmid=0`;

    const initMsg: InitConnectionMessage = [
      'initConnection',
      {
        desiredQueriesPatch: this.#queries.map((ast, i) => ({
          op: 'put',
          hash: `q_${this.clientID}_${i}`,
          ast,
        })),
        clientSchema: this.#clientSchema,
      },
    ];

    const secProtocol = encodeSecProtocols(initMsg, undefined);
    const ws = new WebSocket(url, secProtocol);
    this.#ws = ws;

    ws.on('open', () => {
      this.#connected = true;
    });

    ws.on('message', (data: WebSocket.RawData, isBinary: boolean) => {
      if (isBinary) {
        return;
      }

      try {
        const msg = JSON.parse(data.toString('utf-8'));
        this.#messageQueue.enqueue(msg);
        this.#handleMessage(msg, connectResolver);
      } catch (e) {
        this.#errors.push(`Parse error: ${String(e)}`);
      }
    });

    ws.on('error', err => {
      if (!this.#closing) {
        this.#errors.push(`WS error: ${err.message}`);
      }
      connectResolver.reject(err);
    });

    ws.on('close', (code, reason) => {
      this.#connected = false;
      if (!this.#closing && code !== 1000) {
        this.#errors.push(`WS closed (${code}): ${reason.toString()}`);
      }
    });

    return connectResolver.promise;
  }

  #handleMessage(msg: unknown, connectResolver: Resolver<void>): void {
    if (!Array.isArray(msg) || msg.length === 0) {
      return;
    }

    const type = msg[0];

    switch (type) {
      case 'connected':
        connectResolver.resolve();
        break;

      case 'ping':
        this.#ws?.send(JSON.stringify(['pong', {}]));
        break;

      case 'pokeStart': {
        const payload = (msg[1] ?? {}) as {
          cookie?: string | undefined;
          watermark?: string | undefined;
        };
        if (payload.cookie) {
          this.#lastPokeCookie = payload.cookie;
        }
        if (payload.watermark) {
          this.#lastPokeWatermark = payload.watermark;
        }
        break;
      }

      case 'pokeEnd': {
        this.#pokesReceived++;
        this.#lastPokeTimestamp = performance.now();

        const payload = (msg[1] ?? {}) as {
          cookie?: string | undefined;
          watermark?: string | undefined;
        };
        if (payload.cookie) {
          this.#lastPokeCookie = payload.cookie;
        }
        if (payload.watermark) {
          this.#lastPokeWatermark = payload.watermark;
        }

        if (this.#initialHydrationDurationMs === null) {
          this.#initialHydrationDurationMs =
            this.#lastPokeTimestamp - this.#connectTime;
          this.#initialHydrationResolver.resolve();
        }

        const waiters = this.#nextPokeResolvers;
        this.#nextPokeResolvers = [];
        for (const w of waiters) {
          w.resolve();
        }
        break;
      }

      case 'error': {
        this.#errors.push(`Protocol error: ${JSON.stringify(msg[1])}`);
        break;
      }
    }
  }

  /**
   * Waits for the client to receive its initial PokeEnd (hydration complete).
   */
  async waitForInitialHydration(timeoutMs = 60_000): Promise<void> {
    const timer = new Promise<never>((_, reject) =>
      setTimeout(
        () =>
          reject(
            new Error(
              `Hydration timed out after ${timeoutMs}ms for ${this.clientID}`,
            ),
          ),
        timeoutMs,
      ),
    );
    await Promise.race([this.#initialHydrationResolver.promise, timer]);
  }

  /**
   * Waits for the next poke to arrive.
   */
  waitForNextPoke(timeoutMs = 10_000): Promise<void> {
    const r = resolver<void>();
    this.#nextPokeResolvers.push(r);

    const timer = setTimeout(() => {
      const idx = this.#nextPokeResolvers.indexOf(r);
      if (idx !== -1) {
        this.#nextPokeResolvers.splice(idx, 1);
      }
      r.reject(new Error(`waitForNextPoke timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    return r.promise.finally(() => clearTimeout(timer));
  }

  get pokesReceived(): number {
    return this.#pokesReceived;
  }

  getStats(): ClientStats {
    return {
      clientID: this.clientID,
      clientGroupID: this.clientGroupID,
      viewSyncerIndex: this.viewSyncerIndex,
      connected: this.#connected,
      initialHydrationDurationMs: this.#initialHydrationDurationMs,
      pokesReceived: this.#pokesReceived,
      lastPokeTimestamp: this.#lastPokeTimestamp,
      lastPokeCookie: this.#lastPokeCookie,
      lastPokeWatermark: this.#lastPokeWatermark,
      errors: [...this.#errors],
    };
  }

  close(): void {
    this.#closing = true;
    if (this.#ws) {
      try {
        this.#ws.close(1000, 'benchmark complete');
      } catch {
        // ignore
      }
      this.#ws = null;
    }
    this.#connected = false;
  }
}

export interface ClientGroupSimulatorOptions {
  readonly viewSyncerPorts: readonly number[];
  readonly clientsPerViewSyncer: number;
  readonly queries: readonly AST[];
  readonly clientSchema: ClientSchema;
}

export class ClientGroupSimulator {
  readonly #clients: SimulatedClient[] = [];

  constructor(options: ClientGroupSimulatorOptions) {
    let clientIdx = 0;
    for (let vsIdx = 0; vsIdx < options.viewSyncerPorts.length; vsIdx++) {
      const port = options.viewSyncerPorts[vsIdx];
      for (let c = 0; c < options.clientsPerViewSyncer; c++) {
        const clientID = `bench_client_${clientIdx++}`;
        const clientGroupID = `bench_group_${vsIdx}_${c}`;
        this.#clients.push(
          new SimulatedClient({
            clientID,
            clientGroupID,
            viewSyncerIndex: vsIdx,
            port,
            queries: options.queries,
            clientSchema: options.clientSchema,
          }),
        );
      }
    }
  }

  get totalClients(): number {
    return this.#clients.length;
  }

  async connectAll(): Promise<void> {
    await Promise.all(this.#clients.map(client => client.connect()));
  }

  async waitForAllHydrated(timeoutMs = 60_000): Promise<void> {
    await Promise.all(
      this.#clients.map(client => client.waitForInitialHydration(timeoutMs)),
    );
  }

  /**
   * Waits until all clients have received all expected pokes or until timeoutMs expires.
   * Resolves immediately when:
   * 1. All clients have received >= expectedPokesPerClient, OR
   * 2. No new pokes have arrived across all clients for quiescentMs (settled).
   */
  async waitForDrain(
    options: {
      expectedPokesPerClient?: number | undefined;
      quiescentMs?: number | undefined;
      timeoutMs?: number | undefined;
    } = {},
  ): Promise<{drained: boolean; elapsedMs: number; totalPokes: number}> {
    const expected = options.expectedPokesPerClient ?? 0;
    const quiescentMs = options.quiescentMs ?? 2000;
    const timeoutMs = options.timeoutMs ?? 30_000;

    const start = performance.now();
    let lastTotalPokes = this.#clients.reduce(
      (sum, c) => sum + c.pokesReceived,
      0,
    );
    let lastPokeTime = performance.now();

    while (performance.now() - start < timeoutMs) {
      await sleep(100);

      const currentPokes = this.#clients.reduce(
        (sum, c) => sum + c.pokesReceived,
        0,
      );

      if (currentPokes > lastTotalPokes) {
        lastTotalPokes = currentPokes;
        lastPokeTime = performance.now();
      }

      // Check if all clients have reached target
      const allReachedTarget =
        expected > 0 && this.#clients.every(c => c.pokesReceived >= expected);

      if (allReachedTarget) {
        return {
          drained: true,
          elapsedMs: performance.now() - start,
          totalPokes: currentPokes,
        };
      }

      // Check if quiescence reached (no new pokes for quiescentMs after load started)
      if (
        performance.now() - lastPokeTime >= quiescentMs &&
        currentPokes > this.#clients.length
      ) {
        return {
          drained: true,
          elapsedMs: performance.now() - start,
          totalPokes: currentPokes,
        };
      }
    }

    return {
      drained: false,
      elapsedMs: performance.now() - start,
      totalPokes: lastTotalPokes,
    };
  }

  getAllStats(): ClientStats[] {
    return this.#clients.map(client => client.getStats());
  }

  closeAll(): void {
    for (const client of this.#clients) {
      client.close();
    }
  }
}
