/**
 * Runs a client scenario against the real zero-client (`TestZero` over a
 * `MockSocket`, fake timers, in-memory storage) and records what an app and
 * the server would observe after every step. The Swift runner replays the
 * steps and must observe the same.
 *
 * Values that differ from run to run are replaced by placeholders: the
 * client ID by `$clientID`, the client group ID by `$clientGroupID`, the
 * connect URL's `wsid` and `profileID` by `$wsid` and `$profileID`, and every
 * `requestID` by `$requestID`. The URL's `ts` (a monotonic clock reading) is
 * dropped. Downstream messages in the steps may use `$clientID` wherever the
 * server would send the client's ID, in keys and in values.
 */
import {vi} from 'vitest';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {
  MockSocket,
  TestZero,
} from '../../../zero-client/src/client/test-utils.ts';
import {decodeSecProtocols} from '../../../zero-protocol/src/connect.ts';
import {POKE_CHUNK_MESSAGE_TYPE} from '../../../zero-protocol/src/poke.ts';
import type {TTL} from '../../../zql/src/query/ttl.ts';
import {issueMutators, mutators, queries, schema} from './app.ts';

/** `Date.now()` when every scenario starts. */
export const START_TIME = 1_700_000_000_000;

export type ScenarioOptions = {
  /** Defaults to `u1`; `null` omits it (a logged-out client). */
  userID?: string | null | undefined;
  /** Defaults to no auth token. */
  auth?: string | undefined;
  /** Defaults to `http://zero.test`; `null` leaves the client unconnected. */
  cacheURL?: string | null | undefined;
  pingTimeoutMs?: number | undefined;
  disconnectTimeoutMs?: number | undefined;
  queryChangeThrottleMs?: number | undefined;
  maxHeaderLength?: number | undefined;
  mutateURL?: string | undefined;
  mutateHeaders?: Record<string, string> | undefined;
  queryURL?: string | undefined;
  queryHeaders?: Record<string, string> | undefined;
  storageKey?: string | undefined;
  /** Defaults to 5 minutes, the JS default. */
  hiddenTabDisconnectDelay?: number | undefined;
};

export type Step =
  | {op: 'materialize'; view: string; query: string; args?: Args; ttl?: TTL}
  | {op: 'destroy'; view: string}
  | {op: 'updateTTL'; view: string; ttl: TTL}
  | {op: 'preload'; preload: string; query: string; args?: Args; ttl?: TTL}
  | {op: 'cleanup'; preload: string}
  | {
      op: 'run';
      run: string;
      query: string;
      args?: Args;
      type?: 'unknown' | 'cached' | 'complete';
    }
  | {op: 'mutate'; mutation: string; mutator: string; args?: Args}
  /**
   * A text frame from the server. Socket steps go to the newest socket, or
   * to `socket` (an index in opening order) when given.
   */
  | {op: 'receive'; message: ReadonlyJSONValue; socket?: number}
  /**
   * Several text frames, back to back with nothing settling in between, as
   * when they arrive in one network read.
   */
  | {op: 'receiveAll'; messages: ReadonlyJSONValue[]; socket?: number}
  /**
   * A binary poke chunk. Its payload is `parts` (`pokePart` bodies) as JSON,
   * or exactly `text`, or exactly the UTF-8 `bytes`, so a poke can be split
   * anywhere, even inside a code point.
   */
  | {
      op: 'receiveChunk';
      parts?: ReadonlyJSONValue[];
      text?: string;
      bytes?: number[];
      socket?: number;
    }
  /** The socket closes from the server or network side. */
  | {
      op: 'socketClose';
      code?: number;
      reason?: string;
      wasClean?: boolean;
      socket?: number;
    }
  | {op: 'advance'; ms: number}
  /**
   * The app goes to the background (`hidden`) or comes back (`visible`), as a
   * tab does (`document.visibilityState`).
   */
  | {op: 'visibility'; state: 'visible' | 'hidden'}
  /** `zero.connection.connect()`. */
  | {op: 'connect'; auth?: string}
  | {op: 'close'}
  /**
   * Closes the client and starts a new one on the same storage, as an app
   * does when it is relaunched. `advance` moves the clock while no client
   * runs. `options` replace the scenario's options; the storage stays the
   * scenario's unless they set another `storageKey`. `mutators: 'issues'`
   * registers only the `issue` mutators. The old client's views, runs,
   * mutations and preloads are dropped, and its IDs become `$clientID.<n>`
   * and `$clientGroupID.<n>` for the n-th client, from 0. The step settles
   * once the new client has opened its storage.
   */
  | {
      op: 'restart';
      advance?: number;
      options?: ScenarioOptions;
      mutators?: 'all' | 'issues';
    };

type Args = ReadonlyJSONValue | undefined;

export type Scenario = {
  name: string;
  options?: ScenarioOptions | undefined;
  steps: Step[];
};

type Settled = 'pending' | ReadonlyJSONValue;

/** The document whose visibility the client follows. */
class FakeDocument {
  visibilityState: DocumentVisibilityState = 'visible';
  readonly #listeners = new Set<() => void>();

  addEventListener(
    _type: string,
    listener: () => void,
    options?: {signal?: AbortSignal},
  ): void {
    this.#listeners.add(listener);
    options?.signal?.addEventListener('abort', () =>
      this.#listeners.delete(listener),
    );
  }

  removeEventListener(_type: string, listener: () => void): void {
    this.#listeners.delete(listener);
  }

  setVisibility(state: DocumentVisibilityState): void {
    this.visibilityState = state;
    for (const listener of [...this.#listeners]) listener();
  }
}

export type StepResult = {
  /** Thrown synchronously by the step, such as an unknown query. */
  threw?: string;
  /** Sockets the client opened during the step. */
  opened?: {url: SocketURL; protocol: ReadonlyJSONValue}[];
  /** Messages the client sent during the step, oldest first. */
  sent?: ReadonlyJSONValue[];
  /** Indexes (in opening order) of sockets the client closed. */
  closed?: number[];
  /** `connection.state.current`. */
  state: ReadonlyJSONValue;
  /** Every view not yet destroyed. */
  views?: Record<
    string,
    {data: ReadonlyJSONValue; resultType: string; notifications: number}
  >;
  mutations?: Record<string, {client: Settled; server: Settled}>;
  runs?: Record<string, Settled>;
  preloads?: Record<string, {cached: Settled; complete: Settled}>;
  /** `onUpdateNeeded` and `onClientStateNotFound` calls. */
  events?: ReadonlyJSONValue[];
};

class CloseEventPolyfill extends Event {
  readonly code: number;
  readonly reason: string;
  readonly wasClean: boolean;
  constructor(
    type: string,
    init?: {code?: number; reason?: string; wasClean?: boolean},
  ) {
    super(type);
    this.code = init?.code ?? 0;
    this.reason = init?.reason ?? '';
    this.wasClean = init?.wasClean ?? false;
  }
}

type View = {
  destroy(): void;
  updateTTL(ttl: TTL): void;
  addListener(
    cb: (data: unknown, resultType: string, error?: unknown) => void,
  ): () => void;
};

// oxlint-disable-next-line @typescript-eslint/no-explicit-any
type AnyFn = (...args: any[]) => any;

function lookup(registry: object, name: string): AnyFn {
  let v: unknown = registry;
  for (const part of name.split('.')) {
    v = (v as Record<string, unknown>)[part];
  }
  if (typeof v !== 'function') {
    throw new Error(`Unknown name ${name}`);
  }
  return v as AnyFn;
}

function json(v: unknown): ReadonlyJSONValue {
  return v === undefined ? null : JSON.parse(JSON.stringify(v));
}

function errorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/** Settles promises and the timers they schedule at the current time. */
async function settle() {
  for (let i = 0; i < 20; i++) {
    await vi.advanceTimersByTimeAsync(0);
  }
}

let scenarioCount = 0;

export async function runScenario(scenario: Scenario): Promise<StepResult[]> {
  vi.useFakeTimers({now: START_TIME});
  // A zero-delay timer runs at the current time. Fake timers push one that is
  // created while timers run to the next millisecond, so work the client
  // defers with `setTimeout(0)` (applying pokes) would wait for the next
  // `advance`.
  const fakeSetTimeout = globalThis.setTimeout;
  const clock = (fakeSetTimeout as unknown as {clock: {duringTick: boolean}})
    .clock;
  globalThis.setTimeout = ((
    fn: () => void,
    ms?: number,
    ...args: unknown[]
  ) => {
    if (ms) return fakeSetTimeout(fn, ms, ...args);
    const {duringTick} = clock;
    clock.duringTick = false;
    try {
      return fakeSetTimeout(fn, 0, ...args);
    } finally {
      clock.duringTick = duringTick;
    }
  }) as typeof setTimeout;
  const sockets: MockSocket[] = [];
  class RecordingSocket extends MockSocket {
    constructor(url: string | URL, protocol?: string) {
      super(url, protocol);
      sockets.push(this);
    }
  }
  vi.stubGlobal('WebSocket', RecordingSocket);
  vi.stubGlobal('CloseEvent', CloseEventPolyfill);
  const document = new FakeDocument();
  vi.stubGlobal('document', document);

  const events: ReadonlyJSONValue[] = [];
  const scenarioKey = ++scenarioCount;
  const makeZero = (
    options: ScenarioOptions,
    registered: 'all' | 'issues' = 'all',
  ) =>
    new TestZero({
      cacheURL:
        options.cacheURL === null
          ? undefined
          : (options.cacheURL ?? 'http://zero.test'),
      userID: options.userID === null ? undefined : (options.userID ?? 'u1'),
      auth: options.auth,
      schema,
      mutators: registered === 'all' ? mutators : issueMutators,
      kvStore: 'mem',
      pingTimeoutMs: options.pingTimeoutMs,
      disconnectTimeoutMs: options.disconnectTimeoutMs,
      queryChangeThrottleMs: options.queryChangeThrottleMs,
      maxHeaderLength: options.maxHeaderLength,
      mutateURL: options.mutateURL,
      mutateHeaders: options.mutateHeaders,
      queryURL: options.queryURL,
      queryHeaders: options.queryHeaders,
      hiddenTabDisconnectDelay: options.hiddenTabDisconnectDelay,
      // In-memory stores outlive their Zero instance, keyed by name, so every
      // scenario gets a store of its own. The storage key is not sent.
      storageKey: `${options.storageKey ?? ''}#scenario-${scenarioKey}`,
      onUpdateNeeded: reason => {
        events.push({onUpdateNeeded: json(reason)});
      },
      onClientStateNotFound: () => {
        events.push('onClientStateNotFound');
      },
    });
  let z = makeZero(scenario.options ?? {});
  let {clientID} = z;
  let clientGroupID = await z.clientGroupID;
  let restarts = 0;

  const placeholders = new Map<string, string>([
    [clientID, '$clientID'],
    [clientGroupID, '$clientGroupID'],
  ]);
  const ids = new Map<string, string>([['$clientID', clientID]]);
  const normalize = (v: ReadonlyJSONValue, key?: string): ReadonlyJSONValue => {
    if (typeof v === 'string') {
      if (key === 'requestID') return '$requestID';
      return placeholders.get(v) ?? v;
    }
    if (Array.isArray(v)) return v.map(e => normalize(e));
    if (v !== null && typeof v === 'object') {
      return Object.fromEntries(
        Object.entries(v).map(([k, e]) => [
          placeholders.get(k) ?? k,
          normalize(e as ReadonlyJSONValue, k),
        ]),
      );
    }
    return v;
  };
  const substitute = (v: ReadonlyJSONValue): ReadonlyJSONValue => {
    if (typeof v === 'string') return ids.get(v) ?? v;
    if (Array.isArray(v)) return v.map(substitute);
    if (v !== null && typeof v === 'object') {
      return Object.fromEntries(
        Object.entries(v).map(([k, e]) => [
          ids.get(k) ?? k,
          substitute(e as ReadonlyJSONValue),
        ]),
      );
    }
    return v;
  };

  const views = new Map<
    string,
    {
      view: View;
      data: ReadonlyJSONValue;
      resultType: string;
      notifications: number;
    }
  >();
  const mutations = new Map<string, {client: Settled; server: Settled}>();
  const runs = new Map<string, Settled>();
  const preloads = new Map<
    string,
    {cached: Settled; complete: Settled; cleanup: () => void}
  >();
  const sentCounts: number[] = [];
  const closedBefore: boolean[] = [];

  const targetSocket = (index: number | undefined) => {
    const socket = index === undefined ? sockets.at(-1) : sockets[index];
    if (!socket) throw new Error('No socket');
    return socket;
  };

  const request = (query: string, args: Args) =>
    lookup(queries, query)(...(args === undefined ? [] : [args]));

  async function perform(step: Step): Promise<void> {
    switch (step.op) {
      case 'materialize': {
        const view = z.materialize(
          request(step.query, step.args),
          step.ttl === undefined ? undefined : {ttl: step.ttl},
        ) as unknown as View;
        const entry: {
          view: View;
          data: ReadonlyJSONValue;
          resultType: string;
          notifications: number;
        } = {view, data: null, resultType: '', notifications: 0};
        views.set(step.view, entry);
        view.addListener((data, resultType) => {
          entry.data = json(data);
          entry.resultType = resultType;
          entry.notifications++;
        });
        return;
      }
      case 'destroy':
        views.get(step.view)?.view.destroy();
        views.delete(step.view);
        return;
      case 'updateTTL':
        views.get(step.view)?.view.updateTTL(step.ttl);
        return;
      case 'preload': {
        const {cleanup, cached, complete} = z.preload(
          request(step.query, step.args),
          step.ttl === undefined ? undefined : {ttl: step.ttl},
        );
        const entry = {
          cached: 'pending' as Settled,
          complete: 'pending' as Settled,
          cleanup,
        };
        preloads.set(step.preload, entry);
        cached.then(
          () => (entry.cached = 'resolved'),
          e => (entry.cached = {rejected: json(e)}),
        );
        complete.then(
          () => (entry.complete = 'resolved'),
          e => (entry.complete = {rejected: json(e)}),
        );
        return;
      }
      case 'cleanup':
        preloads.get(step.preload)?.cleanup();
        return;
      case 'run': {
        runs.set(step.run, 'pending');
        z.run(
          request(step.query, step.args),
          step.type === undefined ? undefined : {type: step.type},
        ).then(
          data => runs.set(step.run, {data: json(data)}),
          e => runs.set(step.run, {rejected: json(e)}),
        );
        return;
      }
      case 'mutate': {
        const mutator = lookup(mutators, step.mutator);
        const result = z.mutate(
          mutator(...(step.args === undefined ? [] : [step.args])),
        );
        const entry: {client: Settled; server: Settled} = {
          client: 'pending',
          server: 'pending',
        };
        mutations.set(step.mutation, entry);
        void result.client.then(r => (entry.client = json(r)));
        void result.server.then(r => (entry.server = json(r)));
        return;
      }
      case 'receive':
        targetSocket(step.socket).dispatchEvent(
          new MessageEvent('message', {
            data: JSON.stringify(substitute(step.message)),
          }),
        );
        return;
      case 'receiveAll': {
        const socket = targetSocket(step.socket);
        for (const message of step.messages) {
          socket.dispatchEvent(
            new MessageEvent('message', {
              data: JSON.stringify(substitute(message)),
            }),
          );
        }
        return;
      }
      case 'receiveChunk': {
        const payload = step.bytes
          ? Uint8Array.from(step.bytes)
          : new TextEncoder().encode(
              step.text ?? JSON.stringify(substitute(step.parts ?? [])),
            );
        const bytes = new Uint8Array(payload.byteLength + 1);
        bytes[0] = POKE_CHUNK_MESSAGE_TYPE;
        bytes.set(payload, 1);
        targetSocket(step.socket).dispatchEvent(
          new MessageEvent('message', {data: bytes.buffer}),
        );
        return;
      }
      case 'socketClose':
        targetSocket(step.socket).dispatchEvent(
          new CloseEvent('close', {
            code: step.code ?? 1006,
            reason: step.reason ?? '',
            wasClean: step.wasClean ?? false,
          }),
        );
        return;
      case 'advance':
        await vi.advanceTimersByTimeAsync(step.ms);
        return;
      case 'visibility':
        document.setVisibility(step.state);
        return;
      case 'connect':
        void z.connection.connect(
          step.auth === undefined ? undefined : {auth: step.auth},
        );
        return;
      case 'close':
        void z.close();
        return;
      case 'restart': {
        await z.close();
        await settle();
        if (step.advance !== undefined) {
          await vi.advanceTimersByTimeAsync(step.advance);
        }
        const n = restarts++;
        const old = {clientID, clientGroupID};
        z = makeZero(step.options ?? scenario.options ?? {}, step.mutators);
        clientID = z.clientID;
        clientGroupID = await z.clientGroupID;
        placeholders.set(old.clientID, `$clientID.${n}`);
        ids.set(`$clientID.${n}`, old.clientID);
        if (old.clientGroupID !== clientGroupID) {
          placeholders.set(old.clientGroupID, `$clientGroupID.${n}`);
        }
        placeholders.set(clientID, '$clientID');
        placeholders.set(clientGroupID, '$clientGroupID');
        ids.set('$clientID', clientID);
        views.clear();
        mutations.clear();
        runs.clear();
        preloads.clear();
        return;
      }
    }
  }

  const results: StepResult[] = [];
  try {
    for (const step of scenario.steps) {
      for (let i = 0; i < sockets.length; i++) {
        sentCounts[i] = sockets[i].messages.length;
        closedBefore[i] = sockets[i].closed;
      }
      const socketsBefore = sockets.length;
      const result: Partial<StepResult> = {};
      try {
        await perform(step);
      } catch (e) {
        result.threw = errorMessage(e);
      }
      await settle();

      const opened = sockets.slice(socketsBefore).map(s => ({
        url: socketURL(s, placeholders),
        protocol: normalize(json(decodeSecProtocols(s.protocol))),
      }));
      if (opened.length > 0) result.opened = opened;
      const sent = sockets.flatMap((s, i) =>
        s.messages
          .slice(sentCounts[i] ?? 0)
          .map(m => normalize(JSON.parse(m) as ReadonlyJSONValue)),
      );
      if (sent.length > 0) result.sent = sent;
      const closed = sockets.flatMap((s, i) =>
        s.closed && !closedBefore[i] ? [i] : [],
      );
      if (closed.length > 0) result.closed = closed;
      result.state = normalize(json(z.connection.state.current));
      if (views.size > 0) {
        result.views = Object.fromEntries(
          Array.from(views, ([k, {data, resultType, notifications}]) => [
            k,
            {data, resultType, notifications},
          ]),
        );
        for (const entry of views.values()) entry.notifications = 0;
      }
      if (mutations.size > 0) {
        result.mutations = Object.fromEntries(
          Array.from(mutations, ([k, v]) => [k, {...v}]),
        );
      }
      if (runs.size > 0) result.runs = Object.fromEntries(runs);
      if (preloads.size > 0) {
        result.preloads = Object.fromEntries(
          Array.from(preloads, ([k, {cached, complete}]) => [
            k,
            {cached, complete},
          ]),
        );
      }
      if (events.length > 0) {
        result.events = events.splice(0);
      }
      results.push(result as StepResult);
    }
  } finally {
    await z.close();
    globalThis.setTimeout = fakeSetTimeout;
    vi.useRealTimers();
    vi.unstubAllGlobals();
  }
  return results;
}

/** The connect URL, with its query parameters in order. */
export type SocketURL = {
  origin: string;
  path: string;
  query: [string, string][];
};

function socketURL(
  socket: MockSocket,
  placeholders: Map<string, string>,
): SocketURL {
  const url = new URL(String(socket.url));
  const query: [string, string][] = [];
  for (const [k, v] of url.searchParams) {
    switch (k) {
      case 'ts':
        break;
      case 'wsid':
      case 'profileID':
        query.push([k, `$${k}`]);
        break;
      default:
        query.push([k, placeholders.get(v) ?? v]);
    }
  }
  return {origin: url.origin, path: url.pathname, query};
}
