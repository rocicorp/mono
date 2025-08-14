import type {LogLevel} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
// import {type VitestUtils} from 'vitest';
import type {Store} from '../../../replicache/src/dag/store.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import {TestLogSink} from '../../../shared/src/logging-test-utils.ts';
import type {ConnectedMessage} from '../../../zero-protocol/src/connect.ts';
import type {Downstream} from '../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import type {ErrorMessage} from '../../../zero-protocol/src/error.ts';
import type {
  PokeEndBody,
  PokeEndMessage,
  PokePartBody,
  PokePartMessage,
  PokeStartBody,
  PokeStartMessage,
} from '../../../zero-protocol/src/poke.ts';
import type {PongMessage} from '../../../zero-protocol/src/pong.ts';
import type {
  PullResponseBody,
  PullResponseMessage,
} from '../../../zero-protocol/src/pull.ts';
import type {
  PushResponse,
  PushResponseMessage,
} from '../../../zero-protocol/src/push.ts';
import {upstreamSchema} from '../../../zero-protocol/src/up.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {PullRow, Query} from '../../../zql/src/query/query.ts';
import * as ConnectionState from './connection-state-enum.ts';
import type {CustomMutatorDefs} from './custom.ts';
import type {LogOptions} from './log-options.ts';
import type {ZeroOptions} from './options.ts';
import {
  Zero,
  createLogOptionsSymbol,
  exposedToTestingSymbol,
  getInternalReplicacheImplForTesting,
  onSetConnectionStateSymbol,
  type TestingContext,
} from './zero.ts';

type ConnectionState = Enum<typeof ConnectionState>;

// Do not use an import statement here because vitest will then load that file
// which does not work in a worker context.
type VitestUtils = import('vitest').VitestUtils;

export async function tickAFewTimes(vi: VitestUtils, duration = 100) {
  const n = 10;
  const t = Math.ceil(duration / n);
  for (let i = 0; i < n; i++) {
    await vi.advanceTimersByTimeAsync(t);
  }
}

export class MockSocket extends EventTarget {
  readonly url: string | URL;
  protocol: string;
  messages: string[] = [];
  closed = false;
  #listeners = new Set<(message: string) => void>();

  constructor(url: string | URL, protocol = '') {
    super();
    this.url = url;
    this.protocol = protocol;
  }

  send(message: string) {
    this.messages.push(message);
    for (const listener of this.#listeners) {
      listener(message);
    }
  }

  onUpstream(callback: (message: string) => void): () => void {
    this.#listeners.add(callback);
    return () => {
      this.#listeners.delete(callback);
    };
  }

  close() {
    this.closed = true;
    this.dispatchEvent(new CloseEvent('close'));
  }
}

export class TestZero<
  const S extends Schema,
  MD extends CustomMutatorDefs | undefined = undefined,
> extends Zero<S, MD> {
  pokeIDCounter = 0;

  #connectionStateResolvers: Set<{
    state: ConnectionState;
    resolve: (state: ConnectionState) => void;
  }> = new Set();

  constructor(options: ZeroOptions<S, MD>) {
    super(options);
  }

  get perdag(): Store {
    return getInternalReplicacheImplForTesting(this).perdag;
  }

  get connectionState() {
    assert(TESTING);
    return this[exposedToTestingSymbol].connectionState();
  }

  get connectionStateAsString(): string {
    switch (this.connectionState) {
      case ConnectionState.Disconnected:
        return 'Disconnected';
      case ConnectionState.Connecting:
        return 'Connecting';
      case ConnectionState.Connected:
        return 'Connected';
    }
  }

  get connectingStart() {
    return this[exposedToTestingSymbol].connectStart;
  }

  // Testing only hook
  [onSetConnectionStateSymbol](newState: ConnectionState) {
    for (const entry of this.#connectionStateResolvers) {
      const {state, resolve} = entry;
      if (state === newState) {
        this.#connectionStateResolvers.delete(entry);
        resolve(newState);
      }
    }
  }

  [createLogOptionsSymbol](options: {consoleLogLevel: LogLevel}): LogOptions {
    assert(TESTING);
    return {
      logLevel: options.consoleLogLevel,
      logSink: new TestLogSink(),
    };
  }

  get testLogSink(): TestLogSink {
    assert(TESTING);
    const {logSink} = this[exposedToTestingSymbol].logOptions;
    assert(logSink instanceof TestLogSink);
    return logSink;
  }

  waitForConnectionState(state: ConnectionState) {
    if (this.connectionState === state) {
      return Promise.resolve(state);
    }
    const {promise, resolve} = resolver<ConnectionState>();
    this.#connectionStateResolvers.add({state, resolve});
    return promise;
  }

  get socket(): Promise<MockSocket> {
    return this[exposedToTestingSymbol].socketResolver()
      .promise as Promise<unknown> as Promise<MockSocket>;
  }

  async triggerMessage(data: Downstream): Promise<void> {
    const socket = await this.socket;
    assert(!socket.closed);
    socket.dispatchEvent(
      new MessageEvent('message', {data: JSON.stringify(data)}),
    );
  }

  async triggerConnected(): Promise<void> {
    const msg: ConnectedMessage = ['connected', {wsid: 'wsidx'}];
    await this.triggerMessage(msg);
    await this.waitForConnectionState(ConnectionState.Connected);
  }

  triggerPong(): Promise<void> {
    const msg: PongMessage = ['pong', {}];
    return this.triggerMessage(msg);
  }

  triggerPokeStart(pokeStartBody: PokeStartBody): Promise<void> {
    const msg: PokeStartMessage = ['pokeStart', pokeStartBody];
    return this.triggerMessage(msg);
  }

  triggerPokePart(pokePart: PokePartBody): Promise<void> {
    const msg: PokePartMessage = ['pokePart', pokePart];
    return this.triggerMessage(msg);
  }

  triggerPokeEnd(pokeEnd: PokeEndBody): Promise<void> {
    const msg: PokeEndMessage = ['pokeEnd', pokeEnd];
    return this.triggerMessage(msg);
  }

  async triggerPoke(
    cookieStart: string | null,
    cookieEnd: string,
    pokePart: Omit<PokePartBody, 'pokeID'>,
  ): Promise<void> {
    const id = `${this.pokeIDCounter++}`;
    await this.triggerPokeStart({
      pokeID: id,
      baseCookie: cookieStart,
    });
    await this.triggerPokePart({
      ...pokePart,
      pokeID: id,
    });
    await this.triggerPokeEnd({
      pokeID: id,
      cookie: cookieEnd,
    });
  }

  triggerPullResponse(pullResponseBody: PullResponseBody): Promise<void> {
    const msg: PullResponseMessage = ['pull', pullResponseBody];
    return this.triggerMessage(msg);
  }

  triggerPushResponse(pushResponseBody: PushResponse): Promise<void> {
    const msg: PushResponseMessage = ['pushResponse', pushResponseBody];
    return this.triggerMessage(msg);
  }

  triggerError(kind: ErrorKind, message: string, body = {}): Promise<void> {
    const msg: ErrorMessage = ['error', {kind, message, ...body}];
    return this.triggerMessage(msg);
  }

  async triggerClose(): Promise<void> {
    const socket = await this.socket;
    socket.dispatchEvent(new CloseEvent('close'));
  }

  async triggerGotQueriesPatch(
    q: Query<S, keyof S['tables'] & string>,
  ): Promise<void> {
    q.hash();
    await this.triggerPoke(null, '1', {
      gotQueriesPatch: [
        {
          op: 'put',
          hash: q.hash(),
        },
      ],
    });
  }

  declare [exposedToTestingSymbol]: TestingContext;

  get pusher() {
    assert(TESTING);
    return this[exposedToTestingSymbol].pusher;
  }

  get puller() {
    assert(TESTING);
    return this[exposedToTestingSymbol].puller;
  }

  set reload(r: () => void) {
    assert(TESTING);
    this[exposedToTestingSymbol].setReload(r);
  }

  persist(): Promise<void> {
    return getInternalReplicacheImplForTesting(this).persist();
  }

  markQueryAsGot<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn = PullRow<TTable, TSchema>,
  >(q: Query<TSchema, TTable, TReturn>): Promise<void> {
    // TODO(arv): The cookies here could be better... Not sure if the client
    // ever checks these?
    return this.triggerPoke(null, '1', {
      gotQueriesPatch: [
        {
          op: 'put',
          hash: q.hash(),
        },
      ],
    });
  }
}

declare const TESTING: boolean;

let testZeroCounter = 0;

export function zeroForTest<
  const S extends Schema,
  MD extends CustomMutatorDefs | undefined = undefined,
>(
  options: Partial<ZeroOptions<S, MD>> = {},
  errorOnUpdateNeeded = true,
): TestZero<S, MD> {
  // Special case kvStore. If not present we default to 'mem'. This allows
  // passing `undefined` to get the default behavior.
  const newOptions = {...options};
  if (!('kvStore' in options)) {
    newOptions.kvStore = 'mem';
  }

  return new TestZero({
    server: 'https://example.com/',
    // Make sure we do not reuse IDB instances between tests by default
    userID: options.userID ?? 'test-user-id-' + testZeroCounter++,
    auth: () => 'test-auth',
    schema: options.schema ?? ({tables: {}} as S),
    // We do not want any unexpected onUpdateNeeded calls in tests. If the test
    // needs to call onUpdateNeeded it should set this as needed.
    onUpdateNeeded: errorOnUpdateNeeded
      ? reason => {
          throw new Error(`Unexpected update needed: ${reason.type}`);
        }
      : undefined,
    ...newOptions,
  } satisfies ZeroOptions<S, MD>);
}

export async function waitForUpstreamMessage(
  r: TestZero<Schema>,
  name: string,
  vi: VitestUtils,
) {
  let gotMessage = false;
  const socket = await r.socket;
  const cleanup = socket.onUpstream(message => {
    const v = JSON.parse(message);
    const [kind] = upstreamSchema.parse(v);
    if (kind === name) {
      gotMessage = true;
    }
  });
  for (;;) {
    await vi.advanceTimersByTimeAsync(100);
    if (gotMessage) {
      cleanup();
      break;
    }
  }
}

export function storageMock(storage: Record<string, string>): Storage {
  return {
    setItem: (key, value) => {
      storage[key] = value || '';
    },
    getItem: key => (key in storage ? storage[key] : null),
    removeItem: key => {
      delete storage[key];
    },
    clear: () => {
      for (const key of Object.keys(storage)) {
        delete storage[key];
      }
    },
    get length() {
      return Object.keys(storage).length;
    },
    key: i => {
      const keys = Object.keys(storage);
      return keys[i] || null;
    },
  };
}
