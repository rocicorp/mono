import type {Context, LogLevel, LogSink} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {
  ConnectedMessage,
  Downstream,
  ErrorKind,
  ErrorMessage,
  PokeBody,
  PokeMessage,
  PongMessage,
  PullResponseBody,
  PullResponseMessage,
  upstreamSchema,
} from 'reflect-protocol';
import type {MutatorDefs} from 'reflect-shared/src/types.js';
import {assert} from 'shared/src/asserts.js';
import type {SinonFakeTimers} from 'sinon';
import type {LogOptions} from './log-options.js';
import type {ZeroOptions} from './options.js';
import {
  ConnectionState,
  QueryDefs,
  TestingContext,
  Zero,
  createLogOptionsSymbol,
  exposedToTestingSymbol,
  onSetConnectionStateSymbol,
} from './zero.js';

export async function tickAFewTimes(clock: SinonFakeTimers, duration = 100) {
  const n = 10;
  const t = Math.ceil(duration / n);
  for (let i = 0; i < n; i++) {
    await clock.tickAsync(t);
  }
}

export class MockSocket extends EventTarget {
  readonly url: string | URL;
  protocol: string;
  messages: string[] = [];
  closed = false;
  onUpstream?: (message: string) => void;

  constructor(url: string | URL, protocol = '') {
    super();
    this.url = url;
    this.protocol = protocol;
  }

  send(message: string) {
    this.messages.push(message);
    this.onUpstream?.(message);
  }

  close() {
    this.closed = true;
    this.dispatchEvent(new CloseEvent('close'));
  }
}

export class TestZero<
  MD extends MutatorDefs,
  QD extends QueryDefs,
> extends Zero<MD, QD> {
  #connectionStateResolvers: Set<{
    state: ConnectionState;
    resolve: (state: ConnectionState) => void;
  }> = new Set();

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

  triggerConnected(): Promise<void> {
    const msg: ConnectedMessage = ['connected', {wsid: 'wsidx'}];
    return this.triggerMessage(msg);
  }

  triggerPong(): Promise<void> {
    const msg: PongMessage = ['pong', {}];
    return this.triggerMessage(msg);
  }

  triggerPoke(pokeBody: PokeBody): Promise<void> {
    const msg: PokeMessage = ['poke', pokeBody];
    return this.triggerMessage(msg);
  }

  triggerPullResponse(pullResponseBody: PullResponseBody): Promise<void> {
    const msg: PullResponseMessage = ['pull', pullResponseBody];
    return this.triggerMessage(msg);
  }

  triggerError(kind: ErrorKind, message: string): Promise<void> {
    const msg: ErrorMessage = ['error', kind, message];
    return this.triggerMessage(msg);
  }

  async triggerClose(): Promise<void> {
    const socket = await this.socket;
    socket.dispatchEvent(new CloseEvent('close'));
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
}

declare const TESTING: boolean;

const testZeroInstances = new Set<TestZero<MutatorDefs, QueryDefs>>();

let testZeroCounter = 0;

export function zeroForTest<MD extends MutatorDefs, QD extends QueryDefs>(
  options: Partial<ZeroOptions<MD, QD>> = {},
): TestZero<MD, QD> {
  const r = new TestZero({
    server: 'https://example.com/',
    // Make sure we do not reuse IDB instances between tests by default
    userID: 'test-user-id-' + testZeroCounter++,
    roomID: 'test-room-id',
    auth: 'test-auth',
    ...options,
  });
  // We do not want any unexpected onUpdateNeeded calls in tests. If the test
  // needs to call onUpdateNeeded it should set this as needed.
  r.onUpdateNeeded = () => {
    throw new Error('Unexpected update needed');
  };

  // Keep track of all instances so we can close them in teardown.
  testZeroInstances.add(r as TestZero<MutatorDefs, QueryDefs>);
  return r;
}

export class TestLogSink implements LogSink {
  messages: [LogLevel, Context | undefined, unknown[]][] = [];
  flushCallCount = 0;

  log(level: LogLevel, context: Context | undefined, ...args: unknown[]): void {
    this.messages.push([level, context, args]);
  }

  flush() {
    this.flushCallCount++;
    return Promise.resolve();
  }
}

export async function waitForUpstreamMessage(
  r: TestZero<MutatorDefs, QueryDefs>,
  name: string,
  clock: SinonFakeTimers,
) {
  let gotMessage = false;
  (await r.socket).onUpstream = message => {
    const v = JSON.parse(message);
    const [kind] = upstreamSchema.parse(v);
    if (kind === name) {
      gotMessage = true;
    }
  };
  for (;;) {
    await clock.tickAsync(100);
    if (gotMessage) {
      break;
    }
  }
}
