import {resolver} from '@rocicorp/resolver';
import type {
  ConnectedMessage,
  Downstream,
  ErrorKind,
  ErrorMessage,
  PokeBody,
  PokeMessage,
  PongMessage,
  PullResponseBody,
  PullResponseMessage,
} from 'reflect-protocol';
import type {MutatorDefs} from 'replicache';
import type {SinonFakeTimers} from 'sinon';
import {assert} from '../util/asserts.js';
import type {ReflectOptions} from './options.js';
import {ConnectionState, Reflect} from './reflect.js';

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

  constructor(url: string | URL, protocol = '') {
    super();
    this.url = url;
    this.protocol = protocol;
  }

  send(message: string) {
    this.messages.push(message);
  }

  close() {
    this.closed = true;
    this.dispatchEvent(new CloseEvent('close'));
  }
}

export class TestReflect<MD extends MutatorDefs> extends Reflect<MD> {
  #connectionStateResolvers: Set<{
    state: ConnectionState;
    resolve: (state: ConnectionState) => void;
  }> = new Set();

  get connectionState() {
    return this._connectionState;
  }

  get connectionStateAsString(): string {
    switch (this._connectionState) {
      case ConnectionState.Disconnected:
        return 'Disconnected';
      case ConnectionState.Connecting:
        return 'Connecting';
      case ConnectionState.Connected:
        return 'Connected';
    }
  }

  get connectingStart() {
    return this._connectingStart;
  }

  protected get _connectionState(): ConnectionState {
    return super._connectionState;
  }
  protected set _connectionState(newState: ConnectionState) {
    super._connectionState = newState;
    for (const entry of this.#connectionStateResolvers) {
      const {state, resolve} = entry;
      if (state === newState) {
        this.#connectionStateResolvers.delete(entry);
        resolve(newState);
      }
    }
  }

  waitForConnectionState(state: ConnectionState) {
    if (this._connectionState === state) {
      return Promise.resolve(state);
    }
    const {promise, resolve} = resolver<ConnectionState>();
    this.#connectionStateResolvers.add({state, resolve});
    return promise;
  }

  get socket(): Promise<MockSocket> {
    return this._socketResolver
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

  get pusher() {
    // @ts-expect-error Property '_pusher' is private
    return this._pusher;
  }

  get puller() {
    // @ts-expect-error Property '_puller' is private
    return this._puller;
  }
}

export function reflectForTest<MD extends MutatorDefs>(
  options: Partial<ReflectOptions<MD>> = {},
): TestReflect<MD> {
  return new TestReflect({
    socketOrigin: 'wss://example.com/',
    userID: 'test-user-id',
    roomID: 'test-room-id',
    auth: 'test-auth',
    logSinks: [],
    ...options,
  });
}
