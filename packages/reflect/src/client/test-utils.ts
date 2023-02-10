import type {MutatorDefs} from 'replicache';
import type {SinonFakeTimers} from 'sinon';
import type {ConnectedMessage} from '../protocol/connected';
import type {PongMessage} from '../protocol/pong';
import type {ReflectOptions} from './options';
import {Reflect} from './reflect.js';

export async function tickAFewTimes(clock: SinonFakeTimers, duration = 100) {
  const n = 10;
  const t = Math.ceil(duration / n);
  for (let i = 0; i < n; i++) {
    await clock.tickAsync(t);
  }
}

export class MockSocket extends EventTarget {
  args: unknown[] = [];
  messages: string[] = [];
  closed = false;

  constructor(...args: unknown[]) {
    super();
    this.args = args;
  }

  send(message: string) {
    this.messages.push(message);
  }

  close() {
    this.closed = true;
  }
}

export class TestReflect<MD extends MutatorDefs> extends Reflect<MD> {
  constructor(options: ReflectOptions<MD>) {
    super(options);
    // @ts-expect-error MockSocket is not compatible with WebSocket
    this._WSClass = MockSocket;
  }

  get connectionState() {
    return this._state;
  }

  get socket() {
    return this._socket;
  }

  triggerConnected() {
    const msg: ConnectedMessage = ['connected', {}];
    this._socket?.dispatchEvent(
      new MessageEvent('message', {data: JSON.stringify(msg)}),
    );
  }

  triggerPong() {
    const msg: PongMessage = ['pong', {}];
    this._socket?.dispatchEvent(
      new MessageEvent('message', {data: JSON.stringify(msg)}),
    );
  }

  triggerClose() {
    this._socket?.dispatchEvent(new CloseEvent('close'));
  }

  get pusher() {
    // @ts-expect-error Property '_pusher' is private
    return this._pusher;
  }

  get puller() {
    // @ts-expect-error Property '_puller' is private
    return this._puller;
  }

  async waitForSocket(clock: SinonFakeTimers): Promise<WebSocket> {
    for (let i = 0; i < 100; i++) {
      if (this._socket) {
        return this._socket;
      }
      await tickAFewTimes(clock);
    }
    throw new Error('Could not get socket');
  }
}

export const reflectForTest = <MD extends MutatorDefs>(
  options: Partial<ReflectOptions<MD>> = {},
): TestReflect<MD> => {
  const r = new TestReflect({
    socketOrigin: 'wss://example.com/',
    userID: 'test-user-id',
    roomID: 'test-room-id',
    auth: 'test-auth',
    ...options,
  });

  return r;
};
