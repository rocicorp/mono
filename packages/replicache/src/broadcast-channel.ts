/* eslint-disable @typescript-eslint/no-explicit-any */

import {notImplemented} from 'shared/src/asserts.js';

class NoopBroadcastChannel implements BroadcastChannel {
  readonly name: string;

  onmessage: ((this: BroadcastChannel, ev: MessageEvent<any>) => any) | null =
    null;

  onmessageerror:
    | ((this: BroadcastChannel, ev: MessageEvent<any>) => any)
    | null = null;

  constructor(name: string) {
    this.name = name;
  }

  addEventListener(): void {
    notImplemented();
  }
  removeEventListener(): void {
    notImplemented();
  }
  dispatchEvent(): boolean {
    notImplemented();
  }

  close(): void {
    // noop
  }

  postMessage(_message: any): void {
    // noop
  }
}

const bc: typeof BroadcastChannel =
  typeof BroadcastChannel === 'undefined'
    ? NoopBroadcastChannel
    : BroadcastChannel;

export {bc as BroadcastChannel};
