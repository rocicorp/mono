/* eslint-disable @typescript-eslint/no-explicit-any */

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
    // noop
  }
  removeEventListener(): void {
    // noop
  }
  dispatchEvent(): boolean {
    return false;
  }

  close(): void {
    // noop
  }

  postMessage(): void {
    // noop
  }
}

const bc: typeof BroadcastChannel =
  typeof BroadcastChannel === 'undefined'
    ? NoopBroadcastChannel
    : BroadcastChannel;

export {bc as BroadcastChannel};
