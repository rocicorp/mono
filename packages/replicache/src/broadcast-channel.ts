class NoopBroadcastChannel extends EventTarget implements BroadcastChannel {
  readonly name: string;

  onmessage:
    | ((this: BroadcastChannel, ev: MessageEvent<unknown>) => unknown)
    | null = null;

  onmessageerror:
    | ((this: BroadcastChannel, ev: MessageEvent<unknown>) => unknown)
    | null = null;

  constructor(name: string) {
    super();
    this.name = name;
  }

  close(): void {
    // noop
  }

  postMessage(_message: unknown): void {
    // noop
  }
}

const bc: typeof BroadcastChannel =
  typeof BroadcastChannel === 'undefined'
    ? NoopBroadcastChannel
    : BroadcastChannel;

export {bc as BroadcastChannel};
