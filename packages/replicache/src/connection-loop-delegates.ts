import type {ConnectionLoopDelegate} from './connection-loop.js';
import type {RequestOptions} from './types.js';

export type ConnectionLoopDelegateOptions = {
  requestOptions: Required<RequestOptions>;
  pullInterval: number | null;
  pushDelay: number;
};

class ConnectionLoopDelegateImpl {
  readonly options: ConnectionLoopDelegateOptions;
  readonly invokeSend: () => Promise<boolean>;

  // TODO: Remove the ability to have more than one concurrent connection and update tests.
  // Bug: https://github.com/rocicorp/replicache-internal/issues/303
  readonly maxConnections = 1;

  constructor(
    rep: ConnectionLoopDelegateOptions,
    invokeSend: () => Promise<boolean>,
  ) {
    this.options = rep;
    this.invokeSend = invokeSend;
  }

  get maxDelayMs(): number {
    return this.options.requestOptions.maxDelayMs;
  }

  get minDelayMs(): number {
    return this.options.requestOptions.minDelayMs;
  }
}

export class PullDelegate
  extends ConnectionLoopDelegateImpl
  implements ConnectionLoopDelegate
{
  readonly debounceDelay = 0;

  get watchdogTimer(): number | null {
    return this.options.pullInterval;
  }
}

export class PushDelegate
  extends ConnectionLoopDelegateImpl
  implements ConnectionLoopDelegate
{
  get debounceDelay(): number {
    return this.options.pushDelay;
  }

  watchdogTimer = null;
}
