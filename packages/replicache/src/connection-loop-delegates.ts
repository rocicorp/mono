import type {ConnectionLoopDelegate} from './connection-loop.js';
import type {Replicache} from './replicache.js';

class ConnectionLoopDelegateImpl {
  readonly rep: Replicache;
  readonly invokeSend: () => Promise<boolean>;

  // TODO: Remove the ability to have more than one concurrent connection and update tests.
  // Bug: https://github.com/rocicorp/replicache-internal/issues/303
  readonly maxConnections = 1;

  constructor(rep: Replicache, invokeSend: () => Promise<boolean>) {
    this.rep = rep;
    this.invokeSend = invokeSend;
  }

  get maxDelayMs(): number {
    return this.rep.requestOptions.maxDelayMs;
  }

  get minDelayMs(): number {
    return this.rep.requestOptions.minDelayMs;
  }
}

export class PullDelegate
  extends ConnectionLoopDelegateImpl
  implements ConnectionLoopDelegate
{
  readonly debounceDelay = 0;

  get watchdogTimer(): number | null {
    return this.rep.pullInterval;
  }
}

export class PushDelegate
  extends ConnectionLoopDelegateImpl
  implements ConnectionLoopDelegate
{
  get debounceDelay(): number {
    return this.rep.pushDelay;
  }

  watchdogTimer = null;
}
