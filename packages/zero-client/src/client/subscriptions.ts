import type {LogContext} from '@rocicorp/logger';
import type {Materialite} from '@rocicorp/zql/src/zql/ivm/materialite.js';
import type {Diff} from 'replicache/src/btree/node.js';
import {
  SubscriptionsManagerImpl,
  WatchSubscription,
  type Subscription,
  type WatchCallback,
} from 'replicache/src/subscriptions.js';
import type {QueryInternal} from 'replicache/src/types.js';

type UnknownSubscription = Subscription<unknown>;

export class ZQLSubscriptionsManager extends SubscriptionsManagerImpl {
  readonly #materialite: Materialite;

  constructor(
    materialite: Materialite,
    queryInternal: QueryInternal,
    lc: LogContext,
  ) {
    super(queryInternal, lc);
    this.#materialite = materialite;
  }

  callCallbacks(
    subs: readonly UnknownSubscription[],
    results: PromiseSettledResult<unknown>[],
  ): void {
    this.#materialite.tx(() => {
      super.callCallbacks(subs, results);
    });
  }
}

export class ZQLWatchSubscription
  extends WatchSubscription
  implements Subscription<Diff | undefined>
{
  constructor(name: string, callback: WatchCallback) {
    super(callback, {prefix: name + '/', initialValuesInFirstDiff: true});
  }
}
