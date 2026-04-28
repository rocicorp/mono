import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {Subscription} from '../../../types/subscription.ts';
import {
  getSubscriptionState,
  type SubscriptionState,
} from '../../replicator/schema/replication-state.ts';
import type {ChangeSource, ChangeStream} from '../change-source.ts';
import type {
  ChangeSourceUpstream,
  ChangeStreamMessage,
} from '../protocol/current.ts';

/**
 * Initializes a "static" change source that reads watermark/publication
 * metadata from a pre-existing replica file and never emits any changes.
 *
 * This is used for `ZERO_UPSTREAM_TYPE=static` — a read-only debugging mode
 * where zero-cache serves queries from an existing replica file without
 * connecting to upstream Postgres or performing initial sync.
 */
export function initializeStaticChangeSource(
  lc: LogContext,
  replicaDbFile: string,
): {subscriptionState: SubscriptionState; changeSource: ChangeSource} {
  const replica = new Database(lc, replicaDbFile, {readonly: true});
  let subscriptionState: SubscriptionState;
  try {
    subscriptionState = getSubscriptionState(new StatementRunner(replica));
  } finally {
    replica.close();
  }
  lc.info?.(
    `static change-source: opened ${replicaDbFile} at watermark ` +
      `${subscriptionState.watermark} (replicaVersion ` +
      `${subscriptionState.replicaVersion})`,
  );
  return {subscriptionState, changeSource: new StaticChangeSource(lc)};
}

class StaticChangeSource implements ChangeSource {
  readonly #lc: LogContext;

  constructor(lc: LogContext) {
    this.#lc = lc.withContext('component', 'static-change-source');
  }

  startLagReporter() {
    return null;
  }

  startStream(): Promise<ChangeStream> {
    this.#lc.debug?.('starting no-op change stream');
    const changes = Subscription.create<ChangeStreamMessage>();
    const acks = Subscription.create<ChangeSourceUpstream>();
    return Promise.resolve({changes, acks});
  }

  stop(): Promise<void> {
    return Promise.resolve();
  }
}
