import type {ReplicacheImpl} from '../../../replicache/src/replicache-impl.ts';
import type {ClientID} from '../../../replicache/src/sync/ids.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import {TDigest} from '../../../shared/src/tdigest.ts';
import {
  mapAST,
  normalizeAST,
  type AST,
} from '../../../zero-protocol/src/ast.ts';
import type {ChangeDesiredQueriesMessage} from '../../../zero-protocol/src/change-desired-queries.ts';
import type {UpQueriesPatchOp} from '../../../zero-protocol/src/queries-patch.ts';
import {
  hashOfAST,
  hashOfNameAndArgs,
} from '../../../zero-protocol/src/query-hash.ts';
import {
  clientToServer,
  type NameMapper,
} from '../../../zero-schema/src/name-mapper.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {MetricMap} from '../../../zql/src/query/metrics-delegate.ts';
import type {CustomQueryID} from '../../../zql/src/query/named.ts';
import type {GotCallback} from '../../../zql/src/query/query-delegate.ts';
import {clampTTL, compareTTL, type TTL} from '../../../zql/src/query/ttl.ts';
import type {InspectorMetricsDelegate} from './inspector/inspector.ts';
import {desiredQueriesPrefixForClient, GOT_QUERIES_KEY_PREFIX} from './keys.ts';
import type {MutationTracker} from './mutation-tracker.ts';
import type {ReadTransaction} from './replicache-types.ts';
import type {ZeroLogContext} from './zero-log-context.ts';

type QueryHash = string;

type Entry = {
  // May be undefined if the entry is a custom (named) query.
  // Will be removed when we no longer support ad-hoc queries that fall back to the server.
  normalized: AST | undefined;
  name: string | undefined;
  args: readonly ReadonlyJSONValue[] | undefined;
  count: number;
  gotCallbacks: GotCallback[];
  ttl: TTL;
};

type Metric = {
  [K in keyof MetricMap]: TDigest;
};

/**
 * Tracks what queries the client is currently subscribed to on the server.
 * Sends `changeDesiredQueries` message to server when this changes.
 * Deduplicates requests so that we only listen to a given unique query once.
 */
export class QueryManager implements InspectorMetricsDelegate {
  readonly #clientID: ClientID;
  readonly #clientToServer: NameMapper;
  readonly #send: (change: ChangeDesiredQueriesMessage) => void;
  readonly #queries: Map<QueryHash, Entry> = new Map();
  readonly #recentQueriesMaxSize: number;
  readonly #recentQueries: Set<string> = new Set();
  readonly #gotQueries: Set<string> = new Set();
  readonly #mutationTracker: MutationTracker;
  readonly #pendingQueryChanges: UpQueriesPatchOp[] = [];
  readonly #queryChangeThrottleMs: number;
  #pendingRemovals: Array<() => void> = [];
  #batchTimer: ReturnType<typeof setTimeout> | undefined;
  readonly #lc: ZeroLogContext;
  readonly #metrics: Metric = {
    'query-materialization-client': new TDigest(),
    'query-materialization-end-to-end': new TDigest(),
  };
  readonly #queryMetrics: Map<string, Metric> = new Map();
  readonly #slowMaterializeThreshold: number;

  constructor(
    lc: ZeroLogContext,
    mutationTracker: MutationTracker,
    clientID: ClientID,
    tables: Record<string, TableSchema>,
    send: (change: ChangeDesiredQueriesMessage) => void,
    experimentalWatch: ReplicacheImpl['experimentalWatch'],
    recentQueriesMaxSize: number,
    queryChangeThrottleMs: number,
    slowMaterializeThreshold: number,
  ) {
    this.#lc = lc.withContext('QueryManager');
    this.#clientID = clientID;
    this.#clientToServer = clientToServer(tables);
    this.#recentQueriesMaxSize = recentQueriesMaxSize;
    this.#send = send;
    this.#mutationTracker = mutationTracker;
    this.#queryChangeThrottleMs = queryChangeThrottleMs;
    this.#slowMaterializeThreshold = slowMaterializeThreshold;

    this.#mutationTracker.onAllMutationsApplied(() => {
      if (this.#pendingRemovals.length === 0) {
        return;
      }
      const pendingRemovals = this.#pendingRemovals;
      this.#pendingRemovals = [];
      for (const removal of pendingRemovals) {
        removal();
      }
    });

    experimentalWatch(
      diff => {
        for (const diffOp of diff) {
          const queryHash = diffOp.key.substring(GOT_QUERIES_KEY_PREFIX.length);
          switch (diffOp.op) {
            case 'add':
              this.#gotQueries.add(queryHash);
              this.#fireGotCallbacks(queryHash, true);
              break;
            case 'del':
              this.#gotQueries.delete(queryHash);
              this.#fireGotCallbacks(queryHash, false);
              break;
          }
        }
      },
      {
        prefix: GOT_QUERIES_KEY_PREFIX,
        initialValuesInFirstDiff: true,
      },
    );
  }

  #fireGotCallbacks(queryHash: string, got: boolean) {
    const gotCallbacks = this.#queries.get(queryHash)?.gotCallbacks ?? [];
    for (const gotCallback of gotCallbacks) {
      gotCallback(got);
    }
  }

  /**
   * Get the queries that need to be registered with the server.
   *
   * An optional `lastPatch` can be provided. This is the last patch that was
   * sent to the server and may not yet have been acked. If `lastPatch` is provided,
   * this method will return a patch that does not include any events sent in `lastPatch`.
   *
   * This diffing of last patch and current patch is needed since we send
   * a set of queries to the server when we first connect inside of the `sec-protocol` as
   * the `initConnectionMessage`.
   *
   * While we're waiting for the `connected` response to come back from the server,
   * the client may have registered more queries. We need to diff the `initConnectionMessage`
   * queries with the current set of queries to understand what those were.
   */
  async getQueriesPatch(
    tx: ReadTransaction,
    lastPatch?: Map<string, UpQueriesPatchOp> | undefined,
  ): Promise<Map<string, UpQueriesPatchOp>> {
    const existingQueryHashes = new Set<string>();
    const prefix = desiredQueriesPrefixForClient(this.#clientID);
    for await (const key of tx.scan({prefix}).keys()) {
      existingQueryHashes.add(key.substring(prefix.length, key.length));
    }
    const patch: Map<string, UpQueriesPatchOp> = new Map();
    for (const hash of existingQueryHashes) {
      if (!this.#queries.has(hash)) {
        patch.set(hash, {op: 'del', hash});
      }
    }

    for (const [hash, {normalized, ttl, name, args}] of this.#queries) {
      if (!existingQueryHashes.has(hash)) {
        patch.set(hash, {
          op: 'put',
          hash,
          ast: normalized,
          name,
          args,
          // We get TTL out of the DagStore so it is possible that the TTL was written
          // with a too high TTL.
          ttl: clampTTL(ttl), // no lc here since no need to log here
        });
      }
    }

    if (lastPatch) {
      // if there are any `puts` in `lastPatch` that are not in `patch` then we need to
      // send a `del` event in `patch`.
      for (const [hash, {op}] of lastPatch) {
        if (op === 'put' && !patch.has(hash)) {
          patch.set(hash, {op: 'del', hash});
        }
      }
      // Remove everything from `patch` that was already sent in `lastPatch`.
      for (const [hash, {op}] of patch) {
        const lastPatchOp = lastPatch.get(hash);
        if (lastPatchOp && lastPatchOp.op === op) {
          patch.delete(hash);
        }
      }
    }

    return patch;
  }

  addCustom(
    {name, args}: CustomQueryID,
    ttl: TTL,
    gotCallback?: GotCallback | undefined,
  ): () => void {
    const queryId = hashOfNameAndArgs(name, args);
    return this.#add(
      queryId,
      undefined, // normalized is undefined for custom queries
      name,
      args,
      ttl,
      gotCallback,
    );
  }

  addLegacy(
    ast: AST,
    ttl: TTL,
    gotCallback?: GotCallback | undefined,
  ): () => void {
    const normalized = normalizeAST(ast);
    const astHash = hashOfAST(normalized);
    return this.#add(
      astHash,
      normalized,
      undefined, // name is undefined for legacy queries
      undefined, // args are undefined for legacy queries
      ttl,
      gotCallback,
    );
  }

  #add(
    queryId: string,
    normalized: AST | undefined,
    name: string | undefined,
    args: readonly ReadonlyJSONValue[] | undefined,
    ttl: TTL,
    gotCallback?: GotCallback | undefined,
  ) {
    assert(
      (normalized === undefined && name !== undefined && args !== undefined) ||
        (normalized !== undefined && name === undefined && args === undefined),
      'Either normalized or name and args must be defined, but not both.',
    );
    ttl = clampTTL(ttl, this.#lc);
    let entry = this.#queries.get(queryId);
    this.#recentQueries.delete(queryId);
    if (!entry) {
      if (normalized !== undefined) {
        normalized = mapAST(normalized, this.#clientToServer);
      }
      entry = {
        normalized,
        name,
        args,
        count: 1,
        gotCallbacks: gotCallback ? [gotCallback] : [],
        ttl,
      };
      this.#queries.set(queryId, entry);
      this.#queueQueryChange({
        op: 'put',
        hash: queryId,
        ast: normalized,
        name,
        args,
        ttl,
      });
    } else {
      ++entry.count;
      this.#updateEntry(entry, queryId, ttl);

      if (gotCallback) {
        entry.gotCallbacks.push(gotCallback);
      }
    }

    if (gotCallback) {
      gotCallback(this.#gotQueries.has(queryId));
    }

    let removed = false;
    return () => {
      if (removed) {
        return;
      }
      removed = true;

      // We cannot remove queries while mutations are pending
      // as that could take data out of scope that is needed in a rebase
      if (this.#mutationTracker.size > 0) {
        this.#pendingRemovals.push(() =>
          this.#remove(entry, queryId, gotCallback),
        );
        return;
      }

      this.#remove(entry, queryId, gotCallback);
    };
  }

  updateCustom({name, args}: CustomQueryID, ttl: TTL) {
    const hash = hashOfNameAndArgs(name, args);
    const entry = must(this.#queries.get(hash));
    this.#updateEntry(entry, hash, ttl);
  }

  updateLegacy(ast: AST, ttl: TTL) {
    const normalized = normalizeAST(ast);
    const astHash = hashOfAST(normalized);
    const entry = must(this.#queries.get(astHash));
    this.#updateEntry(entry, astHash, ttl);
  }

  #updateEntry(entry: Entry, hash: string, ttl: TTL): void {
    // If the query already exists and the new ttl is larger than the old one
    // we send a changeDesiredQueries message to the server to update the ttl.
    ttl = clampTTL(ttl, this.#lc);
    if (compareTTL(ttl, entry.ttl) > 0) {
      entry.ttl = ttl;
      this.#queueQueryChange({
        op: 'put',
        hash,
        ast: entry.normalized,
        name: entry.name,
        args: entry.args,
        ttl,
      });
    }
  }

  #queueQueryChange(op: UpQueriesPatchOp) {
    this.#pendingQueryChanges.push(op);
    this.#scheduleBatch();
  }

  #scheduleBatch() {
    if (this.#batchTimer === undefined) {
      this.#batchTimer = setTimeout(
        () => this.flushBatch(),
        this.#queryChangeThrottleMs,
      );
    }
  }

  flushBatch() {
    if (this.#batchTimer !== undefined) {
      clearTimeout(this.#batchTimer);
      this.#batchTimer = undefined;
    }
    if (this.#pendingQueryChanges.length > 0) {
      this.#send([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [...this.#pendingQueryChanges],
        },
      ]);
      this.#pendingQueryChanges.length = 0;
    }
  }

  #remove(entry: Entry, astHash: string, gotCallback: GotCallback | undefined) {
    if (gotCallback) {
      const index = entry.gotCallbacks.indexOf(gotCallback);
      entry.gotCallbacks.splice(index, 1);
    }
    --entry.count;
    if (entry.count === 0) {
      this.#recentQueries.add(astHash);
      if (this.#recentQueries.size > this.#recentQueriesMaxSize) {
        const lruQueryID = this.#recentQueries.values().next().value;
        assert(lruQueryID);
        this.#queries.delete(lruQueryID);
        this.#recentQueries.delete(lruQueryID);
        this.#queryMetrics.delete(lruQueryID);
        this.#queueQueryChange({op: 'del', hash: lruQueryID});
      }
    }
  }

  /**
   * Gets the aggregated metrics for all queries managed by this QueryManager.
   */
  get metrics(): Metric {
    return this.#metrics;
  }

  addMetric<K extends keyof MetricMap>(
    metric: K,
    value: number,
    ...args: MetricMap[K]
  ): void {
    // We track all materializations of queries as well as per
    // query materializations.
    this.#metrics[metric].add(value);

    const queryID = args[0];

    // Handle slow query logging for end-to-end materialization
    if (metric === 'query-materialization-end-to-end') {
      const ast = args[1];

      if (
        this.#slowMaterializeThreshold !== undefined &&
        value > this.#slowMaterializeThreshold
      ) {
        this.#lc.warn?.(
          'Slow query materialization (including server/network)',
          queryID,
          ast,
          value,
        );
      } else {
        this.#lc.debug?.(
          'Materialized query (including server/network)',
          queryID,
          ast,
          value,
        );
      }
    }

    // The query manager manages metrics that are per query.
    let existing = this.#queryMetrics.get(queryID);
    if (!existing) {
      existing = {
        'query-materialization-client': new TDigest(),
        'query-materialization-end-to-end': new TDigest(),
      };
      this.#queryMetrics.set(queryID, existing);
    }
    existing[metric].add(value);
  }

  getQueryMetrics(queryID: string): Metric | undefined {
    return this.#queryMetrics.get(queryID);
  }
}
