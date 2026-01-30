import type {LogContext} from '@rocicorp/logger';
import type {ReplicacheImpl} from '../../../replicache/src/replicache-impl.ts';
import type {ClientID} from '../../../replicache/src/sync/ids.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import {difference} from '../../../shared/src/set-utils.ts';
import {TDigest} from '../../../shared/src/tdigest.ts';
import {
  mapAST,
  normalizeAST,
  type AST,
} from '../../../zero-protocol/src/ast.ts';
import type {ChangeDesiredQueriesMessage} from '../../../zero-protocol/src/change-desired-queries.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import {ProtocolError} from '../../../zero-protocol/src/error.ts';
import type {UpQueriesPatchOp} from '../../../zero-protocol/src/queries-patch.ts';
import {
  hashOfAST,
  hashOfNameAndArgs,
} from '../../../zero-protocol/src/query-hash.ts';
import {
  clientToServer,
  serverToClient,
  type NameMapper,
} from '../../../zero-schema/src/name-mapper.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {ClientMetricMap} from '../../../zql/src/query/metrics-delegate.ts';
import type {CustomQueryID} from '../../../zql/src/query/named.ts';
import type {GotCallback} from '../../../zql/src/query/query-delegate.ts';
import {clampTTL, compareTTL, type TTL} from '../../../zql/src/query/ttl.ts';
import type {ClientErrorKind} from './client-error-kind.ts';
import type {ClientError} from './error.ts';
import {type ZeroError} from './error.ts';
import type {InspectorDelegate} from './inspector/inspector.ts';
import type {ZeroQueryHooks} from './options.ts';
import {desiredQueriesPrefixForClient, GOT_QUERIES_KEY_PREFIX} from './keys.ts';
import type {MutationTracker} from './mutation-tracker.ts';
import type {ReadTransaction} from './replicache-types.ts';

type QueryHash = string;

type Entry = {
  // We keep track of the AST so we can use it in the inspector.
  normalized: AST;
  name: string | undefined;
  args: readonly ReadonlyJSONValue[] | undefined;
  count: number;
  gotCallbacks: GotCallback[];
  ttl: TTL;
};

type ClientMetric = {
  [K in keyof ClientMetricMap]: TDigest;
};

/**
 * Tracks what queries the client is currently subscribed to on the server.
 * Sends `changeDesiredQueries` message to server when this changes.
 * Deduplicates requests so that we only listen to a given unique query once.
 */
export class QueryManager implements InspectorDelegate {
  readonly #clientID: ClientID;
  readonly #clientToServer: NameMapper;
  readonly #serverToClient: NameMapper;
  readonly #send: (change: ChangeDesiredQueriesMessage) => void;
  readonly #onFatalError: (error: ZeroError) => void;
  readonly #queries: Map<QueryHash, Entry> = new Map();
  readonly #recentQueriesMaxSize: number;
  readonly #recentQueries: Set<string> = new Set();
  readonly #gotQueries: Set<string> = new Set();
  readonly #mutationTracker: MutationTracker;
  readonly #pendingQueryChanges: UpQueriesPatchOp[] = [];
  readonly #queryChangeThrottleMs: number;
  #pendingRemovals: Array<() => void> = [];
  #batchTimer: ReturnType<typeof setTimeout> | undefined;
  readonly #lc: LogContext;
  readonly #metrics: ClientMetric = newMetrics();
  readonly #queryMetrics: Map<string, ClientMetric> = new Map();
  readonly #slowMaterializeThreshold: number;
  readonly #hooks: ZeroQueryHooks | undefined;
  #closedError: ZeroError | undefined;

  constructor(
    lc: LogContext,
    mutationTracker: MutationTracker,
    clientID: ClientID,
    tables: Record<string, TableSchema>,
    send: (change: ChangeDesiredQueriesMessage) => void,
    experimentalWatch: ReplicacheImpl['experimentalWatch'],
    recentQueriesMaxSize: number,
    queryChangeThrottleMs: number,
    slowMaterializeThreshold: number,
    onFatalError: (error: ZeroError) => void,
    hooks?: ZeroQueryHooks | undefined,
  ) {
    this.#lc = lc.withContext('QueryManager');
    this.#clientID = clientID;
    this.#clientToServer = clientToServer(tables);
    this.#serverToClient = serverToClient(tables);
    this.#recentQueriesMaxSize = recentQueriesMaxSize;
    this.#send = send;
    this.#mutationTracker = mutationTracker;
    this.#queryChangeThrottleMs = queryChangeThrottleMs;
    this.#slowMaterializeThreshold = slowMaterializeThreshold;
    this.#onFatalError = onFatalError;
    this.#hooks = hooks;
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

  getAST(queryID: string): AST | undefined {
    const ast = this.#queries.get(queryID)?.normalized;
    return ast && mapAST(ast, this.#serverToClient);
  }

  mapClientASTToServer(ast: AST): AST {
    return mapAST(ast, this.#clientToServer);
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
    lastPatch?: Map<string, UpQueriesPatchOp>,
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
          ast: name === undefined ? normalized : undefined,
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

  handleTransformErrors(errors: ErroredQuery[]) {
    for (const error of errors) {
      const queryId = error.id;
      const entry = this.#queries.get(queryId);

      // if we don't have the query registered, continue
      if (!entry) {
        continue;
      }

      if (error.error === 'app' || error.error === 'parse') {
        entry.gotCallbacks.forEach(callback => callback(false, error));
      }
      // this code path is not possible technically since errors were never implemented in the legacy query transform error
      // but is included for backwards compatibility and we have a test case for it
      else {
        this.#onFatalError(
          new ProtocolError({
            kind: ErrorKind.TransformFailed,
            origin: ErrorOrigin.ZeroCache,
            reason: ErrorReason.Internal,
            message: `Unknown error transforming queries: ${JSON.stringify(error)}`,
            queryIDs: [],
          }),
        );
        // unreachable(error); TODO(0xcadams): this should eventually be unreachable
      }
    }
  }

  handleClosed(
    reason: ClientError<{kind: ClientErrorKind.ClientClosed; message: string}>,
  ) {
    if (this.#closedError) {
      return;
    }
    this.#closedError = reason;
    for (const [queryId, entry] of this.#queries) {
      const erroredQuery: ErroredQuery = {
        error: 'app',
        id: queryId,
        name: entry.name ?? 'legacy',
        message: reason.message,
        details: {kind: reason.kind},
      };
      for (const gotCallback of entry.gotCallbacks) {
        gotCallback(false, erroredQuery);
      }
    }
  }

  addCustom(
    ast: AST,
    {name, args}: CustomQueryID,
    ttl: TTL,
    gotCallback?: GotCallback,
  ): () => void {
    const normalized = normalizeAST(ast);
    const queryId = hashOfNameAndArgs(name, args);
    return this.#add(queryId, normalized, name, args, ttl, gotCallback);
  }

  addLegacy(ast: AST, ttl: TTL, gotCallback?: GotCallback): () => void {
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
    normalized: AST,
    name: string | undefined,
    args: readonly ReadonlyJSONValue[] | undefined,
    ttl: TTL,
    gotCallback?: GotCallback,
  ) {
    assert(
      (name === undefined) === (args === undefined),
      'If name is defined, args must be defined',
    );
    ttl = clampTTL(ttl, this.#lc);
    let entry = this.#queries.get(queryId);
    this.#recentQueries.delete(queryId);
    if (!entry) {
      normalized = mapAST(normalized, this.#clientToServer);

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
        ast: name === undefined ? normalized : undefined,
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
    const cleanupCb = () => {
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
    return cleanupCb;
  }

  updateCustom({name, args}: CustomQueryID, ttl: TTL) {
    const queryID = hashOfNameAndArgs(name, args);
    const entry = must(this.#queries.get(queryID));
    this.#updateEntry(entry, queryID, ttl);
  }

  updateLegacy(ast: AST, ttl: TTL) {
    const normalized = normalizeAST(ast);
    const queryID = hashOfAST(normalized);
    const entry = must(this.#queries.get(queryID));
    this.#updateEntry(entry, queryID, ttl);
  }

  #updateEntry(entry: Entry, queryID: string, ttl: TTL): void {
    // If the query already exists and the new ttl is larger than the old one
    // we send a changeDesiredQueries message to the server to update the ttl.
    ttl = clampTTL(ttl, this.#lc);
    if (compareTTL(ttl, entry.ttl) > 0) {
      entry.ttl = ttl;
      this.#queueQueryChange({
        op: 'put',
        hash: queryID,
        ast: entry.name === undefined ? entry.normalized : undefined,
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
  get metrics(): ClientMetric {
    return this.#metrics;
  }

  addMetric<K extends keyof ClientMetricMap>(
    metric: K,
    value: number,
    ...args: ClientMetricMap[K]
  ): void {
    // Only query metrics are tracked at this point.
    // If this check fails then we need to add a runtime check.
    metric satisfies `query-${string}`;

    // We track all materializations of queries as well as per
    // query materializations.
    this.#metrics[metric].add(value);

    const queryID = args[0];

    if (metric === 'query-materialization-client') {
      try {
        this.#hooks?.onQueryMaterializeClient?.({id: queryID});
      } catch {
        // Consumer hook errors must not break query execution.
      }
    }

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

      try {
        if (ast) {
          const entry = this.#queries.get(queryID);
          this.#hooks?.onQueryMaterialize?.({
            id: queryID,
            ast,
            durationMs: value,
            name: entry?.name,
          });
        }
      } catch {
        // Consumer hook errors must not break query execution.
      }
    }

    // The query manager manages metrics that are per query.
    let existing = this.#queryMetrics.get(queryID);
    if (!existing) {
      existing = newMetrics();
      this.#queryMetrics.set(queryID, existing);
    }
    existing[metric].add(value);
  }

  getQueryMetrics(queryID: string): ClientMetric | undefined {
    return this.#queryMetrics.get(queryID);
  }

  /**
   * For testing only: returns all query hashes currently registered.
   */
  getAllNonGotQueryHashes(): Iterable<string> {
    return difference(this.#queries, this.#gotQueries);
  }
}

function newMetrics(): ClientMetric {
  return {
    'query-materialization-client': new TDigest(),
    'query-materialization-end-to-end': new TDigest(),
    'query-update-client': new TDigest(),
  };
}
