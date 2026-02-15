import type {LogLevel} from '@rocicorp/logger';
import type {StoreProvider} from '../../../replicache/src/kv/store.ts';
import * as v from '../../../shared/src/valita.ts';
import type {
  DefaultContext,
  DefaultSchema,
} from '../../../zero-types/src/default-types.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {AnyMutatorRegistry} from '../../../zql/src/mutate/mutator-registry.ts';
import type {CustomMutatorDefs} from './custom.ts';
import {UpdateNeededReasonType} from './update-needed-reason-type.ts';

/**
 * Event fired when a query has fully materialized, including server and
 * network round-trip time.
 */
export type QueryMaterializeEvent = {
  /** The query hash identifier. */
  id: string;
  /** The normalized AST of the query. */
  ast: AST;
  /** Duration of the full materialization in milliseconds. */
  durationMs: number;
  /** The name of the query, if it is a named (custom) query. */
  name: string | undefined;
};

/**
 * Hooks for observing query lifecycle events. All hooks are optional and
 * wrapped in try/catch internally, so errors in hooks never affect query
 * execution.
 *
 * @example
 * ```ts
 * const z = new Zero({
 *   // ...
 *   hooks: {
 *     onQueryMaterialize({id, ast, durationMs, name}) {
 *       const tracer = trace.getTracer('zero-client');
 *       const endTime = Date.now();
 *       const startTime = endTime - durationMs;
 *       tracer.startSpan('zero.query.materialize', {
 *         startTime,
 *         attributes: {
 *           'zero.query.id': id,
 *           'zero.query.table': ast.table,
 *           ...(name && {'zero.query.name': name}),
 *           'zero.query.duration_ms': durationMs,
 *         },
 *       }).end(endTime);
 *     },
 *   },
 * });
 * ```
 */
export type ZeroQueryHooks = {
  /**
   * Called when end-to-end query materialization completes, including
   * server processing and network round-trip time.
   */
  onQueryMaterialize?: (event: QueryMaterializeEvent) => void;
};

/**
 * Configuration for {@linkcode Zero}.
 */
export interface ZeroOptions<
  S extends Schema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  C = DefaultContext,
> {
  /**
   * URL to the zero-cache. This can be a simple hostname, e.g.
   * - "https://myapp-myteam.zero.ms"
   * or a prefix with a single path component, e.g.
   * - "https://myapp-myteam.zero.ms/zero"
   * - "https://myapp-myteam.zero.ms/db"
   *
   * The latter is useful for configuring routing rules (e.g. "/zero/\*") when
   * the zero-cache is hosted on the same domain as the application. **Note that
   * only a single path segment is allowed (e.g. it cannot be "/proxy/zero/\*")**.
   */
  cacheURL?: string | null | undefined;

  /**
   * @deprecated Use {@linkcode cacheURL} instead.
   */
  server?: string | null | undefined;

  /**
   * A token to identify and authenticate the user.
   *
   * Set `auth` to `null` or `undefined` if there is no logged in user.
   *
   * The call to `connect` is handled automatically by the ZeroProvider component
   * for React and SolidJS when the `auth` prop changes.
   *
   * When `auth` changes while connected, Zero refreshes server-side auth context
   * and re-transforms queries without reconnecting.
   *
   * When a 401 or 403 HTTP status code is received from your server, Zero will
   * transition to the `needs-auth` connection state. The app should call
   * `zero.connection.connect({auth: newToken})` with a new token to reconnect.
   */
  auth?: string | null | undefined;

  /**
   * A unique identifier for the user. Must be non-empty.
   *
   * Each userID gets its own client-side storage so that the app can switch
   * between users without losing state.
   *
   * This must match the `sub` claim of the `auth` token if
   * `auth` is provided.
   */
  userID: string;

  /**
   * Distinguishes the storage used by this Zero instance from that of other
   * instances with the same userID. Useful in the case where the app wants to
   * have multiple Zero instances for the same user for different parts of the
   * app.
   */
  storageKey?: string | undefined;

  /**
   * Determines the level of detail at which Zero logs messages about
   * its operation. Messages are logged to the `console`.
   *
   * When this is set to `'debug'`, `'info'` and `'error'` messages are also
   * logged. When set to `'info'`, `'info'` and `'error'` but not
   * `'debug'` messages are logged. When set to `'error'` only `'error'`
   * messages are logged.
   *
   * Default is `'error'`.
   */
  logLevel?: LogLevel | undefined;

  /**
   * This defines the schema of the tables used in Zero and their relationships
   * to one another.
   */
  schema: S;

  /**
   * `mutators` is a map of custom mutator definitions. The keys are
   * namespaces or names of the mutators. The values are the mutator
   * implementations. Client side mutators must be idempotent as a
   * mutation can be rebased multiple times when folding in authoritative
   * changes from the server to the client.
   *
   * Define mutators using the `defineMutator` function to create type-safe,
   * parameterized mutations. Mutators can be top-level or grouped in namespaces.
   *
   * @example
   * ```ts
   * import {defineMutator} from '@rocicorp/zero';
   *
   * const z = new Zero({
   *   schema,
   *   userID,
   *   mutators: {
   *     // Top-level mutator
   *     increment: defineMutator(({tx, args}: {tx: Transaction<Schema>, args: {id: string}}) =>
   *       tx.mutate.counter.update({id: args.id, value: tx.query.counter.where('id', '=', args.id).value + 1})
   *     ),
   *     // Namespace with multiple mutators
   *     issues: {
   *       create: defineMutator(({tx, args}: {tx: Transaction<Schema>, args: {title: string}}) =>
   *         tx.mutate.issues.insert({id: nanoid(), title: args.title, status: 'open'})
   *       ),
   *       close: defineMutator(({tx, args}: {tx: Transaction<Schema>, args: {id: string}}) =>
   *         tx.mutate.issues.update({id: args.id, status: 'closed'})
   *       ),
   *     },
   *   },
   * });
   *
   * // Usage
   * await z.mutate.increment({id: 'counter-1'}).client;
   * await z.mutate.issues.create({title: 'New issue'}).client;
   * await z.mutate.issues.close({id: 'issue-123'}).client;
   * ```
   */
  mutators?: MD extends CustomMutatorDefs ? MD : AnyMutatorRegistry | undefined;

  /**
   * Custom URL for mutation requests sent to your API server.
   * If not provided, uses the default configured in zero-cache.
   */
  mutateURL?: string | undefined;

  /**
   * Custom headers to include in mutation requests sent to your API server.
   * These headers are passed through zero-cache to the mutate endpoint.
   */
  mutateHeaders?: Record<string, string> | undefined;

  /**
   * Custom URL for query requests sent to your API server.
   * If not provided, uses the default configured in zero-cache.
   *
   * @deprecated Use {@linkcode queryURL} instead.
   */
  getQueriesURL?: string | undefined;

  /**
   * Custom URL for query requests sent to your API server.
   * If not provided, uses the default configured in zero-cache.
   */
  queryURL?: string | undefined;

  /**
   * Custom headers to include in query requests sent to your API server.
   * These headers are passed through zero-cache to the query endpoint.
   */
  queryHeaders?: Record<string, string> | undefined;

  /**
   * `onOnlineChange` is called when the Zero instance's online status changes.
   *
   * @deprecated Use {@linkcode Connection.state.subscribe} on the Zero instance instead. e.g.
   * ```ts
   * const zero = new Zero({...});
   * zero.connection.state.subscribe((state) => {
   *   console.log('Connection state:', state.name);
   * });
   * ```
   *
   * Or use a hook like {@linkcode useConnectionState} to subscribe to state changes.
   */
  onOnlineChange?: ((online: boolean) => void) | undefined;

  /**
   * `onUpdateNeeded` is called when a client code update is needed.
   *
   * See {@link UpdateNeededReason} for why updates can be needed.
   *
   * The default behavior is to reload the page (using `location.reload()`).
   * Provide your own function to prevent the page from
   * reloading automatically. You may want to display a toast to inform the end
   * user there is a new version of your app available and prompt them to
   * refresh.
   */
  onUpdateNeeded?: ((reason: UpdateNeededReason) => void) | undefined;

  /**
   * `onClientStateNotFound` is called when this client is no longer able
   * to sync with the zero-cache due to missing synchronization state.  This
   * can be because:
   * - the local persistent synchronization state has been garbage collected.
   *   This can happen if the client has no pending mutations and has not been
   *   used for a while (e.g. the client's tab has been hidden for a long time).
   * - the zero-cache fails to find the server side synchronization state for
   *   this client.
   *
   * The default behavior is to reload the page (using `location.reload()`).
   * Provide your own function to prevent the page from reloading automatically.
   */
  onClientStateNotFound?: (() => void) | undefined;

  /**
   * The number of milliseconds to wait before disconnecting a Zero
   * instance whose tab has become hidden.
   *
   * Instances in hidden tabs are disconnected to save resources.
   *
   * Default is 5 minutes.
   */
  hiddenTabDisconnectDelay?: number | undefined;

  /**
   * The number of milliseconds to wait before disconnecting a Zero
   * instance when the connection to the server has timed out.
   *
   * Default is 1 minute.
   */
  disconnectTimeoutMs?: number | undefined;

  /**
   * The timeout in milliseconds for ping operations. This value is used for:
   * - How long to wait in idle before sending a ping to the server
   * - How long to wait for a pong response after sending a ping
   *
   * Total time to detect a dead connection is 2 × pingTimeoutMs.
   *
   * Default is 5_000.
   */
  pingTimeoutMs?: number | undefined;

  /**
   * Determines what kind of storage implementation to use on the client.
   *
   * Defaults to `'idb'` which means that Zero uses an IndexedDB storage
   * implementation. This allows the data to be persisted on the client and
   * enables faster syncs between application restarts.
   *
   * By setting this to `'mem'`, Zero uses an in memory storage and
   * the data is not persisted on the client.
   *
   * You can also set this to a function that is used to create new KV stores,
   * allowing a custom implementation of the underlying storage layer.
   */
  kvStore?: 'mem' | 'idb' | StoreProvider | undefined;

  /**
   * The maximum number of bytes to allow in a single header.
   *
   * Zero adds some extra information to headers on initialization if possible.
   * This speeds up data synchronization. This number should be kept less than
   * or equal to the maximum header size allowed by the zero-cache and any load
   * balancers.
   *
   * Default value: 8kb.
   */
  maxHeaderLength?: number | undefined;

  /**
   * The maximum amount of milliseconds to wait for a materialization to
   * complete (including network/server time) before printing a warning to the
   * console.
   *
   * Default value: 5_000.
   */
  slowMaterializeThreshold?: number | undefined;

  /**
   * UI rendering libraries will often provide a utility for batching multiple
   * state updates into a single render. Some examples are React's
   * `unstable_batchedUpdates`, and solid-js's `batch`.
   *
   * This option enables integrating these batch utilities with Zero.
   *
   * When `batchViewUpdates` is provided, Zero will call it whenever
   * it updates query view state with an `applyViewUpdates` function
   * that performs the actual state updates.
   *
   * Zero updates query view state when:
   * 1. creating a new view
   * 2. updating all existing queries' views to a new consistent state
   *
   * When creating a new view, that single view's creation will be wrapped
   * in a `batchViewUpdates` call.
   *
   * When updating existing queries, all queries will be updated in a single
   * `batchViewUpdates` call, so that the transition to the new consistent
   * state can be done in a single render.
   *
   * Implementations must always call `applyViewUpdates` synchronously.
   */
  batchViewUpdates?: ((applyViewUpdates: () => void) => void) | undefined;

  /**
   * The maximum number of recent queries, no longer subscribed to by a preload
   * or view, to continue syncing.
   *
   * Defaults is 0.
   *
   * @deprecated Use ttl instead
   */
  maxRecentQueries?: number | undefined;

  /**
   * Changes to queries are sent to server in batches. This option controls
   * the number of milliseconds to wait before sending the next batch.
   *
   * Defaults is 10.
   */
  queryChangeThrottleMs?: number | undefined;

  /**
   * Context is passed to Synced Queries when they are executed
   */
  // TODO(arv): Mutators should also get context.
  context?: C | undefined;

  /**
   * Optional hooks for observing query lifecycle events. Useful for
   * instrumenting query performance with OpenTelemetry or other telemetry
   * systems.
   *
   * Hook invocations are wrapped in try/catch internally, so errors in
   * hooks never affect query execution.
   *
   * @see {@link ZeroQueryHooks}
   */
  hooks?: ZeroQueryHooks | undefined;
}

/**
 * @deprecated Use {@link ZeroOptions} instead.
 */
export interface ZeroAdvancedOptions<
  S extends Schema,
  MD extends CustomMutatorDefs | undefined,
  Context,
> extends ZeroOptions<S, MD, Context> {}

type UpdateNeededReasonBase = {
  message?: string;
};

export type UpdateNeededReason =
  | ({type: UpdateNeededReasonType.NewClientGroup} & UpdateNeededReasonBase)
  | ({
      type: UpdateNeededReasonType.VersionNotSupported;
    } & UpdateNeededReasonBase)
  | ({
      type: UpdateNeededReasonType.SchemaVersionNotSupported;
    } & UpdateNeededReasonBase);

export const updateNeededReasonTypeSchema: v.Type<UpdateNeededReason['type']> =
  v.literalUnion(
    UpdateNeededReasonType.NewClientGroup,
    UpdateNeededReasonType.VersionNotSupported,
    UpdateNeededReasonType.SchemaVersionNotSupported,
  );
