import type {LogLevel} from '@rocicorp/logger';
import type {KVStoreProvider} from '../../../replicache/src/mod.js';
import type {MaybePromise} from '../../../shared/src/types.js';
import type {Schema} from '../../../zero-schema/src/mod.js';

/**
 * Configuration for [[Zero]].
 */
export interface ZeroOptions<S extends Schema> {
  /**
   * URL to the server. This can be a simple hostname, e.g.
   * - "https://myapp-myteam.zero.ms"
   * or a prefix with a single path component, e.g.
   * - "https://myapp-myteam.zero.ms/zero"
   * - "https://myapp-myteam.zero.ms/db"
   *
   * The latter is useful for configuring routing rules (e.g. "zero/**") when
   * the server is hosted on the same domain as the application.
   */
  server?: string | null | undefined;

  /**
   * A string token to identify and authenticate the user, a function that
   * returns such a token, or undefined if there is no logged in user.
   *
   * If the server determines the token is invalid (expired, can't be decoded,
   * bad signature, etc):
   * 1. if a function was provided Zero will call the function to get a new
   *    token with the error argument set to `'invalid-token'`.
   * 2. if a string token was provided Zero will continue to retry with the
   *    provided token.
   */
  auth?:
    | string
    | ((error?: 'invalid-token') => MaybePromise<string | undefined>)
    | undefined;

  /**
   * A unique identifier for the user. Must be non-empty.
   *
   * For efficiency, a new Zero instance will initialize its state from
   * the persisted state of an existing Zero instance with the same
   * `userID`, domain and browser profile.
   *
   * This must match the user identified by the `auth` token if
   * `auth` is provided.
   */
  userID: string;

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
   * `onOnlineChange` is called when the Zero instance's online status changes.
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
   * to sync with the server due to missing synchronization state.  This can be
   * because:
   * - the local persistent synchronization state has been garbage collected.
   *   This can happen if the client has no pending mutations and has not been
   *   used for a while (e.g. the client's tab has been hidden for a long time).
   * - the server fails to find the server side synchronization state for
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
   * Default is 5_000.
   */
  hiddenTabDisconnectDelay?: number | undefined;

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
  kvStore?: 'mem' | 'idb' | KVStoreProvider | undefined;

  /**
   * The maximum number of bytes to allow in a single header.
   *
   * Zero adds some extra information to headers on initialization if possible.
   * This speeds up data synchronization. This number should be kept less than
   * or equal to the maximum header size allowed by the server and any load
   * balancers.
   *
   * Default value: 8kb.
   */
  maxHeaderLength?: number | undefined;
}

export interface ZeroAdvancedOptions<S extends Schema> extends ZeroOptions<S> {
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
}

export type UpdateNeededReason =
  // There is a new client group due to a another tab loading new code which
  // cannot sync locally with this tab until it updates to the new code.
  // This tab can still sync with the server.
  | {type: 'NewClientGroup'}
  // This client was unable to connect to the server because it is using a
  // protocol version that the server does not support.
  | {type: 'VersionNotSupported'}
  // This client was unable to connect to the server because it is using a
  // schema version (see {@link Schema}) that the server does not support.
  | {type: 'SchemaVersionNotSupported'};
