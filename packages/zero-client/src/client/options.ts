import type {LogLevel} from '@rocicorp/logger';
import type {KVStoreProvider} from '../../../replicache/src/mod.js';
import type {MaybePromise} from '../../../shared/src/types.js';
import type {Schema} from './zero.js';

/**
 * Configuration for [[Zero]].
 */
export interface ZeroOptions<S extends Schema> {
  /**
   * Server to connect to, for example "https://myapp-myteam.zero.ms/".
   */
  server?: string | null | undefined;

  /**
   * Identifies and authenticates the user.
   *
   * This value is required when you provide a `authHandler` to your ReflectServer.
   * During connection this value is passed to your provided `authHandler`, which should use it to
   * authenticate the user. The `userID` returned by your `authHandler` for this value
   * must be equal to [[ReflectOptions.userID]].
   *
   * In the case authentication fails, the connection to the server will be
   * closed and Reflect will retry connecting with exponential backoff.
   *
   * If a function is provided here, that function is invoked before each
   * attempt. This provides the application the opportunity to calculate or
   * fetch a fresh token.
   */
  auth?: string | (() => MaybePromise<string>) | undefined;

  /**
   * A unique identifier for the user. Must be non-empty.
   *
   * For efficiency, a new Zero instance will initialize its state from
   * the persisted state of an existing Zero instance with the same
   * `userID`, `roomID`, domain and browser profile.
   */
  userID: string;

  /**
   * The server side data can be restricted to a jurisdiction. This is
   * useful for GDPR compliance.
   */
  jurisdiction?: 'eu' | undefined;

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
   * `onOnlineChange` is called when the Zero instance's online status changes
   */
  onOnlineChange?: ((online: boolean) => void) | undefined;

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
   * Help Zero improve its service by automatically sending diagnostic and
   * usage data.
   *
   * Default is true.
   */
  enableAnalytics?: boolean | undefined;

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
   * or equal to the maximum header size allowed by the server and any load balancers.
   *
   * Default value: 8kb.
   */
  maxHeaderLength?: number | undefined;
}

export interface ZeroOptionsInternal<S extends Schema> extends ZeroOptions<S> {
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
