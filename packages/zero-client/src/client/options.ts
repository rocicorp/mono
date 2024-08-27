import type {LogLevel} from '@rocicorp/logger';
import type {KVStoreProvider} from 'replicache';
import type {MaybePromise} from 'shared/src/types.js';
import type {SchemaDefs} from './zero.js';

/**
 * Configuration for [[Zero]].
 */
export interface ZeroOptions<QD extends SchemaDefs> {
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
   * The schema version of the data understood by this application. This enables
   * versioning of mutators and the client view.
   */
  schemaVersion?: string | undefined;

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
   * This defines the schemas of the tables used in Zero and
   * their relationships to one another.
   */
  schemas?: QD | undefined;

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
}
