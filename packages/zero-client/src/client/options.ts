import type {LogLevel} from '@rocicorp/logger';
import type {KVStoreProvider, MaybePromise, MutatorDefs} from 'replicache';
import type {ReadonlyJSONObject} from 'shared/src/json.js';
import type {QueryDefs} from './zero.js';

export type QueryParseDefs<QD extends QueryDefs> = {
  readonly [K in keyof QD]: (value: ReadonlyJSONObject) => QD[K];
};

/**
 * Configuration for [[Zero]].
 */
export interface ZeroOptions<MD extends MutatorDefs, QD extends QueryDefs> {
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
   * An object used as a map to define the *mutators* for this application.
   *
   * *Mutators* are used to make changes to the Zero data.
   *
   * The registered *mutations* are reflected on the
   * [[Zero.mutate|mutate]] property of the [[Zero]] instance.
   *
   * #### Example
   *
   * ```ts
   * const zero = new Zero({
   *   server: 'https://example.com/',
   *   userID: 'user-id',
   *   roomID: 'room-id',
   *   mutators: {
   *     async createTodo(tx: WriteTransaction, args: JSONValue) {
   *       const key = `/todo/${args.id}`;
   *       if (await tx.has(key)) {
   *         throw new Error('Todo already exists');
   *       }
   *       await tx.set(key, args);
   *     },
   *     async deleteTodo(tx: WriteTransaction, id: number) {
   *       ...
   *     },
   *   },
   * });
   * ```
   *
   * This will create the function to later use:
   *
   * ```ts
   * await zero.mutate.createTodo({
   *   id: 1234,
   *   title: 'Make things realtime',
   *   complete: true,
   * });
   * ```
   *
   * #### Replays
   *
   * *Mutators* run once when they are initially invoked, but they might also be
   * *replayed* multiple times during sync. As such *mutators* should not modify
   * application state directly. Also, it is important that the set of
   * registered mutator names only grows over time. If Zero syncs and a
   * needed *mutator* is not registered, it will substitute a no-op mutator, but
   * this might be a poor user experience.
   *
   * #### Server application
   *
   * During sync, a description of each mutation is sent to the server where it
   * is applied. Once the *mutation* has been applied successfully, the local
   * version of the *mutation* is removed. See the [design
   * doc](https://doc.replicache.dev/design#commits) for additional details on
   * the sync protocol.
   *
   * #### Transactionality
   *
   * *Mutators* are atomic: all their changes are applied together, or none are.
   * Throwing an exception aborts the transaction. Otherwise, it is committed.
   * As with [[query]] and [[subscribe]] all reads will see a consistent view of
   * the cache while they run.
   */
  mutators?: MD | undefined;

  /**
   * This defines the names and types of the queries that Zero manages. The
   * return type of the parse function is used to infer the type of the query.
   *
   * At the moment the parse functions are not being used to validate the data
   * stored by Zero but future work will enable this.
   */
  queries?: QueryParseDefs<QD> | undefined;

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
   * Defaults to `'mem'` which means that Zero uses an in memory storage and
   * the data is not persisted on the client.
   *
   * By setting this to `'idb'` the data is persisted on the client using
   * IndexedDB, allowing faster syncs between application restarts.
   *
   * You can also set this to a function that is used to create new KV stores,
   * allowing a custom implementation of the underlying storage layer.
   */
  kvStore?: 'mem' | 'idb' | KVStoreProvider | undefined;
}
