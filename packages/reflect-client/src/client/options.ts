import type {LogLevel} from '@rocicorp/logger';
import type {MutatorDefs} from 'reflect-shared';
import type {
  ExperimentalCreateKVStore as CreateKVStore,
  MaybePromise,
} from 'replicache';
import {WSString, toHTTPString, type HTTPString} from './http-string.js';

/**
 * Configuration for [[Reflect]].
 */
export interface ReflectOptions<MD extends MutatorDefs> {
  /**
   * Server to connect to, for example "https://myapp-myteam.reflect.net/".
   */
  server?: string | null | undefined;

  /**
   * Server to connect to, for example "wss://myapp-myteam.reflect.net/".
   * @deprecated Use {@code server} instead.
   */
  socketOrigin?: string | null | undefined;

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
   * For efficiency, a new Reflect instance will initialize its state from
   * the persisted state of an existing Reflect instance with the same
   * `userID`, `roomID`, domain and browser profile.
   */
  userID: string;

  /**
   * A unique identifier for the room.
   *
   * For efficiency, a new Reflect instance will initialize its state from
   * the persisted state of an existing Reflect instance with the same
   * `userID`, `roomID`, domain and browser profile.
   *
   * Mutations from one Reflect instance may be pushed using the
   * [[Reflect.auth]] of another Reflect instance with the same
   * `userID`, `roomID`, domain and browser profile.
   */
  roomID: string;

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
   * Determines the level of detail at which Reflect logs messages about
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
   * *Mutators* are used to make changes to the Reflect data.
   *
   * The registered *mutations* are reflected on the
   * [[Reflect.mutate|mutate]] property of the [[Reflect]] instance.
   *
   * #### Example
   *
   * ```ts
   * const reflect = new Reflect({
   *   server: 'https://example.com/',
   *   userID: 'user-id',
   *   roomID: 'room-id',
   *   mutators: {
   *     async createTodo(tx: WriteTransaction, args: JSONValue) {
   *       const key = `/todo/${args.id}`;
   *       if (await tx.has(key)) {
   *         throw new Error('Todo already exists');
   *       }
   *       await tx.put(key, args);
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
   * await reflect.mutate.createTodo({
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
   * registered mutator names only grows over time. If Reflect syncs and a
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
   * `onOnlineChange` is called when the Reflect instance's online status changes
   */
  onOnlineChange?: ((online: boolean) => void) | undefined;

  /**
   * The number of milliseconds to wait before disconnecting a Reflect
   * instance whose tab has become hidden.
   *
   * Instances in hidden tabs are disconnected to save resources.
   *
   * Default is 5_000.
   */
  hiddenTabDisconnectDelay?: number | undefined;

  /**
   * Help Reflect improve its service by automatically sending diagnostic and
   * usage data.
   *
   * Default is true.
   */
  enableAnalytics?: boolean | undefined;

  /**
   * Determines what kind of storage implementation to use on the client.
   *
   * Defaults to `'mem'` which means that Reflect uses an in memory storage and
   * the data is not persisted on the client.
   *
   * By setting this to `'idb'` the data is persisted on the client using
   * IndexedDB, allowing faster syncs between application restarts.
   *
   * You can also set this to a function that is used to create new KV stores,
   * allowing a custom implementation of the underlying storage layer.
   */
  kvStore?: 'mem' | 'idb' | CreateKVStore | undefined;
}

function validateServerParam<
  S extends 'ws' | 'http',
  R = S extends 'ws' ? WSString : HTTPString,
>(paramName: string, server: string, expectedProtocol: S): R {
  if (server) {
    if (
      !server.startsWith(`${expectedProtocol}://`) &&
      !server.startsWith(`${expectedProtocol}s://`)
    ) {
      throw new Error(
        `ReflectOptions.${paramName} must use the '${expectedProtocol}' or '${expectedProtocol}s' scheme.`,
      );
    }
    if (!server.endsWith('/')) {
      throw new Error(
        `ReflectOptions.${paramName} must not contain a path component. For example: "https://myapp-myteam.reflect.net/".`,
      );
    }
  }
  return server as R;
}

export function getServer(
  server: string | null | undefined,
  socketOrigin: string | null | undefined,
): HTTPString | null {
  if (server) {
    return validateServerParam('server', server, 'http') as HTTPString;
  }

  if (socketOrigin) {
    const validatedSocketOrigin = validateServerParam(
      'socketOrigin',
      socketOrigin,
      'ws',
    );
    return toHTTPString(validatedSocketOrigin);
  }

  return null;
}
