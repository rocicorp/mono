import type {LogLevel, LogSink} from '@rocicorp/logger';
import type {InternalDiff} from './btree/node.ts';
import type {Read, Store} from './dag/store.ts';
import type {Hash} from './hash.ts';
import type {IndexDefinitions} from './index-defs.ts';
import type {StoreProvider} from './kv/store.ts';
import type {Puller} from './puller.ts';
import type {Pusher} from './pusher.ts';
import type {MutatorDefs, RequestOptions} from './types.ts';

/**
 * The options passed to {@link Replicache}.
 */

export interface ReplicacheOptions<MD extends MutatorDefs> {
  /**
   * This is the URL to the server endpoint dealing with the push updates. See
   * [Push Endpoint Reference](https://doc.replicache.dev/reference/server-push) for more
   * details.
   *
   * If not provided, push requests will not be made unless a custom
   * {@link ReplicacheOptions.pusher} is provided.
   */
  pushURL?: string | undefined;

  /**
   * This is the authorization token used when doing a
   * [pull](https://doc.replicache.dev/reference/server-pull#authorization) and
   * [push](https://doc.replicache.dev/reference/server-push#authorization).
   */
  auth?: string | undefined;

  /**
   * This is the URL to the server endpoint dealing with pull. See [Pull
   * Endpoint Reference](https://doc.replicache.dev/reference/server-pull) for more
   * details.
   *
   * If not provided, pull requests will not be made unless a custom
   * {@link ReplicacheOptions.puller} is provided.
   */
  pullURL?: string | undefined;

  /**
   * The name of the Replicache database.
   *
   * It is important to use user specific names so that if there are multiple
   * tabs open for different distinct users their data is kept separate.
   *
   * For efficiency and performance, a new {@link Replicache} instance will
   * initialize its state from the persisted state of an existing {@link Replicache}
   * instance with the same `name`, domain and browser profile.
   *
   * Mutations from one {@link Replicache} instance may be pushed using the
   * {@link ReplicacheOptions.auth}, {@link ReplicacheOptions.pushURL},
   * {@link ReplicacheOptions.pullURL}, {@link ReplicacheOptions.pusher}, and
   * {@link ReplicacheOptions.puller}  of another Replicache instance with the same
   * `name`, domain and browser profile.
   *
   * You can use multiple Replicache instances for the same user as long as the
   * names are unique.  e.g. `name: `$userID:$roomID`
   */
  name: string;

  /**
   * The schema version of the data understood by this application. This enables
   * versioning of mutators (in the push direction) and the client view (in the
   * pull direction).
   */
  schemaVersion?: string | undefined;

  /**
   * The duration between each {@link pull} in milliseconds. Set this to `null` to
   * prevent pulling in the background.  Defaults to 60 seconds.
   */
  pullInterval?: number | null | undefined;

  /**
   * The delay between when a change is made to Replicache and when Replicache
   * attempts to push that change.
   */
  pushDelay?: number | undefined;

  /**
   * Determines how much logging to do. When this is set to `'debug'`,
   * Replicache will also log `'info'` and `'error'` messages. When set to
   * `'info'` we log `'info'` and `'error'` but not `'debug'`. When set to
   * `'error'` we only log `'error'` messages.
   * Default is `'info'`.
   */
  logLevel?: LogLevel | undefined;

  /**
   * Enables custom handling of logs.
   *
   * By default logs are logged to the console.  If you would like logs to be
   * sent elsewhere (e.g. to a cloud logging service like DataDog) you can
   * provide an array of {@link LogSink}s.  Logs at or above
   * {@link ReplicacheOptions.logLevel} are sent to each of these {@link LogSink}s.
   * If you would still like logs to go to the console, include
   * `consoleLogSink` in the array.
   *
   * ```ts
   * logSinks: [consoleLogSink, myCloudLogSink],
   * ```
   */
  logSinks?: LogSink[] | undefined;

  /**
   * An object used as a map to define the *mutators*. These gets registered at
   * startup of {@link Replicache}.
   *
   * *Mutators* are used to make changes to the data.
   *
   * #### Example
   *
   * The registered *mutations* are reflected on the
   * {@link Replicache.mutate | mutate} property of the {@link Replicache} instance.
   *
   * ```ts
   * const rep = new Replicache({
   *   name: 'user-id',
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
   * await rep.mutate.createTodo({
   *   id: 1234,
   *   title: 'Make things work offline',
   *   complete: true,
   * });
   * ```
   *
   * #### Replays
   *
   * *Mutators* run once when they are initially invoked, but they might also be
   * *replayed* multiple times during sync. As such *mutators* should not modify
   * application state directly. Also, it is important that the set of
   * registered mutator names only grows over time. If Replicache syncs and
   * needed *mutator* is not registered, it will substitute a no-op mutator, but
   * this might be a poor user experience.
   *
   * #### Server application
   *
   * During push, a description of each mutation is sent to the server's [push
   * endpoint](https://doc.replicache.dev/reference/server-push) where it is applied. Once
   * the *mutation* has been applied successfully, as indicated by the client
   * view's
   * [`lastMutationId`](https://doc.replicache.dev/reference/server-pull#lastmutationid)
   * field, the local version of the *mutation* is removed. See the [design
   * doc](https://doc.replicache.dev/design#commits) for additional details on
   * the sync protocol.
   *
   * #### Transactionality
   *
   * *Mutators* are atomic: all their changes are applied together, or none are.
   * Throwing an exception aborts the transaction. Otherwise, it is committed.
   * As with {@link query} and {@link subscribe} all reads will see a consistent view of
   * the cache while they run.
   */
  mutators?: MD | undefined;

  /**
   * Options to use when doing pull and push requests.
   */
  requestOptions?: RequestOptions | undefined;

  /**
   * Allows passing in a custom implementation of a {@link Puller} function. This
   * function is called when doing a pull and it is responsible for
   * communicating with the server.
   *
   * Normally, this is just a POST to a URL with a JSON body but you can provide
   * your own function if you need to do things differently.
   */
  puller?: Puller | undefined;

  /**
   * Allows passing in a custom implementation of a {@link Pusher} function. This
   * function is called when doing a push and it is responsible for
   * communicating with the server.
   *
   * Normally, this is just a POST to a URL with a JSON body but you can provide
   * your own function if you need to do things differently.
   */
  pusher?: Pusher | undefined;

  /**
   * @deprecated Replicache no longer uses a license key. This option is now
   * ignored and will be removed in a future release.
   */
  licenseKey?: string | undefined;

  /**
   * Allows providing a custom implementation of the underlying storage layer.
   */
  kvStore?: 'mem' | 'idb' | StoreProvider | undefined;

  /**
   * Defines the indexes, if any, to use on the data.
   */
  readonly indexes?: IndexDefinitions | undefined;

  /**
   * The maximum age of a client in milliseconds. If a client hasn't been seen
   * and has no pending mutations for this long, it will be removed from the
   * cache. Default is 24 hours.
   *
   * This means that this is the maximum time a tab can be in the background
   * (frozen or in fbcache) and still be able to sync when it comes back to the
   * foreground. If tab comes back after this time the
   * {@linkcode onClientStateNotFound} callback is called on the Replicache
   * instance.
   */
  clientMaxAgeMs?: number | undefined;
}

/**
 * Replicache calls the `ZeroOption` to create a new
 * IVM branch at the correct head. This branch
 * is tacked onto Replicache's `WriteTransaction`
 * which is passed to the mutators.
 *
 * Replicache shouldn't depend on Zero directly, so
 * we define a minimal interface as a placeholder.
 *
 * Zero will cast `ZeroTxData` to `IVMSourceBranch`
 * inside of it's `Transaction` object.
 *
 * ```ts
 * const zeroData = await zeroOption.getTxData(expectedHead, desiredHead);
 * const tx = new WriteTransaction(
 *   zeroData,
 * );
 * await mutatorImpl(tx, args);
 * ```
 *
 * `mutatorImpl` is a function that was created by Zero
 *
 */
export interface ZeroTxData {
  ivmSources: unknown;
  token: string | undefined;
}

export type ZeroReadOptions = {
  openLazyRead?: Read | undefined;
  openLazySourceRead?: Read | undefined;
};

declare const idTag: unique symbol;
export type EphemeralID = number & {[idTag]: true};

export type MutationTrackingData = {
  ephemeralID: EphemeralID;
  serverPromise: Promise<unknown>;
};

/**
 * Minimal interface that Replicache needs to communicate with Zero.
 * Prevents us from creating any direct dependencies on Zero.
 */
export interface ZeroOption {
  auth: string;

  /**
   * Allow Zero to initialize its IVM state from the given hash and dag.
   */
  init(hash: Hash, store: Store): Promise<void>;

  /**
   * When a refresh, persist, or pullEnd occurs Zero must fork its IVM sources
   * for use in rebase operations. Replicache will call zero during these
   * operations so it can fork its IVM state to the desired head.
   *
   * The data returned by `getTxData` will be available on the Replicache transaction
   * object for use in Zero's mutators.
   */
  getTxData(
    desiredHead: Hash,
    readOptions?: ZeroReadOptions | undefined,
  ): Promise<ZeroTxData> | undefined;

  /**
   * When Replicache's main head moves forward, Zero must advance its IVM state.
   */
  advance(expectedHash: Hash, newHash: Hash, changes: InternalDiff): void;

  trackMutation(): MutationTrackingData;
  mutationIDAssigned(ephemeralID: EphemeralID, mutationID: number): void;
  rejectMutation(ephemeralID: EphemeralID, ex: unknown): void;
}
