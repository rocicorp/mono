export {TEST_LICENSE_KEY} from '@rocicorp/licensing/src/client';
export {consoleLogSink} from '@rocicorp/logger';
export type {LogLevel, LogSink} from '@rocicorp/logger';
export type {
  Diff as ExperimentalDiff,
  DiffOperation as ExperimentalDiffOperation,
  DiffOperationAdd as ExperimentalDiffOperationAdd,
  DiffOperationChange as ExperimentalDiffOperationChange,
  DiffOperationDel as ExperimentalDiffOperationDel,
  IndexDiff as ExperimentalIndexDiff,
  NoIndexDiff as ExperimentalNoIndexDiff,
} from './btree/node.js';
export type {Cookie} from './cookies.js';
export type {IndexKey} from './db/index.js';
export type {
  ClientStateNotFoundResponse,
  VersionNotSupportedResponse,
} from './error-responses.js';
export {filterAsyncIterable} from './filter-async-iterable.js';
export {getDefaultPuller} from './get-default-puller.js';
export type {HTTPRequestInfo} from './http-request-info.js';
export type {IndexDefinition, IndexDefinitions} from './index-defs.js';
export type {IterableUnion} from './iterable-union.js';
export type {
  JSONObject,
  JSONValue,
  ReadonlyJSONObject,
  ReadonlyJSONValue,
} from './json.js';
export {IDBNotFoundError} from './kv/idb-store.js';
export {MemStore as ExperimentalMemKVStore} from './kv/mem-store.js';
export type {
  CreateStore as ExperimentalCreateKVStore,
  Read as ExperimentalKVRead,
  Store as ExperimentalKVStore,
  Write as ExperimentalKVWrite,
} from './kv/store.js';
export {mergeAsyncIterables} from './merge-async-iterables.js';
export type {PatchOperation} from './patch-operation.js';
export {
  deleteAllReplicacheData,
  dropAllDatabases,
  dropDatabase,
} from './persist/mod.js';
export type {
  PullResponse,
  PullResponseOKV0,
  PullResponseOKV1,
  PullResponseV0,
  PullResponseV1,
  Puller,
  PullerResult,
  PullerResultV0,
  PullerResultV1,
} from './puller.js';
export type {PushError, PushResponse, Pusher, PusherResult} from './pusher.js';
export type {ReplicacheOptions} from './replicache-options.js';
export {Replicache, makeIDBName} from './replicache.js';
export type {
  MaybePromise,
  MutatorDefs,
  MutatorReturn,
  PendingMutation,
  Poke,
  RequestOptions,
  UpdateNeededReason,
} from './replicache.js';
export {makeScanResult} from './scan-iterator.js';
export type {
  AsyncIterableIteratorToArray,
  GetIndexScanIterator,
  GetScanIterator,
  ScanResult,
} from './scan-iterator.js';
export {isScanIndexOptions} from './scan-options.js';
export type {
  KeyTypeForScanOptions,
  ScanIndexOptions,
  ScanNoIndexOptions,
  ScanOptionIndexedStartKey,
  ScanOptions,
} from './scan-options.js';
export type {
  WatchCallbackForOptions as ExperimentalWatchCallbackForOptions,
  WatchIndexCallback as ExperimentalWatchIndexCallback,
  WatchIndexOptions as ExperimentalWatchIndexOptions,
  WatchNoIndexCallback as ExperimentalWatchNoIndexCallback,
  WatchNoIndexOptions as ExperimentalWatchNoIndexOptions,
  WatchOptions as ExperimentalWatchOptions,
  SubscribeOptions,
} from './subscriptions.js';
export type {ClientGroupID, ClientID} from './sync/ids.js';
export {PullError} from './sync/pull-error.js';
export type {PullRequest, PullRequestV0, PullRequestV1} from './sync/pull.js';
export type {
  MutationV0,
  MutationV1,
  PushRequest,
  PushRequestV0,
  PushRequestV1,
} from './sync/push.js';
export {TransactionClosedError} from './transaction-closed-error.js';
export type {
  CreateIndexDefinition,
  DeepReadonly,
  DeepReadonlyObject,
  ReadTransaction,
  TransactionEnvironment,
  TransactionReason,
  WriteTransaction,
} from './transactions.js';
export {version} from './version.js';
