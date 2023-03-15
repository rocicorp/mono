export {Replicache, makeIDBName} from './replicache.js';
export {TransactionClosedError} from './transaction-closed-error.js';
export {consoleLogSink} from '@rocicorp/logger';

export type {
  MaybePromise,
  MutatorDefs,
  MutatorReturn,
  Poke,
  RequestOptions,
  UpdateNeededReason,
  PendingMutation,
} from './replicache.js';
export type {ReplicacheOptions} from './replicache-options.js';
export type {IndexDefinition, IndexDefinitions} from './index-defs.js';
export type {
  ReadTransaction,
  WriteTransaction,
  CreateIndexDefinition,
  TransactionEnvironment,
  TransactionReason,
} from './transactions.js';
export type {
  GetScanIterator,
  GetIndexScanIterator,
  ScanResult,
  AsyncIterableIteratorToArray,
} from './scan-iterator.js';
export {makeScanResult} from './scan-iterator.js';
export type {LogSink, LogLevel} from '@rocicorp/logger';
export type {
  JSONObject,
  JSONValue,
  ReadonlyJSONValue,
  ReadonlyJSONObject,
} from './json.js';
export type {
  KeyTypeForScanOptions,
  ScanIndexOptions,
  ScanNoIndexOptions,
  ScanOptionIndexedStartKey,
  ScanOptions,
} from './scan-options.js';
export {isScanIndexOptions} from './scan-options.js';
export type {HTTPRequestInfo} from './http-request-info.js';
export type {
  Puller,
  PullResponseV1,
  PullResponseOKV1,
  PullerResultV1,
  PullerResultV0,
  PullResponseV0,
  PullResponseOKV0,
  PullerResult,
  PullResponse,
} from './puller.js';
export type {PatchOperation} from './patch-operation.js';
export {PullError} from './sync/pull-error.js';
export {getDefaultPuller} from './get-default-puller.js';
export type {Pusher, PushError, PusherResult, PushResponse} from './pusher.js';
export type {
  ClientStateNotFoundResponse,
  VersionNotSupportedResponse,
} from './error-responses.js';

export type {
  Store as ExperimentalKVStore,
  Read as ExperimentalKVRead,
  Write as ExperimentalKVWrite,
  CreateStore as ExperimentalCreateKVStore,
} from './kv/store.js';

export {MemStore as ExperimentalMemKVStore} from './kv/mem-store.js';

export type {PullRequest, PullRequestV0, PullRequestV1} from './sync/pull.js';

export type {ClientID, ClientGroupID} from './sync/ids.js';

export type {
  PushRequestV1,
  PushRequestV0,
  PushRequest,
  MutationV0,
  MutationV1,
} from './sync/push.js';

export {TEST_LICENSE_KEY} from '@rocicorp/licensing/src/client';

export type {IndexKey} from './db/index.js';

export type {
  Diff as ExperimentalDiff,
  DiffOperation as ExperimentalDiffOperation,
  DiffOperationAdd as ExperimentalDiffOperationAdd,
  DiffOperationDel as ExperimentalDiffOperationDel,
  DiffOperationChange as ExperimentalDiffOperationChange,
  IndexDiff as ExperimentalIndexDiff,
  NoIndexDiff as ExperimentalNoIndexDiff,
} from './btree/node.js';
export type {
  WatchNoIndexCallback as ExperimentalWatchNoIndexCallback,
  WatchOptions as ExperimentalWatchOptions,
  WatchNoIndexOptions as ExperimentalWatchNoIndexOptions,
  WatchIndexOptions as ExperimentalWatchIndexOptions,
  WatchCallbackForOptions as ExperimentalWatchCallbackForOptions,
  WatchIndexCallback as ExperimentalWatchIndexCallback,
  SubscribeOptions,
} from './subscriptions.js';

export {mergeAsyncIterables} from './merge-async-iterables.js';
export {filterAsyncIterable} from './filter-async-iterable.js';
export {deleteAllReplicacheData} from './persist/mod.js';
export type {IterableUnion} from './iterable-union.js';

export {version} from './version.js';
export type {Cookie} from './cookies.js';
