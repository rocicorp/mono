export {Replicache, makeIDBName} from './replicache';
export {TransactionClosedError} from './transaction-closed-error';
export {consoleLogSink} from '@rocicorp/logger';

export type {
  MaybePromise,
  MutatorDefs,
  Poke,
  RequestOptions,
  ClientStateNotFoundReason,
  PendingMutation,
} from './replicache';
export type {ReplicacheOptions} from './replicache-options';
export type {IndexDefinition, IndexDefinitions} from './index-defs';
export type {ReadTransaction, WriteTransaction} from './transactions';
export type {
  GetScanIterator,
  GetIndexScanIterator,
  ScanResult,
  AsyncIterableIteratorToArray,
} from './scan-iterator';
export {makeScanResult} from './scan-iterator';
export type {LogSink, LogLevel} from '@rocicorp/logger';
export type {
  JSONObject,
  JSONValue,
  ReadonlyJSONValue,
  ReadonlyJSONObject,
} from './json';
export type {
  KeyTypeForScanOptions,
  ScanIndexOptions,
  ScanNoIndexOptions,
  ScanOptionIndexedStartKey,
  ScanOptions,
} from './scan-options';
export {isScanIndexOptions} from './scan-options';
export type {HTTPRequestInfo} from './http-request-info';
export type {
  PatchOperation,
  Puller,
  PullResponse,
  PullResponseOK,
  ClientStateNotFoundResponse,
  PullerResult,
  PullError,
} from './puller';
export type {Pusher, PushError} from './pusher';

export type {
  Store as ExperimentalKVStore,
  Read as ExperimentalKVRead,
  Write as ExperimentalKVWrite,
} from './kv/store';

export type {PullRequest} from './sync/pull';
export type {PushRequest} from './sync/push';

export {TEST_LICENSE_KEY} from '@rocicorp/licensing/src/client';

export type {IndexKey} from './db/index';

export type {
  Diff as ExperimentalDiff,
  DiffOperation as ExperimentalDiffOperation,
  DiffOperationAdd as ExperimentalDiffOperationAdd,
  DiffOperationDel as ExperimentalDiffOperationDel,
  DiffOperationChange as ExperimentalDiffOperationChange,
  IndexDiff as ExperimentalIndexDiff,
  NoIndexDiff as ExperimentalNoIndexDiff,
} from './btree/node';
export type {
  WatchNoIndexCallback as ExperimentalWatchNoIndexCallback,
  WatchOptions as ExperimentalWatchOptions,
  WatchNoIndexOptions as ExperimentalWatchNoIndexOptions,
  WatchIndexOptions as ExperimentalWatchIndexOptions,
  WatchCallbackForOptions as ExperimentalWatchCallbackForOptions,
  WatchIndexCallback as ExperimentalWatchIndexCallback,
  WatchHash as ExperimentalWatchHash,
  SubscribeOptions,
} from './subscriptions';

export {mergeAsyncIterables} from './merge-async-iterables';
export {filterAsyncIterable} from './filter-async-iterable';
export {deleteAllReplicacheData} from './persist/mod';
export type {IterableUnion} from './iterable-union';
export {WatchHashInitialRun as ExperimentalWatchHashInitialRun} from './subscriptions';

export {version} from './version';

export type {CreateIndexDefinition} from './db/commit';
