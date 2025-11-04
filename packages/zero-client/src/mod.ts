export type {VersionNotSupportedResponse} from '../../replicache/src/error-responses.ts';
export {getDefaultPuller} from '../../replicache/src/get-default-puller.ts';
export type {HTTPRequestInfo} from '../../replicache/src/http-request-info.ts';
export {IDBNotFoundError} from '../../replicache/src/kv/idb-store.ts';
export type {
  CreateStore as CreateKVStore,
  Read as KVRead,
  Store as KVStore,
  Write as KVWrite,
} from '../../replicache/src/kv/store.ts';
export {makeIDBName} from '../../replicache/src/make-idb-name.ts';
export {
  dropAllDatabases,
  dropDatabase,
} from '../../replicache/src/persist/collect-idb-databases.ts';
export type {ClientGroupID, ClientID} from '../../replicache/src/sync/ids.ts';
export {TransactionClosedError} from '../../replicache/src/transaction-closed-error.ts';
export type {
  JSONObject,
  JSONValue,
  ReadonlyJSONObject,
  ReadonlyJSONValue,
} from '../../shared/src/json.ts';
export type {MaybePromise} from '../../shared/src/types.ts';
export {ApplicationError} from '../../zero-protocol/src/application-error.ts';
export type {ApplicationErrorOptions} from '../../zero-protocol/src/application-error.ts';
export type {
  AST,
  Bound,
  ColumnReference,
  CompoundKey,
  Condition,
  Conjunction,
  CorrelatedSubquery,
  CorrelatedSubqueryCondition,
  CorrelatedSubqueryConditionOperator,
  Disjunction,
  EqualityOps,
  InOps,
  LikeOps,
  LiteralReference,
  LiteralValue,
  Ordering,
  OrderOps,
  OrderPart,
  Parameter,
  SimpleCondition,
  SimpleOperator,
  ValuePosition,
} from '../../zero-protocol/src/ast.ts';
export {
  transformRequestMessageSchema,
  transformResponseMessageSchema,
  type TransformRequestBody,
  type TransformRequestMessage,
  type TransformResponseBody,
  type TransformResponseMessage,
} from '../../zero-protocol/src/custom-queries.ts';
export {ErrorKind} from '../../zero-protocol/src/error-kind.ts';
export {ErrorOrigin} from '../../zero-protocol/src/error-origin.ts';
export {ErrorReason} from '../../zero-protocol/src/error-reason.ts';
export {relationships} from '../../zero-schema/src/builder/relationship-builder.ts';
export {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
export {
  boolean,
  enumeration,
  json,
  number,
  string,
  table,
  type ColumnBuilder,
  type TableBuilderWithColumns,
} from '../../zero-schema/src/builder/table-builder.ts';
export type {
  AssetPermissions as CompiledAssetPermissions,
  PermissionsConfig as CompiledPermissionsConfig,
  Policy as CompiledPermissionsPolicy,
  Rule as CompiledPermissionsRule,
} from '../../zero-schema/src/compiled-permissions.ts';
export {
  ANYONE_CAN,
  ANYONE_CAN_DO_ANYTHING,
  definePermissions,
  NOBODY_CAN,
} from '../../zero-schema/src/permissions.ts';
export type {
  AssetPermissions,
  PermissionRule,
  PermissionsConfig,
} from '../../zero-schema/src/permissions.ts';
export {type TableSchema} from '../../zero-schema/src/table-schema.ts';
export type {
  SchemaValue,
  SchemaValueWithCustomType,
  ValueType,
} from '../../zero-schema/src/table-schema.ts';
export type {Schema} from '../../zero-types/src/schema.ts';
export type {Change} from '../../zql/src/ivm/change.ts';
export type {Node} from '../../zql/src/ivm/data.ts';
export type {Input, Output} from '../../zql/src/ivm/operator.ts';
export type {Stream} from '../../zql/src/ivm/stream.ts';
export {
  applyChange,
  type ViewChange,
} from '../../zql/src/ivm/view-apply-change.ts';
export type {
  AnyViewFactory,
  Entry,
  Format,
  View,
  ViewFactory,
} from '../../zql/src/ivm/view.ts';
export type {
  DeleteID,
  InsertValue,
  SchemaQuery,
  ServerTransaction,
  Transaction,
  UpdateValue,
  UpsertValue,
} from '../../zql/src/mutate/custom.ts';
export {
  defineQuery,
  defineQueryWithContextType,
} from '../../zql/src/query/define-query.ts';
export type {
  AnyNamedQueryFunction,
  DefineQueryFunc,
  DefineQueryOptions,
  NamedQueryFunction,
} from '../../zql/src/query/define-query.ts';
export {escapeLike} from '../../zql/src/query/escape-like.ts';
export type {
  ExpressionBuilder,
  ExpressionFactory,
} from '../../zql/src/query/expression.ts';
export {
  createBuilder,
  syncedQuery,
  syncedQueryWithContext,
  withValidation,
} from '../../zql/src/query/named.ts';
export type {
  CustomQueryID,
  HasParseFn,
  ParseFn,
  Parser,
  QueryFn,
  SyncedQuery,
} from '../../zql/src/query/named.ts';
export type {QueryInternals} from '../../zql/src/query/query-internals.ts';
export type {
  AnyQuery,
  HumanReadable,
  PullRow,
  Query,
  QueryResultType,
  QueryRowType,
  Row,
  RunOptions,
  ZeRow,
} from '../../zql/src/query/query.ts';
export {RootNamedQuery} from '../../zql/src/query/root-named-query.ts';
export {type TTL} from '../../zql/src/query/ttl.ts';
export type {ResultType, TypedView} from '../../zql/src/query/typed-view.ts';
export {
  bindingsForZero,
  registerZeroDelegate,
  type BindingsForZero,
} from './client/bindings.ts';
export {ClientErrorKind} from './client/client-error-kind.ts';
export type {ConnectionState} from './client/connection-manager.ts';
export {ConnectionStatus} from './client/connection-status.ts';
export type {
  Connection,
  ConnectionSource,
  Source,
} from './client/connection.ts';
export type {BatchMutator, DBMutator, TableMutator} from './client/crud.ts';
export type {
  CustomMutatorDefs,
  CustomMutatorImpl,
  MakeCustomMutatorInterface,
  MakeCustomMutatorInterfaces,
  MutationResultErrorDetails,
  MutationResultSuccessDetails,
  MutatorResultDetails,
  MutatorResult as PromiseWithServerResult,
} from './client/custom.ts';
export {isClientError, isServerError, isZeroError} from './client/error.ts';
export type {
  AuthError,
  ClientError,
  ClientErrorBody,
  ClosedError,
  NeedsAuthReason,
  OfflineError,
  ServerError,
  ZeroError,
  ZeroErrorBody,
  ZeroErrorDetails,
  ZeroErrorKind,
} from './client/error.ts';
export type {ClientGroup as InspectorClientGroup} from './client/inspector/client-group.ts';
export type {Client as InspectorClient} from './client/inspector/client.ts';
export type {Inspector} from './client/inspector/inspector.ts';
export type {Query as InspectorQuery} from './client/inspector/query.ts';
export type {UpdateNeededReason, ZeroOptions} from './client/options.ts';
export {UpdateNeededReasonType} from './client/update-needed-reason-type.ts';
export {Zero, type MakeEntityQueriesFromSchema} from './client/zero.ts';
export type {
  QueryErrorDetails,
  QueryResultDetails,
} from './types/query-result.ts';
