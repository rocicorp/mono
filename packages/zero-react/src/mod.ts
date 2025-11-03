export type {Expand} from '../../shared/src/expand.ts';
export type {ConnectionState} from '../../zero-client/src/client/connection-manager.ts';
export {ConnectionStatus} from '../../zero-client/src/client/connection-status.ts';
export type {
  AuthError,
  NeedsAuthReason,
  ZeroError,
  ZeroErrorKind,
} from '../../zero-client/src/client/error.ts';
export type {QueryResultDetails} from '../../zero-client/src/types/query-result.ts';
export type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';
export type {
  RelationshipsSchema,
  SchemaValue,
  TableSchema,
} from '../../zero-schema/src/table-schema.ts';
export type {Schema} from '../../zero-types/src/schema.ts';
export type {HumanReadable} from '../../zql/src/query/query.ts';
export type {ResultType} from '../../zql/src/query/typed-view.ts';
export {ZeroInspector} from './components/zero-inspector.tsx';
export {
  useQuery,
  useSuspenseQuery,
  type QueryResult,
  type UseQueryOptions,
} from './use-query.tsx';
export {useZeroConnectionState} from './use-zero-connection-state.tsx';
export {useZeroOnline} from './use-zero-online.tsx';
export {createUseZero, useZero, ZeroProvider} from './zero-provider.tsx';
