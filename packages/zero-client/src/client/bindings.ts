// Internal APIs for building UI bindings (React, Solid, etc.)
// These are NOT part of the public API and should not be used directly.
// They are only used by the binding packages.
// This module should not export anything that is exported by ./zero.ts.

export {assert, unreachable} from '../../../shared/src/asserts.ts';
export {deepClone} from '../../../shared/src/deep-clone.ts';
export type {Immutable} from '../../../shared/src/immutable.ts';
export {must} from '../../../shared/src/must.ts';
export {skipYields} from '../../../zql/src/ivm/operator.ts';
export {consume} from '../../../zql/src/ivm/stream.ts';
export {
  applyChange,
  idSymbol,
  refCountSymbol,
  type ViewChange,
} from '../../../zql/src/ivm/view-apply-change.ts';
export type {QueryDelegate} from '../../../zql/src/query/query-delegate.ts';
export {newQuery, type QueryImpl} from '../../../zql/src/query/query-impl.ts';
export {
  asQueryInternals,
  queryInternalsTag,
} from '../../../zql/src/query/query-internals.ts';
export {addContextToQuery} from '../../../zql/src/query/query-registry.ts';
export {DEFAULT_TTL_MS} from '../../../zql/src/query/ttl.ts';
