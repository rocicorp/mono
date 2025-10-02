// Export all types and classes from the new query structure
export type {
  AnyChainQuery,
  ChainQuery,
  Func,
  QueryImplementation,
} from './types.ts';

export {ChainedQuery} from './chained-query.ts';
export {RootNamedQuery} from './root-named-query.ts';

// For backward compatibility, re-export RootNamedQuery as NamedQuery
export {RootNamedQuery as NamedQuery} from './root-named-query.ts';
