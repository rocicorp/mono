// Re-export everything from the new modular structure
export type {
  AnyChainQuery,
  ChainQuery,
  Func,
  QueryImplementation,
} from './new/index.ts';

export {ChainedQuery, NamedQuery, RootNamedQuery} from './new/index.ts';
