export interface Resolver<R = void, E = unknown> {
  promise: Promise<R>;
  resolve: (res: R) => void;
  reject: (err: E) => void;
}

type WithResolvers = <R = void, E = unknown>() => Resolver<R, E>;

export const resolver: WithResolvers = () => Promise.withResolvers();
