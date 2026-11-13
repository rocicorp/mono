export interface Resolver<R = void, E = unknown> {
  promise: Promise<R>;
  resolve: (res: R) => void;
  reject: (err: E) => void;
}

type WithResolvers = <R = void, E = unknown>() => Resolver<R, E>;

export const resolver: WithResolvers =
  typeof Promise.withResolvers === 'function'
    ? () => Promise.withResolvers()
    : <R = void, E = unknown>() => {
        let resolve!: (res: R) => void;
        let reject!: (err: E) => void;
        const promise = new Promise<R>((res, rej) => {
          resolve = res;
          reject = rej;
        });
        return {promise, resolve, reject};
      };
