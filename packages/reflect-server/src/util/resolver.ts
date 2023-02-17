export interface Resolver<R = void, E = unknown> {
  promise: Promise<R>;
  resolve: (res: R) => void;
  reject: (err: E) => void;
}

export function resolver<R = void, E = unknown>(): Resolver<R, E> {
  let resolve!: (res: R) => void;
  let reject!: (err: E) => void;
  const promise = new Promise<R>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return {promise, resolve, reject};
}
