import {resolver} from './resolver.js';

// This is lifted from Replicache. Perhaps should be refactored into shared repo?
// Same with resolver.ts.
export class Lock {
  private _lockP: Promise<void> | null = null;

  async lock(): Promise<() => void> {
    const previous = this._lockP;
    const {promise, resolve} = resolver();
    this._lockP = promise;
    await previous;
    return resolve;
  }

  withLock<R>(f: () => R | Promise<R>): Promise<R> {
    return run(this.lock(), f);
  }
}

async function run<R>(
  p: Promise<() => void>,
  f: () => R | Promise<R>,
): Promise<R> {
  const release = await p;
  try {
    return await f();
  } finally {
    release();
  }
}
