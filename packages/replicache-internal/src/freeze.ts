import {skipFreeze} from './config.js';

export function freeze<T>(o: T): Readonly<T> {
  if (skipFreeze) {
    return o;
  }
  return Object.freeze(o);
}
