export function emptyFunction() {}
export const emptyObject = Object.freeze({});
export const emptyArray = Object.freeze([]);
export function identity<T>(x: T): T {
  return x;
}
