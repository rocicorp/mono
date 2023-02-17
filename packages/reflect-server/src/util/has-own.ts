/* eslint-disable @typescript-eslint/no-explicit-any */
const objectPrototypeHasOwnProperty = Object.prototype.hasOwnProperty;

/**
 * Object.hasOwn polyfill
 */
export const hasOwn: (object: any, key: PropertyKey) => boolean =
  (Object as any).hasOwn ||
  ((object, key) => objectPrototypeHasOwnProperty.call(object, key));
