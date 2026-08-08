import type {Downstream} from '../../../zero-protocol/src/down.ts';

const serializedDownstreams = new WeakMap<object, string>();

/**
 * Associates a downstream message with its already-computed JSON encoding.
 * The message remains structurally unchanged for in-process consumers.
 */
export function setSerializedDownstream<T extends Downstream>(
  message: T,
  serialized: string,
): T {
  serializedDownstreams.set(message, serialized);
  return message;
}

export function stringifyDownstream(message: Downstream): string {
  return serializedDownstreams.get(message) ?? JSON.stringify(message);
}
