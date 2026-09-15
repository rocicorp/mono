import type {Hash} from '../hash.ts';

/**
 * Thrown when a ref count read from the store is not a valid Uint16. This
 * means the store is corrupt (due to some unknown bug) and cannot be repaired.
 */
export class InvalidRefCountError extends Error {
  name = 'InvalidRefCountError';
  readonly hash: Hash;
  readonly value: unknown;
  constructor(hash: Hash, value: unknown) {
    super(
      `Invalid ref count ${String(value)} for ${hash}. We expect the value to be a Uint16`,
    );
    this.hash = hash;
    this.value = value;
  }
}
