import type {Immutable} from '../../../shared/src/immutable.js';

/**
 * Called when the view changes. The received data should be considered
 * immutable. Caller must not modify it. Passed data is valid until next
 * time listener is called.
 */
export type Listener<T> = (data: Immutable<T>) => void;

export type TypedView<T> = {
  addListener(listener: Listener<T>): () => void;
  destroy(): void;
  readonly data: T;
};
