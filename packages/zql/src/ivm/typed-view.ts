import type {Immutable} from '../../../shared/src/immutable.ts';

export type ResultType = 'unknown' | 'complete';

/**
 * Called when the view changes. The received data should be considered
 * immutable. Caller must not modify it. Passed data is valid until next
 * time listener is called.
 */
export type Listener<T> = (data: Immutable<T>, resultType: ResultType) => void;

export type TypedView<T, TOptions = unknown> = {
  addListener(listener: Listener<T>): () => void;
  destroy(): void;
  updateOptions(options: TOptions): void;
  readonly data: T;
};
