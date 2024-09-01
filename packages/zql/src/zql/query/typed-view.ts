import {Immutable} from 'shared/src/immutable.js';

export type Listener<T> = (data: Immutable<T>) => void;

export type TypedView<T> = {
  addListener(listener: Listener<T>): () => void;
  destroy(): void;
  hydrate(): void;
  onCommit(): void;
  readonly data: T;
};
