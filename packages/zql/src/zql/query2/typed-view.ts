import {DeepReadonly} from 'replicache';

export type Listener<T> = (data: DeepReadonly<T>) => void;

export type TypedView<T> = {
  addListener(listener: Listener<T>): () => void;
  destroy(): void;
  hydrate(): void;
  readonly data: T;
};
