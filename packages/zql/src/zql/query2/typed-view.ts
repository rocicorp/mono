import {Immutable} from 'shared/src/immutable.js';
import {ResultType} from '../ivm2/array-view.js';

export type Listener<T> = (data: Immutable<T>, resultType: ResultType) => void;

export type TypedView<T> = {
  addListener(listener: Listener<T>): () => void;
  destroy(): void;
  hydrate(): void;
  readonly data: T;
};
