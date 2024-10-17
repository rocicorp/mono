import type {Immutable} from '../../../../shared/src/immutable.js';
import type {ResultType} from '../ivm/array-view.js';

export type Listener<T> = (
  data: Immutable<T>,
  details: {resultType: ResultType},
) => void;

export type TypedView<T> = {
  addListener(listener: Listener<T>): () => void;
  destroy(): void;
  hydrate(): void;
  onCommit(): void;
  readonly data: T;
};
