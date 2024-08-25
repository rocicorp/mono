export type Listener<T> = (data: Iterable<T>) => void;

export type TypedView<T> = {
  addListener(listener: Listener<T>): void;
  removeListener(listener: Listener<T>): void;
  destroy(): void;
  hydrate(): void;
};
