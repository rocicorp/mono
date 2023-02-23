export type IterableUnion<T> = AsyncIterable<T> | Iterable<T>;

type IteratorUnion<T> = AsyncIterator<T> | Iterator<T>;

export function getIterator<T>(it: IterableUnion<T>): IteratorUnion<T> {
  return (
    (it as AsyncIterable<T>)[Symbol.asyncIterator]?.() ||
    (it as Iterable<T>)[Symbol.iterator]()
  );
}
