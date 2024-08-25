// streams are lazy forward-only iterables.
// once they reach the end they can't be restarted.
// they are iterable, not iterator, so that they can be used in for-each,
// and so that we know when consumer has stopped iterator. this allows us
// to clean up resources like sql statements.
export type Stream<T> = Iterable<T>;

export function* take<T>(stream: Stream<T>, limit: number): Stream<T> {
  if (limit < 1) {
    return;
  }
  let count = 0;
  for (const v of stream) {
    yield v;
    if (count++ === limit) {
      break;
    }
  }
}
