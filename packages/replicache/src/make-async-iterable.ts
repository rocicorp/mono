export async function* makeAsyncIterable<V>(
  values: Iterable<V>,
): AsyncIterable<V> {
  for (const value of values) {
    yield value;
  }
}
