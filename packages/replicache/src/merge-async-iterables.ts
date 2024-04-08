import {getIterator, IterableUnion} from './iterable-union.js';

/**
 * Merges an iterable on to another iterable.
 *
 * The two iterables need to be ordered and the `compare` function is used to
 * compare two different elements.
 *
 * If two elements are equal (`compare` returns `0`) then the element from the
 * second iterable is picked.
 *
 * This utility function is provided because it is useful when using
 * {@link makeScanResult}. It can be used to merge an in memory pending async
 * iterable on to a persistent async iterable for example.
 */
export async function* mergeAsyncIterables<A, B>(
  iterableBase: IterableUnion<A>,
  iterableOverlay: IterableUnion<B>,
  compare: (a: A, b: B) => number,
): AsyncIterable<A | B> {
  const a = getIterator(iterableBase);
  const b = getIterator(iterableOverlay);

  let iterResultA = await a.next();
  let iterResultB = await b.next();

  while (true) {
    if (iterResultA.done) {
      if (iterResultB.done) {
        break;
      }
      yield iterResultB.value;
      iterResultB = await b.next();
      continue;
    }

    if (iterResultB.done) {
      yield iterResultA.value;
      iterResultA = await a.next();
      continue;
    }

    const ord = compare(iterResultA.value, iterResultB.value);
    if (ord === 0) {
      yield iterResultB.value;
      iterResultA = await a.next();
      iterResultB = await b.next();
    } else if (ord < 0) {
      yield iterResultA.value;
      iterResultA = await a.next();
    } else {
      yield iterResultB.value;
      iterResultB = await b.next();
    }
  }

  // Both done. Call return if defined.
  if (a.return) {
    await a.return();
  }
  if (b.return) {
    await b.return();
  }
}
