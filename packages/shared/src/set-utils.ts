// TODO: Change lib to pick up this type definition

interface ReadonlySetLike<T> {
  /**
   * Despite its name, returns an iterator of the values in the set-like.
   */
  keys(): Iterator<T>;
  /**
   * @returns a boolean indicating whether an element with the specified value exists in the set-like or not.
   */
  has(value: T): boolean;
  /**
   * @returns the number of (unique) elements in the set-like.
   */
  readonly size: number;
}

export function equals<T>(
  a: ReadonlySetLike<T>,
  b: ReadonlySetLike<T>,
): boolean {
  if (a.size !== b.size) {
    return false;
  }
  const iterator = a.keys();
  for (
    let {done, value} = iterator.next();
    !done;
    {done, value} = iterator.next()
  ) {
    if (!b.has(value)) {
      return false;
    }
  }
  return true;
}

export function union<T>(...sets: ReadonlySetLike<T>[]): Set<T> {
  const result = new Set<T>();
  for (const set of sets) {
    const iterator = set.keys();
    for (
      let {done, value} = iterator.next();
      !done;
      {done, value} = iterator.next()
    ) {
      result.add(value);
    }
  }
  return result;
}

export function intersection<T>(
  a: ReadonlySetLike<T>,
  b: ReadonlySetLike<T>,
): Set<T> {
  const result = new Set<T>();
  if (a.size > b.size) {
    // Optimization: iterate over the smaller Set.
    const swap = a;
    a = b;
    b = swap;
  }
  const iterator = a.keys();
  for (
    let {done, value} = iterator.next();
    !done;
    {done, value} = iterator.next()
  ) {
    if (b.has(value)) {
      result.add(value);
    }
  }
  return result;
}

/**
 * Returns the elements in {@link a} that are not in {@link b}.
 */
export function difference<T>(
  a: ReadonlySetLike<T>,
  b: ReadonlySetLike<T>,
): Set<T> {
  const result = new Set<T>();
  const iterator = a.keys();
  for (
    let {done, value} = iterator.next();
    !done;
    {done, value} = iterator.next()
  ) {
    if (!b.has(value)) {
      result.add(value);
    }
  }
  return result;
}

export function symmetricDifferences<T>(
  a: ReadonlySetLike<T>,
  b: ReadonlySetLike<T>,
): [onlyA: Set<T>, onlyB: Set<T>] {
  const onlyA = new Set<T>();
  const iteratorA = a.keys();
  for (
    let {done, value} = iteratorA.next();
    !done;
    {done, value} = iteratorA.next()
  ) {
    onlyA.add(value);
  }

  const onlyB = new Set<T>();
  const iteratorB = b.keys();
  for (
    let {done, value} = iteratorB.next();
    !done;
    {done, value} = iteratorB.next()
  ) {
    if (a.has(value)) {
      onlyA.delete(value);
      onlyB.delete(value);
    } else {
      onlyB.add(value);
    }
  }
  return [onlyA, onlyB];
}
