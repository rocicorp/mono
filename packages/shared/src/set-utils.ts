export function equals<T>(a: Set<T>, b: Set<T>) {
  if (a.size !== b.size) {
    return false;
  }
  for (const value of a) {
    if (!b.has(value)) {
      return false;
    }
  }
  return true;
}

export function union<T>(...sets: Set<T>[]): Set<T> {
  const result = new Set<T>();
  for (const set of sets) {
    for (const value of set) {
      result.add(value);
    }
  }
  return result;
}

export function intersection<T>(a: ReadonlySet<T>, b: ReadonlySet<T>): Set<T> {
  const result = new Set<T>();
  if (a.size > b.size) {
    // Optimization: iterate over the smaller Set.
    const swap = a;
    a = b;
    b = swap;
  }
  for (const value of a) {
    if (b.has(value)) {
      result.add(value);
    }
  }
  return result;
}

/**
 * Returns the elements in {@link a} that are not in {@link b}.
 */
export function difference<T>(a: ReadonlySet<T>, b: ReadonlySet<T>): Set<T> {
  const result = new Set<T>();
  for (const value of a) {
    if (!b.has(value)) {
      result.add(value);
    }
  }
  return result;
}

export function symmetricDifference<T>(a: Set<T>, b: Set<T>): Set<T> {
  const result = new Set<T>(a);
  for (const value of b) {
    if (a.has(value)) {
      result.delete(value);
    } else {
      result.add(value);
    }
  }
  return result;
}
