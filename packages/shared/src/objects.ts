export function mapValues<T extends Record<string, unknown>, U>(
  input: T,
  mapper: (value: T[keyof T]) => U,
): {[K in keyof T]: U} {
  return mapEntries(input, (k, v) => [k, mapper(v as T[keyof T])]) as {
    [K in keyof T]: U;
  };
}

export function mapEntries<T, U>(
  input: Record<string, T>,
  mapper: (key: string, val: T) => [key: string, val: U],
): Record<string, U> {
  // Direct assignment is faster than Object.fromEntries()
  // https://github.com/rocicorp/mono/pull/3927#issuecomment-2706059475
  const output: Record<string, U> = {};

  // In chrome Object.entries is faster than for-in (13x) or Object.keys (15x)
  // https://gist.github.com/arv/1b4e113724f6a14e2d4742bcc760d1fa
  for (const entry of Object.entries(input)) {
    const mapped = mapper(entry[0], entry[1]);
    output[mapped[0]] = mapped[1];
  }
  return output;
}

export function mapAllEntries<T, U>(
  input: Record<string, T>,
  mapper: (entries: [key: string, val: T][]) => [key: string, val: U][],
): Record<string, U> {
  // Direct assignment is faster than Object.fromEntries()
  // https://github.com/rocicorp/mono/pull/3927#issuecomment-2706059475
  const output: Record<string, U> = {};
  for (const mapped of mapper(Object.entries(input))) {
    output[mapped[0]] = mapped[1];
  }
  return output;
}
