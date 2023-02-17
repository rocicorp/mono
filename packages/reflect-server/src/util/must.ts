export function must<T>(value: T | undefined | null, message?: string): T {
  // eslint-disable-next-line eqeqeq
  if (value == null) {
    throw new Error(message ?? `Unexpected ${value} value`);
  }
  return value;
}
