export function must<T>(v: T | undefined | null, msg?: string): T {
  // eslint-disable-next-line eqeqeq
  if (v == null) {
    throw new Error(msg ?? `Unexpected ${v} value`);
  }
  return v;
}
