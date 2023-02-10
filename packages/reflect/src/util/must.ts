export function must<T>(
  arg: T | undefined,
  msg = 'Unexpected undefined value',
): T {
  if (arg === undefined) {
    throw new Error(msg);
  }
  return arg;
}
