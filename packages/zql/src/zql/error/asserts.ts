export class InvariantViolation extends Error {}

export function assert(b: unknown, msg = 'Assertion failed'): asserts b {
  if (!b) {
    throw new InvariantViolation(msg);
  }
}

export function invariant(p: boolean, msg: string) {
  if (!p) {
    throw new InvariantViolation(msg);
  }
}

export function must<T>(v: T | undefined | null, msg?: string): T {
  // eslint-disable-next-line eqeqeq
  if (v == null) {
    throw new InvariantViolation(msg ?? `Unexpected ${v} value`);
  }
  return v;
}

export function unreachable(): never {
  throw new InvariantViolation('Unreachable');
}
