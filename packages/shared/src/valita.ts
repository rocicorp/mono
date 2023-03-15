import type * as v from '@badrap/valita';

export * from '@badrap/valita';

function toDisplay(v: unknown): string {
  switch (typeof v) {
    case 'string':
    case 'number':
    case 'boolean':
      return JSON.stringify(v);
    case 'undefined':
      return 'undefined';
    case 'bigint':
      return v.toString() + 'n';
    default:
      if (v === null) {
        return 'null';
      }
      return typeof v;
  }
}

type Key = string | number;

function toDisplayAtPath(v: unknown, path: Key[] | undefined): string {
  if (!path?.length) {
    return toDisplay(v);
  }

  let cur = v;
  for (const p of path) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    cur = (cur as any)[p];
  }
  return toDisplay(cur);
}

function displayList<T>(
  word: string,
  expected: T[],
  toDisplay: (x: T) => string | number = x => String(x),
): string | number {
  if (expected.length === 1) {
    return toDisplay(expected[0]);
  }

  const suffix = `${toDisplay(
    expected[expected.length - 2],
  )} ${word} ${toDisplay(expected[expected.length - 1])}`;
  if (expected.length === 2) {
    return suffix;
  }
  return `${expected.slice(0, -2).map(toDisplay).join(', ')}, ${suffix}`;
}

function getMessage(err: v.Err, v: unknown): string {
  const firstIssue = err.issues[0];
  const {path} = firstIssue;
  const atPath = path?.length ? ` at ${path.join('.')}` : '';

  switch (firstIssue.code) {
    case 'invalid_type':
      return `Expected ${displayList(
        'or',
        firstIssue.expected,
      )}${atPath}. Got ${toDisplayAtPath(v, path)}`;
    case 'missing_value': {
      const atPath =
        path && path.length > 1 ? ` at ${path.slice(0, -1).join('.')}` : '';

      if (firstIssue.path?.length) {
        return `Missing property ${firstIssue.path.at(-1)}${atPath}`;
      }
      return `TODO Unknown missing property${atPath}`;
    }

    case 'invalid_literal':
      return `Expected literal value ${displayList(
        'or',
        firstIssue.expected,
        toDisplay,
      )}${atPath} Got ${toDisplayAtPath(v, path)}`;

    case 'invalid_length': {
      return `Expected array with length ${
        firstIssue.minLength === firstIssue.maxLength
          ? firstIssue.minLength
          : `between ${firstIssue.minLength} and ${firstIssue.maxLength}`
      }${atPath}. Got array with length ${(v as {length: number}).length}`;
    }

    case 'unrecognized_keys':
      if (firstIssue.keys.length === 1) {
        return `Unexpected property ${firstIssue.keys[0]}${atPath}`;
      }
      return `Unexpected properties ${displayList(
        'and',
        firstIssue.keys,
      )}${atPath}`;

    case 'invalid_union':
      return `Invalid union value${atPath}`;

    case 'custom_error': {
      const {error} = firstIssue;
      return (
        (typeof error === 'string'
          ? error
          : typeof error === 'undefined'
          ? 'unknown'
          : error.message ?? 'unknown') + atPath
      );
    }
  }
}

export function parse<T>(v: unknown, s: Type<T>): T {
  const res = test(v, s);
  if (!res.ok) {
    throw new TypeError(res.error);
  }
  return res.value;
}

export function is<T>(v: unknown, s: Type<T>): v is T {
  return (s as v.Type<T>).try(v).ok;
}

export function assert<T>(v: unknown, s: Type<T>): asserts v is T {
  parse(v, s);
}

type Result<T> = {ok: true; value: T} | {ok: false; error: string};

export function test<T>(v: unknown, s: Type<T>): Result<T> {
  const res = (s as v.Type<T>).try(v);
  if (!res.ok) {
    return {ok: false, error: getMessage(res, v)};
  }
  return res;
}

// We re-export the valita type `Type` but we only allow the `optional`
// property. This is to prevent calling `.parse` on it which would not use our
// formatting of the error message.
export type Type<T> = Omit<
  v.Type<T>,
  'parse' | 'try' | 'assert' | 'map' | 'chain'
>;

// Re-export the valita type `Type` using a longer less convenient name because
// we do need it in one place.
// TODO(arv): Remove this.
export type ValitaType<T> = v.Type<T>;
