import * as v from '@badrap/valita';

export * from '@badrap/valita';

function toDisplay(value: unknown): string {
  switch (typeof value) {
    case 'string':
    case 'number':
    case 'boolean':
      return JSON.stringify(value);
    case 'undefined':
      return 'undefined';
    case 'bigint':
      return value.toString() + 'n';
    default:
      if (value === null) {
        return 'null';
      }
      if (Array.isArray(value)) {
        return 'array';
      }
      return typeof value;
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
      const message = !error
        ? 'unknown'
        : typeof error === 'string'
        ? error
        : error.message ?? 'unknown';
      return `${message}${atPath}. Got ${toDisplayAtPath(v, path)}`;
    }
  }
}

/**
 * 'strip' allows unknown properties and removes unknown properties.
 * 'strict' errors if there are unknown properties.
 * 'passthrough' allows unknown properties.
 */
export type ParseOptionsMode = 'passthrough' | 'strict' | 'strip';

export function parse<T>(
  value: unknown,
  schema: v.Type<T>,
  mode?: ParseOptionsMode,
): T {
  const res = test(value, schema, mode);
  if (!res.ok) {
    throw new TypeError(res.error);
  }
  return res.value;
}

export function is<T>(
  value: unknown,
  schema: v.Type<T>,
  mode?: ParseOptionsMode,
): value is T {
  return test(value, schema, mode).ok;
}

export function assert<T>(
  value: unknown,
  schema: v.Type<T>,
  mode?: ParseOptionsMode,
): asserts value is T {
  parse(value, schema, mode);
}

type Result<T> = {ok: true; value: T} | {ok: false; error: string};

export function test<T>(
  value: unknown,
  schema: v.Type<T>,
  mode?: ParseOptionsMode,
): Result<T> {
  const res = (schema as v.Type<T>).try(value, mode ? {mode} : undefined);
  if (!res.ok) {
    return {ok: false, error: getMessage(res, value)};
  }
  return res;
}

/**
 * Shallowly marks the schema as readonly.
 */
export function readonly<T extends v.Type>(t: T): v.Type<Readonly<v.Infer<T>>> {
  return t as v.Type<Readonly<v.Infer<T>>>;
}

export function readonlyObject<T extends Record<string, v.Type | v.Optional>>(
  t: T,
): v.ObjectType<Readonly<T>, undefined> {
  return v.object(t);
}

export function readonlyArray<T extends v.Type>(
  t: T,
): v.Type<
  readonly [...Readonly<T extends v.Type<unknown> ? v.Infer<T>[] : []>]
> {
  return v.array(t);
}

export function readonlyRecord<T extends v.Type>(
  t: T,
): v.Type<Readonly<Record<string, v.Infer<T>>>> {
  return v.record(t);
}
