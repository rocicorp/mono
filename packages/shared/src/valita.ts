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
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
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
    // oxlint-disable-next-line typescript/no-non-null-assertion
  )} ${word} ${toDisplay(expected.at(-1)!)}`;
  if (expected.length === 2) {
    return suffix;
  }
  return `${expected.slice(0, -2).map(toDisplay).join(', ')}, ${suffix}`;
}

function getMessage(
  err: v.Err,
  v: unknown,
  schema: v.Type | v.Optional,
  mode: ParseOptionsMode | undefined,
): string {
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
      return schema.name === 'union'
        ? getDeepestUnionParseError(v, schema as v.UnionType, mode ?? 'strict')
        : `Invalid union value${atPath}`;

    case 'custom_error':
      return `${firstIssue.message}${atPath}. Got ${toDisplayAtPath(v, path)}`;
  }
}

type FailedType = {type: v.Type; err: v.Err};

function getDeepestUnionParseError(
  value: unknown,
  schema: v.UnionType,
  mode: ParseOptionsMode,
): string {
  const failures: FailedType[] = [];
  for (const type of schema.options) {
    const r = type.try(value, {mode});
    if (!r.ok) {
      failures.push({type, err: r});
    }
  }
  if (failures.length) {
    // compare the first and second longest-path errors
    failures.sort(pathCmp);
    if (failures.length === 1 || pathCmp(failures[0], failures[1]) < 0) {
      return getMessage(failures[0].err, value, failures[0].type, mode);
    }
  }
  // paths are equivalent
  try {
    const str = JSON.stringify(value);
    return `Invalid union value: ${str}`;
  } catch {
    // fallback if the value could not be stringified
    return `Invalid union value`;
  }
}

// Descending-order comparison of Issue paths.
// * [1, 'a'] sorts before [1]
// * [1] sorts before [0]  (i.e. errors later in the tuple sort before earlier errors)
function pathCmp(a: FailedType, b: FailedType) {
  const aPath = a.err.issues[0].path;
  const bPath = b.err.issues[0].path;
  if (aPath.length !== bPath.length) {
    return bPath.length - aPath.length;
  }
  for (let i = 0; i < aPath.length; i++) {
    if (bPath[i] > aPath[i]) {
      return -1;
    }
    if (bPath[i] < aPath[i]) {
      return 1;
    }
  }
  return 0;
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
  // A Type<T> only yields undefined if T includes it.
  return testOptional(value, schema, mode) as Result<T>;
}

/**
 * Similar to {@link test} but works for AbstractTypes such as Optional.
 * This is for advanced usage. Prefer {@link test} unless you really need
 * to operate directly on an Optional field.
 */
export function testOptional<T>(
  value: unknown,
  schema: v.Type<T> | v.Optional<T>,
  mode?: ParseOptionsMode,
): Result<T | undefined> {
  const res = schema.try(value, mode ? {mode} : undefined);
  if (!res.ok) {
    return {ok: false, error: getMessage(res, value, schema, mode)};
  }
  return res;
}

const STRICT = {mode: 'strict'} as const;

/**
 * Parses `schema` in strict mode, whatever mode the enclosing parse uses. Use
 * it for the parts of a message that a reader must not silently ignore inside
 * a lenient parse.
 *
 * Wrap a field, not a union variant: a union sees the wrapped type as
 * `unknown`, so it can no longer pick the variant by its literal fields.
 */
export function strict<T>(schema: v.Type<T>): v.Type<T> {
  const type = v.unknown().chain(value => schema.try(value, STRICT));
  // A transform serializes as just its name. Show the wrapped schema too, so a
  // change to it still changes the hash in protocol-version.test.ts.
  return Object.assign(type, {strict: schema});
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
): v.Type<readonly v.Infer<T>[]> {
  return v.array(t);
}

export function readonlyRecord<T extends v.Type>(
  t: T,
): v.Type<Readonly<Record<string, v.Infer<T>>>> {
  return v.record(t);
}

const AbstractType = Object.getPrototypeOf(
  Object.getPrototypeOf(v.string().optional()),
).constructor;

export function instanceOfAbstractType<T = unknown>(
  obj: unknown,
): obj is v.Type<T> | v.Optional<T> {
  return obj instanceof AbstractType;
}

// valita does not export TerminalType.
export type TerminalType = Parameters<Parameters<v.Type['_toTerminals']>[0]>[0];

/**
 * Calls `func` for each terminal type that `type` is composed of, looking
 * through unions, optionals and the transforms created by
 * `.optional(() => x)`, `.assert()`, `.map()` and `.chain()`.
 *
 * This is the only use of valita's internal `_toTerminals`. The public API
 * has no way to look inside a transform, so there is no alternative. The
 * `toTerminals` test in valita.test.ts pins the behavior options.ts depends
 * on, so a valita upgrade that changes it fails there instead of misparsing
 * flags.
 */
export function toTerminals(
  type: v.Type | v.Optional,
  func: (t: TerminalType) => void,
): void {
  type._toTerminals(func);
}

type ObjectShape = Record<string, typeof AbstractType>;

/**
 * Similar to `ObjectType.partial()` except it recurses into nested objects.
 * Rest types are not supported.
 */
export function deepPartial<Shape extends ObjectShape>(
  s: v.ObjectType<Shape, undefined>,
) {
  const shape = {} as Record<string, unknown>;
  for (const [key, type] of Object.entries(s.shape)) {
    if (type.name === 'object') {
      shape[key] = deepPartial(
        type as v.ObjectType<ObjectShape, undefined>,
      ).optional();
    } else {
      shape[key] = type.optional();
    }
  }
  return v.object(shape as {[K in keyof Shape]: v.Optional<v.Infer<Shape[K]>>});
}

type Literal = string | number | bigint | boolean;

export function literalUnion<T extends [...Literal[]]>(
  ...literals: T
): v.Type<T[number]> {
  return v.union(...literals.map(v.literal));
}

declare const unrecognized: unique symbol;

/** A value of an open enum that this build doesn't know. Keeps the raw value. */
export type Unrecognized<T extends Literal = string> = T & {
  readonly [unrecognized]: true;
};

/** A tuple whose first element, its tag, is a literal. */
type TaggedTuple = v.TupleType<[v.Type<Literal>, ...v.Type[]]>;

/** A tuple with a tag this build doesn't know, as {@link taggedTupleUnion} parses it. */
export type UnrecognizedTuple = [
  tag: Unrecognized<Literal>,
  ...rest: unknown[],
];

/**
 * A union of tuples tagged by a literal in their first element, such as
 * `['ping', {}]`. Unlike `union`, it picks the tuple by its tag instead of
 * trying each in turn.
 *
 * With `open`, a tuple whose tag it doesn't know, but of the same type as a
 * known tag, parses as is, typed as {@link UnrecognizedTuple}, so a reader can
 * skip kinds a newer writer added. Use `isKnown` to tell them apart:
 * TypeScript can't narrow the union by comparing the tag, because a tag
 * literal is comparable to `Unrecognized`. A closed union is not implemented
 * yet.
 */
export function taggedTupleUnion<T extends TaggedTuple[]>(
  tuples: T,
  options: {readonly open: boolean},
): v.Type<v.Infer<T[number]> | UnrecognizedTuple> & {
  isKnown(
    value: v.Infer<T[number]> | UnrecognizedTuple,
  ): value is v.Infer<T[number]>;
} {
  if (!options.open) {
    throw new Error('A closed taggedTupleUnion is not implemented');
  }
  type Known = v.Infer<T[number]>;
  const byTag = new Map<unknown, v.Type>();
  const tagTypes = new Set<string>();
  for (const tuple of tuples) {
    toTerminals(tuple.prefix[0], t => {
      if (t.name !== 'literal') {
        throw new Error('Expected every tag to be a literal');
      }
      if (byTag.has(t.value)) {
        throw new Error(`Duplicate tag ${toDisplay(t.value)}`);
      }
      byTag.set(t.value, tuple);
      tagTypes.add(typeof t.value);
    });
  }
  const expectedTag = `Expected ${displayList('or', [...tagTypes])}`;
  const type = v
    .unknown()
    .chain((value, options): v.ValitaResult<Known | UnrecognizedTuple> => {
      if (!Array.isArray(value)) {
        return v.err('Expected array');
      }
      const [tag] = value;
      const tuple = byTag.get(tag);
      if (tuple) {
        return tuple.try(value, options) as v.ValitaResult<Known>;
      }
      return tagTypes.has(typeof tag)
        ? v.ok(value as UnrecognizedTuple)
        : v.err({message: expectedTag, path: [0]});
    });
  return Object.assign(type, {
    // A transform serializes as just its name. Show the tuples too, so a
    // change to them still changes the hash in protocol-version.test.ts.
    taggedTupleUnion: tuples,
    isKnown: (value: Known | UnrecognizedTuple): value is Known =>
      byTag.has(value[0]),
  });
}
