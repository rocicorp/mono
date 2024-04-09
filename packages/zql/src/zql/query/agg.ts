type AggregateBase<Field extends string, Alias extends string> = {
  field: Field;
  alias: Alias;
};

export type Aggregate<Field extends string, Alias extends string> =
  | Min<Field, Alias>
  | Max<Field, Alias>
  | Sum<Field, Alias>
  | Count<Alias>
  | Avg<Field, Alias>
  | AggArray<Field, Alias>;

type Min<Field extends string, Alias extends string> = {
  aggregate: 'min';
} & AggregateBase<Field, Alias>;

type Max<Field extends string, Alias extends string> = {
  aggregate: 'max';
} & AggregateBase<Field, Alias>;

type Sum<Field extends string, Alias extends string> = {
  aggregate: 'sum';
} & AggregateBase<Field, Alias>;

type Avg<Field extends string, Alias extends string> = {
  aggregate: 'avg';
} & AggregateBase<Field, Alias>;

export type Count<Alias extends string> = {
  aggregate: 'count';
} & {
  alias: Alias;
};

export type AggArray<Field extends string, Alias extends string> = {
  aggregate: 'array';
} & AggregateBase<Field, Alias>;

export function min<Field extends string>(field: Field): Min<Field, Field>;
export function min<Field extends string, Alias extends string>(
  field: Field,
  alias: Alias,
): Min<Field, Alias>;
export function min<Field extends string, Alias extends string>(
  field: Field,
  alias?: Alias | undefined,
): Min<Field, Alias> {
  return {
    aggregate: 'min',
    field,
    alias: alias ?? (field as unknown as Alias),
  };
}

export function max<Field extends string>(field: Field): Max<Field, Field>;
export function max<Field extends string, Alias extends string>(
  field: Field,
  alias: Alias,
): Max<Field, Alias>;
export function max<Field extends string, Alias extends string>(
  field: Field,
  alias?: Alias | undefined,
): Max<Field, Alias> {
  return {
    aggregate: 'max',
    field,
    alias: alias ?? (field as unknown as Alias),
  };
}

export function sum<Field extends string>(field: Field): Sum<Field, Field>;
export function sum<Field extends string, Alias extends string>(
  field: Field,
  alias: Alias,
): Sum<Field, Alias>;
export function sum<Field extends string, Alias extends string>(
  field: Field,
  alias?: Alias | undefined,
): Sum<Field, Alias> {
  return {
    aggregate: 'sum',
    field,
    alias: alias ?? (field as unknown as Alias),
  };
}

export function count(): Count<'count'>;
export function count<Alias extends string>(alias: Alias): Count<Alias>;
export function count<Alias extends string>(
  alias?: Alias | undefined,
): Count<Alias> {
  return {
    aggregate: 'count',
    alias: alias ?? ('count' as Alias),
  };
}

export function avg<Field extends string>(field: Field): Avg<Field, Field>;
export function avg<Field extends string, Alias extends string>(
  field: Field,
  alias: Alias,
): Avg<Field, Alias>;
export function avg<Field extends string, Alias extends string>(
  field: Field,
  alias?: Alias | undefined,
): Avg<Field, Alias> {
  return {
    aggregate: 'avg',
    field,
    alias: alias ?? (field as unknown as Alias),
  };
}

export function array<Field extends string>(
  field: Field,
): AggArray<Field, Field>;
export function array<Field extends string, Alias extends string>(
  field: Field,
  alias: Alias,
): AggArray<Field, Alias>;
export function array<Field extends string, Alias extends string>(
  field: Field,
  alias?: Alias | undefined,
): AggArray<Field, Alias> {
  return {
    aggregate: 'array',
    field,
    alias: alias ?? (field as unknown as Alias),
  };
}

export function isAggregate<Field extends string, Alias extends string>(
  x: unknown,
): x is Aggregate<Field, Alias> {
  return (
    x !== null &&
    typeof x === 'object' &&
    typeof (x as Record<string, unknown>).aggregate === 'string'
  );
}
