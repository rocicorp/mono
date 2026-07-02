/**
 * The **space of things the generator can vary** (ported from rusty-ivm
 * `rindle-fuzz/src/axes.rs`, design §2/§4/§5):
 *
 * 1. A generator-side **schema graph** read directly from the zql {@link schema}
 *    (tables, columns + types + nullability, primary keys, and relationships with
 *    their child table / cardinality / junction-ness). Unlike the Rust port — which
 *    re-declared the chinook graph because its AST lacked correlation keys — we lower
 *    through the fluent query builder, which already knows the correlation keys, so
 *    this module is a thin read-model and can never drift from the schema.
 * 2. Per-table **value roles** ({@link Roles}) — which column plays the numeric-filter
 *    / text-filter / nullable / order role, with literals tuned to the {@link miniData}
 *    so a generated filter actually partitions the rows (the Rust `RootCols`,
 *    generalized to all 11 tables). These are the one hand-authored piece; they are
 *    pinned against the schema by `axes.test.ts`.
 * 3. The formal **decoration axes** ({@link AXES}) the t-wise covering array (L1)
 *    covers and `coverage.ts` measures: `filter × exists × order × limit`. Each axis
 *    value is self-contained, so the covering array needs no inter-axis constraint
 *    solver — the only realizability gate is per-table (a text filter needs a text
 *    column; an EXISTS needs an outgoing relationship).
 *
 * **Known-inert axes dropped vs the Rust port** (design §8): the Rust `select` axis
 * (mono ZQL has no projection — the AST returns all columns) and the `start` (keyset
 * paging) axis (z2s does not compile `start`, so the Postgres oracle silently ignores
 * it — it cannot validate paging, and every `start` case would diverge spuriously).
 * `start` push/refetch consistency is checked separately by the driver; re-add it here
 * only if z2s gains keyset support.
 */

import {must} from '../../../../shared/src/must.ts';
import type {ValueType} from '../../../../zero-types/src/schema-value.ts';
import type {Schema} from '../../../../zero-types/src/schema.ts';
import {schema as typedSchema} from '../schema.ts';

// The concrete chinook schema, read through the generic `Schema` interface so the
// reader functions can index it by dynamic (string) table/column/relationship names.
const schema: Schema = typedSchema;

// ── the schema graph (read from the zql schema) ──────────────────────────────────────

export type Card = 'one' | 'many';

/** One column of a table. */
export type Col = {
  readonly name: string;
  readonly type: ValueType;
  readonly optional: boolean;
};

/**
 * One outgoing relationship: the child table it reaches, its cardinality, and whether
 * it is a **junction** (a hidden two-hop, e.g. `track.playlists`). The relationship
 * *name* is what the fluent builder lowers (`.related(name)` / `.whereExists(name)`),
 * so we never need the correlation keys here.
 */
export type Rel = {
  readonly name: string;
  readonly child: string;
  readonly card: Card;
  readonly junction: boolean;
};

/** All modeled (client) table names, in schema declaration order. */
export function tables(): string[] {
  return Object.keys(schema.tables);
}

/** The columns of `table`, in declaration order. */
export function columnsOf(table: string): Col[] {
  return Object.entries(schema.tables[table].columns).map(([name, col]) => ({
    name,
    type: col.type,
    optional: !!col.optional,
  }));
}

/** Whether `table` has a column named `name`. */
export function hasColumn(table: string, name: string): boolean {
  return name in schema.tables[table].columns;
}

/** The primary-key column names of `table`, in key order. */
export function pkOf(table: string): readonly string[] {
  return schema.tables[table].primaryKey;
}

/** The outgoing relationships of `table`, in declaration order. */
export function relsOf(table: string): Rel[] {
  const rels = schema.relationships[table] ?? {};
  return Object.entries(rels).map(([name, conns]) => {
    const chain = conns as ReadonlyArray<{
      destSchema: string;
      cardinality: Card;
    }>;
    const last = must(chain.at(-1));
    return {
      name,
      child: last.destSchema,
      // A junction (multi-hop) is always plural; a single hop carries its own card.
      card: chain.length > 1 ? 'many' : chain[0].cardinality,
      junction: chain.length > 1,
    };
  });
}

/** The relationship named `name` on `table`, if declared. */
export function relOf(table: string, name: string): Rel | undefined {
  return relsOf(table).find(r => r.name === name);
}

/** Whether `table` has a text column (so LIKE/ILIKE/NOT LIKE are realizable on it). */
export function hasText(table: string): boolean {
  return rolesOf(table).text !== null;
}

// ── per-table value roles (the non-vacuous-literal tuning, keyed to `miniData`) ───────

/**
 * Per-table value roles — which column to use for each filter/order axis, with
 * literals tuned so the filter partitions the `mini` rows (the Rust `RootCols`, for
 * all 11 tables). Kept in sync with the schema + data by `axes.test.ts`.
 */
export type Roles = {
  /** A numeric column for `= != < … IN` filters. */
  readonly num: string;
  /** A present value near the middle of `num`'s range (so `<`/`>` both partition). */
  readonly numMid: number;
  /** Two present values for an `IN (a, b)` filter. */
  readonly inSet: readonly [number, number];
  /** A text column for LIKE/ILIKE — `null` if the table has none. */
  readonly text: string | null;
  /** A prefix matching some (not all) `text` values, for `LIKE 'prefix%'`. */
  readonly likePrefix: string;
  /** The same prefix in the opposite case, for `ILIKE 'PREFIX%'`. */
  readonly ilikePrefix: string;
  /** A column to probe with `IS NULL` / `IS NOT NULL`. */
  readonly nullable: string;
  /** A column to `orderBy` (chosen to interleave with the PK for a non-trivial sort). */
  readonly orderCol: string;
};

const ROLES: Record<string, Roles> = {
  artist: {
    num: 'id',
    numMid: 2,
    inSet: [1, 3],
    text: 'name',
    likePrefix: 'A',
    ilikePrefix: 'a',
    nullable: 'name',
    orderCol: 'name',
  },
  album: {
    num: 'id',
    numMid: 11,
    inSet: [10, 20],
    text: 'title',
    likePrefix: 'A-',
    ilikePrefix: 'a-',
    nullable: 'title',
    orderCol: 'artistId',
  },
  track: {
    num: 'id',
    numMid: 104,
    inSet: [100, 102],
    text: 'name',
    likePrefix: 't-',
    ilikePrefix: 'T-',
    nullable: 'composer',
    orderCol: 'milliseconds',
  },
  genre: {
    num: 'id',
    numMid: 2,
    inSet: [1, 4],
    text: 'name',
    likePrefix: 'R',
    ilikePrefix: 'r',
    nullable: 'name',
    orderCol: 'name',
  },
  customer: {
    num: 'id',
    numMid: 2,
    inSet: [1, 3],
    text: 'country',
    likePrefix: 'U',
    ilikePrefix: 'u',
    nullable: 'country',
    orderCol: 'country',
  },
  invoice: {
    num: 'total',
    numMid: 7,
    inSet: [5, 10],
    text: null, // billing_* are all null in mini ⇒ no realizable text filter
    likePrefix: '',
    ilikePrefix: '',
    nullable: 'billingCountry',
    orderCol: 'invoiceDate', // ≠ num (total) so a 2-col mixed order is non-degenerate
  },
  invoiceLine: {
    num: 'quantity',
    numMid: 1,
    inSet: [1, 2],
    text: null, // no text column
    likePrefix: '',
    ilikePrefix: '',
    nullable: 'quantity',
    orderCol: 'unitPrice',
  },
  employee: {
    num: 'id',
    numMid: 2,
    inSet: [1, 3],
    text: 'lastName',
    likePrefix: 'A',
    ilikePrefix: 'a',
    nullable: 'reportsTo',
    orderCol: 'reportsTo',
  },
  mediaType: {
    num: 'id',
    numMid: 2,
    inSet: [1, 2],
    text: 'name',
    likePrefix: 'M',
    ilikePrefix: 'm',
    nullable: 'name',
    orderCol: 'name',
  },
  playlist: {
    num: 'id',
    numMid: 1,
    inSet: [1, 2],
    text: 'name',
    likePrefix: 'P',
    ilikePrefix: 'p',
    nullable: 'name',
    orderCol: 'name',
  },
  playlistTrack: {
    num: 'trackId',
    numMid: 101,
    inSet: [100, 102],
    text: null,
    likePrefix: '',
    ilikePrefix: '',
    nullable: 'trackId',
    orderCol: 'playlistId', // ≠ num (trackId)
  },
};

export function rolesOf(table: string): Roles {
  const r = ROLES[table];
  if (!r) {
    throw new Error(`no roles defined for table ${table}`);
  }
  return r;
}

// ── the decoration axes (the t-way covering-array space) ──────────────────────────────

export const FILTER_VALS = [
  'none',
  'eq',
  'ne',
  'lt',
  'le',
  'gt',
  'ge',
  'in',
  'is_null',
  'is_not_null',
  'like',
  'ilike',
  'not_like',
  'and2',
  'or2',
  'and_or',
] as const;
export type FilterVal = (typeof FILTER_VALS)[number];

/** Whether a filter value needs a text column (so it is unrealizable on a textless table). */
export function filterNeedsText(v: FilterVal): boolean {
  return (
    v === 'like' ||
    v === 'ilike' ||
    v === 'not_like' ||
    v === 'and2' ||
    v === 'and_or'
  );
}

/** Whether filter value `v` is realizable on `table` (the only per-table gate). */
export function filterRealizable(table: string, v: FilterVal): boolean {
  return !filterNeedsText(v) || hasText(table);
}

export const EXISTS_VALS = [
  'none',
  'exists_top',
  'exists_and',
  'exists_or',
  'not_exists_top',
  'not_exists_and',
  'not_exists_or',
] as const;
export type ExistsVal = (typeof EXISTS_VALS)[number];

export const ORDER_VALS = ['none', 'asc1', 'desc1', 'mixed2'] as const;
export type OrderVal = (typeof ORDER_VALS)[number];

export const LIMIT_VALS = ['none', 'small', 'large'] as const;
export type LimitVal = (typeof LIMIT_VALS)[number];

/** One formal axis: a name + its ordered value-token domain. */
export type AxisSpec = {
  readonly name: string;
  readonly values: readonly string[];
};

/**
 * The decoration axes (design §2), kept orthogonal so the covering array needs no
 * inter-axis constraint solver: each value is a complete, independently-applicable
 * modification of a collection node.
 */
export const AXES: readonly AxisSpec[] = [
  {name: 'filter', values: FILTER_VALS},
  {name: 'exists', values: EXISTS_VALS},
  {name: 'order', values: ORDER_VALS},
  {name: 'limit', values: LIMIT_VALS},
];

export const N_AXES = AXES.length;

/** Axis index by name (throws on an unknown axis — a static-table programming error). */
export function axisIndex(name: string): number {
  const i = AXES.findIndex(a => a.name === name);
  if (i < 0) {
    throw new Error(`unknown axis ${name}`);
  }
  return i;
}
