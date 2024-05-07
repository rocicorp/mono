import type {
  Condition,
  Primitive,
  SimpleCondition,
} from '@rocicorp/zql/src/zql/ast/ast.js';
import {compareUTF8} from 'compare-utf8';
import {defined} from 'shared/src/arrays.js';
import {BigIntJSON} from '../types/bigint-json.js';
import {
  NormalizedInvalidationFilterSpec,
  invalidationHash,
  normalizeFilterSpec,
  type InvalidationFilterSpec,
  type RowTag,
} from '../types/invalidation.js';
import type {Normalized} from './normalize.js';

export type InvalidationInfo = {
  readonly filters: readonly NormalizedInvalidationFilterSpec[];
  readonly hashes: readonly string[];
};

/** Computes the InvalidationInfo (filters and hashes) for the `normalized` AST. */
export function computeInvalidationInfo(
  normalized: Normalized,
): InvalidationInfo {
  const {schema = 'public', table, select, aggregate, where} = normalized.ast();

  const selected = new Set<string>([
    ...(select ?? []).map(([col]) => col),
    ...(aggregate ?? []).map(agg => agg.field ?? '*'),
  ]);
  const selectedColumns: readonly string[] | undefined = selected.has('*')
    ? undefined
    : [...selected]
        .map(col => {
          const parts = col.split('.');
          if (parts.length >= 3 && parts.at(-3) !== schema) {
            return ''; // not a column of this table. filtered in next step.
          }
          if (parts.length >= 2 && parts.at(-2) !== table) {
            return ''; // not a column of this table. filtered in next step.
          }
          return parts.at(-1) ?? col;
        })
        .filter(col => col.length)
        .sort(compareUTF8);

  const hashes = new Set<string>();
  const filters = new Map<string, NormalizedInvalidationFilterSpec>();

  computeMatchers(where).forEach(matcher =>
    matcher.addInvalidationInfo(
      {schema, table, selectedColumns},
      hashes,
      filters,
    ),
  );

  if (filters.size) {
    // All queries (except impossibilities) are invalidated by full table invalidation.
    // The Replicator automatically produces this hash for `TRUNCATE` operations;
    // no filter registration is required.
    hashes.add(invalidationHash({schema: 'public', table, allRows: true}));
  }

  return {filters: [...filters.values()], hashes: [...hashes]};
}

/**
 * ## Basics
 *
 * Invalidation filters operate over instances of the equality (`=`) {@link Condition}.
 * (TODO: Add support for the `IN` operatior well, as it is expressible as an `OR` of
 *  `=` expressions.)
 *
 * A simple equality expression:
 *
 * ```
 * WHERE a = 1
 * ```
 *
 * creates a Matcher:
 * * `{a: 1}`
 *
 * From which both an {@link InvalidationFilterSpec} (e.g. for the `a` column)
 * and invalidation hash for the corresponding value (e.g. `1`) are derived.
 * The former is registered with the Replicator such that it is applied to incoming
 * changes, while the latter is checked by the View Syncer to determine whether
 * the query has been invalidated since its version.
 *
 * Note that each distinct Matcher produces its own invalidation hash, but
 * Matchers with the same keys produce the same filter spec. For example, the
 * set of Matchers `[{a: 1}, {a: 2}, {b: 3}]` will produce three hashes but
 * only two filter specs (one on column `a` and one on column `b`).
 *
 * ## Single-level Conjunctions
 *
 * `OR` conjunctions create separate Matchers, from which any match
 * indicates that the query should be invalidated:
 *
 * ```
 * WHERE a = 1 OR b = 2 OR c = 3
 * ```
 *
 * creates Matchers
 * * `{a: 1}`
 * * `{b: 2}`
 * * `{c: 3}`.
 *
 * `AND` conjunctions create a single Matcher that "concatenates"
 *  all of sub expressions:
 *
 * ```
 * WHERE a = 1 AND b = 2 AND c = 3
 * ```
 *
 * creates a single Matcher:
 * * `{a: 1, b: 2, c: 3}`
 *
 * ## Generalizing to Nested Conjunctions
 *
 * Conjunctions can form multi-level trees. Fortunately, AST normalization
 * simplifies the problem space such that nestings of like operators
 * (e.g. a branch of `AND`s) are flattened to a single Conjunction, so the
 * only types of nesting possible are those with disparate operators,
 * i.e. an `AND` may contain `OR` children and vice versa.
 *
 * The logic for single-level conjunctions can be generalized such that
 * the `AND` and `OR` functions handle any level of nesting.
 *
 * An `OR` of arbitrary Conditions is straightforward: Matchers returned from
 * each child are returned as is, i.e. as separate Matchers:
 *
 * ```
 * WHERE (a = 1 OR (b = 2 AND c = 3 AND d = 4) OR (e = 5 AND f = 6))
 * ```
 *
 * creates separate Matchers for each child condition, similar to
 * what happens for single-level `OR`s:
 * * `{a: 1}`
 * * `{b: 2, c: 3, d: 4}`
 * * `{e: 5, f: 6}`
 *
 * An `AND` of arbitrary Conditions is more interesting. Consider the example:
 *
 * ```
 * WHERE (a = 1 AND (b = 2 OR c = 3 OR d = 4) AND (e = 5 OR f = 6))
 * ```
 *
 * Note that each sub-expression produces a group of Matchers:
 * * [`{a = 1}`]
 * * [`{b = 2}`, `{c = 3}`, `{d = 4}`]
 * * [`{e = 5}`, `{f = 6}`]
 *
 * The requirement for matching the parent `AND` expression statement is
 * at least one condition in each of the sub-expressions be matched.
 * Each possibility is expressed by the concatenating one Match from
 * each group. The space of all possibilities can be computed by
 * accumulating pairwise concatenations between groups.
 *
 * Expressed mathematically, if each group is thought of as a vector,
 * and concatenation thought of as multiplication, the computation is the
 * cumulative outer product of all sub-expression vectors.
 *
 * The above examples results in the following Matchers:
 * * `{a = 1, b = 2, e = 5}`
 * * `{a = 1, c = 3, e = 5}`
 * * `{a = 1, d = 4, e = 5}`
 * * `{a = 1, b = 2, f = 6}`
 * * `{a = 1, c = 3, f = 6}`
 * * `{a = 1, d = 4, f = 6}`
 *
 * ## Result Normalization
 *
 * Although the result of a cumulative outer product technically grows exponentially
 * with the number of subgroups, many results can be normalized away:
 *
 * ### Impossibilities
 *
 * Concatenating equality conditions for the same column with
 * different values are pruned since they will never match. For example, in the
 * expression:
 *
 * ```
 * (a = 1 OR b = 2) AND (a = 3 OR b = 4)
 * ```
 *
 * the products `(a = 1 AND a = 3)` and `(b = 2 AND b = 4)` are impossibilities and
 * can thus be discarded, leaving only `(a = 1 AND b = 4)` and `(a = 3 AND b = 2)`.
 *
 * ### Subsumption
 *
 * At each level, Matchers that are subsumed by a sibling
 * (i.e. they only match when the sibling matches) are discarded. For example, if
 * a result contains:
 * * `{a = 1, b = 2}`
 * * `{a = 1, b = 2, c = 3}`
 *
 * the latter can be discarded because the former will always match whenever the
 * latter does.
 *
 * ## Fail safes
 *
 * Nevertheless, certain fail-safes should be in place to protect against
 * "query of death" situations that could otherwise blow up memory usage:
 *
 * * Max depth: At a certain depth of traversal, just return the empty
 *   match-all Matcher.
 *
 * * TODO: Consider limiting the total number of filter specs produced.
 *   This is less critical because the total number is already limited to
 *   the size of the power set of columns. However, filter specs are run
 *   on the critical path, so this is worth revisiting.
 */
export function computeMatchers(
  cond: Condition | undefined,
  maxDepth = 10,
  depth = 0,
): Matcher[] {
  if (cond === undefined || cond.type === 'simple') {
    return [new Matcher(cond)];
  }
  if (depth >= maxDepth) {
    console.warn(`Max depth reached while computing invalidation filters`);
    // Since subtrees are ignored, they must be represented by a "match anything"
    // Matcher, which will hopefully be narrowed / concatenated with a more
    // discerning Matcher from the parent.
    return [new Matcher()];
  }
  const matchers =
    cond.op === 'OR'
      ? // An OR is the list of independent Matchers for each sub-condition.
        cond.conditions.flatMap(c => computeMatchers(c, maxDepth, depth + 1))
      : // An AND is the cumulative outer product (concatenation) of the groups
        // of Matchers produced by sub-conditions.
        cond.conditions
          .map(c => computeMatchers(c, maxDepth, depth + 1))
          .reduce((acc, group) => outerProduct(acc, group), [new Matcher()]);
  return removeRedundant(matchers);
}

class Matcher {
  readonly #match = new Map<string, Primitive>();

  constructor(cond?: SimpleCondition) {
    if (cond?.op === '=' && !Array.isArray(cond.value.value)) {
      this.#match.set(cond.field, cond.value.value);
    }
    // For all other simple operators, or the absence of a condition,
    // the empty set matches any change to the table.
  }

  /**
   * Returns a new Matchers object that is the concatenation of `this` and `other`,
   * or `undefined` if the concatenation results in an impossible combination
   * of conditions (e.g. `a = 1 AND a = 2`).
   */
  concat(other: Matcher): Matcher | undefined {
    return new Matcher().#addAllFrom(this, other);
  }

  #addAllFrom(...matchers: Matcher[]): Matcher | undefined {
    for (const m of matchers) {
      for (const [k, v] of m.#match.entries()) {
        const exists = this.#match.get(k);
        if (exists !== undefined && exists !== v) {
          return undefined; // impossible to satisfy different v's for the same k.
        }
        this.#match.set(k, v);
      }
    }
    return this;
  }

  /**
   * @returns Whether this Matcher subsumes the `other`, meaning that this
   *    Matcher will necessarily match whenever the `other` does, obviating
   *    the `other`. Technically, this means that `this` set of Matches is a
   *    subset of the `other` set, with the base case being an empty set, which
   *    subsumes (i.e. matches) everything.
   */
  subsumes(other: Matcher): boolean {
    if (this.#match.size > other.#match.size) {
      // `this` cannot be a subset of `other` if it has more Matches.
      return false;
    }
    for (const [k, v] of this.#match.entries()) {
      if (other.#match.get(k) !== v) {
        return false;
      }
    }
    return true;
  }

  /** Adds the invalidation info for this Matcher to the given `hashes` and `filters`. */
  addInvalidationInfo(
    base: {
      schema: string;
      table: string;
      selectedColumns: readonly string[] | undefined;
    },
    hashes: Set<string>,
    filters: Map<string, NormalizedInvalidationFilterSpec>,
  ) {
    const filteredColumns = [...this.#match.keys()].sort(compareUTF8);
    const filterSpec: InvalidationFilterSpec = {
      ...base,
      filteredColumns: Object.fromEntries(
        filteredColumns.map(col => [col, '=']),
      ),
    };
    // Note: Even though bigints will not appear in ASTs, BigIntJSON.stringify() is used
    //       here so that the hashing behavior matches exactly what is done in the Replicator.
    //       @see InvalidationProcessor.#computeInvalidationHashes()
    const rowTag: RowTag = {
      ...base,
      filteredColumns: Object.fromEntries(
        filteredColumns.map(col => [
          col,
          BigIntJSON.stringify(this.#match.get(col)),
        ]),
      ),
    };

    hashes.add(invalidationHash(rowTag));
    const normalizedSpec = normalizeFilterSpec(filterSpec);
    filters.set(normalizedSpec.id, normalizedSpec);
  }

  /** For testing convenience. */
  getMatch(): Record<string, Primitive> {
    return Object.fromEntries(this.#match.entries());
  }
}

/**
 * Returns the set of Matchers resulting from concatenating every Matcher in `m1`
 * with every Matcher in `m2`, filtering out concatenations which never match anything.
 * This is used for processing an AND conjunction.
 */
function outerProduct(m1: Matcher[], m2: Matcher[]): Matcher[] {
  const product = defined(m1.flatMap(a => m2.flatMap(b => a.concat(b))));
  return removeRedundant(product);
}

/**
 * Removes any Matcher that is subsumed by another Matcher. Note that the
 * input array `m` is modified, and returned for convenience.
 */
function removeRedundant(m: Matcher[]): Matcher[] {
  for (let i = 0; i < m.length; i++) {
    for (let j = i + 1; j < m.length; j++) {
      if (m[i].subsumes(m[j])) {
        m.splice(j, 1);
        j--;
      } else if (m[j].subsumes(m[i])) {
        m.splice(i, 1);
        i--;
        break; // if i is affected, restart the j loop
      }
    }
  }
  return m;
}
