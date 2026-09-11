import type {LogContext} from '@rocicorp/logger';
import {h64} from '../../../shared/src/hash.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {
  planQueryBlueprint,
  type FlipBlueprint,
} from '../planner/planner-builder.ts';
import type {ConnectionCostModel} from '../planner/planner-connection.ts';

/**
 * Bumped whenever the planner's search or cost arithmetic changes in a way
 * that can produce different decisions for the same input. Cached decisions
 * from other versions are never read.
 */
export const PLANNER_ALGORITHM_VERSION = 1;

/**
 * A store of planner decisions, owned by the host rather than by zql. The host
 * decides how large the store is, when it is cleared, and what an epoch means.
 */
export interface PlanCacheStore {
  /**
   * Returns the blueprint stored under `key`, computing and storing it when
   * `key` is absent.
   *
   * `canonical` is the full canonical serialization of the planner input that
   * `key` was derived from. `key` embeds only a 64 bit hash of it, so an
   * implementation MUST compare `canonical` against the stored entry and treat
   * a mismatch as a miss.
   *
   * `compute` runs synchronously. If it throws, nothing may be stored under
   * `key`.
   */
  getOrCompute(
    key: string,
    canonical: string,
    compute: () => FlipBlueprint,
  ): FlipBlueprint;
}

/**
 * The plan cache capability a {@link BuilderDelegate} can offer.
 *
 * `epoch` scopes every decision the host puts in `store`. Planner decisions
 * depend on the database's schema, indexes and statistics, none of which zql
 * can observe, so the host must supply a value that changes at least as often
 * as those do.
 */
export type PlanCache = {
  readonly store: PlanCacheStore;
  readonly epoch: string;
};

export type PlanCacheStats = {
  readonly hits: number;
  readonly misses: number;
  /** Lookups where the key matched but the canonical planner input did not. */
  readonly collisions: number;
  readonly evictions: number;
  readonly entries: number;
  readonly bytes: number;
};

/**
 * Serializes `value` with object keys in a fixed order and array order
 * preserved.
 *
 * Key order must not matter: two ASTs that differ only in property insertion
 * order describe the same query. Array order must matter: the planner numbers
 * correlated subqueries by their position in the condition tree, so reordering
 * conditions changes which decision belongs to which join.
 */
function canonicalJSON(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value ?? null);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJSON).join(',')}]`;
  }
  const parts: string[] = [];
  for (const key of Object.keys(value).sort()) {
    const v = (value as Record<string, unknown>)[key];
    if (v !== undefined) {
      parts.push(`${JSON.stringify(key)}:${canonicalJSON(v)}`);
    }
  }
  return `{${parts.join(',')}}`;
}

/**
 * The canonical identity of a planner input.
 *
 * This is the exact AST handed to the planner, after name mapping and ordering
 * completion, so it already subsumes auth literals, resolved scalar
 * subqueries, and the primary keys the client schema contributes to ordering.
 */
export function canonicalizePlannerInput(ast: AST): string {
  return canonicalJSON(ast);
}

/**
 * Plans `ast` through `cache`, reusing an earlier identical query's decisions
 * when the cache has them.
 */
export function planWithCache(
  cache: PlanCache,
  ast: AST,
  model: ConnectionCostModel,
  lc?: LogContext,
): FlipBlueprint {
  const canonical = canonicalizePlannerInput(ast);
  const key = `${PLANNER_ALGORITHM_VERSION}/${cache.epoch}/${h64(canonical).toString(36)}`;
  return cache.store.getOrCompute(key, canonical, () =>
    planQueryBlueprint(ast, model, undefined, lc),
  );
}

type Entry = {
  readonly canonical: string;
  readonly blueprint: FlipBlueprint;
  readonly bytes: number;
};

/**
 * Rough retained size of an entry. UTF-16 string storage dominates; the
 * blueprint itself is a few small arrays and objects.
 */
function sizeOf(
  key: string,
  canonical: string,
  blueprint: FlipBlueprint,
): number {
  let size = 2 * (key.length + canonical.length) + 64;
  const visit = (b: FlipBlueprint) => {
    size += 32 + 8 * b.flips.length;
    for (const [alias, sub] of Object.entries(b.related)) {
      size += 2 * alias.length + 16;
      visit(sub);
    }
  };
  visit(blueprint);
  return size;
}

/**
 * An LRU {@link PlanCacheStore} bounded by both entry count and estimated
 * bytes, so that a workload with high cardinality keys (arbitrary query
 * arguments, per-user auth literals) cannot grow it without limit.
 */
export class BoundedPlanCache implements PlanCacheStore {
  readonly #maxEntries: number;
  readonly #maxBytes: number;
  // Map iteration order is insertion order, so the first key is the least
  // recently used.
  readonly #entries = new Map<string, Entry>();
  #bytes = 0;
  #hits = 0;
  #misses = 0;
  #collisions = 0;
  #evictions = 0;

  constructor(maxEntries: number, maxBytes: number) {
    this.#maxEntries = maxEntries;
    this.#maxBytes = maxBytes;
  }

  getOrCompute(
    key: string,
    canonical: string,
    compute: () => FlipBlueprint,
  ): FlipBlueprint {
    const existing = this.#entries.get(key);
    if (existing) {
      if (existing.canonical === canonical) {
        this.#hits++;
        this.#entries.delete(key);
        this.#entries.set(key, existing);
        return existing.blueprint;
      }
      this.#collisions++;
      this.#drop(key, existing);
    } else {
      this.#misses++;
    }

    // Nothing is stored if this throws, so a failed plan cannot poison the key.
    const blueprint = compute();

    const bytes = sizeOf(key, canonical, blueprint);
    if (bytes <= this.#maxBytes) {
      this.#entries.set(key, {canonical, blueprint, bytes});
      this.#bytes += bytes;
      this.#evict();
    }
    return blueprint;
  }

  #evict(): void {
    while (
      this.#entries.size > this.#maxEntries ||
      this.#bytes > this.#maxBytes
    ) {
      const [lruKey, lru] = this.#entries.entries().next().value!;
      this.#drop(lruKey, lru);
      this.#evictions++;
    }
  }

  #drop(key: string, entry: Entry): void {
    this.#entries.delete(key);
    this.#bytes -= entry.bytes;
  }

  clear(): void {
    this.#entries.clear();
    this.#bytes = 0;
  }

  stats(): PlanCacheStats {
    return {
      hits: this.#hits,
      misses: this.#misses,
      collisions: this.#collisions,
      evictions: this.#evictions,
      entries: this.#entries.size,
      bytes: this.#bytes,
    };
  }
}
