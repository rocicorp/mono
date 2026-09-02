import {expect, suite, test, vi} from 'vitest';
import type {AST, Conjunction} from '../../../zero-protocol/src/ast.ts';
import {
  applyFlipBlueprint,
  planQuery,
  planQueryBlueprint,
  type FlipBlueprint,
} from '../planner/planner-builder.ts';
import {simpleCostModel} from '../planner/test/helpers.ts';
import {builder} from '../planner/test/test-schema.ts';
import {asQueryInternals} from '../query/query-internals.ts';
import type {AnyQuery} from '../query/query.ts';
import {
  BoundedPlanCache,
  canonicalizePlannerInput,
  planWithCache,
  PLANNER_ALGORITHM_VERSION,
  type PlanCache,
} from './plan-cache.ts';

function getAST(q: AnyQuery): AST {
  return asQueryInternals(q).ast;
}

const trackerAST = getAST(
  builder.users
    .whereExists('posts', q => q.whereExists('comments'))
    .whereExists('likes')
    .related('comments', q => q.whereExists('post')),
);

function blueprintOf(ast: AST): FlipBlueprint {
  return planQueryBlueprint(ast, simpleCostModel);
}

suite('canonicalizePlannerInput', () => {
  test('ignores property order but not condition order', () => {
    const a: AST = {
      table: 'users',
      orderBy: [['id', 'asc']],
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            left: {type: 'column', name: 'a'},
            op: '=',
            right: {type: 'literal', value: 1},
          },
          {
            type: 'simple',
            left: {type: 'column', name: 'b'},
            op: '=',
            right: {type: 'literal', value: 2},
          },
        ],
      },
    };
    const reordered: AST = {
      where: a.where,
      orderBy: a.orderBy,
      table: a.table,
    };
    const conjunction = a.where as Conjunction;
    const swapped: AST = {
      ...a,
      where: {type: 'and', conditions: conjunction.conditions.toReversed()},
    };

    expect(canonicalizePlannerInput(reordered)).toBe(
      canonicalizePlannerInput(a),
    );
    expect(canonicalizePlannerInput(swapped)).not.toBe(
      canonicalizePlannerInput(a),
    );
  });

  test('distinguishes scalar literal substitutions', () => {
    const withOne = getAST(builder.users.where('id', 1));
    const withTwo = getAST(builder.users.where('id', 2));
    expect(canonicalizePlannerInput(withOne)).not.toBe(
      canonicalizePlannerInput(withTwo),
    );
  });

  test('distinguishes client-schema ordering completion', () => {
    const byId: AST = {...trackerAST, orderBy: [['id', 'asc']]};
    const byStatusThenId: AST = {
      ...trackerAST,
      orderBy: [
        ['status', 'asc'],
        ['id', 'asc'],
      ],
    };
    expect(canonicalizePlannerInput(byId)).not.toBe(
      canonicalizePlannerInput(byStatusThenId),
    );
  });

  test('treats an absent field and an undefined field alike', () => {
    expect(canonicalizePlannerInput({table: 'users', limit: undefined})).toBe(
      canonicalizePlannerInput({table: 'users'}),
    );
  });
});

suite('planWithCache', () => {
  function cacheOf(epoch = 'v1', maxEntries = 8, maxBytes = 1 << 20) {
    const store = new BoundedPlanCache(maxEntries, maxBytes);
    return {store, cache: {store, epoch} satisfies PlanCache};
  }

  test('a hit returns the same decisions as a miss, without replanning', () => {
    const {store, cache} = cacheOf();
    const model = vi.fn(simpleCostModel);

    const first = planWithCache(cache, trackerAST, model);
    const callsAfterMiss = model.mock.calls.length;
    const second = planWithCache(cache, trackerAST, model);

    expect(callsAfterMiss).toBeGreaterThan(0);
    expect(model.mock.calls.length).toBe(callsAfterMiss);
    expect(second).toBe(first);
    expect(second).toEqual(blueprintOf(trackerAST));
    expect(store.stats()).toMatchObject({hits: 1, misses: 1, entries: 1});
  });

  test('a hit stamps the same AST as planning from scratch', () => {
    const {cache} = cacheOf();
    planWithCache(cache, trackerAST, simpleCostModel);

    // A structurally identical but distinct AST, as a second client group
    // would produce.
    const twin = structuredClone(trackerAST) as AST;
    const cached = applyFlipBlueprint(
      twin,
      planWithCache(cache, twin, simpleCostModel),
    );

    expect(cached).toEqual(planQuery(twin, simpleCostModel));
    expect(canonicalizePlannerInput(twin)).toBe(
      canonicalizePlannerInput(trackerAST),
    );
  });

  test('a different epoch misses', () => {
    const store = new BoundedPlanCache(8, 1 << 20);
    planWithCache({store, epoch: 'v1'}, trackerAST, simpleCostModel);
    planWithCache({store, epoch: 'v2'}, trackerAST, simpleCostModel);

    expect(store.stats()).toMatchObject({hits: 0, misses: 2, entries: 2});
  });

  test('the key namespaces the planner algorithm version', () => {
    const store = new BoundedPlanCache(8, 1 << 20);
    const seen: string[] = [];
    planWithCache(
      {
        store: {
          getOrCompute: (key, canonical, compute) => {
            seen.push(key);
            return store.getOrCompute(key, canonical, compute);
          },
        },
        epoch: 'v1',
      },
      trackerAST,
      simpleCostModel,
    );

    expect(seen[0].startsWith(`${PLANNER_ALGORITHM_VERSION}/v1/`)).toBe(true);
  });

  test('a planning failure does not poison the key', () => {
    const {store, cache} = cacheOf();
    const boom = () => {
      throw new Error('cost model unavailable');
    };

    expect(() => planWithCache(cache, trackerAST, boom)).toThrow(
      'cost model unavailable',
    );
    expect(store.stats()).toMatchObject({entries: 0});

    expect(planWithCache(cache, trackerAST, simpleCostModel)).toEqual(
      blueprintOf(trackerAST),
    );
  });
});

suite('BoundedPlanCache', () => {
  const blueprint = blueprintOf(trackerAST);
  const compute = () => blueprint;

  test('verifies the canonical input rather than trusting the hash', () => {
    const store = new BoundedPlanCache(8, 1 << 20);
    const other = blueprintOf(getAST(builder.users.whereExists('posts')));

    expect(store.getOrCompute('k', 'canonical-a', compute)).toBe(blueprint);
    // Same key, different content: a 64 bit hash collision. The stored entry
    // must not be returned.
    expect(store.getOrCompute('k', 'canonical-b', () => other)).toBe(other);
    expect(store.stats()).toMatchObject({
      hits: 0,
      misses: 1,
      collisions: 1,
      entries: 1,
    });

    expect(store.getOrCompute('k', 'canonical-b', compute)).toBe(other);
    expect(store.stats()).toMatchObject({hits: 1});
  });

  test('evicts least recently used entries past the entry bound', () => {
    const store = new BoundedPlanCache(2, 1 << 20);
    store.getOrCompute('a', 'a', compute);
    store.getOrCompute('b', 'b', compute);
    store.getOrCompute('a', 'a', compute); // 'a' is now most recently used
    store.getOrCompute('c', 'c', compute);

    expect(store.stats()).toMatchObject({entries: 2, evictions: 1});
    store.getOrCompute('a', 'a', compute);
    store.getOrCompute('c', 'c', compute);
    expect(store.stats()).toMatchObject({hits: 3, misses: 3});
  });

  test('evicts past the byte bound and never exceeds it', () => {
    const store = new BoundedPlanCache(1000, 2000);
    for (let i = 0; i < 50; i++) {
      store.getOrCompute(`k${i}`, 'x'.repeat(400), compute);
    }
    const stats = store.stats();
    expect(stats.bytes).toBeLessThanOrEqual(2000);
    expect(stats.entries).toBeLessThan(50);
    expect(stats.evictions).toBeGreaterThan(0);
  });

  test('refuses to store an entry larger than the whole bound', () => {
    const store = new BoundedPlanCache(1000, 500);
    expect(store.getOrCompute('k', 'x'.repeat(10_000), compute)).toBe(
      blueprint,
    );
    expect(store.stats()).toMatchObject({entries: 0, bytes: 0});
  });

  test('clear() drops every entry and its bytes', () => {
    const store = new BoundedPlanCache(8, 1 << 20);
    store.getOrCompute('a', 'a', compute);
    store.getOrCompute('b', 'b', compute);
    store.clear();
    expect(store.stats()).toMatchObject({entries: 0, bytes: 0});
  });
});
