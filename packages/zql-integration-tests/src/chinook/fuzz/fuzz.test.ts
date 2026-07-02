/**
 * Fast (no-PG) unit tests for the coverage-driven generator machinery — mirroring the
 * Rust crate's `#[cfg(test)]` checks. These verify the **generator** (enumeration
 * counts, covering-array completeness, coverage math) and that every lowered query
 * actually **builds + hydrates** through the in-memory IVM over the mini data — without
 * needing Postgres. The PG differential parity sweep lives in the `*.pg.test.ts`.
 */

import {describe, expect, test} from 'vitest';
import {must} from '../../../../shared/src/must.ts';
import type {AST, Condition} from '../../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import {MemorySource} from '../../../../zql/src/ivm/memory-source.ts';
import {makeSourceChangeAdd} from '../../../../zql/src/ivm/source.ts';
import {consume} from '../../../../zql/src/ivm/stream.ts';
import {RandomYieldSource} from '../../../../zql/src/ivm/test/random-yield-source.ts';
import {asQueryInternals} from '../../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {QueryDelegateImpl as TestMemoryQueryDelegate} from '../../../../zql/src/query/test/query-delegate.ts';
import {schema} from '../schema.ts';
import {AXES, hasText, pkOf, relsOf, tables} from './axes.ts';
import {CostModel} from './cost.ts';
import {
  decorate,
  decorateChild,
  decoratableRoots,
  greedyCover,
} from './cover.ts';
import {Coverage, tags} from './coverage.ts';
import {flipAssignments, flippableExistsCount, setFlips} from './flip.ts';
import {Data} from './literals.ts';
import {RELATIONS, transform} from './metamorphic.ts';
import {miniData} from './mini.ts';
import {mutate} from './mutate.ts';
import {fourPhase, pushForSkeleton} from './push.ts';
import {
  loadRegressions,
  parseRegression,
  type Regression,
  regressionsDir,
  serializeRegression,
} from './regressions.ts';
import {rng} from './rng.ts';
import {
  buildScalar,
  hasScalarSubquery,
  makeScalarExecutor,
  resolveScalarForIvm,
  scalarCandidates,
} from './scalar.ts';
import {constructCount, shrinkAst} from './shrink.ts';
import {
  backboneBounds,
  depthOf,
  enumerate,
  label,
  lower,
  nExists,
  nRelated,
  type Skeleton,
} from './skeleton.ts';
import {Mask, swarmGen} from './swarm.ts';
import {tailBounds, tailGen} from './tail.ts';
import {wrapAst} from './wrap.ts';

const data = new Data(miniData, pkOf);

/** A fresh in-memory delegate seeded with the mini data (client-named rows). */
function memoryDelegate(): TestMemoryQueryDelegate {
  const sources = Object.fromEntries(
    Object.entries(schema.tables).map(([key, ts]) => {
      const src = new MemorySource(ts.name, ts.columns, ts.primaryKey);
      for (const row of miniData[key] ?? []) {
        consume(src.push(makeSourceChangeAdd(row as Row)));
      }
      return [key, src];
    }),
  );
  return new TestMemoryQueryDelegate({sources});
}

/** Hydrate `q` through the in-memory IVM — throws if the pipeline fails to build/run. */
async function hydrates(
  delegate: TestMemoryQueryDelegate,
  q: AnyQuery,
): Promise<void> {
  await delegate.run(q);
}

// ── coverage math ─────────────────────────────────────────────────────────────────────

describe('coverage', () => {
  test('greedy covering array is complete pairwise + far smaller than the cross-product', () => {
    const rows = greedyCover(2);
    const cov = new Coverage(2);
    for (const r of rows) {
      cov.observe(r);
    }
    expect(cov.fraction()).toBe(1);
    expect(cov.missed()).toEqual([]);
    // Far smaller than the full cross-product (16·7·4·3·4 = 5376) …
    expect(rows.length).toBeLessThan(200);
    // … but at least the largest single-pair domain product (filter·exists = 16·7).
    expect(rows.length).toBeGreaterThanOrEqual(16 * 7);
  });

  test('observe marks every t-subset; total is the pairwise tuple count', () => {
    const cov = new Coverage(2);
    expect(cov.hitCount()).toBe(0);
    // Pairwise total is Σ over axis-pairs of dom_i·dom_j.
    const domains = AXES.map(a => a.values.length);
    const expected = domains.flatMap((d, i) =>
      domains.slice(i + 1).map(e => d * e),
    );
    const pairCount = (AXES.length * (AXES.length - 1)) / 2;
    const total = cov.total();
    expect(total).toBe(expected.reduce((acc, n) => acc + n, 0));
    cov.observe([0, 0, 0, 0, 0]); // one assignment hits C(N_AXES,2) tuples
    expect(cov.hitCount()).toBe(pairCount);
    cov.observe([1, 1, 1, 1, 1]); // a fully-different assignment adds fresh tuples
    expect(cov.hitCount()).toBe(pairCount * 2);
    cov.observe([0, 0, 0, 0, 0]); // re-observing is idempotent
    expect(cov.hitCount()).toBe(pairCount * 2);
  });
});

// ── L0 enumeration ────────────────────────────────────────────────────────────────────

describe('L0 skeletons', () => {
  test('enumerated skeletons respect the caps and lower to hydratable queries', async () => {
    const bounds = backboneBounds();
    const skels = enumerate(bounds);
    expect(skels.length).toBeGreaterThan(0);
    const delegate = memoryDelegate();
    for (const s of skels) {
      expect(depthOf(s)).toBeLessThanOrEqual(bounds.depth);
      expect(nRelated(s)).toBeLessThanOrEqual(bounds.related);
      expect(nExists(s)).toBeLessThanOrEqual(bounds.exists);
      await hydrates(delegate, lower(s));
    }
    // eslint-disable-next-line no-console
    console.log(`L0(D≤2): ${skels.length} skeletons`);
  });

  test('a bare-root skeleton for every table hydrates', async () => {
    const delegate = memoryDelegate();
    for (const t of tables()) {
      const s: Skeleton = {table: t, children: []};
      await hydrates(delegate, lower(s));
    }
  });

  test('exists subtrees nest no materialized children', () => {
    const skels = enumerate(backboneBounds());
    const checkNoRelatedUnderExists = (
      s: Skeleton,
      underExists: boolean,
    ): void => {
      for (const c of s.children) {
        if (underExists) {
          expect(c.kind).not.toBe('related');
        }
        checkNoRelatedUnderExists(c.sub, underExists || c.kind !== 'related');
      }
    };
    for (const s of skels) {
      checkNoRelatedUnderExists(s, false);
    }
  });
});

// ── L1 covering array ─────────────────────────────────────────────────────────────────

describe('L1 covering array', () => {
  test('every covering-array row realizes + hydrates on the universal `track` root', async () => {
    const delegate = memoryDelegate();
    const rows = greedyCover(2);
    for (const r of rows) {
      const res = decorate('track', r, data);
      expect(res, `track could not realize ${r}`).not.toBeNull();
      await hydrates(delegate, res![0]);
    }
  });

  test('decorations realize + hydrate on every decoratable root, reaching 100% pairwise', async () => {
    const delegate = memoryDelegate();
    const rows = greedyCover(2);
    const cov = new Coverage(2);
    for (const r of rows) {
      for (const root of decoratableRoots()) {
        const res = decorate(root, r, data);
        if (!res) {
          continue; // unrealizable on this root (text filter on a textless table)
        }
        await hydrates(delegate, res[0]);
        cov.observe(r);
      }
    }
    expect(cov.fraction(), `missed: ${JSON.stringify(cov.missed())}`).toBe(1);
  });

  test('child decorations nest a decorated collection that hydrates', async () => {
    const delegate = memoryDelegate();
    const rows = greedyCover(2);
    let realized = 0;
    for (const r of rows) {
      const res = decorateChild('album', 'tracks', r, data);
      if (!res) {
        continue;
      }
      const ast = asQueryInternals(res[0]).ast;
      expect(ast.related?.[0]?.subquery.alias).toBe('tracks');
      await hydrates(delegate, res[0]);
      realized += 1;
    }
    expect(realized).toBeGreaterThan(rows.length / 2);
  });
});

// ── data + roles ──────────────────────────────────────────────────────────────────────

describe('data-driven literals', () => {
  test('Data pulls real PK + column values from mini', () => {
    // track PKs are 100..=107 ⇒ a present median in that range.
    const mid = data.pkMid('track');
    expect(typeof mid).toBe('number');
    expect(mid as number).toBeGreaterThanOrEqual(100);
    expect(mid as number).toBeLessThanOrEqual(107);
    // The composer index is populated and non-null (some tracks have null composer).
    const comps = data.values('track', 'composer');
    expect(comps.length).toBeGreaterThan(0);
    expect(comps.every(v => v !== null)).toBe(true);
    const start = data.startRow('track', [['milliseconds', 'asc']]);
    expect(start?.id).toBeDefined();
    expect(start?.milliseconds).toBeDefined();
  });

  test('text-filter realizability tracks the presence of a text column', () => {
    expect(hasText('track')).toBe(true);
    expect(hasText('invoiceLine')).toBe(false); // no text column
  });
});

// ── feature tags ──────────────────────────────────────────────────────────────────────

describe('coverage tags', () => {
  test('tags are derived for a known shape', () => {
    // album: (id > 5) AND (title LIKE 'A%' OR EXISTS tracks), + tracks child desc, + root desc + limit
    const q = decorate; // keep import referenced
    void q;
    const ast = asQueryInternals(
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      (lower({table: 'album', children: []}) as any)
        .where('id', '>', 5)
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
        .where(({or, exists, cmp}: any) =>
          or(cmp('title', 'LIKE', 'A%'), exists('tracks')),
        )
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
        .related('tracks', (t: any) => t.orderBy('milliseconds', 'desc'))
        .orderBy('id', 'desc')
        .limit(3),
    ).ast;
    const t = tags(ast);
    for (const want of [
      'filter:>',
      'filter:LIKE',
      'where:or',
      'exists',
      'exists_under_or',
      'exists@depth0',
      'related@depth1',
      'order:desc',
      'limit',
      'limit+order',
    ]) {
      expect(t.has(want), `missing tag ${want} in ${[...t].join(',')}`).toBe(
        true,
      );
    }
    expect(t.has('not_exists')).toBe(false);
    expect(t.has('self_ref')).toBe(false);
  });

  test('self-ref + nullable order tags', () => {
    // employee ordered by reportsTo (nullable) desc, with the self-join reportsToEmployee.
    const ast = asQueryInternals(
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      (lower({table: 'employee', children: []}) as any)
        .related('reportsToEmployee', (e: AnyQuery) => e)
        .orderBy('reportsTo', 'desc'),
    ).ast;
    const t = tags(ast);
    expect(t.has('self_ref')).toBe(true);
    expect(t.has('order_desc_nullable')).toBe(true);
  });
});

// ── structure-aware shrinker ──────────────────────────────────────────────────────────

function astHasExists(ast: AST): boolean {
  const condHas = (c: Condition): boolean => {
    switch (c.type) {
      case 'simple':
        return false;
      case 'correlatedSubquery':
        return true;
      case 'and':
      case 'or':
        return c.conditions.some(condHas);
    }
  };
  return (
    (ast.where ? condHas(ast.where) : false) ||
    (ast.related ?? []).some(r => astHasExists(r.subquery))
  );
}

describe('shrinker', () => {
  test('reduces a big query to the minimal trigger (synthetic bug: EXISTS + limit)', async () => {
    // A rich query: filter + EXISTS + order + limit + a related child.
    const big = asQueryInternals(
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      (lower({table: 'track', children: []}) as any)
        .where('milliseconds', '>', 100_000)
        .whereExists('album')
        .orderBy('milliseconds', 'desc')
        .limit(3)
        .related('genre', (g: AnyQuery) => g),
    ).ast as AST;
    expect(constructCount(big)).toBeGreaterThanOrEqual(6);

    // The synthetic "bug": a case fails iff it has an EXISTS AND a limit.
    const fails = (a: AST): boolean => astHasExists(a) && a.limit !== undefined;
    expect(fails(big)).toBe(true);

    const reduced = await shrinkAst(big, fails);

    // Still reproduces …
    expect(astHasExists(reduced) && reduced.limit !== undefined).toBe(true);
    // … and is minimal: just the EXISTS + the limit.
    expect(constructCount(reduced)).toBeLessThanOrEqual(3);
    expect(reduced.related ?? []).toHaveLength(0);
    expect(reduced.orderBy ?? []).toHaveLength(0);
  });

  test('constructCount counts the pieces', () => {
    const q = asQueryInternals(
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      (lower({table: 'track', children: []}) as any)
        .where('milliseconds', '>', 1)
        .limit(2),
    ).ast as AST;
    // 1 simple condition + 1 limit = 2.
    expect(constructCount(q)).toBe(2);
  });
});

// ── four-phase push generation ────────────────────────────────────────────────────────

describe('push protocol', () => {
  test('four-phase is consistent and restores the seed', () => {
    const p = fourPhase(data, 'track', 2);
    // 2 rows × 4 phases = 8 mutations: 2 remove, 2 add, 4 edit.
    expect(p).toHaveLength(8);
    expect(p.filter(m => m.kind === 'remove')).toHaveLength(2);
    expect(p.filter(m => m.kind === 'add')).toHaveLength(2);
    expect(p.filter(m => m.kind === 'edit')).toHaveLength(4);
    // EditToRandom (first edit) changes a non-PK column but keeps the PK fixed.
    const edit = p[4];
    expect(edit.kind).toBe('edit');
    if (edit.kind === 'edit') {
      expect(edit.row.id).toBe(edit.old.id); // PK fixed
      const changed = Object.keys(edit.row).some(
        k => edit.row[k] !== edit.old[k],
      );
      expect(changed).toBe(true);
    }
    // Every mutated row carries the full track column set (no partial rows).
    const cols = [...new Set(Object.keys(miniData.track[0]))].toSorted();
    expect(cols.length).toBeGreaterThan(0);
    for (const m of p) {
      expect(Object.keys(m.row).toSorted()).toEqual(cols);
    }
  });

  test('skeleton push targets both root and the deepest leaf', () => {
    // album → tracks: pushes hit both album (root) and track (leaf).
    const skel: Skeleton = {
      table: 'album',
      children: [
        {rel: 'tracks', kind: 'related', sub: {table: 'track', children: []}},
      ],
    };
    const tables = new Set(pushForSkeleton(data, skel, 1).map(m => m.table));
    expect(tables.has('album')).toBe(true);
    expect(tables.has('track')).toBe(true);
  });
});

// ── L2 swarm ──────────────────────────────────────────────────────────────────────────

describe('swarm (L2)', () => {
  test('random masks generate realizable, hydratable queries', async () => {
    const delegate = memoryDelegate();
    const r = rng(0xf00d);
    let made = 0;
    for (let i = 0; i < 400; i++) {
      const mask = Mask.random(r);
      const res = swarmGen(r, mask, data);
      if (!res) {
        continue; // unrealizable pick (text filter on a textless table) — retry
      }
      await hydrates(delegate, res[0]);
      made += 1;
    }
    expect(
      made,
      `swarm produced too few realizable queries: ${made}`,
    ).toBeGreaterThan(100);
  });

  test('a disabled axis is pinned to its baseline (none)', () => {
    // Every axis off, no nesting ⇒ a bare decorated root: every axis at value 0 = none.
    const mask = new Mask([false, false, false, false, false], false);
    const res = swarmGen(rng(1), mask, data);
    expect(res).not.toBeNull();
    const ast = asQueryInternals(res![0]).ast;
    expect(ast.where).toBeUndefined();
    expect(ast.orderBy ?? []).toHaveLength(0);
    expect(ast.limit).toBeUndefined();
    expect(ast.start).toBeUndefined();
    expect(ast.related ?? []).toHaveLength(0);
  });
});

// ── L3 mutation-from-corpus ───────────────────────────────────────────────────────────

describe('mutation (L3)', () => {
  test('one twist keeps corpus queries buildable (alias-collision guard)', async () => {
    const delegate = memoryDelegate();
    // A depth-2 corpus includes roots with a materialized `related` — the shape an
    // AddExists twist must NOT collide with. Build through the REAL IVM (memory view) so
    // a colliding alias fails here, not silently.
    const corpus = enumerate({depth: 2, related: 1, exists: 1}).slice(0, 80);
    expect(corpus.length).toBeGreaterThan(0);
    for (const seed of [1, 2, 3, 7, 42]) {
      const r = rng(seed);
      for (const s of corpus) {
        const base = asQueryInternals(lower(s)).ast;
        const mutated = mutate(r, base);
        await hydrates(delegate, wrapAst(mutated));
      }
    }
  });

  test('one twist grows a bare root by a small, bounded amount', () => {
    // A bare `track` root (0 constructs) gains a single twist: a filter, an EXISTS (which
    // may carry an AND/OR companion and, on a junction relationship, a hidden two-hop —
    // a handful of nodes), an order term, or a limit. Small by construction, and the base
    // AST is never mutated in place.
    const base = asQueryInternals(lower({table: 'track', children: []})).ast;
    expect(constructCount(base)).toBe(0);
    for (const seed of [0, 1, 2, 3, 4, 5, 6, 7]) {
      const grew = constructCount(mutate(rng(seed), base));
      expect(grew).toBeGreaterThanOrEqual(1);
      expect(grew).toBeLessThanOrEqual(8); // "simple + one twist" stays small
      expect(constructCount(base)).toBe(0); // base untouched
    }
  });
});

// ── L4 random tail ────────────────────────────────────────────────────────────────────

describe('random tail (L4)', () => {
  test('generates deep, hydratable queries reaching past the enumerator backbone', async () => {
    const delegate = memoryDelegate();
    const r = rng(0xdeef);
    let maxDepth = 0;
    let made = 0;
    for (let i = 0; i < 300; i++) {
      const res = tailGen(r, tailBounds());
      if (!res) {
        continue;
      }
      await hydrates(delegate, res[0]);
      maxDepth = Math.max(maxDepth, astDepth(asQueryInternals(res[0]).ast));
      made += 1;
    }
    expect(made).toBeGreaterThan(100);
    // The tail actually reaches deeper than the enumerator's D ≤ 2 backbone.
    expect(
      maxDepth,
      `tail never went deep: max ${maxDepth}`,
    ).toBeGreaterThanOrEqual(3);
  });
});

/** Structural nesting depth (root = 0): deepest `related` child or EXISTS subquery. */
function astDepth(ast: AST): number {
  let m = 0;
  if (ast.where) {
    m = Math.max(m, condDepth(ast.where));
  }
  for (const r of ast.related ?? []) {
    m = Math.max(m, 1 + astDepth(r.subquery));
  }
  return m;
}

function condDepth(c: Condition): number {
  switch (c.type) {
    case 'simple':
      return 0;
    case 'correlatedSubquery':
      return 1 + astDepth(c.related.subquery);
    case 'and':
    case 'or':
      return c.conditions.reduce((m, cc) => Math.max(m, condDepth(cc)), 0);
  }
}

// ── static cost gate ──────────────────────────────────────────────────────────────────

describe('cost gate', () => {
  test('a deep materialized chain trips the static gate; a flat query does not', () => {
    // Synthetic large fixture: every table 1000 rows.
    const cost = CostModel.fromSizes(
      tables().map(t => [t, 1000] as const),
      100_000,
    );
    // artist → albums → tracks: 1000³ = 1e9 ≫ threshold → gated.
    const deep = asQueryInternals(
      lower({
        table: 'artist',
        children: [
          {
            rel: 'albums',
            kind: 'related',
            sub: {
              table: 'album',
              children: [
                {
                  rel: 'tracks',
                  kind: 'related',
                  sub: {table: 'track', children: []},
                },
              ],
            },
          },
        ],
      }),
    ).ast;
    expect(cost.tooExpensive(deep), `estimate ${cost.estimate(deep)}`).toBe(
      true,
    );

    const flat = asQueryInternals(lower({table: 'artist', children: []})).ast;
    expect(cost.tooExpensive(flat)).toBe(false);
  });

  test('an EXISTS adds its table size to the cost', () => {
    const cost = CostModel.fromSizes(
      [
        ['album', 10],
        ['track', 500],
      ],
      Number.MAX_SAFE_INTEGER,
    );
    const withExists = asQueryInternals(
      lower({
        table: 'album',
        children: [
          {rel: 'tracks', kind: 'exists', sub: {table: 'track', children: []}},
        ],
      }),
    ).ast;
    // 10 × (1 + 500) = 5010.
    expect(cost.estimate(withExists)).toBe(5010);
  });
});

// ── metamorphic relations (IVM self-consistency, oracle-free) ─────────────────────────

describe('metamorphic', () => {
  test('semantically-invariant transforms leave the memory-IVM result unchanged', async () => {
    const delegate = memoryDelegate();
    const skels = enumerate({depth: 1, related: 1, exists: 1});
    let checked = 0;
    for (const s of skels) {
      const base = lower(s);
      const ast = asQueryInternals(base).ast;
      const original = await delegate.run(base);
      for (const r of RELATIONS) {
        const t = transform(ast, r);
        if (!t) {
          continue;
        }
        const got = await delegate.run(wrapAst(t));
        expect(
          got,
          `metamorphic ${r} changed the IVM result for ${label(s)}`,
        ).toEqual(original);
        checked += 1;
      }
    }
    expect(checked).toBeGreaterThan(0);
  });

  test('each transform applies where expected on a bare root', () => {
    const bare = asQueryInternals(lower({table: 'track', children: []})).ast;
    // A bare query: redundant-conjunct / large-limit / start-before-first apply; and-
    // reorder does not (no AND).
    expect(transform(bare, 'redundantConjunct')).not.toBeNull();
    expect(transform(bare, 'largeLimit')?.limit).toBe(100_000);
    expect(transform(bare, 'startBeforeFirst')?.start).toEqual({
      row: {id: -1_000_000},
      exclusive: false,
    });
    expect(transform(bare, 'andReorder')).toBeNull();
  });
});

// ── flip-invariance (plan-choice flag manipulation) ───────────────────────────────────

describe('flip-invariance', () => {
  test('flippableExistsCount counts only positive EXISTS; set(false/true) flips them', () => {
    // track whereExists(album) AND not(exists(playlists)): one flippable gate.
    const ast = asQueryInternals(
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      (lower({table: 'track', children: []}) as any)
        .whereExists('album')
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
        .where(({not, exists}: any) => not(exists('genre'))),
    ).ast;
    expect(flippableExistsCount(ast)).toBe(1);
    expect(flipAssignments(1)).toEqual([[false], [true]]);

    const flipped = setFlips(ast, [true]);
    // Exactly one correlatedSubquery now carries flip=true; the NOT EXISTS is untouched.
    let flippedCount = 0;
    let untouched = 0;
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    const walk = (c: any): void => {
      if (c.type === 'correlatedSubquery') {
        if (c.op === 'EXISTS') {
          expect(c.flip).toBe(true);
          flippedCount += 1;
        } else {
          expect(c.flip).toBeUndefined();
          untouched += 1;
        }
      } else if (c.type === 'and' || c.type === 'or') {
        c.conditions.forEach(walk);
      }
    };
    walk(flipped.where);
    expect(flippedCount).toBe(1);
    expect(untouched).toBe(1);
  });

  test('every flip plan of an EXISTS query hydrates identically (memory IVM)', async () => {
    const delegate = memoryDelegate();
    const skels = enumerate(backboneBounds());
    let checked = 0;
    for (const s of skels) {
      const base = asQueryInternals(lower(s)).ast;
      const k = flippableExistsCount(base);
      if (k === 0 || k > 4) {
        continue;
      }
      const variants = flipAssignments(k).map(bits => setFlips(base, bits));
      const first = await delegate.run(wrapAst(variants[0]));
      for (const v of variants.slice(1)) {
        const got = await delegate.run(wrapAst(v));
        expect(got, `flip plan diverged for ${label(s)}`).toEqual(first);
        checked += 1;
      }
    }
    expect(checked).toBeGreaterThan(0);
  });
});

// ── regression replay machinery ───────────────────────────────────────────────────────

describe('regressions', () => {
  test('serialize → parse round-trips a regression', () => {
    const ast = asQueryInternals(
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      (lower({table: 'track', children: []}) as any).where(
        'milliseconds',
        '>',
        1,
      ),
    ).ast as AST;
    const reg: Regression = {note: 'sample', ast};
    const back = parseRegression(serializeRegression(reg));
    expect(back.note).toBe('sample');
    expect(back.ast).toEqual(ast);
  });

  test('a parsed regression AST re-wraps and hydrates through the memory IVM', async () => {
    const delegate = memoryDelegate();
    const ast = asQueryInternals(lower({table: 'album', children: []}))
      .ast as AST;
    const back = parseRegression(serializeRegression({note: 'h', ast}));
    await hydrates(delegate, wrapAst(back.ast));
  });

  test('loadRegressions tolerates an absent/empty directory', () => {
    // The committed directory may hold only the README (no *.json) — load returns [].
    expect(Array.isArray(loadRegressions(regressionsDir()))).toBe(true);
    expect(loadRegressions('/no/such/dir/at/all')).toEqual([]);
  });
});

// ── scalar subqueries (production resolve mirror) ─────────────────────────────────────

describe('scalar subqueries', () => {
  test('candidates are one-hop (no junctions) with a single-col-PK child', () => {
    const cands = scalarCandidates();
    expect(cands.length).toBeGreaterThan(0);
    for (const c of cands) {
      expect(relsOf(c.table).find(r => r.name === c.rel)?.junction).toBe(false);
      expect(pkOf(c.child)).toEqual([c.childPk]);
    }
  });

  test('a built gate carries scalar:true and the resolver rewrites it to a literal =', async () => {
    const delegate = memoryDelegate();
    let checked = 0;
    for (const c of scalarCandidates()) {
      const q = buildScalar(c, data);
      if (!q) {
        continue;
      }
      const ast = asQueryInternals(q).ast;
      // The raw gate is a scalar correlated subquery …
      expect(hasScalarSubquery(ast), `${c.table}.${c.rel} not scalar`).toBe(
        true,
      );
      // … which the production resolver rewrites away to a plain comparison …
      const resolved = resolveScalarForIvm(ast, miniData);
      expect(
        hasScalarSubquery(resolved),
        `unresolved scalar for ${c.table}.${c.rel}`,
      ).toBe(false);
      expect(resolved.where?.type).toBe('simple');
      // … and the resolved query hydrates through the IVM.
      await hydrates(delegate, wrapAst(resolved));
      checked += 1;
    }
    expect(checked).toBeGreaterThan(0);
  });

  test('the executor returns the childField of the constrained row', () => {
    // album.tracks: SELECT track.albumId WHERE track.id = <mid> ⇒ that track's albumId
    // (childField ≠ the constrained PK — the non-trivial direction).
    const c = must(
      scalarCandidates().find(x => x.table === 'album' && x.rel === 'tracks'),
      'album.tracks candidate missing',
    );
    const mid = data.pkMid('track');
    const wantRow = must(miniData.track.find(r => r.id === mid));
    const gate = must(asQueryInternals(must(buildScalar(c, data))).ast.where);
    expect(gate.type).toBe('correlatedSubquery');
    if (gate.type !== 'correlatedSubquery') {
      return;
    }
    const childField = gate.related.correlation.childField[0];
    expect(childField).toBe('albumId');
    expect(
      makeScalarExecutor(miniData)(gate.related.subquery, childField),
    ).toBe(wantRow.albumId);
  });
});

// ── random-yield interleave (reentrancy axis) ─────────────────────────────────────────

describe('random-yield interleave', () => {
  test('wrapping the memory sources with random yields leaves hydration unchanged', async () => {
    const plain = memoryDelegate();
    // A second delegate whose sources inject a `'yield'` before a random half of items.
    const r = rng(0xbead);
    const wrapped = new TestMemoryQueryDelegate({
      sources: Object.fromEntries(
        Object.entries(schema.tables).map(([key, ts]) => {
          const src = new MemorySource(ts.name, ts.columns, ts.primaryKey);
          for (const row of miniData[key] ?? []) {
            consume(src.push(makeSourceChangeAdd(row as Row)));
          }
          return [key, new RandomYieldSource(src, () => r.float(), 0.5)];
        }),
      ),
    });
    const skels = enumerate(backboneBounds());
    let checked = 0;
    for (const s of skels) {
      const q = lower(s);
      expect(
        await wrapped.run(q),
        `yield interleave changed ${label(s)}`,
      ).toEqual(await plain.run(q));
      checked += 1;
    }
    expect(checked).toBeGreaterThan(0);
  });
});

// ── sanity: relationships read from the schema ────────────────────────────────────────

test('schema graph exposes junction + self-join relationships', () => {
  expect(tables()).toHaveLength(11);
  const trackRels = relsOf('track');
  expect(trackRels.find(r => r.name === 'playlists')?.junction).toBe(true);
  expect(trackRels.find(r => r.name === 'album')?.junction).toBe(false);
  expect(
    relsOf('employee').find(r => r.name === 'reportsToEmployee')?.child,
  ).toBe('employee');
});
