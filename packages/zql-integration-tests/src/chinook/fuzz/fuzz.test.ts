/**
 * Fast (no-PG) unit tests for the coverage-driven generator machinery — mirroring the
 * Rust crate's `#[cfg(test)]` checks. These verify the **generator** (enumeration
 * counts, covering-array completeness, coverage math) and that every lowered query
 * actually **builds + hydrates** through the in-memory IVM over the mini data — without
 * needing Postgres. The PG differential parity sweep lives in the `*.pg.test.ts`.
 */

import {describe, expect, test} from 'vitest';
import type {AST, Condition} from '../../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import {MemorySource} from '../../../../zql/src/ivm/memory-source.ts';
import {makeSourceChangeAdd} from '../../../../zql/src/ivm/source.ts';
import {consume} from '../../../../zql/src/ivm/stream.ts';
import {asQueryInternals} from '../../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {QueryDelegateImpl as TestMemoryQueryDelegate} from '../../../../zql/src/query/test/query-delegate.ts';
import {schema} from '../schema.ts';
import {hasText, pkOf, relsOf, tables} from './axes.ts';
import {
  decorate,
  decorateChild,
  decoratableRoots,
  greedyCover,
} from './cover.ts';
import {Coverage, tags} from './coverage.ts';
import {Data} from './literals.ts';
import {miniData} from './mini.ts';
import {constructCount, shrinkAst} from './shrink.ts';
import {
  backboneBounds,
  depthOf,
  enumerate,
  lower,
  nExists,
  nRelated,
  type Skeleton,
} from './skeleton.ts';

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
    // Far smaller than the full cross-product (16·7·4·3 = 1344) …
    expect(rows.length).toBeLessThan(200);
    // … but at least the largest single-pair domain product (filter·exists = 16·7).
    expect(rows.length).toBeGreaterThanOrEqual(16 * 7);
  });

  test('observe marks every t-subset; total is the pairwise tuple count', () => {
    const cov = new Coverage(2);
    expect(cov.hitCount()).toBe(0);
    // domains [16,7,4,3] ⇒ Σ over the 6 axis-pairs of dom_i·dom_j = 285.
    const total = cov.total();
    expect(total).toBe(16 * 7 + 16 * 4 + 16 * 3 + 7 * 4 + 7 * 3 + 4 * 3);
    cov.observe([0, 0, 0, 0]); // one assignment hits C(4,2) = 6 pairwise tuples
    expect(cov.hitCount()).toBe(6);
    cov.observe([1, 1, 1, 1]); // a fully-different assignment adds 6 fresh tuples
    expect(cov.hitCount()).toBe(12);
    cov.observe([0, 0, 0, 0]); // re-observing is idempotent
    expect(cov.hitCount()).toBe(12);
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
      const res = decorate('track', r);
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
        const res = decorate(root, r);
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
      const res = decorateChild('album', 'tracks', r);
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
