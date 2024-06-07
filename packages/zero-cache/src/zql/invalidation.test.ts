import {describe, expect, test} from 'vitest';
import type {Condition, Primitive} from 'zql/src/zql/ast/ast.js';
import {
  NormalizedInvalidationFilterSpec,
  invalidationHash,
} from '../types/invalidation.js';
import {expandSelection} from './expansion.js';
import {computeInvalidationInfo, computeMatchers} from './invalidation.js';
import {getNormalized} from './normalize.js';
import {and, cond, or} from './query-test-util.js';
import type {ServerAST} from './server-ast.js';

describe('zql/invalidation matchers', () => {
  type Case = {
    name: string;
    cond: Condition | undefined;
    matches: Record<string, Primitive>[];
  };

  const cases: Case[] = [
    {
      name: 'no WHERE clause',
      cond: undefined,
      matches: [{}],
    },
    {
      name: 'inequality',
      cond: cond(['foo', 'foo'], '>', 3),
      matches: [{}],
    },
    {
      name: 'AND inequalities',
      cond: and(cond(['foo', 'foo'], '>', 3), cond(['foo', 'foo'], '!=', 10)),
      matches: [{}],
    },
    {
      name: 'OR inequalities',
      cond: or(cond(['foo', 'foo'], '>', 3), cond(['foo', 'foo'], '!=', 10)),
      matches: [{}],
    },
    {
      name: 'equality',
      cond: cond(['foo', 'foo'], '=', 3),
      matches: [{foo: 3}],
    },
    {
      name: 'AND equality',
      cond: and(
        cond(['foo', 'foo'], '=', 3),
        cond(['foo', 'bar'], '=', 'baz'),
        cond(['foo', 'boo'], '=', 'bonk'),
      ),
      matches: [{bar: 'baz', boo: 'bonk', foo: 3}],
    },
    {
      name: 'AND equality, never match',
      cond: and(
        cond(['foo', 'foo'], '=', 3),
        cond(['foo', 'bar'], '=', 'baz'),
        cond(['foo', 'foo'], '=', 10),
      ),
      matches: [],
    },
    {
      name: 'OR equality',
      cond: or(
        cond(['foo', 'foo'], '=', 3),
        cond(['foo', 'bar'], '=', 'baz'),
        cond(['foo', 'boo'], '=', 'bonk'),
      ),
      matches: [{bar: 'baz'}, {boo: 'bonk'}, {foo: 3}],
    },
    {
      name: 'AND: mixed equality and inequality',
      cond: and(
        cond(['foo', 'foo'], '=', 3),
        cond(['foo', 'bar'], '>', 4),
        cond(['foo', 'boo'], '=', 'bonk'),
      ),
      matches: [{boo: 'bonk', foo: 3}],
    },
    {
      name: 'OR: mixed equality and inequality',
      cond: or(
        cond(['foo', 'foo'], '=', 3),
        cond(['foo', 'bar'], '>', 4),
        cond(['foo', 'boo'], '=', 'bonk'),
      ),
      matches: [{}],
    },
    {
      name: 'Nesting: OR of ANDs with subsumption',
      cond: or(
        cond(['foo', 'foo'], '=', 1),
        and(cond(['foo', 'foo'], '=', 3), cond(['foo', 'boo'], '=', 'bonk')),
        and(cond(['foo', 'foo'], '=', 4), cond(['foo', 'boo'], '=', 'bar')),
        and(
          cond(['foo', 'foo'], '=', 4),
          cond(['foo', 'boo'], '=', 'bar'),
          cond(['foo', 'should-be'], '=', 'subsumed'),
        ),
        and(
          cond(['foo', 'foo'], '=', 2),
          cond(['foo', 'boo'], '=', 'bar'),
          cond(['foo', 'not'], '=', 'subsumed'),
        ),
      ),
      matches: [
        {foo: 1},
        {boo: 'bonk', foo: 3},
        {boo: 'bar', foo: 4},
        {boo: 'bar', foo: 2, not: 'subsumed'},
      ],
    },
    {
      name: 'Nesting: AND of ORs',
      cond: and(
        cond(['foo', 'do'], '=', 1),
        or(cond(['foo', 'foo'], '=', 3), cond(['foo', 'boo'], '=', 'bonk')),
        or(
          cond(['foo', 'food'], '=', 2),
          cond(['foo', 'bood'], '=', 'bar'),
          cond(['foo', 'bonk'], '=', 'boom'),
        ),
      ),
      matches: [
        {do: 1, foo: 3, food: 2},
        {do: 1, foo: 3, bood: 'bar'},
        {do: 1, foo: 3, bonk: 'boom'},
        {do: 1, boo: 'bonk', food: 2},
        {do: 1, boo: 'bonk', bood: 'bar'},
        {do: 1, boo: 'bonk', bonk: 'boom'},
      ],
    },
    {
      name: 'Nesting: AND of ORs with never removal',
      cond: and(
        or(cond(['foo', 'foo'], '=', 3), cond(['foo', 'boo'], '=', 'bonk')),
        or(cond(['foo', 'foo'], '=', 4), cond(['foo', 'boo'], '=', 'bar')),
      ),
      matches: [
        {foo: 3, boo: 'bar'},
        {foo: 4, boo: 'bonk'},
      ],
    },
    {
      name: 'Nesting: AND of ORs with never removal and subsumption',
      cond: and(
        or(cond(['foo', 'foo'], '=', 3), cond(['foo', 'boo'], '=', 'bonk')),
        or(cond(['foo', 'foo'], '=', 4), cond(['foo', 'boo'], '=', 'bar')),
        or(
          cond(['foo', 'foo'], '=', 2),
          cond(['foo', 'boo'], '=', 'bar'),
          cond(['foo', 'sometimes'], '=', 'subsumed'),
        ),
      ),
      matches: [
        {foo: 3, boo: 'bar'},
        // Subsumed by previous match: {foo: 3, boo: 'bar', sometimes: 'subsumed'},
        {foo: 4, boo: 'bonk', sometimes: 'subsumed'},
      ],
    },
    {
      name: 'Max depth successful', // MAX_DEPTH is set to 3 for the test.
      cond: and(
        cond(['foo', 'foo'], '=', 1),
        or(
          cond(['foo', 'bar'], '=', 3),
          and(cond(['foo', 'boo'], '=', 'bonk'), cond(['foo', 'do'], '=', 4)),
        ),
      ),
      matches: [
        {foo: 1, bar: 3},
        {foo: 1, boo: 'bonk', do: 4},
      ],
    },
    {
      name: 'Max depth exceeded', // MAX_DEPTH is set to 3 for the test.
      cond: and(
        cond(['foo', 'foo'], '=', 1),
        or(
          cond(['foo', 'bar'], '=', 3),
          and(
            cond(['foo', 'boo'], '=', 'bonk'),
            // This OR is not traversed and represented by "match anything".
            or(cond(['foo', 'bar'], '=', 'baz'), cond(['foo', 'do'], '=', 4)),
          ),
        ),
      ),
      matches: [
        {foo: 1, bar: 3},
        {foo: 1, boo: 'bonk'},
      ],
    },
  ];

  const MAX_DEPTH = 3;

  for (const c of cases) {
    test(c.name, () => {
      const matches = computeMatchers(c.cond, col => col[1], MAX_DEPTH).map(m =>
        m.getMatch(),
      );
      expect(new Set(matches)).toEqual(new Set(c.matches));
    });
  }
});

describe('zql/invalidation hashes filters and hashes', () => {
  type Case = {
    name: string;
    ast: ServerAST;
    filters: NormalizedInvalidationFilterSpec[];
    hashes: string[];
  };

  const FULL_TABLE_INVALIDATION = invalidationHash({
    schema: 'public',
    table: 'foo',
    allRows: true,
  });

  const cases: Case[] = [
    {
      name: 'no WHERE',
      ast: {
        schema: 'zero',
        table: 'foo',
        select: [[['foo', 'id'], 'id']],
        orderBy: [[['foo', 'id'], 'asc']],
      },
      filters: [
        {
          id: '16dq50vgca6xn',
          schema: 'zero',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {},
        },
      ],
      hashes: [
        FULL_TABLE_INVALIDATION,
        invalidationHash({
          schema: 'zero',
          table: 'foo',
          selectedColumns: ['id'],
        }),
      ],
    },
    {
      name: 'subquery',
      ast: {
        schema: 'zero',
        table: 'foo',
        select: [[['foo', 'id'], 'id']],
        subQuery: {
          ast: {
            schema: 'zero',
            table: 'foo',
            select: [[['foo', 'id'], 'id']],
            orderBy: [[['foo', 'id'], 'asc']],
          },
          alias: 'foo',
        },
      },
      filters: [
        {
          id: '16dq50vgca6xn',
          schema: 'zero',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {},
        },
      ],
      hashes: [
        FULL_TABLE_INVALIDATION,
        invalidationHash({
          schema: 'zero',
          table: 'foo',
          selectedColumns: ['id'],
        }),
      ],
    },
    {
      name: 'aggregation with column',
      ast: {
        table: 'foo',
        aggregate: [
          {aggregate: 'min', field: ['foo', 'priority'], alias: 'ignored'},
        ],
        orderBy: [[['foo', 'ignored'], 'asc']],
      },
      filters: [
        {
          id: 'jx5gcczzxpcz',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['priority'],
          filteredColumns: {},
        },
      ],
      hashes: [
        FULL_TABLE_INVALIDATION,
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['priority'],
        }),
      ],
    },
    {
      name: 'aggregation without column',
      ast: {
        table: 'foo',
        aggregate: [{aggregate: 'count', alias: 'ignored'}],
        orderBy: [[['foo', 'ignored'], 'asc']],
      },
      filters: [
        {
          id: '36jnh0mt9ui1w',
          schema: 'public',
          table: 'foo',
          filteredColumns: {},
        },
      ],
      hashes: [
        FULL_TABLE_INVALIDATION,
        invalidationHash({
          schema: 'public',
          table: 'foo',
        }),
      ],
    },
    {
      name: 'AND filter',
      ast: {
        table: 'foo',
        select: [[['foo', 'id'], 'id']],
        orderBy: [[['foo', 'id'], 'asc']],
        where: and(
          cond(['foo', 'foo'], '=', 'bar'),
          cond(['foo', 'bar'], '=', 2),
          cond(['foo', 'a'], '<', 3), // Ignored
        ),
      },
      filters: [
        {
          id: '3aqv7m9tgnnqr',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {foo: '=', bar: '='},
        },
      ],
      hashes: [
        FULL_TABLE_INVALIDATION,
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {bar: '2', foo: '"bar"'},
        }),
      ],
    },
    {
      name: 'AND filter with selectors in fields',
      ast: {
        table: 'foo',
        select: [[['foo', 'id'], 'id']],
        orderBy: [[['foo', 'id'], 'asc']],
        where: and(
          cond(['foo', 'foo'], '=', 'bar'),
          cond(['join.alias', 'baz'], '=', 3), // Ignored
          cond(['public.foo', 'bar'], '=', 2),
          cond(['foo', 'a'], '<', 3), // Ignored
        ),
      },
      filters: [
        {
          id: '3aqv7m9tgnnqr',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {foo: '=', bar: '='},
        },
      ],
      hashes: [
        FULL_TABLE_INVALIDATION,
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {bar: '2', foo: '"bar"'},
        }),
      ],
    },
    {
      name: 'OR filter',
      ast: {
        table: 'foo',
        select: [[['foo', 'id'], 'id']],
        orderBy: [[['foo', 'id'], 'asc']],
        where: or(
          cond(['foo', 'foo'], '=', 'bar'),
          cond(['foo', 'bar'], '=', 2),
          and(cond(['foo', 'foo'], '=', 'boo'), cond(['foo', 'bar'], '=', 3)),
        ),
      },
      filters: [
        {
          id: 's10pisblnaw5',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {foo: '='},
        },
        {
          id: 'blpzmgykw6hk',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {bar: '='},
        },
        {
          id: '3aqv7m9tgnnqr',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {foo: '=', bar: '='},
        },
      ],
      hashes: [
        FULL_TABLE_INVALIDATION,
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {bar: '2'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {foo: '"bar"'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {bar: '3', foo: '"boo"'},
        }),
      ],
    },
    {
      name: 'OR filter (subsumption)',
      ast: {
        table: 'foo',
        select: [[['foo', 'id'], 'id']],
        orderBy: [[['foo', 'id'], 'asc']],
        where: or(
          cond(['foo', 'foo'], '=', 'bar'),
          cond(['foo', 'bar'], '=', 2),
          and(cond(['foo', 'foo'], '=', 'bar'), cond(['foo', 'bar'], '=', 3)),
        ),
      },
      filters: [
        {
          id: 's10pisblnaw5',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {foo: '='},
        },
        {
          id: 'blpzmgykw6hk',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {bar: '='},
        },
      ],
      hashes: [
        FULL_TABLE_INVALIDATION,
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {bar: '2'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {foo: '"bar"'},
        }),
      ],
    },
    {
      name: 'OR filter on the same field (multiple tags for a filter)',
      ast: {
        table: 'foo',
        select: [[['foo', 'id'], 'id']],
        orderBy: [[['foo', 'id'], 'asc']],
        where: or(
          cond(['foo', 'foo'], '=', 'bar'),
          cond(['foo', 'foo'], '=', 'baz'),
          cond(['foo', 'foo'], '=', 'boo'),
        ),
      },
      filters: [
        {
          id: 's10pisblnaw5',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {foo: '='},
        },
      ],
      hashes: [
        FULL_TABLE_INVALIDATION,
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {foo: '"bar"'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {foo: '"baz"'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {foo: '"boo"'},
        }),
      ],
    },
    {
      name: 'AND with nested ORs (full outer product)',
      ast: {
        table: 'foo',
        select: [[['foo', 'id'], 'id']],
        orderBy: [[['foo', 'id'], 'asc']],
        where: and(
          or(cond(['foo', 'a'], '=', 1), cond(['foo', 'b'], '=', 2)),
          or(cond(['foo', 'c'], '=', 3), cond(['foo', 'd'], '=', 4)),
          or(cond(['foo', 'e'], '=', 5), cond(['foo', 'f'], '=', 6)),
        ),
      },
      filters: [
        {
          id: '2t1fufx1makbn',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {a: '=', c: '=', e: '='},
        },
        {
          id: '2br2e2gteqg2u',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {a: '=', d: '=', e: '='},
        },
        {
          id: '19qclbkdx7o4b',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {a: '=', c: '=', f: '='},
        },
        {
          id: '38l330qvpozvv',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {a: '=', d: '=', f: '='},
        },
        {
          id: 'im36hl0oh86g',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {b: '=', c: '=', e: '='},
        },
        {
          id: '1ypj3fl6fjjjh',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {b: '=', d: '=', e: '='},
        },
        {
          id: 'lw9gqwn29foc',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {b: '=', c: '=', f: '='},
        },
        {
          id: '3flty9g04yhjn',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {b: '=', d: '=', f: '='},
        },
      ],
      hashes: [
        FULL_TABLE_INVALIDATION,
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {a: '1', c: '3', e: '5'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {a: '1', d: '4', e: '5'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {a: '1', c: '3', f: '6'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {a: '1', d: '4', f: '6'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {b: '2', c: '3', e: '5'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {b: '2', d: '4', e: '5'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {b: '2', c: '3', f: '6'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {b: '2', d: '4', f: '6'},
        }),
      ],
    },
    {
      name: 'AND with nested ORs (impossibilities pruned)',
      ast: {
        table: 'foo',
        select: [[['foo', 'id'], 'id']],
        orderBy: [[['foo', 'id'], 'asc']],
        where: and(
          or(cond(['foo', 'foo'], '=', 'bar'), cond(['foo', 'bar'], '=', 1)),
          or(cond(['foo', 'bar'], '=', 2), cond(['foo', 'do'], '=', 'foo')),
          or(cond(['foo', 'foo'], '=', 'boo'), cond(['foo', 'do'], '=', 'boo')),
        ),
      },
      filters: [
        {
          id: '3mozdzxkk72v',
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {do: '=', bar: '=', foo: '='},
        },
      ],
      hashes: [
        FULL_TABLE_INVALIDATION,
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {do: '"boo"', bar: '2', foo: '"bar"'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['id'],
          filteredColumns: {do: '"foo"', bar: '1', foo: '"boo"'},
        }),
      ],
    },
    {
      name: 'impossibility',
      ast: {
        table: 'foo',
        select: [[['foo', 'id'], 'id']],
        orderBy: [[['foo', 'id'], 'asc']],
        where: and(
          cond(['foo', 'foo'], '=', 'bar'),
          cond(['foo', 'bar'], '=', 2),
          or(cond(['foo', 'foo'], '=', 'boo'), cond(['foo', 'bar'], '=', 3)),
        ),
      },
      filters: [],
      hashes: [],
    },
    {
      name: 'Expanded AND with nested ORs (full outer product)',
      ast: expandSelection(
        {
          table: 'foo',
          select: [[['foo', 'id'], 'id']],
          orderBy: [[['foo', 'id'], 'asc']],
          where: and(
            or(cond(['foo', 'a'], '=', 1), cond(['foo', 'b'], '=', 2)),
            or(cond(['foo', 'c'], '=', 3), cond(['foo', 'd'], '=', 4)),
            or(cond(['foo', 'e'], '=', 5), cond(['foo', 'f'], '=', 6)),
          ),
        },
        () => [],
      ),
      filters: [
        {
          filteredColumns: {
            a: '=',
            c: '=',
            e: '=',
          },
          id: '94cwrmogrdg6',
          schema: 'public',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          table: 'foo',
        },
        {
          filteredColumns: {
            a: '=',
            c: '=',
            f: '=',
          },
          id: '22tz9cwb7jht7',
          schema: 'public',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          table: 'foo',
        },
        {
          filteredColumns: {
            a: '=',
            d: '=',
            e: '=',
          },
          id: '3m1rfajv6hbwb',
          schema: 'public',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          table: 'foo',
        },
        {
          filteredColumns: {
            a: '=',
            d: '=',
            f: '=',
          },
          id: '16jh6tzb2nz5d',
          schema: 'public',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          table: 'foo',
        },
        {
          filteredColumns: {
            b: '=',
            c: '=',
            e: '=',
          },
          id: '1j32xv49y8eog',
          schema: 'public',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          table: 'foo',
        },
        {
          filteredColumns: {
            b: '=',
            c: '=',
            f: '=',
          },
          id: '170umsu0w1edm',
          schema: 'public',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          table: 'foo',
        },
        {
          filteredColumns: {
            b: '=',
            d: '=',
            e: '=',
          },
          id: '1unv470xwgp9g',
          schema: 'public',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          table: 'foo',
        },
        {
          filteredColumns: {
            b: '=',
            d: '=',
            f: '=',
          },
          id: '2ekf70d5m36eq',
          schema: 'public',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          table: 'foo',
        },
      ],
      hashes: [
        FULL_TABLE_INVALIDATION,
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          filteredColumns: {a: '1', c: '3', e: '5'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          filteredColumns: {a: '1', d: '4', e: '5'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          filteredColumns: {a: '1', c: '3', f: '6'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          filteredColumns: {a: '1', d: '4', f: '6'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          filteredColumns: {b: '2', c: '3', e: '5'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          filteredColumns: {b: '2', d: '4', e: '5'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          filteredColumns: {b: '2', c: '3', f: '6'},
        }),
        invalidationHash({
          schema: 'public',
          table: 'foo',
          selectedColumns: ['a', 'b', 'c', 'd', 'e', 'f', 'id'],
          filteredColumns: {b: '2', d: '4', f: '6'},
        }),
      ],
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      const inv = computeInvalidationInfo(getNormalized(c.ast));
      expect(new Set(inv.filters)).toEqual(new Set(c.filters));
      expect(new Set(inv.hashes)).toEqual(new Set(c.hashes));
    });
  }
});
