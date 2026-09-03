import {beforeEach, expect, test, vi} from 'vitest';
import * as queryHash from '../../../zero-protocol/src/query-hash-visitor.ts';
import {defaultFormat} from '../ivm/default-format.ts';
import {createBuilder} from './create-builder.ts';
import {asQueryImpl, newQuery, newQueryImpl} from './query-impl.ts';
import {asQueryInternals} from './query-internals.ts';
import {MAX_SCAN} from './query-transitions.ts';
import type {AnyQuery} from './query.ts';
import {newRunnableQuery} from './runnable-query-impl.ts';
import {newStaticQuery} from './static-query.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';
import {schema} from './test/test-schemas.ts';

let issue: ReturnType<typeof newQuery<'issue', typeof schema>>;

beforeEach(() => {
  issue = newQuery(schema, 'issue');
});

test('the same chain built twice is the same instance', () => {
  const build = () =>
    newQuery(schema, 'issue')
      .where('closed', false)
      .orderBy('createdAt', 'desc')
      .limit(10);

  expect(build()).toBe(build());
});

test.each([
  ['where, 2-arg', (q: AnyQuery) => q.where('closed', false)],
  ['where, 3-arg', (q: AnyQuery) => q.where('title', '=', 'hi')],
  [
    'where, expression',
    (q: AnyQuery) => q.where(({cmp}) => cmp('closed', true)),
  ],
  ['limit', (q: AnyQuery) => q.limit(10)],
  ['orderBy', (q: AnyQuery) => q.orderBy('createdAt', 'desc')],
  ['one', (q: AnyQuery) => q.one()],
  ['start', (q: AnyQuery) => q.start({id: 'a'})],
  ['start inclusive', (q: AnyQuery) => q.start({id: 'a'}, {inclusive: true})],
  ['related', (q: AnyQuery) => q.related('owner')],
  ['related with cb', (q: AnyQuery) => q.related('comments', c => c.limit(3))],
  ['related two-hop', (q: AnyQuery) => q.related('labels')],
  ['whereExists', (q: AnyQuery) => q.whereExists('comments')],
  [
    'whereExists with cb',
    (q: AnyQuery) => q.whereExists('comments', c => c.where('text', '=', 'x')),
  ],
  ['whereExists two-hop', (q: AnyQuery) => q.whereExists('labels')],
])('%s is interned', (_name, op) => {
  expect(op(issue as AnyQuery)).toBe(op(issue as AnyQuery));
});

test('nameAndArgs is interned', () => {
  const withName = (args: readonly [number]) =>
    asQueryInternals(issue).nameAndArgs('issues', args);
  expect(withName([1])).toBe(withName([1]));
  expect(withName([1])).not.toBe(withName([2]));
});

test('different arguments produce different instances', () => {
  expect(issue.limit(10)).not.toBe(issue.limit(11));
  expect(issue.orderBy('createdAt', 'asc')).not.toBe(
    issue.orderBy('createdAt', 'desc'),
  );
  expect(issue.orderBy('createdAt', 'asc')).not.toBe(
    issue.orderBy('title', 'asc'),
  );
  expect(issue.where('closed', false)).not.toBe(issue.where('closed', true));
  expect(issue.where('closed', false)).not.toBe(issue.where('title', '=', 'a'));
  expect(issue.start({id: 'a'})).not.toBe(issue.start({id: 'b'}));
  expect(issue.start({id: 'a'})).not.toBe(
    issue.start({id: 'a'}, {inclusive: true}),
  );
  expect(issue.related('comments', c => c.limit(3))).not.toBe(
    issue.related('comments', c => c.limit(4)),
  );
});

test('`one` and `limit(1)` differ, because their formats do', () => {
  // Same AST, different format.
  expect(issue.one()).not.toBe(issue.limit(1));
  expect(asQueryImpl(issue.one() as AnyQuery).ast).toEqual(
    asQueryImpl(issue.limit(1) as AnyQuery).ast,
  );
});

test('interning is order sensitive even where hashing is not', () => {
  const a = issue.where('closed', false).where('title', '=', 'x');
  const b = issue.where('title', '=', 'x').where('closed', false);

  // `normalizeAST` sorts conditions, so these two hash the same...
  expect(asQueryInternals(a).hash()).toBe(asQueryInternals(b).hash());
  // ...but they are distinct nodes in the transition tree. Identity implies an
  // equal hash; the converse does not hold.
  expect(a).not.toBe(b);
});

test('sub-queries handed to callbacks are themselves interned', () => {
  const seen: AnyQuery[] = [];
  const capture = (q: AnyQuery) => {
    seen.push(q);
    return q.limit(3);
  };

  issue.related('comments', capture);
  issue.related('comments', capture);
  expect(seen).toHaveLength(2);
  // This is what makes the outer `related` comparison a pointer compare.
  expect(seen[0]).toBe(seen[1]);

  const existsSeen: AnyQuery[] = [];
  const captureExists = (q: AnyQuery) => {
    existsSeen.push(q);
    return q;
  };
  issue.whereExists('comments', captureExists);
  issue.whereExists('comments', captureExists);
  expect(existsSeen[0]).toBe(existsSeen[1]);
});

test('a non-deterministic callback simply misses the cache', () => {
  let n = 0;
  const build = () => issue.related('comments', c => c.limit(++n));
  expect(build()).not.toBe(build());
});

test('derived queries retain their parent', () => {
  const q = asQueryImpl(issue.where('closed', false).limit(5) as AnyQuery);
  const parent = q.derivedFrom;
  expect(parent).toBeDefined();
  expect(parent!.derivedFrom).toBe(asQueryImpl(issue as AnyQuery));
  expect(asQueryImpl(issue as AnyQuery).derivedFrom).toBe(undefined);
});

test('roots are interned per schema and system', () => {
  expect(newQuery(schema, 'issue')).toBe(newQuery(schema, 'issue'));
  expect(newQuery(schema, 'issue')).not.toBe(newQuery(schema, 'comment'));
  // `newStaticQuery` uses the 'permissions' system.
  expect(newStaticQuery(schema, 'issue')).toBe(newStaticQuery(schema, 'issue'));
  expect(newStaticQuery(schema, 'issue')).not.toBe(newQuery(schema, 'issue'));
});

test('separate createBuilder calls share roots', () => {
  expect(createBuilder(schema).issue).toBe(createBuilder(schema).issue);
});

test('a caller-supplied AST is not interned', () => {
  const ast = {table: 'issue', limit: 5} as const;
  expect(newQueryImpl(schema, 'issue', ast, defaultFormat, 'client')).not.toBe(
    newQueryImpl(schema, 'issue', ast, defaultFormat, 'client'),
  );
  // Nor is a non-default format, even with a root AST.
  const format = {relationships: {}, singular: true};
  expect(
    newQueryImpl(schema, 'issue', {table: 'issue'}, format, 'client'),
  ).not.toBe(newQueryImpl(schema, 'issue', {table: 'issue'}, format, 'client'));
});

test('runnable roots are interned per delegate', () => {
  const a = new QueryDelegateImpl();
  const b = new QueryDelegateImpl();

  expect(newRunnableQuery(a, schema, 'issue')).toBe(
    newRunnableQuery(a, schema, 'issue'),
  );
  expect(newRunnableQuery(a, schema, 'issue')).not.toBe(
    newRunnableQuery(b, schema, 'issue'),
  );
  // ...and the transition tree below them is shared too.
  expect(newRunnableQuery(a, schema, 'issue').limit(3)).toBe(
    newRunnableQuery(a, schema, 'issue').limit(3),
  );
});

test('hash is computed once for a repeatedly rebuilt chain', () => {
  const spy = vi.spyOn(queryHash, 'hashOfQueryInternals');
  try {
    const build = () =>
      asQueryInternals(
        newQuery(schema, 'issue').where('closed', false).limit(10),
      ).hash();

    const first = build();
    const callsAfterFirst = spy.mock.calls.length;
    expect(callsAfterFirst).toBeGreaterThan(0);

    for (let i = 0; i < 10; i++) {
      expect(build()).toBe(first);
    }
    expect(spy.mock.calls).toHaveLength(callsAfterFirst);
  } finally {
    spy.mockRestore();
  }
});

test('a list of live sibling row queries all stay interned', () => {
  const zql = createBuilder(schema);
  const ids = Array.from({length: MAX_SCAN * 4}, (_, i) => `row-${i}`);

  // Hold them all alive, the way a rendered list would, then rebuild the whole
  // list the way the next render would. Every row must still be shared: the
  // discriminating value is in the transition key, so none of these siblings
  // compete for one bucket.
  const first = ids.map(id => zql.issue.where('id', '=', id));
  const second = ids.map(id => zql.issue.where('id', '=', id));
  expect(second).toEqual(first.map(q => q));
  first.forEach((q, i) => expect(second[i]).toBe(q));
});

test('primitive where values are distinguished by type, not coerced', () => {
  const q = newQuery(schema, 'issue');
  expect(q.where('title', '=', '1')).not.toBe(
    q.where('title', '=', 1 as unknown as string),
  );
  expect(q.where('closed', '=', false)).not.toBe(
    q.where('closed', '=', 'false' as unknown as boolean),
  );
});

test('array and parameter comparisons are interned too', () => {
  const q = newQuery(schema, 'issue');
  expect(q.where('id', 'IN', ['a', 'b'])).toBe(q.where('id', 'IN', ['a', 'b']));
  expect(q.where('id', 'IN', ['a', 'b'])).not.toBe(
    q.where('id', 'IN', ['a', 'c']),
  );
  // Element order is part of the value, not a set.
  expect(q.where('id', 'IN', ['a', 'b'])).not.toBe(
    q.where('id', 'IN', ['b', 'a']),
  );
});

test('a list of live sibling IN queries all stay interned', () => {
  const zql = createBuilder(schema);
  const lists = Array.from({length: MAX_SCAN * 2}, (_, i) => [
    `a${i}`,
    `b${i}`,
  ]);
  const first = lists.map(l => zql.issue.where('id', 'IN', l));
  const second = lists.map(l => zql.issue.where('id', 'IN', l));
  first.forEach((q, i) => expect(second[i]).toBe(q));
});

test('expression-factory trees are interned, and split by relationship', () => {
  const q = newQuery(schema, 'issue');
  const byOwner = (name: string) =>
    q.whereExists('owner', o => o.where('name', '=', name));

  expect(byOwner('alice')).toBe(byOwner('alice'));
  expect(byOwner('alice')).not.toBe(byOwner('bob'));
  // A different relationship keys differently rather than sharing a bucket.
  expect(q.whereExists('comments')).not.toBe(q.whereExists('owner'));
  expect(q.whereExists('comments')).toBe(q.whereExists('comments'));
});

test('interning does not change the AST or format a query produces', () => {
  const q = newQuery(schema, 'issue')
    .related('comments', c => c.orderBy('createdAt', 'desc').limit(5))
    .related('labels')
    .whereExists('owner', o => o.where('name', '=', 'alice'))
    .where('closed', false)
    .orderBy('createdAt', 'desc')
    .limit(20);

  const qi = asQueryInternals(q);
  expect(qi.ast).toMatchInlineSnapshot(`
    {
      "alias": undefined,
      "limit": 20,
      "orderBy": [
        [
          "createdAt",
          "desc",
        ],
      ],
      "related": [
        {
          "correlation": {
            "childField": [
              "issueId",
            ],
            "parentField": [
              "id",
            ],
          },
          "hidden": undefined,
          "subquery": {
            "alias": "comments",
            "limit": 5,
            "orderBy": [
              [
                "createdAt",
                "desc",
              ],
            ],
            "related": undefined,
            "schema": undefined,
            "start": undefined,
            "table": "comment",
            "where": undefined,
          },
          "system": "client",
        },
        {
          "correlation": {
            "childField": [
              "issueId",
            ],
            "parentField": [
              "id",
            ],
          },
          "hidden": true,
          "subquery": {
            "alias": "labels",
            "limit": undefined,
            "orderBy": undefined,
            "related": [
              {
                "correlation": {
                  "childField": [
                    "id",
                  ],
                  "parentField": [
                    "labelId",
                  ],
                },
                "hidden": undefined,
                "subquery": {
                  "alias": "labels",
                  "limit": undefined,
                  "orderBy": undefined,
                  "related": undefined,
                  "schema": undefined,
                  "start": undefined,
                  "table": "label",
                  "where": undefined,
                },
                "system": "client",
              },
            ],
            "schema": undefined,
            "start": undefined,
            "table": "issueLabel",
            "where": undefined,
          },
          "system": "client",
        },
      ],
      "schema": undefined,
      "start": undefined,
      "table": "issue",
      "where": {
        "conditions": [
          {
            "left": {
              "name": "closed",
              "type": "column",
            },
            "op": "=",
            "right": {
              "type": "literal",
              "value": false,
            },
            "type": "simple",
          },
          {
            "flip": undefined,
            "op": "EXISTS",
            "related": {
              "correlation": {
                "childField": [
                  "id",
                ],
                "parentField": [
                  "ownerId",
                ],
              },
              "hidden": undefined,
              "subquery": {
                "alias": "zsubq_owner",
                "limit": undefined,
                "orderBy": undefined,
                "related": undefined,
                "schema": undefined,
                "start": undefined,
                "table": "user",
                "where": {
                  "left": {
                    "name": "name",
                    "type": "column",
                  },
                  "op": "=",
                  "right": {
                    "type": "literal",
                    "value": "alice",
                  },
                  "type": "simple",
                },
              },
              "system": "client",
            },
            "scalar": undefined,
            "type": "correlatedSubquery",
          },
        ],
        "type": "and",
      },
    }
  `);
  expect(qi.format).toMatchInlineSnapshot(`
    {
      "relationships": {
        "comments": {
          "relationships": {},
          "singular": false,
        },
        "labels": {
          "relationships": {},
          "singular": false,
        },
      },
      "singular": false,
    }
  `);
});

test('interning holds only schema-shaped things strongly', () => {
  // The safety property behind the strong first-child slot and the bounded
  // store: what is retained strongly is keyed by program structure — a
  // relationship name, one first child per node — never by data. Every axis that
  // grows with what an app queries *for* stays weak and collectible.
  //
  // A non-root AST is never interned, so this starts from a node no other test
  // has derived from.
  const root = newQueryImpl(
    schema,
    'issue',
    {table: 'issue', alias: 'retention'},
    defaultFormat,
    'client',
  ) as unknown as AnyQuery;

  // One value-keyed sibling takes the first slot; the other 49 go to the weak
  // store, where they can be collected.
  const built = Array.from({length: 50}, (_, i) =>
    root.where('id', '=', `row-${i}`),
  );
  const t = asQueryImpl(root).transitionsForTesting!;
  expect(t.first).toBe(built[0]);
  // 49 in the weak store, all under one key — the row id is the transition
  // *value*, never concatenated into the key.
  expect(t.restSize).toBe(49);
  expect(t.rest!.size).toBe(1);

  // Relationship bases are bounded by the schema, so they are strong — and they
  // do not compete for the first slot or spill into the weak store.
  root.related('comments');
  root.related('labels');
  expect(t.lookupBounded('relatedBase:comments')).toBeDefined();
  expect(t.lookupBounded('relatedBase:labels')).toBeDefined();
  expect(t.first).toBe(built[0]);
});

test('flip and scalar options are not collapsed', () => {
  const q = newQuery(schema, 'issue');
  const plain = q.whereExists('comments');
  const flipFalse = q.whereExists('comments', {flip: false});
  const flipTrue = q.whereExists('comments', {flip: true});

  // `#exists` only sets the property when it is defined, so absent and `false`
  // are genuinely different ASTs and must not intern to each other.
  expect(plain).not.toBe(flipFalse);
  expect(plain).not.toBe(flipTrue);
  expect(flipFalse).not.toBe(flipTrue);
  expect(flipFalse).toBe(q.whereExists('comments', {flip: false}));
  expect(flipTrue).toBe(q.whereExists('comments', {flip: true}));

  // Same three-state handling for `scalar`.
  const cb = (c: AnyQuery) => c;
  const scalarFalse = q.whereExists('comments', cb, {scalar: false});
  expect(q.whereExists('comments', cb)).not.toBe(scalarFalse);
  expect(scalarFalse).toBe(q.whereExists('comments', cb, {scalar: false}));
});

test('condition keys cannot collide: every token is self-delimiting', () => {
  // A non-root AST is never interned, so this starts from a node no other
  // test has derived from.
  const root = newQueryImpl(
    schema,
    'issue',
    {table: 'issue', alias: 'collision'},
    defaultFormat,
    'client',
  ) as unknown as AnyQuery;

  // Two different trees whose keys ran together when a string value was
  // written bare: the first value swallowed the start of the next condition.
  const a = root.where(({cmp, and}) =>
    and(cmp('title', '=', 'S2:id1:=:sfoo'), cmp('id', '=', 'bar')),
  );
  const b = root.where(({cmp, and}) =>
    and(cmp('title', '=', ''), cmp('id', '=', 'fooS2:id1:=:sbar')),
  );
  expect(a).not.toBe(b);
  expect(asQueryImpl(a).ast.where).not.toEqual(asQueryImpl(b).ast.where);
  expect(
    root.where(({cmp, and}) =>
      and(cmp('title', '=', ''), cmp('id', '=', 'fooS2:id1:=:sbar')),
    ),
  ).toBe(b);

  // The same holds for a value that JSON would write like another: Infinity,
  // NaN and null all serialize as `null`.
  expect(root.where('id', 'IN', [1, Infinity])).not.toBe(
    root.where('id', 'IN', [1, null]),
  );
  expect(root.where('id', 'IN', [1, Infinity])).toBe(
    root.where('id', 'IN', [1, Infinity]),
  );
  expect(root.where('id', 'IN', [[1], 2])).not.toBe(
    root.where('id', 'IN', [1, [2]]),
  );
});
