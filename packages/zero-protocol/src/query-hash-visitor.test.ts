import {expect, test} from 'vitest';
import type {Format} from '../../zero-types/src/format.ts';
import {normalizeAST, type AST, type Condition} from './ast.ts';
import {
  hashAST,
  hashNameAndArgs,
  hashOfQueryInternals,
} from './query-hash-visitor.ts';

const base: AST = {table: 'issue'};

function ast(overrides: Partial<AST>): AST {
  return normalizeAST({...base, ...overrides});
}

function where(w: Condition): AST {
  return ast({where: w});
}

function simple(
  left: string,
  op: 'IS' | '=' | '!=' | '<' | 'LIKE' | 'IN',
  right: unknown,
): Condition {
  return {
    type: 'simple',
    op,
    left: {type: 'column', name: left},
    // oxlint-disable-next-line no-explicit-any
    right: {type: 'literal', value: right as any},
  };
}

/**
 * Spread across every field of the AST and every node kind: each condition
 * type, correlated subqueries, static parameters, compound keys, bounds,
 * orderings, and the value types a literal can hold.
 */
const CORPUS: AST[] = [
  base,
  ast({table: 'comment'}),
  ast({schema: 'public'}),
  ast({alias: 'a'}),
  ast({alias: 'b'}),
  ast({limit: 1}),
  ast({limit: 2}),
  ast({limit: 1.5}),
  ast({orderBy: [['id', 'asc']]}),
  ast({orderBy: [['id', 'desc']]}),
  ast({orderBy: [['created', 'asc']]}),
  ast({
    orderBy: [
      ['id', 'asc'],
      ['created', 'desc'],
    ],
  }),
  ast({
    orderBy: [
      ['created', 'desc'],
      ['id', 'asc'],
    ],
  }),
  ast({start: {row: {id: 'a'}, exclusive: true}}),
  ast({start: {row: {id: 'a'}, exclusive: false}}),
  ast({start: {row: {id: 'b'}, exclusive: true}}),
  ast({start: {row: {id: 'a', created: 1}, exclusive: true}}),

  // Literal value types that stringify alike or nearly so.
  where(simple('id', '=', 'a')),
  where(simple('id', '=', 'b')),
  where(simple('id', '=', '1')),
  where(simple('id', '=', 1)),
  where(simple('id', '=', 1.5)),
  where(simple('id', '=', true)),
  where(simple('id', '=', 'true')),
  where(simple('id', '=', false)),
  where(simple('id', '=', null)),
  where(simple('id', '=', 'null')),
  where(simple('id', 'IN', ['a', 'b'])),
  where(simple('id', 'IN', ['b', 'a'])),
  where(simple('id', 'IN', ['ab', 'c'])),
  where(simple('id', 'IN', ['a', 'bc'])),

  // Operators and column selection.
  where(simple('id', '!=', 'a')),
  where(simple('id', '<', 'a')),
  where(simple('id', 'LIKE', 'a')),
  where(simple('id', 'IS', null)),
  where(simple('title', '=', 'a')),

  // Static parameters.
  where({
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'creatorID'},
    right: {type: 'static', anchor: 'authData', field: 'sub'},
  }),
  where({
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'creatorID'},
    right: {type: 'static', anchor: 'preMutationRow', field: 'sub'},
  }),
  where({
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'creatorID'},
    right: {type: 'static', anchor: 'authData', field: ['a', 'b']},
  }),

  // Conjunctions and disjunctions.
  where({
    type: 'and',
    conditions: [simple('id', '=', 'a'), simple('title', '=', 'b')],
  }),
  where({
    type: 'or',
    conditions: [simple('id', '=', 'a'), simple('title', '=', 'b')],
  }),
  where({
    type: 'or',
    conditions: [simple('id', '=', 'a'), simple('title', '=', 'c')],
  }),

  // Correlated subqueries, as a condition and as a relationship.
  where({
    type: 'correlatedSubquery',
    op: 'EXISTS',
    related: {
      correlation: {parentField: ['creatorID'], childField: ['id']},
      subquery: {table: 'user', alias: 'creator'},
    },
  }),
  where({
    type: 'correlatedSubquery',
    op: 'NOT EXISTS',
    related: {
      correlation: {parentField: ['creatorID'], childField: ['id']},
      subquery: {table: 'user', alias: 'creator'},
    },
  }),
  where({
    type: 'correlatedSubquery',
    op: 'EXISTS',
    flip: true,
    related: {
      correlation: {parentField: ['creatorID'], childField: ['id']},
      subquery: {table: 'user', alias: 'creator'},
    },
  }),
  where({
    type: 'correlatedSubquery',
    op: 'EXISTS',
    scalar: true,
    related: {
      correlation: {parentField: ['creatorID'], childField: ['id']},
      subquery: {table: 'user', alias: 'creator'},
    },
  }),
  where({
    type: 'correlatedSubquery',
    op: 'EXISTS',
    related: {
      correlation: {parentField: ['assigneeID'], childField: ['id']},
      subquery: {table: 'user', alias: 'creator'},
    },
  }),
  where({
    type: 'correlatedSubquery',
    op: 'EXISTS',
    related: {
      // Compound key, and one that reverses the pair.
      correlation: {parentField: ['a', 'b'], childField: ['id']},
      subquery: {table: 'user', alias: 'creator'},
    },
  }),
  where({
    type: 'correlatedSubquery',
    op: 'EXISTS',
    related: {
      correlation: {parentField: ['b', 'a'], childField: ['id']},
      subquery: {table: 'user', alias: 'creator'},
    },
  }),
  ast({
    related: [
      {
        correlation: {parentField: ['creatorID'], childField: ['id']},
        subquery: {table: 'user', alias: 'creator'},
      },
    ],
  }),
  ast({
    related: [
      {
        correlation: {parentField: ['creatorID'], childField: ['id']},
        subquery: {table: 'user', alias: 'creator'},
        hidden: true,
      },
    ],
  }),
  ast({
    related: [
      {
        correlation: {parentField: ['creatorID'], childField: ['id']},
        subquery: {table: 'user', alias: 'creator'},
        system: 'permissions',
      },
    ],
  }),
  ast({
    related: [
      {
        correlation: {parentField: ['creatorID'], childField: ['id']},
        subquery: {table: 'user', alias: 'creator'},
      },
      {
        correlation: {parentField: ['assigneeID'], childField: ['id']},
        subquery: {table: 'user', alias: 'assignee'},
      },
    ],
  }),
  // Same two relationships, nested one level deeper.
  ast({
    related: [
      {
        correlation: {parentField: ['creatorID'], childField: ['id']},
        subquery: {
          table: 'user',
          alias: 'creator',
          related: [
            {
              correlation: {parentField: ['id'], childField: ['creatorID']},
              subquery: {table: 'issue', alias: 'createdIssues'},
            },
          ],
        },
      },
    ],
  }),
];

const JSONS = CORPUS.map(a => JSON.stringify(a));

test('the corpus really is distinct', () => {
  // Guards the collision tests: if two entries normalized to the same AST,
  // asserting they hash differently would be asserting something false, and
  // asserting the hash count would pass vacuously.
  const seen = new Map<string, number>();
  for (let i = 0; i < JSONS.length; i++) {
    const dupe = seen.get(JSONS[i]);
    expect(dupe, `entries ${dupe} and ${i} are the same AST`).toBeUndefined();
    seen.set(JSONS[i], i);
  }
});

test(`hashAST separates every AST in the corpus`, () => {
  const byHash = new Map<string, string>();
  for (let i = 0; i < CORPUS.length; i++) {
    const h = hashAST(CORPUS[i]);
    const existing = byHash.get(h);
    expect(
      existing,
      `collision between\n  ${existing}\n  ${JSONS[i]}`,
    ).toBeUndefined();
    byHash.set(h, JSONS[i]);
  }
});

test(`hashAST depends only on structure, not identity or call order`, () => {
  for (let i = 0; i < CORPUS.length; i++) {
    const a = hashAST(CORPUS[i]);
    // Interleave another input to catch state leaking between calls.
    hashAST(CORPUS[(i + 1) % CORPUS.length]);
    expect(hashAST(CORPUS[i])).toBe(a);
    // A structurally equal but distinct object.
    expect(hashAST(JSON.parse(JSONS[i]) as AST)).toBe(a);
  }
});

test('hashAST reads every field of the AST', () => {
  // If the specialized visitor forgets a field, two ASTs differing only in
  // that field hash the same. Mutate each field in turn and require a change.
  const full = normalizeAST({
    schema: 'public',
    table: 'issue',
    alias: 'i',
    where: simple('open', '=', true),
    related: [
      {
        correlation: {parentField: ['creatorID'], childField: ['id']},
        subquery: {table: 'user', alias: 'creator'},
        hidden: false,
        system: 'client',
      },
      {
        correlation: {parentField: ['assigneeID'], childField: ['id']},
        subquery: {table: 'user', alias: 'assignee'},
      },
    ],
    start: {row: {id: 'i1', created: 1}, exclusive: true},
    limit: 10,
    orderBy: [['modified', 'desc']],
  });
  const baseHash = hashAST(full);

  // Typed so that every field of the AST must appear as a key: adding a field
  // to AST breaks this line until a mutation is written for it, which is the
  // regression this test exists to catch. Extra descriptive keys are allowed.
  const mutations: Record<keyof Required<AST>, AST> & Record<string, AST> = {
    'schema': {...full, schema: 'other'},
    'table': {...full, table: 'comment'},
    'alias': {...full, alias: 'other'},
    'where': {...full, where: undefined},
    'where value': {...full, where: simple('open', '=', false)},
    'related': {...full, related: full.related!.slice(1)},
    'related order': {...full, related: full.related!.toReversed()},
    'related correlation': {
      ...full,
      related: [
        {
          ...full.related![0],
          correlation: {parentField: ['x'], childField: ['id']},
        },
        full.related![1],
      ],
    },
    'related hidden': {
      ...full,
      related: [{...full.related![0], hidden: true}, full.related![1]],
    },
    'related system': {
      ...full,
      related: [{...full.related![0], system: 'permissions'}, full.related![1]],
    },
    // The remaining member, and the absent case. Each of these only has to
    // differ from `full`; that two systems also differ from *each other* is
    // what the pairwise test below covers, since this loop compares against
    // `baseHash` alone.
    'related system test': {
      ...full,
      related: [{...full.related![0], system: 'test'}, full.related![1]],
    },
    // Clearing it, which has to come off the *second* entry: normalizeAST sorts
    // related by alias, so `related[0]` is `assignee`, which has no system to
    // begin with, and `related[1]` is `creator`, which does.
    'related system absent': {
      ...full,
      related: [full.related![0], {...full.related![1], system: undefined}],
    },
    'related subquery': {
      ...full,
      related: [
        {...full.related![0], subquery: {table: 'label', alias: 'creator'}},
        full.related![1],
      ],
    },
    'start': {...full, start: {...full.start!, exclusive: false}},
    'start row': {
      ...full,
      start: {...full.start!, row: {...full.start!.row, id: 'i2'}},
    },
    'limit': {...full, limit: 11},
    'limit absent': {...full, limit: undefined},
    'orderBy': {...full, orderBy: [['created', 'desc']]},
    'orderBy direction': {...full, orderBy: [['modified', 'asc']]},
    'orderBy absent': {...full, orderBy: undefined},
  };

  for (const [field, mutated] of Object.entries(mutations)) {
    expect(
      hashAST(mutated),
      `mutating ${field} did not change the hash`,
    ).not.toBe(baseHash);
  }
});

const NAME_AND_ARGS: [string, readonly unknown[]][] = [
  ['q', []],
  ['r', []],
  ['q', [1]],
  ['q', [2]],
  ['q', [1.5]],
  ['q', ['1']],
  ['q', [true]],
  ['q', ['true']],
  ['q', [null]],
  ['q', [1, 2]],
  ['q', [2, 1]],
  ['q', [[1, 2]]],
  ['q', [[1], [2]]],
  ['q', [{a: 1}]],
  ['q', [{a: 2}]],
  ['q', [{b: 1}]],
  ['q', [{a: 1, b: 2}]],
  ['q', ['a', 'b']],
  ['q', ['ab']],
  ['q', ['a', ['b']]],
  // The pieces must not run together: these differ only in where the boundary
  // between the name and a string argument falls.
  ['qa', ['b']],
  ['q', ['ab', '']],
  ['issues', [{open: true, labels: ['bug', 'p0']}]],
  ['issues', [{open: true, labels: ['bug', 'p1']}]],
];

test('hashNameAndArgs separates every name/args pair', () => {
  const byHash = new Map<string, string>();
  for (const [name, args] of NAME_AND_ARGS) {
    const key = `${name} ${JSON.stringify(args)}`;
    const h = hashNameAndArgs(name, args);
    const existing = byHash.get(h);
    expect(
      existing,
      `collision between\n  ${existing}\n  ${key}`,
    ).toBeUndefined();
    byHash.set(h, key);
  }
});

test('hashNameAndArgs depends only on the values, not call order', () => {
  for (const [name, args] of NAME_AND_ARGS) {
    const a = hashNameAndArgs(name, args);
    hashAST(CORPUS[0]);
    hashNameAndArgs('other', [1, 2, 3]);
    expect(hashNameAndArgs(name, args)).toBe(a);
    expect(hashNameAndArgs(name, JSON.parse(JSON.stringify(args)))).toBe(a);
  }
});

test('hashNameAndArgs and hashAST occupy disjoint hash spaces', () => {
  // Both produce query IDs that share a namespace, so no AST may collide with
  // any name/args pair.
  const astHashes = new Set(CORPUS.map(hashAST));
  for (const [name, args] of NAME_AND_ARGS) {
    expect(
      astHashes.has(hashNameAndArgs(name, args)),
      `${name}(${JSON.stringify(args)}) collided with an AST hash`,
    ).toBe(false);
  }
});

test('an undefined argument hashes as null, as it serializes', () => {
  // args reach the server as JSON, where `undefined` in an array is `null`.
  // The hash has to agree with that or two queries that are identical on the
  // wire would get different IDs.
  expect(hashNameAndArgs('q', [undefined])).toBe(hashNameAndArgs('q', [null]));
  expect(hashNameAndArgs('q', [1, undefined])).toBe(
    hashNameAndArgs('q', [1, null]),
  );
  // But it must not vanish: `[undefined]` is not `[]`.
  expect(hashNameAndArgs('q', [undefined])).not.toBe(hashNameAndArgs('q', []));
  // An undefined object *property* is dropped, which is also what
  // JSON.stringify does.
  expect(hashNameAndArgs('q', [{a: 1, b: undefined}])).toBe(
    hashNameAndArgs('q', [{a: 1}]),
  );
});

test('an optional field is never confusable with its own absence', () => {
  // Every optional field writes a marker when absent. That marker must not be
  // reproducible by a legal value of the field. `limit` is the sharp case:
  // absent and present each write a single word, so an untagged limit equal to
  // the marker (TAG_UNDEF, 0x100a === 4106) would collide.
  for (const limit of [4097, 4106, 4111, 8193, 8201, 0, 1, 1000]) {
    expect(
      hashAST(ast({limit})),
      `limit ${limit} hashed the same as no limit`,
    ).not.toBe(hashAST(ast({})));
  }

  // The count-carrying fields are safe for a different reason -- a count is
  // followed by that many items -- but pin it rather than trust the argument.
  expect(hashAST(ast({related: []}))).not.toBe(hashAST(ast({})));
  expect(hashAST(ast({orderBy: []}))).not.toBe(hashAST(ast({})));
  expect(hashAST(ast({alias: ''}))).not.toBe(hashAST(ast({})));
  expect(hashAST(ast({schema: ''}))).not.toBe(hashAST(ast({})));
});

test('the digest is stable across object key reordering', () => {
  // Postgres JSONB reorders object keys (length, then bytewise), and
  // normalization does not rebuild condition objects, so an AST reloaded from
  // the CVR arrives with different key order than when it was stored. The old
  // JSON.stringify-based digest changed under that round trip, which made
  // every reloaded query record look changed and spuriously re-execute on
  // view-syncer restart. The visiting digest walks fields in a fixed order and
  // must not care.
  function reordered<T>(v: T): T {
    if (Array.isArray(v)) {
      return v.map(reordered) as T;
    }
    if (v !== null && typeof v === 'object') {
      const out: Record<string, unknown> = {};
      for (const k of Object.keys(v).reverse()) {
        out[k] = reordered((v as Record<string, unknown>)[k]);
      }
      return out as T;
    }
    return v;
  }

  for (let i = 0; i < CORPUS.length; i++) {
    const shuffled = reordered(CORPUS[i]);
    // Key order is the one thing that may differ...
    expect(shuffled).toEqual(CORPUS[i]);
    // ...and the digest must not see it.
    expect(hashAST(shuffled), JSONS[i]).toBe(hashAST(CORPUS[i]));
  }

  expect(hashNameAndArgs('q', [reordered({a: 1, b: [{c: 2, d: 3}]})])).toBe(
    hashNameAndArgs('q', [{a: 1, b: [{c: 2, d: 3}]}]),
  );
});

test('a non-finite argument hashes as null, as it serializes', () => {
  // Same rule as undefined-in-arrays: JSON.stringify writes null for NaN and
  // Infinity, so that is what they are on the wire.
  expect(JSON.stringify([NaN])).toBe(JSON.stringify([null]));
  for (const v of [NaN, Infinity, -Infinity]) {
    expect(hashNameAndArgs('q', [v])).toBe(hashNameAndArgs('q', [null]));
  }
  expect(hashNameAndArgs('q', [NaN])).not.toBe(hashNameAndArgs('q', []));
  // -0 serializes as 0 and must hash as 0.
  expect(JSON.stringify([-0])).toBe(JSON.stringify([0]));
  expect(hashNameAndArgs('q', [-0])).toBe(hashNameAndArgs('q', [0]));
  // Finite floats are still themselves.
  expect(hashNameAndArgs('q', [1.5])).not.toBe(hashNameAndArgs('q', [null]));
});

test('a hash taken from inside another walk does not corrupt it', () => {
  // args are arbitrary runtime objects: a getter is structurally valid JSON
  // and can do anything, including asking for another hash mid-walk.
  const plain = hashNameAndArgs('q', [{a: 1, b: 2}]);
  const innerExpected = hashAST(CORPUS[3]);
  let inner = '';
  const sneaky = {
    get a() {
      inner = hashAST(CORPUS[3]);
      return 1;
    },
    b: 2,
  };
  expect(hashNameAndArgs('q', [sneaky])).toBe(plain);
  expect(inner).toBe(innerExpected);
});

const fmt: Format = {singular: false, relationships: {}};

test('hashOfQueryInternals separates queries that differ only by name or args', () => {
  const a = CORPUS[0];
  // `nameAndArgs` reuses the AST it is given, so the name and args are the only
  // thing telling these apart -- the AST and the format are identical.
  const seen = new Map<string, string>();
  const cases: [string | undefined, readonly unknown[] | undefined][] = [
    [undefined, undefined],
    ['foo', []],
    ['foo', [1]],
    ['foo', [2]],
    ['foo', [1, 2]],
    ['bar', [1]],
    ['bar', []],
  ];
  for (const [name, args] of cases) {
    const key = `${name} ${JSON.stringify(args)}`;
    const h = hashOfQueryInternals(a, fmt, 'client', name, args);
    const existing = seen.get(h);
    expect(
      existing,
      `collision between\n  ${existing}\n  ${key}`,
    ).toBeUndefined();
    seen.set(h, key);
  }
});

test('hashOfQueryInternals still separates every AST in the corpus', () => {
  const byHash = new Map<string, number>();
  CORPUS.forEach((a, i) => {
    const h = hashOfQueryInternals(a, fmt, 'client', 'q', [1]);
    expect(byHash.get(h), `collision at corpus ${i}`).toBeUndefined();
    byHash.set(h, i);
  });
});

test('hashOfQueryInternals depends only on the values, not call order', () => {
  const a = CORPUS[0];
  const h = hashOfQueryInternals(a, fmt, 'client', 'foo', [1, {b: 2}]);
  hashAST(CORPUS[1]);
  hashNameAndArgs('other', [9]);
  expect(hashOfQueryInternals(a, fmt, 'client', 'foo', [1, {b: 2}])).toBe(h);
  // Structurally equal but distinct args hash the same.
  expect(hashOfQueryInternals(a, fmt, 'client', 'foo', [1, {b: 2}])).toBe(h);
});

test('an absent custom query is not confusable with a named one', () => {
  const a = CORPUS[0];
  // The absent case mixes its own tag rather than nothing at all, so a query
  // with no name cannot fold like some query that has one.
  expect(hashOfQueryInternals(a, fmt, 'client', undefined, undefined)).not.toBe(
    hashOfQueryInternals(a, fmt, 'client', '', []),
  );
});

test('hashOfQueryInternals separates queries that differ only by system', () => {
  // A query with no relationship and no `exists` has nowhere to stamp its
  // system, so the AST is identical whether it was built for the client or for
  // permissions. This is the only place system is not already covered.
  const a = CORPUS[0];
  const client = hashOfQueryInternals(a, fmt, 'client', undefined, undefined);
  const perms = hashOfQueryInternals(
    a,
    fmt,
    'permissions',
    undefined,
    undefined,
  );
  const test_ = hashOfQueryInternals(a, fmt, 'test', undefined, undefined);
  expect(new Set([client, perms, test_]).size).toBe(3);
});

test('every simple operator hashes distinctly', () => {
  // The operator is looked up in a Record rather than switched on, so a
  // duplicated tag would be silent -- two operators would hash alike and the
  // Record's type would still be satisfied. Pin every one apart.
  const ops = [
    '=',
    '!=',
    'IS',
    'IS NOT',
    '<',
    '>',
    '<=',
    '>=',
    'LIKE',
    'NOT LIKE',
    'ILIKE',
    'NOT ILIKE',
    'IN',
    'NOT IN',
  ] as const;
  const byHash = new Map<string, string>();
  for (const op of ops) {
    const h = hashAST(
      where({
        type: 'simple',
        op,
        left: {type: 'column', name: 'title'},
        right: {type: 'literal', value: 'x'},
      }),
    );
    const existing = byHash.get(h);
    expect(existing, `${op} collided with ${existing}`).toBeUndefined();
    byHash.set(h, op);
  }
  expect(byHash.size).toBe(ops.length);
});

test('both exists operators hash distinctly', () => {
  const related = {
    correlation: {parentField: ['id'], childField: ['issueId']},
    subquery: {table: 'comment', alias: 'c'},
  } as const;
  const cond = (op: 'EXISTS' | 'NOT EXISTS') =>
    hashAST(where({type: 'correlatedSubquery', op, related}));
  expect(cond('EXISTS')).not.toBe(cond('NOT EXISTS'));
});

test('both parameter anchors hash distinctly', () => {
  const cond = (anchor: 'authData' | 'preMutationRow') =>
    hashAST(
      where({
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'ownerID'},
        right: {type: 'static', anchor, field: 'sub'},
      }),
    );
  expect(cond('authData')).not.toBe(cond('preMutationRow'));
});

test('every system on a correlated subquery hashes distinctly', () => {
  // The mutation test above compares each variant against one baseline, so two
  // systems sharing a tag by accident would satisfy it. Pin all four against
  // each other.
  const variants = [undefined, 'client', 'permissions', 'test'] as const;
  const byHash = new Map<string, string>();
  for (const system of variants) {
    const h = hashAST(
      normalizeAST({
        table: 'issue',
        related: [
          {
            correlation: {parentField: ['id'], childField: ['issueId']},
            subquery: {table: 'comment', alias: 'c'},
            system,
          },
        ],
      }),
    );
    const existing = byHash.get(h);
    expect(
      existing,
      `system ${system} collided with ${existing}`,
    ).toBeUndefined();
    byHash.set(h, String(system));
  }
  expect(byHash.size).toBe(variants.length);
});
