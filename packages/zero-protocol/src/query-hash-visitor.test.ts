import {expect, test} from 'vitest';
import {normalizeAST, type AST, type Condition} from './ast.ts';
import {hashAST} from './query-hash-visitor.ts';

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

  const mutations: Record<string, AST> = {
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
