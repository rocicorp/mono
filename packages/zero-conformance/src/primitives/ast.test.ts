import {test} from 'vitest';
import type {
  AST,
  Condition,
  CorrelatedSubquery,
  SimpleOperator,
  ValuePosition,
} from '../../../zero-protocol/src/ast.ts';
import {normalizeAST} from '../../../zero-protocol/src/ast.ts';
import {
  hashOfAST,
  hashOfNameAndArgs,
} from '../../../zero-protocol/src/query-hash.ts';
import {interestingStrings, Random} from '../random.ts';
import {emitGeneratedVectors, emitVectors} from '../vectors.ts';

const ops: SimpleOperator[] = [
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
];
const columns = ['id', 'title', 'closed', 'ownerId', 'createdAt', 'é', 'a'];
const tables = ['issue', 'comment', 'user', 'label'];
const scalars = [
  0,
  -0,
  1,
  -1,
  1.5,
  2 ** 31,
  -(2 ** 31) - 1,
  1e21,
  true,
  false,
  'a',
  'b',
  '',
  '1',
  '\u{1f600}',
  'é',
  'é',
];

function literal(r: Random): ValuePosition {
  const kind = r.int(4);
  if (kind === 0) {
    return {type: 'literal', value: null};
  }
  if (kind === 1) {
    const n = r.int(4);
    const value: (string | number | boolean)[] = [];
    for (let i = 0; i < n; i++) {
      value.push(r.pick(scalars));
    }
    return {type: 'literal', value};
  }
  return {type: 'literal', value: r.pick(scalars)};
}

function right(r: Random): Exclude<ValuePosition, {type: 'column'}> {
  if (r.bool(0.1)) {
    return {
      type: 'static',
      anchor: r.pick(['authData', 'preMutationRow'] as const),
      field: r.bool() ? r.pick(['sub', 'role']) : ['a', r.pick(['b', 'c'])],
    };
  }
  return literal(r) as Exclude<ValuePosition, {type: 'column'}>;
}

function left(r: Random): ValuePosition {
  if (r.bool(0.1)) {
    return literal(r);
  }
  if (r.bool(0.05)) {
    return {type: 'static', anchor: 'authData', field: 'sub'};
  }
  return {type: 'column', name: r.pick(columns)};
}

function condition(r: Random, depth: number): Condition {
  const kind = depth > 2 ? 0 : r.int(depth === 0 ? 4 : 5);
  switch (kind) {
    case 0:
    case 1:
      return {type: 'simple', op: r.pick(ops), left: left(r), right: right(r)};
    case 2:
    case 3: {
      const n = r.int(5);
      const conditions: Condition[] = [];
      for (let i = 0; i < n; i++) {
        conditions.push(condition(r, depth + 1));
      }
      return {type: r.bool() ? 'and' : 'or', conditions};
    }
    default:
      return {
        type: 'correlatedSubquery',
        op: r.bool() ? 'EXISTS' : 'NOT EXISTS',
        related: correlated(r, depth + 1),
        ...(r.bool(0.3) ? {flip: r.bool()} : {}),
        ...(r.bool(0.2) ? {scalar: r.bool()} : {}),
      };
  }
}

function correlated(r: Random, depth: number): CorrelatedSubquery {
  const compound = r.bool(0.2);
  return {
    correlation: {
      parentField: compound ? ['id', 'ownerId'] : [r.pick(columns)],
      childField: compound ? ['issueId', 'authorId'] : [r.pick(columns)],
    },
    subquery: ast(
      r,
      depth,
      r.pick(['zsubq_a', 'zsubq_b', 'comments', 'owner']),
    ),
    ...(r.bool(0.3)
      ? {system: r.pick(['client', 'permissions', 'test'] as const)}
      : {}),
    ...(r.bool(0.2) ? {hidden: r.bool()} : {}),
  };
}

function ast(r: Random, depth: number, alias?: string): AST {
  const result: {-readonly [K in keyof AST]: AST[K]} = {table: r.pick(tables)};
  if (alias !== undefined) {
    result.alias = alias;
  }
  if (r.bool(0.1)) {
    result.schema = 'public';
  }
  if (r.bool(0.7)) {
    result.where = condition(r, depth);
  }
  if (depth < 2 && r.bool(0.4)) {
    const n = 1 + r.int(3);
    const related: CorrelatedSubquery[] = [];
    for (let i = 0; i < n; i++) {
      related.push(correlated(r, depth + 1));
    }
    result.related = related;
  }
  if (r.bool(0.3)) {
    result.limit = r.pick([0, 1, 10, 100, 4106, 1.5, 2 ** 32]);
  }
  if (r.bool(0.5)) {
    result.orderBy = [
      [r.pick(columns), r.bool() ? 'asc' : 'desc'],
      ...(r.bool() ? ([['id', 'asc']] as const) : []),
    ];
  }
  if (r.bool(0.2)) {
    result.start = {
      row: {id: r.pick(scalars), createdAt: r.pick([1, 2.5, null])},
      exclusive: r.bool(),
    };
  }
  return result;
}

// Hand-written cases for the parts of normalization random ASTs rarely hit.
const handWritten: AST[] = [
  {table: 'issue'},
  {table: 'issue', where: {type: 'and', conditions: []}},
  {table: 'issue', where: {type: 'or', conditions: []}},
  {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'b'},
          right: {type: 'literal', value: 1},
        },
      ],
    },
  },
  {
    // Nested conjunctions of the same type flatten; a subquery and an `or`
    // exercise the asymmetric branch of the comparator.
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'x'},
              right: {type: 'literal', value: 'b'},
            },
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'x'},
              right: {type: 'literal', value: 'a'},
            },
          ],
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            correlation: {parentField: ['id'], childField: ['issueId']},
            subquery: {table: 'comment', alias: 'zsubq_comments'},
          },
        },
        {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              op: '<',
              left: {type: 'column', name: 'a'},
              right: {type: 'literal', value: 5},
            },
            {
              type: 'correlatedSubquery',
              op: 'NOT EXISTS',
              related: {
                correlation: {parentField: ['id'], childField: ['issueId']},
                subquery: {table: 'label', alias: 'zsubq_labels'},
              },
            },
          ],
        },
        {
          type: 'simple',
          op: 'IN',
          left: {type: 'column', name: 'a'},
          right: {type: 'literal', value: ['b', 'a', 1, true]},
        },
      ],
    },
    related: [
      {
        correlation: {parentField: ['id'], childField: ['issueId']},
        subquery: {table: 'comment', alias: 'comments', limit: 3},
      },
      {
        correlation: {parentField: ['ownerId'], childField: ['id']},
        subquery: {table: 'user', alias: 'owner'},
      },
      {
        correlation: {parentField: ['id'], childField: ['issueId']},
        subquery: {table: 'issueLabel', alias: 'comments'},
      },
    ],
  },
];

function astCase(a: AST) {
  return {
    ast: a,
    normalized: JSON.parse(JSON.stringify(normalizeAST(a))),
    hash: hashOfAST(a),
  };
}

test('ast', () => {
  const r = new Random(0xa57);
  const asts = [...handWritten];
  for (let i = 0; i < 30; i++) {
    asts.push(ast(r, 0));
  }
  emitVectors(
    'ast',
    'ast',
    'normalizeAST(ast) (as JSON) and hashOfAST(ast).',
    asts.map(astCase),
  );
});

test('ast (fuzz)', () => {
  const r = new Random(0xa57f);
  const asts: AST[] = [];
  for (let i = 0; i < 2000; i++) {
    asts.push(ast(r, 0));
  }
  emitGeneratedVectors(
    'ast-fuzz',
    'ast',
    'Seeded random ASTs: normalizeAST(ast) (as JSON) and hashOfAST(ast).',
    asts.map(astCase),
  );
});

test('name-args-hash', () => {
  const r = new Random(0x4e41);
  const cases: {name: string; args: unknown[]}[] = [
    {name: 'issues.byId', args: []},
    {name: 'issues.byId', args: [{id: '1'}]},
    {name: 'issues.byId', args: [{id: 1}]},
    {name: 'issues.byId', args: [{b: 1, a: 2}]},
    {name: 'issues.byId', args: [{a: 2, b: 1}]},
    {name: 'x', args: [null, true, false, 0, -0, 1.5, 'x', [1, [2]], {}]},
    {name: 'x', args: [{nested: {z: 1, a: [true, null]}}]},
    {name: '', args: [{'': ''}]},
    {name: '\u{1f600}', args: [{'\u{1f600}': 'é', 'é': 'é'}]},
  ];
  for (const s of interestingStrings) {
    cases.push({name: s, args: [s]});
  }
  for (let i = 0; i < 100; i++) {
    const args: unknown[] = [];
    const n = r.int(3);
    for (let j = 0; j < n; j++) {
      args.push(
        r.bool()
          ? {
              [r.pick(columns)]: r.pick(scalars),
              [r.pick(columns)]: r.pick(scalars),
            }
          : r.pick(scalars),
      );
    }
    cases.push({name: r.pick(['a', 'issues.list', 'users.byId']), args});
  }
  emitVectors(
    'name-args-hash',
    'name-args-hash',
    'hashOfNameAndArgs(name, args): the ID of a named query.',
    cases.map(c => ({...c, hash: hashOfNameAndArgs(c.name, c.args)})),
  );
});
