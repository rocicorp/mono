import {test} from 'vitest';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {schema} from '../../../zql/src/query/test/test-schemas.ts';
import {Random} from '../random.ts';
import {emitGeneratedVectors, emitVectors} from '../vectors.ts';
import {makeData, makePushes, makeProgram} from './generate.ts';
import {
  buildProgram,
  runQuery,
  schemaJSON,
  type Program,
  type Push,
  type QueryResult,
} from './harness.ts';

type Dataset = {data: Record<string, Row[]>; pushes: Push[]};

type Case = {
  name: string;
  dataset: number;
  program: Program;
  ast: unknown;
  format: unknown;
  result: QueryResult;
};

function datasets(seed: number, count: number, pushes: number): Dataset[] {
  const r = new Random(seed);
  return Array.from({length: count}, () => {
    const data = makeData(r, schema);
    return {data, pushes: makePushes(r, schema, data, pushes)};
  });
}

const p = (table: string, ...ops: Program['ops']): Program => ({table, ops});

/** Queries that between them reach every client operator. */
const curated: [string, Program][] = [
  ['table scan', p('issue')],
  ['where equals', p('issue', {op: 'where', field: 'closed', value: false})],
  [
    'where string order',
    p('issue', {op: 'where', field: 'title', cmp: '>', value: 'a'}),
  ],
  [
    'where like',
    p('issue', {op: 'where', field: 'title', cmp: 'ILIKE', value: '%b%'}),
  ],
  [
    'where in',
    p('comment', {
      op: 'where',
      field: 'issueId',
      cmp: 'IN',
      value: ['i1', 'i2'],
    }),
  ],
  [
    'where is null',
    p('issue', {op: 'where', field: 'ownerId', cmp: 'IS', value: null}),
  ],
  ['primary key lookup', p('issue', {op: 'where', field: 'id', value: 'i3'})],
  ['order desc', p('issue', {op: 'orderBy', field: 'createdAt', dir: 'desc'})],
  ['order by text', p('comment', {op: 'orderBy', field: 'text', dir: 'asc'})],
  [
    'limit',
    p(
      'issue',
      {op: 'orderBy', field: 'title', dir: 'asc'},
      {op: 'limit', n: 3},
    ),
  ],
  [
    'limit 1',
    p(
      'issue',
      {op: 'orderBy', field: 'createdAt', dir: 'desc'},
      {op: 'limit', n: 1},
    ),
  ],
  ['limit 0', p('issue', {op: 'limit', n: 0})],
  ['one', p('issue', {op: 'where', field: 'id', value: 'i2'}, {op: 'one'})],
  [
    'start exclusive',
    p('issue', {op: 'start', row: {id: 'i3'}}, {op: 'limit', n: 3}),
  ],
  [
    'start inclusive desc',
    p(
      'issue',
      {op: 'orderBy', field: 'id', dir: 'desc'},
      {op: 'start', row: {id: 'i5'}, inclusive: true},
    ),
  ],
  ['related many', p('issue', {op: 'related', rel: 'comments'})],
  ['related one', p('comment', {op: 'related', rel: 'author'})],
  ['related junction', p('issue', {op: 'related', rel: 'labels'})],
  [
    'related limited',
    p('issue', {
      op: 'related',
      rel: 'comments',
      sub: [
        {op: 'orderBy', field: 'createdAt', dir: 'desc'},
        {op: 'limit', n: 2},
      ],
    }),
  ],
  [
    'related nested',
    p('user', {
      op: 'related',
      rel: 'issues',
      sub: [
        {
          op: 'related',
          rel: 'comments',
          sub: [{op: 'related', rel: 'revisions'}],
        },
        {op: 'related', rel: 'labels'},
      ],
    }),
  ],
  [
    'related one()',
    p('issue', {op: 'related', rel: 'comments', sub: [{op: 'one'}]}),
  ],
  ['exists', p('issue', {op: 'whereExists', rel: 'comments'})],
  [
    'exists filtered',
    p('issue', {
      op: 'whereExists',
      rel: 'comments',
      sub: [{op: 'where', field: 'authorId', value: 'u1'}],
    }),
  ],
  ['exists junction', p('issue', {op: 'whereExists', rel: 'labels'})],
  [
    'exists with limit',
    p(
      'issue',
      {op: 'whereExists', rel: 'comments'},
      {op: 'orderBy', field: 'title', dir: 'desc'},
      {op: 'limit', n: 2},
    ),
  ],
  [
    'exists flipped',
    p('issue', {op: 'whereExists', rel: 'comments', flip: true}),
  ],
  [
    'exists flipped junction limited',
    p(
      'issue',
      {
        op: 'whereExists',
        rel: 'labels',
        flip: true,
        sub: [{op: 'where', field: 'name', cmp: '!=', value: 'a'}],
      },
      {op: 'limit', n: 3},
    ),
  ],
  [
    'or of exists',
    p('issue', {
      op: 'whereExpr',
      expr: {
        or: [
          {
            exists: 'comments',
            sub: [{op: 'where', field: 'authorId', value: 'u2'}],
          },
          {cmp: ['closed', '=', true]},
        ],
      },
    }),
  ],
  [
    'or of flipped exists',
    p(
      'issue',
      {
        op: 'whereExpr',
        expr: {
          or: [
            {exists: 'comments', flip: true},
            {
              exists: 'owner',
              flip: true,
              sub: [{op: 'where', field: 'name', value: 'a'}],
            },
            {cmp: ['title', '=', 'b']},
          ],
        },
      },
      {op: 'related', rel: 'owner'},
    ),
  ],
  [
    'and of conditions',
    p('issue', {
      op: 'whereExpr',
      expr: {
        and: [
          {cmp: ['closed', '=', false]},
          {or: [{cmp: ['createdAt', '>', 1]}, {cmp: ['title', 'LIKE', 'a%']}]},
          {not: {cmp: ['ownerId', 'IS', null]}},
        ],
      },
    }),
  ],
  [
    'exists and related',
    p(
      'comment',
      {
        op: 'whereExists',
        rel: 'issue',
        sub: [{op: 'where', field: 'closed', value: false}],
      },
      {op: 'related', rel: 'issue', sub: [{op: 'related', rel: 'owner'}]},
      {op: 'related', rel: 'revisions'},
    ),
  ],
  [
    'nested exists',
    p('user', {
      op: 'whereExists',
      rel: 'issues',
      sub: [{op: 'whereExists', rel: 'labels'}],
    }),
  ],
  [
    'pushdown',
    p(
      'issue',
      {op: 'where', field: 'id', cmp: 'IN', value: ['i1', 'i4', 'i6']},
      {op: 'related', rel: 'comments', sub: [{op: 'limit', n: 1}]},
    ),
  ],
  [
    'label issues',
    p('label', {
      op: 'related',
      rel: 'issues',
      sub: [{op: 'related', rel: 'owner'}],
    }),
  ],
];

function makeCase(
  name: string,
  dataset: number,
  data: Dataset,
  program: Program,
): Case | undefined {
  let built;
  try {
    built = buildProgram(schema, program);
  } catch {
    return undefined;
  }
  return {
    name,
    dataset,
    program,
    ast: built.ast,
    format: built.format,
    result: runQuery(schema, data.data, built.ast, built.format, data.pushes),
  };
}

test('ivm curated', () => {
  const sets = datasets(7, 1, 25);
  const cases: Case[] = [];
  for (const [name, program] of curated) {
    sets.forEach((set, i) => {
      const c = makeCase(name, i, set, program);
      if (c) {
        cases.push(c);
      }
    });
  }
  emitVectors(
    'ivm',
    'ivm',
    'Queries over the zql test schema: the view after hydration and after each source push, and the changes the view received.',
    cases,
    {schema: schemaJSON(schema), datasets: sets},
    {compact: true},
  );
});

// IVM_FUZZ_SEED and IVM_FUZZ_CASES widen the search for a local deep run.
const fuzzSeed = Number(process.env.IVM_FUZZ_SEED ?? 99);
const fuzzCases = Number(process.env.IVM_FUZZ_CASES ?? 600);

test('ivm fuzz', () => {
  const sets = datasets(1234, 10, 25);
  const r = new Random(fuzzSeed);
  const cases: Case[] = [];
  let attempts = 0;
  while (cases.length < fuzzCases && attempts < fuzzCases * 8) {
    attempts++;
    const dataset = r.int(sets.length);
    const program = makeProgram(r, schema, sets[dataset].data);
    const c = makeCase(`fuzz ${attempts}`, dataset, sets[dataset], program);
    if (c && !('error' in c.result)) {
      cases.push(c);
    }
  }
  emitGeneratedVectors(
    'ivm-fuzz',
    'ivm',
    'Seeded random queries and pushes over the zql test schema.',
    cases,
    {schema: schemaJSON(schema), datasets: sets},
  );
});
