// Adversarial IVM vectors for the Swift bug sweep. Writes
// `generated/sweep-ivm-*.json` (not checked in), which
// `swift/Tests/ConformanceTests/SweepIVMTests.swift` replays.
import {test} from 'vitest';
import type {JSONValue} from '../../../shared/src/json.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {schema as testSchema} from '../../../zql/src/query/test/test-schemas.ts';

const schema: Schema = testSchema;
import {Random} from '../random.ts';
import {emitGeneratedVectors} from '../vectors.ts';
import {makeProgram} from './generate.ts';
import {
  buildProgram,
  runQuery,
  schemaJSON,
  type Program,
  type Push,
} from './harness.ts';

type Dataset = {data: Record<string, Row[]>; pushes: Push[]};

// Few distinct values so sort keys collide often; strings that order
// differently in UTF-8 and UTF-16, and that need dotall in LIKE.
const texts = [
  'a',
  'a',
  'b',
  '',
  '\uff61',
  '\u{1f600}',
  '\u{1f600}a',
  'a\nb',
  'A\nB',
  '\ue000',
  'é',
  'e\u0301',
  'ab%',
];
const numbers = [0, 1, 1, 2, -1, 2 ** 53 - 1, -(2 ** 53), 1e300, 0.5];

const issueIDs = ['i1', 'i2', 'i3', 'i4', 'i5', 'i6'];
const userIDs = ['u1', 'u2', 'u3'];
const labelIDs = ['l1', 'l2', 'l3'];
const commentIDs = ['c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8'];
const revisionIDs = ['r1', 'r2', 'r3', 'r4'];

function idsFor(table: string): string[] {
  return (
    {
      issue: issueIDs,
      user: userIDs,
      label: labelIDs,
      comment: commentIDs,
      revision: revisionIDs,
    }[table] ?? []
  );
}

function columnValue(r: Random, table: string, column: string): JSONValue {
  switch (`${table}.${column}`) {
    case 'issue.ownerId':
      return r.bool(0.3) ? null : r.pick(userIDs);
    case 'issue.closed':
      return r.bool();
    case 'issue.createdAt':
    case 'comment.createdAt':
      return r.pick(numbers);
    case 'comment.issueId':
    case 'issueLabel.issueId':
      return r.pick(issueIDs);
    case 'comment.authorId':
    case 'revision.authorId':
      return r.pick(userIDs);
    case 'revision.commentId':
      return r.pick(commentIDs);
    case 'issueLabel.labelId':
      return r.pick(labelIDs);
    case 'user.metadata':
      return null;
    default:
      return r.pick(texts);
  }
}

function makeRow(r: Random, table: string, id?: string): Row {
  const row: Record<string, JSONValue> = {};
  for (const column of Object.keys(schema.tables[table].columns)) {
    row[column] =
      column === 'id' && id !== undefined ? id : columnValue(r, table, column);
  }
  return row as Row;
}

const pk = (table: string, row: Row) =>
  JSON.stringify(schema.tables[table].primaryKey.map(k => row[k]));

function makeData(r: Random): Record<string, Row[]> {
  const data: Record<string, Row[]> = {};
  for (const table of Object.keys(schema.tables)) {
    const rows = new Map<string, Row>();
    if (table === 'issueLabel') {
      for (let i = 0; i < 8; i++) {
        const row = makeRow(r, table);
        rows.set(pk(table, row), row);
      }
    } else {
      for (const id of idsFor(table)) {
        if (r.bool(0.75)) {
          rows.set(id, makeRow(r, table, id));
        }
      }
    }
    data[table] = [...rows.values()];
  }
  return data;
}

function makePushes(
  r: Random,
  data: Record<string, Row[]>,
  count: number,
): Push[] {
  const current = new Map(
    Object.entries(data).map(([t, rows]) => [
      t,
      new Map(rows.map(row => [pk(t, row), row])),
    ]),
  );
  const tables = Object.keys(schema.tables);
  const pushes: Push[] = [];
  while (pushes.length < count) {
    const table = r.pick(tables);
    const rows = current.get(table)!;
    const kind = r.int(4);
    if (kind === 0) {
      const candidates =
        table === 'issueLabel'
          ? [makeRow(r, table)]
          : idsFor(table).map(id => makeRow(r, table, id));
      const row = candidates.find(c => !rows.has(pk(table, c)));
      if (row) {
        rows.set(pk(table, row), row);
        pushes.push({table, type: 'add', row});
      }
    } else if (rows.size > 0) {
      const existing = r.pick([...rows.values()]);
      const key = pk(table, existing);
      if (kind === 1) {
        rows.delete(key);
        pushes.push({table, type: 'remove', row: existing});
      } else {
        const pkCols = schema.tables[table].primaryKey;
        const columns = Object.keys(schema.tables[table].columns).filter(
          c => !pkCols.includes(c),
        );
        if (columns.length === 0) {
          continue;
        }
        const row = {...existing} as Record<string, JSONValue>;
        for (let i = 0, n = 1 + r.int(2); i < n; i++) {
          const column = r.pick(columns);
          row[column] = columnValue(r, table, column);
        }
        rows.set(key, row as Row);
        pushes.push({table, type: 'edit', row: row as Row, oldRow: existing});
      }
    }
  }
  return pushes;
}

function makeCase(name: string, dataset: number, set: Dataset, p: Program) {
  let built;
  try {
    built = buildProgram(schema, p);
  } catch {
    return undefined;
  }
  return {
    name,
    dataset,
    program: p,
    ast: built.ast,
    format: built.format,
    result: runQuery(schema, set.data, built.ast, built.format, set.pushes),
  };
}

const seed = Number(process.env.SWEEP_IVM_SEED ?? 4242);
const caseCount = Number(process.env.SWEEP_IVM_CASES ?? 3000);

test('sweep ivm fuzz', () => {
  const r = new Random(seed);
  const sets: Dataset[] = Array.from({length: 12}, () => {
    const data = makeData(r);
    return {data, pushes: makePushes(r, data, 40)};
  });
  const cases: NonNullable<ReturnType<typeof makeCase>>[] = [];
  let attempts = 0;
  while (cases.length < caseCount && attempts < caseCount * 8) {
    attempts++;
    const dataset = r.int(sets.length);
    const program = makeProgram(r, schema, sets[dataset].data);
    const c = makeCase(`sweep ${attempts}`, dataset, sets[dataset], program);
    if (c && !('error' in c.result)) {
      cases.push(c);
    }
  }
  emitGeneratedVectors(
    'sweep-ivm-fuzz',
    'ivm',
    'Adversarial seeded queries and pushes (duplicate sort keys, UTF-8 vs UTF-16 strings, newlines).',
    cases,
    {schema: schemaJSON(schema), datasets: sets},
  );
});

// Hand-built scenarios: each dataset is paired with the queries it targets.
const issue = (
  id: string,
  title: string,
  createdAt: number,
  extra: Partial<Record<string, JSONValue>> = {},
): Row =>
  ({
    id,
    title,
    description: '',
    closed: false,
    ownerId: null,
    createdAt,
    ...extra,
  }) as Row;
const comment = (
  id: string,
  issueId: string,
  createdAt: number,
  text = '',
): Row => ({id, authorId: 'u1', issueId, text, createdAt}) as Row;

const p = (table: string, ...ops: Program['ops']): Program => ({table, ops});

const issues = [
  issue('i1', 'a', 1),
  issue('i2', 'b', 2),
  issue('i3', 'c', 3),
  issue('i4', 'd', 4),
  issue('i5', 'e', 5),
];
const edit = (row: Row, patch: Record<string, JSONValue>): Push => ({
  table: 'issue',
  type: 'edit',
  row: {...row, ...patch} as Row,
  oldRow: row,
});

const scenarios: {
  data: Record<string, Row[]>;
  pushes: Push[];
  programs: [string, Program][];
}[] = [
  {
    // Edits that move rows across a limit boundary, both ways, and ties.
    data: {issue: issues},
    pushes: [
      edit(issues[0], {createdAt: 10}),
      edit(issues[4], {createdAt: 0}),
      edit(issues[1], {createdAt: 3}),
      {table: 'issue', type: 'remove', row: issues[2]},
      {table: 'issue', type: 'add', row: issue('i6', 'f', 3)},
    ],
    programs: [
      [
        'limit 2 by createdAt',
        p(
          'issue',
          {op: 'orderBy', field: 'createdAt', dir: 'asc'},
          {op: 'limit', n: 2},
        ),
      ],
      [
        'limit 3 desc by createdAt',
        p(
          'issue',
          {op: 'orderBy', field: 'createdAt', dir: 'desc'},
          {op: 'limit', n: 3},
        ),
      ],
      [
        'start deleted row, limit 2',
        p(
          'issue',
          {op: 'orderBy', field: 'createdAt', dir: 'asc'},
          {op: 'start', row: issues[2]},
          {op: 'limit', n: 2},
        ),
      ],
      [
        'start inclusive deleted row',
        p(
          'issue',
          {op: 'orderBy', field: 'createdAt', dir: 'asc'},
          {op: 'start', row: issues[2], inclusive: true},
          {op: 'limit', n: 2},
        ),
      ],
      [
        'start desc, limit 2',
        p(
          'issue',
          {op: 'orderBy', field: 'createdAt', dir: 'desc'},
          {op: 'start', row: issues[3]},
          {op: 'limit', n: 2},
        ),
      ],
      [
        'filter + limit',
        p(
          'issue',
          {op: 'where', field: 'createdAt', cmp: '<', value: 5},
          {op: 'orderBy', field: 'createdAt', dir: 'asc'},
          {op: 'limit', n: 2},
        ),
      ],
      [
        'one by createdAt',
        p(
          'issue',
          {op: 'orderBy', field: 'createdAt', dir: 'asc'},
          {op: 'one'},
        ),
      ],
    ],
  },
  {
    // Edits to a join key, moving comments between issues.
    data: {
      issue: issues.slice(0, 3),
      comment: [
        comment('c1', 'i1', 1),
        comment('c2', 'i1', 2),
        comment('c3', 'i2', 3),
        comment('c4', 'i2', 3),
      ],
    },
    pushes: [
      {
        table: 'comment',
        type: 'edit',
        row: comment('c1', 'i2', 1),
        oldRow: comment('c1', 'i1', 1),
      },
      {
        table: 'comment',
        type: 'edit',
        row: comment('c2', 'i3', 2),
        oldRow: comment('c2', 'i1', 2),
      },
      {
        table: 'comment',
        type: 'edit',
        row: comment('c3', 'i1', 0),
        oldRow: comment('c3', 'i2', 3),
      },
      {table: 'comment', type: 'remove', row: comment('c4', 'i2', 3)},
      {table: 'issue', type: 'remove', row: issues[1]},
    ],
    programs: [
      ['related comments', p('issue', {op: 'related', rel: 'comments'})],
      [
        'related comments limit 1',
        p('issue', {
          op: 'related',
          rel: 'comments',
          sub: [
            {op: 'orderBy', field: 'createdAt', dir: 'asc'},
            {op: 'limit', n: 1},
          ],
        }),
      ],
      ['exists comments', p('issue', {op: 'whereExists', rel: 'comments'})],
      [
        'exists comments flipped',
        p('issue', {op: 'whereExists', rel: 'comments', flip: true}),
      ],
      [
        'exists flipped limit 1',
        p(
          'issue',
          {op: 'whereExists', rel: 'comments', flip: true},
          {op: 'orderBy', field: 'createdAt', dir: 'desc'},
          {op: 'limit', n: 1},
        ),
      ],
      [
        'or(cmp, flipped exists)',
        p('issue', {
          op: 'whereExpr',
          expr: {
            or: [{cmp: ['title', '=', 'a']}, {exists: 'comments', flip: true}],
          },
        }),
      ],
      [
        'or(cmp, flipped exists) related',
        p(
          'issue',
          {
            op: 'whereExpr',
            expr: {
              or: [
                {cmp: ['title', '=', 'b']},
                {exists: 'comments', flip: true},
              ],
            },
          },
          {op: 'related', rel: 'comments'},
          {op: 'limit', n: 2},
        ),
      ],
      ['comment issue', p('comment', {op: 'related', rel: 'issue'})],
      [
        'comment exists issue',
        p(
          'comment',
          {op: 'whereExists', rel: 'issue'},
          {op: 'related', rel: 'issue'},
        ),
      ],
    ],
  },
  {
    // Unicode, newlines, and large numbers in comparisons.
    data: {
      issue: [
        issue('i1', '\uff61', 2 ** 53 - 1),
        issue('i2', '\u{1f600}', -(2 ** 53)),
        issue('i3', 'a\nb', 1e300),
        issue('i4', 'é', 0.1),
        issue('i5', 'e\u0301', 0.30000000000000004),
        issue('i6', '\ue000', 9007199254740990),
      ],
    },
    pushes: [
      edit(issue('i1', '\uff61', 2 ** 53 - 1), {title: '\u{1f600}a'}),
      edit(issue('i3', 'a\nb', 1e300), {title: 'z\nab'}),
    ],
    programs: [
      [
        'lt emoji',
        p('issue', {op: 'where', field: 'title', cmp: '<', value: '\u{1f600}'}),
      ],
      [
        'ge private use',
        p('issue', {op: 'where', field: 'title', cmp: '>=', value: '\ue000'}),
      ],
      [
        'like a%b newline',
        p('issue', {op: 'where', field: 'title', cmp: 'LIKE', value: 'a%b'}),
      ],
      [
        'like a_b newline',
        p('issue', {op: 'where', field: 'title', cmp: 'LIKE', value: 'a_b'}),
      ],
      [
        'like a% newline',
        p('issue', {op: 'where', field: 'title', cmp: 'LIKE', value: 'a%'}),
      ],
      [
        'ilike E%',
        p('issue', {op: 'where', field: 'title', cmp: 'ILIKE', value: 'E%'}),
      ],
      ['eq precomposed', p('issue', {op: 'where', field: 'title', value: 'é'})],
      [
        'big int gt',
        p('issue', {
          op: 'where',
          field: 'createdAt',
          cmp: '>',
          value: 9007199254740990,
        }),
      ],
      [
        'float eq',
        p('issue', {
          op: 'where',
          field: 'createdAt',
          value: 0.30000000000000004,
        }),
      ],
      [
        'order by title',
        p(
          'issue',
          {op: 'orderBy', field: 'title', dir: 'asc'},
          {op: 'limit', n: 4},
        ),
      ],
      [
        'start emoji',
        p(
          'issue',
          {op: 'orderBy', field: 'title', dir: 'asc'},
          {op: 'start', row: issue('i2', '\u{1f600}', 0)},
        ),
      ],
    ],
  },
];

test('sweep ivm curated', () => {
  const sets: Dataset[] = scenarios.map(s => ({
    data: Object.fromEntries(
      Object.keys(schema.tables).map(t => [t, s.data[t] ?? []]),
    ),
    pushes: s.pushes,
  }));
  const cases: NonNullable<ReturnType<typeof makeCase>>[] = [];
  scenarios.forEach((s, i) => {
    for (const [name, program] of s.programs) {
      const c = makeCase(name, i, sets[i], program);
      if (c) {
        cases.push(c);
      }
    }
  });
  emitGeneratedVectors(
    'sweep-ivm-curated',
    'ivm',
    'Hand-built edge cases: limit boundaries, deleted start rows, join key edits, unicode.',
    cases,
    {schema: schemaJSON(schema), datasets: sets},
  );
});
