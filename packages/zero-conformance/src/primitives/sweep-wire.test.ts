/**
 * Adversarial vectors for the Swift client's JSON layer and query hashes:
 * JSON texts at the edges of `JSON.parse`, name-and-args hashes of awkward
 * arguments, and the `hash()` of queries built with the query builder.
 */
import {test} from 'vitest';
import * as v from '../../../shared/src/valita.ts';
import type {Condition} from '../../../zero-protocol/src/ast.ts';
import {downstreamSchema} from '../../../zero-protocol/src/down.ts';
import {hashOfNameAndArgs} from '../../../zero-protocol/src/query-hash.ts';
import {defaultFormat} from '../../../zql/src/ivm/default-format.ts';
import {
  newQuery,
  newQueryImpl,
  type QueryImpl,
} from '../../../zql/src/query/query-impl.ts';
import {newStaticQuery} from '../../../zql/src/query/static-query.ts';
import {schema} from '../../../zql/src/query/test/test-schemas.ts';
import {Random} from '../random.ts';
import {emitVectors} from '../vectors.ts';

/**
 * `JSON.stringify` with object keys sorted by UTF-16 code unit: a form of a
 * value that doesn't depend on the order its keys were set in.
 */
function sortedStringify(v: unknown): string {
  if (Array.isArray(v)) {
    return '[' + v.map(sortedStringify).join(',') + ']';
  }
  if (v !== null && typeof v === 'object') {
    const o = v as Record<string, unknown>;
    return (
      '{' +
      Object.keys(o)
        .sort()
        .map(k => JSON.stringify(k) + ':' + sortedStringify(o[k]))
        .join(',') +
      '}'
    );
  }
  return JSON.stringify(v);
}

test('sweep-json', () => {
  const nested = (n: number) => '['.repeat(n) + ']'.repeat(n);
  const ok = [
    // Canonically equivalent but different keys: two properties in JS.
    '{"\\u00e9":1,"e\\u0301":2}',
    '{"\u00e9":1,"e\u0301":2}',
    '{"\\u212b":1,"\\u00c5":2,"A\\u030a":3}',
    '{"\\uac00":1,"\\u1100\\u1161":2}',
    '[{"k":"\\u00e9"},{"k":"e\\u0301"}]',
    // Code-unit order of keys: an astral character sorts below U+FFFF.
    '{"\\uffff":1,"\\ud83d\\ude00":2,"\\ue000":3,"z":4,"":5}',
    // Integer-like keys come first in JS insertion order, not when sorted.
    '{"b":1,"a":2,"10":3,"9":4,"1":5}',
    '{"__proto__":{"a":1},"constructor":2,"toString":3}',
    '{"\\u0000":"\\u0000","a\\u0000b":"\\u001f\\u007f"}',
    '"\\ud83d\\ude00\\ud83d\\ude00"',
    '"\\u2028\\u2029"',
    '"\\/\\b\\f\\n\\r\\t"',
    // Numbers.
    '-0',
    '-0.0',
    '0e0',
    '-0e-0',
    '[0,-0]',
    '1e21',
    '1e20',
    '100000000000000000000',
    '1000000000000000000000',
    '123456789012345680000',
    '1E+2',
    '0.1e1',
    '1e-7',
    '1.5e-7',
    '0.000001',
    '0.0000001',
    '123e-20',
    '9007199254740991',
    '9007199254740992',
    '9007199254740993',
    '-9007199254740993',
    '18446744073709551616',
    '123456789012345678',
    '999999999999999',
    '-999999999999999',
    '9999999999999999',
    '0.30000000000000004441',
    '1.7976931348623157e308',
    '1.7976931348623158e308',
    '1.7976931348623159e308',
    '-1e400',
    '1e-400',
    '-1e-400',
    '2.4703282292062328e-324',
    '2.4703282292062327e-324',
    '4.9406564584124654e-324',
    '2.2250738585072011e-308',
    '2.2250738585072014e-308',
    '0.' + '0'.repeat(400) + '1',
    '1' + '0'.repeat(400),
    '1' + '0'.repeat(308),
    '[1.0,1.50,100e-2]',
    // Duplicate keys: the last wins.
    '{"a":1,"a":2}',
    '{"a":{"x":1},"a":{"y":2}}',
    // Deep, but within what JS and the Swift parser both handle.
    nested(1000),
    '{"a":' + nested(500) + '}',
    // Whitespace JSON allows.
    '\t\n\r [ 1 , 2 ] \n',
  ];
  const bad = [
    '\ufeff{}',
    '\ufeff',
    '{}\ufeff',
    '[1,]',
    '{"a":1,}',
    '[,1]',
    '{,}',
    '{"a":1,,"b":2}',
    '[1,,2]',
    '01',
    '-01',
    '1.e1',
    '.1',
    '1e+',
    '-',
    '- 1',
    '+0',
    '"\t"',
    '"\n"',
    '\u00a01',
    '1\u000b',
    '\u20281',
    '[1]\u0000',
    '"\\u00G0"',
    '"\\U0041"',
    '"\\v"',
    '"\\x41"',
    '"\\\'"',
    '"\\ud800\\uZZZZ"',
    '"\\ud800\\u12"',
    '"a\\',
    '"a',
    'tRue',
    'nulll',
    '{"a" : 1 "b":2}',
    '/*c*/1',
    '1//',
    'NaN',
    '-Infinity',
    '0x1',
    '{"a"}',
    '{1:2}',
    "{'a':1}",
    '[',
    ']',
    '{"a":1}]',
  ];
  emitVectors(
    'sweep-json',
    'sweep-json',
    'JSON.parse acceptance of edge cases; accepted inputs carry JSON.stringify of the result.',
    [
      ...ok.map(input => ({
        input,
        ok: true,
        stringify: JSON.stringify(JSON.parse(input)),
      })),
      ...bad.map(input => {
        let accepted = false;
        try {
          JSON.parse(input);
          accepted = true;
        } catch (e) {
          if (!(e instanceof SyntaxError)) {
            throw e;
          }
        }
        if (accepted) {
          throw new Error(`expected ${JSON.stringify(input)} to be rejected`);
        }
        return {input, ok: false};
      }),
    ],
  );
});

test('sweep-name-args-hash', () => {
  // Arguments as JSON text, so that what JS hashes is what the Swift
  // parser reads.
  const args = [
    '[{"\\u00e9":1,"e\\u0301":2}]',
    '[{"e\\u0301":1}]',
    '[{"\\u00e9":1}]',
    '["\\u00e9"]',
    '["e\\u0301"]',
    '[{"\\uffff":1,"\\ud83d\\ude00":2,"\\ue000":3,"z":4}]',
    '[{"b":1,"a":2,"10":3,"9":4,"1":5}]',
    '[{"__proto__":1}]',
    '[{"__proto__":{"a":1}}]',
    '[-0]',
    '[0]',
    '[2147483647]',
    '[2147483648]',
    '[-2147483648]',
    '[-2147483649]',
    '[4294967296]',
    '[9007199254740993]',
    '[1e21]',
    '[5e-324]',
    '[1e400]',
    '[[1e400,-1e400]]',
    '[{"a":1e400}]',
    '[0.1]',
    '[1.5,-1.5]',
    '["\\u0000"]',
    '[{"":null}]',
    '[null,true,false]',
    '[{"a":{"b":{"c":[]}}}]',
    '[[]]',
    '[{}]',
    '[]',
  ];
  const names = ['q', '', '\u00e9', 'e\u0301'];
  const cases = [];
  for (const text of args) {
    cases.push({
      name: 'q',
      args: text,
      hash: hashOfNameAndArgs('q', JSON.parse(text)),
    });
  }
  for (const name of names) {
    cases.push({name, args: '[]', hash: hashOfNameAndArgs(name, [])});
  }
  emitVectors(
    'sweep-name-args-hash',
    'sweep-name-args-hash',
    'hashOfNameAndArgs of awkward arguments, given as JSON text.',
    cases,
  );
});

test('sweep-downstream', () => {
  // Message texts, as the client receives them; the output is what valita
  // returns for them, with keys sorted: the Swift client re-encodes a
  // message from its fields, in an order of its own. The client parses in passthrough
  // mode, which differs from strip only in keeping unknown keys, and these
  // messages have none.
  const messages = [
    '["pokePart",{"pokeID":"p","rowsPatch":[{"op":"put","tableName":"t","value":{"id":"x","\\u00e9":1,"e\\u0301":2}}]}]',
    '["pokePart",{"pokeID":"p","rowsPatch":[{"op":"del","tableName":"t","id":{"\\u00c5":"a","\\u212b":"b"}}]}]',
    '["pokePart",{"pokeID":"p","rowsPatch":[{"op":"update","tableName":"t","id":{"id":"x"},"merge":{"\\u00e9":1,"e\\u0301":null}}]}]',
    '["pokePart",{"pokeID":"p","lastMutationIDChanges":{"\\u00e9":1,"e\\u0301":2}}]',
    '["pokePart",{"pokeID":"p","desiredQueriesPatches":{"\\u00e9":[{"op":"clear"}],"e\\u0301":[{"op":"del","hash":"h"}]}}]',
    '["pokePart",{"pokeID":"p","lastMutationIDChanges":{"__proto__":5,"c":1}}]',
    '["pokePart",{"pokeID":"p","rowsPatch":[{"op":"put","tableName":"__proto__","value":{"__proto__":{"a":1},"id":"x"}}]}]',
    '["pokePart",{"pokeID":"p","rowsPatch":[{"op":"put","tableName":"t","value":{"id":"x","n":1e400,"m":-0,"big":9007199254740993}}]}]',
  ];
  emitVectors(
    'sweep-downstream',
    'sweep-downstream',
    'Downstream message texts and what the JS client parses them to (downstreamSchema), keys sorted.',
    messages.map(text => {
      const result = v.test(JSON.parse(text), downstreamSchema, 'strip');
      if (!result.ok) {
        throw new Error(`${text} was rejected: ${result.error}`);
      }
      return {text, output: sortedStringify(result.value)};
    }),
  );
});

// Queries built with the query builder, as a small program both sides run.
//
//   query  = {table, system?, ops}
//   op     = ["where", field, op, value] | ["whereExpr", expr]
//          | ["whereExists", relationship, options|null, ops]
//          | ["related", relationship, ops] | ["one"] | ["limit", n]
//          | ["orderBy", field, "asc"|"desc"] | ["start", row, inclusive]
//          | ["nameAndArgs", name, args]
//   expr   = {cmp: [field, op, value]} | {cmpLit: [left, op, right]}
//          | {and: expr[]} | {or: expr[]} | {not: expr}
//          | {exists: [relationship, options|null, ops]}

type Op = unknown[];
type Expr = Record<string, unknown>;
type Spec = {table: string; system?: string; ops: Op[]};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyQ = any;

function run(q: AnyQ, ops: Op[]): AnyQ {
  for (const op of ops) {
    switch (op[0]) {
      case 'where':
        q = q.where(op[1], op[2], op[3]);
        break;
      case 'whereExpr':
        q = q.where((eb: AnyQ) => expr(eb, op[1] as Expr));
        break;
      case 'whereExists':
        q = q.whereExists(
          op[1],
          (s: AnyQ) => run(s, op[3] as Op[]),
          op[2] ?? undefined,
        );
        break;
      case 'related':
        q = q.related(op[1], (s: AnyQ) => run(s, op[2] as Op[]));
        break;
      case 'one':
        q = q.one();
        break;
      case 'limit':
        q = q.limit(op[1]);
        break;
      case 'orderBy':
        q = q.orderBy(op[1], op[2]);
        break;
      case 'start':
        q = q.start(op[1], op[2] ? {inclusive: true} : undefined);
        break;
      case 'nameAndArgs':
        q = q.nameAndArgs(op[1], op[2]);
        break;
      default:
        throw new Error(`unknown op ${String(op[0])}`);
    }
  }
  return q;
}

function expr(eb: AnyQ, e: Expr): Condition {
  if ('cmp' in e) {
    const [f, op, v] = e['cmp'] as [string, string, unknown];
    return eb.cmp(f, op, v);
  }
  if ('cmpLit' in e) {
    const [l, op, r] = e['cmpLit'] as [unknown, string, unknown];
    return eb.cmpLit(l, op, r);
  }
  if ('and' in e) {
    return eb.and(...(e['and'] as Expr[]).map(c => expr(eb, c)));
  }
  if ('or' in e) {
    return eb.or(...(e['or'] as Expr[]).map(c => expr(eb, c)));
  }
  if ('not' in e) {
    return eb.not(expr(eb, e['not'] as Expr));
  }
  const [rel, options, ops] = e['exists'] as [string, unknown, Op[]];
  return eb.exists(rel, (s: AnyQ) => run(s, ops), options ?? undefined);
}

function root(spec: Spec): AnyQ {
  switch (spec.system ?? 'client') {
    case 'client':
      return newQuery(schema, spec.table as 'issue');
    case 'permissions':
      return newStaticQuery(schema, spec.table as 'issue');
    default:
      return newQueryImpl(
        schema,
        spec.table as 'issue',
        {table: spec.table},
        defaultFormat,
        'test',
      );
  }
}

const handQueries: Spec[] = [
  {table: 'issue', ops: []},
  {table: 'issue', system: 'permissions', ops: []},
  {table: 'issue', system: 'test', ops: []},
  // a802d2434: one() and limit(1) differ only in the format.
  {table: 'issue', ops: [['one']]},
  {table: 'issue', ops: [['limit', 1]]},
  {table: 'issue', ops: [['related', 'owner', [['one']]]]},
  {table: 'issue', ops: [['related', 'owner', [['limit', 1]]]]},
  {table: 'issue', ops: [['related', 'comments', []]]},
  {table: 'issue', ops: [['related', 'oneComment', []]]},
  // 3cdfa2eb3: name, args and system.
  {table: 'issue', ops: [['nameAndArgs', 'foo', [1]]]},
  {table: 'issue', ops: [['nameAndArgs', 'bar', [1]]]},
  {table: 'issue', ops: [['nameAndArgs', 'foo', [2]]]},
  {table: 'issue', ops: [['nameAndArgs', 'foo', []]]},
  {table: 'issue', ops: [['nameAndArgs', '', []]]},
  {table: 'issue', ops: [['nameAndArgs', 'foo', [{b: 1, a: [null, true]}]]]},
  {table: 'issue', system: 'permissions', ops: [['nameAndArgs', 'foo', [1]]]},
  {
    table: 'issue',
    ops: [
      ['nameAndArgs', 'foo', [1]],
      ['where', 'id', '=', 'x'],
    ],
  },
  // b2d4e4b30: the junction of a two-hop relationship.
  {table: 'issue', ops: [['related', 'labels', []]]},
  {
    table: 'issue',
    ops: [['related', 'labels', [['where', 'name', '=', 'bug']]]],
  },
  {table: 'label', ops: [['related', 'issues', []]]},
  {table: 'issue', ops: [['whereExists', 'labels', null, []]]},
  {
    table: 'issue',
    ops: [
      ['whereExists', 'labels', {flip: true}, [['where', 'name', '=', 'x']]],
    ],
  },
  {
    table: 'issue',
    ops: [['whereExists', 'labels', {scalar: true}, []]],
  },
  {
    table: 'issue',
    ops: [['related', 'labels', [['related', 'issues', [['one']]]]]],
  },
  // Conditions, flattened and sorted.
  {
    table: 'issue',
    ops: [
      ['where', 'title', '=', 'a'],
      ['where', 'id', '=', 'b'],
    ],
  },
  {
    table: 'issue',
    ops: [
      ['where', 'id', '=', 'b'],
      ['where', 'title', '=', 'a'],
    ],
  },
  {
    table: 'issue',
    ops: [
      [
        'whereExpr',
        {
          or: [
            {cmp: ['title', 'LIKE', '%x%']},
            {and: [{cmp: ['id', '>', 'a']}, {cmp: ['id', '<', 'z']}]},
            {exists: ['comments', null, [['where', 'body', '=', 'hi']]]},
            {not: {exists: ['owner', {flip: false}, []]}},
          ],
        },
      ],
    ],
  },
  {
    table: 'issue',
    ops: [
      [
        'whereExpr',
        {and: [{cmp: ['id', 'IN', ['b', 'a', '\u00e9', 'e\u0301']]}]},
      ],
    ],
  },
  {table: 'issue', ops: [['whereExpr', {and: []}]]},
  {table: 'issue', ops: [['whereExpr', {or: []}]]},
  {
    table: 'issue',
    ops: [['whereExpr', {or: [{and: []}, {cmp: ['id', '=', 'x']}]}]],
  },
  {
    table: 'issue',
    ops: [
      [
        'whereExpr',
        {not: {or: [{cmp: ['id', 'IS', null]}, {cmp: ['id', 'IS NOT', null]}]}},
      ],
    ],
  },
  {table: 'issue', ops: [['whereExpr', {cmpLit: [1, '=', 1]}]]},
  {table: 'issue', ops: [['whereExpr', {cmpLit: [true, '!=', 'a']}]]},
  {
    table: 'issue',
    ops: [
      ['where', 'id', '=', '\u00e9'],
      ['where', 'id', '=', 'e\u0301'],
    ],
  },
  {
    table: 'issue',
    ops: [
      ['where', 'createdAt', '>', 2147483648],
      ['where', 'createdAt', '<', -2147483649],
      ['where', 'createdAt', '!=', 0.5],
      ['where', 'createdAt', '>=', 1e21],
    ],
  },
  // Ordering, bounds and limits.
  {
    table: 'issue',
    ops: [
      ['orderBy', 'title', 'desc'],
      ['orderBy', 'id', 'asc'],
    ],
  },
  {
    table: 'issue',
    ops: [
      ['orderBy', 'id', 'asc'],
      ['start', {id: 'x', title: 'e\u0301'}, false],
    ],
  },
  {table: 'issue', ops: [['start', {id: 'x'}, true]]},
  {
    table: 'issue',
    ops: [['start', {'id': 'x', '\u00e9': 1, 'e\u0301': 2}, true]],
  },
  {table: 'issue', ops: [['limit', 0]]},
  {table: 'issue', ops: [['limit', 4106]]},
  {table: 'issue', ops: [['limit', 2147483647]]},
  // Relationships are kept sorted by alias, whatever order they are added in.
  {
    table: 'issue',
    ops: [
      ['related', 'owner', []],
      ['related', 'comments', []],
      ['related', 'labels', []],
    ],
  },
  {
    table: 'issue',
    ops: [
      ['related', 'labels', []],
      ['related', 'comments', []],
      ['related', 'owner', []],
    ],
  },
  {
    table: 'issue',
    ops: [
      [
        'related',
        'comments',
        [['related', 'revisions', [['related', 'author', []]]]],
      ],
      ['related', 'comments', [['limit', 3]]],
    ],
  },
  {
    table: 'comment',
    ops: [
      ['related', 'issue', [['related', 'labels', []]]],
      ['whereExists', 'author', null, [['whereExists', 'issues', null, []]]],
    ],
  },
];

function relationshipsOf(table: string): string[] {
  return Object.keys(
    (schema.relationships as Record<string, Record<string, unknown>>)[table] ??
      {},
  );
}

function destOf(table: string, rel: string): {dest: string; hops: number} {
  const hops = (
    schema.relationships as Record<
      string,
      Record<string, {destSchema: string}[]>
    >
  )[table][rel];
  return {dest: hops.at(-1)!.destSchema, hops: hops.length};
}

const values: unknown[] = [
  null,
  0,
  1,
  -1.5,
  2147483648,
  'a',
  '',
  '\u00e9',
  'e\u0301',
  '\u{1f600}',
  true,
];
const simpleOps = [
  '=',
  '!=',
  '<',
  '>',
  '<=',
  '>=',
  'LIKE',
  'ILIKE',
  'IS',
  'IS NOT',
];

function randomOps(
  r: Random,
  table: string,
  depth: number,
  junction: boolean,
): Op[] {
  const ops: Op[] = [];
  const columns = Object.keys(
    (schema.tables as Record<string, {columns: Record<string, unknown>}>)[table]
      .columns,
  );
  const n = r.int(depth > 1 ? 2 : 4);
  for (let i = 0; i < n; i++) {
    const rels = relationshipsOf(table);
    switch (r.int(junction ? 4 : 7)) {
      case 0:
        ops.push(['where', r.pick(columns), r.pick(simpleOps), r.pick(values)]);
        break;
      case 1:
        ops.push(['whereExpr', randomExpr(r, table, columns, depth)]);
        break;
      case 2: {
        if (depth > 2 || rels.length === 0) break;
        const rel = r.pick(rels);
        const {dest, hops} = destOf(table, rel);
        ops.push(['related', rel, randomOps(r, dest, depth + 1, hops === 2)]);
        break;
      }
      case 3: {
        if (depth > 2 || rels.length === 0) break;
        const rel = r.pick(rels);
        const {dest, hops} = destOf(table, rel);
        ops.push([
          'whereExists',
          rel,
          r.pick([null, {flip: true}, {flip: false}, {scalar: true}]),
          randomOps(r, dest, depth + 1, hops === 2),
        ]);
        break;
      }
      case 4:
        ops.push(r.bool() ? ['one'] : ['limit', r.int(3)]);
        break;
      case 5:
        ops.push(['orderBy', r.pick(columns), r.bool() ? 'asc' : 'desc']);
        break;
      default:
        ops.push(['start', {[r.pick(columns)]: r.pick(values)}, r.bool()]);
        break;
    }
  }
  return ops;
}

function randomExpr(
  r: Random,
  table: string,
  columns: string[],
  depth: number,
): Expr {
  const k = r.int(depth > 2 ? 2 : 6);
  switch (k) {
    case 0:
    case 1:
      return {cmp: [r.pick(columns), r.pick(simpleOps), r.pick(values)]};
    case 2:
      return {
        and: Array.from({length: r.int(4)}, () =>
          randomExpr(r, table, columns, depth + 1),
        ),
      };
    case 3:
      return {
        or: Array.from({length: r.int(4)}, () =>
          randomExpr(r, table, columns, depth + 1),
        ),
      };
    case 4:
      return {not: randomExpr(r, table, columns, depth + 1)};
    default: {
      const rels = relationshipsOf(table);
      if (rels.length === 0) {
        return {cmp: [r.pick(columns), '=', 'x']};
      }
      const rel = r.pick(rels);
      const {dest, hops} = destOf(table, rel);
      return {
        exists: [
          rel,
          r.pick([null, {flip: true}, {scalar: false}]),
          randomOps(r, dest, depth + 1, hops === 2).filter(
            op => op[0] !== 'one',
          ),
        ],
      };
    }
  }
}

test('sweep-query-hash', () => {
  const r = new Random(0x5eeb);
  const specs = [...handQueries];
  const tables = Object.keys(schema.tables);
  for (let i = 0; i < 300; i++) {
    const table = r.pick(tables);
    const spec: Spec = {table, ops: randomOps(r, table, 0, false)};
    if (r.bool(0.15)) {
      spec.system = r.pick(['permissions', 'test']);
    }
    if (r.bool(0.2)) {
      spec.ops.push(['nameAndArgs', r.pick(['a', 'b']), [r.pick(values)]]);
    }
    specs.push(spec);
  }
  emitVectors(
    'sweep-query-hash',
    'sweep-query-hash',
    'QueryImpl.hash() of queries over the zql test schema, built by a small program of builder calls.',
    specs.map(spec => {
      const q = run(root(spec), spec.ops) as QueryImpl<'issue', typeof schema>;
      return {query: spec, hash: q.hash(), ast: q.ast};
    }),
    undefined,
    {compact: true},
  );
});
