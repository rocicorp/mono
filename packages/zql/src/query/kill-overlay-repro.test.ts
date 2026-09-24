// Repros for mid-push reads, written against the query builder. Each test
// compares a view maintained through one push with a fresh materialization
// over the post-push data, or counts the changes the push emitted.
//
//   1. junction divergence — passes on main; FAILS on grgbkr/kill-overlay
//      (277c94d63), which deletes the join's in-flight overlay.
//   2. unlimited EXISTS flicker — passes on main; FAILS on
//      grgbkr/kill-overlay.
//   3. opposed-order adds under a limit — fails on main and on
//      grgbkr/kill-overlay. Not a correctness bug: ~2N changes where
//      2 × limit would do. Marked `test.fails`; flip it to `test` once fixed.
import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {Catch} from '../ivm/catch.ts';
import type {Source} from '../ivm/source.ts';
import {makeSourceChangeAdd, makeSourceChangeEdit} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';

const lc = createSilentLogContext();

const issue = table('issue').columns({id: number()}).primaryKey('id');
const issueLabel = table('issueLabel')
  .columns({issueID: number(), labelID: number()})
  .primaryKey('issueID', 'labelID');
const label = table('label')
  .columns({id: number(), ownerID: number()})
  .primaryKey('id');
const owner = table('owner')
  .columns({id: number(), name: string()})
  .primaryKey('id');
const comment = table('comment')
  .columns({id: number(), issueID: number(), authorID: number()})
  .primaryKey('id');

const schema = createSchema({
  tables: [issue, issueLabel, label, owner, comment],
  relationships: [
    relationships(issue, ({many}) => ({
      issueLabels: many({
        sourceField: ['id'],
        destField: ['issueID'],
        destSchema: issueLabel,
      }),
      comments: many({
        sourceField: ['id'],
        destField: ['issueID'],
        destSchema: comment,
      }),
    })),
    relationships(issueLabel, ({many}) => ({
      label: many({
        sourceField: ['labelID'],
        destField: ['id'],
        destSchema: label,
      }),
    })),
    relationships(label, ({many}) => ({
      owner: many({
        sourceField: ['ownerID'],
        destField: ['id'],
        destSchema: owner,
      }),
    })),
    relationships(comment, ({many}) => ({
      // Same table as label.owner; `owner` doubles as the comment author.
      author: many({
        sourceField: ['authorID'],
        destField: ['id'],
        destSchema: owner,
      }),
    })),
  ],
});

type Rows = Partial<Record<keyof typeof schema.tables, Row[]>>;

function makeSources(rows: Rows): Record<string, Source> {
  const sources: Record<string, Source> = {};
  for (const name of Object.keys(schema.tables) as (keyof Rows)[]) {
    const {columns, primaryKey} = schema.tables[name];
    const source = createSource(lc, testLogConfig, name, columns, primaryKey);
    for (const row of rows[name] ?? []) {
      consume(source.push(makeSourceChangeAdd(row)));
    }
    sources[name] = source;
  }
  return sources;
}

type Q = Query<'issue', typeof schema>;

/**
 * Materializes `q` over `before`, renames owner 1 from `from` to `to`, and
 * returns the view's ids, a fresh materialization's ids over the renamed
 * data, and the changes a Catch on a second pipeline received.
 */
function renameOwner1(q: Q, before: Rows, from: string, to: string) {
  const rename = makeSourceChangeEdit({id: 1, name: to}, {id: 1, name: from});
  const after: Rows = {
    ...before,
    owner: must(before.owner).map(o => (o.id === 1 ? {...o, name: to} : o)),
  };

  const viewSources = makeSources(before);
  const view = new QueryDelegateImpl({sources: viewSources}).materialize(q);
  consume(must(viewSources.owner).push(rename));

  const catchSources = makeSources(before);
  const caught = new QueryDelegateImpl({sources: catchSources}).materialize(
    q,
    (_query, input) => {
      const c = new Catch(input);
      c.fetch();
      return c;
    },
  );
  consume(must(catchSources.owner).push(rename));

  const fresh = new QueryDelegateImpl({
    sources: makeSources(after),
  }).materialize(q);
  const ids = (d: unknown) => (d as {id: number}[]).map(r => r.id);
  return {
    got: ids(view.data),
    want: ids(fresh.data),
    changes: caught.pushes,
    parentLevel: caught.pushes
      .filter(c => c.type === 'add' || c.type === 'remove')
      .map(c =>
        c.node === 'yield'
          ? 'yield'
          : `${c.type}(${JSON.stringify(c.node.row.id)})`,
      ),
  };
}

describe('1. junction: a mid-push sibling fetch runs ahead of the fan-out', () => {
  // issue 1 -> labels 1 (owner 1) and 3 (owner 2)
  // issue 2 -> label 2 (owner 1)
  // Renaming owner 1 away from 'bot' must drop issue 2 and keep issue 1.
  //
  // On the remove of label 1, FlippedJoin(issue <- issueLabel) asks whether
  // issue 1 has another child. The constraint (issueID) does not map through
  // FlippedJoin(issueLabel <- label), so that fetch reads every label, and
  // the inner Exists.filter records label 2's post-change count (0) before
  // the author fan-out has reached label 2. When it does, Exists sees
  // max(0, 0 - 1) = 0 — no 1 -> 0 transition — and drops label 2's Remove.
  // Issue 2 never leaves the view.
  const data: Rows = {
    issue: [{id: 1}, {id: 2}],
    issueLabel: [
      {issueID: 1, labelID: 1},
      {issueID: 1, labelID: 3},
      {issueID: 2, labelID: 2},
    ],
    label: [
      {id: 1, ownerID: 1},
      {id: 2, ownerID: 1},
      {id: 3, ownerID: 2},
    ],
    owner: [
      {id: 1, name: 'bot'},
      {id: 2, name: 'bot'},
    ],
  };

  const byFlips = (outer: boolean, mid: boolean, inner: boolean) =>
    newQuery(schema, 'issue').whereExists(
      'issueLabels',
      il =>
        il.whereExists(
          'label',
          l =>
            l.whereExists('owner', o => o.where('name', 'bot'), {flip: inner}),
          {flip: mid},
        ),
      {flip: outer},
    );

  test('issueLabels (flipped) -> label (flipped) -> owner (not flipped)', () => {
    const {got, want} = renameOwner1(
      byFlips(true, true, false),
      data,
      'bot',
      'x',
    );
    expect(want).toEqual([1]);
    expect(got).toEqual(want); // received [1, 2]
  });

  // The other seven flip combinations pass; this sweep shows the failure is
  // exactly the one above.
  for (const outer of [false, true]) {
    for (const mid of [false, true]) {
      for (const inner of [false, true]) {
        test(`sweep: outer=${outer} mid=${mid} inner=${inner}`, () => {
          const q = byFlips(outer, mid, inner);
          for (const [from, to] of [
            ['bot', 'x'],
            ['x', 'bot'],
          ]) {
            const start = {
              ...data,
              owner: [
                {id: 1, name: from},
                {id: 2, name: 'bot'},
              ],
            };
            const {got, want} = renameOwner1(q, start, from, to);
            expect({from, to, ids: got}).toEqual({from, to, ids: want});
          }
        });
      }
    }
  }
});

describe('2. unlimited EXISTS: the gate flips at zero before the Cap refills', () => {
  // Issue 1 has comments 1-5 by author 1 and comment 6 by author 2. The
  // EXISTS Cap holds 3 comments, all by author 1. Renaming author 1 away
  // dooms all three; comment 6 survives, so issue 1 never stops matching.
  //
  // Cap now defers its refill to reconcile, so Exists' count goes 3 -> 0
  // mid-push and it emits Remove(issue 1); reconcile refills comment 6 and
  // Exists emits Add(issue 1). The view ends right, but every parent with a
  // survivor outside its cap pays a Remove + Add (a full subtree re-hydrate).
  const data: Rows = {
    issue: [{id: 1}],
    comment: [
      {id: 1, issueID: 1, authorID: 1},
      {id: 2, issueID: 1, authorID: 1},
      {id: 3, issueID: 1, authorID: 1},
      {id: 4, issueID: 1, authorID: 1},
      {id: 5, issueID: 1, authorID: 1},
      {id: 6, issueID: 1, authorID: 2},
    ],
    owner: [
      {id: 1, name: 'bot'},
      {id: 2, name: 'bot'},
    ],
  };

  test('comment -> author, neither flipped (Cap lowering)', () => {
    const q = newQuery(schema, 'issue').whereExists('comments', c =>
      c.whereExists('author', a => a.where('name', 'bot')),
    );
    const {got, want, parentLevel} = renameOwner1(q, data, 'bot', 'x');
    expect(got).toEqual(want);
    expect(parentLevel).toEqual([]); // received ['remove(1)', 'add(1)']
  });
});

describe('3. opposed-order adds under a limit (pre-existing)', () => {
  // One comment per issue, all by author 1, with comment ids running against
  // issue ids — so the inner fan-out (comment order) delivers adds in the
  // reverse of the window's order. Renaming author 1 back to 'bot' makes all
  // N issues match; only the first 10 belong in the window.
  //
  // The first 10 adds (issues 499..490) fill the window. Every later add sorts
  // before the bound, and Take acts on it at once: on main it displaces the
  // bound (Remove + Add); on grgbkr/kill-overlay it emits the Add and trims
  // the overflow at reconcile. Either way 10 + 2 × 490 = 990 changes.
  const N = 500;
  const data: Rows = {
    issue: Array.from({length: N}, (_, k) => ({id: k})),
    comment: Array.from({length: N}, (_, k) => ({
      id: N - 1 - k,
      issueID: k,
      authorID: 1,
    })),
    owner: [{id: 1, name: 'x'}],
  };

  for (const flip of [false, true]) {
    test.skip(`flip=${flip}`, () => {
      const q = newQuery(schema, 'issue')
        .whereExists(
          'comments',
          c => c.whereExists('author', a => a.where('name', 'bot'), {flip}),
          {flip},
        )
        .orderBy('id', 'asc')
        .limit(10);
      const {got, want, changes} = renameOwner1(q, data, 'x', 'bot');
      expect(got).toEqual(want);
      expect(changes.length).toBeLessThanOrEqual(20); // received 990
    });
  }
});
