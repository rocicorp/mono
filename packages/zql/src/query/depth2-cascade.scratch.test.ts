// SCRATCH — not for commit. Mirror of rindle's tests/scratch_depth2_cascade.rs:
// does the cascade survive when the join that fans out sits one EXISTS level BELOW
// the limited table?
//
//   issue.whereExists(comments, c => c.whereExists(author, name = 'bot'))
//        .orderBy(id).limit(10)
import {appendFileSync} from 'node:fs';
import {expect, test} from 'vitest';
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
import type {Input} from '../ivm/operator.ts';
import type {Source} from '../ivm/source.ts';
import {makeSourceChangeAdd, makeSourceChangeEdit} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {newQuery} from './query-impl.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';

const lc = createSilentLogContext();

const issue = table('issue').columns({id: number()}).primaryKey('id');
const comment = table('comment')
  .columns({id: number(), issueID: number(), authorID: number()})
  .primaryKey('id');
const author = table('author')
  .columns({id: number(), name: string()})
  .primaryKey('id');

const schema = createSchema({
  tables: [issue, comment, author],
  relationships: [
    relationships(issue, ({many}) => ({
      comments: many({
        sourceField: ['id'],
        destField: ['issueID'],
        destSchema: comment,
      }),
    })),
    relationships(comment, ({many}) => ({
      author: many({
        sourceField: ['authorID'],
        destField: ['id'],
        destSchema: author,
      }),
    })),
  ],
});

function makeSources(
  n: number,
  survivors: number,
  reversed: boolean,
): Record<string, Source> {
  const total = n + survivors;
  const rows: Record<string, Row[]> = {issue: [], comment: [], author: []};
  for (let i = 1; i <= total; i++) {
    rows.issue.push({id: i});
    rows.comment.push({
      id: i,
      issueID: i <= n && reversed ? n + 1 - i : i,
      authorID: i <= n ? 1 : 2,
    });
  }
  rows.author.push({id: 1, name: 'bot'}, {id: 2, name: 'bot'});
  const sources: Record<string, Source> = {};
  for (const name of ['issue', 'comment', 'author'] as const) {
    const {columns, primaryKey} = schema.tables[name];
    const source = createSource(lc, testLogConfig, name, columns, primaryKey);
    for (const row of rows[name]) {
      consume(source.push(makeSourceChangeAdd(row)));
    }
    sources[name] = source;
  }
  return sources;
}

function run(n: number, survivors: number, flip: boolean, reversed: boolean) {
  const q = newQuery(schema, 'issue')
    .whereExists(
      'comments',
      c => c.whereExists('author', a => a.where('name', 'bot'), {flip}),
      {flip},
    )
    .orderBy('id', 'asc')
    .limit(10);
  const sources = makeSources(n, survivors, reversed);
  let pushes = 0;
  let top: Input | undefined;
  new QueryDelegateImpl({sources}).materialize(q, (_q, input) => {
    top = input;
    input.setOutput({
      push() {
        pushes++;
        return [];
      },
    });
    let hydrated = 0;
    for (const node of input.fetch({})) {
      if (node !== 'yield') {
        hydrated++;
      }
    }
    expect(hydrated).toBe(10);
    return undefined;
  });
  const size = () => {
    let c = 0;
    for (const node of must(top).fetch({})) {
      if (node !== 'yield') {
        c++;
      }
    }
    return c;
  };

  let t = performance.now();
  consume(
    must(sources.author).push(
      makeSourceChangeEdit({id: 1, name: 'bot2'}, {id: 1, name: 'bot'}),
    ),
  );
  const awayMs = performance.now() - t;
  const away = pushes;
  expect(size()).toBe(Math.min(survivors, 10));

  t = performance.now();
  consume(
    must(sources.author).push(
      makeSourceChangeEdit({id: 1, name: 'bot'}, {id: 1, name: 'bot2'}),
    ),
  );
  const backMs = performance.now() - t;
  const back = pushes - away;
  expect(size()).toBe(10);

  appendFileSync(
    '/private/tmp/claude-501/-Users-mlaw-workspace-rindle-ws-1/2886b268-a56f-457b-914f-8d17e6b13c8c/scratchpad/zero-depth2.txt',
    `flip=${String(flip).padEnd(5)} ${reversed ? 'opposed' : 'aligned'} N=${String(n).padStart(5)} surv=${String(survivors).padStart(2)} | away ${awayMs.toFixed(1).padStart(9)}ms ${String(away).padStart(6)} changes | back ${backMs.toFixed(1).padStart(9)}ms ${String(back).padStart(6)} changes\n`,
  );
}

test('depth-2 exists chain rename', {timeout: 600_000}, () => {
  for (const flip of [false, true]) {
    for (const reversed of [false, true]) {
      for (const survivors of [0, 50]) {
        run(2000, survivors, flip, reversed);
      }
    }
  }
});
