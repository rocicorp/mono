/**
 * End-to-end prototype of the relationship-aggregate path:
 *   AST (related + aggregate) -> buildPipeline -> Join(child=Aggregate) -> output.
 *
 * Demonstrates both initial hydration (aggregate computed at fetch) and
 * incremental maintenance (a comment add/remove surfaces as a CHILD(EDIT) of
 * the singular aggregate relationship on the affected issue only).
 */
import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {Catch} from '../ivm/catch.ts';
import {makeSourceChangeAdd, makeSourceChangeRemove} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {buildPipeline} from './builder.ts';
import {TestBuilderDelegate} from './test-builder-delegate.ts';

const lc = createSilentLogContext();

function setup() {
  const issue = createSource(
    lc,
    testLogConfig,
    'issue',
    {id: {type: 'number'}, title: {type: 'string'}},
    ['id'],
  );
  consume(issue.push(makeSourceChangeAdd({id: 1, title: 'a'})));
  consume(issue.push(makeSourceChangeAdd({id: 2, title: 'b'})));
  consume(issue.push(makeSourceChangeAdd({id: 3, title: 'c'})));

  const comment = createSource(
    lc,
    testLogConfig,
    'comment',
    {id: {type: 'number'}, issueID: {type: 'number'}, text: {type: 'string'}},
    ['id'],
  );
  // issue 1 -> 2 comments, issue 2 -> 1 comment, issue 3 -> 0 comments.
  consume(comment.push(makeSourceChangeAdd({id: 1, issueID: 1, text: 'a'})));
  consume(comment.push(makeSourceChangeAdd({id: 2, issueID: 1, text: 'b'})));
  consume(comment.push(makeSourceChangeAdd({id: 3, issueID: 2, text: 'c'})));

  const delegate = new TestBuilderDelegate({issue, comment});

  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [
      {
        correlation: {parentField: ['id'], childField: ['issueID']},
        aggregate: {fn: 'count'},
        subquery: {
          table: 'comment',
          alias: 'commentCount',
          orderBy: [['id', 'asc']],
        },
      },
    ],
  };

  const out = new Catch(buildPipeline(ast, delegate, 'count-query'));
  return {issue, comment, out};
}

test('hydration: each issue gets a singular aggregate row', () => {
  const {out} = setup();
  expect(out.fetch()).toEqual([
    {
      row: {id: 1, title: 'a'},
      relationships: {
        commentCount: [{row: {issueID: 1, value: 2}, relationships: {}}],
      },
    },
    {
      row: {id: 2, title: 'b'},
      relationships: {
        commentCount: [{row: {issueID: 2, value: 1}, relationships: {}}],
      },
    },
    {
      row: {id: 3, title: 'c'},
      relationships: {
        commentCount: [{row: {issueID: 3, value: 0}, relationships: {}}],
      },
    },
  ]);
});

test('adding a comment edits only the affected issue’s count', () => {
  const {comment, out} = setup();
  out.fetch(); // materialize so counts are tracked
  out.reset();

  consume(comment.push(makeSourceChangeAdd({id: 10, issueID: 3, text: 'new'})));

  expect(out.pushes).toEqual([
    {
      type: 'child',
      row: {id: 3, title: 'c'},
      child: {
        relationshipName: 'commentCount',
        change: {
          type: 'edit',
          oldRow: {issueID: 3, value: 0},
          row: {issueID: 3, value: 1},
        },
      },
    },
  ]);
});

test('removing a comment decrements the affected issue’s count', () => {
  const {comment, out} = setup();
  out.fetch();
  out.reset();

  consume(comment.push(makeSourceChangeRemove({id: 1, issueID: 1, text: 'a'})));

  expect(out.pushes).toEqual([
    {
      type: 'child',
      row: {id: 1, title: 'a'},
      child: {
        relationshipName: 'commentCount',
        change: {
          type: 'edit',
          oldRow: {issueID: 1, value: 2},
          row: {issueID: 1, value: 1},
        },
      },
    },
  ]);
});
