/**
 * Synced relationship-aggregate end to end, simulating the client/server sync
 * boundary at the IVM level.
 *
 * The point of a synced aggregate is to NOT sync the child rows. This works
 * because the two sides build *asymmetric* pipelines from the same AST:
 *
 *   server (compute mode):     issue ── Join ── Aggregate(comment)
 *                                                 └─ consumes comment rows, so
 *                                                    they never flow downstream
 *                                                    (never sync). Emits one
 *                                                    {issueID, value} row per
 *                                                    issue → a synthetic table.
 *
 *   client (aggregatesFromSource): issue ── Join ── <synthetic aggregate source>
 *                                                 └─ the pre-computed values,
 *                                                    synced from the server. No
 *                                                    comment rows on the client.
 *
 * This test wires the boundary by hand: it runs the server pipeline, takes the
 * aggregate rows it would stream, feeds them into the client's synthetic
 * source, and checks the client renders the values while holding zero comment
 * rows. Then it mutates a comment on the server and propagates the resulting
 * edit to the client.
 */
import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {
  Catch,
  type CaughtChildChange,
  type CaughtEditChange,
  type CaughtNode,
} from '../ivm/catch.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  type Source,
} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {aggregateTableName, buildPipeline} from './builder.ts';
import {TestBuilderDelegate} from './test-builder-delegate.ts';

const lc = createSilentLogContext();

const QUERY: AST = {
  table: 'issue',
  orderBy: [['id', 'asc']],
  related: [
    {
      correlation: {parentField: ['id'], childField: ['issueID']},
      aggregate: {fn: 'count'},
      subquery: {table: 'comment', alias: 'comments', orderBy: [['id', 'asc']]},
    },
  ],
};

const QUERY_ID = 'q';
const AGG_TABLE = aggregateTableName(QUERY_ID, must(QUERY.related)[0]);

/** A client that reads aggregates from the synthetic source instead of computing. */
class SyncedClientDelegate extends TestBuilderDelegate {
  readonly aggregatesFromSource = true;
}

function issueSource(): Source {
  const issue = createSource(
    lc,
    testLogConfig,
    'issue',
    {id: {type: 'string'}, title: {type: 'string'}},
    ['id'],
  );
  for (const id of ['1', '2', '3']) {
    consume(issue.push(makeSourceChangeAdd({id, title: `issue ${id}`})));
  }
  return issue;
}

function commentSource(): Source {
  return createSource(
    lc,
    testLogConfig,
    'comment',
    {id: {type: 'string'}, issueID: {type: 'string'}},
    ['id'],
  );
}

function aggSource(): Source {
  return createSource(
    lc,
    testLogConfig,
    AGG_TABLE,
    {issueID: {type: 'string'}, value: {type: 'number'}},
    ['issueID'],
  );
}

/** The {issueID, value} rows hanging off each issue (what the server streams). */
function aggRows(nodes: CaughtNode[]): Row[] {
  return nodes.map(n => {
    if (n === 'yield') {
      throw new Error('unexpected yield');
    }
    const child = (n.relationships.comments as CaughtNode[])[0];
    if (child === 'yield') {
      throw new Error('unexpected yield');
    }
    return child.row;
  });
}

function rowsOf(source: Source): unknown[] {
  return [...source.connect([['id', 'asc']]).fetch({})].filter(
    n => n !== 'yield',
  );
}

test('synced aggregate: server computes, only values cross, client holds no children', () => {
  // ---- SERVER (compute mode: Aggregate over the comment rows) ----
  const serverComment = commentSource();
  // issue 1 -> 2 comments, 2 -> 1, 3 -> 0
  for (const [id, issueID] of [
    ['c1', '1'],
    ['c2', '1'],
    ['c3', '2'],
  ]) {
    consume(serverComment.push(makeSourceChangeAdd({id, issueID})));
  }
  const serverDelegate = new TestBuilderDelegate({
    issue: issueSource(),
    comment: serverComment,
  });
  const serverSink = new Catch(buildPipeline(QUERY, serverDelegate, 'q'));

  // The synthetic aggregate rows the server would stream to AGG_TABLE:
  expect(aggRows(serverSink.fetch())).toEqual([
    {issueID: '1', value: 2},
    {issueID: '2', value: 1},
    {issueID: '3', value: 0},
  ]);

  // ---- SYNC BOUNDARY: only the aggregate rows cross ----
  const synthetic = aggSource();
  for (const row of aggRows(serverSink.fetch())) {
    consume(synthetic.push(makeSourceChangeAdd(row)));
  }

  // ---- CLIENT (source-read mode): issue rows + synthetic values, NO comments ----
  const clientComment = commentSource(); // schema known, zero rows
  const clientDelegate = new SyncedClientDelegate({
    issue: issueSource(),
    comment: clientComment,
    [AGG_TABLE]: synthetic,
  });
  const clientSink = new Catch(buildPipeline(QUERY, clientDelegate, 'q'));

  // Client renders the values...
  expect(aggRows(clientSink.fetch())).toEqual([
    {issueID: '1', value: 2},
    {issueID: '2', value: 1},
    {issueID: '3', value: 0},
  ]);
  // ...while holding zero comment rows.
  expect(rowsOf(clientComment)).toEqual([]);

  // ---- INCREMENTAL: a comment is added on the server ----
  serverSink.reset();
  consume(serverComment.push(makeSourceChangeAdd({id: 'c4', issueID: '3'})));

  // The server emits a CHILD change carrying an EDIT of issue 3's value (0 -> 1).
  expect(serverSink.pushes).toMatchObject([
    {
      type: 'child',
      row: {id: '3'},
      child: {
        relationshipName: 'comments',
        change: {
          type: 'edit',
          oldRow: {issueID: '3', value: 0},
          row: {issueID: '3', value: 1},
        },
      },
    },
  ]);

  // ---- SYNC BOUNDARY: apply the edit to the synthetic source ----
  const edit = (serverSink.pushes[0] as CaughtChildChange).child
    .change as CaughtEditChange;
  clientSink.reset();
  consume(synthetic.push(makeSourceChangeEdit(edit.row, edit.oldRow)));

  // The client incrementally updates issue 3's value — never having seen 'c4'.
  expect(clientSink.pushes).toMatchObject([
    {
      type: 'child',
      row: {id: '3'},
      child: {
        relationshipName: 'comments',
        change: {type: 'edit', row: {issueID: '3', value: 1}},
      },
    },
  ]);
  expect(rowsOf(clientComment)).toEqual([]);
});
