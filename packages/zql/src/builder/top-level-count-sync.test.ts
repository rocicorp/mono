/**
 * Synced top-level (ungrouped) aggregate end to end, simulating the
 * client/server sync boundary at the IVM level. This is the top-level analog of
 * count-sync.test.ts (which covers relationship aggregates).
 *
 * A top-level aggregate's result is a single scalar. The two sides build
 * *asymmetric* pipelines from the same AST:
 *
 *   server (compute mode):    issue ── Aggregate([], count)
 *                                        └─ consumes ALL issue rows, so they
 *                                           never flow downstream (never sync).
 *                                           Emits ONE synthetic row
 *                                           {'': 0, value} → a synthetic table
 *                                           `aggregate:<queryID>`.
 *
 *   client (aggregatesFromSource): <synthetic aggregate source> (one row)
 *                                        └─ the precomputed value, synced from
 *                                           the server. The client has no issue
 *                                           source at all.
 *
 * This test wires the boundary by hand: it runs the server pipeline, takes the
 * single aggregate row it would stream, feeds it into the client's synthetic
 * source, and checks the client renders the value while holding zero issue rows.
 * Then it adds an issue on the server and propagates the resulting edit.
 */
import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {AGGREGATE_KEY_COLUMN} from '../ivm/aggregate.ts';
import {Catch, type CaughtEditChange, type CaughtNode} from '../ivm/catch.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  type Source,
} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {buildPipeline, topLevelAggregateTableName} from './builder.ts';
import {TestBuilderDelegate} from './test-builder-delegate.ts';

const lc = createSilentLogContext();

const QUERY: AST = {
  table: 'issue',
  aggregate: {fn: 'count'},
};

const QUERY_ID = 'q';
const AGG_TABLE = topLevelAggregateTableName(QUERY_ID);

/** A client that reads the aggregate from the synthetic source instead of computing. */
class SyncedClientDelegate extends TestBuilderDelegate {
  readonly aggregatesFromSource = true;
}

function issueSource(ids: readonly string[]): Source {
  const issue = createSource(
    lc,
    testLogConfig,
    'issue',
    {id: {type: 'string'}, title: {type: 'string'}},
    ['id'],
  );
  for (const id of ids) {
    consume(issue.push(makeSourceChangeAdd({id, title: `issue ${id}`})));
  }
  return issue;
}

function aggSource(): Source {
  return createSource(
    lc,
    testLogConfig,
    AGG_TABLE,
    {[AGGREGATE_KEY_COLUMN]: {type: 'number'}, value: {type: 'number'}},
    [AGGREGATE_KEY_COLUMN],
  );
}

/** The single {'': 0, value} synthetic row at the root of the server pipeline. */
function aggRow(nodes: CaughtNode[]): Row {
  expect(nodes).toHaveLength(1);
  const n = nodes[0];
  if (n === 'yield') {
    throw new Error('unexpected yield');
  }
  return n.row;
}

test('synced top-level count: server computes, only the scalar crosses, client holds no rows', () => {
  // ---- SERVER (compute mode: Aggregate over all issue rows) ----
  const serverIssue = issueSource(['1', '2', '3']); // 3 issues
  const serverDelegate = new TestBuilderDelegate({issue: serverIssue});
  const serverSink = new Catch(buildPipeline(QUERY, serverDelegate, QUERY_ID));

  // The single synthetic aggregate row the server would stream to AGG_TABLE:
  expect(aggRow(serverSink.fetch())).toEqual({
    [AGGREGATE_KEY_COLUMN]: 0,
    value: 3,
  });

  // ---- SYNC BOUNDARY: only the one aggregate row crosses ----
  const synthetic = aggSource();
  consume(synthetic.push(makeSourceChangeAdd(aggRow(serverSink.fetch()))));

  // ---- CLIENT (source-read mode): NO issue source at all, only the synthetic
  // aggregate source. The value can only come from the synced row. ----
  const clientDelegate = new SyncedClientDelegate({[AGG_TABLE]: synthetic});
  const clientSink = new Catch(buildPipeline(QUERY, clientDelegate, QUERY_ID));

  expect(aggRow(clientSink.fetch())).toEqual({
    [AGGREGATE_KEY_COLUMN]: 0,
    value: 3,
  });

  // ---- INCREMENTAL: an issue is added on the server ----
  serverSink.reset();
  consume(serverIssue.push(makeSourceChangeAdd({id: '4', title: 'issue 4'})));

  // The server emits an EDIT of the single value (3 -> 4).
  expect(serverSink.pushes).toMatchObject([
    {
      type: 'edit',
      oldRow: {[AGGREGATE_KEY_COLUMN]: 0, value: 3},
      row: {[AGGREGATE_KEY_COLUMN]: 0, value: 4},
    },
  ]);

  // ---- SYNC BOUNDARY: apply the edit to the synthetic source ----
  const edit = serverSink.pushes[0] as CaughtEditChange;
  clientSink.reset();
  consume(synthetic.push(makeSourceChangeEdit(edit.row, edit.oldRow)));

  // The client incrementally updates the value — never having seen issue '4'.
  expect(clientSink.pushes).toMatchObject([
    {
      type: 'edit',
      oldRow: {[AGGREGATE_KEY_COLUMN]: 0, value: 3},
      row: {[AGGREGATE_KEY_COLUMN]: 0, value: 4},
    },
  ]);
});
