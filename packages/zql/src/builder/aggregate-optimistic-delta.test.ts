/**
 * The builder side of optimistic aggregate deltas: in `aggregatesFromSource`
 * mode (the synced client), a relationship aggregate registers metadata that
 * lets a local child mutation bump the synced value without a server round-trip.
 *
 * A `where` on the aggregate's child is honored by compiling it to a per-row
 * predicate — but only when the whole `where` is per-row evaluable. A correlated
 * subquery in the `where` can't be judged from a single child row, so the
 * aggregate stays server-authoritative (no optimistic delta at all). These tests
 * pin down which `where`s register a predicate vs. fall back.
 */
import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  AggregateFunction,
  AST,
  CompoundKey,
  Condition,
} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-types/src/schema-value.ts';
import type {Source} from '../ivm/source.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {aggregateTableName, buildPipeline} from './builder.ts';
import {TestBuilderDelegate} from './test-builder-delegate.ts';

const lc = createSilentLogContext();

type CapturedDelta =
  | {
      readonly table: string;
      readonly childField: CompoundKey;
      readonly fn: AggregateFunction;
      readonly field: string | undefined;
      readonly predicate: ((row: Row) => boolean) | undefined;
    }
  | undefined;

/**
 * A synced client that reads aggregates from the synthetic source and records
 * the `optimisticDelta` the builder passes when provisioning it.
 */
class CapturingClientDelegate extends TestBuilderDelegate {
  readonly aggregatesFromSource = true;
  readonly #aggTable: string;
  called = false;
  captured: CapturedDelta;

  constructor(sources: Readonly<Record<string, Source>>, aggTable: string) {
    super(sources);
    this.#aggTable = aggTable;
  }

  // Report the synthetic source as not-yet-existing so the builder takes the
  // getAggregateSource provisioning branch (where the delta is registered).
  override getSource(name: string): Source | undefined {
    return name === this.#aggTable ? undefined : super.getSource(name);
  }

  getAggregateSource(
    name: string,
    columns: Record<string, SchemaValue>,
    primaryKey: PrimaryKey,
    optimisticDelta?: CapturedDelta,
  ): Source {
    this.called = true;
    this.captured = optimisticDelta;
    return createSource(lc, testLogConfig, name, columns, primaryKey);
  }
}

function makeDelegate(aggTable: string): CapturingClientDelegate {
  return new CapturingClientDelegate(
    {
      issue: createSource(
        lc,
        testLogConfig,
        'issue',
        {id: {type: 'string'}, title: {type: 'string'}},
        ['id'],
      ),
      comment: createSource(
        lc,
        testLogConfig,
        'comment',
        {
          id: {type: 'string'},
          issueID: {type: 'string'},
          points: {type: 'number'},
        },
        ['id'],
      ),
    },
    aggTable,
  );
}

/** `issue.related('comments', c => c?.where?.(...).count())`. */
function countQuery(where?: Condition): AST {
  return {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [
      {
        correlation: {parentField: ['id'], childField: ['issueID']},
        aggregate: {fn: 'count'},
        subquery: {
          table: 'comment',
          alias: 'comments',
          orderBy: [['id', 'asc']],
          where,
        },
      },
    ],
  };
}

function build(query: AST): CapturingClientDelegate {
  const aggTable = aggregateTableName('q', must(query.related)[0]);
  const delegate = makeDelegate(aggTable);
  buildPipeline(query, delegate, 'q');
  return delegate;
}

test('no where → optimistic delta registered with no predicate', () => {
  const {called, captured} = build(countQuery());
  expect(called).toBe(true);
  expect(captured).toMatchObject({table: 'comment', fn: 'count'});
  expect(captured?.predicate).toBeUndefined();
});

test('simple where → optimistic delta registered with a working predicate', () => {
  const {captured} = build(
    countQuery({
      type: 'simple',
      left: {type: 'column', name: 'points'},
      op: '>=',
      right: {type: 'literal', value: 5},
    }),
  );
  expect(captured).toMatchObject({table: 'comment', fn: 'count'});
  const {predicate} = must(captured);
  expect(predicate).toBeTypeOf('function');
  expect(must(predicate)({id: 'c', issueID: 'i', points: 9})).toBe(true);
  expect(must(predicate)({id: 'c', issueID: 'i', points: 1})).toBe(false);
});

test('correlated-subquery where → no optimistic delta (server-authoritative)', () => {
  const {called, captured} = build(
    countQuery({
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        correlation: {parentField: ['id'], childField: ['issueID']},
        subquery: {table: 'comment', alias: 'sub', orderBy: [['id', 'asc']]},
      },
    }),
  );
  // The synthetic source is still provisioned, just without optimism.
  expect(called).toBe(true);
  expect(captured).toBeUndefined();
});
