import {LogContext} from '@rocicorp/logger';
import {expect, suite, test} from 'vitest';
import {TestLogSink} from '../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {asQueryInternals} from '../query/query-internals.ts';
import type {AnyQuery} from '../query/query.ts';
import {buildPlanGraph} from './planner-builder.ts';
import type {ConnectionCostModel} from './planner-connection.ts';
import {builder} from './test/test-schema.ts';

function getAST(q: AnyQuery): AST {
  return asQueryInternals(q).ast;
}

const fanout = () => ({fanout: 1, confidence: 'none'}) as const;

// A constraint is an index seek (1 row); an unconstrained scan returns the
// whole table.
function sized(sizes: Record<string, number>): ConnectionCostModel {
  return (table, _sort, _filters, constraint) => ({
    startupCost: 0,
    rows:
      constraint && Object.keys(constraint).length > 0
        ? 1
        : (sizes[table] ?? 100),
    fanout,
  });
}

function planAndCollectWarnings(ast: AST, model: ConnectionCostModel) {
  const sink = new TestLogSink();
  const lc = new LogContext('warn', undefined, sink);
  const plans = buildPlanGraph(ast, model, true);
  plans.plan.plan(undefined, lc);
  const warnings = sink.messages
    .filter(([level]) => level === 'warn')
    .map(([, , args]) => String(args[0]));
  return {plans, warnings};
}

suite('manual flip cost warning', () => {
  test('warns when {flip: true} drives from the larger side', () => {
    const ast = getAST(builder.users.whereExists('posts', {flip: true}));
    const {warnings} = planAndCollectWarnings(
      ast,
      sized({users: 10, posts: 1_000_000}),
    );
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain('{flip: true} on users ⋈ posts');
  });

  test('is quiet when {flip: true} drives from the smaller side', () => {
    const ast = getAST(builder.users.whereExists('posts', {flip: true}));
    const {warnings} = planAndCollectWarnings(
      ast,
      sized({users: 1_000_000, posts: 10}),
    );
    expect(warnings).toHaveLength(0);
  });

  test('is quiet when there is no manual flip', () => {
    const ast = getAST(builder.users.whereExists('posts'));
    const {warnings} = planAndCollectWarnings(
      ast,
      sized({users: 10, posts: 1_000_000}),
    );
    expect(warnings).toHaveLength(0);
  });

  test('is quiet without a LogContext', () => {
    const ast = getAST(builder.users.whereExists('posts', {flip: true}));
    const plans = buildPlanGraph(
      ast,
      sized({users: 10, posts: 1_000_000}),
      true,
    );
    expect(() => plans.plan.plan()).not.toThrow();
  });

  test('warns per manual flip and leaves the chosen plan intact', () => {
    // The incident shape: two manual flips inside an OR (branches do not
    // constrain each other), plus one join left to the planner. Only the bad
    // flip should warn, and no join type should change.
    const ast = getAST(
      builder.users
        .where(({or, exists}) =>
          or(
            exists('posts', q => q, {flip: true}),
            exists('comments', q => q, {flip: true}),
          ),
        )
        .whereExists('likes'),
    );
    // likes is large so the planner keeps it semi; a flipped likes would push
    // its constraint into the OR branches and make the posts flip cheap.
    const model = sized({
      users: 100,
      posts: 1_000_000,
      comments: 1,
      likes: 1_000_000,
    });

    const silent = buildPlanGraph(ast, model, true);
    silent.plan.plan();
    const expectedTypes = silent.plan.joins.map(j => j.type);
    expect(expectedTypes).toEqual(['flipped', 'flipped', 'semi']);

    const {plans, warnings} = planAndCollectWarnings(ast, model);
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain('FO ⋈ posts');
    expect(plans.plan.joins.map(j => j.type)).toEqual(expectedTypes);
  });
});
