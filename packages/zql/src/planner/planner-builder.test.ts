import {expect, suite, test} from 'vitest';
import {buildPlanGraph, planQuery} from './planner-builder.ts';
import {
  simpleCostModel,
  createAST,
  createExistsCondition,
  createCorrelatedSubquery,
} from './test/helpers.ts';
import {planIdSymbol} from '../../../zero-protocol/src/ast.ts';

suite('buildPlanGraph', () => {
  test('creates graph with single connection for simple query', () => {
    const ast = createAST('users');

    const {plan, subPlans} = buildPlanGraph(ast, simpleCostModel);

    expect(plan.connections).toHaveLength(1);
    expect(plan.joins).toHaveLength(0);
    expect(plan.fanOuts).toHaveLength(0);
    expect(plan.fanIns).toHaveLength(0);
    expect(subPlans).toEqual({});
  });

  test('creates join for EXISTS condition', () => {
    const subquery = createAST('posts', {
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'published'},
        right: {type: 'literal', value: true},
      },
    });

    const correlated = createCorrelatedSubquery(subquery, ['id'], ['userId']);

    const ast = createAST('users', {
      where: createExistsCondition(correlated, 'EXISTS'),
    });

    const {plan} = buildPlanGraph(ast, simpleCostModel);

    expect(plan.connections).toHaveLength(2); // users + posts
    expect(plan.joins).toHaveLength(1);
    expect(plan.joins[0].type).toBe('left');
  });

  test('assigns unique plan IDs to joins and AST nodes', () => {
    const subquery = createAST('posts');
    const correlated = createCorrelatedSubquery(subquery, ['id'], ['userId']);
    const condition = createExistsCondition(correlated, 'EXISTS');

    const ast = createAST('users', {
      where: condition,
    });

    const {plan} = buildPlanGraph(ast, simpleCostModel);

    // Check that the join has a plan ID
    expect(plan.joins[0].planId).toBeDefined();

    // Check that the AST condition has the same plan ID
    const astPlanId = (condition as unknown as Record<symbol, number>)[
      planIdSymbol
    ];
    expect(astPlanId).toBe(plan.joins[0].planId);
  });

  test('creates AND joins sequentially', () => {
    const subquery1 = createAST('posts');
    const correlated1 = createCorrelatedSubquery(subquery1, ['id'], ['userId']);
    const condition1 = createExistsCondition(correlated1, 'EXISTS');

    const subquery2 = createAST('comments');
    const correlated2 = createCorrelatedSubquery(subquery2, ['id'], ['userId']);
    const condition2 = createExistsCondition(correlated2, 'EXISTS');

    const ast = createAST('users', {
      where: {
        type: 'and',
        conditions: [condition1, condition2],
      },
    });

    const {plan} = buildPlanGraph(ast, simpleCostModel);

    expect(plan.connections).toHaveLength(3); // users + posts + comments
    expect(plan.joins).toHaveLength(2);
  });

  test('creates FanOut/FanIn for OR with subqueries', () => {
    const subquery1 = createAST('posts');
    const correlated1 = createCorrelatedSubquery(subquery1, ['id'], ['userId']);
    const condition1 = createExistsCondition(correlated1, 'EXISTS');

    const subquery2 = createAST('comments');
    const correlated2 = createCorrelatedSubquery(subquery2, ['id'], ['userId']);
    const condition2 = createExistsCondition(correlated2, 'EXISTS');

    const ast = createAST('users', {
      where: {
        type: 'or',
        conditions: [condition1, condition2],
      },
    });

    const {plan} = buildPlanGraph(ast, simpleCostModel);

    expect(plan.connections).toHaveLength(3); // users + posts + comments
    expect(plan.joins).toHaveLength(2);
    expect(plan.fanOuts).toHaveLength(1);
    expect(plan.fanIns).toHaveLength(1);
  });

  test('skips FanOut/FanIn for OR without subqueries', () => {
    const ast = createAST('users', {
      where: {
        type: 'or',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'name'},
            right: {type: 'literal', value: 'Alice'},
          },
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'name'},
            right: {type: 'literal', value: 'Bob'},
          },
        ],
      },
    });

    const {plan} = buildPlanGraph(ast, simpleCostModel);

    expect(plan.connections).toHaveLength(1); // Only users
    expect(plan.joins).toHaveLength(0);
    expect(plan.fanOuts).toHaveLength(0); // No fan nodes for simple conditions
    expect(plan.fanIns).toHaveLength(0);
  });

  test('creates subPlans for related queries', () => {
    const relatedSubquery = createAST('posts', {alias: 'userPosts'});
    const correlated = createCorrelatedSubquery(
      relatedSubquery,
      ['id'],
      ['userId'],
    );

    const ast = createAST('users', {
      related: [correlated],
    });

    const {plan, subPlans} = buildPlanGraph(ast, simpleCostModel);

    expect(plan.connections).toHaveLength(1); // Only main query
    expect(Object.keys(subPlans)).toHaveLength(1);
    expect(subPlans['userPosts']).toBeDefined();
    expect(subPlans['userPosts'].plan.connections).toHaveLength(1);
  });

  test('NOT EXISTS join is not flippable', () => {
    const subquery = createAST('posts');
    const correlated = createCorrelatedSubquery(subquery, ['id'], ['userId']);
    const condition = createExistsCondition(correlated, 'NOT EXISTS');

    const ast = createAST('users', {
      where: condition,
    });

    const {plan} = buildPlanGraph(ast, simpleCostModel);

    expect(plan.joins).toHaveLength(1);
    // The join should be created but we can't directly test flippability
    // That's tested in the planner algorithm tests
    expect(() => plan.joins[0].flip()).toThrow();
  });
});

suite('planQuery', () => {
  test('returns AST with joins planned', () => {
    // Create a query WITH a join so planning can actually run
    const subquery = createAST('posts');
    const correlated = createCorrelatedSubquery(subquery, ['id'], ['userId']);
    const condition = createExistsCondition(correlated, 'EXISTS');

    const ast = createAST('users', {
      where: condition,
    });

    const result = planQuery(ast, simpleCostModel);

    // The result should have the same structure
    expect(result.table).toBe('users');
    expect(result.where).toBeDefined();
  });

  test('applies flip flags to AST', () => {
    // Create a query where the planner might flip a join
    const subquery = createAST('posts');
    const correlated = createCorrelatedSubquery(subquery, ['id'], ['userId']);
    const condition = createExistsCondition(correlated, 'EXISTS');

    const ast = createAST('users', {
      where: condition,
    });

    const result = planQuery(ast, simpleCostModel);

    // The result should have the same structure but potentially with flip flags
    expect(result.table).toBe('users');
    expect(result.where).toBeDefined();

    // flip flag is either true or undefined (not set)
    if (result.where && result.where.type === 'correlatedSubquery') {
      expect([true, undefined]).toContain(result.where.flip);
    }
  });

  test('recursively plans subPlans with joins', () => {
    // Create a related subquery WITH a join so it can be planned
    const nestedSubquery = createAST('comments');
    const nestedCorrelated = createCorrelatedSubquery(
      nestedSubquery,
      ['id'],
      ['postId'],
    );
    const nestedCondition = createExistsCondition(nestedCorrelated, 'EXISTS');

    const relatedSubquery = createAST('posts', {
      alias: 'userPosts',
      where: nestedCondition,
    });
    const correlated = createCorrelatedSubquery(
      relatedSubquery,
      ['id'],
      ['userId'],
    );

    const ast = createAST('users', {
      related: [correlated],
    });

    const result = planQuery(ast, simpleCostModel);

    expect(result.related).toHaveLength(1);
    expect(result.related![0].subquery.alias).toBe('userPosts');
  });
});
