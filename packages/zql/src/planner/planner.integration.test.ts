import {expect, suite, test} from 'vitest';
import {planQuery, buildPlanGraph} from './planner-builder.ts';
import {
  createAST,
  createExistsCondition,
  createCorrelatedSubquery,
  simpleCostModel,
  predictableCostModel,
  linearChainCostModel,
  starSchemaCostModel,
  diamondCostModel,
  wideNarrowCostModel,
  deepNestingCostModel,
} from './test/helpers.ts';

suite('Planner Integration Tests', () => {
  test('plans simple EXISTS query', () => {
    // Query: SELECT * FROM users WHERE EXISTS (SELECT * FROM posts WHERE posts.userId = users.id)
    const postsSubquery = createAST('posts');
    const correlated = createCorrelatedSubquery(postsSubquery, ['id'], ['userId']);
    const condition = createExistsCondition(correlated, 'EXISTS');

    const ast = createAST('users', {
      where: condition,
    });

    const result = planQuery(ast, simpleCostModel);

    // Should return an AST with the same structure
    expect(result.table).toBe('users');
    expect(result.where).toBeDefined();
    expect(result.where!.type).toBe('correlatedSubquery');

    // flip flag should be set (either true or undefined depending on planner decision)
    if (result.where!.type === 'correlatedSubquery') {
      expect(result.where!.flip === true || result.where!.flip === undefined).toBe(true);
    }
  });

  test('plans AND with multiple EXISTS', () => {
    // Query: users WHERE EXISTS posts AND EXISTS comments
    const postsSubquery = createAST('posts');
    const postsCorrelated = createCorrelatedSubquery(postsSubquery, ['id'], ['userId']);
    const postsCondition = createExistsCondition(postsCorrelated, 'EXISTS');

    const commentsSubquery = createAST('comments');
    const commentsCorrelated = createCorrelatedSubquery(commentsSubquery, ['id'], ['userId']);
    const commentsCondition = createExistsCondition(commentsCorrelated, 'EXISTS');

    const ast = createAST('users', {
      where: {
        type: 'and',
        conditions: [postsCondition, commentsCondition],
      },
    });

    const result = planQuery(ast, simpleCostModel);

    expect(result.table).toBe('users');
    expect(result.where).toBeDefined();
    expect(result.where!.type).toBe('and');

    if (result.where!.type === 'and') {
      expect(result.where!.conditions).toHaveLength(2);
    }
  });

  test('plans OR with EXISTS', () => {
    // Query: users WHERE EXISTS posts OR EXISTS comments
    const postsSubquery = createAST('posts');
    const postsCorrelated = createCorrelatedSubquery(postsSubquery, ['id'], ['userId']);
    const postsCondition = createExistsCondition(postsCorrelated, 'EXISTS');

    const commentsSubquery = createAST('comments');
    const commentsCorrelated = createCorrelatedSubquery(commentsSubquery, ['id'], ['userId']);
    const commentsCondition = createExistsCondition(commentsCorrelated, 'EXISTS');

    const ast = createAST('users', {
      where: {
        type: 'or',
        conditions: [postsCondition, commentsCondition],
      },
    });

    const result = planQuery(ast, simpleCostModel);

    expect(result.table).toBe('users');
    expect(result.where).toBeDefined();
    expect(result.where!.type).toBe('or');
  });

  test('plans NOT EXISTS (non-flippable join)', () => {
    // Query: users WHERE NOT EXISTS posts
    const postsSubquery = createAST('posts');
    const correlated = createCorrelatedSubquery(postsSubquery, ['id'], ['userId']);
    const condition = createExistsCondition(correlated, 'NOT EXISTS');

    const ast = createAST('users', {
      where: condition,
    });

    const result = planQuery(ast, simpleCostModel);

    expect(result.table).toBe('users');
    expect(result.where).toBeDefined();
    expect(result.where!.type).toBe('correlatedSubquery');

    // NOT EXISTS should not be flipped
    if (result.where!.type === 'correlatedSubquery') {
      expect(result.where!.flip).not.toBe(true);
    }
  });

  test('plans nested subqueries', () => {
    // Query: users WHERE EXISTS (posts WHERE EXISTS comments)
    const commentsSubquery = createAST('comments');
    const commentsCorrelated = createCorrelatedSubquery(commentsSubquery, ['id'], ['postId']);
    const commentsCondition = createExistsCondition(commentsCorrelated, 'EXISTS');

    const postsSubquery = createAST('posts', {
      where: commentsCondition,
    });
    const postsCorrelated = createCorrelatedSubquery(postsSubquery, ['id'], ['userId']);
    const postsCondition = createExistsCondition(postsCorrelated, 'EXISTS');

    const ast = createAST('users', {
      where: postsCondition,
    });

    const result = planQuery(ast, simpleCostModel);

    expect(result.table).toBe('users');
    expect(result.where).toBeDefined();

    // Check nested structure is preserved
    if (result.where!.type === 'correlatedSubquery') {
      expect(result.where!.related.subquery.where).toBeDefined();
      expect(result.where!.related.subquery.where!.type).toBe('correlatedSubquery');
    }
  });

  test('plans related subqueries independently', () => {
    // Query: users with related posts (WHERE posts has comments)
    const commentsSubquery = createAST('comments');
    const commentsCorrelated = createCorrelatedSubquery(commentsSubquery, ['id'], ['postId']);
    const commentsCondition = createExistsCondition(commentsCorrelated, 'EXISTS');

    const postsSubquery = createAST('posts', {
      alias: 'userPosts',
      where: commentsCondition,
    });
    const postsCorrelated = createCorrelatedSubquery(postsSubquery, ['id'], ['userId']);

    const ast = createAST('users', {
      related: [postsCorrelated],
    });

    const result = planQuery(ast, simpleCostModel);

    expect(result.table).toBe('users');
    expect(result.related).toBeDefined();
    expect(result.related).toHaveLength(1);
    expect(result.related![0].subquery.alias).toBe('userPosts');

    // Verify the nested subquery in related was also planned
    expect(result.related![0].subquery.where).toBeDefined();
  });

  test('plans complex query with AND, OR, and nested subqueries', () => {
    // Query: users WHERE (EXISTS posts OR EXISTS comments) AND EXISTS likes
    const postsSubquery = createAST('posts');
    const postsCorrelated = createCorrelatedSubquery(postsSubquery, ['id'], ['userId']);
    const postsCondition = createExistsCondition(postsCorrelated, 'EXISTS');

    const commentsSubquery = createAST('comments');
    const commentsCorrelated = createCorrelatedSubquery(commentsSubquery, ['id'], ['userId']);
    const commentsCondition = createExistsCondition(commentsCorrelated, 'EXISTS');

    const likesSubquery = createAST('likes');
    const likesCorrelated = createCorrelatedSubquery(likesSubquery, ['id'], ['userId']);
    const likesCondition = createExistsCondition(likesCorrelated, 'EXISTS');

    const orCondition = {
      type: 'or' as const,
      conditions: [postsCondition, commentsCondition],
    };

    const ast = createAST('users', {
      where: {
        type: 'and',
        conditions: [orCondition, likesCondition],
      },
    });

    const result = planQuery(ast, simpleCostModel);

    expect(result.table).toBe('users');
    expect(result.where).toBeDefined();
    expect(result.where!.type).toBe('and');

    if (result.where!.type === 'and') {
      expect(result.where!.conditions).toHaveLength(2);
      expect(result.where!.conditions[0].type).toBe('or');
      expect(result.where!.conditions[1].type).toBe('correlatedSubquery');
    }
  });

  test('handles multiple levels of nested related subqueries', () => {
    // Users with posts (posts with comments)
    const commentsSubquery = createAST('comments', {alias: 'postComments'});
    const commentsCorrelated = createCorrelatedSubquery(commentsSubquery, ['id'], ['postId']);

    const postsSubquery = createAST('posts', {
      alias: 'userPosts',
      related: [commentsCorrelated],
    });
    const postsCorrelated = createCorrelatedSubquery(postsSubquery, ['id'], ['userId']);

    const ast = createAST('users', {
      related: [postsCorrelated],
    });

    const result = planQuery(ast, simpleCostModel);

    expect(result.table).toBe('users');
    expect(result.related).toHaveLength(1);
    expect(result.related![0].subquery.alias).toBe('userPosts');
    expect(result.related![0].subquery.related).toHaveLength(1);
    expect(result.related![0].subquery.related![0].subquery.alias).toBe('postComments');
  });

  test('selects optimal plan based on cost model', () => {
    // Query structure from diagram:
    // issue
    //   .whereExists('project', p => p.whereExists('project_member', m => m.where('memberId', ?)))
    //   .whereExists('creator', c => c.where('name', ?))
    //
    // Expected optimal order based on costs:
    // Initial: issue=10000, project=100, project_member=1, creator=2
    // 1. Pick creator (cost: 2) - lowest initial cost, issue gets creatorId constraint (10000->2000)
    // 2. Pick project_member (cost: 1) - project gets projectId (100->1), issue gets projectId (2000->20)
    // 3. Pick project (cost: 1) - with projectId constraint
    // 4. Pick issue (cost: 20) - with creatorId+projectId constraints
    // Total expected cost: 2 + 1 + 1 + 20 = 24 (optimal!)
    //
    // Suboptimal path (picking project_member first): 1 + 2 + 1 + 2000 = 2004

    // Build nested EXISTS: project -> project_member
    const projectMemberSubquery = createAST('project_member', {
      orderBy: [['project_member.id', 'asc']],
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'memberId'},
        right: {type: 'literal', value: 'user123'},
      },
    });
    const projectMemberCorrelated = createCorrelatedSubquery(
      projectMemberSubquery,
      ['projectId'],
      ['projectId'],
    );
    const projectMemberCondition = createExistsCondition(projectMemberCorrelated, 'EXISTS');

    const projectSubquery = createAST('project', {
      orderBy: [['project.id', 'asc']],
      where: projectMemberCondition,
    });
    const projectCorrelated = createCorrelatedSubquery(projectSubquery, ['projectId'], ['projectId']);
    const projectCondition = createExistsCondition(projectCorrelated, 'EXISTS');

    // Build creator EXISTS
    const creatorSubquery = createAST('creator', {
      orderBy: [['creator.id', 'asc']],
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'name'},
        right: {type: 'literal', value: 'Alice'},
      },
    });
    const creatorCorrelated = createCorrelatedSubquery(creatorSubquery, ['creatorId'], ['creatorId']);
    const creatorCondition = createExistsCondition(creatorCorrelated, 'EXISTS');

    // Combine with AND
    const ast = createAST('issue', {
      orderBy: [['issue.id', 'asc']],
      where: {
        type: 'and',
        conditions: [projectCondition, creatorCondition],
      },
    });

    // Build the plan graph to inspect costs
    const {plan} = buildPlanGraph(ast, predictableCostModel);

    // Run the planner
    plan.plan();

    // Verify all joins are properly planned
    const summary = plan.getPlanSummary();
    expect(summary.totalConnections).toBe(4); // issue, project, project_member, creator
    expect(summary.pinnedConnections).toBe(4); // All should be pinned
    expect(summary.totalJoins).toBe(3); // Three EXISTS joins
    expect(summary.pinnedJoins).toBe(3); // All joins should be pinned

    // Verify the planner found the optimal plan!
    // The multi-start greedy search should try:
    // - Attempt 1: Start with project_member (cost 1) → total 103
    // - Attempt 2: Start with creator (cost 2) → total 24 (optimal!)
    // - Attempt 3: Start with project (cost 100) → total 202
    // - Attempt 4: Start with issue (cost 10000) → total 10003
    const totalCost = plan.getTotalCost();
    expect(totalCost).toBe(24); // Optimal plan!
  });

  test('optimal plan: linear chain (A → B → C)', () => {
    // Query: A.whereExists(B.whereExists(C))
    // Structure: A(1000) ← B(100) ← C(10)
    //
    // Constraints only propagate one level at a time:
    // Expected optimal order:
    // 1. Pick C (cost: 10) - no constraint yet
    // 2. Pick B (cost: 10) - with cId constraint (100/10)
    // 3. Pick A (cost: 100) - with bId constraint (1000/10)
    // Total: 10 + 10 + 100 = 120

    // Build nested chain: A → B → C
    const cSubquery = createAST('C', {
      orderBy: [['C.id', 'asc']],
    });
    const cCorrelated = createCorrelatedSubquery(cSubquery, ['cId'], ['cId']);
    const cCondition = createExistsCondition(cCorrelated, 'EXISTS');

    const bSubquery = createAST('B', {
      orderBy: [['B.id', 'asc']],
      where: cCondition,
    });
    const bCorrelated = createCorrelatedSubquery(bSubquery, ['bId'], ['bId']);
    const bCondition = createExistsCondition(bCorrelated, 'EXISTS');

    const ast = createAST('A', {
      orderBy: [['A.id', 'asc']],
      where: bCondition,
    });

    const {plan} = buildPlanGraph(ast, linearChainCostModel);
    plan.plan();

    // Verify plan is complete
    const summary = plan.getPlanSummary();
    expect(summary.totalConnections).toBe(3); // A, B, C
    expect(summary.pinnedConnections).toBe(3);
    expect(summary.totalJoins).toBe(2);
    expect(summary.pinnedJoins).toBe(2);

    // Verify optimal cost
    const totalCost = plan.getTotalCost();
    expect(totalCost).toBe(120); // C(10) + B(10) + A(100) = 120
  });

  test('optimal plan: star schema (central with 3 satellites)', () => {
    // Query: central.whereExists(sat1).whereExists(sat2).whereExists(sat3)
    // Structure: Central table with 3 independent satellites
    //
    // Base costs: central=1000, sat1=100, sat2=50, sat3=10
    // Each satellite constraint divides central by 2
    //
    // Expected optimal order:
    // 1. Pick sat3 (cost: 10) - central gets sat3Id (1000->500)
    // 2. Pick sat2 (cost: 50) - central gets sat2Id (500->250)
    // 3. Pick sat1 (cost: 100) - central gets sat1Id (250->125)
    // 4. Pick central (cost: 125) - with all 3 constraints
    // Total: 10 + 50 + 100 + 125 = 285

    const sat1Subquery = createAST('sat1', {
      orderBy: [['sat1.id', 'asc']],
    });
    const sat1Correlated = createCorrelatedSubquery(sat1Subquery, ['sat1Id'], ['sat1Id']);
    const sat1Condition = createExistsCondition(sat1Correlated, 'EXISTS');

    const sat2Subquery = createAST('sat2', {
      orderBy: [['sat2.id', 'asc']],
    });
    const sat2Correlated = createCorrelatedSubquery(sat2Subquery, ['sat2Id'], ['sat2Id']);
    const sat2Condition = createExistsCondition(sat2Correlated, 'EXISTS');

    const sat3Subquery = createAST('sat3', {
      orderBy: [['sat3.id', 'asc']],
    });
    const sat3Correlated = createCorrelatedSubquery(sat3Subquery, ['sat3Id'], ['sat3Id']);
    const sat3Condition = createExistsCondition(sat3Correlated, 'EXISTS');

    const ast = createAST('central', {
      orderBy: [['central.id', 'asc']],
      where: {
        type: 'and',
        conditions: [sat1Condition, sat2Condition, sat3Condition],
      },
    });

    const {plan} = buildPlanGraph(ast, starSchemaCostModel);
    plan.plan();

    const summary = plan.getPlanSummary();
    expect(summary.totalConnections).toBe(4); // central, sat1, sat2, sat3
    expect(summary.pinnedConnections).toBe(4);
    expect(summary.totalJoins).toBe(3);
    expect(summary.pinnedJoins).toBe(3);

    const totalCost = plan.getTotalCost();
    expect(totalCost).toBe(285); // sat3(10) + sat2(50) + sat1(100) + central(125) = 285
  });

  test('optimal plan: diamond pattern (two paths converge)', () => {
    // Query: root.whereExists(left).whereExists(right.whereExists(bottom))
    // Structure:
    //      root
    //      /  \
    //   left  right
    //           |
    //         bottom
    //
    // Base costs: root=10000, left=50, right=100, bottom=1
    // leftId divides root by 10, rightId divides root by 5
    // bottomId divides right by 10
    //
    // Expected optimal order:
    // 1. Pick bottom (cost: 1) - right gets bottomId (100->10)
    // 2. Pick right (cost: 10) - with bottomId constraint, root gets rightId (10000->2000)
    // 3. Pick left (cost: 50) - root gets leftId (2000->200)
    // 4. Pick root (cost: 200) - with leftId+rightId constraints
    // Total: 1 + 10 + 50 + 200 = 261

    const bottomSubquery = createAST('bottom', {
      orderBy: [['bottom.id', 'asc']],
    });
    const bottomCorrelated = createCorrelatedSubquery(bottomSubquery, ['bottomId'], ['bottomId']);
    const bottomCondition = createExistsCondition(bottomCorrelated, 'EXISTS');

    const rightSubquery = createAST('right', {
      orderBy: [['right.id', 'asc']],
      where: bottomCondition,
    });
    const rightCorrelated = createCorrelatedSubquery(rightSubquery, ['rightId'], ['rightId']);
    const rightCondition = createExistsCondition(rightCorrelated, 'EXISTS');

    const leftSubquery = createAST('left', {
      orderBy: [['left.id', 'asc']],
    });
    const leftCorrelated = createCorrelatedSubquery(leftSubquery, ['leftId'], ['leftId']);
    const leftCondition = createExistsCondition(leftCorrelated, 'EXISTS');

    const ast = createAST('root', {
      orderBy: [['root.id', 'asc']],
      where: {
        type: 'and',
        conditions: [leftCondition, rightCondition],
      },
    });

    const {plan} = buildPlanGraph(ast, diamondCostModel);
    plan.plan();

    const summary = plan.getPlanSummary();
    expect(summary.totalConnections).toBe(4); // root, left, right, bottom
    expect(summary.pinnedConnections).toBe(4);
    expect(summary.totalJoins).toBe(3);
    expect(summary.pinnedJoins).toBe(3);

    const totalCost = plan.getTotalCost();
    expect(totalCost).toBe(261); // bottom(1) + right(10) + left(50) + root(200) = 261
  });

  test('optimal plan: wide vs narrow branches', () => {
    // Query: main.whereExists(wide).whereExists(narrow)
    // Structure: Main table with two independent sibling branches
    //
    // Base costs: main=10000, wide=1000, narrow=10
    // Each constraint divides main by 100
    //
    // Greedy behavior after picking narrow (10):
    // - main gets narrowId constraint (10000→100)
    // - Next choice: main(100) vs wide(1000)
    // - Since joins are independent, main CAN be selected after just narrow
    // - Greedy picks main (100)
    // - Then wide (1000)
    // Total: narrow(10) → main(100) → wide(1000) = 1110
    //
    // This demonstrates a case where the greedy algorithm finds a local optimum
    // that is not globally optimal. A better order would be:
    // narrow(10) → wide(1000) → main(1 with both constraints) = 1011
    //
    // However, the multi-start greedy with only 3 connections tries all 3 starting
    // points, and all lead to similar greedy choices.

    const wideSubquery = createAST('wide', {
      orderBy: [['wide.id', 'asc']],
    });
    const wideCorrelated = createCorrelatedSubquery(wideSubquery, ['wideId'], ['wideId']);
    const wideCondition = createExistsCondition(wideCorrelated, 'EXISTS');

    const narrowSubquery = createAST('narrow', {
      orderBy: [['narrow.id', 'asc']],
    });
    const narrowCorrelated = createCorrelatedSubquery(narrowSubquery, ['narrowId'], ['narrowId']);
    const narrowCondition = createExistsCondition(narrowCorrelated, 'EXISTS');

    const ast = createAST('main', {
      orderBy: [['main.id', 'asc']],
      where: {
        type: 'and',
        conditions: [wideCondition, narrowCondition],
      },
    });

    const {plan} = buildPlanGraph(ast, wideNarrowCostModel);
    plan.plan();

    const summary = plan.getPlanSummary();
    expect(summary.totalConnections).toBe(3); // main, wide, narrow
    expect(summary.pinnedConnections).toBe(3);
    expect(summary.totalJoins).toBe(2);
    expect(summary.pinnedJoins).toBe(2);

    const totalCost = plan.getTotalCost();
    // Greedy algorithm finds: narrow(10) + main(100) + wide(1000) = 1110
    // This is suboptimal, demonstrating greedy limitations
    expect(totalCost).toBe(1110);
  });

  test('optimal plan: deep nesting with constraint accumulation', () => {
    // Query: A.whereExists(B.whereExists(C.whereExists(D.whereExists(E))))
    // Structure: A ← B ← C ← D ← E (deeply nested chain)
    //
    // Base costs: A=10000, B=1000, C=100, D=10, E=1
    // Each parent-child constraint divides the parent by 10
    //
    // Constraints propagate one level at a time through joins:
    // Expected order (starting from leaf):
    // 1. Pick E (cost: 1) - no constraint yet
    // 2. Pick D (cost: 1) - with eId constraint from E (10/10)
    // 3. Pick C (cost: 10) - with dId constraint from D (100/10)
    // 4. Pick B (cost: 100) - with cId constraint from C (1000/10)
    // 5. Pick A (cost: 1000) - with bId constraint from B (10000/10)
    // Total: 1 + 1 + 10 + 100 + 1000 = 1112
    //
    // This shows that constraints propagate incrementally, not transitively

    const eSubquery = createAST('E', {
      orderBy: [['E.id', 'asc']],
    });
    const eCorrelated = createCorrelatedSubquery(eSubquery, ['eId'], ['eId']);
    const eCondition = createExistsCondition(eCorrelated, 'EXISTS');

    const dSubquery = createAST('D', {
      orderBy: [['D.id', 'asc']],
      where: eCondition,
    });
    const dCorrelated = createCorrelatedSubquery(dSubquery, ['dId'], ['dId']);
    const dCondition = createExistsCondition(dCorrelated, 'EXISTS');

    const cSubquery = createAST('C', {
      orderBy: [['C.id', 'asc']],
      where: dCondition,
    });
    const cCorrelated = createCorrelatedSubquery(cSubquery, ['cId'], ['cId']);
    const cCondition = createExistsCondition(cCorrelated, 'EXISTS');

    const bSubquery = createAST('B', {
      orderBy: [['B.id', 'asc']],
      where: cCondition,
    });
    const bCorrelated = createCorrelatedSubquery(bSubquery, ['bId'], ['bId']);
    const bCondition = createExistsCondition(bCorrelated, 'EXISTS');

    const ast = createAST('A', {
      orderBy: [['A.id', 'asc']],
      where: bCondition,
    });

    const {plan} = buildPlanGraph(ast, deepNestingCostModel);
    plan.plan();

    const summary = plan.getPlanSummary();
    expect(summary.totalConnections).toBe(5); // A, B, C, D, E
    expect(summary.pinnedConnections).toBe(5);
    expect(summary.totalJoins).toBe(4);
    expect(summary.pinnedJoins).toBe(4);

    const totalCost = plan.getTotalCost();
    expect(totalCost).toBe(1112); // E(1) + D(1) + C(10) + B(100) + A(1000) = 1112
  });
});
