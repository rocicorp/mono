import {expect, suite, test} from 'vitest';
import {planQuery, buildPlanGraph} from './planner-builder.ts';
import {createAST, createExistsCondition, createCorrelatedSubquery, simpleCostModel, predictableCostModel} from './test/helpers.ts';

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
});
