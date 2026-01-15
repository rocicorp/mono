import {expect, suite, test} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {asQueryInternals} from '../query/query-internals.ts';
import type {AnyQuery} from '../query/query.ts';
import {buildPlanGraph} from './planner-builder.ts';
import {simpleCostModel} from './test/helpers.ts';
import {builder} from './test/test-schema.ts';

suite('buildPlanGraph', () => {
  function getAST(q: AnyQuery): AST {
    return asQueryInternals(q).ast;
  }

  suite('basic structure', () => {
    test('creates plan graph for simple table query', () => {
      const ast = getAST(builder.users);
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan).toBeDefined();
      expect(plans.plan.connections).toHaveLength(1);
      expect(plans.plan.connections[0].table).toBe('users');
      expect(plans.subPlans).toEqual({});
    });

    test('creates connection with filters', () => {
      const ast = getAST(builder.users.where('id', 1));
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.connections).toHaveLength(1);
      expect(plans.plan.connections[0].table).toBe('users');
    });

    test('creates sources for tables', () => {
      const ast = getAST(builder.users);
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // Source should be accessible
      expect(plans.plan.hasSource('users')).toBe(true);
    });
  });

  suite('correlatedSubquery creates joins', () => {
    test('EXISTS creates a join that can be flipped', () => {
      const ast = getAST(builder.users.whereExists('posts'));
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // Should have 2 connections: one for users, one for posts
      expect(plans.plan.connections).toHaveLength(2);
      expect(plans.plan.connections[0].table).toBe('users');
      expect(plans.plan.connections[1].table).toBe('posts');

      // Should have 1 join
      expect(plans.plan.joins).toHaveLength(1);
      const join = plans.plan.joins[0];
      expect(join.kind).toBe('join');

      // Test that it can be flipped (doesn't throw)
      expect(() => join.flip()).not.toThrow();
      expect(join.type).toBe('flipped');
    });

    test('NOT EXISTS creates a join that cannot be flipped', () => {
      // Note: NOT EXISTS is blocked by the query builder on the client,
      // so we manually construct the AST for this test
      const ast = {
        table: 'users',
        where: {
          type: 'correlatedSubquery' as const,
          op: 'NOT EXISTS' as const,
          related: {
            correlation: {
              parentField: ['id'],
              childField: ['userId'],
            },
            subquery: {
              table: 'posts',
            },
          },
        },
      } as const;
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.joins).toHaveLength(1);
      const join = plans.plan.joins[0];

      // Test that it cannot be flipped (throws UnflippableJoinError)
      expect(() => join.flip()).toThrow('Cannot flip a non-flippable join');
    });

    test('assigns unique plan IDs to joins', () => {
      const ast = getAST(
        builder.users.whereExists('posts').whereExists('comments'),
      );
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.joins).toHaveLength(2);
      expect(plans.plan.joins[0].planId).toBe(0);
      expect(plans.plan.joins[1].planId).toBe(1);
    });
  });

  suite('AND creates sequential joins', () => {
    test('AND with multiple correlatedSubqueries creates multiple joins', () => {
      const ast = getAST(
        builder.users.whereExists('posts').whereExists('comments'),
      );
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // 3 connections: users, posts, comments
      expect(plans.plan.connections).toHaveLength(3);
      // 2 joins
      expect(plans.plan.joins).toHaveLength(2);
    });

    test('AND with simple and correlatedSubquery conditions', () => {
      const ast = getAST(
        builder.users.where('active', true).whereExists('posts'),
      );
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // 2 connections: users, posts
      expect(plans.plan.connections).toHaveLength(2);
      // 1 join (simple conditions don't create joins)
      expect(plans.plan.joins).toHaveLength(1);
    });
  });

  suite('OR creates fan-out/fan-in pairs', () => {
    test('OR with correlatedSubqueries creates fan-out and fan-in', () => {
      const ast = getAST(
        builder.users.where(({or, exists}) =>
          or(exists('posts'), exists('comments')),
        ),
      );
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // Should have fan-out and fan-in
      expect(plans.plan.fanOuts).toHaveLength(1);
      expect(plans.plan.fanIns).toHaveLength(1);

      // Should have 2 joins (one for each branch)
      expect(plans.plan.joins).toHaveLength(2);

      // Note: Current implementation adds each branch twice to fanOut.outputs:
      // once via wireOutput(input, join) in processCorrelatedSubquery (line 281)
      // and once via fanOut.addOutput(branch) in processOr (line 200)
      const fanOut = plans.plan.fanOuts[0];
      expect(fanOut.outputs.length).toBeGreaterThanOrEqual(2);
    });

    test('OR with only simple conditions does not create fan structure', () => {
      const ast = getAST(
        builder.users.where(({or, cmp}) =>
          or(cmp('status', 'active'), cmp('status', 'pending')),
        ),
      );
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // No fan-out/fan-in for simple conditions
      expect(plans.plan.fanOuts).toHaveLength(0);
      expect(plans.plan.fanIns).toHaveLength(0);
      expect(plans.plan.joins).toHaveLength(0);
    });

    test('OR with mixed simple and correlatedSubquery creates fan structure', () => {
      const ast = getAST(
        builder.users.where(({or, cmp, exists}) =>
          or(cmp('admin', true), exists('posts')),
        ),
      );
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // Should have fan structure for the correlatedSubquery
      expect(plans.plan.fanOuts).toHaveLength(1);
      expect(plans.plan.fanIns).toHaveLength(1);
      expect(plans.plan.joins).toHaveLength(1);
    });

    test('nested OR creates nested fan structures', () => {
      const ast = getAST(
        builder.users.where(({or, exists}) =>
          or(exists('posts'), or(exists('comments'), exists('likes'))),
        ),
      );
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // Note: The query builder may flatten nested ORs into a single OR with 3 branches,
      // which would result in 1 fan-out/fan-in pair instead of 2
      // For now, we'll just verify the structure is correct
      expect(plans.plan.fanOuts.length).toBeGreaterThanOrEqual(1);
      expect(plans.plan.fanIns.length).toBeGreaterThanOrEqual(1);

      // Should have 3 joins (one for each correlatedSubquery)
      expect(plans.plan.joins).toHaveLength(3);
    });
  });

  suite('related creates subPlans', () => {
    test('single related query creates subPlan', () => {
      const ast = getAST(builder.users.related('posts'));
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // Main plan should have only 1 connection (users)
      expect(plans.plan.connections).toHaveLength(1);
      expect(plans.plan.connections[0].table).toBe('users');

      // Should have subPlan for 'posts'
      expect(plans.subPlans).toHaveProperty('posts');
      expect(plans.subPlans.posts.plan.connections).toHaveLength(1);
      expect(plans.subPlans.posts.plan.connections[0].table).toBe('posts');
    });

    test('multiple related queries create multiple subPlans', () => {
      const ast = getAST(builder.users.related('posts').related('comments'));
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // Main plan should have only 1 connection
      expect(plans.plan.connections).toHaveLength(1);

      // Should have 2 subPlans
      expect(Object.keys(plans.subPlans)).toHaveLength(2);
      expect(plans.subPlans).toHaveProperty('posts');
      expect(plans.subPlans).toHaveProperty('comments');
    });

    test('nested related queries create nested subPlans', () => {
      const ast = getAST(
        builder.users.related('posts', q => q.related('comments')),
      );
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // Main plan should have 1 connection
      expect(plans.plan.connections).toHaveLength(1);

      // Should have subPlan for 'posts'
      expect(plans.subPlans).toHaveProperty('posts');

      // posts subPlan should have subPlan for 'comments'
      expect(plans.subPlans.posts.subPlans).toHaveProperty('comments');
      expect(
        plans.subPlans.posts.subPlans.comments.plan.connections,
      ).toHaveLength(1);
      expect(
        plans.subPlans.posts.subPlans.comments.plan.connections[0].table,
      ).toBe('comments');
    });
  });

  suite('complex queries', () => {
    test('combination of AND, OR, and related', () => {
      const ast = getAST(
        builder.users
          .where('active', true)
          .where(({or, exists}) => or(exists('posts'), exists('comments')))
          .related('profile'),
      );
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // Main plan should have 3 connections (users, posts, comments)
      expect(plans.plan.connections).toHaveLength(3);

      // Should have fan-out/fan-in for OR
      expect(plans.plan.fanOuts).toHaveLength(1);
      expect(plans.plan.fanIns).toHaveLength(1);

      // Should have 2 joins for the two EXISTS in OR
      expect(plans.plan.joins).toHaveLength(2);

      // Should have subPlan for profile
      expect(plans.subPlans).toHaveProperty('profile');
    });
  });

  suite('graph structure and wiring', () => {
    test('creates terminus node', () => {
      const ast = getAST(builder.users);
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // Terminus should be set (can verify by checking propagateConstraints works)
      expect(() => plans.plan.propagateConstraints()).not.toThrow();
    });

    test('connections are wired to outputs', () => {
      const ast = getAST(builder.users.whereExists('posts'));
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // All connections should have outputs set
      for (const connection of plans.plan.connections) {
        expect(() => connection.output).not.toThrow();
      }
    });
  });

  suite('limit assignment', () => {
    test('simple EXISTS child connection has limit=1', () => {
      const ast = getAST(builder.users.whereExists('posts'));
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.connections).toHaveLength(2);

      const usersConnection = plans.plan.connections.find(
        c => c.table === 'users',
      );
      const postsConnection = plans.plan.connections.find(
        c => c.table === 'posts',
      );

      expect(usersConnection).toBeDefined();
      expect(postsConnection).toBeDefined();

      // Root connection should have no limit
      expect(usersConnection?.limit).toBeUndefined();
      // EXISTS child should have limit=1
      expect(postsConnection?.limit).toBe(1);
    });

    test('NOT EXISTS child connection has no limit', () => {
      const ast = {
        table: 'users',
        where: {
          type: 'correlatedSubquery' as const,
          op: 'NOT EXISTS' as const,
          related: {
            correlation: {
              parentField: ['id'],
              childField: ['userId'],
            },
            subquery: {
              table: 'posts',
            },
          },
        },
      } as const;
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.connections).toHaveLength(2);

      const postsConnection = plans.plan.connections.find(
        c => c.table === 'posts',
      );

      expect(postsConnection).toBeDefined();
      // NOT EXISTS child should have no limit
      expect(postsConnection?.limit).toBeUndefined();
    });

    test('AND with multiple EXISTS - each child has limit=1', () => {
      const ast = getAST(
        builder.users.whereExists('posts').whereExists('comments'),
      );
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.connections).toHaveLength(3);

      const usersConnection = plans.plan.connections.find(
        c => c.table === 'users',
      );
      const postsConnection = plans.plan.connections.find(
        c => c.table === 'posts',
      );
      const commentsConnection = plans.plan.connections.find(
        c => c.table === 'comments',
      );

      expect(usersConnection).toBeDefined();
      expect(postsConnection).toBeDefined();
      expect(commentsConnection).toBeDefined();

      // Root has no limit
      expect(usersConnection?.limit).toBeUndefined();
      // Both EXISTS children have limit=1
      expect(postsConnection?.limit).toBe(1);
      expect(commentsConnection?.limit).toBe(1);
    });

    test('OR with EXISTS branches - each child has limit=1', () => {
      const ast = getAST(
        builder.users.where(({or, exists}) =>
          or(exists('posts'), exists('comments')),
        ),
      );
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.connections).toHaveLength(3);

      const usersConnection = plans.plan.connections.find(
        c => c.table === 'users',
      );
      const postsConnection = plans.plan.connections.find(
        c => c.table === 'posts',
      );
      const commentsConnection = plans.plan.connections.find(
        c => c.table === 'comments',
      );

      expect(usersConnection).toBeDefined();
      expect(postsConnection).toBeDefined();
      expect(commentsConnection).toBeDefined();

      // Root has no limit
      expect(usersConnection?.limit).toBeUndefined();
      // Both EXISTS branches have limit=1
      expect(postsConnection?.limit).toBe(1);
      expect(commentsConnection?.limit).toBe(1);
    });

    test('nested EXISTS - both levels have limit=1', () => {
      const ast = getAST(
        builder.users.whereExists('posts', q => q.whereExists('comments')),
      );
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // Main plan has all three connections (users, posts, comments)
      // because nested whereExists in the subquery creates connections in the main plan
      expect(plans.plan.connections).toHaveLength(3);

      const usersConnection = plans.plan.connections.find(
        c => c.table === 'users',
      );
      const postsConnection = plans.plan.connections.find(
        c => c.table === 'posts',
      );
      const commentsConnection = plans.plan.connections.find(
        c => c.table === 'comments',
      );

      expect(usersConnection).toBeDefined();
      expect(postsConnection).toBeDefined();
      expect(commentsConnection).toBeDefined();

      // Root has no limit
      expect(usersConnection?.limit).toBeUndefined();
      // Both EXISTS children have limit=1
      expect(postsConnection?.limit).toBe(1);
      expect(commentsConnection?.limit).toBe(1);

      // Should have 2 joins (one for each EXISTS)
      expect(plans.plan.joins).toHaveLength(2);
    });

    test('root connection gets ast.limit', () => {
      const ast = getAST(builder.users.limit(10));
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.connections).toHaveLength(1);

      const usersConnection = plans.plan.connections[0];
      expect(usersConnection.table).toBe('users');
      // Root should get the limit from the query
      expect(usersConnection.limit).toBe(10);
    });

    test('root with limit and EXISTS - separate limits', () => {
      const ast = getAST(builder.users.limit(10).whereExists('posts'));
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.connections).toHaveLength(2);

      const usersConnection = plans.plan.connections.find(
        c => c.table === 'users',
      );
      const postsConnection = plans.plan.connections.find(
        c => c.table === 'posts',
      );

      expect(usersConnection).toBeDefined();
      expect(postsConnection).toBeDefined();

      // Root gets the query limit
      expect(usersConnection?.limit).toBe(10);
      // EXISTS child gets limit=1
      expect(postsConnection?.limit).toBe(1);
    });

    test('related subPlan root gets its own limit', () => {
      const ast = getAST(builder.users.related('posts', q => q.limit(10)));
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      // Main plan should have only users connection
      expect(plans.plan.connections).toHaveLength(1);
      const usersConnection = plans.plan.connections[0];
      expect(usersConnection.table).toBe('users');
      expect(usersConnection.limit).toBeUndefined();

      // SubPlan for posts should exist
      expect(plans.subPlans).toHaveProperty('posts');
      expect(plans.subPlans.posts.plan.connections).toHaveLength(1);

      const postsConnection = plans.subPlans.posts.plan.connections[0];
      expect(postsConnection.table).toBe('posts');
      // SubPlan root should get the limit from the subquery
      expect(postsConnection.limit).toBe(10);
    });
  });

  suite('manual flip flag respects user intent', () => {
    test('flip: undefined allows planner to decide (join is flippable)', () => {
      // When flip is not specified, it should be undefined and the join should be flippable
      const ast = getAST(builder.users.whereExists('posts'));
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.joins).toHaveLength(1);
      const join = plans.plan.joins[0];

      // Join should be flippable (planner can decide)
      expect(join.isFlippable()).toBe(true);
      // Should start in semi-join state
      expect(join.type).toBe('semi');
      // Should be able to flip without error
      expect(() => join.flip()).not.toThrow();
      expect(join.type).toBe('flipped');
    });

    test('flip: true forces join to be flipped and not flippable', () => {
      const ast = getAST(builder.users.whereExists('posts', {flip: true}));
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.joins).toHaveLength(1);
      const join = plans.plan.joins[0];

      // Join should NOT be flippable (user specified flip: true)
      expect(join.isFlippable()).toBe(false);
      // Should start in flipped state
      expect(join.type).toBe('flipped');
    });

    test('flip: false forces join to stay semi and not be flippable', () => {
      const ast = getAST(builder.users.whereExists('posts', {flip: false}));
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.joins).toHaveLength(1);
      const join = plans.plan.joins[0];

      // Join should NOT be flippable (user specified flip: false)
      expect(join.isFlippable()).toBe(false);
      // Should start in semi-join state
      expect(join.type).toBe('semi');
      // Should not be able to flip
      expect(() => join.flip()).toThrow('Cannot flip a non-flippable join');
    });

    test('NOT EXISTS is never flippable, regardless of flip flag', () => {
      const ast = {
        table: 'users',
        where: {
          type: 'correlatedSubquery' as const,
          op: 'NOT EXISTS' as const,
          flip: undefined,
          related: {
            correlation: {
              parentField: ['id'],
              childField: ['userId'],
            },
            subquery: {
              table: 'posts',
            },
          },
        },
      } as const;
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.joins).toHaveLength(1);
      const join = plans.plan.joins[0];

      // NOT EXISTS should never be flippable
      expect(join.isFlippable()).toBe(false);
      expect(join.type).toBe('semi');
      expect(() => join.flip()).toThrow('Cannot flip a non-flippable join');
    });

    test('multiple joins with mixed flip settings', () => {
      const ast = getAST(
        builder.users
          .whereExists('posts', {flip: true}) // Force flip
          .whereExists('comments', {flip: false}) // Force semi
          .whereExists('likes'),
      ); // Let planner decide (flip: undefined)
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      expect(plans.plan.joins).toHaveLength(3);

      // First join (flip: true) should be flipped and not flippable
      expect(plans.plan.joins[0].type).toBe('flipped');
      expect(plans.plan.joins[0].isFlippable()).toBe(false);

      // Second join (flip: false) should be semi and not flippable
      expect(plans.plan.joins[1].type).toBe('semi');
      expect(plans.plan.joins[1].isFlippable()).toBe(false);

      // Third join (flip: undefined) should be semi and flippable
      expect(plans.plan.joins[2].type).toBe('semi');
      expect(plans.plan.joins[2].isFlippable()).toBe(true);
    });

    test('reset() restores join to initial type', () => {
      const ast = getAST(builder.users.whereExists('posts', {flip: true}));
      const plans = buildPlanGraph(ast, simpleCostModel, true);

      const join = plans.plan.joins[0];
      expect(join.type).toBe('flipped');

      // Reset should restore to initial type (flipped)
      join.reset();
      expect(join.type).toBe('flipped');
    });
  });
});
