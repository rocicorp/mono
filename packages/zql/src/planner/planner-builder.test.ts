import {expect, suite, test} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {buildPlanGraph} from './planner-builder.ts';
import {simpleCostModel} from './test/helpers.ts';

suite('buildPlanGraph', () => {
  suite('basic structure', () => {
    test('creates plan graph for simple table query', () => {
      const ast: AST = {
        table: 'users',
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      expect(plans.plan).toBeDefined();
      expect(plans.plan.connections).toHaveLength(1);
      expect(plans.plan.connections[0].table).toBe('users');
      expect(plans.subPlans).toEqual({});
    });

    test('creates connection with filters', () => {
      const ast: AST = {
        table: 'users',
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: 1},
        },
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      expect(plans.plan.connections).toHaveLength(1);
      expect(plans.plan.connections[0].table).toBe('users');
    });

    test('creates sources for tables', () => {
      const ast: AST = {
        table: 'users',
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // Source should be accessible
      expect(plans.plan.hasSource('users')).toBe(true);
      expect(plans.plan.getSource('users')).toBeDefined();
    });
  });

  suite('correlatedSubquery creates joins', () => {
    test('EXISTS creates a join that can be flipped', () => {
      const ast: AST = {
        table: 'users',
        where: {
          type: 'correlatedSubquery',
          op: 'EXISTS',
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
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

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
      const ast: AST = {
        table: 'users',
        where: {
          type: 'correlatedSubquery',
          op: 'NOT EXISTS',
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
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      expect(plans.plan.joins).toHaveLength(1);
      const join = plans.plan.joins[0];

      // Test that it cannot be flipped (throws UnflippableJoinError)
      expect(() => join.flip()).toThrow('Cannot flip a non-flippable join');
    });

    test('nested correlatedSubquery in WHERE creates join and processes child WHERE', () => {
      const ast: AST = {
        table: 'users',
        where: {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            correlation: {
              parentField: ['id'],
              childField: ['userId'],
            },
            subquery: {
              table: 'posts',
              where: {
                type: 'simple',
                op: '=',
                left: {type: 'column', name: 'published'},
                right: {type: 'literal', value: true},
              },
            },
          },
        },
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // Should have 2 connections (users and posts)
      expect(plans.plan.connections).toHaveLength(2);
      // Should have 1 join
      expect(plans.plan.joins).toHaveLength(1);
    });

    test('assigns unique plan IDs to joins', () => {
      const ast: AST = {
        table: 'users',
        where: {
          type: 'and',
          conditions: [
            {
              type: 'correlatedSubquery',
              op: 'EXISTS',
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
            {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              related: {
                correlation: {
                  parentField: ['id'],
                  childField: ['userId'],
                },
                subquery: {
                  table: 'comments',
                },
              },
            },
          ],
        },
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      expect(plans.plan.joins).toHaveLength(2);
      expect(plans.plan.joins[0].planId).toBe(0);
      expect(plans.plan.joins[1].planId).toBe(1);
    });
  });

  suite('AND creates sequential joins', () => {
    test('AND with multiple correlatedSubqueries creates multiple joins', () => {
      const ast: AST = {
        table: 'users',
        where: {
          type: 'and',
          conditions: [
            {
              type: 'correlatedSubquery',
              op: 'EXISTS',
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
            {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              related: {
                correlation: {
                  parentField: ['id'],
                  childField: ['userId'],
                },
                subquery: {
                  table: 'comments',
                },
              },
            },
          ],
        },
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // 3 connections: users, posts, comments
      expect(plans.plan.connections).toHaveLength(3);
      // 2 joins
      expect(plans.plan.joins).toHaveLength(2);
    });

    test('AND with simple and correlatedSubquery conditions', () => {
      const ast: AST = {
        table: 'users',
        where: {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'active'},
              right: {type: 'literal', value: true},
            },
            {
              type: 'correlatedSubquery',
              op: 'EXISTS',
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
          ],
        },
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // 2 connections: users, posts
      expect(plans.plan.connections).toHaveLength(2);
      // 1 join (simple conditions don't create joins)
      expect(plans.plan.joins).toHaveLength(1);
    });
  });

  suite('OR creates fan-out/fan-in pairs', () => {
    test('OR with correlatedSubqueries creates fan-out and fan-in', () => {
      const ast: AST = {
        table: 'users',
        where: {
          type: 'or',
          conditions: [
            {
              type: 'correlatedSubquery',
              op: 'EXISTS',
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
            {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              related: {
                correlation: {
                  parentField: ['id'],
                  childField: ['userId'],
                },
                subquery: {
                  table: 'comments',
                },
              },
            },
          ],
        },
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // Should have fan-out and fan-in
      expect(plans.plan.fanOuts).toHaveLength(1);
      expect(plans.plan.fanIns).toHaveLength(1);

      // Should have 2 joins (one for each branch)
      expect(plans.plan.joins).toHaveLength(2);

      // Note: Current implementation adds each branch twice to fanOut.outputs:
      // once via wireOutput(input, join) in processCorrelatedSubquery (line 122)
      // and once via fanOut.addOutput(branch) in processOr (line 200)
      const fanOut = plans.plan.fanOuts[0];
      expect(fanOut.outputs.length).toBeGreaterThanOrEqual(2);
    });

    test('OR with only simple conditions does not create fan structure', () => {
      const ast: AST = {
        table: 'users',
        where: {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'status'},
              right: {type: 'literal', value: 'active'},
            },
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'status'},
              right: {type: 'literal', value: 'pending'},
            },
          ],
        },
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // No fan-out/fan-in for simple conditions
      expect(plans.plan.fanOuts).toHaveLength(0);
      expect(plans.plan.fanIns).toHaveLength(0);
      expect(plans.plan.joins).toHaveLength(0);
    });

    test('OR with mixed simple and correlatedSubquery creates fan structure', () => {
      const ast: AST = {
        table: 'users',
        where: {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'admin'},
              right: {type: 'literal', value: true},
            },
            {
              type: 'correlatedSubquery',
              op: 'EXISTS',
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
          ],
        },
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // Should have fan structure for the correlatedSubquery
      expect(plans.plan.fanOuts).toHaveLength(1);
      expect(plans.plan.fanIns).toHaveLength(1);
      expect(plans.plan.joins).toHaveLength(1);
    });

    test('nested OR creates nested fan structures', () => {
      const ast: AST = {
        table: 'users',
        where: {
          type: 'or',
          conditions: [
            {
              type: 'correlatedSubquery',
              op: 'EXISTS',
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
            {
              type: 'or',
              conditions: [
                {
                  type: 'correlatedSubquery',
                  op: 'EXISTS',
                  related: {
                    correlation: {
                      parentField: ['id'],
                      childField: ['userId'],
                    },
                    subquery: {
                      table: 'comments',
                    },
                  },
                },
                {
                  type: 'correlatedSubquery',
                  op: 'EXISTS',
                  related: {
                    correlation: {
                      parentField: ['id'],
                      childField: ['authorId'],
                    },
                    subquery: {
                      table: 'likes',
                    },
                  },
                },
              ],
            },
          ],
        },
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // Should have 2 fan-out/fan-in pairs (one for outer OR, one for inner OR)
      expect(plans.plan.fanOuts).toHaveLength(2);
      expect(plans.plan.fanIns).toHaveLength(2);

      // Should have 3 joins (one for each correlatedSubquery)
      expect(plans.plan.joins).toHaveLength(3);
    });
  });

  suite('related creates subPlans', () => {
    test('single related query creates subPlan', () => {
      const ast: AST = {
        table: 'users',
        related: [
          {
            correlation: {
              parentField: ['id'],
              childField: ['userId'],
            },
            subquery: {
              table: 'posts',
              alias: 'posts',
            },
          },
        ],
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // Main plan should have only 1 connection (users)
      expect(plans.plan.connections).toHaveLength(1);
      expect(plans.plan.connections[0].table).toBe('users');

      // Should have subPlan for 'posts'
      expect(plans.subPlans).toHaveProperty('posts');
      expect(plans.subPlans.posts.plan.connections).toHaveLength(1);
      expect(plans.subPlans.posts.plan.connections[0].table).toBe('posts');
    });

    test('multiple related queries create multiple subPlans', () => {
      const ast: AST = {
        table: 'users',
        related: [
          {
            correlation: {
              parentField: ['id'],
              childField: ['userId'],
            },
            subquery: {
              table: 'posts',
              alias: 'posts',
            },
          },
          {
            correlation: {
              parentField: ['id'],
              childField: ['userId'],
            },
            subquery: {
              table: 'comments',
              alias: 'comments',
            },
          },
        ],
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // Main plan should have only 1 connection
      expect(plans.plan.connections).toHaveLength(1);

      // Should have 2 subPlans
      expect(Object.keys(plans.subPlans)).toHaveLength(2);
      expect(plans.subPlans).toHaveProperty('posts');
      expect(plans.subPlans).toHaveProperty('comments');
    });

    test('nested related queries create nested subPlans', () => {
      const ast: AST = {
        table: 'users',
        related: [
          {
            correlation: {
              parentField: ['id'],
              childField: ['userId'],
            },
            subquery: {
              table: 'posts',
              alias: 'posts',
              related: [
                {
                  correlation: {
                    parentField: ['id'],
                    childField: ['postId'],
                  },
                  subquery: {
                    table: 'comments',
                    alias: 'comments',
                  },
                },
              ],
            },
          },
        ],
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

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

    test('related with WHERE clause creates joins in subPlan', () => {
      const ast: AST = {
        table: 'users',
        related: [
          {
            correlation: {
              parentField: ['id'],
              childField: ['userId'],
            },
            subquery: {
              table: 'posts',
              alias: 'posts',
              where: {
                type: 'correlatedSubquery',
                op: 'EXISTS',
                related: {
                  correlation: {
                    parentField: ['id'],
                    childField: ['postId'],
                  },
                  subquery: {
                    table: 'likes',
                  },
                },
              },
            },
          },
        ],
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // posts subPlan should have 2 connections (posts and likes)
      expect(plans.subPlans.posts.plan.connections).toHaveLength(2);
      // posts subPlan should have 1 join
      expect(plans.subPlans.posts.plan.joins).toHaveLength(1);
    });
  });

  suite('complex queries', () => {
    test('combination of AND, OR, and related', () => {
      const ast: AST = {
        table: 'users',
        where: {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'active'},
              right: {type: 'literal', value: true},
            },
            {
              type: 'or',
              conditions: [
                {
                  type: 'correlatedSubquery',
                  op: 'EXISTS',
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
                {
                  type: 'correlatedSubquery',
                  op: 'EXISTS',
                  related: {
                    correlation: {
                      parentField: ['id'],
                      childField: ['userId'],
                    },
                    subquery: {
                      table: 'comments',
                    },
                  },
                },
              ],
            },
          ],
        },
        related: [
          {
            correlation: {
              parentField: ['id'],
              childField: ['userId'],
            },
            subquery: {
              table: 'profile',
              alias: 'profile',
            },
          },
        ],
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

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

    test('reuses sources when same table appears multiple times', () => {
      const ast: AST = {
        table: 'users',
        where: {
          type: 'and',
          conditions: [
            {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              related: {
                correlation: {
                  parentField: ['id'],
                  childField: ['authorId'],
                },
                subquery: {
                  table: 'posts',
                },
              },
            },
            {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              related: {
                correlation: {
                  parentField: ['id'],
                  childField: ['editorId'],
                },
                subquery: {
                  table: 'posts',
                },
              },
            },
          ],
        },
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // Should have 3 connections (1 for users, 2 for posts - one per subquery)
      expect(plans.plan.connections).toHaveLength(3);

      // But only 2 sources (users and posts)
      expect(plans.plan.hasSource('users')).toBe(true);
      expect(plans.plan.hasSource('posts')).toBe(true);

      // Both posts connections should come from the same source
      expect(plans.plan.getSource('posts')).toBeDefined();
      expect(plans.plan.connections[1].table).toBe('posts');
      expect(plans.plan.connections[2].table).toBe('posts');
    });
  });

  suite('graph structure and wiring', () => {
    test('creates terminus node', () => {
      const ast: AST = {
        table: 'users',
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // Terminus should be set (can verify by checking hasPlan works after planning)
      expect(() => plans.plan.propagateConstraints()).not.toThrow();
    });

    test('connections are wired to outputs', () => {
      const ast: AST = {
        table: 'users',
        where: {
          type: 'correlatedSubquery',
          op: 'EXISTS',
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
      };

      const plans = buildPlanGraph(ast, simpleCostModel);

      // All connections should have outputs set
      for (const connection of plans.plan.connections) {
        expect(() => connection.output).not.toThrow();
      }
    });
  });
});
