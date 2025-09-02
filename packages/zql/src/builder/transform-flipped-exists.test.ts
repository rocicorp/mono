import {expect, test} from 'vitest';
import type {AST, CorrelatedSubqueryCondition} from '../../../zero-protocol/src/ast.ts';
import {transformFlippedExists} from './transform-flipped-exists.ts';

test('no flip returns original AST', () => {
  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['userId']},
        subquery: {
          table: 'orders',
          alias: 'orders_exists',
          orderBy: [['id', 'asc']],
        },
      },
      op: 'EXISTS',
    },
  };

  const result = transformFlippedExists(ast);
  
  expect(result.ast).toEqual(ast);
  expect(result.pathToOriginalRoot).toEqual([]);
});

test('simple flipped EXISTS transformation', () => {
  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['userId']},
        subquery: {
          table: 'orders',
          alias: 'orders_exists',
          orderBy: [['id', 'asc']],
        },
      },
      op: 'EXISTS',
      flip: true,
    } as CorrelatedSubqueryCondition,
  };

  const result = transformFlippedExists(ast);
  
  // The orders table should now be the root
  expect(result.ast.table).toBe('orders');
  
  // The WHERE should have an EXISTS checking for users
  expect(result.ast.where).toBeDefined();
  expect(result.ast.where?.type).toBe('correlatedSubquery');
  
  const existsCondition = result.ast.where as CorrelatedSubqueryCondition;
  expect(existsCondition.op).toBe('EXISTS');
  expect(existsCondition.related.subquery.table).toBe('users');
  
  // Correlation should be swapped
  expect(existsCondition.related.correlation.parentField).toEqual(['userId']);
  expect(existsCondition.related.correlation.childField).toEqual(['id']);
  
  // No flip on the transformed condition
  expect(existsCondition.flip).toBeFalsy();
  
  // Path to original root
  expect(result.pathToOriginalRoot).toEqual(['users']);
});

test('flipped EXISTS with additional WHERE conditions', () => {
  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          op: '>',
          left: {type: 'column', name: 'age'},
          right: {type: 'literal', value: 30},
        },
        {
          type: 'correlatedSubquery',
          related: {
            system: 'client',
            correlation: {parentField: ['id'], childField: ['userId']},
            subquery: {
              table: 'orders',
              alias: 'orders_exists',
              orderBy: [['id', 'asc']],
              where: {
                type: 'simple',
                op: '=',
                left: {type: 'column', name: 'status'},
                right: {type: 'literal', value: 'completed'},
              },
            },
          },
          op: 'EXISTS',
          flip: true,
        } as CorrelatedSubqueryCondition,
      ],
    },
  };

  const result = transformFlippedExists(ast);
  
  // Orders is now root
  expect(result.ast.table).toBe('orders');
  
  // Orders WHERE should include its original condition AND EXISTS for users
  expect(result.ast.where?.type).toBe('and');
  if (result.ast.where?.type === 'and') {
    // Should have the orders' status condition
    const hasStatusCondition = result.ast.where.conditions.some(
      c => c.type === 'simple' && 
          c.left.type === 'column' && 
          c.left.name === 'status'
    );
    expect(hasStatusCondition).toBe(true);
    
    // Should have EXISTS for users (with age > 30)
    const existsCondition = result.ast.where.conditions.find(
      c => c.type === 'correlatedSubquery'
    ) as CorrelatedSubqueryCondition | undefined;
    expect(existsCondition).toBeDefined();
    expect(existsCondition?.related.subquery.table).toBe('users');
    
    // The users subquery should have the age condition
    const usersWhere = existsCondition?.related.subquery.where;
    expect(usersWhere?.type).toBe('simple');
    if (usersWhere?.type === 'simple') {
      expect(usersWhere.left.type).toBe('column');
      if (usersWhere.left.type === 'column') {
        expect(usersWhere.left.name).toBe('age');
      }
    }
  }
  
  expect(result.pathToOriginalRoot).toEqual(['users']);
});

test('nested flipped EXISTS', () => {
  // users WHERE EXISTS(orders WHERE EXISTS(orderItems with flip))
  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['userId']},
        subquery: {
          table: 'orders',
          alias: 'orders_exists',
          orderBy: [['id', 'asc']],
          where: {
            type: 'correlatedSubquery',
            related: {
              system: 'client',
              correlation: {parentField: ['id'], childField: ['orderId']},
              subquery: {
                table: 'orderItems',
                alias: 'items_exists',
                orderBy: [['id', 'asc']],
              },
            },
            op: 'EXISTS',
            flip: true, // Inner flip
          } as CorrelatedSubqueryCondition,
        },
      },
      op: 'EXISTS',
      flip: false, // Outer not flipped
    },
  };

  const result = transformFlippedExists(ast);
  
  // orderItems should be the new root (innermost flip is processed)
  expect(result.ast.table).toBe('orderItems');
  
  // Path should reflect the transformation
  // The flip was on the orders->orderItems relationship, 
  // so orders is what we flipped with
  expect(result.pathToOriginalRoot).toEqual(['orders']);
  
  // The WHERE should have EXISTS for orders
  const existsCondition = result.ast.where as CorrelatedSubqueryCondition;
  expect(existsCondition?.type).toBe('correlatedSubquery');
  expect(existsCondition?.related.subquery.table).toBe('orders');
  
  // The orders subquery should have EXISTS for users
  const ordersWhere = existsCondition?.related.subquery.where;
  expect(ordersWhere?.type).toBe('correlatedSubquery');
  if (ordersWhere?.type === 'correlatedSubquery') {
    expect(ordersWhere.related.subquery.table).toBe('users');
  }
});

test('multiple nested flips', () => {
  // Both outer and inner EXISTS are flipped
  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['userId']},
        subquery: {
          table: 'orders',
          alias: 'orders_exists',
          orderBy: [['id', 'asc']],
          where: {
            type: 'correlatedSubquery',
            related: {
              system: 'client',
              correlation: {parentField: ['id'], childField: ['orderId']},
              subquery: {
                table: 'orderItems',
                alias: 'items_exists',
                orderBy: [['id', 'asc']],
              },
            },
            op: 'EXISTS',
            flip: true, // Inner flip
          } as CorrelatedSubqueryCondition,
        },
      },
      op: 'EXISTS',
      flip: true, // Outer flip too
    } as CorrelatedSubqueryCondition,
  };

  const result = transformFlippedExists(ast);
  
  // Should handle both flips, innermost first
  // First flip: orderItems becomes root, orders becomes subquery
  // Second flip: orders becomes root, users becomes subquery
  // But since we process depth-first, orderItems should still be root
  expect(result.ast.table).toBe('orderItems');
  
  // Path should show the complete transformation path
  // The exact path depends on the order of transformation
  expect(result.pathToOriginalRoot.length).toBeGreaterThan(0);
  expect(result.pathToOriginalRoot).toContain('orders');
});

test('NOT EXISTS with flip', () => {
  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['userId']},
        subquery: {
          table: 'orders',
          alias: 'orders_not_exists',
          orderBy: [['id', 'asc']],
        },
      },
      op: 'NOT EXISTS',
      flip: true,
    } as CorrelatedSubqueryCondition,
  };

  const result = transformFlippedExists(ast);
  
  expect(result.ast.table).toBe('orders');
  
  // The transformed condition should preserve NOT EXISTS
  const existsCondition = result.ast.where as CorrelatedSubqueryCondition;
  expect(existsCondition.op).toBe('NOT EXISTS');
  expect(existsCondition.related.subquery.table).toBe('users');
  
  expect(result.pathToOriginalRoot).toEqual(['users']);
});

test('flipped EXISTS in OR condition', () => {
  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
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
          type: 'correlatedSubquery',
          related: {
            system: 'client',
            correlation: {parentField: ['id'], childField: ['userId']},
            subquery: {
              table: 'orders',
              alias: 'orders_exists',
              orderBy: [['id', 'asc']],
            },
          },
          op: 'EXISTS',
          flip: true,
        } as CorrelatedSubqueryCondition,
      ],
    },
  };

  const result = transformFlippedExists(ast);
  
  // Orders becomes root
  expect(result.ast.table).toBe('orders');
  
  // The WHERE should have EXISTS for users
  const existsCondition = result.ast.where as CorrelatedSubqueryCondition;
  expect(existsCondition?.type).toBe('correlatedSubquery');
  expect(existsCondition?.related.subquery.table).toBe('users');
  
  // The users subquery WHERE should have the status condition
  // (since other OR branches move with the parent)
  const usersWhere = existsCondition?.related.subquery.where;
  expect(usersWhere?.type).toBe('simple');
  if (usersWhere?.type === 'simple') {
    expect(usersWhere.left.type).toBe('column');
    if (usersWhere.left.type === 'column') {
      expect(usersWhere.left.name).toBe('status');
    }
  }
  
  expect(result.pathToOriginalRoot).toEqual(['users']);
});

test('multiple flipped EXISTS in AND - transforms only first', () => {
  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    where: {
      type: 'and',
      conditions: [
        {
          type: 'correlatedSubquery',
          related: {
            system: 'client',
            correlation: {parentField: ['id'], childField: ['userId']},
            subquery: {
              table: 'orders',
              alias: 'orders_exists',
              orderBy: [['id', 'asc']],
            },
          },
          op: 'EXISTS',
          flip: true, // First flip - should be transformed
        } as CorrelatedSubqueryCondition,
        {
          type: 'correlatedSubquery',
          related: {
            system: 'client',
            correlation: {parentField: ['id'], childField: ['userId']},
            subquery: {
              table: 'reviews',
              alias: 'reviews_exists',
              orderBy: [['id', 'asc']],
            },
          },
          op: 'EXISTS',
          flip: true, // Second flip - should remain as regular EXISTS
        } as CorrelatedSubqueryCondition,
      ],
    },
  };

  const result = transformFlippedExists(ast);
  
  // orders should be the new root (first flip wins)
  expect(result.ast.table).toBe('orders');
  expect(result.pathToOriginalRoot).toEqual(['users']);
  
  // The WHERE should be an AND with two conditions:
  // 1. EXISTS(users) - the original parent
  // 2. EXISTS(reviews) - the second flipped condition becomes regular EXISTS
  expect(result.ast.where?.type).toBe('and');
  if (result.ast.where?.type === 'and') {
    expect(result.ast.where.conditions).toHaveLength(2);
    
    // First condition should be EXISTS(users)
    const usersCondition = result.ast.where.conditions.find(
      c => c.type === 'correlatedSubquery' && 
          c.related.subquery.table === 'users'
    ) as CorrelatedSubqueryCondition | undefined;
    expect(usersCondition).toBeDefined();
    expect(usersCondition?.flip).toBeFalsy(); // No flip
    
    // Second condition should be EXISTS(reviews) - the second flip becomes regular EXISTS
    const reviewsCondition = result.ast.where.conditions.find(
      c => c.type === 'correlatedSubquery' && 
          c.related.subquery.table === 'reviews'
    ) as CorrelatedSubqueryCondition | undefined;
    expect(reviewsCondition).toBeDefined();
    expect(reviewsCondition?.flip).toBeFalsy(); // No flip - converted to regular EXISTS
  }
});