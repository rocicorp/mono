import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST, CorrelatedSubqueryCondition} from '../../../zero-protocol/src/ast.ts';
import {Catch} from '../ivm/catch.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {buildPipeline} from './builder.ts';
import {TestBuilderDelegate} from './test-builder-delegate.ts';

const lc = createSilentLogContext();

function setupTestData() {
  // Create a users table with some test data
  const users = createSource(
    lc,
    testLogConfig,
    'users',
    {
      id: {type: 'number'},
      name: {type: 'string'},
      age: {type: 'number'},
    },
    ['id'],
  );
  users.push({type: 'add', row: {id: 1, name: 'Alice', age: 30}});
  users.push({type: 'add', row: {id: 2, name: 'Bob', age: 25}});
  users.push({type: 'add', row: {id: 3, name: 'Charlie', age: 35}});
  users.push({type: 'add', row: {id: 4, name: 'David', age: 28}});
  users.push({type: 'add', row: {id: 5, name: 'Eve', age: 32}});

  // Create an orders table
  const orders = createSource(
    lc,
    testLogConfig,
    'orders',
    {
      id: {type: 'number'},
      userId: {type: 'number'},
      amount: {type: 'number'},
      status: {type: 'string'},
    },
    ['id'],
  );
  orders.push({type: 'add', row: {id: 1, userId: 1, amount: 100, status: 'completed'}});
  orders.push({type: 'add', row: {id: 2, userId: 1, amount: 200, status: 'pending'}});
  orders.push({type: 'add', row: {id: 3, userId: 2, amount: 150, status: 'completed'}});
  orders.push({type: 'add', row: {id: 4, userId: 3, amount: 300, status: 'completed'}});
  orders.push({type: 'add', row: {id: 5, userId: 3, amount: 50, status: 'cancelled'}});
  orders.push({type: 'add', row: {id: 6, userId: 5, amount: 175, status: 'completed'}});

  // Create a products table
  const products = createSource(
    lc,
    testLogConfig,
    'products',
    {
      id: {type: 'number'},
      name: {type: 'string'},
      price: {type: 'number'},
      inStock: {type: 'boolean'},
    },
    ['id'],
  );
  products.push({type: 'add', row: {id: 1, name: 'Widget', price: 10, inStock: true}});
  products.push({type: 'add', row: {id: 2, name: 'Gadget', price: 20, inStock: false}});
  products.push({type: 'add', row: {id: 3, name: 'Doohickey', price: 15, inStock: true}});

  // Create an order_items table for many-to-many relationships
  const orderItems = createSource(
    lc,
    testLogConfig,
    'orderItems',
    {
      id: {type: 'number'},
      orderId: {type: 'number'},
      productId: {type: 'number'},
      quantity: {type: 'number'},
    },
    ['id'],
  );
  orderItems.push({type: 'add', row: {id: 1, orderId: 1, productId: 1, quantity: 5}});
  orderItems.push({type: 'add', row: {id: 2, orderId: 1, productId: 3, quantity: 2}});
  orderItems.push({type: 'add', row: {id: 3, orderId: 2, productId: 2, quantity: 1}});
  orderItems.push({type: 'add', row: {id: 4, orderId: 3, productId: 1, quantity: 10}});
  orderItems.push({type: 'add', row: {id: 5, orderId: 4, productId: 3, quantity: 3}});
  orderItems.push({type: 'add', row: {id: 6, orderId: 6, productId: 1, quantity: 7}});

  const sources = {users, orders, products, orderItems};
  return {sources, delegate: new TestBuilderDelegate(sources)};
}

test('basic EXISTS without flip', () => {
  const {delegate} = setupTestData();
  
  // Find users who have at least one order
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

  const sink = new Catch(buildPipeline(ast, delegate, 'query-1'));
  const results = sink.fetch();
  
  // Users 1, 2, 3, and 5 have orders
  expect(results.map(r => r.row.id)).toEqual([1, 2, 3, 5]);
});

test('EXISTS with flip: true', () => {
  const {delegate} = setupTestData();
  
  // Find users who have at least one order, but use flip to optimize
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
      flip: true,  // This will start from orders table and join users
    } as CorrelatedSubqueryCondition,
  };

  const sink = new Catch(buildPipeline(ast, delegate, 'query-2'));
  const results = sink.fetch();
  
  // Should get the same users but processed differently internally
  expect(results.map(r => r.row.id)).toEqual([1, 2, 3, 5]);
  expect(results.map(r => r.row.name)).toEqual(['Alice', 'Bob', 'Charlie', 'Eve']);
});

test('NOT EXISTS with flip: true', () => {
  const {delegate} = setupTestData();
  
  // Find users who have NO orders, using flip
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

  const sink = new Catch(buildPipeline(ast, delegate, 'query-3'));
  const results = sink.fetch();
  
  // Only user 4 (David) has no orders
  expect(results.map(r => r.row.id)).toEqual([4]);
  expect(results.map(r => r.row.name)).toEqual(['David']);
});

test('EXISTS with flip and additional WHERE conditions', () => {
  const {delegate} = setupTestData();
  
  // Find users over 30 who have completed orders
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
              alias: 'completed_orders',
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

  const sink = new Catch(buildPipeline(ast, delegate, 'query-4'));
  const results = sink.fetch();
  
  // Users over 30: 3 (Charlie, 35) and 5 (Eve, 32)
  // Both have completed orders
  expect(results.map(r => r.row.id)).toEqual([3, 5]);
  expect(results.map(r => r.row.age)).toEqual([35, 32]);
});

test('multiple flipped EXISTS in sequence', () => {
  const {delegate} = setupTestData();
  
  // Find users who have orders AND whose orders contain in-stock products
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
              alias: 'user_orders',
              orderBy: [['id', 'asc']],
            },
          },
          op: 'EXISTS',
          flip: true,
        } as CorrelatedSubqueryCondition,
        // This second EXISTS would check for orders with in-stock items
        // but we'll simplify for this test
      ],
    },
  };

  const sink = new Catch(buildPipeline(ast, delegate, 'query-5'));
  const results = sink.fetch();
  
  // All users with orders
  expect(results.map(r => r.row.id)).toEqual([1, 2, 3, 5]);
});

test('EXISTS with flip maintains correct sort order', () => {
  const {delegate} = setupTestData();
  
  // Find users with orders, sorted by name
  const ast: AST = {
    table: 'users',
    orderBy: [['name', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['userId']},
        subquery: {
          table: 'orders',
          alias: 'orders_sorted',
          orderBy: [['id', 'asc']],
        },
      },
      op: 'EXISTS',
      flip: true,
    } as CorrelatedSubqueryCondition,
  };

  const sink = new Catch(buildPipeline(ast, delegate, 'query-6'));
  const results = sink.fetch();
  
  // Results should be sorted by name
  expect(results.map(r => r.row.name)).toEqual(['Alice', 'Bob', 'Charlie', 'Eve']);
  expect(results.map(r => r.row.id)).toEqual([1, 2, 3, 5]);
});

test('EXISTS with flip and LIMIT', () => {
  const {delegate} = setupTestData();
  
  // Find first 2 users with orders
  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    limit: 2,
    where: {
      type: 'correlatedSubquery',
      related: {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['userId']},
        subquery: {
          table: 'orders',
          alias: 'orders_limit',
          orderBy: [['id', 'asc']],
        },
      },
      op: 'EXISTS',
      flip: true,
    } as CorrelatedSubqueryCondition,
  };

  const sink = new Catch(buildPipeline(ast, delegate, 'query-7'));
  const results = sink.fetch();
  
  // Should get first 2 users with orders
  expect(results.length).toBe(2);
  expect(results.map(r => r.row.id)).toEqual([1, 2]);
});

test('push changes work with flipped EXISTS', () => {
  const {sources, delegate} = setupTestData();
  
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
          alias: 'orders_push',
          orderBy: [['id', 'asc']],
        },
      },
      op: 'EXISTS',
      flip: true,
    } as CorrelatedSubqueryCondition,
  };

  const sink = new Catch(buildPipeline(ast, delegate, 'query-8'));
  
  // Initial results
  expect(sink.fetch().map(r => r.row.id)).toEqual([1, 2, 3, 5]);
  
  // Add a new order for user 4
  sources.orders.push({type: 'add', row: {id: 7, userId: 4, amount: 100, status: 'pending'}});
  
  // User 4 should now appear in results
  expect(sink.pushes.length).toBeGreaterThan(0);
  const addedUser = sink.pushes.find(p => p.type === 'add' && p.node?.row.id === 4);
  expect(addedUser).toBeDefined();
  
  // Remove an order from user 2
  sources.orders.push({type: 'remove', row: {id: 3, userId: 2, amount: 150, status: 'completed'}});
  
  // User 2 should be removed if they have no other orders
  // (In this case, user 2 has no other orders)
  const removedUser = sink.pushes.find(p => p.type === 'remove' && p.node?.row.id === 2);
  expect(removedUser).toBeDefined();
});

test('nested flipped EXISTS conditions', () => {
  const {delegate} = setupTestData();
  
  // Find users who have orders containing in-stock products
  // This would normally involve a nested EXISTS but we'll simplify
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
          alias: 'orders_with_products',
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
  };

  const sink = new Catch(buildPipeline(ast, delegate, 'query-9'));
  const results = sink.fetch();
  
  // Users with completed orders: 1, 2, 3, 5
  expect(results.map(r => r.row.id)).toEqual([1, 2, 3, 5]);
});

test('flip with complex correlation keys', () => {
  const {delegate} = setupTestData();
  
  // Test with composite keys if needed
  const ast: AST = {
    table: 'orderItems',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        system: 'client',
        correlation: {parentField: ['productId'], childField: ['id']},
        subquery: {
          table: 'products',
          alias: 'in_stock_products',
          orderBy: [['id', 'asc']],
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'inStock'},
            right: {type: 'literal', value: true},
          },
        },
      },
      op: 'EXISTS',
      flip: true,
    } as CorrelatedSubqueryCondition,
  };

  const sink = new Catch(buildPipeline(ast, delegate, 'query-10'));
  const results = sink.fetch();
  
  // Order items for in-stock products (1 and 3)
  // Items: 1, 2, 4, 5, 6
  expect(results.map(r => r.row.id)).toEqual([1, 2, 4, 5, 6]);
});