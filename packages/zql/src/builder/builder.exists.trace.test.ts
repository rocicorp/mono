import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST, CorrelatedSubqueryCondition} from '../../../zero-protocol/src/ast.ts';
import {Catch} from '../ivm/catch.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {buildPipeline} from './builder.ts';
import {TestBuilderDelegate} from './test-builder-delegate.ts';

const lc = createSilentLogContext();

test('trace flipped EXISTS logic', () => {
  // Create minimal test data
  const users = createSource(
    lc,
    testLogConfig,
    'users',
    {
      id: {type: 'number'},
      name: {type: 'string'},
    },
    ['id'],
  );
  users.push({type: 'add', row: {id: 1, name: 'Alice'}});
  users.push({type: 'add', row: {id: 2, name: 'Bob'}});
  users.push({type: 'add', row: {id: 3, name: 'Charlie'}});

  const orders = createSource(
    lc,
    testLogConfig,
    'orders',
    {
      id: {type: 'number'},
      userId: {type: 'number'},
    },
    ['id'],
  );
  orders.push({type: 'add', row: {id: 1, userId: 1}});
  orders.push({type: 'add', row: {id: 2, userId: 3}});

  const sources = {users, orders};
  const delegate = new TestBuilderDelegate(sources);

  const astWithFlip: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['userId']},
        subquery: {
          table: 'orders',
          alias: 'orders_exists_flip',
          orderBy: [['id', 'asc']],
        },
      },
      op: 'EXISTS',
      flip: true,
    } as CorrelatedSubqueryCondition,
  };

  console.log('Building pipeline with flip...');
  const pipeline = buildPipeline(astWithFlip, delegate, 'query-trace');
  
  // Check if the pipeline was built
  console.log('Pipeline built:', pipeline);
  
  // Let's trace step by step - build just the orders source first
  const ordersOnly = sources.orders.connect(
    [['id', 'asc']],
    undefined,
    new Set(),
  );
  const ordersCatch = new Catch(ordersOnly);
  console.log('Orders only:', ordersCatch.fetch().map(r => r.row));
  
  const sink = new Catch(pipeline);
  
  // Manually trace through what the pipeline should do:
  // 1. Start with orders table [id:1, userId:1], [id:2, userId:3]
  // 2. Join users (flipped): orders becomes parent, users becomes child
  //    After join, we have:
  //    - Order 1 with user 1 nested
  //    - Order 2 with user 3 nested
  // 3. Apply EXISTS filter on 'parent' relationship
  //    Since we have the relationship, both pass
  // 4. Extract users from 'parent' path
  //    Should extract user 1 and user 3
  // 5. Sort by id ascending
  //    Results: [1, 3]
  
  console.log('Fetching from pipeline...');
  const results = sink.fetch();
  console.log('Raw results:', results);
  console.log('Results:', results.map(r => r.row));
  
  // The test - we expect users 1 and 3 (those with orders)
  expect(results.map(r => r.row.id)).toEqual([1, 3]);
});