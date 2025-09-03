import {expect, test} from 'vitest';
import type {AST, CorrelatedSubqueryCondition} from '../../../zero-protocol/src/ast.ts';
import {buildPipeline} from './builder.ts';
import {TestBuilderDelegate} from './test-builder-delegate.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';

const lc = createSilentLogContext();

test('buildPipeline transforms flipped EXISTS', () => {
  // Create test sources
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
  
  const delegate = new TestBuilderDelegate({users, orders});
  
  // AST with flipped EXISTS
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
  
  // Build pipeline - should transform the AST internally
  const pipeline = buildPipeline(ast, delegate, 'test-query');
  
  // The pipeline should be built successfully
  expect(pipeline).toBeDefined();
  
  // TODO: Add more assertions once we can inspect the pipeline structure
  // For now, just verify it doesn't throw
});

test('buildPipeline handles non-flipped EXISTS normally', () => {
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
  
  const delegate = new TestBuilderDelegate({users, orders});
  
  // AST without flip
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
      // No flip: true
    },
  };
  
  // Build pipeline - should not transform
  const pipeline = buildPipeline(ast, delegate, 'test-query');
  
  // Should build successfully
  expect(pipeline).toBeDefined();
});