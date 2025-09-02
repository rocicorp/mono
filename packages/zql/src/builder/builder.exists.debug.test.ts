import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST, CorrelatedSubqueryCondition} from '../../../zero-protocol/src/ast.ts';
import {Catch} from '../ivm/catch.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {buildPipeline} from './builder.ts';
import {TestBuilderDelegate} from './test-builder-delegate.ts';

const lc = createSilentLogContext();

test('debug flipped EXISTS', () => {
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
  const delegate = new TestBuilderDelegate(sources, true); // Enable logging

  // Test without flip first
  console.log('Testing WITHOUT flip:');
  const astNoFlip: AST = {
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

  const sinkNoFlip = new Catch(buildPipeline(astNoFlip, delegate, 'query-no-flip'));
  const resultsNoFlip = sinkNoFlip.fetch();
  console.log('Results without flip:', resultsNoFlip.map(r => r.row));
  expect(resultsNoFlip.map(r => r.row.id)).toEqual([1, 3]);

  // Clear log
  delegate.clearLog();

  // Test WITH flip
  console.log('\nTesting WITH flip:');
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

  const sinkWithFlip = new Catch(buildPipeline(astWithFlip, delegate, 'query-with-flip'));
  const resultsWithFlip = sinkWithFlip.fetch();
  console.log('Results with flip:', resultsWithFlip.map(r => r.row));
  
  // Log pipeline structure
  console.log('\nPipeline log:');
  delegate.log.forEach(msg => {
    console.log(`  ${JSON.stringify(msg)}`);
  });

  expect(resultsWithFlip.map(r => r.row.id)).toEqual([1, 3]);
});