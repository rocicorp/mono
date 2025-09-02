import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {Catch} from '../ivm/catch.ts';
import {Join} from '../ivm/join.ts';
import {Exists} from '../ivm/exists.ts';
import {ExtractMatchingKeys} from '../ivm/extract-matching-keys.ts';
import {SortToRootOrder} from '../ivm/sort-to-root-order.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {MemoryStorage} from '../ivm/memory-storage.ts';
import {buildPipeline} from './builder.ts';
import {TestBuilderDelegate} from './test-builder-delegate.ts';

const lc = createSilentLogContext();

test('step by step flipped EXISTS', () => {
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

  // Step 1: Connect to sources
  const ordersConn = orders.connect([['id', 'asc']], undefined, new Set());
  const usersConn = users.connect([['id', 'asc']], undefined, new Set());
  
  console.log('Step 1 - Orders source:');
  const ordersCatch1 = new Catch(ordersConn);
  console.log(ordersCatch1.fetch().map(r => r.row));
  
  console.log('Step 1 - Users source:');
  const usersCatch1 = new Catch(usersConn);
  console.log(usersCatch1.fetch().map(r => r.row));
  
  // Step 2: Create flipped join (orders as parent, users as child)
  const flippedJoin = new Join({
    parent: ordersConn,
    child: usersConn,
    storage: new MemoryStorage(),
    parentKey: ['userId'],  // orders.userId
    childKey: ['id'],       // users.id
    relationshipName: 'parent',
    hidden: false,
    system: 'client',
  });
  
  console.log('\nStep 2 - After flipped join:');
  const joinCatch = new Catch(flippedJoin);
  const joinResults = joinCatch.fetch();
  console.log('Number of results:', joinResults.length);
  joinResults.forEach(r => {
    console.log('Order:', r.row);
    console.log('  Has parent relationship?', 'parent' in r.relationships);
    if ('parent' in r.relationships) {
      const parentRel = r.relationships.parent;
      if (typeof parentRel === 'function') {
        const parentGen = parentRel();
        const parents = Array.from(parentGen);
        console.log('  Parent users:', parents.map(p => p.row));
      } else {
        console.log('  Parent relationship is not a function:', typeof parentRel);
      }
    }
  });
  
  // Step 3: Apply EXISTS filter
  const exists = new Exists(
    flippedJoin,
    new MemoryStorage(),
    'parent',
    ['userId'],  // The key field in the parent (orders)
    'EXISTS',
  );
  
  console.log('\nStep 3 - After EXISTS filter:');
  const existsCatch = new Catch(exists);
  const existsResults = existsCatch.fetch();
  console.log('Number of results:', existsResults.length);
  existsResults.forEach(r => {
    console.log('Order:', r.row);
    if ('parent' in r.relationships) {
      const parentRel = r.relationships.parent;
      if (typeof parentRel === 'function') {
        const parentGen = parentRel();
        const parents = Array.from(parentGen);
        console.log('  Parent users:', parents.map(p => p.row));
      } else {
        console.log('  Parent relationship is not a function:', typeof parentRel);
      }
    }
  });
  
  // Step 4: Extract users from 'parent' relationship
  const extractor = new ExtractMatchingKeys({
    input: exists,
    targetTable: 'users',
    targetPath: ['parent'],
    targetSchema: users.connect([['id', 'asc']], undefined, new Set()).getSchema(),
  });
  
  console.log('\nStep 4 - After extraction:');
  const extractCatch = new Catch(extractor);
  const extractResults = extractCatch.fetch();
  console.log('Extracted users:', extractResults.map(r => r.row));
  
  // Step 5: Sort to original order
  const sorter = new SortToRootOrder({
    input: extractor,
    storage: new MemoryStorage(),
    targetSort: [['id', 'asc']],
  });
  
  console.log('\nStep 5 - After sorting:');
  const sortCatch = new Catch(sorter);
  const sortResults = sortCatch.fetch();
  console.log('Final sorted users:', sortResults.map(r => r.row));
  
  // Test that we get the expected users
  expect(sortResults.map(r => r.row.id)).toEqual([1, 3]);
});