import {describe, expect, test} from 'vitest';
import {HLLStatsManager, type Row} from './hll-stats-manager.ts';

describe('HLLStatsManager', () => {
  describe('basic operations', () => {
    test('starts empty', () => {
      const manager = new HLLStatsManager();
      expect(manager.getTables()).toEqual([]);
      expect(manager.getRowCount('users')).toBe(0);
    });

    test('onAdd increments row count', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('users', {id: 1, name: 'Alice'});
      expect(manager.getRowCount('users')).toBe(1);

      manager.onAdd('users', {id: 2, name: 'Bob'});
      expect(manager.getRowCount('users')).toBe(2);
    });

    test('onAdd updates column sketches', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('users', {id: 1, name: 'Alice'});
      manager.onAdd('users', {id: 2, name: 'Bob'});
      manager.onAdd('users', {id: 3, name: 'Charlie'});

      const idCardinality = manager.getCardinality('users', 'id');
      const nameCardinality = manager.getCardinality('users', 'name');

      expectWithinError(idCardinality.cardinality, 3, 0.2);
      expectWithinError(nameCardinality.cardinality, 3, 0.2);
    });

    test('onRemove decrements row count', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('users', {id: 1, name: 'Alice'});
      manager.onAdd('users', {id: 2, name: 'Bob'});
      expect(manager.getRowCount('users')).toBe(2);

      manager.onRemove('users', {id: 1, name: 'Alice'});
      expect(manager.getRowCount('users')).toBe(1);

      manager.onRemove('users', {id: 2, name: 'Bob'});
      expect(manager.getRowCount('users')).toBe(0);
    });

    test('onRemove does not go below zero', () => {
      const manager = new HLLStatsManager();

      manager.onRemove('users', {id: 1});
      expect(manager.getRowCount('users')).toBe(0);
    });

    test('onEdit does not change row count', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('users', {id: 1, name: 'Alice'});
      expect(manager.getRowCount('users')).toBe(1);

      manager.onEdit('users', {id: 1, name: 'Alice'}, {id: 1, name: 'Alicia'});
      expect(manager.getRowCount('users')).toBe(1);
    });

    test('onEdit updates changed columns only', () => {
      const manager = new HLLStatsManager();

      // Add initial rows
      manager.onAdd('users', {id: 1, name: 'Alice', age: 30});
      manager.onAdd('users', {id: 2, name: 'Bob', age: 25});

      // Edit name but not age
      manager.onEdit(
        'users',
        {id: 1, name: 'Alice', age: 30},
        {id: 1, name: 'Alicia', age: 30},
      );

      // Name cardinality should increase (Alice + Alicia + Bob = 3)
      const nameCardinality = manager.getCardinality('users', 'name');
      expectWithinError(nameCardinality.cardinality, 3, 0.3);

      // Age cardinality should stay at 2 (30, 25)
      const ageCardinality = manager.getCardinality('users', 'age');
      expectWithinError(ageCardinality.cardinality, 2, 0.3);
    });
  });

  describe('cardinality queries', () => {
    test('returns zero for non-existent table/column', () => {
      const manager = new HLLStatsManager();
      const result = manager.getCardinality('users', 'id');
      expect(result.cardinality).toBe(0);
      expect(result.confidence).toBe('none');
    });

    test('estimates cardinality correctly', () => {
      const manager = new HLLStatsManager();

      // Add rows with varying cardinalities
      for (let i = 0; i < 1000; i++) {
        manager.onAdd('users', {
          id: i, // 1000 distinct
          category: i % 10, // 10 distinct
          status: i % 2, // 2 distinct
        });
      }

      const idCard = manager.getCardinality('users', 'id');
      const categoryCard = manager.getCardinality('users', 'category');
      const statusCard = manager.getCardinality('users', 'status');

      expectWithinError(idCard.cardinality, 1000, 0.05);
      expectWithinError(categoryCard.cardinality, 10, 0.2);
      expectWithinError(statusCard.cardinality, 2, 0.5);
    });

    test('handles null and undefined values', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('users', {id: 1, name: 'Alice'});
      manager.onAdd('users', {id: 2, name: null});
      manager.onAdd('users', {id: 3, name: undefined});
      manager.onAdd('users', {id: 4}); // name is undefined

      const nameCard = manager.getCardinality('users', 'name');
      // Alice, null, undefined = 3 distinct
      expectWithinError(nameCard.cardinality, 3, 0.3);
    });

    test('confidence levels based on sample size', () => {
      const manager = new HLLStatsManager();

      // Add 50 rows (below med threshold)
      for (let i = 0; i < 50; i++) {
        manager.onAdd('t1', {id: i});
      }

      // Add 500 rows (med threshold)
      for (let i = 0; i < 500; i++) {
        manager.onAdd('t2', {id: i});
      }

      // Add 5000 rows (high threshold)
      for (let i = 0; i < 5000; i++) {
        manager.onAdd('t3', {id: i});
      }

      expect(manager.getCardinality('t1', 'id').confidence).toBe('none');
      expect(manager.getCardinality('t2', 'id').confidence).toBe('med');
      expect(manager.getCardinality('t3', 'id').confidence).toBe('high');
    });
  });

  describe('fanout queries', () => {
    test('returns 1 for empty table', () => {
      const manager = new HLLStatsManager();
      const result = manager.getFanout('users', 'id');
      expect(result.fanout).toBe(1);
      expect(result.confidence).toBe('none');
    });

    test('calculates fanout correctly', () => {
      const manager = new HLLStatsManager();

      // Add 1000 rows with 10 distinct categories
      // Expected fanout: 1000 / 10 = 100
      for (let i = 0; i < 1000; i++) {
        manager.onAdd('users', {
          id: i,
          category: i % 10,
        });
      }

      const fanout = manager.getFanout('users', 'category');
      expectWithinError(fanout.fanout, 100, 0.1);
      expect(fanout.confidence).toBe('none'); // 10 distinct values (below threshold)
    });

    test('fanout of 1 for unique column', () => {
      const manager = new HLLStatsManager();

      for (let i = 0; i < 100; i++) {
        manager.onAdd('users', {id: i});
      }

      const fanout = manager.getFanout('users', 'id');
      expectWithinError(fanout.fanout, 1, 0.1);
    });

    test('high fanout for low cardinality', () => {
      const manager = new HLLStatsManager();

      // 1000 rows, 2 distinct values
      // Expected fanout: 1000 / 2 = 500
      for (let i = 0; i < 1000; i++) {
        manager.onAdd('users', {status: i % 2});
      }

      const fanout = manager.getFanout('users', 'status');
      expectWithinError(fanout.fanout, 500, 0.1);
    });
  });

  describe('deletion tracking', () => {
    test('tracks deletion count', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('users', {id: 1});
      manager.onAdd('users', {id: 2});
      manager.onAdd('users', {id: 3});

      expect(manager.getDeletionRatio('users')).toBe(0);

      manager.onRemove('users', {id: 1});
      // Deletion ratio = deletions / (rows + deletions) = 1 / (2 + 1) = 1/3
      expect(manager.getDeletionRatio('users')).toBeCloseTo(1 / 3, 2);

      manager.onRemove('users', {id: 2});
      // Deletion ratio = deletions / (rows + deletions) = 2 / (1 + 2) = 2/3
      expect(manager.getDeletionRatio('users')).toBeCloseTo(2 / 3, 2);
    });

    test('shouldRebuild based on threshold', () => {
      const manager = new HLLStatsManager();

      // Add 100 rows
      for (let i = 0; i < 100; i++) {
        manager.onAdd('users', {id: i});
      }

      expect(manager.shouldRebuild('users', 0.2)).toBe(false);

      // Delete 15 rows (15% deletion ratio)
      for (let i = 0; i < 15; i++) {
        manager.onRemove('users', {id: i});
      }
      expect(manager.shouldRebuild('users', 0.2)).toBe(false);

      // Delete 10 more rows (25% deletion ratio)
      for (let i = 15; i < 25; i++) {
        manager.onRemove('users', {id: i});
      }
      expect(manager.shouldRebuild('users', 0.2)).toBe(true);
    });
  });

  describe('snapshot and restore', () => {
    test('snapshot captures all state', () => {
      const manager = new HLLStatsManager();

      for (let i = 0; i < 100; i++) {
        manager.onAdd('users', {id: i, name: `user-${i}`});
      }

      const snapshot = manager.snapshot();

      expect(snapshot.version).toBe(1);
      expect(snapshot.rowCounts.users).toBe(100);
      expect(snapshot.sketches['users:id']).toBeDefined();
      expect(snapshot.sketches['users:name']).toBeDefined();
    });

    test('restore recreates exact state', () => {
      const manager1 = new HLLStatsManager();

      for (let i = 0; i < 100; i++) {
        manager1.onAdd('users', {id: i, name: `user-${i}`});
      }

      const snapshot = manager1.snapshot();

      const manager2 = new HLLStatsManager();
      manager2.restore(snapshot);

      expect(manager2.getRowCount('users')).toBe(100);

      const card1 = manager1.getCardinality('users', 'id');
      const card2 = manager2.getCardinality('users', 'id');
      expect(card2.cardinality).toBeCloseTo(card1.cardinality, 0);
    });

    test('restore clears existing state', () => {
      const manager = new HLLStatsManager();

      // Add data to one table
      manager.onAdd('users', {id: 1});

      // Restore snapshot from different table
      const snapshot = {
        version: 1,
        sketches: {},
        rowCounts: {posts: 5},
      };

      manager.restore(snapshot);

      expect(manager.getRowCount('users')).toBe(0);
      expect(manager.getRowCount('posts')).toBe(5);
    });

    test('restore throws on version mismatch', () => {
      const manager = new HLLStatsManager();

      const invalidSnapshot = {
        version: 999,
        sketches: {},
        rowCounts: {},
      };

      expect(() => manager.restore(invalidSnapshot)).toThrow(/version/i);
    });

    test('snapshot after deletions', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('users', {id: 1});
      manager.onAdd('users', {id: 2});
      manager.onRemove('users', {id: 1});

      const snapshot = manager.snapshot();
      expect(snapshot.rowCounts.users).toBe(1);

      // Restore and verify
      const manager2 = new HLLStatsManager();
      manager2.restore(snapshot);
      expect(manager2.getRowCount('users')).toBe(1);
    });
  });

  describe('rebuild functionality', () => {
    test('rebuild from data source', () => {
      const manager = new HLLStatsManager();

      // Simulate some operations that create stale state
      manager.onAdd('users', {id: 1, name: 'Alice'});
      manager.onAdd('users', {id: 2, name: 'Bob'});
      manager.onRemove('users', {id: 1, name: 'Alice'});

      expect(manager.getRowCount('users')).toBe(1);

      // Now rebuild from fresh data
      const dataSource = [
        {table: 'users', row: {id: 2, name: 'Bob'}},
        {table: 'users', row: {id: 3, name: 'Charlie'}},
      ];

      manager.rebuild(dataSource);

      expect(manager.getRowCount('users')).toBe(2);
      expect(manager.getDeletionRatio('users')).toBe(0); // Deletions cleared

      const idCard = manager.getCardinality('users', 'id');
      expectWithinError(idCard.cardinality, 2, 0.3);
    });

    test('rebuild with multiple tables', () => {
      const manager = new HLLStatsManager();

      const dataSource = [
        {table: 'users', row: {id: 1}},
        {table: 'users', row: {id: 2}},
        {table: 'posts', row: {id: 1, userId: 1}},
        {table: 'posts', row: {id: 2, userId: 1}},
        {table: 'posts', row: {id: 3, userId: 2}},
      ];

      manager.rebuild(dataSource);

      expect(manager.getRowCount('users')).toBe(2);
      expect(manager.getRowCount('posts')).toBe(3);
      expect(manager.getTables().sort()).toEqual(['posts', 'users']);
    });

    test('rebuildTable only affects one table', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('users', {id: 1});
      manager.onAdd('posts', {id: 1});

      expect(manager.getRowCount('users')).toBe(1);
      expect(manager.getRowCount('posts')).toBe(1);

      // Rebuild only users
      const userData = [{id: 1}, {id: 2}, {id: 3}];
      manager.rebuildTable('users', userData);

      expect(manager.getRowCount('users')).toBe(3);
      expect(manager.getRowCount('posts')).toBe(1); // Unchanged
    });

    test('rebuildTable clears deletion tracking', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('users', {id: 1});
      manager.onAdd('users', {id: 2});
      manager.onRemove('users', {id: 1});

      expect(manager.getDeletionRatio('users')).toBeGreaterThan(0);

      manager.rebuildTable('users', [{id: 2}]);

      expect(manager.getDeletionRatio('users')).toBe(0);
    });
  });

  describe('utility methods', () => {
    test('getTables returns all tables', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('users', {id: 1});
      manager.onAdd('posts', {id: 1});
      manager.onAdd('comments', {id: 1});

      const tables = manager.getTables().sort();
      expect(tables).toEqual(['comments', 'posts', 'users']);
    });

    test('getColumns returns columns for table', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('users', {
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
      });

      const columns = manager.getColumns('users').sort();
      expect(columns).toEqual(['email', 'id', 'name']);
    });

    test('getColumns returns empty for non-existent table', () => {
      const manager = new HLLStatsManager();
      expect(manager.getColumns('users')).toEqual([]);
    });

    test('clear removes all state', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('users', {id: 1, name: 'Alice'});
      manager.onAdd('posts', {id: 1, title: 'Post'});

      expect(manager.getTables()).toHaveLength(2);

      manager.clear();

      expect(manager.getTables()).toEqual([]);
      expect(manager.getRowCount('users')).toBe(0);
      expect(manager.getCardinality('users', 'id').cardinality).toBe(0);
    });
  });

  describe('edge cases', () => {
    test('handles empty rows', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('users', {});
      expect(manager.getRowCount('users')).toBe(1);
      expect(manager.getColumns('users')).toEqual([]);
    });

    test('handles rows with many columns', () => {
      const manager = new HLLStatsManager();

      const row: Row = {};
      for (let i = 0; i < 100; i++) {
        row[`col${i}`] = i;
      }

      manager.onAdd('users', row);
      expect(manager.getColumns('users')).toHaveLength(100);
    });

    test('handles special characters in table/column names', () => {
      const manager = new HLLStatsManager();

      manager.onAdd('user-table', {'column:name': 'value'});

      const result = manager.getCardinality('user-table', 'column:name');
      expectWithinError(result.cardinality, 1, 0.5);
    });

    test('consistent results across operations', () => {
      const manager = new HLLStatsManager();

      // Add same data multiple times
      for (let trial = 0; trial < 3; trial++) {
        for (let i = 0; i < 100; i++) {
          manager.onAdd('users', {id: i});
        }
      }

      // Should still estimate ~100 distinct IDs
      const card = manager.getCardinality('users', 'id');
      expectWithinError(card.cardinality, 100, 0.1);
    });
  });

  describe('real-world scenarios', () => {
    test('tracks users table with updates', () => {
      const manager = new HLLStatsManager();

      // Insert 1000 users
      for (let i = 0; i < 1000; i++) {
        manager.onAdd('users', {
          id: i,
          email: `user${i}@example.com`,
          country: ['US', 'UK', 'CA', 'AU'][i % 4],
          status: i % 10 < 8 ? 'active' : 'inactive',
        });
      }

      // Update some emails
      for (let i = 0; i < 100; i++) {
        manager.onEdit(
          'users',
          {
            id: i,
            email: `user${i}@example.com`,
            country: 'US',
            status: 'active',
          },
          {
            id: i,
            email: `newuser${i}@example.com`,
            country: 'US',
            status: 'active',
          },
        );
      }

      // Delete some users
      for (let i = 0; i < 50; i++) {
        manager.onRemove('users', {id: i});
      }

      expect(manager.getRowCount('users')).toBe(950);

      const idCard = manager.getCardinality('users', 'id');
      const countryCard = manager.getCardinality('users', 'country');
      const statusCard = manager.getCardinality('users', 'status');

      expectWithinError(idCard.cardinality, 1000, 0.05);
      expectWithinError(countryCard.cardinality, 4, 0.3);
      expectWithinError(statusCard.cardinality, 2, 0.5);
    });

    test('multi-table e-commerce scenario', () => {
      const manager = new HLLStatsManager();

      // Products: 100 products, 10 categories
      for (let i = 0; i < 100; i++) {
        manager.onAdd('products', {
          id: i,
          categoryId: i % 10,
          price: Math.floor(Math.random() * 100),
        });
      }

      // Orders: 1000 orders from 200 customers
      for (let i = 0; i < 1000; i++) {
        manager.onAdd('orders', {
          id: i,
          customerId: i % 200,
          productId: i % 100,
        });
      }

      // Check fanouts
      const productCategory = manager.getFanout('products', 'categoryId');
      expectWithinError(productCategory.fanout, 10, 0.2); // 100/10 = 10

      const orderCustomer = manager.getFanout('orders', 'customerId');
      expectWithinError(orderCustomer.fanout, 5, 0.2); // 1000/200 = 5

      const orderProduct = manager.getFanout('orders', 'productId');
      expectWithinError(orderProduct.fanout, 10, 0.2); // 1000/100 = 10
    });
  });
});

/**
 * Helper to assert that a value is within a relative error of the expected value.
 */
function expectWithinError(
  actual: number,
  expected: number,
  relativeError: number,
): void {
  const diff = Math.abs(actual - expected);
  const maxDiff = expected * relativeError;
  expect(diff).toBeLessThan(maxDiff);
}
