import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {createPushScript} from './create-push-script.ts';

const user = table('user')
  .columns({
    id: number(),
    name: string(),
    age: number().optional(),
  })
  .primaryKey('id');

const testSchema = createSchema({
  tables: [user],
  relationships: [],
});

function createFaker(seed: number) {
  const randomizer = generateMersenne53Randomizer(seed);
  const rng = () => randomizer.next();
  const faker = new Faker({locale: en, randomizer});
  return {rng, faker};
}

test('edit and remove operations only reference previously added rows (by value)', () => {
  const {rng, faker} = createFaker(12345);
  const query: AST = {
    table: 'user',
    limit: 5,
    where: {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'age'},
      right: {type: 'literal', value: 25},
    },
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  // Track which rows have been added (by JSON serialization for value comparison)
  const addedRows = new Map<string, Set<string>>(); // table -> set of JSON strings

  for (const [tableName, change] of changes) {
    if (!addedRows.has(tableName)) {
      addedRows.set(tableName, new Set());
    }

    const tableRows = addedRows.get(tableName)!;

    switch (change.type) {
      case 'add': {
        // Add the row to our tracking
        const rowKey = JSON.stringify(change.row);
        tableRows.add(rowKey);
        break;
      }

      case 'edit': {
        // The oldRow must exist in our added rows
        const oldRowKey = JSON.stringify(change.oldRow);
        expect(
          tableRows.has(oldRowKey),
          `Edit operation references row that was not previously added. ` +
            `Table: ${tableName}, oldRow: ${JSON.stringify(change.oldRow)}`,
        ).toBe(true);

        // Remove old row and add new row
        tableRows.delete(oldRowKey);
        const newRowKey = JSON.stringify(change.row);
        tableRows.add(newRowKey);
        break;
      }

      case 'remove': {
        // The row must exist in our added rows
        const rowKey = JSON.stringify(change.row);
        expect(
          tableRows.has(rowKey),
          `Remove operation references row that was not previously added. ` +
            `Table: ${tableName}, row: ${JSON.stringify(change.row)}`,
        ).toBe(true);

        // Remove the row from tracking
        tableRows.delete(rowKey);
        break;
      }
    }
  }
});

test('edit and remove operations only reference previously added rows (by identity)', () => {
  const {rng, faker} = createFaker(12345);
  const query: AST = {
    table: 'user',
    limit: 5,
    where: {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'age'},
      right: {type: 'literal', value: 25},
    },
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  // Track which row objects have been added (by reference/identity)
  const addedRows = new Map<string, Set<unknown>>(); // table -> set of row objects

  for (const [tableName, change] of changes) {
    if (!addedRows.has(tableName)) {
      addedRows.set(tableName, new Set());
    }

    const tableRows = addedRows.get(tableName)!;

    switch (change.type) {
      case 'add':
        // Add the row to our tracking
        tableRows.add(change.row);
        break;

      case 'edit':
        // The oldRow must exist in our added rows (by identity)
        expect(
          tableRows.has(change.oldRow),
          `Edit operation references row object that was not previously added. ` +
            `Table: ${tableName}, oldRow: ${JSON.stringify(change.oldRow)}`,
        ).toBe(true);

        // Remove old row and add new row
        tableRows.delete(change.oldRow);
        tableRows.add(change.row);
        break;

      case 'remove':
        // The row must exist in our added rows (by identity)
        expect(
          tableRows.has(change.row),
          `Remove operation references row object that was not previously added. ` +
            `Table: ${tableName}, row: ${JSON.stringify(change.row)}`,
        ).toBe(true);

        // Remove the row from tracking
        tableRows.delete(change.row);
        break;
    }
  }
});

test('no edits or removes before any adds', () => {
  const {rng, faker} = createFaker(54321);
  const query: AST = {
    table: 'user',
    limit: 10,
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  const firstNonAddIndex = changes.findIndex(
    ([, change]) => change.type !== 'add',
  );

  if (firstNonAddIndex !== -1) {
    // There should be at least one add before the first edit/remove
    const addsBeforeFirstNonAdd = changes
      .slice(0, firstNonAddIndex)
      .filter(([, change]) => change.type === 'add');

    expect(
      addsBeforeFirstNonAdd.length,
      'Found edit/remove operation before any add operations',
    ).toBeGreaterThan(0);
  }
});

test('primary keys are unique across all tables', () => {
  const {rng, faker} = createFaker(77777);
  const query: AST = {
    table: 'user',
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  // Collect all primary keys across all tables
  const allPKs = new Set<string>();

  for (const [, change] of changes) {
    if (change.type === 'add') {
      const pkValue = JSON.stringify([(change.row as {id: unknown}).id]);

      expect(
        allPKs.has(pkValue),
        `Duplicate primary key found across tables: ${pkValue}`,
      ).toBe(false);

      allPKs.add(pkValue);
    }
  }

  // Should have generated multiple unique PKs
  expect(allPKs.size).toBeGreaterThan(0);
});

test('no duplicate primary keys in add operations', () => {
  const {rng, faker} = createFaker(99999);
  const query: AST = {
    table: 'user',
    limit: 20,
    where: {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'age'},
      right: {type: 'literal', value: 25},
    },
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  // Track which primary keys have been added (and not removed)
  const activePrimaryKeys = new Map<string, Set<string>>(); // table -> set of PK JSON strings

  for (const [tableName, change] of changes) {
    if (!activePrimaryKeys.has(tableName)) {
      activePrimaryKeys.set(tableName, new Set());
    }

    const tablePKs = activePrimaryKeys.get(tableName)!;

    switch (change.type) {
      case 'add': {
        // Get the primary key for this row (assuming 'id' is the PK)
        const pkValue = JSON.stringify([(change.row as {id: unknown}).id]);

        expect(
          tablePKs.has(pkValue),
          `Duplicate primary key found in add operation. ` +
            `Table: ${tableName}, PK: ${pkValue}, Row: ${JSON.stringify(change.row)}`,
        ).toBe(false);

        tablePKs.add(pkValue);
        break;
      }

      case 'edit': {
        // Remove old PK, add new PK
        const oldPkValue = JSON.stringify([
          (change.oldRow as {id: unknown}).id,
        ]);
        const newPkValue = JSON.stringify([(change.row as {id: unknown}).id]);

        tablePKs.delete(oldPkValue);
        if (oldPkValue !== newPkValue) {
          expect(
            tablePKs.has(newPkValue),
            `Edit operation creates duplicate primary key. ` +
              `Table: ${tableName}, PK: ${newPkValue}`,
          ).toBe(false);
        }
        tablePKs.add(newPkValue);
        break;
      }

      case 'remove': {
        // Remove PK from active set
        const pkValue = JSON.stringify([(change.row as {id: unknown}).id]);
        tablePKs.delete(pkValue);
        break;
      }
    }
  }
});
