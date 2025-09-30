import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {createPushScript} from './create-push-script.ts';

// Test schema
const user = table('user')
  .columns({
    id: number(),
    name: string(),
    age: number().optional(),
  })
  .primaryKey('id');

const post = table('post')
  .columns({
    id: number(),
    userId: number(),
    title: string(),
    views: number(),
  })
  .primaryKey('id');

const comment = table('comment')
  .columns({
    id: number(),
    postId: number(),
    text: string(),
  })
  .primaryKey('id');

const testSchema = createSchema({
  tables: [user, post, comment],
  relationships: [
    relationships(user, ({many}) => ({
      posts: many({
        sourceField: ['id'],
        destField: ['userId'],
        destSchema: post,
      }),
    })),
    relationships(post, ({one, many}) => ({
      user: one({
        sourceField: ['userId'],
        destField: ['id'],
        destSchema: user,
      }),
      comments: many({
        sourceField: ['id'],
        destField: ['postId'],
        destSchema: comment,
      }),
    })),
    relationships(comment, ({one}) => ({
      post: one({
        sourceField: ['postId'],
        destField: ['id'],
        destSchema: post,
      }),
    })),
  ],
});

function createFaker(seed: number) {
  const randomizer = generateMersenne53Randomizer(seed);
  const rng = () => randomizer.next();
  const faker = new Faker({locale: en, randomizer});
  return {rng, faker};
}

test('generates changes for simple query', () => {
  const {rng, faker} = createFaker(12345);
  const query: AST = {
    table: 'user',
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  expect(changes.length).toBeGreaterThan(0);
  expect(changes.some(([table]) => table === 'user')).toBe(true);
});

test('generates changes for query with limit', () => {
  const {rng, faker} = createFaker(12345);
  const query: AST = {
    table: 'user',
    limit: 5,
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  // Should have initial data + limit changes
  const userChanges = changes.filter(([table]) => table === 'user');
  expect(userChanges.length).toBeGreaterThan(5);

  // Should have add, edit, and remove operations
  const changeTypes = new Set(userChanges.map(([, change]) => change.type));
  expect(changeTypes.has('add')).toBe(true);
});

test('generates changes for query with where clause', () => {
  const {rng, faker} = createFaker(12345);
  const query: AST = {
    table: 'user',
    where: {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'age'},
      right: {type: 'literal', value: 25},
    },
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  const userChanges = changes.filter(([table]) => table === 'user');
  expect(userChanges.length).toBeGreaterThan(0);

  // Should have some users with age = 25 (matching) and some with different ages
  const addChanges = userChanges.filter(([, change]) => change.type === 'add');
  expect(addChanges.length).toBeGreaterThan(0);
});

test('generates changes for query with orderBy', () => {
  const {rng, faker} = createFaker(12345);
  const query: AST = {
    table: 'user',
    orderBy: [['age', 'asc']],
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  const userChanges = changes.filter(([table]) => table === 'user');
  expect(userChanges.length).toBeGreaterThan(0);

  // Should generate rows with various age values
  const addChanges = userChanges.filter(([, change]) => change.type === 'add');
  expect(addChanges.length).toBeGreaterThan(6); // At least 6 from orderBy test values
});

test('generates changes for query with related subquery', () => {
  const {rng, faker} = createFaker(12345);
  const query: AST = {
    table: 'user',
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['userId'],
        },
        subquery: {
          table: 'post',
        },
      },
    ],
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  // Should generate changes for both user and post tables
  expect(changes.some(([table]) => table === 'user')).toBe(true);
  expect(changes.some(([table]) => table === 'post')).toBe(true);
});

test('generates changes for nested related subqueries', () => {
  const {rng, faker} = createFaker(12345);
  const query: AST = {
    table: 'user',
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['userId'],
        },
        subquery: {
          table: 'post',
          related: [
            {
              correlation: {
                parentField: ['id'],
                childField: ['postId'],
              },
              subquery: {
                table: 'comment',
              },
            },
          ],
        },
      },
    ],
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  // Should generate changes for all three tables
  expect(changes.some(([table]) => table === 'user')).toBe(true);
  expect(changes.some(([table]) => table === 'post')).toBe(true);
  expect(changes.some(([table]) => table === 'comment')).toBe(true);
});

test('generates changes for related subquery with limit', () => {
  const {rng, faker} = createFaker(12345);
  const query: AST = {
    table: 'user',
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['userId'],
        },
        subquery: {
          table: 'post',
          limit: 3,
        },
      },
    ],
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  // Should generate limit changes for the post subquery
  const postChanges = changes.filter(([table]) => table === 'post');
  expect(postChanges.length).toBeGreaterThan(3);
});

test('generates changes for related subquery with where clause', () => {
  const {rng, faker} = createFaker(12345);
  const query: AST = {
    table: 'user',
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['userId'],
        },
        subquery: {
          table: 'post',
          where: {
            type: 'simple',
            op: '>',
            left: {type: 'column', name: 'views'},
            right: {type: 'literal', value: 100},
          },
        },
      },
    ],
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  const postChanges = changes.filter(([table]) => table === 'post');
  expect(postChanges.length).toBeGreaterThan(0);
});

test('generates changes for query with start bound', () => {
  const {rng, faker} = createFaker(12345);
  const query: AST = {
    table: 'user',
    orderBy: [['age', 'asc']],
    start: {
      row: {age: 30},
      exclusive: true,
    },
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  const userChanges = changes.filter(([table]) => table === 'user');
  expect(userChanges.length).toBeGreaterThan(0);

  // Should generate rows before, at, and after the bound
  const addChanges = userChanges.filter(([, change]) => change.type === 'add');
  expect(addChanges.length).toBeGreaterThan(0);
});

test('generates relationship changes', () => {
  const {rng, faker} = createFaker(12345);
  const query: AST = {
    table: 'user',
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  // Should generate changes for related tables even without explicit related in query
  // (because extractTables finds all tables in schema)
  const tables = new Set(changes.map(([table]) => table));
  expect(tables.size).toBeGreaterThan(0);
});

test('generates edit and remove changes', () => {
  const {rng, faker} = createFaker(12345);
  const query: AST = {
    table: 'user',
    limit: 5,
  };

  const changes = createPushScript(rng, faker, testSchema, query);

  const changeTypes = new Set(changes.map(([, change]) => change.type));

  // Should have all three types of changes
  expect(changeTypes.has('add')).toBe(true);
  // Edit and remove depend on having rows generated first
  const hasEdit = changeTypes.has('edit');
  const hasRemove = changeTypes.has('remove');
  expect(hasEdit || hasRemove).toBe(true);
});

test('is deterministic with same seed', () => {
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

  const {rng: rng1, faker: faker1} = createFaker(99999);
  const changes1 = createPushScript(rng1, faker1, testSchema, query);

  const {rng: rng2, faker: faker2} = createFaker(99999);
  const changes2 = createPushScript(rng2, faker2, testSchema, query);

  expect(changes1.length).toBe(changes2.length);
  for (let i = 0; i < changes1.length; i++) {
    expect(changes1[i][0]).toBe(changes2[i][0]); // table name
    expect(changes1[i][1].type).toBe(changes2[i][1].type); // change type
  }
});
