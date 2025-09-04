import {expect, test} from 'vitest';
import {QueryImpl, defaultFormat} from './query-impl.ts';
import type {QueryDelegate} from './query-delegate.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {assert} from '../../../shared/src/asserts.ts';

const mockDelegate = {} as QueryDelegate;

// Create test schema with users and posts
const usersTable = table('users')
  .columns({
    id: number(),
    name: string(),
  })
  .primaryKey('id');

const postsTable = table('posts')
  .columns({
    id: number(),
    userId: number(),
    title: string(),
    published: boolean().optional(),
  })
  .primaryKey('id');

const usersRelationships = relationships(usersTable, ({many}) => ({
  posts: many({
    sourceField: ['id'],
    destField: ['userId'],
    destSchema: postsTable,
  }),
}));

const postsRelationships = relationships(postsTable, ({one}) => ({
  user: one({
    sourceField: ['userId'],
    destField: ['id'],
    destSchema: usersTable,
  }),
}));

const testSchema = createSchema({
  tables: [usersTable, postsTable],
  relationships: [usersRelationships, postsRelationships],
});

test('whereExists accepts flip option', () => {
  // Test with flip option
  const query = new QueryImpl(
    mockDelegate,
    testSchema,
    'users',
    {table: 'users', orderBy: [['id', 'asc']]},
    defaultFormat,
    undefined,
    undefined,
  ).whereExists('posts', {root: true});

  const {ast} = query;
  expect(ast.where).toBeDefined();
  assert(ast.where?.type === 'correlatedSubquery');
  expect(ast.where?.root).toBe(true);
});

test('whereExists with callback and flip option', () => {
  // Test with callback and flip option
  const query = new QueryImpl(
    mockDelegate,
    testSchema,
    'users',
    {table: 'users', orderBy: [['id', 'asc']]},
    defaultFormat,
    undefined,
    undefined,
  ).whereExists('posts', q => q, {root: true});

  const {ast} = query;
  expect(ast.where).toBeDefined();
  assert(ast.where?.type === 'correlatedSubquery');
  expect(ast.where?.root).toBe(true);
});

test('exists in where clause with flip option', () => {
  // Test exists method with flip option
  const query = new QueryImpl(
    mockDelegate,
    testSchema,
    'users',
    {table: 'users', orderBy: [['id', 'asc']]},
    defaultFormat,
    undefined,
    undefined,
  ).where(({exists}) => exists('posts', {root: true}));

  const {ast} = query;
  expect(ast.where).toBeDefined();
  assert(ast.where?.type === 'correlatedSubquery');
  expect(ast.where?.root).toBe(true);
});

test('exists with callback and flip option', () => {
  // Test exists with callback and flip option
  const query = new QueryImpl(
    mockDelegate,
    testSchema,
    'users',
    {table: 'users', orderBy: [['id', 'asc']]},
    defaultFormat,
    undefined,
    undefined,
  ).where(({exists}) => exists('posts', q => q, {root: true}));

  const {ast} = query;
  expect(ast.where).toBeDefined();
  assert(ast.where?.type === 'correlatedSubquery');
  expect(ast.where?.root).toBe(true);
});
