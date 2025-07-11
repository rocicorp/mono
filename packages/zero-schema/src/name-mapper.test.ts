import {expect, test} from 'vitest';
import {createSchema} from './builder/schema-builder.ts';
import {boolean, string, table} from './builder/table-builder.ts';
import {clientToServer, serverToClient} from './name-mapper.ts';

const schema = createSchema({
  tables: [
    table('issue')
      .from('issues')
      .columns({
        id: string(),
        title: string(),
        description: string(),
        closed: boolean(),
        ownerId: string().from('owner_id').nullable(),
      })
      .primaryKey('id'),
    table('comment')
      .from('comments')
      .columns({
        id: string().from('comment_id'),
        issueId: string().from('issue_id'),
        description: string(),
      })
      .primaryKey('id'),
    table('noMappings')
      .columns({
        id: string(),
        description: string(),
      })
      .primaryKey('id'),
  ],
});

test('name mapping to server', () => {
  const map = serverToClient(schema.tables);

  expect(map.tableName('issues')).toBe('issue');
  expect(map.tableName('comments')).toBe('comment');
  expect(map.tableName('noMappings')).toBe('noMappings');
  expect(() => map.tableName('unknown')).toThrowErrorMatchingInlineSnapshot(
    `[Error: unknown table "unknown" ]`,
  );

  expect(map.columnName('issues', 'id')).toBe('id');
  expect(map.columnName('comments', 'comment_id')).toBe('id');
  expect(map.columnName('noMappings', 'id')).toBe('id');
  expect(() =>
    map.columnName('comments', 'unknown'),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: unknown column "unknown" of "comments" table ]`,
  );

  expect(
    map.row('issues', {id: 'foo', ['owner_id']: 'bar', unknown: 'passthrough'}),
  ).toEqual({id: 'foo', ownerId: 'bar', unknown: 'passthrough'});
  expect(
    map.row('comments', {
      ['comment_id']: 'baz',
      ['issue_id']: 'foo',
      unknown: 'passthrough',
    }),
  ).toEqual({id: 'baz', issueId: 'foo', unknown: 'passthrough'});
  const uncopiedRow = {id: 'boo', description: 'uncopied'};
  expect(map.row('noMappings', uncopiedRow)).toBe(uncopiedRow);

  expect(map.columns('issues', ['id', 'owner_id', 'unknown'])).toEqual([
    'id',
    'ownerId',
    'unknown',
  ]);
  expect(
    map.columns('comments', ['comment_id', 'issue_id', 'unknown']),
  ).toEqual(['id', 'issueId', 'unknown']);

  const uncopiedColumns = ['id', 'description'];
  expect(map.columns('noMappings', uncopiedColumns)).toBe(uncopiedColumns);
});

test('name mapping to client', () => {
  const map = clientToServer(schema.tables);

  expect(map.tableName('issue')).toBe('issues');
  expect(map.tableName('comment')).toBe('comments');
  expect(map.tableName('noMappings')).toBe('noMappings');
  expect(() => map.tableName('unknown')).toThrowErrorMatchingInlineSnapshot(
    `[Error: unknown table "unknown" ]`,
  );

  expect(map.columnName('issue', 'id')).toBe('id');
  expect(map.columnName('comment', 'id')).toBe('comment_id');
  expect(map.columnName('noMappings', 'id')).toBe('id');
  expect(() =>
    map.columnName('comments', 'unknown'),
  ).toThrowErrorMatchingInlineSnapshot(`[Error: unknown table "comments" ]`);

  expect(
    map.row('issue', {id: 'foo', ownerId: 'bar', unknown: 'passthrough'}),
  ).toEqual({id: 'foo', ['owner_id']: 'bar', unknown: 'passthrough'});
  expect(
    map.row('comment', {
      id: 'baz',
      issueId: 'foo',
      unknown: 'passthrough',
    }),
  ).toEqual({
    ['comment_id']: 'baz',
    ['issue_id']: 'foo',
    unknown: 'passthrough',
  });
  const uncopiedRow = {id: 'boo', description: 'uncopied'};
  expect(map.row('noMappings', uncopiedRow)).toBe(uncopiedRow);

  expect(map.columns('issue', ['id', 'ownerId', 'unknown'])).toEqual([
    'id',
    'owner_id',
    'unknown',
  ]);
  expect(map.columns('comment', ['id', 'issueId', 'unknown'])).toEqual([
    'comment_id',
    'issue_id',
    'unknown',
  ]);

  const uncopiedColumns = ['id', 'description'];
  expect(map.columns('noMappings', uncopiedColumns)).toBe(uncopiedColumns);
});
