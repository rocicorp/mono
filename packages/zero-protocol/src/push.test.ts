import {expect, test} from 'vitest';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {clientToServer} from '../../zero-schema/src/name-mapper.ts';
import {mapCRUD} from './push.ts';

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

test('map names', () => {
  const mapper = clientToServer(schema.tables);
  expect(
    mapCRUD(
      {
        ops: [
          {
            op: 'insert',
            tableName: 'issue',
            primaryKey: ['id'],
            value: {id: 'foo', ownerId: 'bar', closed: true},
          },
          {
            op: 'update',
            tableName: 'comment',
            primaryKey: ['id'],
            value: {id: 'baz', issueId: 'foo', description: 'boom'},
          },
          {
            op: 'upsert',
            tableName: 'noMappings',
            primaryKey: ['id'],
            value: {id: 'voo', description: 'doo'},
          },
          {
            op: 'delete',
            tableName: 'comment',
            primaryKey: ['id'],
            value: {id: 'boo'},
          },
        ],
      },
      mapper,
    ),
  ).toMatchInlineSnapshot(`
    {
      "ops": [
        {
          "op": "insert",
          "primaryKey": [
            "id",
          ],
          "tableName": "issues",
          "value": {
            "closed": true,
            "id": "foo",
            "owner_id": "bar",
          },
        },
        {
          "op": "update",
          "primaryKey": [
            "comment_id",
          ],
          "tableName": "comments",
          "value": {
            "comment_id": "baz",
            "description": "boom",
            "issue_id": "foo",
          },
        },
        {
          "op": "upsert",
          "primaryKey": [
            "id",
          ],
          "tableName": "noMappings",
          "value": {
            "description": "doo",
            "id": "voo",
          },
        },
        {
          "op": "delete",
          "primaryKey": [
            "comment_id",
          ],
          "tableName": "comments",
          "value": {
            "comment_id": "boo",
          },
        },
      ],
    }
  `);
});
