import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.js';
import {Database} from '../../../zqlite/src/db.js';
import {WriteAuthorizerImpl} from './write-authorizer.js';
import type {Rule} from '../../../zero-schema/src/compiled-permissions.js';
import type {
  DeleteOp,
  InsertOp,
  UpdateOp,
} from '../../../zero-protocol/src/push.js';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.js';
import {string, table} from '../../../zero-schema/src/builder/table-builder.js';

const lc = createSilentLogContext();

const allowIfSubject = [
  'allow',
  {
    type: 'simple',
    left: {
      type: 'column',
      name: 'id',
    },
    op: '=',
    right: {anchor: 'authData', field: 'sub', type: 'static'},
  },
] satisfies Rule;

const allowIfAIsSubject = [
  'allow',
  {
    type: 'simple',
    left: {
      type: 'column',
      name: 'a',
    },
    op: '=',
    right: {anchor: 'authData', field: 'sub', type: 'static'},
  },
] satisfies Rule;

const schema = createSchema(1, {
  tables: [
    table('foo')
      .columns({
        id: string(),
        a: string(),
      })
      .primaryKey('id'),
  ],
});

let replica: Database;
beforeEach(() => {
  replica = new Database(lc, ':memory:');
  replica.exec(/*sql*/ `CREATE TABLE foo (id TEXT PRIMARY KEY, a TEXT);
      INSERT INTO foo VALUES ('1', 'a');`);
});

describe('normalize ops', () => {
  // upserts are converted to inserts/updates correctly
  // upsert where row exists
  // upsert where row does not exist
  test('upsert converted to update if row exists', () => {
    const authorizer = new WriteAuthorizerImpl(
      lc,
      {},
      schema,
      {},
      replica,
      'cg',
    );
    const normalized = authorizer.normalizeOps([
      {
        op: 'upsert',
        primaryKey: ['id'],
        tableName: 'foo',
        value: {id: '1', a: 'b'},
      },
    ]);
    expect(normalized).toEqual([
      {
        op: 'update',
        primaryKey: ['id'],
        tableName: 'foo',
        value: {id: '1', a: 'b'},
      },
    ]);
  });
  test('upsert converted to insert if row does not exist', () => {
    const authorizer = new WriteAuthorizerImpl(
      lc,
      {},
      schema,
      {},
      replica,
      'cg',
    );
    const normalized = authorizer.normalizeOps([
      {
        op: 'upsert',
        primaryKey: ['id'],
        tableName: 'foo',
        value: {id: '2', a: 'b'},
      },
    ]);
    expect(normalized).toEqual([
      {
        op: 'insert',
        primaryKey: ['id'],
        tableName: 'foo',
        value: {id: '2', a: 'b'},
      },
    ]);
  });
});

describe('pre & post mutation', () => {
  test('delete is run pre-mutation', () => {
    const authorizer = new WriteAuthorizerImpl(
      lc,
      {},
      schema,
      {
        foo: {
          row: {
            delete: [allowIfSubject],
          },
        },
      },
      replica,
      'cg',
    );

    const op: DeleteOp = {
      op: 'delete',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '1'},
    };

    expect(authorizer.canPreMutation({sub: '2'}, [op])).toBe(false);
    // there is nothing to check post-mutation for delete so it will always pass post-mutation checks.
    // post mutation checks are anded with pre-mutation checks so this is correct.
    expect(authorizer.canPostMutation({sub: '2'}, [op])).toBe(true);

    // this passes the rule since the subject is correct
    expect(authorizer.canPreMutation({sub: '1'}, [op])).toBe(true);
  });

  test('insert is run post-mutation', () => {
    const authorizer = new WriteAuthorizerImpl(
      lc,
      {},
      schema,
      {
        foo: {
          row: {
            insert: [allowIfSubject],
          },
        },
      },
      replica,
      'cg',
    );

    const op: InsertOp = {
      op: 'insert',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '2', a: 'b'},
    };

    // insert does not run pre-mutation checks so it'll return true.
    expect(authorizer.canPreMutation({sub: '1'}, [op])).toBe(true);
    // insert checks are run post mutation.
    expect(authorizer.canPostMutation({sub: '1'}, [op])).toBe(false);

    // passes the rule since the subject is correct.
    expect(authorizer.canPostMutation({sub: '2'}, [op])).toBe(true);
  });

  test('update is run pre-mutation when specified', () => {
    const authorizer = new WriteAuthorizerImpl(
      lc,
      {},
      schema,
      {
        foo: {
          row: {
            update: {
              preMutation: [allowIfSubject],
            },
          },
        },
      },
      replica,
      'cg',
    );

    const op: UpdateOp = {
      op: 'update',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '1', a: 'b'},
    };

    // subject is not correct and there is a pre-mutation rule
    expect(authorizer.canPreMutation({sub: '2'}, [op])).toBe(false);
    // incorrect subject but no post-mutation rule so it is allowed
    expect(authorizer.canPostMutation({sub: '2'}, [op])).toBe(true);

    expect(authorizer.canPreMutation({sub: '1'}, [op])).toBe(true);
    expect(authorizer.canPostMutation({sub: '1'}, [op])).toBe(true);
  });

  test('update is run post-mutation when specified', () => {
    const authorizer = new WriteAuthorizerImpl(
      lc,
      {},
      schema,
      {
        foo: {
          row: {
            update: {
              postMutation: [allowIfAIsSubject],
            },
          },
        },
      },
      replica,
      'cg',
    );

    const op: UpdateOp = {
      op: 'update',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '1', a: 'b'},
    };

    // no pre-mutation rule so allowed.
    expect(authorizer.canPreMutation({sub: '2'}, [op])).toBe(true);
    // subject doesn't match
    expect(authorizer.canPostMutation({sub: '2'}, [op])).toBe(false);
    // subject does match the updated value of `a`
    expect(authorizer.canPostMutation({sub: 'b'}, [op])).toBe(true);
  });
});
