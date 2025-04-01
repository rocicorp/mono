import {beforeEach, describe, expect, test} from 'vitest';
import {h128} from '../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {
  DeleteOp,
  InsertOp,
  UpdateOp,
} from '../../../zero-protocol/src/push.ts';
import type {
  PermissionsConfig,
  Rule,
} from '../../../zero-schema/src/compiled-permissions.ts';
import {Database} from '../../../zqlite/src/db.ts';
import type {ZeroConfig} from '../config/zero-config.ts';
import {WriteAuthorizerImpl} from './write-authorizer.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';

const lc = createSilentLogContext();
const zeroConfig = {
  log: testLogConfig,
} as unknown as ZeroConfig;

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

let replica: Database;

beforeEach(() => {
  replica = new Database(lc, ':memory:');
  replica.exec(/*sql*/ `
    CREATE TABLE foo (id TEXT PRIMARY KEY, a TEXT, b TEXT_NOT_SUPPORTED);
    INSERT INTO foo (id, a) VALUES ('1', 'a');
    CREATE TABLE "the_app.permissions" (permissions JSON, hash TEXT);
    INSERT INTO "the_app.permissions" (permissions) VALUES (NULL);
    `);
});

function setPermissions(permissions: PermissionsConfig) {
  const json = JSON.stringify(permissions);
  replica
    .prepare(
      /* sql */ `
    UPDATE "the_app.permissions" SET permissions = ?, hash = ?`,
    )
    .run(json, h128(json).toString(16));
}

describe('normalize ops', () => {
  // upserts are converted to inserts/updates correctly
  // upsert where row exists
  // upsert where row does not exist
  test('upsert converted to update if row exists', () => {
    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
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
      zeroConfig,
      replica,
      'the_app',
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

describe('default deny', () => {
  test('deny', () => {
    setPermissions({
      tables: {},
    });

    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
      'cg',
    );

    expect(
      authorizer.canPostMutation({sub: '2'}, [
        {op: 'insert', primaryKey: ['id'], tableName: 'foo', value: {id: '2'}},
      ]),
    ).toBe(false);

    expect(
      authorizer.canPreMutation({sub: '1'}, [
        {op: 'update', primaryKey: ['id'], tableName: 'foo', value: {id: '1'}},
      ]),
    ).toBe(false);
    expect(
      authorizer.canPostMutation({sub: '1'}, [
        {op: 'update', primaryKey: ['id'], tableName: 'foo', value: {id: '1'}},
      ]),
    ).toBe(false);

    expect(
      authorizer.canPreMutation({sub: '1'}, [
        {op: 'delete', primaryKey: ['id'], tableName: 'foo', value: {id: '1'}},
      ]),
    ).toBe(false);
  });

  test('insert is run post-mutation', () => {
    setPermissions({
      tables: {
        foo: {
          row: {
            insert: [allowIfSubject],
          },
        },
      },
    });

    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
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
    setPermissions({
      tables: {
        foo: {
          row: {
            update: {
              preMutation: [allowIfSubject],
            },
          },
        },
      },
    });

    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
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
    // no post-mutation rule, default to false
    expect(authorizer.canPostMutation({sub: '2'}, [op])).toBe(false);

    expect(authorizer.canPreMutation({sub: '1'}, [op])).toBe(true);
  });

  test('update is run post-mutation when specified', () => {
    setPermissions({
      tables: {
        foo: {
          row: {
            update: {
              postMutation: [allowIfAIsSubject],
            },
          },
        },
      },
    });

    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
      'cg',
    );

    const op: UpdateOp = {
      op: 'update',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '1', a: 'b'},
    };

    // no pre-mutation rule so disallowed.
    expect(authorizer.canPreMutation({sub: '2'}, [op])).toBe(false);
    // subject doesn't match
    expect(authorizer.canPostMutation({sub: '2'}, [op])).toBe(false);
    // subject does match the updated value of `a`
    expect(authorizer.canPostMutation({sub: 'b'}, [op])).toBe(true);
  });
});

describe('pre & post mutation', () => {
  test('delete is run pre-mutation', () => {
    setPermissions({
      tables: {
        foo: {
          row: {
            delete: [allowIfSubject],
          },
        },
      },
    });

    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
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
    setPermissions({
      tables: {
        foo: {
          row: {
            insert: [allowIfSubject],
          },
        },
      },
    });

    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
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
    setPermissions({
      tables: {
        foo: {
          row: {
            update: {
              preMutation: [allowIfSubject],
            },
          },
        },
      },
    });

    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
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
    // no post-mutation rule, default to false
    expect(authorizer.canPostMutation({sub: '2'}, [op])).toBe(false);

    expect(authorizer.canPreMutation({sub: '1'}, [op])).toBe(true);
  });

  test('update is run post-mutation when specified', () => {
    setPermissions({
      tables: {
        foo: {
          row: {
            update: {
              postMutation: [allowIfAIsSubject],
            },
          },
        },
      },
    });

    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
      'cg',
    );

    const op: UpdateOp = {
      op: 'update',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '1', a: 'b'},
    };

    // no pre-mutation rule so disallowed.
    expect(authorizer.canPreMutation({sub: '2'}, [op])).toBe(false);
    // subject doesn't match
    expect(authorizer.canPostMutation({sub: '2'}, [op])).toBe(false);
    // subject does match the updated value of `a`
    expect(authorizer.canPostMutation({sub: 'b'}, [op])).toBe(true);
  });
});
