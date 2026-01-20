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
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../zqlite/src/database-storage.ts';
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
let writeAuthzStorage: DatabaseStorage;

beforeEach(() => {
  replica = new Database(lc, ':memory:');
  replica.exec(/*sql*/ `
    CREATE TABLE foo (id TEXT PRIMARY KEY, a TEXT, b TEXT_NOT_SUPPORTED);
    INSERT INTO foo (id, a) VALUES ('1', 'a');
    CREATE TABLE "the_app.permissions" (permissions JSON, hash TEXT);
    INSERT INTO "the_app.permissions" (permissions) VALUES (NULL);
    `);
  const storageDb = new Database(lc, ':memory:');
  storageDb.prepare(CREATE_STORAGE_TABLE).run();
  writeAuthzStorage = new DatabaseStorage(storageDb);
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
      writeAuthzStorage,
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
      writeAuthzStorage,
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
  test('deny', async () => {
    setPermissions({
      tables: {},
    });

    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
      'cg',
      writeAuthzStorage,
    );

    expect(
      await authorizer.canPostMutation({sub: '2'}, [
        {op: 'insert', primaryKey: ['id'], tableName: 'foo', value: {id: '2'}},
      ]),
    ).toBe(false);

    expect(
      await authorizer.canPreMutation({sub: '1'}, [
        {op: 'update', primaryKey: ['id'], tableName: 'foo', value: {id: '1'}},
      ]),
    ).toBe(false);
    expect(
      await authorizer.canPostMutation({sub: '1'}, [
        {op: 'update', primaryKey: ['id'], tableName: 'foo', value: {id: '1'}},
      ]),
    ).toBe(false);

    expect(
      await authorizer.canPreMutation({sub: '1'}, [
        {op: 'delete', primaryKey: ['id'], tableName: 'foo', value: {id: '1'}},
      ]),
    ).toBe(false);
  });

  test('insert is run post-mutation', async () => {
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
      writeAuthzStorage,
    );

    const op: InsertOp = {
      op: 'insert',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '2', a: 'b'},
    };

    // insert does not run pre-mutation checks so it'll return true.
    expect(await authorizer.canPreMutation({sub: '1'}, [op])).toBe(true);
    // insert checks are run post mutation.
    expect(await authorizer.canPostMutation({sub: '1'}, [op])).toBe(false);

    // passes the rule since the subject is correct.
    expect(await authorizer.canPostMutation({sub: '2'}, [op])).toBe(true);
  });

  test('update is run pre-mutation when specified', async () => {
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
      writeAuthzStorage,
    );

    const op: UpdateOp = {
      op: 'update',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '1', a: 'b'},
    };

    // subject is not correct and there is a pre-mutation rule
    expect(await authorizer.canPreMutation({sub: '2'}, [op])).toBe(false);
    // no post-mutation rule, default to false
    expect(await authorizer.canPostMutation({sub: '2'}, [op])).toBe(false);

    expect(await authorizer.canPreMutation({sub: '1'}, [op])).toBe(true);
  });

  test('update is run post-mutation when specified', async () => {
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
      writeAuthzStorage,
    );

    const op: UpdateOp = {
      op: 'update',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '1', a: 'b'},
    };

    // no pre-mutation rule so disallowed.
    expect(await authorizer.canPreMutation({sub: '2'}, [op])).toBe(false);
    // subject doesn't match
    expect(await authorizer.canPostMutation({sub: '2'}, [op])).toBe(false);
    // subject does match the updated value of `a`
    expect(await authorizer.canPostMutation({sub: 'b'}, [op])).toBe(true);
  });
});

describe('pre & post mutation', () => {
  test('delete is run pre-mutation', async () => {
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
      writeAuthzStorage,
    );

    const op: DeleteOp = {
      op: 'delete',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '1'},
    };

    expect(await authorizer.canPreMutation({sub: '2'}, [op])).toBe(false);
    // there is nothing to check post-mutation for delete so it will always pass post-mutation checks.
    // post mutation checks are anded with pre-mutation checks so this is correct.
    expect(await authorizer.canPostMutation({sub: '2'}, [op])).toBe(true);

    // this passes the rule since the subject is correct
    expect(await authorizer.canPreMutation({sub: '1'}, [op])).toBe(true);
  });

  test('insert is run post-mutation', async () => {
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
      writeAuthzStorage,
    );

    const op: InsertOp = {
      op: 'insert',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '2', a: 'b'},
    };

    // insert does not run pre-mutation checks so it'll return true.
    expect(await authorizer.canPreMutation({sub: '1'}, [op])).toBe(true);
    // insert checks are run post mutation.
    expect(await authorizer.canPostMutation({sub: '1'}, [op])).toBe(false);

    // passes the rule since the subject is correct.
    expect(await authorizer.canPostMutation({sub: '2'}, [op])).toBe(true);
  });

  test('update is run pre-mutation when specified', async () => {
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
      writeAuthzStorage,
    );

    const op: UpdateOp = {
      op: 'update',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '1', a: 'b'},
    };

    // subject is not correct and there is a pre-mutation rule
    expect(await authorizer.canPreMutation({sub: '2'}, [op])).toBe(false);
    // no post-mutation rule, default to false
    expect(await authorizer.canPostMutation({sub: '2'}, [op])).toBe(false);

    expect(await authorizer.canPreMutation({sub: '1'}, [op])).toBe(true);
  });

  test('update is run post-mutation when specified', async () => {
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
      writeAuthzStorage,
    );

    const op: UpdateOp = {
      op: 'update',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '1', a: 'b'},
    };

    // no pre-mutation rule so disallowed.
    expect(await authorizer.canPreMutation({sub: '2'}, [op])).toBe(false);
    // subject doesn't match
    expect(await authorizer.canPostMutation({sub: '2'}, [op])).toBe(false);
    // subject does match the updated value of `a`
    expect(await authorizer.canPostMutation({sub: 'b'}, [op])).toBe(true);
  });

  test('destroy', () => {
    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
      'cg',
      writeAuthzStorage,
    );

    expect(() => authorizer.destroy()).not.toThrow();
  });
});

describe('primary key validation', () => {
  test('rejects update when primary key column missing from value', async () => {
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
      writeAuthzStorage,
    );

    const op: UpdateOp = {
      op: 'update',
      primaryKey: ['a'],
      tableName: 'foo',
      value: {a: 'value'},
    };

    await expect(authorizer.canPreMutation({sub: '1'}, [op])).rejects.toThrow(
      "Primary key column 'id' is missing from operation value for table foo",
    );
  });

  test('rejects delete when primary key column missing from value', async () => {
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
      writeAuthzStorage,
    );

    const op: DeleteOp = {
      op: 'delete',
      primaryKey: ['a'],
      tableName: 'foo',
      value: {a: 'value'},
    };

    await expect(authorizer.canPreMutation({sub: '1'}, [op])).rejects.toThrow(
      "Primary key column 'id' is missing from operation value for table foo",
    );
  });

  test('rejects insert when primary key column missing from value', async () => {
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
      writeAuthzStorage,
    );

    const op: InsertOp = {
      op: 'insert',
      primaryKey: ['a'],
      tableName: 'foo',
      value: {a: 'value'},
    };

    await expect(authorizer.canPostMutation({sub: '2'}, [op])).rejects.toThrow(
      "Primary key column 'id' is missing from operation value for table foo",
    );
  });

  test('rejects upsert when primary key column missing from value', () => {
    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
      'cg',
      writeAuthzStorage,
    );

    expect(() =>
      authorizer.normalizeOps([
        {
          op: 'upsert',
          primaryKey: ['a'],
          tableName: 'foo',
          value: {a: 'value'},
        },
      ]),
    ).toThrow(
      "Primary key column 'id' is missing from operation value for table foo",
    );
  });

  test('accepts update with required primary key column in value', async () => {
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
      writeAuthzStorage,
    );

    const op: UpdateOp = {
      op: 'update',
      primaryKey: ['id'],
      tableName: 'foo',
      value: {id: '1', a: 'b'},
    };

    expect(await authorizer.canPreMutation({sub: '1'}, [op])).toBe(true);
  });

  test('ignores client-provided primaryKey array and uses server schema', async () => {
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
      writeAuthzStorage,
    );

    const op: UpdateOp = {
      op: 'update',
      primaryKey: ['a'],
      tableName: 'foo',
      value: {id: '1', a: 'b'},
    };

    expect(await authorizer.canPreMutation({sub: '1'}, [op])).toBe(true);
  });
});

describe('table name validation', () => {
  test('rejects operations with invalid table name', () => {
    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
      'cg',
      writeAuthzStorage,
    );

    expect(() =>
      authorizer.validateTableNames([
        {
          op: 'insert',
          primaryKey: ['id'],
          tableName: 'nonexistent_table',
          value: {id: '1'},
        },
      ]),
    ).toThrow("Table 'nonexistent_table' is not a valid table.");
  });

  test('accepts operations with valid table name', () => {
    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
      'cg',
      writeAuthzStorage,
    );

    expect(() =>
      authorizer.validateTableNames([
        {
          op: 'insert',
          primaryKey: ['id'],
          tableName: 'foo',
          value: {id: '1'},
        },
      ]),
    ).not.toThrow();
  });

  test('validates all operations in a batch', () => {
    const authorizer = new WriteAuthorizerImpl(
      lc,
      zeroConfig,
      replica,
      'the_app',
      'cg',
      writeAuthzStorage,
    );

    expect(() =>
      authorizer.validateTableNames([
        {
          op: 'insert',
          primaryKey: ['id'],
          tableName: 'foo',
          value: {id: '1'},
        },
        {
          op: 'update',
          primaryKey: ['id'],
          tableName: 'invalid_table',
          value: {id: '1'},
        },
      ]),
    ).toThrow("Table 'invalid_table' is not a valid table.");
  });
});
