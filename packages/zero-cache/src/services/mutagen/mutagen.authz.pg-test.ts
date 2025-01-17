import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import * as MutationType from '../../../../zero-protocol/src/mutation-type-enum.js';
import {
  ANYONE_CAN,
  definePermissions,
  NOBODY_CAN,
} from '../../../../zero-schema/src/permissions.js';
import {ExpressionBuilder} from '../../../../zql/src/query/expression.js';
import type {Row} from '../../../../zql/src/query/query.js';
import {Database} from '../../../../zqlite/src/db.js';
import {WriteAuthorizerImpl} from '../../auth/write-authorizer.js';
import {testDBs} from '../../test/db.js';
import type {PostgresDB} from '../../types/pg.js';
import {zeroSchema} from './mutagen-test-shared.js';
import {processMutation} from './mutagen.js';
import {
  createSchema,
  type Schema as ZeroSchema,
} from '../../../../zero-schema/src/builder/schema-builder.js';
import {
  boolean,
  json,
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.js';

const SHARD_ID = '0';
const CG_ID = 'abc';
const TEST_SCHEMA_VERSION = 1;

const sqlSchema = /* sql */ `
CREATE TABLE "user" (
  id text PRIMARY KEY,
  role text
);

INSERT INTO "user" VALUES ('admn', 'admin');
INSERT INTO "user" VALUES ('usr', 'user');

CREATE TABLE "roColumn" (
  id text PRIMARY KEY,
  "a" text,
  "b" text
);

-- a row against which we can test delete and update
INSERT INTO "roColumn" VALUES ('1', 'a', 'b');

CREATE TABLE "roCell" (
  id text PRIMARY KEY,
  "a" text,
  b text
);

-- a row against which we can test delete and update
INSERT INTO "roCell" VALUES ('1', 'a', 'b');

CREATE TABLE "roRow" (
  id text PRIMARY KEY,
  a text,
  b text
);

-- a row against which we can test delete and update
INSERT INTO "roRow" VALUES ('1', 'a', 'b');

CREATE TABLE "adminOnlyCell" (
  id text PRIMARY KEY,
  a text,
  "adminLocked" boolean
);

INSERT INTO "adminOnlyCell" VALUES ('unlocked', 'a', false);
INSERT INTO "adminOnlyCell" VALUES ('locked', 'a', true);

CREATE TABLE "adminOnlyRow" (
  id text PRIMARY KEY,
  a text,
  "adminLocked" boolean,
  "json" json
);

INSERT INTO "adminOnlyRow" VALUES ('unlocked', 'a', false, '{}');
INSERT INTO "adminOnlyRow" VALUES ('locked', 'a', true, '[]');

CREATE TABLE "loggedInRow" (
  id text PRIMARY KEY,
  a text
);

INSERT INTO "loggedInRow" VALUES ('1', 'a');

CREATE TABLE "userMatch" (
  id text PRIMARY KEY,
  a text
);

CREATE TABLE "dataTypeTest" (
  "id" text PRIMARY KEY,
  "j" json,
  "b" boolean,
  "r" real,
  "i" bigint
);

INSERT INTO "dataTypeTest" (
  "id", "j", "b", "r", "i"
) VALUES (
  '100', '{}', true, 1.1, 100
);
`;

async function createUpstreamTables(db: PostgresDB) {
  await db.unsafe(sqlSchema + zeroSchema(SHARD_ID));
}

function createReplicaTables(db: Database) {
  db.exec(sqlSchema);
}

const schema = createSchema(
  TEST_SCHEMA_VERSION,
  {
    user: table('user')
      .columns({
        id: string(),
        role: string(),
      })
      .primaryKey('id'),

    roCell: table('roCell')
      .columns({
        id: string(),
        a: string(),
        b: string(),
      })
      .primaryKey('id'),

    roRow: table('roRow')
      .columns({
        id: string(),
        a: string(),
        b: string(),
      })
      .primaryKey('id'),

    adminOnlyCell: table('adminOnlyCell')
      .columns({
        id: string(),
        a: string(),
        adminLocked: boolean(),
      })
      .primaryKey('id'),

    adminOnlyRow: table('adminOnlyRow')
      .columns({
        id: string(),
        a: string(),
        adminLocked: boolean(),
      })
      .primaryKey('id'),

    loggedInRow: table('loggedInRow')
      .columns({
        id: string(),
        a: string(),
      })
      .primaryKey('id'),

    userMatch: table('userMatch')
      .columns({
        id: string(),
        a: string(),
      })
      .primaryKey('id'),

    dataTypeTest: table('dataTypeTest')
      .columns({
        id: string(),
        j: json().optional(),
        b: boolean().optional(),
        r: number().optional(),
        i: number().optional(),
      })
      .primaryKey('id'),
  },
  {
    // No relationships were defined in the original schema
  },
);

type Schema = typeof schema;

type AuthData = {
  sub: string;
  role: string;
};

const permissionsConfig = await definePermissions<AuthData, typeof schema>(
  schema,
  () => {
    const allowIfAdmin = (
      authData: AuthData,
      {cmpLit}: ExpressionBuilder<ZeroSchema, string>,
    ) => cmpLit(authData.role, '=', 'admin');

    const allowIfNotAdminLockedRow = (
      _authData: AuthData,
      {cmp}: ExpressionBuilder<Schema, 'adminOnlyRow'>,
    ) => cmp('adminLocked', false);
    const allowIfNotAdminLockedCell = (
      _authData: AuthData,
      {cmp}: ExpressionBuilder<Schema, 'adminOnlyCell'>,
    ) => cmp('adminLocked', false);
    const allowIfLoggedIn = (
      authData: AuthData,
      {cmpLit}: ExpressionBuilder<ZeroSchema, string>,
    ) => cmpLit(authData.sub, 'IS NOT', null);
    const allowIfPostMutationIDMatchesLoggedInUser = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<Schema, 'userMatch'>,
    ) => cmp('id', '=', authData.sub);

    return {
      roCell: {
        cell: {
          a: {
            insert: NOBODY_CAN,
            update: {
              preMutation: NOBODY_CAN,
            },
            delete: NOBODY_CAN,
          },
        },
      },
      roRow: {
        row: {
          insert: NOBODY_CAN,
          update: {
            preMutation: NOBODY_CAN,
          },
          delete: NOBODY_CAN,
        },
      },
      adminOnlyCell: {
        cell: {
          a: {
            // insert is always allow since it can't be admin locked on create.
            // TODO (mlaw): this should raise a type error due to schema mismatch between rule and auth def
            update: {
              preMutation: [allowIfNotAdminLockedCell, allowIfAdmin],
            },
            delete: [allowIfNotAdminLockedCell, allowIfAdmin],
          },
        },
      },
      adminOnlyRow: {
        row: {
          // insert is always allow since it can't be admin locked on create.
          update: {preMutation: [allowIfNotAdminLockedRow, allowIfAdmin]},
          delete: [allowIfNotAdminLockedRow, allowIfAdmin],
        },
      },
      loggedInRow: {
        row: {
          insert: [allowIfLoggedIn],
          update: {preMutation: [allowIfLoggedIn]},
          delete: [allowIfLoggedIn],
        },
      },
      userMatch: {
        row: {
          insert: [allowIfPostMutationIDMatchesLoggedInUser],
        },
      },
      dataTypeTest: {
        row: {
          insert: ANYONE_CAN,
          update: ANYONE_CAN,
          delete: ANYONE_CAN,
        },
      },
    };
  },
);

let upstream: PostgresDB;
let replica: Database;
let authorizer: WriteAuthorizerImpl;
let lmid = 0;
const lc = createSilentLogContext();
beforeEach(async () => {
  upstream = await testDBs.create('authz');
  await createUpstreamTables(upstream);
  replica = new Database(lc, ':memory:');
  createReplicaTables(replica);
  authorizer = new WriteAuthorizerImpl(
    lc,
    {},
    schema,
    permissionsConfig,
    replica,
    SHARD_ID,
  );
  lmid = 0;
});

afterEach(async () => {
  await testDBs.drop(upstream);
});

function procMutation(
  tableName: string,
  op: 'insert' | 'upsert' | 'update' | 'delete',
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  value: any,
  uid: string | undefined = undefined,
) {
  return processMutation(
    lc,
    uid === undefined
      ? undefined
      : {sub: uid, role: uid === 'admn' ? 'admin' : 'user'},
    upstream,
    SHARD_ID,
    CG_ID,
    {
      type: MutationType.CRUD,
      id: ++lmid,
      clientID: '123',
      name: '_zero_crud',
      args: [
        {
          ops: [
            {
              op,
              tableName,
              primaryKey: ['id'],
              value,
            },
          ],
        },
      ],
      timestamp: Date.now(),
    },
    authorizer,
    TEST_SCHEMA_VERSION,
  );
}

test('it is possible to write to a row with a read only column if that column is not written to', async () => {
  await procMutation('roColumn', 'update', {id: '1', b: 'UPDATED'});
  const rows = await upstream`SELECT * FROM "roColumn" WHERE id = '1'`;
  expect(rows).toEqual([{id: '1', b: 'UPDATED', a: 'a'}]);
});

test('it is impossible to write to a read-only cell', async () => {
  await procMutation('roCell', 'insert', {id: '2', a: 'a', b: 'b'});
  let rows = await upstream`SELECT * FROM "roCell" WHERE id = '2'`;
  expect(rows.length).toBe(0);

  await procMutation('roCell', 'update', {
    id: '1',
    a: 'UPDATED',
  });
  rows = await upstream`SELECT * FROM "roCell" WHERE id = '1'`;
  expect(rows).toEqual([{id: '1', a: 'a', b: 'b'}]);

  await procMutation('roCell', 'delete', {id: '1'});
  rows = await upstream`SELECT * FROM "roCell" WHERE id = '1'`;
  expect(rows.length).toBe(1);
});

test('a row with a read-only cell can be updated if the cell is not written to', async () => {
  await procMutation('roCell', 'update', {id: '1', b: 'UPDATED'});
  const rows = await upstream`SELECT * FROM "roCell" WHERE id = '1'`;
  expect(rows).toEqual([{id: '1', b: 'UPDATED', a: 'a'}]);
});

test('is is impossible to update a read-only row', async () => {
  await procMutation('roRow', 'insert', {id: '2', a: 'a', b: 'b'});
  let rows = await upstream`SELECT * FROM "roRow" WHERE id = '2'`;
  expect(rows.length).toBe(0);

  await procMutation('roRow', 'update', {
    id: '1',
    a: 'UPDATED',
  });
  rows = await upstream`SELECT * FROM "roRow" WHERE id = '1'`;
  expect(rows).toEqual([{id: '1', a: 'a', b: 'b'}]);

  await procMutation('roRow', 'delete', {id: '1'});
  rows = await upstream`SELECT * FROM "roRow" WHERE id = '1'`;
  expect(rows.length).toBe(1);
});

test('non-admins cannot update admin-only cells', async () => {
  await procMutation(
    'adminOnlyCell',
    'update',
    {id: 'locked', a: 'UPDATED'},
    'usr',
  );
  let rows = await upstream`SELECT * FROM "adminOnlyCell" WHERE id = 'locked'`;
  expect(rows).toEqual([{id: 'locked', a: 'a', adminLocked: true}]);

  await procMutation('adminOnlyCell', 'delete', {id: 'locked'}, 'usr');
  rows = await upstream`SELECT * FROM "adminOnlyCell" WHERE id = 'locked'`;
  expect(rows.length).toBe(1);
});

test('non-admins can update unlocked cells', async () => {
  await procMutation(
    'adminOnlyCell',
    'update',
    {id: 'unlocked', a: 'UPDATED'},
    'usr',
  );
  let rows =
    await upstream`SELECT * FROM "adminOnlyCell" WHERE id = 'unlocked'`;
  expect(rows).toEqual([{id: 'unlocked', a: 'UPDATED', adminLocked: false}]);

  await procMutation('adminOnlyCell', 'delete', {id: 'unlocked'}, 'usr');
  rows = await upstream`SELECT * FROM "adminOnlyCell" WHERE id = 'unlocked'`;
  expect(rows.length).toBe(0);
});

test('admins can update locked cells', async () => {
  await procMutation(
    'adminOnlyCell',
    'update',
    {id: 'locked', a: 'UPDATED'},
    'admn',
  );
  let rows = await upstream`SELECT * FROM "adminOnlyCell" WHERE id = 'locked'`;
  expect(rows).toEqual([{id: 'locked', a: 'UPDATED', adminLocked: true}]);

  await procMutation('adminOnlyCell', 'delete', {id: 'locked'}, 'admn');
  rows = await upstream`SELECT * FROM "adminOnlyCell" WHERE id = 'locked'`;
  expect(rows.length).toBe(0);
});

test('non-admins cannot update admin-only rows', async () => {
  await procMutation(
    'adminOnlyRow',
    'update',
    {id: 'locked', a: 'UPDATED', json: 'some string'},
    'usr',
  );
  let rows = await upstream`SELECT * FROM "adminOnlyRow" WHERE id = 'locked'`;
  expect(rows).toEqual([{id: 'locked', a: 'a', adminLocked: true, json: []}]);

  await procMutation('adminOnlyRow', 'delete', {id: 'locked'}, 'usr');
  rows = await upstream`SELECT * FROM "adminOnlyRow" WHERE id = 'locked'`;
  expect(rows.length).toBe(1);
});

test('non-admins can update unlocked rows', async () => {
  await procMutation(
    'adminOnlyRow',
    'update',
    {id: 'unlocked', a: 'UPDATED'},
    'usr',
  );
  let rows = await upstream`SELECT * FROM "adminOnlyRow" WHERE id = 'unlocked'`;
  expect(rows).toEqual([
    {id: 'unlocked', a: 'UPDATED', adminLocked: false, json: {}},
  ]);

  await procMutation(
    'adminOnlyRow',
    'update',
    {id: 'unlocked', a: 'UPDATED2', json: {a: true}},
    'usr',
  );
  rows = await upstream`SELECT * FROM "adminOnlyRow" WHERE id = 'unlocked'`;
  expect(rows).toEqual([
    {id: 'unlocked', a: 'UPDATED2', adminLocked: false, json: {a: true}},
  ]);

  await procMutation('adminOnlyRow', 'delete', {id: 'unlocked'}, 'usr');
  rows = await upstream`SELECT * FROM "adminOnlyRow" WHERE id = 'unlocked'`;
  expect(rows.length).toBe(0);
});

test('admins can update locked rows', async () => {
  await procMutation(
    'adminOnlyRow',
    'update',
    {id: 'locked', a: 'UPDATED'},
    'admn',
  );
  let rows = await upstream`SELECT * FROM "adminOnlyRow" WHERE id = 'locked'`;
  expect(rows).toEqual([
    {id: 'locked', a: 'UPDATED', adminLocked: true, json: []},
  ]);

  await procMutation('adminOnlyRow', 'delete', {id: 'locked'}, 'admn');
  rows = await upstream`SELECT * FROM "adminOnlyRow" WHERE id = 'locked'`;
  expect(rows.length).toBe(0);
});

test('denies if not logged in', async () => {
  await procMutation(
    'loggedInRow',
    'update',
    {id: '1', a: 'UPDATED'},
    undefined,
  );
  let rows = await upstream`SELECT * FROM "loggedInRow" WHERE id = '1'`;
  expect(rows).toEqual([{id: '1', a: 'a'}]);

  await procMutation('loggedInRow', 'delete', {id: '1'}, undefined);
  rows = await upstream`SELECT * FROM "loggedInRow" WHERE id = '1'`;
  expect(rows.length).toBe(1);

  await procMutation('loggedInRow', 'insert', {id: '2', a: 'a'}, undefined);
  rows = await upstream`SELECT * FROM "loggedInRow" WHERE id = '2'`;
  expect(rows.length).toBe(0);
});

test('allows if logged in', async () => {
  await procMutation('loggedInRow', 'update', {id: '1', a: 'UPDATED'}, 'usr');
  let rows = await upstream`SELECT * FROM "loggedInRow" WHERE id = '1'`;
  expect(rows).toEqual([{id: '1', a: 'UPDATED'}]);

  await procMutation('loggedInRow', 'delete', {id: '1'}, 'usr');
  rows = await upstream`SELECT * FROM "loggedInRow" WHERE id = '1'`;
  expect(rows.length).toBe(0);

  await procMutation('loggedInRow', 'insert', {id: '2', a: 'a'}, 'usr');
  rows = await upstream`SELECT * FROM "loggedInRow" WHERE id = '2'`;
  expect(rows.length).toBe(1);
});

// TODO (mlaw): expand "post mutation" rules to be enabled for update as well.
test('userMatch postMutation check', async () => {
  await procMutation('userMatch', 'insert', {id: '1', a: 'a'}, 'usr');
  let rows = await upstream`SELECT * FROM "userMatch" WHERE id = '1'`;
  expect(rows.length).toBe(0);

  await procMutation('userMatch', 'insert', {id: '1', a: 'a'}, 'admn');
  rows = await upstream`SELECT * FROM "userMatch" WHERE id = '1'`;
  expect(rows.length).toBe(0);

  await procMutation('userMatch', 'insert', {id: '1', a: 'a'}, '1');
  rows = await upstream`SELECT * FROM "userMatch" WHERE id = '1'`;
  expect(rows.length).toBe(1);
});

describe('data type test', () => {
  function runMutation(
    op: 'insert' | 'upsert' | 'update' | 'delete',
    value: Partial<Row<typeof schema.tables.dataTypeTest>>,
  ) {
    return procMutation('dataTypeTest', op, value, 'usr');
  }

  function select(id?: string) {
    if (id === undefined) {
      return upstream`SELECT * FROM "dataTypeTest"`;
    }
    return upstream`SELECT * FROM "dataTypeTest" WHERE id = ${id}`;
  }

  test('partial inserts', async () => {
    // only pk
    await runMutation('insert', {id: '1'});
    expect(await select('1')).toMatchInlineSnapshot(`
      Result [
        {
          "b": null,
          "i": null,
          "id": "1",
          "j": null,
          "r": null,
        },
      ]
    `);

    // pk and json
    await runMutation('insert', {id: '2', j: {a: 1}});
    expect(await select('2')).toMatchInlineSnapshot(`
      Result [
        {
          "b": null,
          "i": null,
          "id": "2",
          "j": {
            "a": 1,
          },
          "r": null,
        },
      ]
    `);
  });

  test('complete inserts', async () => {
    await runMutation('insert', {
      id: '3',
      j: {a: 1},
      b: true,
      r: 1.1,
      i: 1,
    });
    expect(await select('3')).toMatchInlineSnapshot(`
      Result [
        {
          "b": true,
          "i": 1n,
          "id": "3",
          "j": {
            "a": 1,
          },
          "r": 1.1,
        },
      ]
    `);
  });

  test('json insert edge cases', async () => {
    await runMutation('insert', {id: '1', j: null});
    expect(await select('1')).toMatchInlineSnapshot(`
      Result [
        {
          "b": null,
          "i": null,
          "id": "1",
          "j": null,
          "r": null,
        },
      ]
    `);

    await runMutation('insert', {id: '2', j: {}});
    expect(await select('2')).toMatchInlineSnapshot(`
      Result [
        {
          "b": null,
          "i": null,
          "id": "2",
          "j": {},
          "r": null,
        },
      ]
    `);

    await runMutation('insert', {id: '3', j: []});
    expect(await select('3')).toMatchInlineSnapshot(`
      Result [
        {
          "b": null,
          "i": null,
          "id": "3",
          "j": [],
          "r": null,
        },
      ]
    `);

    // These both fail inside of postgres... This is valid though, right?
    // await runMutation('insert', {id: '4', j: true});
    // expect(await select('4')).toMatchInlineSnapshot(`Result []`);

    // await runMutation('insert', {id: '5', j: false});
    // expect(await select('5')).toMatchInlineSnapshot(`Result []`);

    await runMutation('insert', {id: '6', j: 'string'});
    expect(await select('6')).toMatchInlineSnapshot(`
      Result [
        {
          "b": null,
          "i": null,
          "id": "6",
          "j": "string",
          "r": null,
        },
      ]
    `);

    await runMutation('insert', {id: '7', j: 0});
    expect(await select('7')).toMatchInlineSnapshot(`
      Result [
        {
          "b": null,
          "i": null,
          "id": "7",
          "j": 0,
          "r": null,
        },
      ]
    `);
  });

  test('json update edge cases', async () => {
    await runMutation('update', {id: '100', j: {a: 1}});
    expect(await select('100')).toMatchInlineSnapshot(`
      Result [
        {
          "b": true,
          "i": 100n,
          "id": "100",
          "j": {
            "a": 1,
          },
          "r": 1.1,
        },
      ]
    `);

    await runMutation('update', {id: '100', j: null});
    expect(await select('100')).toMatchInlineSnapshot(`
      Result [
        {
          "b": true,
          "i": 100n,
          "id": "100",
          "j": null,
          "r": 1.1,
        },
      ]
    `);

    await runMutation('update', {id: '100', j: 'string'});
    expect(await select('100')).toMatchInlineSnapshot(`
      Result [
        {
          "b": true,
          "i": 100n,
          "id": "100",
          "j": "string",
          "r": 1.1,
        },
      ]
    `);

    await runMutation('update', {id: '100', j: 0});
    expect(await select('100')).toMatchInlineSnapshot(`
      Result [
        {
          "b": true,
          "i": 100n,
          "id": "100",
          "j": 0,
          "r": 1.1,
        },
      ]
    `);

    await runMutation('update', {id: '100', j: {}});
    expect(await select('100')).toMatchInlineSnapshot(`
      Result [
        {
          "b": true,
          "i": 100n,
          "id": "100",
          "j": {},
          "r": 1.1,
        },
      ]
    `);

    await runMutation('update', {id: '100', j: []});
    expect(await select('100')).toMatchInlineSnapshot(`
      Result [
        {
          "b": true,
          "i": 100n,
          "id": "100",
          "j": [],
          "r": 1.1,
        },
      ]
    `);
  });

  test('boolean conversion', async () => {
    await runMutation('update', {id: '100', b: true});
    expect(await select('100')).toMatchInlineSnapshot(`
      Result [
        {
          "b": true,
          "i": 100n,
          "id": "100",
          "j": {},
          "r": 1.1,
        },
      ]
    `);
    await runMutation('update', {id: '100', b: false});
    expect(await select('100')).toMatchInlineSnapshot(`
      Result [
        {
          "b": false,
          "i": 100n,
          "id": "100",
          "j": {},
          "r": 1.1,
        },
      ]
    `);
  });

  test('bigint range', async () => {
    await runMutation('update', {id: '100', i: Number.MAX_SAFE_INTEGER + 1});
    expect(await select('100')).toMatchInlineSnapshot(`
      Result [
        {
          "b": true,
          "i": 9007199254740992n,
          "id": "100",
          "j": {},
          "r": 1.1,
        },
      ]
    `);

    await runMutation('update', {id: '100', i: Number.MIN_SAFE_INTEGER - 1});
    expect(await select('100')).toMatchInlineSnapshot(`
      Result [
        {
          "b": true,
          "i": -9007199254740992n,
          "id": "100",
          "j": {},
          "r": 1.1,
        },
      ]
    `);
  });
});
