import {test, beforeEach, afterEach, expect} from 'vitest';
import type {PostgresDB} from '../../types/pg.js';
import {testDBs} from '../../test/db.js';
import {createSchema} from '../../../../zero-schema/src/schema.js';
import type {Supertype} from '../../../../zero-schema/src/table-schema.js';
import type {Query} from '../../../../zql/src/query/query.js';
import {Database} from '../../../../zqlite/src/db.js';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import {processMutation} from './mutagen.js';
import {WriteAuthorizerImpl, type WriteAuthorizer} from './write-authorizer.js';
import {MutationType} from '../../../../zero-protocol/src/push.js';
import {zeroSchema} from './mutagen-test-shared.js';
import {defineAuthorization} from '../../../../zero-schema/src/authorization.js';

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

CREATE TABLE "roTable" (
  id text PRIMARY KEY,
  a text
);

-- a row against which we can test delete and update
INSERT INTO "roTable" VALUES ('1', 'a');

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

CREATE TABLE "adminOnlyTable" (
  id text PRIMARY KEY,
  a text
);

-- a row against which we can test delete and update
INSERT INTO "adminOnlyTable" VALUES ('1', 'a');

CREATE TABLE "adminOnlyColumn" (
  id text PRIMARY KEY,
  a text,
  b text
);

-- a row against which we can test delete and update
INSERT INTO "adminOnlyColumn" VALUES ('1', 'a', 'b');

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
  "adminLocked" boolean
);

INSERT INTO "adminOnlyRow" VALUES ('unlocked', 'a', false);
INSERT INTO "adminOnlyRow" VALUES ('locked', 'a', true);
`;

async function createUpstreamTables(db: PostgresDB) {
  await db.unsafe(sqlSchema + zeroSchema(SHARD_ID));
}

function createReplicaTables(db: Database) {
  db.exec(sqlSchema);
}

const schema = createSchema({
  version: TEST_SCHEMA_VERSION,
  tables: {
    user: {
      tableName: 'user',
      columns: {
        id: {type: 'string'},
        role: {type: 'string'},
      },
      primaryKey: ['id'],
      relationships: {},
    },
    roTable: {
      tableName: 'roTable',
      columns: {
        id: {type: 'string'},
        a: {type: 'string'},
      },
      primaryKey: ['id'],
      relationships: {},
    },
    roColumn: {
      tableName: 'roColumn',
      columns: {
        id: {type: 'string'},
        a: {type: 'string'},
        b: {type: 'string', readOnly: true},
      },
      primaryKey: ['id'],
      relationships: {},
    },
    roCell: {
      tableName: 'roCell',
      columns: {
        id: {type: 'string'},
        a: {type: 'string'},
        b: {type: 'string', readOnly: true},
      },
      primaryKey: ['id'],
      relationships: {},
    },
    roRow: {
      tableName: 'roRow',
      columns: {
        id: {type: 'string'},
        a: {type: 'string'},
        b: {type: 'string'},
      },
      primaryKey: ['id'],
      relationships: {},
    },
    adminOnlyTable: {
      tableName: 'adminOnlyTable',
      columns: {
        id: {type: 'string'},
        a: {type: 'string'},
      },
      primaryKey: ['id'],
      relationships: {},
    },
    adminOnlyColumn: {
      tableName: 'adminOnlyColumn',
      columns: {
        id: {type: 'string'},
        a: {type: 'string'},
        b: {type: 'string'},
      },
      primaryKey: ['id'],
      relationships: {},
    },
    adminOnlyCell: {
      tableName: 'adminOnlyCell',
      columns: {
        id: {type: 'string'},
        a: {type: 'string'},
        adminLocked: {type: 'boolean'},
      },
      primaryKey: ['id'],
      relationships: {},
    },
    adminOnlyRow: {
      tableName: 'adminOnlyRow',
      columns: {
        id: {type: 'string'},
        a: {type: 'string'},
        adminLocked: {type: 'boolean'},
      },
      primaryKey: ['id'],
      relationships: {},
    },
  },
});

const authorizationConfig = await defineAuthorization<
  {sub: string},
  typeof schema
>(schema, query => {
  const allowIfNotAdminLocked =
    (
      query: Query<
        Supertype<
          [
            (typeof schema)['tables']['adminOnlyCell'],
            (typeof schema)['tables']['adminOnlyRow'],
          ]
        >
      >,
    ) =>
    (_authData: {sub: string}, row: {id: string}) =>
      query.where('id', '=', row.id).where('adminLocked', '=', false);

  const allowIfAdmin = (authData: {sub: string}) =>
    query.user.where('id', '=', authData.sub).where('role', '=', 'admin');

  return {
    roTable: {
      table: {
        insert: [],
        update: [],
        delete: [],
      },
    },
    roColumn: {
      column: {
        a: {
          insert: [],
          update: [],
          delete: [],
        },
      },
    },
    roCell: {
      cell: {
        a: {
          insert: [],
          update: [],
          delete: [],
        },
      },
    },
    roRow: {
      row: {
        insert: [],
        update: [],
        delete: [],
      },
    },
    adminOnlyTable: {
      table: {
        insert: [allowIfAdmin],
        update: [allowIfAdmin],
        delete: [allowIfAdmin],
      },
    },
    adminOnlyColumn: {
      column: {
        a: {
          insert: [allowIfAdmin],
          update: [allowIfAdmin],
          delete: [allowIfAdmin],
        },
      },
    },
    adminOnlyCell: {
      cell: {
        a: {
          // insert is always allow since it can't be admin locked on create.
          update: [allowIfNotAdminLocked(query.adminOnlyCell), allowIfAdmin],
          delete: [allowIfNotAdminLocked(query.adminOnlyCell), allowIfAdmin],
        },
      },
    },
    adminOnlyRow: {
      row: {
        // insert is always allow since it can't be admin locked on create.
        update: [allowIfNotAdminLocked(query.adminOnlyRow), allowIfAdmin],
        delete: [allowIfNotAdminLocked(query.adminOnlyRow), allowIfAdmin],
      },
    },
  };
});

let upstream: PostgresDB;
let replica: Database;
let authorizer: WriteAuthorizer;
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
    authorizationConfig,
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
  op: 'create' | 'set' | 'update' | 'delete',
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  value: any,
  uid: string = 'anon',
) {
  return processMutation(
    undefined,
    {sub: uid},
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

test('it is impossible to write to tables that are read-only', async () => {
  await procMutation('roTable', 'create', {id: '2', a: 'a'});
  let rows = await upstream`SELECT * FROM "roTable" WHERE id = '2'`;
  expect(rows.length).toBe(0);

  await procMutation('roTable', 'update', {id: '1', a: 'UPDATED'});
  rows = await upstream`SELECT * FROM "roTable" WHERE id = '1'`;
  expect(rows).toEqual([{id: '1', a: 'a'}]);

  await procMutation('roTable', 'delete', {id: '1'});
  rows = await upstream`SELECT * FROM "roTable" WHERE id = '1'`;
  expect(rows.length).toBe(1);
});

test('it is impossible to write to a column that is read-only', async () => {
  await procMutation('roColumn', 'create', {id: '2', a: 'a', b: 'b'});
  let rows = await upstream`SELECT * FROM "roColumn" WHERE id = '2'`;
  expect(rows.length).toBe(0);

  await procMutation('roColumn', 'update', {
    id: '1',
    a: 'UPDATED',
  });
  rows = await upstream`SELECT * FROM "roColumn" WHERE id = '1'`;
  expect(rows).toEqual([{id: '1', a: 'a', b: 'b'}]);

  await procMutation('roColumn', 'delete', {id: '1'});
  rows = await upstream`SELECT * FROM "roColumn" WHERE id = '1'`;
  expect(rows.length).toBe(1);
});

test('it is possible to write to a row with a read only column if that column is not written to', async () => {
  await procMutation('roColumn', 'update', {id: '1', b: 'UPDATED'});
  const rows = await upstream`SELECT * FROM "roColumn" WHERE id = '1'`;
  expect(rows).toEqual([{id: '1', b: 'UPDATED', a: 'a'}]);
});

test('it is impossible to write to a read-only cell', async () => {
  await procMutation('roCell', 'create', {id: '2', a: 'a', b: 'b'});
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
  await procMutation('roRow', 'create', {id: '2', a: 'a', b: 'b'});
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

test('non-admins cannot update admin-only tables', async () => {
  await procMutation('adminOnlyTable', 'create', {id: '2', a: 'a'}, 'usr');
  let rows = await upstream`SELECT * FROM "adminOnlyTable" WHERE id = '2'`;
  expect(rows.length).toBe(0);

  await procMutation(
    'adminOnlyTable',
    'update',
    {id: '1', a: 'UPDATED'},
    'usr',
  );
  rows = await upstream`SELECT * FROM "adminOnlyTable" WHERE id = '1'`;
  expect(rows).toEqual([{id: '1', a: 'a'}]);

  await procMutation('adminOnlyTable', 'delete', {id: '1'}, 'usr');
  rows = await upstream`SELECT * FROM "adminOnlyTable" WHERE id = '1'`;
  expect(rows.length).toBe(1);
});

test('admins can update admin-only tables', async () => {
  await procMutation('adminOnlyTable', 'create', {id: '2', a: 'a'}, 'admn');
  let rows = await upstream`SELECT * FROM "adminOnlyTable" WHERE id = '2'`;
  expect(rows.length).toBe(1);

  await procMutation(
    'adminOnlyTable',
    'update',
    {id: '2', a: 'UPDATED'},
    'admn',
  );
  rows = await upstream`SELECT * FROM "adminOnlyTable" WHERE id = '2'`;
  expect(rows).toEqual([{id: '2', a: 'UPDATED'}]);

  await procMutation('adminOnlyTable', 'delete', {id: '2'}, 'admn');
  rows = await upstream`SELECT * FROM "adminOnlyTable" WHERE id = '2'`;
  expect(rows.length).toBe(0);
});

test('non-admins cannot update admin-only columns', async () => {
  await procMutation(
    'adminOnlyColumn',
    'create',
    {id: '2', a: 'a', b: 'b'},
    'usr',
  );
  let rows = await upstream`SELECT * FROM "adminOnlyColumn" WHERE id = '2'`;
  expect(rows.length).toBe(0);

  await procMutation(
    'adminOnlyColumn',
    'update',
    {id: '1', a: 'UPDATED'},
    'usr',
  );
  rows = await upstream`SELECT * FROM "adminOnlyColumn" WHERE id = '1'`;
  expect(rows).toEqual([{id: '1', a: 'a', b: 'b'}]);

  await procMutation('adminOnlyColumn', 'delete', {id: '1'}, 'usr');
  rows = await upstream`SELECT * FROM "adminOnlyColumn" WHERE id = '1'`;
  expect(rows.length).toBe(1);
});

test('admins can update admin-only columns', async () => {
  await procMutation(
    'adminOnlyColumn',
    'create',
    {id: '2', a: 'a', b: 'b'},
    'admn',
  );
  let rows = await upstream`SELECT * FROM "adminOnlyColumn" WHERE id = '2'`;
  expect(rows.length).toBe(1);

  await procMutation(
    'adminOnlyColumn',
    'update',
    {id: '2', a: 'UPDATED'},
    'admn',
  );
  rows = await upstream`SELECT * FROM "adminOnlyColumn" WHERE id = '2'`;
  expect(rows).toEqual([{id: '2', a: 'UPDATED', b: 'b'}]);

  await procMutation('adminOnlyColumn', 'delete', {id: '2'}, 'admn');
  rows = await upstream`SELECT * FROM "adminOnlyColumn" WHERE id = '2'`;
  expect(rows.length).toBe(0);
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
    {id: 'locked', a: 'UPDATED'},
    'usr',
  );
  let rows = await upstream`SELECT * FROM "adminOnlyRow" WHERE id = 'locked'`;
  expect(rows).toEqual([{id: 'locked', a: 'a', adminLocked: true}]);

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
  expect(rows).toEqual([{id: 'unlocked', a: 'UPDATED', adminLocked: false}]);

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
  expect(rows).toEqual([{id: 'locked', a: 'UPDATED', adminLocked: true}]);

  await procMutation('adminOnlyRow', 'delete', {id: 'locked'}, 'admn');
  rows = await upstream`SELECT * FROM "adminOnlyRow" WHERE id = 'locked'`;
  expect(rows.length).toBe(0);
});
