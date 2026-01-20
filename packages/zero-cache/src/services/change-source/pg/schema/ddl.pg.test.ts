import type postgres from 'postgres';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../../../shared/src/queue.ts';
import * as v from '../../../../../../shared/src/valita.ts';
import {test, type PgTest} from '../../../../test/db.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import type {
  Message,
  MessageMessage,
} from '../logical-replication/pgoutput.types.ts';
import {subscribe} from '../logical-replication/stream.ts';
import {
  createEventTriggerStatements,
  ddlStartEventSchema,
  ddlUpdateEventSchema,
  type DdlStartEvent,
  type DdlUpdateEvent,
} from './ddl.ts';

const SLOT_NAME = 'ddl_test_slot';

describe('change-source/tables/ddl', () => {
  let upstream: PostgresDB;
  let messages: Queue<Message>;
  let notices: Queue<postgres.Notice>;

  const APP_ID = 'zap';
  const SHARD_NUM = 0;

  beforeEach<PgTest>(async ({testDBs}) => {
    notices = new Queue();
    upstream = await testDBs.create('ddl_test_upstream', n =>
      notices.enqueue(n),
    );

    await upstream.unsafe(STARTING_SCHEMA);

    await upstream.unsafe(
      createEventTriggerStatements({
        appID: APP_ID,
        shardNum: SHARD_NUM,
        publications: ['zero_all', 'zero_sum'],
      }),
    );

    await upstream`SELECT pg_create_logical_replication_slot(${SLOT_NAME}, 'pgoutput')`;

    messages = new Queue<Message>();
    const sub = await subscribe(
      createSilentLogContext(),
      upstream,
      SLOT_NAME,
      ['zero_all'],
      0n,
    );

    void (async function () {
      for await (const [_lsn, msg] of sub.messages) {
        if (msg.tag === 'keepalive') {
          sub.acks.push(0n);
        } else {
          messages.enqueue(msg);
        }
      }
    })();
    return async () => {
      sub.messages.cancel();
      await testDBs.drop(upstream);
    };
  });

  async function drainReplicationMessages(num: number): Promise<Message[]> {
    const drained: Message[] = [];
    while (drained.length < num) {
      drained.push(await messages.dequeue());
    }
    return drained;
  }

  const STARTING_SCHEMA = `
    CREATE SCHEMA zero;
    CREATE SCHEMA pub;
    CREATE SCHEMA private;

    CREATE TABLE zero.foo(id TEXT PRIMARY KEY);

    CREATE TABLE pub.foo(id TEXT PRIMARY KEY, name TEXT UNIQUE, description TEXT);
    CREATE TABLE pub.boo(id TEXT PRIMARY KEY, name TEXT UNIQUE, description TEXT);
    CREATE TABLE pub.yoo(id TEXT PRIMARY KEY, name TEXT UNIQUE, description TEXT);

    CREATE TABLE private.foo(id TEXT PRIMARY KEY, name TEXT UNIQUE, description TEXT);
    CREATE TABLE private.yoo(id TEXT PRIMARY KEY, name TEXT UNIQUE, description TEXT);

    CREATE INDEX foo_custom_index ON pub.foo (description, name);
    CREATE INDEX yoo_custom_index ON pub.yoo (description, name);
    
    CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA pub;
    CREATE PUBLICATION zero_sum FOR TABLE pub.foo (id, name), pub.boo;
    CREATE PUBLICATION nonzeropub FOR TABLE pub.foo, pub.boo;
    `;

  // For zero_all, zero_sum
  const DDL_START: Omit<DdlStartEvent, 'context'> = {
    type: 'ddlStart',
    version: 1,
    schema: {
      tables: [
        {
          oid: expect.any(Number),
          schema: 'pub',
          schemaOID: expect.any(Number),
          name: 'boo',
          replicaIdentity: 'd',
          replicaIdentityColumns: ['id'],
          columns: {
            description: {
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              dflt: null,
              notNull: false,
              pos: 3,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              dflt: null,
              notNull: true,
              pos: 1,
            },
            name: {
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              dflt: null,
              notNull: false,
              pos: 2,
            },
          },
          primaryKey: ['id'],
          publications: {
            ['zero_all']: {rowFilter: null},
            ['zero_sum']: {rowFilter: null},
          },
        },
        {
          oid: expect.any(Number),
          schema: 'pub',
          schemaOID: expect.any(Number),
          name: 'foo',
          replicaIdentity: 'd',
          replicaIdentityColumns: ['id'],
          columns: {
            description: {
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              dflt: null,
              notNull: false,
              pos: 3,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              dflt: null,
              notNull: true,
              pos: 1,
            },
            name: {
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              dflt: null,
              notNull: false,
              pos: 2,
            },
          },
          primaryKey: ['id'],
          publications: {
            ['zero_all']: {rowFilter: null},
            ['zero_sum']: {rowFilter: null},
          },
        },
        {
          oid: expect.any(Number),
          schema: 'pub',
          schemaOID: expect.any(Number),
          name: 'yoo',
          replicaIdentity: 'd',
          replicaIdentityColumns: ['id'],
          columns: {
            description: {
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              dflt: null,
              notNull: false,
              pos: 3,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              dflt: null,
              notNull: true,
              pos: 1,
            },
            name: {
              characterMaximumLength: null,
              dataType: 'text',
              typeOID: 25,
              dflt: null,
              notNull: false,
              pos: 2,
            },
          },
          primaryKey: ['id'],
          publications: {['zero_all']: {rowFilter: null}},
        },
      ],
      indexes: [
        {
          name: 'boo_name_key',
          schema: 'pub',
          tableName: 'boo',
          columns: {name: 'ASC'},
          unique: true,
        },
        {
          name: 'boo_pkey',
          schema: 'pub',
          tableName: 'boo',
          columns: {id: 'ASC'},
          unique: true,
        },
        {
          name: 'foo_custom_index',
          schema: 'pub',
          tableName: 'foo',
          columns: {
            description: 'ASC',
            name: 'ASC',
          },
          unique: false,
        },
        {
          name: 'foo_name_key',
          schema: 'pub',
          tableName: 'foo',
          columns: {name: 'ASC'},
          unique: true,
        },
        {
          name: 'foo_pkey',
          schema: 'pub',
          tableName: 'foo',
          columns: {id: 'ASC'},
          unique: true,
        },
        {
          name: 'yoo_custom_index',
          schema: 'pub',
          tableName: 'yoo',
          columns: {
            description: 'ASC',
            name: 'ASC',
          },
          unique: false,
        },
        {
          name: 'yoo_name_key',
          schema: 'pub',
          tableName: 'yoo',
          columns: {name: 'ASC'},
          unique: true,
        },
        {
          name: 'yoo_pkey',
          schema: 'pub',
          tableName: 'yoo',
          columns: {id: 'ASC'},
          unique: true,
        },
      ],
    },
  } as const;

  function inserted<T>(arr: readonly T[], pos: number, ...items: T[]): T[] {
    return replaced(arr, pos, 0, ...items);
  }

  function dropped<T>(
    arr: readonly T[],
    pos: number,
    deleteCount: number,
  ): T[] {
    return replaced(arr, pos, deleteCount);
  }

  function replaced<T>(
    arr: readonly T[],
    pos: number,
    deleteCount: number,
    ...items: T[]
  ): T[] {
    const copy = arr.slice();
    copy.splice(pos, deleteCount, ...items);
    return copy;
  }

  test.each([
    [
      'create table',
      `CREATE TABLE pub.bar(id TEXT PRIMARY KEY, a INT4 UNIQUE, b INT8 UNIQUE, UNIQUE(b, a))`,
      {
        context: {
          query:
            'CREATE TABLE pub.bar(id TEXT PRIMARY KEY, a INT4 UNIQUE, b INT8 UNIQUE, UNIQUE(b, a))',
        },
        type: 'ddlUpdate',
        version: 1,
        event: {tag: 'CREATE TABLE'},
        schema: {
          tables: inserted(DDL_START.schema.tables, 0, {
            oid: expect.any(Number),
            schema: 'pub',
            schemaOID: expect.any(Number),
            name: 'bar',
            replicaIdentity: 'd',
            replicaIdentityColumns: ['id'],
            columns: {
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: true,
                dflt: null,
                pos: 1,
              },
              a: {
                characterMaximumLength: null,
                dataType: 'int4',
                typeOID: 23,
                notNull: false,
                dflt: null,
                pos: 2,
              },
              b: {
                characterMaximumLength: null,
                dataType: 'int8',
                typeOID: 20,
                notNull: false,
                dflt: null,
                pos: 3,
              },
            },
            primaryKey: ['id'],
            publications: {['zero_all']: {rowFilter: null}},
          }),
          indexes: inserted(
            DDL_START.schema.indexes,
            0,
            {
              columns: {a: 'ASC'},
              name: 'bar_a_key',
              schema: 'pub',
              tableName: 'bar',
              unique: true,
            },
            {
              columns: {
                b: 'ASC',
                a: 'ASC',
              },
              name: 'bar_b_a_key',
              schema: 'pub',
              tableName: 'bar',
              unique: true,
            },
            {
              columns: {b: 'ASC'},
              name: 'bar_b_key',
              schema: 'pub',
              tableName: 'bar',
              unique: true,
            },
            {
              columns: {id: 'ASC'},
              name: 'bar_pkey',
              schema: 'pub',
              tableName: 'bar',
              unique: true,
            },
          ),
        },
      },
    ],
    [
      'create index',
      `CREATE INDEX foo_name_index on pub.foo (name desc, id)`,
      {
        context: {
          query: 'CREATE INDEX foo_name_index on pub.foo (name desc, id)',
        },
        type: 'ddlUpdate',
        version: 1,
        event: {tag: 'CREATE INDEX'},
        schema: {
          tables: DDL_START.schema.tables,
          indexes: inserted(DDL_START.schema.indexes, 3, {
            columns: {
              name: 'DESC',
              id: 'ASC',
            },
            name: 'foo_name_index',
            schema: 'pub',
            tableName: 'foo',
            unique: false,
          }),
        },
      },
    ],
    [
      'rename table',
      `ALTER TABLE pub.foo RENAME TO food`,
      {
        context: {
          query: 'ALTER TABLE pub.foo RENAME TO food',
        },
        type: 'ddlUpdate',
        version: 1,
        event: {tag: 'ALTER TABLE'},
        schema: {
          tables: replaced(DDL_START.schema.tables, 1, 1, {
            oid: expect.any(Number),
            schema: 'pub',
            schemaOID: expect.any(Number),
            name: 'food',
            replicaIdentity: 'd',
            replicaIdentityColumns: ['id'],
            columns: {
              description: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: false,
                dflt: null,
                pos: 3,
              },
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: true,
                dflt: null,
                pos: 1,
              },
              name: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: false,
                dflt: null,
                pos: 2,
              },
            },
            primaryKey: ['id'],
            publications: {
              ['zero_all']: {rowFilter: null},
              ['zero_sum']: {rowFilter: null},
            },
          }),
          indexes: replaced(
            DDL_START.schema.indexes,
            2,
            3,
            {
              columns: {
                description: 'ASC',
                name: 'ASC',
              },
              name: 'foo_custom_index',
              schema: 'pub',
              tableName: 'food',
              unique: false,
            },
            {
              columns: {name: 'ASC'},
              name: 'foo_name_key',
              schema: 'pub',
              tableName: 'food',
              unique: true,
            },
            {
              columns: {id: 'ASC'},
              name: 'foo_pkey',
              schema: 'pub',
              tableName: 'food',
              unique: true,
            },
          ),
        },
      },
    ],
    [
      'add column that results in a new index',
      `ALTER TABLE pub.foo ADD username TEXT UNIQUE`,
      {
        context: {
          query: 'ALTER TABLE pub.foo ADD username TEXT UNIQUE',
        },
        event: {tag: 'ALTER TABLE'},
        type: 'ddlUpdate',
        version: 1,
        schema: {
          tables: replaced(DDL_START.schema.tables, 1, 1, {
            oid: expect.any(Number),
            schema: 'pub',
            schemaOID: expect.any(Number),
            name: 'foo',
            replicaIdentity: 'd',
            replicaIdentityColumns: ['id'],
            columns: {
              description: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: false,
                dflt: null,
                pos: 3,
              },
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: true,
                dflt: null,
                pos: 1,
              },
              name: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: false,
                dflt: null,
                pos: 2,
              },
              username: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: false,
                dflt: null,
                pos: 4,
              },
            },
            primaryKey: ['id'],
            publications: {
              ['zero_all']: {rowFilter: null},
              ['zero_sum']: {rowFilter: null},
            },
          }),
          indexes: replaced(DDL_START.schema.indexes, 5, 0, {
            columns: {username: 'ASC'},
            name: 'foo_username_key',
            schema: 'pub',
            tableName: 'foo',
            unique: true,
          }),
        },
      },
    ],
    [
      'add column with default value',
      `ALTER TABLE pub.foo ADD bar text DEFAULT 'boo'`,
      {
        context: {
          query: "ALTER TABLE pub.foo ADD bar text DEFAULT 'boo'",
        },
        type: 'ddlUpdate',
        version: 1,
        event: {tag: 'ALTER TABLE'},
        schema: {
          indexes: DDL_START.schema.indexes,
          tables: replaced(DDL_START.schema.tables, 1, 1, {
            oid: expect.any(Number),
            schema: 'pub',
            schemaOID: expect.any(Number),
            name: 'foo',
            replicaIdentity: 'd',
            replicaIdentityColumns: ['id'],
            columns: {
              bar: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: false,
                dflt: "'boo'::text",
                pos: 4,
              },
              description: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: false,
                dflt: null,
                pos: 3,
              },
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: true,
                dflt: null,
                pos: 1,
              },
              name: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: false,
                dflt: null,
                pos: 2,
              },
            },
            primaryKey: ['id'],
            publications: {
              ['zero_all']: {rowFilter: null},
              ['zero_sum']: {rowFilter: null},
            },
          }),
        },
      },
    ],
    [
      'alter column default value',
      `ALTER TABLE pub.foo ALTER name SET DEFAULT 'alice'`,
      {
        context: {
          query: "ALTER TABLE pub.foo ALTER name SET DEFAULT 'alice'",
        },
        type: 'ddlUpdate',
        version: 1,
        event: {tag: 'ALTER TABLE'},
        schema: {
          indexes: DDL_START.schema.indexes,
          tables: replaced(DDL_START.schema.tables, 1, 1, {
            oid: expect.any(Number),
            schema: 'pub',
            schemaOID: expect.any(Number),
            name: 'foo',
            replicaIdentity: 'd',
            replicaIdentityColumns: ['id'],
            columns: {
              description: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: false,
                dflt: null,
                pos: 3,
              },
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: true,
                dflt: null,
                pos: 1,
              },
              name: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: false,
                dflt: "'alice'::text",
                pos: 2,
              },
            },
            primaryKey: ['id'],
            publications: {
              ['zero_all']: {rowFilter: null},
              ['zero_sum']: {rowFilter: null},
            },
          }),
        },
      },
    ],
    [
      'rename column',
      `ALTER TABLE pub.foo RENAME name to handle`,
      {
        context: {
          query: 'ALTER TABLE pub.foo RENAME name to handle',
        },
        type: 'ddlUpdate',
        version: 1,
        event: {tag: 'ALTER TABLE'},
        schema: {
          tables: replaced(DDL_START.schema.tables, 1, 1, {
            oid: expect.any(Number),
            schema: 'pub',
            schemaOID: expect.any(Number),
            name: 'foo',
            replicaIdentity: 'd',
            replicaIdentityColumns: ['id'],
            columns: {
              description: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: false,
                dflt: null,
                pos: 3,
              },
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: true,
                dflt: null,
                pos: 1,
              },
              handle: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: false,
                dflt: null,
                pos: 2,
              },
            },
            primaryKey: ['id'],
            publications: {
              ['zero_all']: {rowFilter: null},
              ['zero_sum']: {rowFilter: null},
            },
          }),
          indexes: replaced(
            DDL_START.schema.indexes,
            2,
            2,
            {
              columns: {
                description: 'ASC',
                handle: 'ASC',
              },
              name: 'foo_custom_index',
              schema: 'pub',
              tableName: 'foo',
              unique: false,
            },
            {
              columns: {handle: 'ASC'},
              name: 'foo_name_key',
              schema: 'pub',
              tableName: 'foo',
              unique: true,
            },
          ),
        },
      },
    ],
    [
      'drop column',
      `ALTER TABLE pub.foo drop description`,
      {
        context: {query: 'ALTER TABLE pub.foo drop description'},
        type: 'ddlUpdate',
        version: 1,
        event: {tag: 'ALTER TABLE'},
        schema: {
          tables: replaced(DDL_START.schema.tables, 1, 1, {
            oid: expect.any(Number),
            schema: 'pub',
            schemaOID: expect.any(Number),
            name: 'foo',
            replicaIdentity: 'd',
            replicaIdentityColumns: ['id'],
            columns: {
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: true,
                dflt: null,
                pos: 1,
              },
              name: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                notNull: false,
                dflt: null,
                pos: 2,
              },
            },
            primaryKey: ['id'],
            publications: {
              ['zero_all']: {rowFilter: null},
              ['zero_sum']: {rowFilter: null},
            },
          }),
          // "foo_custom_index" depended on the "description column"
          indexes: dropped(DDL_START.schema.indexes, 2, 1),
        },
      },
    ],
    [
      'drop table',
      `DROP TABLE pub.foo, pub.yoo`,
      {
        context: {query: 'DROP TABLE pub.foo, pub.yoo'},
        type: 'ddlUpdate',
        version: 1,
        event: {tag: 'DROP TABLE'},
        schema: {
          tables: [
            {
              oid: expect.any(Number),
              schema: 'pub',
              schemaOID: expect.any(Number),
              name: 'boo',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['id'],
              columns: {
                description: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  dflt: null,
                  notNull: false,
                  pos: 3,
                },
                id: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  dflt: null,
                  notNull: true,
                  pos: 1,
                },
                name: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  dflt: null,
                  notNull: false,
                  pos: 2,
                },
              },
              primaryKey: ['id'],
              publications: {
                ['zero_all']: {rowFilter: null},
                ['zero_sum']: {rowFilter: null},
              },
            },
          ],
          indexes: [
            {
              columns: {name: 'ASC'},
              name: 'boo_name_key',
              schema: 'pub',
              tableName: 'boo',
              unique: true,
            },
            {
              columns: {id: 'ASC'},
              name: 'boo_pkey',
              schema: 'pub',
              tableName: 'boo',
              unique: true,
            },
          ],
        },
      },
    ],
    [
      'drop index',
      `DROP INDEX pub.foo_custom_index, pub.yoo_custom_index`,
      {
        context: {
          query: 'DROP INDEX pub.foo_custom_index, pub.yoo_custom_index',
        },
        type: 'ddlUpdate',
        version: 1,
        event: {tag: 'DROP INDEX'},
        schema: {
          indexes: [
            {
              columns: {name: 'ASC'},
              name: 'boo_name_key',
              schema: 'pub',
              tableName: 'boo',
              unique: true,
            },
            {
              columns: {id: 'ASC'},
              name: 'boo_pkey',
              schema: 'pub',
              tableName: 'boo',
              unique: true,
            },
            {
              columns: {name: 'ASC'},
              name: 'foo_name_key',
              schema: 'pub',
              tableName: 'foo',
              unique: true,
            },
            {
              columns: {id: 'ASC'},
              name: 'foo_pkey',
              schema: 'pub',
              tableName: 'foo',
              unique: true,
            },
            {
              columns: {name: 'ASC'},
              name: 'yoo_name_key',
              schema: 'pub',
              tableName: 'yoo',
              unique: true,
            },
            {
              columns: {id: 'ASC'},
              name: 'yoo_pkey',
              schema: 'pub',
              tableName: 'yoo',
              unique: true,
            },
          ],
          tables: DDL_START.schema.tables,
        },
      },
    ],
    [
      'alter table publication add table',
      `ALTER PUBLICATION zero_sum ADD TABLE pub.yoo`,
      {
        context: {query: 'ALTER PUBLICATION zero_sum ADD TABLE pub.yoo'},
        type: 'ddlUpdate',
        version: 1,
        event: {tag: 'ALTER PUBLICATION'},
        schema: {
          indexes: DDL_START.schema.indexes,
          tables: replaced(DDL_START.schema.tables, 2, 1, {
            oid: expect.any(Number),
            schema: 'pub',
            schemaOID: expect.any(Number),
            name: 'yoo',
            replicaIdentity: 'd',
            replicaIdentityColumns: ['id'],
            columns: {
              description: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                dflt: null,
                notNull: false,
                pos: 3,
              },
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                dflt: null,
                notNull: true,
                pos: 1,
              },
              name: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                dflt: null,
                notNull: false,
                pos: 2,
              },
            },
            primaryKey: ['id'],
            publications: {
              ['zero_all']: {rowFilter: null},
              ['zero_sum']: {rowFilter: null}, // Now part of zero_sum
            },
          }),
        },
      },
    ],
    [
      'alter table publication drop table',
      `ALTER PUBLICATION zero_sum DROP TABLE pub.foo`,
      {
        context: {query: 'ALTER PUBLICATION zero_sum DROP TABLE pub.foo'},
        type: 'ddlUpdate',
        version: 1,
        event: {tag: 'ALTER PUBLICATION'},
        schema: {
          indexes: DDL_START.schema.indexes,
          tables: replaced(DDL_START.schema.tables, 1, 1, {
            oid: expect.any(Number),
            schema: 'pub',
            schemaOID: expect.any(Number),
            name: 'foo',
            replicaIdentity: 'd',
            replicaIdentityColumns: ['id'],
            columns: {
              description: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                dflt: null,
                notNull: false,
                pos: 3,
              },
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                dflt: null,
                notNull: true,
                pos: 1,
              },
              name: {
                characterMaximumLength: null,
                dataType: 'text',
                typeOID: 25,
                dflt: null,
                notNull: false,
                pos: 2,
              },
            },
            primaryKey: ['id'],
            publications: {
              // No longer part of zero_sum
              ['zero_all']: {rowFilter: null},
            },
          }),
        },
      },
    ],
    [
      'alter schema publication',
      `ALTER PUBLICATION zero_all ADD TABLES IN SCHEMA zero`,
      {
        context: {
          query: 'ALTER PUBLICATION zero_all ADD TABLES IN SCHEMA zero',
        },
        type: 'ddlUpdate',
        version: 1,
        event: {tag: 'ALTER PUBLICATION'},
        schema: {
          indexes: [
            ...DDL_START.schema.indexes,
            {
              name: 'foo_pkey',
              schema: 'zero',
              tableName: 'foo',
              columns: {id: 'ASC'},
              unique: true,
            },
          ],
          tables: [
            ...DDL_START.schema.tables,
            {
              oid: expect.any(Number),
              schema: 'zero',
              schemaOID: expect.any(Number),
              name: 'foo',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['id'],
              columns: {
                id: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  dflt: null,
                  notNull: true,
                  pos: 1,
                },
              },
              primaryKey: ['id'],
              publications: {
                ['zero_all']: {rowFilter: null}, // Now part of zero_all
              },
            },
          ],
        },
      },
    ],
    [
      'alter schema',
      `ALTER SCHEMA pub RENAME TO bup`,
      {
        context: {
          query: 'ALTER SCHEMA pub RENAME TO bup',
        },
        type: 'ddlUpdate',
        version: 1,
        event: {tag: 'ALTER SCHEMA'},
        schema: {
          tables: [
            {
              oid: expect.any(Number),
              schema: 'bup',
              schemaOID: expect.any(Number),
              name: 'boo',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['id'],
              columns: {
                description: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  dflt: null,
                  notNull: false,
                  pos: 3,
                },
                id: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  dflt: null,
                  notNull: true,
                  pos: 1,
                },
                name: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  dflt: null,
                  notNull: false,
                  pos: 2,
                },
              },
              primaryKey: ['id'],
              publications: {
                ['zero_all']: {rowFilter: null},
                ['zero_sum']: {rowFilter: null},
              },
            },
            {
              oid: expect.any(Number),
              schema: 'bup',
              schemaOID: expect.any(Number),
              name: 'foo',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['id'],
              columns: {
                description: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  dflt: null,
                  notNull: false,
                  pos: 3,
                },
                id: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  dflt: null,
                  notNull: true,
                  pos: 1,
                },
                name: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  dflt: null,
                  notNull: false,
                  pos: 2,
                },
              },
              primaryKey: ['id'],
              publications: {
                ['zero_all']: {rowFilter: null},
                ['zero_sum']: {rowFilter: null},
              },
            },
            {
              oid: expect.any(Number),
              schema: 'bup',
              schemaOID: expect.any(Number),
              name: 'yoo',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['id'],
              columns: {
                description: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  dflt: null,
                  notNull: false,
                  pos: 3,
                },
                id: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  dflt: null,
                  notNull: true,
                  pos: 1,
                },
                name: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  dflt: null,
                  notNull: false,
                  pos: 2,
                },
              },
              primaryKey: ['id'],
              publications: {['zero_all']: {rowFilter: null}},
            },
          ],
          indexes: [
            {
              name: 'boo_name_key',
              schema: 'bup',
              tableName: 'boo',
              columns: {name: 'ASC'},
              unique: true,
            },
            {
              name: 'boo_pkey',
              schema: 'bup',
              tableName: 'boo',
              columns: {id: 'ASC'},
              unique: true,
            },
            {
              name: 'foo_custom_index',
              schema: 'bup',
              tableName: 'foo',
              columns: {
                description: 'ASC',
                name: 'ASC',
              },
              unique: false,
            },
            {
              name: 'foo_name_key',
              schema: 'bup',
              tableName: 'foo',
              columns: {name: 'ASC'},
              unique: true,
            },
            {
              name: 'foo_pkey',
              schema: 'bup',
              tableName: 'foo',
              columns: {id: 'ASC'},
              unique: true,
            },
            {
              name: 'yoo_custom_index',
              schema: 'bup',
              tableName: 'yoo',
              columns: {
                description: 'ASC',
                name: 'ASC',
              },
              unique: false,
            },
            {
              name: 'yoo_name_key',
              schema: 'bup',
              tableName: 'yoo',
              columns: {name: 'ASC'},
              unique: true,
            },
            {
              name: 'yoo_pkey',
              schema: 'bup',
              tableName: 'yoo',
              columns: {id: 'ASC'},
              unique: true,
            },
          ],
        },
      },
    ],
  ] satisfies [string, string, DdlUpdateEvent][])(
    '%s',
    async (_, query, ddlUpdate) => {
      await upstream.begin(async tx => {
        await tx`INSERT INTO pub.boo(id) VALUES('1')`;
        await tx.unsafe(query);
      });

      const messages = await drainReplicationMessages(6);
      expect(messages).toMatchObject([
        {tag: 'begin'},
        {tag: 'relation'},
        {tag: 'insert'},
        {
          tag: 'message',
          prefix: 'zap/0',
          content: expect.any(Uint8Array),
          flags: 1,
          transactional: true,
        },
        {
          tag: 'message',
          prefix: 'zap/0',
          content: expect.any(Uint8Array),
          flags: 1,
          transactional: true,
        },
        {tag: 'commit'},
      ]);

      let msg = messages[3] as MessageMessage;
      expect(parseDDLStartEvent(msg)).toMatchObject({
        ...DDL_START,
        context: {query},
      } satisfies DdlStartEvent);

      msg = messages[4] as MessageMessage;
      expect(parseDDLUpdateEvent(msg)).toMatchObject(ddlUpdate);
    },
  );

  // Run the same DDL commands on tables in the "private" schema
  // (or the "nonzeropub" publication) and verify that they do not trigger
  // "ddlUpdate" events.
  test.each([
    [
      'CREATE TABLE private.bar(id TEXT PRIMARY KEY, a INT4 UNIQUE, b INT8 UNIQUE, UNIQUE(b, a))',
      ['zero(0) ignoring private.bar'],
    ],
    [
      'CREATE INDEX foo_name_index on private.foo (name desc, id)',
      ['zero(0) ignoring private.foo_name_index'],
    ],
    [
      'ALTER TABLE private.foo RENAME TO food',
      ['zero(0) ignoring private.food'],
    ],
    [
      'ALTER TABLE private.foo ADD username TEXT UNIQUE',
      ['zero(0) ignoring private.foo'],
    ],
    [
      `ALTER TABLE private.foo ADD bar text DEFAULT 'boo'`,
      ['zero(0) ignoring private.foo'],
    ],
    [
      `ALTER TABLE private.foo ALTER name SET DEFAULT 'alice'`,
      ['zero(0) ignoring private.foo'],
    ],
    [
      `ALTER TABLE private.foo RENAME name to handle`,
      ['zero(0) ignoring private.foo.handle'],
    ],
    [
      `ALTER TABLE private.foo drop description`,
      ['zero(0) ignoring private.foo'],
    ],
    [
      `ALTER PUBLICATION nonzeropub ADD TABLE pub.yoo`,
      ['zero(0) ignoring pub.yoo in publication nonzeropub'],
    ],
    [
      `
      CREATE SCHEMA IF NOT EXISTS "cvr";
      CREATE TABLE IF NOT EXISTS "cvr"."versionHistory" (
        "dataVersion" int NOT NULL,
        "schemaVersion" int NOT NULL,
        "minSafeVersion" int NOT NULL,

        lock char(1) NOT NULL CONSTRAINT DF_schema_meta_lock DEFAULT 'v',
        CONSTRAINT PK_schema_meta_lock PRIMARY KEY (lock),
        CONSTRAINT CK_schema_meta_lock CHECK (lock='v')
      );
      SELECT "dataVersion", "schemaVersion", "minSafeVersion" FROM "cvr"."versionHistory";
      `,
      ['zero(0) ignoring cvr."versionHistory"'],
    ],
    [
      `CREATE TABLE IF NOT EXISTS pub.foo(id TEXT PRIMARY KEY, name TEXT UNIQUE, description TEXT);`,
      [
        `relation "foo" already exists, skipping`,
        `zero(0) ignoring noop CREATE TABLE`,
      ],
    ],
  ] satisfies [string, string[]][])(
    'ignore unrelated events: %s',
    async (query, expectedNotices) => {
      while (notices.size()) {
        await notices.dequeue();
      }

      await upstream.begin(async tx => {
        await tx`INSERT INTO pub.boo(id) VALUES('1')`;
        await tx.unsafe(query);
      });

      // There should only be a ddlStart message, and no ddlUpdate message.
      const messages = await drainReplicationMessages(5);
      expect(messages).toMatchObject([
        {tag: 'begin'},
        {tag: 'relation'},
        {tag: 'insert'},
        {
          tag: 'message',
          prefix: 'zap/0',
          content: expect.any(Uint8Array),
          flags: 1,
          transactional: true,
        },
        {tag: 'commit'},
      ]);

      const {content: start} = messages[3] as MessageMessage;
      expect(JSON.parse(new TextDecoder().decode(start))).toMatchObject({
        type: 'ddlStart',
      });

      for (const n of expectedNotices) {
        const notice = await notices.dequeue();
        expect(notice.message).toBe(n);
      }
    },
  );

  test('postgres documentation: current_query() is unreliable', async () => {
    await upstream`CREATE PROCEDURE procedure_name()
       LANGUAGE SQL
       AS $$ ALTER TABLE pub.foo ADD bar text $$;`;

    await upstream`CALL procedure_name()`;
    await upstream.unsafe(
      `ALTER TABLE pub.foo ADD boo text; ALTER TABLE pub.foo DROP boo;`,
    );

    const messages = await drainReplicationMessages(10);
    expect(messages).toMatchObject([
      {tag: 'begin'},
      {
        tag: 'message',
        prefix: 'zap/0',
        content: expect.any(Uint8Array),
        flags: 1,
        transactional: true,
      },
      {
        tag: 'message',
        prefix: 'zap/0',
        content: expect.any(Uint8Array),
        flags: 1,
        transactional: true,
      },
      {tag: 'commit'},

      {tag: 'begin'},
      {
        tag: 'message',
        prefix: 'zap/0',
        content: expect.any(Uint8Array),
        flags: 1,
        transactional: true,
      },
      {
        tag: 'message',
        prefix: 'zap/0',
        content: expect.any(Uint8Array),
        flags: 1,
        transactional: true,
      },
      {
        tag: 'message',
        prefix: 'zap/0',
        content: expect.any(Uint8Array),
        flags: 1,
        transactional: true,
      },
      {
        tag: 'message',
        prefix: 'zap/0',
        content: expect.any(Uint8Array),
        flags: 1,
        transactional: true,
      },
      {tag: 'commit'},
    ]);

    let msg = messages[2] as MessageMessage;
    expect(parseDDLUpdateEvent(msg)).toMatchObject({
      type: 'ddlUpdate',
      version: 1,
      // Top level query may not provide any information about the actual DDL command.
      context: {query: 'CALL procedure_name()'},
      event: {tag: 'ALTER TABLE'},
    });

    msg = messages[6] as MessageMessage;
    expect(parseDDLUpdateEvent(msg)).toMatchObject({
      type: 'ddlUpdate',
      version: 1,
      context: {
        // A compound top level query may contain more than one DDL command.
        query: `ALTER TABLE pub.foo ADD boo text; ALTER TABLE pub.foo DROP boo;`,
      },
      event: {tag: 'ALTER TABLE'},
    });
    msg = messages[8] as MessageMessage;
    expect(parseDDLUpdateEvent(msg)).toMatchObject({
      type: 'ddlUpdate',
      version: 1,
      context: {
        // A compound top level query may contain more than one DDL command.
        query: `ALTER TABLE pub.foo ADD boo text; ALTER TABLE pub.foo DROP boo;`,
      },
      event: {tag: 'ALTER TABLE'},
    });
  });
});

function parseDDLStartEvent(msg: MessageMessage) {
  return v.parse(
    JSON.parse(new TextDecoder().decode(msg.content)),
    ddlStartEventSchema,
  );
}

function parseDDLUpdateEvent(msg: MessageMessage) {
  return v.parse(
    JSON.parse(new TextDecoder().decode(msg.content)),
    ddlUpdateEventSchema,
  );
}
