import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../../shared/src/logging-test-utils.js';
import {Queue} from '../../../../../../shared/src/queue.js';
import {
  dropReplicationSlot,
  getConnectionURI,
  testDBs,
} from '../../../../test/db.js';
import type {PostgresDB} from '../../../../types/pg.js';
import {createEventTriggerStatements} from './ddl.js';

const SLOT_NAME = 'ddl_test_slot';

describe('change-source/tables/ddl', () => {
  let upstream: PostgresDB;
  let messages: Queue<Pgoutput.Message>;
  let service: LogicalReplicationService;

  const SHARD_ID = '0';

  beforeEach(async () => {
    createSilentLogContext();
    upstream = await testDBs.create('ddl_test_upstream');

    const upstreamURI = getConnectionURI(upstream);
    await upstream.unsafe(`
    CREATE SCHEMA zero;
    CREATE SCHEMA pub;
    CREATE SCHEMA private;

    CREATE TABLE pub.foo(id TEXT PRIMARY KEY, name TEXT UNIQUE, description TEXT);
    CREATE TABLE pub.boo(id TEXT PRIMARY KEY, name TEXT UNIQUE, description TEXT);
    CREATE TABLE pub.yoo(id TEXT PRIMARY KEY, name TEXT UNIQUE, description TEXT);

    CREATE TABLE private.foo(id TEXT PRIMARY KEY, name TEXT UNIQUE, description TEXT);
    CREATE TABLE private.yoo(id TEXT PRIMARY KEY, name TEXT UNIQUE, description TEXT);

    CREATE INDEX foo_custom_index ON pub.foo (description, name);
    CREATE INDEX yoo_custom_index ON pub.yoo (description, name);
    
    CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA pub;
    CREATE PUBLICATION zero_sum FOR TABLE pub.foo, pub.boo;
    `);

    await upstream.unsafe(
      createEventTriggerStatements(SHARD_ID, ['zero_all', 'zero_sum']),
    );

    await upstream`SELECT pg_create_logical_replication_slot(${SLOT_NAME}, 'pgoutput')`;

    messages = new Queue<Pgoutput.Message>();
    service = new LogicalReplicationService(
      {connectionString: upstreamURI},
      {acknowledge: {auto: false, timeoutSeconds: 0}},
    )
      .on('heartbeat', (lsn, _time, respond) => {
        respond && void service.acknowledge(lsn);
      })
      .on('data', (_lsn, msg) => void messages.enqueue(msg));

    void service.subscribe(
      new PgoutputPlugin({
        protoVersion: 1,
        publicationNames: ['zero_all'],
        messages: true,
      }),
      SLOT_NAME,
    );
  });

  afterEach(async () => {
    void service?.stop();
    await dropReplicationSlot(upstream, SLOT_NAME);
    await testDBs.drop(upstream);
  });

  async function drainReplicationMessages(
    num: number,
  ): Promise<Pgoutput.Message[]> {
    const drained: Pgoutput.Message[] = [];
    while (drained.length < num) {
      drained.push(await messages.dequeue());
    }
    return drained;
  }

  test.each([
    [
      'create table',
      `CREATE TABLE pub.bar(id TEXT PRIMARY KEY, a INT4 UNIQUE, b INT8 UNIQUE, UNIQUE(b, a))`,
      {
        type: 'ddl',
        version: 1,
        event: {
          context: {
            query: `CREATE TABLE pub.bar(id TEXT PRIMARY KEY, a INT4 UNIQUE, b INT8 UNIQUE, UNIQUE(b, a))`,
          },
          tag: 'CREATE TABLE',
          table: {
            schema: 'pub',
            name: 'bar',
            columns: {
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: true,
                dflt: null,
                pos: 1,
              },
              a: {
                characterMaximumLength: null,
                dataType: 'int4',
                notNull: false,
                dflt: null,
                pos: 2,
              },
              b: {
                characterMaximumLength: null,
                dataType: 'int8',
                notNull: false,
                dflt: null,
                pos: 3,
              },
            },
            primaryKey: ['id'],
            publications: {['zero_all']: {rowFilter: null}},
          },
          indexes: [
            {
              columns: {a: 'ASC'},
              name: 'bar_a_key',
              schemaName: 'pub',
              tableName: 'bar',
              unique: true,
            },
            {
              columns: {
                b: 'ASC',
                a: 'ASC',
              },
              name: 'bar_b_a_key',
              schemaName: 'pub',
              tableName: 'bar',
              unique: true,
            },
            {
              columns: {b: 'ASC'},
              name: 'bar_b_key',
              schemaName: 'pub',
              tableName: 'bar',
              unique: true,
            },
          ],
        },
      },
    ],
    [
      'create index',
      `CREATE INDEX foo_name_index on pub.foo (name desc, id)`,
      {
        type: 'ddl',
        version: 1,
        event: {
          tag: 'CREATE INDEX',
          context: {
            query: `CREATE INDEX foo_name_index on pub.foo (name desc, id)`,
          },
          index: {
            columns: {
              name: 'DESC',
              id: 'ASC',
            },
            name: 'foo_name_index',
            schemaName: 'pub',
            tableName: 'foo',
            unique: false,
          },
        },
      },
    ],
    [
      'add column',
      `ALTER TABLE pub.foo ADD bar text`,
      {
        type: 'ddl',
        version: 1,
        event: {
          tag: 'ALTER TABLE',
          context: {query: 'ALTER TABLE pub.foo ADD bar text'},
          table: {
            schema: 'pub',
            name: 'foo',
            columns: {
              bar: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: false,
                dflt: null,
                pos: 4,
              },
              description: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: false,
                dflt: null,
                pos: 3,
              },
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: true,
                dflt: null,
                pos: 1,
              },
              name: {
                characterMaximumLength: null,
                dataType: 'text',
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
          },
          indexes: [
            {
              schemaName: 'pub',
              tableName: 'foo',
              name: 'foo_custom_index',
              columns: {
                description: 'ASC',
                name: 'ASC',
              },
              unique: false,
            },
            {
              schemaName: 'pub',
              tableName: 'foo',
              name: 'foo_name_key',
              columns: {name: 'ASC'},
              unique: true,
            },
          ],
        },
      },
    ],
    [
      'add column that results in a new index',
      `ALTER TABLE pub.foo ADD username TEXT UNIQUE`,
      {
        type: 'ddl',
        version: 1,
        event: {
          tag: 'ALTER TABLE',
          context: {query: 'ALTER TABLE pub.foo ADD username TEXT UNIQUE'},
          table: {
            schema: 'pub',
            name: 'foo',
            columns: {
              username: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: false,
                dflt: null,
                pos: 4,
              },
              description: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: false,
                dflt: null,
                pos: 3,
              },
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: true,
                dflt: null,
                pos: 1,
              },
              name: {
                characterMaximumLength: null,
                dataType: 'text',
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
          },
          indexes: [
            {
              schemaName: 'pub',
              tableName: 'foo',
              name: 'foo_custom_index',
              columns: {
                description: 'ASC',
                name: 'ASC',
              },
              unique: false,
            },
            {
              schemaName: 'pub',
              tableName: 'foo',
              name: 'foo_name_key',
              columns: {name: 'ASC'},
              unique: true,
            },
            {
              schemaName: 'pub',
              tableName: 'foo',
              name: 'foo_username_key',
              columns: {username: 'ASC'},
              unique: true,
            },
          ],
        },
      },
    ],
    [
      'add column with default value',
      `ALTER TABLE pub.foo ADD bar text DEFAULT 'boo'`,
      {
        type: 'ddl',
        version: 1,
        event: {
          tag: 'ALTER TABLE',
          context: {query: `ALTER TABLE pub.foo ADD bar text DEFAULT 'boo'`},
          table: {
            schema: 'pub',
            name: 'foo',
            columns: {
              bar: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: false,
                dflt: `'boo'::text`,
                pos: 4,
              },
              description: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: false,
                dflt: null,
                pos: 3,
              },
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: true,
                dflt: null,
                pos: 1,
              },
              name: {
                characterMaximumLength: null,
                dataType: 'text',
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
          },
          indexes: [
            {
              schemaName: 'pub',
              tableName: 'foo',
              name: 'foo_custom_index',
              columns: {
                description: 'ASC',
                name: 'ASC',
              },
              unique: false,
            },
            {
              schemaName: 'pub',
              tableName: 'foo',
              name: 'foo_name_key',
              columns: {name: 'ASC'},
              unique: true,
            },
          ],
        },
      },
    ],
    [
      'alter column default value',
      `ALTER TABLE pub.foo ALTER name SET DEFAULT 'alice'`,
      {
        type: 'ddl',
        version: 1,
        event: {
          tag: 'ALTER TABLE',
          context: {
            query: `ALTER TABLE pub.foo ALTER name SET DEFAULT 'alice'`,
          },
          table: {
            schema: 'pub',
            name: 'foo',
            columns: {
              description: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: false,
                dflt: null,
                pos: 3,
              },
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: true,
                dflt: null,
                pos: 1,
              },
              name: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: false,
                dflt: `'alice'::text`,
                pos: 2,
              },
            },
            primaryKey: ['id'],
            publications: {
              ['zero_all']: {rowFilter: null},
              ['zero_sum']: {rowFilter: null},
            },
          },
          indexes: [
            {
              schemaName: 'pub',
              tableName: 'foo',
              name: 'foo_custom_index',
              columns: {
                description: 'ASC',
                name: 'ASC',
              },
              unique: false,
            },
            {
              schemaName: 'pub',
              tableName: 'foo',
              name: 'foo_name_key',
              columns: {name: 'ASC'},
              unique: true,
            },
          ],
        },
      },
    ],
    [
      'rename column',
      `ALTER TABLE pub.foo RENAME name to handle`,
      {
        type: 'ddl',
        version: 1,
        event: {
          tag: 'ALTER TABLE',
          context: {query: 'ALTER TABLE pub.foo RENAME name to handle'},
          table: {
            schema: 'pub',
            name: 'foo',
            columns: {
              description: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: false,
                dflt: null,
                pos: 3,
              },
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: true,
                dflt: null,
                pos: 1,
              },
              handle: {
                characterMaximumLength: null,
                dataType: 'text',
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
          },
          indexes: [
            {
              schemaName: 'pub',
              tableName: 'foo',
              name: 'foo_custom_index',
              columns: {
                description: 'ASC',
                handle: 'ASC',
              },
              unique: false,
            },
            {
              schemaName: 'pub',
              tableName: 'foo',
              name: 'foo_name_key',
              columns: {handle: 'ASC'},
              unique: true,
            },
          ],
        },
      },
    ],
    [
      'drop column',
      `ALTER TABLE pub.foo drop description`,
      {
        type: 'ddl',
        version: 1,
        event: {
          tag: 'ALTER TABLE',
          context: {query: 'ALTER TABLE pub.foo drop description'},
          table: {
            schema: 'pub',
            name: 'foo',
            columns: {
              id: {
                characterMaximumLength: null,
                dataType: 'text',
                notNull: true,
                dflt: null,
                pos: 1,
              },
              name: {
                characterMaximumLength: null,
                dataType: 'text',
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
          },
          indexes: [
            // Note: foo_custom_index is dropped because it depended on the columns.
            {
              schemaName: 'pub',
              tableName: 'foo',
              name: 'foo_name_key',
              columns: {name: 'ASC'},
              unique: true,
            },
          ],
        },
      },
    ],
    [
      'drop table',
      `DROP TABLE pub.foo, pub.yoo`,
      {
        type: 'ddl',
        version: 1,
        event: {
          tag: 'DROP TABLE',
          context: {query: `DROP TABLE pub.foo, pub.yoo`},
          tables: [
            {
              schema: 'pub',
              objectIdentity: 'pub.yoo',
            },
            {
              schema: 'pub',
              objectIdentity: 'pub.foo',
            },
          ],
        },
      },
    ],

    [
      'drop index',
      `DROP INDEX pub.foo_custom_index, pub.yoo_custom_index`,
      {
        type: 'ddl',
        version: 1,
        event: {
          tag: 'DROP INDEX',
          context: {
            query: `DROP INDEX pub.foo_custom_index, pub.yoo_custom_index`,
          },
          indexes: [
            {
              schema: 'pub',
              objectIdentity: 'pub.yoo_custom_index',
            },
            {
              schema: 'pub',
              objectIdentity: 'pub.foo_custom_index',
            },
          ],
        },
      },
    ],
  ])('%s', async (name, query, event) => {
    await upstream.begin(async tx => {
      await tx`INSERT INTO pub.boo(id) VALUES('1')`;
      await tx.unsafe(query);
    });
    // In the subsequent transaction, perform the same dll operation
    // in the "private" schema.
    await upstream.begin(async tx => {
      await tx`INSERT INTO pub.boo(id) VALUES('2')`;

      // DROP TABLE and DROP INDEX will send all events regardless of whether
      // the tables were published, since we cannot determine if they were
      // published after they have been dropped. So only test
      // the "not-published" behavior for the other events.
      if (name !== 'drop table' && name !== 'drop index') {
        await tx.unsafe(query.replaceAll('pub.', 'private.'));
      }
    });

    const messages = await drainReplicationMessages(8);
    expect(messages.slice(0, 5)).toMatchObject([
      {tag: 'begin'},
      {tag: 'relation'},
      {tag: 'insert'},
      {
        tag: 'message',
        prefix: 'zero/' + SHARD_ID,
        content: expect.any(Uint8Array),
        flags: 1,
        transactional: true,
      },
      {tag: 'commit'},
    ]);

    const {content} = messages[3] as Pgoutput.MessageMessage;
    expect(JSON.parse(new TextDecoder().decode(content))).toEqual(event);

    // Depending on how busy Postgres is, the remaining messages will either
    // be:
    //
    // {tag: 'begin'},
    // {tag: 'insert'},
    // {tag: 'commit'},
    //
    // or:
    //
    // {tag: 'begin'},
    // {tag: 'relation'},
    // {tag: 'insert'},
    // {tag: 'commit'},
    //
    // the latter happening when Postgres loses some state and resends
    // the relation from messages[2]. What we want to verify is that
    // no `tag: 'message'` message arrives, as the schema changes
    // in the 'private' schema should not result in schema updates.
    expect(messages.slice(5).find(m => m.tag === 'message')).toBeUndefined();
  });

  test('postgres documentation: current_query() is unreliable', async () => {
    await upstream`CREATE PROCEDURE procedure_name()
       LANGUAGE SQL
       AS $$ ALTER TABLE pub.foo ADD bar text $$;`;

    await upstream`CALL procedure_name()`;
    await upstream.unsafe(
      `ALTER TABLE pub.foo ADD boo text; ALTER TABLE pub.foo DROP boo;`,
    );

    const messages = await drainReplicationMessages(7);
    expect(messages).toMatchObject([
      {tag: 'begin'},
      {
        tag: 'message',
        prefix: 'zero/' + SHARD_ID,
        content: expect.any(Uint8Array),
        flags: 1,
        transactional: true,
      },
      {tag: 'commit'},

      {tag: 'begin'},
      {
        tag: 'message',
        prefix: 'zero/' + SHARD_ID,
        content: expect.any(Uint8Array),
        flags: 1,
        transactional: true,
      },
      {
        tag: 'message',
        prefix: 'zero/' + SHARD_ID,
        content: expect.any(Uint8Array),
        flags: 1,
        transactional: true,
      },
      {tag: 'commit'},
    ]);

    let msg = messages[1] as Pgoutput.MessageMessage;
    expect(JSON.parse(new TextDecoder().decode(msg.content))).toMatchObject({
      type: 'ddl',
      version: 1,
      event: {
        tag: 'ALTER TABLE',
        // Top level query may not provide any information about the actual DDL command.
        context: {query: 'CALL procedure_name()'},
      },
    });

    msg = messages[4] as Pgoutput.MessageMessage;
    expect(JSON.parse(new TextDecoder().decode(msg.content))).toMatchObject({
      type: 'ddl',
      version: 1,
      event: {
        tag: 'ALTER TABLE',
        // A compound top level query may contain more than one DDL command.
        context: {
          query: `ALTER TABLE pub.foo ADD boo text; ALTER TABLE pub.foo DROP boo;`,
        },
      },
    });
    msg = messages[5] as Pgoutput.MessageMessage;
    expect(JSON.parse(new TextDecoder().decode(msg.content))).toMatchObject({
      type: 'ddl',
      version: 1,
      event: {
        tag: 'ALTER TABLE',
        // A compound top level query may contain more than one DDL command.
        context: {
          query: `ALTER TABLE pub.foo ADD boo text; ALTER TABLE pub.foo DROP boo;`,
        },
      },
    });
  });
});
