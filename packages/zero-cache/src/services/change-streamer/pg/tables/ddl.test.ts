import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {Queue} from 'shared/src/queue.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {
  dropReplicationSlot,
  getConnectionURI,
  testDBs,
} from 'zero-cache/src/test/db.js';
import type {PostgresDB} from 'zero-cache/src/types/pg.js';
import {createEventTriggerStatements} from './ddl.js';

const SLOT_NAME = 'ddl_test_slot';

describe('change-source/tables/ddl', () => {
  let upstream: PostgresDB;
  let messages: Queue<Pgoutput.Message>;
  let service: LogicalReplicationService;

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
    CREATE TABLE private.foo(id TEXT PRIMARY KEY, name TEXT UNIQUE, description TEXT);
    
    CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA pub;
    `);

    await upstream.unsafe(createEventTriggerStatements());

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

    // Hack for setting the `messages 'true'` plugin option until
    // pg-logical-replication supports it.
    // (Pending review: https://github.com/kibae/pg-logical-replication/pull/36)
    const MESSAGES_TRUE_OPTION_HACK = `', messages 'true`;

    void service.subscribe(
      new PgoutputPlugin({
        protoVersion: 1,
        publicationNames: [`zero_all${MESSAGES_TRUE_OPTION_HACK}`],
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
        msg: {
          tag: 'CREATE TABLE',
          query: `CREATE TABLE pub.bar(id TEXT PRIMARY KEY, a INT4 UNIQUE, b INT8 UNIQUE, UNIQUE(b, a))`,
          tables: [
            {
              schema: 'pub',
              name: 'bar',
              columns: {
                id: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  notNull: true,
                  pos: 1,
                },
                a: {
                  characterMaximumLength: null,
                  dataType: 'int4',
                  notNull: false,
                  pos: 2,
                },
                b: {
                  characterMaximumLength: null,
                  dataType: 'int8',
                  notNull: false,
                  pos: 3,
                },
              },
              primaryKey: ['id'],
              publications: {['zero_all']: {rowFilter: null}},
            },
          ],
          indexes: [
            {
              columns: ['a'],
              name: 'bar_a_key',
              schemaName: 'pub',
              tableName: 'bar',
              unique: true,
            },
            {
              columns: ['b', 'a'],
              name: 'bar_b_a_key',
              schemaName: 'pub',
              tableName: 'bar',
              unique: true,
            },
            {
              columns: ['b'],
              name: 'bar_b_key',
              schemaName: 'pub',
              tableName: 'bar',
              unique: true,
            },
          ],
          renamedColumns: [],
        },
      },
    ],
    [
      'create index',
      `CREATE INDEX foo_name_index on pub.foo (name, id)`,
      {
        type: 'ddl',
        msg: {
          tag: 'CREATE INDEX',
          query: `CREATE INDEX foo_name_index on pub.foo (name, id)`,
          tables: [],
          indexes: [
            {
              columns: ['name', 'id'],
              name: 'foo_name_index',
              schemaName: 'pub',
              tableName: 'foo',
              unique: false,
            },
          ],
          renamedColumns: [],
        },
      },
    ],
    [
      'add column',
      `ALTER TABLE pub.foo ADD bar text`,
      {
        type: 'ddl',
        msg: {
          tag: 'ALTER TABLE',
          query: 'ALTER TABLE pub.foo ADD bar text',
          tables: [
            {
              schema: 'pub',
              name: 'foo',
              columns: {
                bar: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  notNull: false,
                  pos: 4,
                },
                description: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  notNull: false,
                  pos: 3,
                },
                id: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  notNull: true,
                  pos: 1,
                },
                name: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  notNull: false,
                  pos: 2,
                },
              },
              primaryKey: ['id'],
              publications: {['zero_all']: {rowFilter: null}},
            },
          ],
          indexes: [],
          renamedColumns: [],
        },
      },
    ],
    [
      'rename column',
      `ALTER TABLE pub.foo RENAME name to handle`,
      {
        type: 'ddl',
        msg: {
          tag: 'ALTER TABLE',
          query: 'ALTER TABLE pub.foo RENAME name to handle',
          tables: [
            {
              schema: 'pub',
              name: 'foo',
              columns: {
                description: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  notNull: false,
                  pos: 3,
                },
                id: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  notNull: true,
                  pos: 1,
                },
                handle: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  notNull: false,
                  pos: 2,
                },
              },
              primaryKey: ['id'],
              publications: {['zero_all']: {rowFilter: null}},
            },
          ],
          indexes: [],
          renamedColumns: [2],
        },
      },
    ],
    [
      'drop column',
      `ALTER TABLE pub.foo drop description`,
      {
        type: 'ddl',
        msg: {
          tag: 'ALTER TABLE',
          query: 'ALTER TABLE pub.foo drop description',
          tables: [
            {
              schema: 'pub',
              name: 'foo',
              columns: {
                id: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  notNull: true,
                  pos: 1,
                },
                name: {
                  characterMaximumLength: null,
                  dataType: 'text',
                  notNull: false,
                  pos: 2,
                },
              },
              primaryKey: ['id'],
              publications: {['zero_all']: {rowFilter: null}},
            },
          ],
          indexes: [],
          renamedColumns: [],
        },
      },
    ],
  ])('%s', async (_name, query, event) => {
    await upstream.begin(async tx => {
      await tx`INSERT INTO pub.boo(id) VALUES('1')`;
      await tx.unsafe(query);
    });
    // In the subsequent transaction, perform the same dll operation
    // in the "private" schema.
    await upstream.begin(async tx => {
      await tx`INSERT INTO pub.boo(id) VALUES('2')`;
      await tx.unsafe(query.replaceAll('pub.', 'private.'));
    });

    const messages = await drainReplicationMessages(8);
    expect(messages).toMatchObject([
      {tag: 'begin'},
      {tag: 'relation'},
      {tag: 'insert'},
      {
        tag: 'message',
        prefix: 'zero',
        content: expect.any(Uint8Array),
        flags: 1,
        transactional: true,
      },
      {tag: 'commit'},

      // There should be no "zero" message emitted in the second transaction
      {tag: 'begin'},
      {tag: 'insert'},
      {tag: 'commit'},
    ]);

    const {content} = messages[3] as Pgoutput.MessageMessage;
    expect(JSON.parse(new TextDecoder().decode(content))).toEqual(event);
  });
});
