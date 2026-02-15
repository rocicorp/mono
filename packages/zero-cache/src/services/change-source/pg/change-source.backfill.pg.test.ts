import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test} from 'vitest';
import {
  BigIntJSON,
  type JSONValue,
} from '../../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../../shared/src/queue.ts';
import {randInt} from '../../../../../shared/src/rand.ts';
import {sleep} from '../../../../../shared/src/sleep.ts';
import type {Database} from '../../../../../zqlite/src/db.ts';
import {computeZqlSpecs} from '../../../db/lite-tables.ts';
import type {LiteTableSpec} from '../../../db/specs.ts';
import {getConnectionURI, testDBs} from '../../../test/db.ts';
import {DbFile, expectMatchingObjectsInTables} from '../../../test/lite.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {Source} from '../../../types/streams.ts';
import type {ChangeProcessor} from '../../replicator/change-processor.ts';
import {createChangeProcessor} from '../../replicator/test-utils.ts';
import type {DataOrSchemaChange} from '../protocol/current/data.ts';
import type {
  ChangeStreamData,
  ChangeStreamMessage,
} from '../protocol/current/downstream.ts';
import {initializePostgresChangeSource} from './change-source.ts';

const APP_ID = 'bf';
const PG_15_UP = 150000;
const PG_18_UP = 180000;

describe('change-source/pg/backfill-test', {timeout: 30000}, () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let pgVersion: number;
  let replicaDbFile: DbFile;
  let replica: Database;
  let changes: Source<ChangeStreamMessage>;
  let downstream: Queue<ChangeStreamMessage | 'timeout'>;
  let replicator: ChangeProcessor;

  beforeEach(async () => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('change_source_backfill_test_upstream');
    [{pgVersion}] =
      await upstream`SELECT current_setting('server_version_num')::int as "pgVersion"`;
    replicaDbFile = new DbFile('change_source_backfill_test_replica');
    replica = replicaDbFile.connect(lc);

    const upstreamURI = getConnectionURI(upstream);

    await upstream.unsafe(/*sql */ `

    -- Initially unpublished
    CREATE TABLE foo(
      id INT PRIMARY KEY,
      num INT,
      big TEXT,
      json JSON,
      jsons JSON[]
    );

    CREATE SCHEMA IF NOT EXISTS published;

    CREATE TABLE published.bar(
      id INT PRIMARY KEY,
      num INT
    );

    DO $$
    BEGIN
      FOR i IN 1..3 LOOP
        INSERT INTO published.bar (id, num) VALUES(i, i);
      END LOOP;
    END $$;

    CREATE TABLE baz(
      id INT PRIMARY KEY,
      num INT,
      ooka TEXT
    );

    DO $$
    BEGIN
      FOR i IN 1..3 LOOP
        INSERT INTO baz (id, num, ooka) VALUES(i, i, 'aaa');
      END LOOP;
    END $$;


    CREATE PUBLICATION published_schema FOR TABLES IN SCHEMA published;
    CREATE PUBLICATION published_tables FOR TABLE baz(id, num);  -- ooka is excluded
    `);

    const source = (
      await initializePostgresChangeSource(
        lc,
        upstreamURI,
        {
          appID: APP_ID,
          publications: ['published_schema', 'published_tables'],
          shardNum: 0,
        },
        replicaDbFile.path,
        {tableCopyWorkers: 5},
      )
    ).changeSource;
    const stream = await source.startStream('00');

    changes = stream.changes;
    downstream = drainToQueue(changes);
    replicator = createChangeProcessor(replica);

    return async () => {
      changes?.cancel();
      await testDBs.drop(upstream);
      replicaDbFile.delete();
    };
  }, 30000);

  function drainToQueue(
    sub: Source<ChangeStreamMessage>,
  ): Queue<ChangeStreamMessage | 'timeout'> {
    const queue = new Queue<ChangeStreamMessage | 'timeout'>();
    void (async () => {
      for await (const msg of sub) {
        queue.enqueue(msg);
      }
    })();
    return queue;
  }

  const BIG_TOASTABLE_VALUE = 'a'.repeat(1_000_000);

  async function nextTransaction(): Promise<DataOrSchemaChange[]> {
    const data: DataOrSchemaChange[] = [];
    for (;;) {
      const change = await downstream.dequeue('timeout', 5000);
      if (change === 'timeout') {
        throw new Error('timed out waiting for change');
      }
      const [type] = change;
      if (type !== 'control' && type !== 'status') {
        replicator.processMessage(
          lc,
          // To properly simulate the changes being passed over the wire,
          // serialize and parse the messages. This has the side effect of
          // removing `undefined` values from UPDATEs with omitted toasted
          // values.
          BigIntJSON.parse(BigIntJSON.stringify(change)) as ChangeStreamData,
        );
      }

      switch (type) {
        case 'begin':
          break;
        case 'data':
          data.push(change[1]);
          break;
        case 'commit':
        case 'rollback':
        case 'control':
        case 'status':
          return data;
        default:
          change satisfies never;
      }
    }
  }

  test.for([
    [
      'table added to published schema',
      PG_15_UP,
      /*sql*/ `
      DO $$
      BEGIN
        FOR i IN 1..3 LOOP
          INSERT INTO foo (id, num, big, json, jsons)
            VALUES(i, i, 
              REPEAT('a', 10), 
              json_build_object('d', i),  
              ARRAY[to_json(i), to_json(i+1), to_json((i+2)::text), json_build_object('e', i+3)]
            );
        END LOOP;
      END $$;

      ALTER TABLE foo SET SCHEMA published;
      `,
      null,
      {
        ['published.foo']: [
          {
            big: 'aaaaaaaaaa',
            id: 1n,
            json: '{"d" : 1}',
            jsons: '[1,2,"3",{"e":4}]',
            num: 1n,
          },
          {
            big: 'aaaaaaaaaa',
            id: 2n,
            json: '{"d" : 2}',
            jsons: '[2,3,"4",{"e":5}]',
            num: 2n,
          },
          {
            big: 'aaaaaaaaaa',
            id: 3n,
            json: '{"d" : 3}',
            jsons: '[3,4,"5",{"e":6}]',
            num: 3n,
          },
        ],
      },
      [
        {
          name: 'published.foo',
          columns: {
            id: {dataType: 'int4|NOT_NULL', pos: 1},
            num: {dataType: 'int4', pos: 2},
            big: {dataType: 'text', pos: 3},
            json: {dataType: 'json', pos: 4},
            jsons: {dataType: 'json[]|TEXT_ARRAY', pos: 5},
          },
        },
      ],
    ],
    [
      'table added to publication with row filter',
      PG_15_UP,
      /*sql*/ `
      DO $$
      BEGIN
        FOR i IN 1..6 LOOP
          INSERT INTO foo (id, num, big, json, jsons)
            VALUES(i, i, 
              REPEAT('a', 10), 
              json_build_object('d', i),  
              ARRAY[to_json(i), to_json(i+1), to_json((i+2)::text), json_build_object('e', i+3)]
            );
        END LOOP;
      END $$;

      ALTER PUBLICATION published_schema 
        ADD TABLE foo WHERE (id % 2 = 0);
      `,
      null,
      {
        ['foo']: [
          {
            big: 'aaaaaaaaaa',
            id: 2n,
            json: '{"d" : 2}',
            jsons: '[2,3,"4",{"e":5}]',
            num: 2n,
          },
          {
            big: 'aaaaaaaaaa',
            id: 4n,
            json: '{"d" : 4}',
            jsons: '[4,5,"6",{"e":7}]',
            num: 4n,
          },
          {
            big: 'aaaaaaaaaa',
            id: 6n,
            json: '{"d" : 6}',
            jsons: '[6,7,"8",{"e":9}]',
            num: 6n,
          },
        ],
      },
      [
        {
          name: 'foo',
          columns: {
            id: {dataType: 'int4|NOT_NULL', pos: 1},
            num: {dataType: 'int4', pos: 2},
            big: {dataType: 'text', pos: 3},
            json: {dataType: 'json', pos: 4},
            jsons: {dataType: 'json[]|TEXT_ARRAY', pos: 5},
          },
        },
      ],
    ],
    [
      'toasted values with updates during backfill',
      PG_15_UP,
      /*sql*/ `
      DO $$
      BEGIN
        FOR i IN 1..9 LOOP
          INSERT INTO foo (id, num, big)
            VALUES(i, i, REPEAT('a', 1000000));  -- BIG_TOASTABLE_VALUE
        END LOOP;
      END $$;

      ALTER TABLE foo SET SCHEMA published;
      `,
      // Then UPDATE non-toasted values during the backfill
      /*sql*/ `
      DO $$
      BEGIN
        FOR i IN 1..9 LOOP
          UPDATE published.foo SET num = i+1 WHERE id = i;
        END LOOP;
      END $$;
      `,
      {
        ['published.foo']: [
          {
            big: BIG_TOASTABLE_VALUE,
            id: 1n,
            num: 2n,
          },
          {
            big: BIG_TOASTABLE_VALUE,
            id: 2n,
            num: 3n,
          },
          {
            big: BIG_TOASTABLE_VALUE,
            id: 3n,
            num: 4n,
          },
          {
            big: BIG_TOASTABLE_VALUE,
            id: 4n,
            num: 5n,
          },
          {
            big: BIG_TOASTABLE_VALUE,
            id: 5n,
            num: 6n,
          },
          {
            big: BIG_TOASTABLE_VALUE,
            id: 6n,
            num: 7n,
          },
          {
            big: BIG_TOASTABLE_VALUE,
            id: 7n,
            num: 8n,
          },
          {
            big: BIG_TOASTABLE_VALUE,
            id: 8n,
            num: 9n,
          },
          {
            big: BIG_TOASTABLE_VALUE,
            id: 9n,
            num: 10n,
          },
        ],
      },
      [
        {
          name: 'published.foo',
          columns: {
            id: {dataType: 'int4|NOT_NULL', pos: 1},
            num: {dataType: 'int4', pos: 2},
            big: {dataType: 'text', pos: 3},
            json: {dataType: 'json', pos: 4},
            jsons: {dataType: 'json[]|TEXT_ARRAY', pos: 5},
          },
        },
      ],
    ],
    [
      'table added, then column and table renamed',
      PG_15_UP,
      /*sql*/ `
      DO $$
      BEGIN
        FOR i IN 1..3 LOOP
          INSERT INTO foo (id, num, big, json, jsons)
            VALUES(i, i, 
              REPEAT('a', 10), 
              json_build_object('d', i),  
              ARRAY[to_json(i), to_json(i+1), to_json((i+2)::text), json_build_object('e', i+3)]
            );
        END LOOP;
      END $$;

      -- Kick off the backfill
      ALTER TABLE foo SET SCHEMA published;
      `,
      // And then rename some of the backfilling stuff.
      /*sql*/ `
      ALTER TABLE published.foo RENAME big to bigz;
      ALTER TABLE published.foo RENAME to zoo;
      `,
      {
        ['published.zoo']: [
          {
            bigz: 'aaaaaaaaaa',
            id: 1n,
            json: '{"d" : 1}',
            jsons: '[1,2,"3",{"e":4}]',
            num: 1n,
          },
          {
            bigz: 'aaaaaaaaaa',
            id: 2n,
            json: '{"d" : 2}',
            jsons: '[2,3,"4",{"e":5}]',
            num: 2n,
          },
          {
            bigz: 'aaaaaaaaaa',
            id: 3n,
            json: '{"d" : 3}',
            jsons: '[3,4,"5",{"e":6}]',
            num: 3n,
          },
        ],
      },
      [
        {
          name: 'published.zoo',
          columns: {
            id: {dataType: 'int4|NOT_NULL', pos: 1},
            num: {dataType: 'int4', pos: 2},
            bigz: {dataType: 'text', pos: 3},
            json: {dataType: 'json', pos: 4},
            jsons: {dataType: 'json[]|TEXT_ARRAY', pos: 5},
          },
        },
      ],
    ],
    [
      'column with default expression added',
      PG_15_UP,
      /*sql*/ `
      ALTER TABLE published.bar ADD COLUMN expr INT DEFAULT (2 + 18) / 4;
      `,
      null,
      {
        ['published.bar']: [
          {
            id: 1n,
            num: 1n,
            expr: 5n,
          },
          {
            id: 2n,
            num: 2n,
            expr: 5n,
          },
          {
            id: 3n,
            num: 3n,
            expr: 5n,
          },
        ],
      },
      [
        {
          name: 'published.bar',
          columns: {
            id: {dataType: 'int4|NOT_NULL', pos: 1},
            num: {dataType: 'int4', pos: 2},
            expr: {dataType: 'int4', pos: 4},
          },
        },
      ],
    ],
    [
      'published column change',
      PG_15_UP,
      /*sql*/ `
      ALTER PUBLICATION published_tables SET TABLE baz (id, ooka);
      `,
      null,
      {
        ['baz']: [
          {
            id: 1n,
            ooka: 'aaa',
          },
          {
            id: 2n,
            ooka: 'aaa',
          },
          {
            id: 3n,
            ooka: 'aaa',
          },
        ],
      },
      [
        {
          name: 'baz',
          columns: {
            id: {dataType: 'int4|NOT_NULL', pos: 1},
            ooka: {dataType: 'text', pos: 3},
          },
        },
      ],
    ],
    [
      'column renamed while being backfilled',
      PG_15_UP,
      /*sql*/ `
      ALTER TABLE published.bar ADD COLUMN expr INT DEFAULT 3 * 8 + 21;
      `,
      /*sql*/ `
      ALTER TABLE published.bar RENAME expr TO expression;
      `,
      {
        ['published.bar']: [
          {
            id: 1n,
            num: 1n,
            expression: 45n,
          },
          {
            id: 2n,
            num: 2n,
            expression: 45n,
          },
          {
            id: 3n,
            num: 3n,
            expression: 45n,
          },
        ],
      },
      [
        {
          name: 'published.bar',
          columns: {
            id: {dataType: 'int4|NOT_NULL', pos: 1},
            num: {dataType: 'int4', pos: 2},
            expression: {dataType: 'int4', pos: 4},
          },
        },
      ],
    ],
    [
      'add generated column',
      PG_18_UP,
      /*sql*/ `
      ALTER PUBLICATION published_schema SET (publish_generated_columns=stored);

      ALTER TABLE published.bar ADD COLUMN "genNum2" INT 
        GENERATED ALWAYS AS (num * 2) STORED;
      `,
      null,
      {
        ['published.bar']: [
          {
            id: 1n,
            num: 1n,
            genNum2: 2n,
          },
          {
            id: 2n,
            num: 2n,
            genNum2: 4n,
          },
          {
            id: 3n,
            num: 3n,
            genNum2: 6n,
          },
        ],
      },
      [
        {
          name: 'published.bar',
          columns: {
            id: {dataType: 'int4|NOT_NULL', pos: 1},
            num: {dataType: 'int4', pos: 2},
            genNum2: {dataType: 'int4', pos: 4},
          },
        },
      ],
    ],
    [
      'empty table with no primary key',
      PG_15_UP,
      /*sql*/ `
      CREATE TABLE no_pk (id TEXT NOT NULL);

      ALTER TABLE no_pk SET SCHEMA published;
      `,
      null,
      {
        ['published.no_pk']: [],
      },
      [
        // The table will not show up in the synced tables because
        // it has no primary key.
      ],
    ],
    [
      'non-empty table that eventually gets a primary key',
      PG_15_UP,
      /*sql*/ `
      CREATE TABLE late_pk (id TEXT NOT NULL);
      INSERT INTO late_pk (id) VALUES ('foo'), ('bar');

      ALTER TABLE late_pk SET SCHEMA published;
      `,
      /*sql*/ `
      ALTER TABLE published.late_pk ADD PRIMARY KEY (id);
      `,
      {
        ['published.late_pk']: [{id: 'foo'}, {id: 'bar'}],
      },
      [
        {
          name: 'published.late_pk',
          columns: {
            id: {dataType: 'text|NOT_NULL', pos: 1},
          },
        },
      ],
    ],
  ] satisfies [
    name: string,
    minPgVersion: number,
    statements: string,
    statementsAfterDelay: string | null,
    expectedData: Record<string, JSONValue>,
    expectedLiteTables: LiteTableSpec[],
  ][])(
    '%s',
    async (
      [
        _name,
        minPgVersion,
        stmts,
        stmtsAfterDelay,
        expectedData,
        expectedTables,
      ],
      {skip},
    ) => {
      if (pgVersion < minPgVersion) {
        skip();
      }
      await upstream.unsafe(stmts);
      const [{lsn}] = await upstream`SELECT pg_current_wal_lsn() as lsn`;
      lc.debug?.(`Executed initial statements: ${lsn}`);

      if (stmtsAfterDelay) {
        await sleep(randInt(1, 5));
        await upstream.unsafe(stmtsAfterDelay);
        const [{lsn}] = await upstream`SELECT pg_current_wal_lsn() as lsn`;
        lc.debug?.(`Executed subsequent statements: ${lsn}`);
      }

      for (let i = 0; i < 2000; i++) {
        const transaction = await nextTransaction();
        if (transaction.at(-1)?.tag === 'backfill-completed') {
          break;
        }
      }

      expectMatchingObjectsInTables(replica, expectedData, 'bigint');

      const tables = computeZqlSpecs(lc, replica, {
        includeBackfillingColumns: false,
      });
      for (const t of expectedTables) {
        const found = tables.get(t.name);
        expect(found?.tableSpec).toMatchObject(t);
      }
    },
  );
});
