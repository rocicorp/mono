import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Subscription} from '../../types/subscription.ts';
import {type Commit} from '../change-source/protocol/current/downstream.ts';
import type {UpstreamStatusMessage} from '../change-source/protocol/current/status.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {type Downstream} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';
import {ensureReplicationConfig, setupCDCTables} from './schema/tables.ts';
import {Storer} from './storer.ts';
import {createSubscriber} from './test-utils.ts';

describe('change-streamer/storer', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;
  let storer: Storer;
  let done: Promise<void>;
  let consumed: Queue<Commit | UpstreamStatusMessage>;
  let fatalErrors: Queue<Error>;
  let shard: {appID: string; shardNum: number};

  const REPLICA_VERSION = '00';
  const APP_ID = 'xero';
  const SHARD_NUM = 5;

  beforeEach<PgTest>(async ({testDBs}) => {
    db = await testDBs.create('change_streamer_storer', undefined, {
      sendStringAsJson: true,
    });
    shard = {appID: APP_ID, shardNum: SHARD_NUM};
    await db.begin(tx => setupCDCTables(lc, tx, shard));
    await ensureReplicationConfig(
      lc,
      db,
      {
        replicaVersion: REPLICA_VERSION,
        publications: [],
        watermark: REPLICA_VERSION,
      },
      shard,
      true,
    );
    await db.begin(async tx => {
      await Promise.all(
        [
          {watermark: '03', pos: 0, change: {tag: 'begin', foo: 'bar'}},
          {watermark: '03', pos: 1, change: {tag: 'insert'}},
          {watermark: '03', pos: 2, change: {tag: 'commit', bar: 'baz'}},
          {watermark: '06', pos: 0, change: {tag: 'begin', boo: 'dar'}},
          {watermark: '06', pos: 1, change: {tag: 'update'}},
          {watermark: '06', pos: 2, change: {tag: 'commit', boo: 'far'}},
        ].map(row => tx`INSERT INTO "xero_5/cdc"."changeLog" ${tx(row)}`),
      );
      await tx`UPDATE "xero_5/cdc"."replicationState" SET "lastWatermark" = '06'`;
    });
    consumed = new Queue();
    fatalErrors = new Queue();

    return async () => {
      await testDBs.drop(db);
      void storer.stop();
      await done;
    };
  });

  async function expectConsumed(...watermarks: string[]) {
    for (const watermark of watermarks) {
      expect((await consumed.dequeue())[2].watermark).toBe(watermark);
    }
  }

  const messages = new ReplicationMessages({issues: 'id'});

  async function drain(sub: Subscription<Downstream>, untilWatermark?: string) {
    const msgs: Downstream[] = [];
    for await (const msg of sub) {
      msgs.push(msg);
      if (msg[0] === 'commit' && msg[2].watermark === untilWatermark) {
        break;
      }
    }
    return msgs;
  }

  describe('protocol: ws', () => {
    beforeEach(async () => {
      storer = new Storer(
        lc,
        shard,
        'task-id',
        'change-streamer:12345',
        'ws',
        db,
        REPLICA_VERSION,
        msg => consumed.enqueue(msg),
        err => fatalErrors.enqueue(err),
        0.04,
      );
      await storer.assumeOwnership();
      done = storer.run();
    });

    test('ownerAddress is set correctly', async () => {
      expect(
        await db`SELECT "ownerAddress" FROM "xero_5/cdc"."replicationState" WHERE owner = 'task-id'`,
      ).toEqual([{ownerAddress: 'change-streamer:12345'}]);
    });

    test('purge', async () => {
      expect(await storer.purgeRecordsBefore('02')).toBe(0);
      expect(
        await db`SELECT watermark, pos FROM "xero_5/cdc"."changeLog"`,
      ).toEqual([
        {watermark: '03', pos: 0n},
        {watermark: '03', pos: 1n},
        {watermark: '03', pos: 2n},
        {watermark: '06', pos: 0n},
        {watermark: '06', pos: 1n},
        {watermark: '06', pos: 2n},
      ]);

      expect(await storer.purgeRecordsBefore('03')).toBe(0);
      expect(
        await db`SELECT watermark, pos FROM "xero_5/cdc"."changeLog"`,
      ).toEqual([
        {watermark: '03', pos: 0n},
        {watermark: '03', pos: 1n},
        {watermark: '03', pos: 2n},
        {watermark: '06', pos: 0n},
        {watermark: '06', pos: 1n},
        {watermark: '06', pos: 2n},
      ]);

      // TODO: Consider rejecting as an invalid watermark?
      expect(await storer.purgeRecordsBefore('04')).toBe(3);
      expect(
        await db`SELECT watermark, pos FROM "xero_5/cdc"."changeLog"`,
      ).toEqual([
        {watermark: '06', pos: 0n},
        {watermark: '06', pos: 1n},
        {watermark: '06', pos: 2n},
      ]);

      expect(await storer.purgeRecordsBefore('06')).toBe(0);
      expect(
        await db`SELECT watermark, pos FROM "xero_5/cdc"."changeLog"`,
      ).toEqual([
        {watermark: '06', pos: 0n},
        {watermark: '06', pos: 1n},
        {watermark: '06', pos: 2n},
      ]);
    });

    test('backfill metadata tracking', async () => {
      expect(
        await storer.getStartStreamInitializationParameters(),
      ).toMatchObject({lastWatermark: '06', backfillRequests: []});

      storer.store([
        '08',
        ['begin', messages.begin(), {commitWatermark: '08'}],
      ]);
      storer.store(['08', ['commit', messages.commit(), {watermark: '08'}]]);

      expect(
        await storer.getStartStreamInitializationParameters(),
      ).toMatchObject({lastWatermark: '08', backfillRequests: []});

      // Add table metadata. This should be stored, but not returned in
      // initialization parameters without any backfill data.
      storer.store([
        '09',
        ['begin', messages.begin(), {commitWatermark: '09'}],
      ]);
      storer.store([
        '09',
        [
          'data',
          {
            tag: 'create-table',
            spec: {
              schema: 'my',
              name: 'foo',
              columns: {},
            },
            metadata: {
              rowKey: {type: 'index', columns: ['a', 'b']},
            },
          },
        ],
      ]);
      storer.store(['09', ['commit', messages.commit(), {watermark: '09'}]]);

      // No backfillRequests should be present.
      expect(await storer.getStartStreamInitializationParameters())
        .toMatchInlineSnapshot(`
        {
          "backfillRequests": Result [],
          "lastWatermark": "09",
        }
      `);

      // Add a different table with backfill metadata only.
      storer.store([
        '0a',
        ['begin', messages.begin(), {commitWatermark: '0a'}],
      ]);
      storer.store([
        '0a',
        [
          'data',
          {
            tag: 'create-table',
            spec: {
              schema: 'your',
              name: 'bar',
              columns: {},
            },
            backfill: {
              a: {fooID: 987, barID: 'zoo'},
              b: {fooID: 843, barID: 'ozz'},
            },
          },
        ],
      ]);
      storer.store(['0a', ['commit', messages.commit(), {watermark: '0a'}]]);

      // The table should appear in the backfillRequests, with null metadata
      // since none was ever specified.
      expect(await storer.getStartStreamInitializationParameters())
        .toMatchInlineSnapshot(`
          {
            "backfillRequests": Result [
              {
                "columns": {
                  "a": {
                    "barID": "zoo",
                    "fooID": 987,
                  },
                  "b": {
                    "barID": "ozz",
                    "fooID": 843,
                  },
                },
                "table": {
                  "metadata": null,
                  "name": "bar",
                  "schema": "your",
                },
              },
            ],
            "lastWatermark": "0a",
          }
        `);

      // Add a column to the original table backfill metadata.
      storer.store([
        '0b',
        ['begin', messages.begin(), {commitWatermark: '0a'}],
      ]);
      storer.store([
        '0b',
        [
          'data',
          {
            tag: 'add-column',
            table: {
              schema: 'my',
              name: 'foo',
            },
            column: {name: 'c', spec: {pos: 3, dataType: 'text'}},
            backfill: {fooID: 123, barID: 'baz'},
          },
        ],
      ]);
      storer.store(['0b', ['commit', messages.commit(), {watermark: '0b'}]]);

      // Now the original table shows up in the backfillRequests, with its
      // table metadata.
      expect(await storer.getStartStreamInitializationParameters())
        .toMatchInlineSnapshot(`
          {
            "backfillRequests": Result [
              {
                "columns": {
                  "a": {
                    "barID": "zoo",
                    "fooID": 987,
                  },
                  "b": {
                    "barID": "ozz",
                    "fooID": 843,
                  },
                },
                "table": {
                  "metadata": null,
                  "name": "bar",
                  "schema": "your",
                },
              },
              {
                "columns": {
                  "c": {
                    "barID": "baz",
                    "fooID": 123,
                  },
                },
                "table": {
                  "metadata": {
                    "rowKey": {
                      "columns": [
                        "a",
                        "b",
                      ],
                      "type": "index",
                    },
                  },
                  "name": "foo",
                  "schema": "my",
                },
              },
            ],
            "lastWatermark": "0b",
          }
        `);

      // Add another column to the same table with new table metadata.
      storer.store([
        '0c',
        ['begin', messages.begin(), {commitWatermark: '0b'}],
      ]);
      storer.store([
        '0c',
        [
          'data',
          {
            tag: 'add-column',
            table: {
              schema: 'my',
              name: 'foo',
            },
            tableMetadata: {
              rowKey: {type: 'default', columns: ['b']},
            },
            column: {name: 'd', spec: {pos: 4, dataType: 'text'}},
            backfill: {fooID: 456, barID: 'boo'},
          },
        ],
      ]);
      storer.store(['0c', ['commit', messages.commit(), {watermark: '0c'}]]);

      expect(await storer.getStartStreamInitializationParameters())
        .toMatchInlineSnapshot(`
          {
            "backfillRequests": Result [
              {
                "columns": {
                  "c": {
                    "barID": "baz",
                    "fooID": 123,
                  },
                  "d": {
                    "barID": "boo",
                    "fooID": 456,
                  },
                },
                "table": {
                  "metadata": {
                    "rowKey": {
                      "columns": [
                        "b",
                      ],
                      "type": "default",
                    },
                  },
                  "name": "foo",
                  "schema": "my",
                },
              },
              {
                "columns": {
                  "a": {
                    "barID": "zoo",
                    "fooID": 987,
                  },
                  "b": {
                    "barID": "ozz",
                    "fooID": 843,
                  },
                },
                "table": {
                  "metadata": null,
                  "name": "bar",
                  "schema": "your",
                },
              },
            ],
            "lastWatermark": "0c",
          }
        `);

      // Update the table metadata of the new table.
      storer.store([
        '0d',
        ['begin', messages.begin(), {commitWatermark: '0c'}],
      ]);
      storer.store([
        '0d',
        [
          'data',
          {
            tag: 'update-table-metadata',
            table: {
              schema: 'your',
              name: 'bar',
            },
            old: {
              rowKey: {type: 'full', columns: ['a', 'b']},
            },
            new: {
              rowKey: {type: 'default', columns: ['a']},
            },
          },
        ],
      ]);
      storer.store(['0d', ['commit', messages.commit(), {watermark: '0d'}]]);

      expect(await storer.getStartStreamInitializationParameters())
        .toMatchInlineSnapshot(`
          {
            "backfillRequests": Result [
              {
                "columns": {
                  "c": {
                    "barID": "baz",
                    "fooID": 123,
                  },
                  "d": {
                    "barID": "boo",
                    "fooID": 456,
                  },
                },
                "table": {
                  "metadata": {
                    "rowKey": {
                      "columns": [
                        "b",
                      ],
                      "type": "default",
                    },
                  },
                  "name": "foo",
                  "schema": "my",
                },
              },
              {
                "columns": {
                  "a": {
                    "barID": "zoo",
                    "fooID": 987,
                  },
                  "b": {
                    "barID": "ozz",
                    "fooID": 843,
                  },
                },
                "table": {
                  "metadata": {
                    "rowKey": {
                      "columns": [
                        "a",
                      ],
                      "type": "default",
                    },
                  },
                  "name": "bar",
                  "schema": "your",
                },
              },
            ],
            "lastWatermark": "0d",
          }
        `);

      // Rename one of the backfilling columns
      storer.store([
        '0e',
        ['begin', messages.begin(), {commitWatermark: '0e'}],
      ]);
      storer.store([
        '0e',
        [
          'data',
          {
            tag: 'update-column',
            table: {
              schema: 'your',
              name: 'bar',
            },
            old: {
              name: 'b',
              spec: {pos: 2, dataType: 'text'},
            },
            new: {
              name: 'newName',
              spec: {pos: 2, dataType: 'text'},
            },
          },
        ],
      ]);
      storer.store(['0e', ['commit', messages.commit(), {watermark: '0e'}]]);

      expect(await storer.getStartStreamInitializationParameters())
        .toMatchInlineSnapshot(`
          {
            "backfillRequests": Result [
              {
                "columns": {
                  "c": {
                    "barID": "baz",
                    "fooID": 123,
                  },
                  "d": {
                    "barID": "boo",
                    "fooID": 456,
                  },
                },
                "table": {
                  "metadata": {
                    "rowKey": {
                      "columns": [
                        "b",
                      ],
                      "type": "default",
                    },
                  },
                  "name": "foo",
                  "schema": "my",
                },
              },
              {
                "columns": {
                  "a": {
                    "barID": "zoo",
                    "fooID": 987,
                  },
                  "newName": {
                    "barID": "ozz",
                    "fooID": 843,
                  },
                },
                "table": {
                  "metadata": {
                    "rowKey": {
                      "columns": [
                        "a",
                      ],
                      "type": "default",
                    },
                  },
                  "name": "bar",
                  "schema": "your",
                },
              },
            ],
            "lastWatermark": "0e",
          }
        `);

      // Drop a backfilling column.
      storer.store([
        '0f',
        ['begin', messages.begin(), {commitWatermark: '0f'}],
      ]);
      storer.store([
        '0f',
        [
          'data',
          {
            tag: 'drop-column',
            table: {
              schema: 'your',
              name: 'bar',
            },
            column: 'newName',
          },
        ],
      ]);
      storer.store(['0f', ['commit', messages.commit(), {watermark: '0f'}]]);

      expect(await storer.getStartStreamInitializationParameters())
        .toMatchInlineSnapshot(`
          {
            "backfillRequests": Result [
              {
                "columns": {
                  "c": {
                    "barID": "baz",
                    "fooID": 123,
                  },
                  "d": {
                    "barID": "boo",
                    "fooID": 456,
                  },
                },
                "table": {
                  "metadata": {
                    "rowKey": {
                      "columns": [
                        "b",
                      ],
                      "type": "default",
                    },
                  },
                  "name": "foo",
                  "schema": "my",
                },
              },
              {
                "columns": {
                  "a": {
                    "barID": "zoo",
                    "fooID": 987,
                  },
                },
                "table": {
                  "metadata": {
                    "rowKey": {
                      "columns": [
                        "a",
                      ],
                      "type": "default",
                    },
                  },
                  "name": "bar",
                  "schema": "your",
                },
              },
            ],
            "lastWatermark": "0f",
          }
        `);

      // Set the other backfilling column to completed
      storer.store([
        '110',
        ['begin', messages.begin(), {commitWatermark: '110'}],
      ]);
      storer.store([
        '110',
        [
          'data',
          {
            tag: 'backfill-completed',
            relation: {
              schema: 'your',
              name: 'bar',
              rowKey: {columns: ['a']},
            },
            columns: [],
            watermark: '0f',
          },
        ],
      ]);
      storer.store(['110', ['commit', messages.commit(), {watermark: '110'}]]);

      expect(await storer.getStartStreamInitializationParameters())
        .toMatchInlineSnapshot(`
          {
            "backfillRequests": Result [
              {
                "columns": {
                  "c": {
                    "barID": "baz",
                    "fooID": 123,
                  },
                  "d": {
                    "barID": "boo",
                    "fooID": 456,
                  },
                },
                "table": {
                  "metadata": {
                    "rowKey": {
                      "columns": [
                        "b",
                      ],
                      "type": "default",
                    },
                  },
                  "name": "foo",
                  "schema": "my",
                },
              },
            ],
            "lastWatermark": "110",
          }
        `);

      // Rename the backfilling table, and a contained column in the same tx.
      storer.store([
        '111',
        ['begin', messages.begin(), {commitWatermark: '111'}],
      ]);
      storer.store([
        '111',
        [
          'data',
          {
            tag: 'rename-table',
            old: {schema: 'my', name: 'foo'},
            new: {schema: 'your', name: 'bloo'},
          },
        ],
      ]);
      storer.store([
        '111',
        [
          'data',
          {
            tag: 'update-column',
            table: {
              schema: 'your',
              name: 'bloo',
            },
            old: {
              name: 'd',
              spec: {pos: 2, dataType: 'text'},
            },
            new: {
              name: 'deez',
              spec: {pos: 2, dataType: 'text'},
            },
          },
        ],
      ]);
      storer.store(['111', ['commit', messages.commit(), {watermark: '111'}]]);

      expect(await storer.getStartStreamInitializationParameters())
        .toMatchInlineSnapshot(`
          {
            "backfillRequests": Result [
              {
                "columns": {
                  "c": {
                    "barID": "baz",
                    "fooID": 123,
                  },
                  "deez": {
                    "barID": "boo",
                    "fooID": 456,
                  },
                },
                "table": {
                  "metadata": {
                    "rowKey": {
                      "columns": [
                        "b",
                      ],
                      "type": "default",
                    },
                  },
                  "name": "bloo",
                  "schema": "your",
                },
              },
            ],
            "lastWatermark": "111",
          }
        `);

      // Drop the backfilling table
      storer.store([
        '112',
        ['begin', messages.begin(), {commitWatermark: '112'}],
      ]);
      storer.store([
        '112',
        [
          'data',
          {
            tag: 'drop-table',
            id: {schema: 'your', name: 'bloo'},
          },
        ],
      ]);
      storer.store(['112', ['commit', messages.commit(), {watermark: '112'}]]);

      expect(await storer.getStartStreamInitializationParameters())
        .toMatchInlineSnapshot(`
        {
          "backfillRequests": Result [],
          "lastWatermark": "112",
        }
      `);
    });

    test('non-owner purge prevented', async () => {
      await db`UPDATE "xero_5/cdc"."replicationState" SET owner = 'different-task-id'`;

      expect(await storer.purgeRecordsBefore('06')).toBe(0);
      expect(
        await db`SELECT watermark, pos FROM "xero_5/cdc"."changeLog"`,
      ).toEqual([
        {watermark: '03', pos: 0n},
        {watermark: '03', pos: 1n},
        {watermark: '03', pos: 2n},
        {watermark: '06', pos: 0n},
        {watermark: '06', pos: 1n},
        {watermark: '06', pos: 2n},
      ]);
    });

    test('ownership change detected at begin aborts transaction', async () => {
      // Change ownership before storing begin — the storer's pipelined
      // SELECT will read the new owner immediately.
      await db`UPDATE "xero_5/cdc"."replicationState" SET owner = 'other-task'`;

      storer.store([
        '07',
        ['begin', messages.begin(), {commitWatermark: '08'}],
      ]);
      storer.store(['07', ['data', messages.insert('issues', {id: 'foo'})]]);
      storer.store(['08', ['commit', messages.commit(), {watermark: '08'}]]);

      await expect(done).rejects.toThrow(
        'changeLog ownership has been assumed by other-task',
      );
    });

    test('ownership change during transaction includes new owner in error', async () => {
      // Start a transaction — this begins a SERIALIZABLE tx that
      // reads replicationState (owner = 'task-id').
      storer.store([
        '07',
        ['begin', messages.begin(), {commitWatermark: '08'}],
      ]);
      storer.store(['07', ['data', messages.insert('issues', {id: 'foo'})]]);

      // Wait for the storer to process 'begin' and start the SERIALIZABLE tx.
      // The pipelined SELECT of replicationState should have executed by now.
      await sleep(100);

      // Change ownership externally — this commits immediately on a separate
      // connection, modifying the replicationState row that the storer's
      // SERIALIZABLE tx has already read.
      await db`UPDATE "xero_5/cdc"."replicationState" SET owner = 'new-owner-task'`;

      // Now send commit. The storer's initial read shows owner = 'task-id'
      // (matching), so it proceeds to UPDATE lastWatermark. But Postgres
      // detects the serialization conflict and throws PG_SERIALIZATION_FAILURE.
      storer.store(['08', ['commit', messages.commit(), {watermark: '08'}]]);

      // The AbortError should include the new owner read from a fresh query.
      await expect(done).rejects.toThrow(
        'changeLog ownership was concurrently assumed by new-owner-task (serialization failure)',
      );
    });

    test('ownership change error message when reading owner fails', async () => {
      // Create a proxy around db that can be made to reject queries.
      let failQueries = false;
      const proxyDb = new Proxy(db, {
        apply(target, thisArg, args) {
          if (failQueries) {
            throw new Error('connection terminated');
          }
          return Reflect.apply(target, thisArg, args);
        },
      });

      // Stop the existing storer before creating a new one with the proxy db.
      void storer.stop();
      await done;

      const proxyStorer = new Storer(
        lc,
        shard,
        'task-id',
        'change-streamer:12345',
        'ws',
        proxyDb,
        REPLICA_VERSION,
        msg => consumed.enqueue(msg),
        err => fatalErrors.enqueue(err),
        0.04,
      );
      await proxyStorer.assumeOwnership();
      const proxyDone = proxyStorer.run();

      proxyStorer.store([
        '07',
        ['begin', messages.begin(), {commitWatermark: '08'}],
      ]);
      proxyStorer.store([
        '07',
        ['data', messages.insert('issues', {id: 'foo'})],
      ]);

      await sleep(100);

      // Change ownership to cause the serialization failure.
      await db`UPDATE "xero_5/cdc"."replicationState" SET owner = 'new-owner-task'`;

      // Now make the proxy db fail so the fresh read of the owner errors.
      failQueries = true;

      proxyStorer.store([
        '08',
        ['commit', messages.commit(), {watermark: '08'}],
      ]);

      await expect(proxyDone).rejects.toThrow(
        'changeLog ownership was concurrently changed (failed to read current owner) (serialization failure)',
      );
    });

    test('abort', async () => {
      storer.store([
        '0b',
        ['begin', messages.begin(), {commitWatermark: '0b'}],
      ]);
      storer.store(['0b', ['data', messages.insert('issues', {id: 'foo'})]]);
      storer.abort();

      storer.store([
        '0a',
        ['begin', messages.begin(), {commitWatermark: '0a'}],
      ]);
      storer.store(['0a', ['data', messages.insert('issues', {id: 'bar'})]]);
      storer.store(['0a', ['commit', messages.commit(), {watermark: '0a'}]]);

      await expectConsumed('0a');

      expect(
        await db`
      SELECT watermark, pos, change FROM "xero_5/cdc"."changeLog"
        WHERE watermark >= '0a'`,
      ).toMatchObject([
        {
          change: {tag: 'begin'},
          pos: 0n,
          watermark: '0a',
        },
        {
          change: {
            tag: 'insert',
            new: {id: 'bar'},
          },
          pos: 1n,
          watermark: '0a',
        },
        {
          change: {tag: 'commit'},
          pos: 2n,
          watermark: '0a',
        },
      ]);
    });

    test('no queueing if not in transaction', async () => {
      const [sub, _, stream] = createSubscriber('00');

      // This should be buffered until catchup is complete.
      void sub.send([
        '07',
        ['begin', messages.begin(), {commitWatermark: '08'}],
      ]);
      void sub.send(['08', ['commit', messages.commit(), {watermark: '08'}]]);

      // Catchup should start immediately since there are no txes in progress.
      storer.catchup(sub, 'backup');

      expect(await drain(stream, '08')).toMatchInlineSnapshot(`
      [
        [
          "status",
          {
            "tag": "status",
          },
        ],
        [
          "begin",
          {
            "foo": "bar",
            "tag": "begin",
          },
          {
            "commitWatermark": "03",
          },
        ],
        [
          "data",
          {
            "tag": "insert",
          },
        ],
        [
          "commit",
          {
            "bar": "baz",
            "tag": "commit",
          },
          {
            "watermark": "03",
          },
        ],
        [
          "begin",
          {
            "boo": "dar",
            "tag": "begin",
          },
          {
            "commitWatermark": "06",
          },
        ],
        [
          "data",
          {
            "tag": "update",
          },
        ],
        [
          "commit",
          {
            "boo": "far",
            "tag": "commit",
          },
          {
            "watermark": "06",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "08",
          },
        ],
        [
          "commit",
          {
            "tag": "commit",
          },
          {
            "watermark": "08",
          },
        ],
      ]
    `);
    });

    test('watermark too old (serving)', async () => {
      // '01' is not the replica version, and not a watermark in the changeLog
      const [sub, _, stream] = createSubscriber('01');
      storer.catchup(sub, 'serving');

      expect(await drain(stream)).toEqual([
        [
          'error',
          {
            type: ErrorType.WatermarkTooOld,
            message: 'earliest supported watermark is 03 (requested 01)',
          },
        ],
      ]);
    });

    test('watermark too old (backup)', async () => {
      // '01' is not the replica version, and not a watermark in the changeLog
      const [sub] = createSubscriber('01');
      storer.catchup(sub, 'backup');

      expect(await fatalErrors.dequeue()).toMatchInlineSnapshot(
        `[AutoResetSignal: backup replica at watermark 01 is behind change db: 03)]`,
      );
    });

    test('queued if transaction in progress', async () => {
      const [sub1, _0, stream1] = createSubscriber('03');
      const [sub2, _1, stream2] = createSubscriber('06');

      // This should be buffered until catchup is complete.
      void sub1.send([
        '09',
        ['begin', messages.begin(), {commitWatermark: '0a'}],
      ]);
      void sub1.send([
        '0a',
        ['commit', messages.commit({buffer: 'me'}), {watermark: '0a'}],
      ]);
      void sub2.send([
        '09',
        ['begin', messages.begin(), {commitWatermark: '0a'}],
      ]);
      void sub2.send([
        '0a',
        ['commit', messages.commit({buffer: 'me'}), {watermark: '0a'}],
      ]);

      // Start a transaction before enqueuing catchup.
      storer.store([
        '07',
        ['begin', messages.begin(), {commitWatermark: '08'}],
      ]);
      // Enqueue catchup before transaction completes.
      storer.catchup(sub1, 'serving');
      storer.catchup(sub2, 'serving');
      // Finish the transaction.
      storer.store([
        '08',
        ['commit', messages.commit({extra: 'stuff'}), {watermark: '08'}],
      ]);

      storer.status(['status', {ack: true}, {watermark: '0e'}]);
      storer.status(['status', {ack: true}, {watermark: '0f'}]);

      // Catchup should wait for the transaction to complete before querying
      // the database, and start after watermark '03'.
      expect(await drain(stream1, '0a')).toMatchInlineSnapshot(`
      [
        [
          "status",
          {
            "tag": "status",
          },
        ],
        [
          "begin",
          {
            "boo": "dar",
            "tag": "begin",
          },
          {
            "commitWatermark": "06",
          },
        ],
        [
          "data",
          {
            "tag": "update",
          },
        ],
        [
          "commit",
          {
            "boo": "far",
            "tag": "commit",
          },
          {
            "watermark": "06",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "07",
          },
        ],
        [
          "commit",
          {
            "extra": "stuff",
            "tag": "commit",
          },
          {
            "watermark": "08",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "0a",
          },
        ],
        [
          "commit",
          {
            "buffer": "me",
            "tag": "commit",
          },
          {
            "watermark": "0a",
          },
        ],
      ]
    `);

      // Catchup should wait for the transaction to complete before querying
      // the database, and start after watermark '06'.
      expect(await drain(stream2, '0a')).toMatchInlineSnapshot(`
              [
                [
                  "status",
                  {
                    "tag": "status",
                  },
                ],
                [
                  "begin",
                  {
                    "tag": "begin",
                  },
                  {
                    "commitWatermark": "07",
                  },
                ],
                [
                  "commit",
                  {
                    "extra": "stuff",
                    "tag": "commit",
                  },
                  {
                    "watermark": "08",
                  },
                ],
                [
                  "begin",
                  {
                    "tag": "begin",
                  },
                  {
                    "commitWatermark": "0a",
                  },
                ],
                [
                  "commit",
                  {
                    "buffer": "me",
                    "tag": "commit",
                  },
                  {
                    "watermark": "0a",
                  },
                ],
              ]
            `);

      expect(
        await db`SELECT * FROM "xero_5/cdc"."changeLog" ORDER BY watermark, pos`,
      ).toMatchInlineSnapshot(`
        Result [
          {
            "change": {
              "foo": "bar",
              "tag": "begin",
            },
            "pos": 0n,
            "precommit": null,
            "watermark": "03",
          },
          {
            "change": {
              "tag": "insert",
            },
            "pos": 1n,
            "precommit": null,
            "watermark": "03",
          },
          {
            "change": {
              "bar": "baz",
              "tag": "commit",
            },
            "pos": 2n,
            "precommit": null,
            "watermark": "03",
          },
          {
            "change": {
              "boo": "dar",
              "tag": "begin",
            },
            "pos": 0n,
            "precommit": null,
            "watermark": "06",
          },
          {
            "change": {
              "tag": "update",
            },
            "pos": 1n,
            "precommit": null,
            "watermark": "06",
          },
          {
            "change": {
              "boo": "far",
              "tag": "commit",
            },
            "pos": 2n,
            "precommit": null,
            "watermark": "06",
          },
          {
            "change": {
              "tag": "begin",
            },
            "pos": 0n,
            "precommit": null,
            "watermark": "07",
          },
          {
            "change": {
              "extra": "stuff",
              "tag": "commit",
            },
            "pos": 1n,
            "precommit": "07",
            "watermark": "08",
          },
        ]
      `);

      await expectConsumed('08', '0e', '0f');
    });

    // Similar to "queued if transaction is in progress" but tests rollback.
    test('queued until transaction is rolled back', async () => {
      const [sub1, _0, stream1] = createSubscriber('03');
      const [sub2, _1, stream2] = createSubscriber('06');

      // This should be buffered until catchup is complete.
      void sub1.send([
        '09',
        ['begin', messages.begin(), {commitWatermark: '0a'}],
      ]);
      void sub1.send([
        '0a',
        ['commit', messages.commit({buffer: 'me'}), {watermark: '0a'}],
      ]);
      void sub2.send([
        '09',
        ['begin', messages.begin(), {commitWatermark: '0a'}],
      ]);
      void sub2.send([
        '0a',
        ['commit', messages.commit({buffer: 'me'}), {watermark: '0a'}],
      ]);

      // Start a transaction before enqueuing catchup.
      storer.store([
        '07',
        ['begin', messages.begin(), {commitWatermark: '08'}],
      ]);
      // Enqueue catchup before transaction completes.
      storer.catchup(sub1, 'backup');
      storer.catchup(sub2, 'serving');
      // Rollback the transaction.
      storer.store(['08', ['rollback', messages.rollback()]]);

      storer.status(['status', {ack: true}, {watermark: '0a'}]);
      storer.status(['status', {ack: true}, {watermark: '0c'}]);

      // Catchup should wait for the transaction to complete before querying
      // the database, and start after watermark '03'.
      expect(await drain(stream1, '0a')).toMatchInlineSnapshot(`
        [
          [
            "status",
            {
              "tag": "status",
            },
          ],
          [
            "begin",
            {
              "boo": "dar",
              "tag": "begin",
            },
            {
              "commitWatermark": "06",
            },
          ],
          [
            "data",
            {
              "tag": "update",
            },
          ],
          [
            "commit",
            {
              "boo": "far",
              "tag": "commit",
            },
            {
              "watermark": "06",
            },
          ],
          [
            "begin",
            {
              "tag": "begin",
            },
            {
              "commitWatermark": "0a",
            },
          ],
          [
            "commit",
            {
              "buffer": "me",
              "tag": "commit",
            },
            {
              "watermark": "0a",
            },
          ],
        ]
      `);

      // Catchup should wait for the transaction to complete before querying
      // the database, and start after watermark '06'.
      expect(await drain(stream2, '0a')).toMatchInlineSnapshot(`
            [
              [
                "status",
                {
                  "tag": "status",
                },
              ],
              [
                "begin",
                {
                  "tag": "begin",
                },
                {
                  "commitWatermark": "0a",
                },
              ],
              [
                "commit",
                {
                  "buffer": "me",
                  "tag": "commit",
                },
                {
                  "watermark": "0a",
                },
              ],
            ]
          `);

      expect(
        await db`SELECT * FROM "xero_5/cdc"."changeLog" ORDER BY watermark, pos`,
      ).toMatchInlineSnapshot(`
        Result [
          {
            "change": {
              "foo": "bar",
              "tag": "begin",
            },
            "pos": 0n,
            "precommit": null,
            "watermark": "03",
          },
          {
            "change": {
              "tag": "insert",
            },
            "pos": 1n,
            "precommit": null,
            "watermark": "03",
          },
          {
            "change": {
              "bar": "baz",
              "tag": "commit",
            },
            "pos": 2n,
            "precommit": null,
            "watermark": "03",
          },
          {
            "change": {
              "boo": "dar",
              "tag": "begin",
            },
            "pos": 0n,
            "precommit": null,
            "watermark": "06",
          },
          {
            "change": {
              "tag": "update",
            },
            "pos": 1n,
            "precommit": null,
            "watermark": "06",
          },
          {
            "change": {
              "boo": "far",
              "tag": "commit",
            },
            "pos": 2n,
            "precommit": null,
            "watermark": "06",
          },
        ]
      `);

      await expectConsumed('0a', '0c');
    });

    test('catchup does not include subsequent transactions', async () => {
      const [sub, _0, stream] = createSubscriber('03');

      // This should be buffered until catchup is complete.
      void sub.send([
        '0b',
        ['begin', messages.begin(), {commitWatermark: '0c'}],
      ]);
      void sub.send([
        '0c',
        ['commit', messages.commit({waa: 'hoo'}), {watermark: '0c'}],
      ]);

      // Start a transaction before enqueuing catchup.
      storer.store([
        '07',
        ['begin', messages.begin(), {commitWatermark: '08'}],
      ]);
      // Enqueue catchup before transaction completes.
      storer.catchup(sub, 'serving');
      // Finish the transaction.
      storer.store([
        '08',
        ['commit', messages.commit({extra: 'fields'}), {watermark: '08'}],
      ]);

      // And finish another the transaction. In reality, these would be
      // sent by the forwarder, but we skip it in the test to confirm that
      // catchup doesn't include the next transaction.
      storer.store([
        '09',
        ['begin', messages.begin(), {commitWatermark: '0a'}],
      ]);
      storer.store(['0a', ['commit', messages.commit(), {watermark: '0a'}]]);

      storer.status(['status', {ack: true}, {watermark: '0d'}]);
      storer.status(['status', {ack: true}, {watermark: '0e'}]);

      // Wait for the storer to commit that transaction.
      for (let i = 0; i < 10; i++) {
        const result =
          await db`SELECT * FROM "xero_5/cdc"."changeLog" WHERE watermark = '0a'`;
        if (result.length) {
          break;
        }
        await sleep(10);
      }

      // Messages should catchup from after '03' and include '06'
      // from the pending transaction. '07' and '08' should not be included
      // in the snapshot used for catchup. We confirm this by sending the '0c'
      // message and ensuring that that was sent.
      expect(await drain(stream, '0c')).toMatchInlineSnapshot(`
      [
        [
          "status",
          {
            "tag": "status",
          },
        ],
        [
          "begin",
          {
            "boo": "dar",
            "tag": "begin",
          },
          {
            "commitWatermark": "06",
          },
        ],
        [
          "data",
          {
            "tag": "update",
          },
        ],
        [
          "commit",
          {
            "boo": "far",
            "tag": "commit",
          },
          {
            "watermark": "06",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "07",
          },
        ],
        [
          "commit",
          {
            "extra": "fields",
            "tag": "commit",
          },
          {
            "watermark": "08",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "0c",
          },
        ],
        [
          "commit",
          {
            "tag": "commit",
            "waa": "hoo",
          },
          {
            "watermark": "0c",
          },
        ],
      ]
    `);
      await expectConsumed('08', '0a', '0d', '0e');
    });
  });

  describe('protocol: wss', () => {
    beforeEach(async () => {
      storer = new Storer(
        lc,
        shard,
        'task-id',
        'change-streamer:12345',
        'wss',
        db,
        REPLICA_VERSION,
        msg => consumed.enqueue(msg),
        err => fatalErrors.enqueue(err),
        0.04,
      );
      await storer.assumeOwnership();
      done = storer.run();
    });

    test('ownerAddress is set correctly', async () => {
      expect(
        await db`SELECT "ownerAddress" FROM "xero_5/cdc"."replicationState" WHERE owner = 'task-id'`,
      ).toEqual([{ownerAddress: 'wss://change-streamer:12345'}]);
    });
  });
});
