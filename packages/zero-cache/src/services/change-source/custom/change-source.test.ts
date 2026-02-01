import websocket from '@fastify/websocket';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import Fastify, {type FastifyInstance} from 'fastify';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import type WebSocket from 'ws';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {DbFile, expectTables} from '../../../test/lite.ts';
import {stream, type Sink} from '../../../types/streams.ts';
import {AutoResetSignal} from '../../change-streamer/schema/tables.ts';
import type {ChangeStreamMessage} from '../protocol/current/downstream.ts';
import {
  changeSourceUpstreamSchema,
  type ChangeSourceUpstream,
} from '../protocol/current/upstream.ts';
import {initializeCustomChangeSource} from './change-source.ts';

const APP_ID = 'bongo';

describe('change-source/custom', () => {
  let lc: LogContext;
  let downstream: Promise<Sink<ChangeStreamMessage>>;
  let server: FastifyInstance;
  let changeSourceURI: string;
  let replicaDbFile: DbFile;

  beforeEach(async () => {
    lc = createSilentLogContext();
    server = Fastify();
    await server.register(websocket);

    const {promise, resolve} = resolver<Sink<ChangeStreamMessage>>();
    downstream = promise;
    server.get('/', {websocket: true}, (ws: WebSocket) => {
      const {outstream} = stream<ChangeSourceUpstream, ChangeStreamMessage>(
        lc,
        ws,
        changeSourceUpstreamSchema,
      );
      resolve(outstream);
    });
    changeSourceURI = await server.listen({port: 0});
    lc.info?.(`server running on ${changeSourceURI}`);
    replicaDbFile = new DbFile('custom-change-source');
  });

  afterEach(async () => {
    await server.close();
    replicaDbFile.delete();
  });

  async function streamChanges(changes: ChangeStreamMessage[]) {
    const sink = await downstream;
    for (const change of changes) {
      sink.push(change);
    }
  }

  test('initial-sync', async () => {
    void streamChanges([
      ['begin', {tag: 'begin'}, {commitWatermark: '123'}],
      [
        'data',
        {
          tag: 'create-table',
          spec: {
            schema: 'public',
            name: 'foo',
            primaryKey: ['id'],
            columns: {
              id: {pos: 0, dataType: 'text', notNull: true},
              bar: {pos: 1, dataType: 'text'},
            },
          },
          metadata: {
            rowKey: {
              columns: ['id'],
            },
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-index',
          spec: {
            name: 'public_foo_index',
            schema: 'public',
            tableName: 'foo',
            columns: {id: 'ASC'},
            unique: true,
          },
        },
      ],
      [
        'data',
        {
          tag: 'insert',
          relation: {
            schema: 'public',
            name: 'foo',
            keyColumns: ['id'],
            rowKey: {
              columns: ['id'],
            },
          },
          new: {id: 'abcde', bar: 'baz'},
        },
      ],
      [
        'data',
        {
          tag: 'create-table',
          spec: {
            schema: 'bongo_0',
            name: 'clients',
            primaryKey: ['clientGroupID', 'clientID'],
            columns: {
              clientGroupID: {pos: 0, dataType: 'text', notNull: true},
              clientID: {pos: 1, dataType: 'text', notNull: true},
              lastMutationID: {pos: 2, dataType: 'bigint'},
              userID: {pos: 3, dataType: 'text'},
            },
          },
          metadata: {
            rowKey: {
              columns: ['clientGroupID', 'clientID'],
            },
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-index',
          spec: {
            name: 'bongo_clients_key',
            schema: 'bongo_0',
            tableName: 'clients',
            columns: {
              clientGroupID: 'ASC',
              clientID: 'ASC',
            },
            unique: true,
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-table',
          spec: {
            schema: 'bongo_0',
            name: 'mutations',
            primaryKey: ['clientGroupID', 'clientID', 'mutationID'],
            columns: {
              clientGroupID: {pos: 0, dataType: 'text', notNull: true},
              clientID: {pos: 1, dataType: 'text', notNull: true},
              mutationID: {pos: 2, dataType: 'bigint', notNull: true},
              mutation: {pos: 3, dataType: 'json'},
            },
          },
          metadata: {
            rowKey: {
              columns: ['clientGroupID', 'clientID', 'mutationID'],
            },
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-index',
          spec: {
            name: 'bongo_mutations_key',
            schema: 'bongo_0',
            tableName: 'mutations',
            columns: {
              clientGroupID: 'ASC',
              clientID: 'ASC',
              mutationID: 'ASC',
            },
            unique: true,
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-table',
          spec: {
            schema: 'bongo',
            name: 'permissions',
            primaryKey: ['lock'],
            columns: {
              lock: {pos: 0, dataType: 'bool', notNull: true},
              permissions: {pos: 1, dataType: 'json'},
              hash: {pos: 2, dataType: 'text'},
            },
          },
          metadata: {
            rowKey: {
              columns: ['lock'],
            },
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-index',
          spec: {
            name: 'bongo_permissions_key',
            schema: 'bongo',
            tableName: 'permissions',
            columns: {lock: 'ASC'},
            unique: true,
          },
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '123'}],
    ]);

    await initializeCustomChangeSource(
      lc,
      changeSourceURI,
      {appID: APP_ID, shardNum: 0, publications: ['b', 'a']},
      replicaDbFile.path,
    );

    expectTables(replicaDbFile.connect(lc), {
      foo: [{id: 'abcde', bar: 'baz', ['_0_version']: '123'}],
      ['bongo_0.clients']: [],
      ['_zero.replicationState']: [{lock: 1, stateVersion: '123'}],
      ['_zero.replicationConfig']: [
        {
          lock: 1,
          replicaVersion: '123',
          publications: '["a","b"]',
        },
      ],
      ['_zero.changeLog2']: [
        // changeLog should be set up but empty, since it is
        // unnecessary / wasteful to record the initial state
        // in the change log.
      ],
      ['_zero.column_metadata']: [
        {
          character_max_length: null,
          column_name: 'id',
          is_array: 0,
          is_enum: 0,
          is_not_null: 1,
          table_name: 'foo',
          upstream_type: 'text',
          backfill: null,
        },
        {
          character_max_length: null,
          column_name: 'bar',
          is_array: 0,
          is_enum: 0,
          is_not_null: 0,
          table_name: 'foo',
          upstream_type: 'text',
          backfill: null,
        },
        {
          character_max_length: null,
          column_name: 'clientGroupID',
          is_array: 0,
          is_enum: 0,
          is_not_null: 1,
          table_name: 'bongo_0.clients',
          upstream_type: 'text',
          backfill: null,
        },
        {
          character_max_length: null,
          column_name: 'clientID',
          is_array: 0,
          is_enum: 0,
          is_not_null: 1,
          table_name: 'bongo_0.clients',
          upstream_type: 'text',
          backfill: null,
        },
        {
          character_max_length: null,
          column_name: 'lastMutationID',
          is_array: 0,
          is_enum: 0,
          is_not_null: 0,
          table_name: 'bongo_0.clients',
          upstream_type: 'bigint',
          backfill: null,
        },
        {
          character_max_length: null,
          column_name: 'userID',
          is_array: 0,
          is_enum: 0,
          is_not_null: 0,
          table_name: 'bongo_0.clients',
          upstream_type: 'text',
          backfill: null,
        },
        {
          character_max_length: null,
          column_name: 'clientGroupID',
          is_array: 0,
          is_enum: 0,
          is_not_null: 1,
          table_name: 'bongo_0.mutations',
          upstream_type: 'text',
          backfill: null,
        },
        {
          character_max_length: null,
          column_name: 'clientID',
          is_array: 0,
          is_enum: 0,
          is_not_null: 1,
          table_name: 'bongo_0.mutations',
          upstream_type: 'text',
          backfill: null,
        },
        {
          character_max_length: null,
          column_name: 'mutationID',
          is_array: 0,
          is_enum: 0,
          is_not_null: 1,
          table_name: 'bongo_0.mutations',
          upstream_type: 'bigint',
          backfill: null,
        },
        {
          character_max_length: null,
          column_name: 'mutation',
          is_array: 0,
          is_enum: 0,
          is_not_null: 0,
          table_name: 'bongo_0.mutations',
          upstream_type: 'json',
          backfill: null,
        },
        {
          character_max_length: null,
          column_name: 'lock',
          is_array: 0,
          is_enum: 0,
          is_not_null: 1,
          table_name: 'bongo.permissions',
          upstream_type: 'bool',
          backfill: null,
        },
        {
          character_max_length: null,
          column_name: 'permissions',
          is_array: 0,
          is_enum: 0,
          is_not_null: 0,
          table_name: 'bongo.permissions',
          upstream_type: 'json',
          backfill: null,
        },
        {
          character_max_length: null,
          column_name: 'hash',
          is_array: 0,
          is_enum: 0,
          is_not_null: 0,
          table_name: 'bongo.permissions',
          upstream_type: 'text',
          backfill: null,
        },
      ],
    });
  });

  test('reset-required in initial-sync', async () => {
    void streamChanges([
      ['control', {tag: 'reset-required', message: 'watermark is too old yo'}],
    ]);

    await expect(
      initializeCustomChangeSource(
        lc,
        changeSourceURI,
        {appID: APP_ID, shardNum: 0, publications: ['b', 'a']},
        replicaDbFile.path,
      ),
    ).rejects.toThrowError(AutoResetSignal);
  });
});
