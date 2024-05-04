import {LogContext, consoleLogSink} from '@rocicorp/logger';
import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {Queue} from 'shared/src/queue.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import type {PokePartBody, Upstream} from 'zero-protocol';
import {TransactionPool} from '../../db/transaction-pool.js';
import {DurableStorage} from '../../storage/durable-storage.js';
import {testDBs} from '../../test/db.js';
import {runWithFakeDurableObjectStorage} from '../../test/fake-do.js';
import {createSilentLogContext} from '../../test/logger.js';
import type {PostgresDB} from '../../types/pg.js';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import type {
  InvalidationWatcher,
  QueryInvalidationUpdate,
  WatchRequest,
} from '../invalidation-watcher/invalidation-watcher.js';
import type {InvalidationWatcherRegistry} from '../invalidation-watcher/registry.js';
import {getPublicationInfo} from '../replicator/tables/published.js';
import type {TableSpec} from '../replicator/tables/specs.js';
import {loadCVR} from './cvr.js';
import {ViewSyncerService} from './view-syncer.js';

describe('view-syncer/service', () => {
  let db: PostgresDB;
  const lc = new LogContext('debug', {}, consoleLogSink);
  createSilentLogContext();

  beforeEach(async () => {
    db = await testDBs.create('view_syncer_service_test');
    await db`
    CREATE TABLE issues (
      id text PRIMARY KEY,
      owner_id text,
      parent_id text,
      title text,
      _0_version VARCHAR(38)
    );
    CREATE TABLE users (
      id text PRIMARY KEY,
      name text,
      _0_version VARCHAR(38)
    );

    INSERT INTO users (id, name, _0_version) VALUES (100, 'Alice', '0a');
    INSERT INTO users (id, name, _0_version) VALUES (101, 'Bob', '0b');
    INSERT INTO users (id, name, _0_version) VALUES (102, 'Candice', '0c');

    INSERT INTO issues (id, title, owner_id, _0_version) VALUES (1, 'parent issue foo', 100, '1a0');
    INSERT INTO issues (id, title, owner_id, _0_version) VALUES (2, 'parent issue bar', 101, '1ab');
    INSERT INTO issues (id, title, owner_id, parent_id, _0_version) VALUES (3, 'foo', 102, 1, '1ca');
    INSERT INTO issues (id, title, owner_id, parent_id, _0_version) VALUES (4, 'bar', 101, 2, '1cd');

    CREATE PUBLICATION zero_all FOR ALL TABLES;
    `.simple();
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  const serviceID = '9876';

  function clientUpstream(...up: Upstream[]): Subscription<Upstream> {
    const stream = new Subscription<Upstream>();
    up.forEach(msg => stream.push(msg));
    return stream;
  }

  const ISSUES_TITLE_QUERY: AST = {
    select: [
      ['id', 'id'],
      ['title', 'title'],
    ],
    table: 'issues',
  };

  test('initializes schema', async () => {
    await runWithFakeDurableObjectStorage(async storage => {
      const watcher = new MockInvalidationWatcher();
      const vs = new ViewSyncerService(
        lc,
        serviceID,
        new DurableStorage(storage),
        watcher,
      );

      const done = vs.run();
      await vs.sync(
        {clientID: 'foo', baseCookie: null},
        clientUpstream([
          'initConnection',
          {
            desiredQueriesPatch: [
              {op: 'put', hash: 'query-hash1', ast: ISSUES_TITLE_QUERY},
            ],
          },
        ]),
      );

      expect(await storage.get('/vs/storage_schema_meta')).toEqual({
        // Update versions as necessary
        version: 1,
        maxVersion: 1,
        minSafeRollbackVersion: 1,
      });

      await vs.stop();
      return done;
    });
  });

  test('adds desired queries from initConnectionMessage', async () => {
    await runWithFakeDurableObjectStorage(async storage => {
      const watcher = new MockInvalidationWatcher();
      const vs = new ViewSyncerService(
        lc,
        serviceID,
        new DurableStorage(storage),
        watcher,
      );

      const done = vs.run();
      await vs.sync(
        {clientID: 'foo', baseCookie: null},
        clientUpstream([
          'initConnection',
          {
            desiredQueriesPatch: [
              {op: 'put', hash: 'query-hash1', ast: ISSUES_TITLE_QUERY},
            ],
          },
        ]),
      );

      const cvr = await loadCVR(new DurableStorage(storage), serviceID);
      expect(cvr).toMatchObject({
        clients: {
          foo: {
            desiredQueryIDs: ['query-hash1'],
            id: 'foo',
            patchVersion: {stateVersion: '00', minorVersion: 1},
          },
        },
        id: '9876',
        queries: {
          'query-hash1': {
            ast: ISSUES_TITLE_QUERY,
            desiredBy: {foo: {stateVersion: '00', minorVersion: 1}},
            id: 'query-hash1',
          },
        },
        version: {stateVersion: '00', minorVersion: 1},
      });

      await vs.stop();
      return done;
    });
  });

  test('subscribes and responds to initial invalidation', async () => {
    await runWithFakeDurableObjectStorage(async storage => {
      const watcher = new MockInvalidationWatcher();
      const vs = new ViewSyncerService(
        lc,
        serviceID,
        new DurableStorage(storage),
        watcher,
      );

      const done = vs.run();
      const downstream = await vs.sync(
        {clientID: 'foo', baseCookie: null},
        clientUpstream([
          'initConnection',
          {
            desiredQueriesPatch: [
              {op: 'put', hash: 'query-hash1', ast: ISSUES_TITLE_QUERY},
            ],
          },
        ]),
      );

      const request = await watcher.requests.dequeue();
      expect(request.fromVersion).toBeUndefined();
      expect(Object.keys(request.queries).length).toBe(1);

      const reader = new TransactionPool(lc);
      const readerDone = reader.run(db);

      watcher.subscriptions[0].push({
        fromVersion: null,
        newVersion: '1xz',
        invalidatedQueries: new Set(),
        reader,
      });

      await watcher.consumed[0].dequeue();
      reader.setDone();

      const expectedPokes = [
        ['pokeStart', {pokeID: '1xz', baseCookie: null, cookie: '1xz'}],
        [
          'pokePart',
          {
            pokeID: '1xz',
            clientsPatch: [{clientID: 'foo', op: 'put'}],
            desiredQueriesPatches: {
              foo: [{ast: ISSUES_TITLE_QUERY, hash: 'query-hash1', op: 'put'}],
            },
            entitiesPatch: [
              {
                op: 'put',
                entityID: {id: '1'},
                entityType: 'issues',
                value: {id: '1', title: 'parent issue foo'},
              },
              {
                op: 'put',
                entityID: {id: '2'},
                entityType: 'issues',
                value: {id: '2', title: 'parent issue bar'},
              },
              {
                op: 'put',
                entityID: {id: '3'},
                entityType: 'issues',
                value: {id: '3', title: 'foo'},
              },
              {
                op: 'put',
                entityID: {id: '4'},
                entityType: 'issues',
                value: {id: '4', title: 'bar'},
              },
            ],
            gotQueriesPatch: [
              {ast: ISSUES_TITLE_QUERY, hash: 'query-hash1', op: 'put'},
            ],
          } satisfies PokePartBody,
        ],
        ['pokeEnd', {pokeID: '1xz'}],
      ];

      let i = 0;
      for await (const poke of downstream) {
        expect(poke).toEqual(expectedPokes[i]);
        if (++i >= expectedPokes.length) {
          break;
        }
      }

      const cvr = await loadCVR(new DurableStorage(storage), serviceID);
      expect(cvr).toMatchObject({
        clients: {
          foo: {
            desiredQueryIDs: ['query-hash1'],
            id: 'foo',
            patchVersion: {stateVersion: '00', minorVersion: 1},
          },
        },
        id: '9876',
        queries: {
          'query-hash1': {
            ast: ISSUES_TITLE_QUERY,
            desiredBy: {foo: {stateVersion: '00', minorVersion: 1}},
            id: 'query-hash1',
            patchVersion: {stateVersion: '1xz'},
            transformationVersion: {stateVersion: '1xz'},
          },
        },
        version: {stateVersion: '1xz'},
      });

      const rowRecords = await storage.list({
        prefix: `/vs/cvr/${serviceID}/d/`,
      });
      expect(new Set(rowRecords.values())).toEqual(
        new Set([
          {
            id: {rowKey: {id: '1'}, schema: 'public', table: 'issues'},
            patchVersion: {stateVersion: '1xz'},
            queriedColumns: {id: ['query-hash1'], title: ['query-hash1']},
            rowVersion: '1a0',
          },
          {
            id: {rowKey: {id: '2'}, schema: 'public', table: 'issues'},
            patchVersion: {stateVersion: '1xz'},
            queriedColumns: {id: ['query-hash1'], title: ['query-hash1']},
            rowVersion: '1ab',
          },
          {
            id: {rowKey: {id: '3'}, schema: 'public', table: 'issues'},
            patchVersion: {stateVersion: '1xz'},
            queriedColumns: {id: ['query-hash1'], title: ['query-hash1']},
            rowVersion: '1ca',
          },
          {
            id: {rowKey: {id: '4'}, schema: 'public', table: 'issues'},
            patchVersion: {stateVersion: '1xz'},
            queriedColumns: {id: ['query-hash1'], title: ['query-hash1']},
            rowVersion: '1cd',
          },
        ]),
      );

      const rowPatches = await storage.list({
        prefix: `/vs/cvr/${serviceID}/p/d/`,
      });
      expect(rowPatches).toEqual(
        new Map([
          [
            '/vs/cvr/9876/p/d/1xz/r/Qxp2tFD-UOgu7-78ZYiLHw',
            {
              columns: ['id', 'title'],
              id: {rowKey: {id: '4'}, schema: 'public', table: 'issues'},
              op: 'put',
              rowVersion: '1cd',
              type: 'row',
            },
          ],
          [
            '/vs/cvr/9876/p/d/1xz/r/VPg9hxKPhJtHB6oYkGqBpw',
            {
              columns: ['id', 'title'],
              id: {rowKey: {id: '2'}, schema: 'public', table: 'issues'},
              op: 'put',
              rowVersion: '1ab',
              type: 'row',
            },
          ],
          [
            '/vs/cvr/9876/p/d/1xz/r/oA1bf0ulYhik9qypZFPeLQ',
            {
              columns: ['id', 'title'],
              id: {rowKey: {id: '1'}, schema: 'public', table: 'issues'},
              op: 'put',
              rowVersion: '1a0',
              type: 'row',
            },
          ],
          [
            '/vs/cvr/9876/p/d/1xz/r/wfZrxQPRsszHpdfLRWoPzA',
            {
              columns: ['id', 'title'],
              id: {rowKey: {id: '3'}, schema: 'public', table: 'issues'},
              op: 'put',
              rowVersion: '1ca',
              type: 'row',
            },
          ],
        ]),
      );

      await vs.stop();
      return Promise.all([done, readerDone]);
    });
  });

  class MockInvalidationWatcher
    implements InvalidationWatcher, InvalidationWatcherRegistry
  {
    readonly requests = new Queue<WatchRequest>();
    readonly subscriptions: Subscription<QueryInvalidationUpdate>[] = [];
    readonly consumed: Queue<true>[] = [];

    watch(
      request: WatchRequest,
    ): Promise<CancelableAsyncIterable<QueryInvalidationUpdate>> {
      void this.requests.enqueue(request);
      const consumed = new Queue<true>();
      const sub = new Subscription<QueryInvalidationUpdate>({
        consumed: () => void consumed.enqueue(true),
      });
      this.subscriptions.push(sub);
      this.consumed.push(consumed);
      return Promise.resolve(sub);
    }

    async getTableSchemas(): Promise<readonly TableSpec[]> {
      const published = await getPublicationInfo(db);
      return published.tables;
    }

    getInvalidationWatcher(): Promise<InvalidationWatcher> {
      return Promise.resolve(this);
    }
  }
});
