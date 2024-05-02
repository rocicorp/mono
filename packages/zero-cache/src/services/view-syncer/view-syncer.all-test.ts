import {LogContext, consoleLogSink} from '@rocicorp/logger';
import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {Queue} from 'shared/src/queue.js';
import {sleep} from 'shared/src/sleep.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import type {Upstream} from 'zero-protocol';
import {DurableStorage} from '../../storage/durable-storage.js';
import {testDBs} from '../../test/db.js';
import {runWithDurableObjectStorage} from '../../test/do.js';
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

  let tables: TableSpec[];

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

    tables = (await getPublicationInfo(db)).tables;
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
    await runWithDurableObjectStorage(async storage => {
      const watcher = new MockInvalidationWatcher();
      const vs = new ViewSyncerService(
        lc,
        serviceID,
        new DurableStorage(storage),
        watcher,
      );

      const done = vs.run();
      await sleep(5);

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
    await runWithDurableObjectStorage(async storage => {
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
              {
                op: 'put',
                hash: 'query-hash1',
                ast: ISSUES_TITLE_QUERY,
              },
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
            putPatch: {minorVersion: 1, stateVersion: '00'},
          },
        },
        id: '9876',
        queries: {
          'query-hash1': {
            ast: {
              select: [
                ['id', 'id'],
                ['title', 'title'],
              ],
              table: 'issues',
            },
            desiredBy: {foo: {minorVersion: 1, stateVersion: '00'}},
            id: 'query-hash1',
          },
        },
        version: {minorVersion: 1, stateVersion: '00'},
      });

      await vs.stop();
      return done;
    });
  });

  class MockInvalidationWatcher
    implements InvalidationWatcher, InvalidationWatcherRegistry
  {
    readonly requests = new Queue<WatchRequest>();
    readonly subscriptions: Subscription<QueryInvalidationUpdate>[] = [];

    watch(
      request: WatchRequest,
    ): Promise<CancelableAsyncIterable<QueryInvalidationUpdate>> {
      void this.requests.enqueue(request);
      const sub = new Subscription<QueryInvalidationUpdate>();
      this.subscriptions.push(sub);
      return Promise.resolve(sub);
    }

    getTableSchemas(): Promise<readonly TableSpec[]> {
      return Promise.resolve(tables);
    }

    getInvalidationWatcher(): Promise<InvalidationWatcher> {
      return Promise.resolve(this);
    }
  }
});
